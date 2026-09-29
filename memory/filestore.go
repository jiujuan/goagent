package memory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jiujuan/goagent/embeddings"
)

// opDelete marks a tombstone record: a later line that removes an earlier
// document by ID.
const opDelete = "del"

// FileStore is a persistent vector store: documents and their embeddings are
// appended to a JSONL file (one record per line) and replayed into memory on
// startup, where Search reuses the same brute-force cosine ranking as
// InMemoryStore. The file is an append-only log, so deletes are recorded as
// tombstones and Compact rewrites it from the live set (see NeedsCompaction).
// It mirrors session.FileStore's JSONL model (ADR 0009) and keeps the
// dependency-free posture — swap in pgvector/qdrant behind the Store interface
// for scale. See ADR 0019.
//
// One process owns the file: Add/Delete append without locking the OS file, so
// concurrent writers would interleave partial lines.
type FileStore struct {
	path     string
	embedder embeddings.Embedder
	mu       sync.RWMutex
	docs     []storedDoc
	rows     int // JSONL lines in the file (live, superseded and tombstoned)
	dropped  int // lines skipped at load as unreadable
}

// record is the on-disk JSONL shape: document plus its embedding, or a
// tombstone (ID + Op) for a deletion.
type record struct {
	ID        string         `json:"id"`
	Op        string         `json:"op,omitempty"`
	Content   string         `json:"content,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Embedding []float32      `json:"embedding,omitempty"`
}

// File opens (or creates) a persistent vector store under dir, backed by
// dir/memory.jsonl, and replays any existing records into memory. Unreadable
// lines are skipped rather than fatal (Dropped reports how many), so a torn
// tail from a crash does not make the whole store unusable.
func File(dir string, e embeddings.Embedder) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("memory: create dir: %w", err)
	}
	fs := &FileStore{path: filepath.Join(dir, "memory.jsonl"), embedder: e}
	if err := fs.load(); err != nil {
		return nil, err
	}
	return fs, nil
}

// load replays the JSONL file into memory.
func (s *FileStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("memory: open %s: %w", s.path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow long lines (embeddings)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		s.rows++
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			s.dropped++
			continue
		}
		s.replay(r)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("memory: read %s: %w", s.path, err)
	}
	return nil
}

// replay folds one record into the in-memory index. Records are appended, never
// merged, so Add keeps its plain append semantics.
func (s *FileStore) replay(r record) {
	if r.Op == opDelete {
		s.drop(r.ID)
		return
	}
	s.docs = append(s.docs, storedDoc{
		doc: Document{ID: r.ID, Content: r.Content, Metadata: r.Metadata},
		vec: r.Embedding,
	})
}

// drop removes every document with the given ID (no-op if absent).
func (s *FileStore) drop(id string) {
	kept := s.docs[:0]
	for _, sd := range s.docs {
		if sd.doc.ID == id {
			continue
		}
		kept = append(kept, sd)
	}
	s.docs = kept
}

// Add implements Store: embeds the documents, appends them to the JSONL file,
// and adds them to the in-memory index. The index only advances once the write
// has succeeded, so a failed append leaves memory and file consistent.
func (s *FileStore) Add(ctx context.Context, docs ...Document) error {
	if len(docs) == 0 {
		return nil
	}
	texts := make([]string, len(docs))
	for i, d := range docs {
		texts[i] = d.Content
	}
	vecs, err := s.embedder.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("memory: embed documents: %w", err)
	}
	if len(vecs) != len(docs) {
		return fmt.Errorf("memory: embedder returned %d vectors for %d docs", len(vecs), len(docs))
	}

	// Build the whole batch before touching either the file or the index.
	var buf bytes.Buffer
	assigned := make([]Document, len(docs))
	for i, d := range docs {
		d = ensureID(d)
		assigned[i] = d
		line, err := json.Marshal(record{ID: d.ID, Content: d.Content, Metadata: d.Metadata, Embedding: vecs[i]})
		if err != nil {
			return fmt.Errorf("memory: marshal record: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.appendLines(buf.Bytes()); err != nil {
		return err
	}
	for i, d := range assigned {
		s.docs = append(s.docs, storedDoc{doc: d, vec: vecs[i]})
	}
	s.rows += len(assigned)
	return nil
}

// Delete implements Mutable: appends a tombstone for each stored id and drops
// the documents from the index. Ids not in the store are ignored.
func (s *FileStore) Delete(_ context.Context, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var existing []string
	for _, id := range ids {
		if s.indexOf(id) >= 0 {
			existing = append(existing, id)
		}
	}
	if len(existing) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, id := range existing {
		line, err := json.Marshal(record{ID: id, Op: opDelete})
		if err != nil {
			return fmt.Errorf("memory: marshal tombstone: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if err := s.appendLines(buf.Bytes()); err != nil {
		return err
	}
	for _, id := range existing {
		s.drop(id)
	}
	s.rows += len(existing)
	return nil
}

// appendLines appends a pre-built block of JSONL lines to the file.
func (s *FileStore) appendLines(block []byte) error {
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("memory: open %s: %w", s.path, err)
	}
	defer f.Close()
	if _, err := f.Write(block); err != nil {
		return fmt.Errorf("memory: write %s: %w", s.path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("memory: sync %s: %w", s.path, err)
	}
	return nil
}

// Compact implements Mutable: rewrites the file so it holds exactly the live
// documents, one line each, and drops every superseded record and tombstone.
// The rewrite goes through a sibling file and an atomic rename. It also clears
// the Dropped counter, since those damaged lines are gone.
func (s *FileStore) Compact(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmp := s.path + ".compact"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("memory: create %s: %w", tmp, err)
	}
	w := bufio.NewWriter(f)
	for _, sd := range s.docs {
		line, err := json.Marshal(record{ID: sd.doc.ID, Content: sd.doc.Content, Metadata: sd.doc.Metadata, Embedding: sd.vec})
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("memory: marshal record: %w", err)
		}
		w.Write(line)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("memory: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("memory: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("memory: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("memory: replace %s: %w", s.path, err)
	}

	s.rows = len(s.docs)
	s.dropped = 0
	return nil
}

// NeedsCompaction implements Mutable: reports whether the log has grown to more
// than twice the size of its live content (superseded records and tombstones).
// Compaction is never implicit — the caller decides when to pay for the rewrite.
func (s *FileStore) NeedsCompaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rows > 2*len(s.docs)
}

// Dropped reports how many lines the last load could not parse and skipped. It
// resets on Compact.
func (s *FileStore) Dropped() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dropped
}

// indexOf returns the position of an id in the index, or -1. Caller holds the lock.
func (s *FileStore) indexOf(id string) int {
	for i := range s.docs {
		if s.docs[i].doc.ID == id {
			return i
		}
	}
	return -1
}

// Search implements Store.
func (s *FileStore) Search(ctx context.Context, query string, k int) ([]Document, error) {
	qv, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("memory: embed query: %w", err)
	}
	if len(qv) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rank(qv[0], s.docs, k), nil
}

// Len reports the number of stored documents.
func (s *FileStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs)
}

var (
	_ Store   = (*FileStore)(nil)
	_ Mutable = (*FileStore)(nil)
)
