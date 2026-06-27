package memory

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jiujuan/goagent/embeddings"
)

// FileStore is a persistent vector store: documents and their embeddings are
// appended to a JSONL file (one record per line) and loaded fully into memory
// on startup, where Search reuses the same brute-force cosine ranking as
// InMemoryStore. It mirrors session.FileStore's append-only JSONL model
// (ADR 0009) and keeps the dependency-free posture — swap in pgvector/qdrant
// behind the Store interface for scale. See ADR 0019.
type FileStore struct {
	path     string
	embedder embeddings.Embedder
	mu       sync.RWMutex
	docs     []storedDoc
}

// record is the on-disk JSONL shape: document plus its embedding.
type record struct {
	ID        string         `json:"id"`
	Content   string         `json:"content"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Embedding []float32      `json:"embedding"`
}

// File opens (or creates) a persistent vector store under dir, backed by
// dir/memory.jsonl, and loads any existing records into memory.
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
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			return fmt.Errorf("memory: parse %s: %w", s.path, err)
		}
		s.docs = append(s.docs, storedDoc{
			doc: Document{ID: r.ID, Content: r.Content, Metadata: r.Metadata},
			vec: r.Embedding,
		})
	}
	return sc.Err()
}

// Add implements Store: embeds the documents, appends them to the JSONL file,
// and adds them to the in-memory index.
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

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("memory: open %s: %w", s.path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for i, d := range docs {
		d = ensureID(d)
		line, err := json.Marshal(record{ID: d.ID, Content: d.Content, Metadata: d.Metadata, Embedding: vecs[i]})
		if err != nil {
			return fmt.Errorf("memory: marshal record: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("memory: write %s: %w", s.path, err)
		}
		s.docs = append(s.docs, storedDoc{doc: d, vec: vecs[i]})
	}
	return w.Flush()
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

var _ Store = (*FileStore)(nil)
