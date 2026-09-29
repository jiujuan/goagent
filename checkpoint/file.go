package checkpoint

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// File is a JSONL-file Checkpointer: each thread is one append-only file
// (<dir>/<thread>.jsonl), one record per line. Because it is on disk, a run
// paused in one process (e.g. for human approval) can be resumed in another —
// durable, cross-process resume. It is safe for concurrent use within a process.
//
// Two record types share the file (ADR-0026):
//
//   - checkpoint lines: the Checkpoint JSON, plus a small "files" index
//     mapping each virtual-filesystem path to the hash of its content;
//   - blob lines: {"blob":hash,"data":...} carrying each distinct file content
//     once, appended only when that hash has not been written for the thread.
//
// This is how State.Files becomes durable despite being excluded from State's
// own JSON: a snapshottable backend (core.Snapshottable — e.g. vfs.InState)
// has its contents stored content-addressed, and reads rehydrate them into
// Checkpoint.FileSnapshot for the agent layer to restore. A backend that is not
// snapshottable (a real directory, a remote store) is externally managed: it is
// not persisted, and the caller re-supplies it via agent.WithRunFiles on resume.
// Messages, todos, KV and plan state always persist; a thread with no file
// records reads back exactly as before this mechanism existed.
type File struct {
	dir string
	mu  sync.Mutex
	// known maps a thread id to the blob hashes already appended to its file,
	// so Save never rewrites a content it has stored. It is a cache, not the
	// source of truth: a fresh process seeds it by scanning the file once.
	known map[string]map[string]bool
}

// NewFile opens (creating if needed) a file-backed checkpointer rooted at dir.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	return &File{dir: dir, known: map[string]map[string]bool{}}, nil
}

// path resolves a thread id to its file inside the store directory. The id names
// the file verbatim (ADR-0028), so it has to be file-name-safe: without the
// check, an id carrying path syntax would place a thread's records outside dir.
func (f *File) path(threadID string) (string, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return "", err
	}
	return filepath.Join(f.dir, threadID+".jsonl"), nil
}

// checkpointRecord is the on-disk shape of a checkpoint line: the Checkpoint
// fields inline (via embedding) plus the file index.
type checkpointRecord struct {
	*Checkpoint
	Files map[string]string `json:"files,omitempty"`
}

// blobRecord is one content-addressed file payload, written at most once per
// hash per thread.
type blobRecord struct {
	Blob string `json:"blob"`
	Data []byte `json:"data"`
}

// Save appends a checkpoint (and any new file blobs) to its thread's file.
func (f *File) Save(_ context.Context, cp *Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return fmt.Errorf("checkpoint: Save requires a non-nil checkpoint with ThreadID")
	}
	if err := core.CheckThreadID(cp.ThreadID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	known, err := f.knownHashes(cp.ThreadID)
	if err != nil {
		return err
	}

	var blobLines [][]byte
	var index map[string]string
	if ss, ok := cp.State.Files.(core.Snapshottable); ok {
		if snap := ss.Snapshot(); len(snap) > 0 {
			index = make(map[string]string, len(snap))
			for path, data := range snap {
				h := hashOf(data)
				index[path] = h
				if !known[h] {
					known[h] = true
					b, err := json.Marshal(&blobRecord{Blob: h, Data: data})
					if err != nil {
						return fmt.Errorf("checkpoint: marshal blob: %w", err)
					}
					blobLines = append(blobLines, b)
				}
			}
		}
	}

	line, err := json.Marshal(&checkpointRecord{Checkpoint: cp, Files: index})
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}

	fp, err := f.path(cp.ThreadID)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	for _, b := range blobLines {
		if _, err := fh.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	_, err = fh.Write(append(line, '\n'))
	return err
}

// knownHashes returns the thread's already-written blob hashes, seeding the
// per-process cache from the file on first touch. Callers hold f.mu.
func (f *File) knownHashes(threadID string) (map[string]bool, error) {
	if k, ok := f.known[threadID]; ok {
		return k, nil
	}
	k := map[string]bool{}
	_, blobs, err := f.scan(threadID)
	if err != nil {
		return nil, err
	}
	for h := range blobs {
		k[h] = true
	}
	f.known[threadID] = k
	return k, nil
}

// scan reads the thread file once: checkpoints in order (without snapshots
// attached yet) and the blob dictionary. Missing file yields empty results.
func (f *File) scan(threadID string) ([]*checkpointRecord, map[string][]byte, error) {
	fp, err := f.path(threadID)
	if err != nil {
		return nil, nil, err
	}
	fh, err := os.Open(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, map[string][]byte{}, nil
		}
		return nil, nil, err
	}
	defer fh.Close()
	var recs []*checkpointRecord
	blobs := map[string][]byte{}
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow large states
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe struct {
			Blob string `json:"blob"`
			ID   string `json:"id"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			return nil, nil, fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
		}
		switch {
		case probe.Blob != "":
			var b blobRecord
			if err := json.Unmarshal(line, &b); err != nil {
				return nil, nil, fmt.Errorf("checkpoint: corrupt blob in %s: %w", threadID, err)
			}
			blobs[b.Blob] = b.Data
		default:
			var r checkpointRecord
			r.Checkpoint = &Checkpoint{}
			if err := json.Unmarshal(line, &r); err != nil {
				return nil, nil, fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
			}
			recs = append(recs, &r)
		}
	}
	return recs, blobs, sc.Err()
}

// rebuild attaches each checkpoint's file snapshot (path→bytes) from its index
// and the thread's blob dictionary. An index referencing an absent blob is a
// corrupt/partial file and reported as such.
func rebuild(cps []*checkpointRecord, blobs map[string][]byte) error {
	for _, r := range cps {
		if len(r.Files) == 0 {
			continue
		}
		snap := make(map[string][]byte, len(r.Files))
		for path, h := range r.Files {
			data, ok := blobs[h]
			if !ok {
				return fmt.Errorf("checkpoint: %s references missing blob %s", r.ThreadID, h)
			}
			snap[path] = data
		}
		r.Checkpoint.FileSnapshot = snap
	}
	return nil
}

// readAll returns the thread's checkpoints, oldest first, with file snapshots
// rehydrated. Records whose own ThreadID differs from the requested one are
// left out: before ADR-0028 two distinct ids could be sanitized onto one file
// name, so a pre-existing file may hold more threads' records than its own.
func (f *File) readAll(threadID string) ([]*Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs, blobs, err := f.scan(threadID)
	if err != nil {
		return nil, err
	}
	own := make([]*checkpointRecord, 0, len(recs))
	for _, r := range recs {
		if r.ThreadID != threadID {
			continue
		}
		own = append(own, r)
	}
	if err := rebuild(own, blobs); err != nil {
		return nil, err
	}
	// A full read is a cheap chance to prime the dedup cache too.
	if _, ok := f.known[threadID]; !ok {
		k := make(map[string]bool, len(blobs))
		for h := range blobs {
			k[h] = true
		}
		f.known[threadID] = k
	}
	out := make([]*Checkpoint, 0, len(own))
	for _, r := range own {
		out = append(out, r.Checkpoint)
	}
	return out, nil
}

// Load returns the checkpoint with checkpointID in threadID.
func (f *File) Load(_ context.Context, threadID, checkpointID string) (*Checkpoint, error) {
	all, err := f.readAll(threadID)
	if err != nil {
		return nil, err
	}
	for _, cp := range all {
		if cp.ID == checkpointID {
			return cp, nil
		}
	}
	return nil, fmt.Errorf("checkpoint: %s/%s not found", threadID, checkpointID)
}

// Latest returns the most recent checkpoint of a thread, or nil if none.
func (f *File) Latest(_ context.Context, threadID string) (*Checkpoint, error) {
	all, err := f.readAll(threadID)
	if err != nil || len(all) == 0 {
		return nil, err
	}
	return all[len(all)-1], nil
}

// History lists a thread's checkpoints oldest-first.
func (f *File) History(_ context.Context, threadID string) ([]*Checkpoint, error) {
	return f.readAll(threadID)
}

func hashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var _ Checkpointer = (*File)(nil)
