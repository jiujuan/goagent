package checkpoint

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// File is a JSONL-file Checkpointer: each thread is one append-only file
// (<dir>/<thread>.jsonl), one checkpoint per line. Because it is on disk, a run
// paused in one process (e.g. for human approval) can be resumed in another —
// durable, cross-process resume. It is safe for concurrent use within a process.
//
// Caveat: core.State.Files (the virtual filesystem backend) is not serialized
// (it is a handle, json:"-"); messages, todos, KV and plan state do persist, so
// LLM-agent and plan resume work. Use a Store-backed vfs if files must survive.
type File struct {
	dir string
	mu  sync.Mutex
}

// NewFile opens (creating if needed) a file-backed checkpointer rooted at dir.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	return &File{dir: dir}, nil
}

func (f *File) path(threadID string) string {
	return filepath.Join(f.dir, safeName(threadID)+".jsonl")
}

// Save appends a checkpoint to its thread's file.
func (f *File) Save(_ context.Context, cp *Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return fmt.Errorf("checkpoint: Save requires a non-nil checkpoint with ThreadID")
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	fh, err := os.OpenFile(f.path(cp.ThreadID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	_, err = fh.Write(append(b, '\n'))
	return err
}

func (f *File) readAll(threadID string) ([]*Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fh, err := os.Open(f.path(threadID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer fh.Close()
	var out []*Checkpoint
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow large states
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var cp Checkpoint
		if err := json.Unmarshal(line, &cp); err != nil {
			return nil, fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
		}
		c := cp
		out = append(out, &c)
	}
	return out, sc.Err()
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

// safeName maps a thread id to a filesystem-safe base name.
func safeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '-' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "thread"
	}
	return b.String()
}

var _ Checkpointer = (*File)(nil)
