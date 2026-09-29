// Package vfs provides virtual-filesystem backends implementing core.FileStore.
// The filesystem is a shared collaboration surface across an agent and its
// subagents (deepagents-style): large tool results and intermediate artifacts
// are offloaded here to keep the model context lean.
package vfs

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// InState is the default backend: an in-memory path→bytes map. It is durable
// with a checkpointer because it implements core.Snapshottable/core.Restorable
// (ADR-0026): the File checkpointer persists its contents as content-addressed
// blob records, and a resumed run rebuilds them into a fresh InState. Within
// one process the Memory checkpointer simply shares the live handle. Safe for
// concurrent use.
type InState struct {
	mu    sync.RWMutex
	files map[string][]byte
}

// NewInState constructs an empty in-state filesystem.
func NewInState() *InState {
	return &InState{files: map[string][]byte{}}
}

// Read returns the file at path, or an error if absent.
func (s *InState) Read(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.files[path]
	if !ok {
		return nil, fmt.Errorf("vfs: %s not found", path)
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// Write stores data at path, overwriting any existing file.
func (s *InState) Write(path string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := make([]byte, len(data))
	copy(b, data)
	s.files[path] = b
	return nil
}

// List returns the paths under prefix, sorted.
func (s *InState) List(prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for p := range s.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Snapshot exports every stored file as a fresh path→bytes map (deep copies),
// satisfying core.Snapshottable so the File checkpointer can persist the file
// state with each checkpoint.
func (s *InState) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.files))
	for p, b := range s.files {
		cp := make([]byte, len(b))
		copy(cp, b)
		out[p] = cp
	}
	return out
}

// Restore replaces the contents from a Snapshot map (core.Restorable), copying
// each value defensively. A nil map empties the store.
func (s *InState) Restore(files map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	nf := make(map[string][]byte, len(files))
	for p, b := range files {
		cp := make([]byte, len(b))
		copy(cp, b)
		nf[p] = cp
	}
	s.files = nf
	return nil
}

var (
	_ core.FileStore     = (*InState)(nil)
	_ core.Snapshottable = (*InState)(nil)
	_ core.Restorable    = (*InState)(nil)
)
