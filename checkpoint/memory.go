package checkpoint

import (
	"context"
	"fmt"
	"sync"
)

// Memory is an in-memory Checkpointer for prototyping and tests. Checkpoints
// are kept append-only per thread, preserving order for Latest/History. It is
// safe for concurrent use.
type Memory struct {
	mu       sync.RWMutex
	byThread map[string][]*Checkpoint
}

// NewMemory constructs an in-memory checkpointer.
func NewMemory() *Memory {
	return &Memory{byThread: map[string][]*Checkpoint{}}
}

// Save appends a copy-by-reference checkpoint to its thread.
func (m *Memory) Save(_ context.Context, cp *Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return fmt.Errorf("checkpoint: Save requires a non-nil checkpoint with ThreadID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byThread[cp.ThreadID] = append(m.byThread[cp.ThreadID], cp)
	return nil
}

// Load returns the checkpoint with checkpointID in threadID, or an error.
func (m *Memory) Load(_ context.Context, threadID, checkpointID string) (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, cp := range m.byThread[threadID] {
		if cp.ID == checkpointID {
			return cp, nil
		}
	}
	return nil, fmt.Errorf("checkpoint: %s/%s not found", threadID, checkpointID)
}

// Latest returns the most recent checkpoint of a thread, or nil if none.
func (m *Memory) Latest(_ context.Context, threadID string) (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hist := m.byThread[threadID]
	if len(hist) == 0 {
		return nil, nil
	}
	return hist[len(hist)-1], nil
}

// History lists a thread's checkpoints oldest-first.
func (m *Memory) History(_ context.Context, threadID string) ([]*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hist := m.byThread[threadID]
	out := make([]*Checkpoint, len(hist))
	copy(out, hist)
	return out, nil
}

var _ Checkpointer = (*Memory)(nil)
