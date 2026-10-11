package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Memory is an in-memory Checkpointer for prototyping and tests. Checkpoints are
// kept append-only per thread, preserving order for Latest/History. Safe for
// concurrent use.
type Memory struct {
	mu       sync.RWMutex
	byThread map[string][]*Checkpoint
	durable  map[string]*memoryDurableState
	now      func() time.Time
}

// NewMemory constructs an in-memory checkpointer.
func NewMemory() *Memory {
	return &Memory{
		byThread: map[string][]*Checkpoint{},
		durable:  map[string]*memoryDurableState{},
		now:      time.Now,
	}
}

func (m *Memory) nowTime() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}

// Save appends a checkpoint to its thread.
func (m *Memory) Save(_ context.Context, cp *Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return fmt.Errorf("checkpoint: Save requires a non-nil checkpoint with ThreadID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.commitLocked(CommitRequest{Checkpoint: cp})
	return err
}

// Load returns the checkpoint with checkpointID in threadID, or an error.
func (m *Memory) Load(_ context.Context, threadID, checkpointID string) (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, cp := range m.byThread[threadID] {
		if cp.ID == checkpointID {
			return cloneCheckpoint(cp)
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
	return cloneCheckpoint(hist[len(hist)-1])
}

// History lists a thread's checkpoints oldest-first.
func (m *Memory) History(_ context.Context, threadID string) ([]*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hist := m.byThread[threadID]
	out := make([]*Checkpoint, len(hist))
	for i, cp := range hist {
		cloned, err := cloneCheckpoint(cp)
		if err != nil {
			return nil, err
		}
		out[i] = cloned
	}
	return out, nil
}

var _ Checkpointer = (*Memory)(nil)
