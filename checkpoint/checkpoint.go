// Package checkpoint is v2's durability layer: a tree of state snapshots that
// gives resume, branch/fork, time-travel and human-in-the-loop pause. It
// replaces v1's event-sourcing-by-Actions-replay with LangGraph-style snapshots,
// which are simpler to reason about and natively support branching.
//
// A Checkpoint snapshots core.State after a step. ParentID links snapshots into
// a tree: a linear thread is the degenerate path; a fork is a child snapshot on
// a new thread.
package checkpoint

import (
	"context"

	"github.com/jiujuan/goagent/core"
)

// Checkpoint is one snapshot of a run's state.
type Checkpoint struct {
	ID       string     `json:"id"`
	ThreadID string     `json:"thread_id"`
	ParentID string     `json:"parent_id,omitempty"` // tree edge; empty at root
	Step     int        `json:"step"`
	State    core.State `json:"state"`

	// Pending, when set, marks this as a human-in-the-loop pause point: the
	// tool calls awaiting approval before the run can continue from here.
	Pending *PendingHITL `json:"pending,omitempty"`

	// FileSnapshot carries the run's file state, reconstructed at read time
	// from the content-addressed blob records the File checkpointer persists
	// (ADR-0026). It is deliberately not serialized inline here — State.Files
	// stays json:"-" so each checkpoint line carries only a small path→hash
	// index — and is the in-memory handoff the agent layer feeds into a
	// core.Restorable backend. nil means the backend was not snapshottable
	// (externally managed) or there was nothing to persist.
	FileSnapshot map[string][]byte `json:"-"`
}

// PendingHITL captures the tool calls a run is blocked on at an interrupt.
type PendingHITL struct {
	Step    int             `json:"step"`
	Pending []core.ToolCall `json:"pending"`
}

// Checkpointer persists and retrieves checkpoints.
type Checkpointer interface {
	// Save stores a checkpoint. Snapshots are append-only and never overwritten,
	// so History can offer time-travel.
	Save(ctx context.Context, cp *Checkpoint) error
	// Load fetches a specific checkpoint by id within a thread.
	Load(ctx context.Context, threadID, checkpointID string) (*Checkpoint, error)
	// Latest returns the most recent checkpoint of a thread, or nil if none.
	Latest(ctx context.Context, threadID string) (*Checkpoint, error)
	// History lists a thread's checkpoints oldest-first (for time-travel).
	History(ctx context.Context, threadID string) ([]*Checkpoint, error)
}
