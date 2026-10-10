// Package checkpoint is v2's durability layer: a tree of state snapshots that
// gives resume, branch/fork, time-travel and human-in-the-loop pause. It
// replaces v1's event-sourcing-by-Actions-replay with LangGraph-style snapshots,
// which are simpler to reason about and natively support branching.
//
// A Checkpoint snapshots core.State after a step. ParentID links snapshots into
// a tree: a linear thread is the degenerate path; a fork is a child snapshot on
// a new thread.
//
// A thread id is a key that durable backends turn into a name, so it has to be
// file-name-safe: core.CheckThreadID is that rule (ADR-0028), and a backend
// rejects an id that fails it instead of sanitizing it.
package checkpoint

import (
	"context"

	"github.com/jiujuan/goagent/core"
)

// Checkpoint is one snapshot of a run's state.
type Checkpoint struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id"`
	ParentID string `json:"parent_id,omitempty"` // tree edge; empty at root
	Step     int    `json:"step"`
	// Revision is assigned by a durable checkpointer when the snapshot becomes
	// visible. It is a per-thread monotonic value used as the compare-and-swap
	// target for a resume claim. Zero is reserved for checkpoints written by
	// older versions and is filled while those histories are read.
	Revision uint64     `json:"revision,omitempty"`
	State    core.State `json:"state"`

	// Pending, when set, marks this as a human-in-the-loop pause point: the
	// tool calls awaiting approval before the run can continue from here.
	Pending *PendingHITL `json:"pending,omitempty"`

	// Pause describes why the run stopped and how it may be recovered. It is
	// optional so checkpoints written before pause metadata was introduced stay
	// readable. Pending pauses use RecoveryResumeTools; model hook pauses use
	// RecoveryReplayModel and deliberately contain no model reply to continue.
	Pause *Pause `json:"pause,omitempty"`

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

// Pause records the phase that requested a durable pause. Recovery is a
// closed set so a reader never guesses how to resume an unfamiliar pause.
type Pause struct {
	// ID identifies this particular pause rather than merely its checkpoint. A
	// structured approval and a resume claim both bind to it, so a decision for
	// an earlier pause cannot unlock a later one accidentally.
	ID       string `json:"id,omitempty"`
	Phase    string `json:"phase"`
	Reason   string `json:"reason,omitempty"`
	Recovery string `json:"recovery"`
}

// EffectivePauseID returns the stable identity used to bind a resume claim or
// approval decision to a checkpoint boundary. Older checkpoints did not carry
// Pause.ID, and failure checkpoints may not carry Pause at all; both need a
// deterministic identity so their recovery path can still be fenced.
func EffectivePauseID(cp *Checkpoint) string {
	if cp == nil {
		return ""
	}
	if cp.Pause != nil {
		if cp.Pause.ID != "" {
			return cp.Pause.ID
		}
		return "legacy-" + cp.ID
	}
	return "checkpoint-" + cp.ID
}

const (
	// RecoveryResumeTools continues a checkpointed, pre-execution tool batch.
	RecoveryResumeTools = "resume_tools"
	// RecoveryReplayModel restarts at the saved model-request boundary.
	RecoveryReplayModel = "replay_model"
)

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
