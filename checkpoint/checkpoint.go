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
	"fmt"
	"time"

	"github.com/jiujuan/goagent/core"
)

// Ref locates one checkpoint inside one thread. Lineage edges and result
// boundaries carry a Ref rather than a bare checkpoint ID because a fork's
// parent lives on another thread, where the ID alone cannot be resolved.
type Ref struct {
	ThreadID     string `json:"thread_id"`
	CheckpointID string `json:"checkpoint_id"`
}

// StepSnapshot records the model-visible step view that was current when a
// checkpoint was written. It deliberately keeps only names and digests: no
// schemas and no messages, so it is a fingerprint of what the model saw rather
// than a second copy of the request. Records written before it existed read
// back nil.
type StepSnapshot struct {
	Step           int      `json:"step"`
	Advertised     []string `json:"advertised,omitempty"`
	ToolDigest     string   `json:"tool_digest,omitempty"`
	SettingsDigest string   `json:"settings_digest,omitempty"`
	Model          string   `json:"model,omitempty"`
}

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

	// Snapshot is the step view that was current when this checkpoint was
	// written: the advertised tool names and the digests of the tools and
	// settings that formed the model request. nil on records written before it
	// existed and on checkpoints written before a view was frozen.
	Snapshot *StepSnapshot `json:"step_snapshot,omitempty"`

	// Completion records the terminal outcome of one finished run on this
	// snapshot: which run ended, whether it completed or failed, and the frozen
	// result. Nothing writes it automatically — the agent layer records it on
	// request (ADR-0033 D4) — so checkpoints written before it existed, and runs
	// that never recorded a terminal state, read back nil.
	Completion *Completion `json:"completion,omitempty"`

	// ParentRef locates this snapshot's parent across threads. ParentID stays the
	// tree edge that older readers use, and when both are present they must agree.
	// A record with only ParentID is a legacy edge: its thread was never recorded,
	// so a reader must not infer one (ADR-0034 D2).
	ParentRef *Ref `json:"parent_ref,omitempty"`
	// RunID and TurnID are the execution identity that committed this snapshot.
	// One user turn can span several runs — the first plus each resume — so both
	// are kept. They are empty on records committed before run identity existed.
	RunID  string `json:"run_id,omitempty"`
	TurnID string `json:"turn_id,omitempty"`
	// CreatedAt is the UTC time this record was first constructed. A commit retry
	// reuses it instead of re-stamping; records written before it existed stay zero.
	CreatedAt time.Time `json:"created_at,omitzero"`

	// FileSnapshot carries the run's file state, reconstructed at read time
	// from the content-addressed blob records the File checkpointer persists
	// (ADR-0026). It is deliberately not serialized inline here — State.Files
	// stays json:"-" so each checkpoint line carries only a small path→hash
	// index — and is the in-memory handoff the agent layer feeds into a
	// core.Restorable backend. nil means no portable file snapshot was supplied;
	// a non-nil empty map is an explicit empty file view.
	FileSnapshot map[string][]byte `json:"-"`
}

// validateLineage gates the lineage fields a writer supplies: a ParentRef is
// either absent, as in every record written before it existed, or complete and
// consistent with ParentID. Legacy records keep loading and re-saving unchanged;
// only a newly written contradiction is rejected, because guessing a parent thread
// would invent lineage the store never recorded.
func validateLineage(cp *Checkpoint) error {
	if cp == nil || cp.ParentRef == nil {
		return nil
	}
	if cp.ParentRef.ThreadID == "" || cp.ParentRef.CheckpointID == "" {
		return fmt.Errorf("checkpoint: parent ref %s/%s requires both a thread ID and a checkpoint ID", cp.ParentRef.ThreadID, cp.ParentRef.CheckpointID)
	}
	if cp.ParentID == "" || cp.ParentID != cp.ParentRef.CheckpointID {
		return fmt.Errorf("checkpoint: parent ref points to checkpoint %q but parent_id is %q", cp.ParentRef.CheckpointID, cp.ParentID)
	}
	return nil
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

const (
	// CompletionCompleted marks a run that finished and produced a result.
	CompletionCompleted = "completed"
	// CompletionFailed marks a run that ended in an error.
	CompletionFailed = "failed"
)

// Completion is the terminal record of one finished run, attached to the
// checkpoint boundary that run last committed. It is written by an explicit
// record call, never by the ordinary save path, and it is immutable once
// stored: a later run on the same thread gets its own record instead of
// overwriting this snapshot (ADR-0033 D4).
type Completion struct {
	// RunID is the run this record belongs to; identity checks rely on it so a
	// stale result cannot be attached to another run's boundary.
	RunID string `json:"run_id"`
	// Kind is CompletionCompleted or CompletionFailed.
	Kind string `json:"kind"`
	// Result is the run's frozen final message. It is nil for a failed run.
	Result *core.Result `json:"result,omitempty"`
	// Err is the failure text for Kind == CompletionFailed, empty otherwise.
	Err string `json:"err,omitempty"`
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
