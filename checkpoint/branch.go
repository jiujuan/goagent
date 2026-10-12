package checkpoint

import (
	"fmt"
	"time"

	"github.com/jiujuan/goagent/core"
)

// lineageNow is the clock a newly constructed record uses for its creation time.
// Tests replace it so a retried construction stays deterministic.
var lineageNow = func() time.Time { return time.Now().UTC() }

// Fork creates a new checkpoint that branches off parent onto a new thread. It
// copies parent's snapshot and links back via ParentID, so the snapshot tree
// records "the same history point, a different continuation" — the basis for
// time-travel and try-another-path.
//
// The returned checkpoint is not yet saved; the caller assigns it to a run and
// Saves it as the branch advances.
func Fork(parent *Checkpoint, newThreadID, newID string) (*Checkpoint, error) {
	if parent == nil {
		return nil, fmt.Errorf("checkpoint: fork requires a non-nil parent")
	}
	if err := core.CheckThreadID(newThreadID); err != nil {
		return nil, fmt.Errorf("checkpoint: fork target thread: %w", err)
	}
	if newThreadID == parent.ThreadID {
		return nil, fmt.Errorf("checkpoint: fork target thread %q matches the parent thread", newThreadID)
	}
	if newID == "" {
		return nil, fmt.Errorf("checkpoint: fork requires a non-empty checkpoint ID")
	}
	if newID == parent.ID {
		return nil, fmt.Errorf("checkpoint: fork checkpoint ID %q matches the parent checkpoint", newID)
	}

	child, err := freezeCheckpoint(parent)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: fork parent: %w", err)
	}
	child.ID = newID
	child.ThreadID = newThreadID
	child.ParentID = parent.ID
	// The parent lives on the source thread, so the edge is only locatable with
	// that thread recorded (ADR-0034 D2).
	child.ParentRef = &Ref{ThreadID: parent.ThreadID, CheckpointID: parent.ID}
	child.Revision = 0
	child.Pending = nil
	child.Pause = nil
	// A branch is a new continuation, not a finished run, so it never inherits
	// the parent's terminal record (ADR-0033 D4). The file snapshot keeps copying.
	child.Completion = nil
	// Nor does it inherit which run or turn produced the parent: the branch's own
	// first input opens a new turn, and its creation time is its own.
	child.RunID = ""
	child.TurnID = ""
	child.CreatedAt = lineageNow()
	return child, nil
}
