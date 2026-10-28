package agent

import (
	"context"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// RunResult is what one finished run reports about itself: the identity that
// produced it, the frozen result, and the checkpoint boundary this run actually
// committed.
//
// CheckpointRef and Revision come from the boundary recorded during the run, never
// from the thread's current head, so a later run on the same thread cannot move an
// earlier result's boundary. A run that committed nothing of its own reports
// CheckpointRef = nil and Revision = 0 — an inherited head is not a credential.
//
// TurnID is empty when the run ended before it started executing (the turn is
// allocated when execution begins) or when it continued a pause recorded before run
// identity existed; the value is left unknown rather than guessed.
type RunResult struct {
	// ThreadID is the thread the run executed on.
	ThreadID string
	// RunID is this run's identity, matching the RunID its committed checkpoints
	// carry.
	RunID string
	// TurnID is the user turn this run belongs to; one turn can span the first run
	// and each resume of it.
	TurnID string
	// Result is the frozen final message, deep-copied so editing what a caller got
	// cannot reach what a later reader sees.
	Result core.Result
	// CheckpointRef locates this run's committed boundary, or nil when the run
	// committed nothing.
	CheckpointRef *checkpoint.Ref
	// Revision is the store revision of that boundary, 0 when there is none.
	Revision uint64
	// Paused reports that the run stopped for a human decision instead of finishing.
	Paused bool
}

// DetailedResult returns the outcome drive() froze when the run settled, together
// with the identity and boundary recorded at that moment. It reads only the frozen
// copy: the exported Run.ID and Run.ThreadID are a caller's to rewrite, and they are
// not consulted here — before the run settles the identifiers reported are the ones
// fixed when the handle was created, with an empty result, no boundary and Paused
// false. Use Start followed by Await, Wait, or consume Iter to reach settlement.
func (r *Run) DetailedResult() RunResult {
	r.terminalMu.Lock()
	defer r.terminalMu.Unlock()
	out := RunResult{ThreadID: r.terminalThreadID, RunID: r.terminalRunID, TurnID: r.terminalTurnID}
	if out.ThreadID == "" {
		out.ThreadID = r.identity.threadID
	}
	if out.RunID == "" {
		out.RunID = r.identity.runID
	}
	if !r.terminalFrozen {
		return out
	}
	out.Paused = r.terminalPaused
	if cloned, err := core.CloneMessage(r.terminalResult.Message); err == nil {
		out.Result = core.Result{Message: cloned}
	}
	if r.boundaryOK && r.boundaryID != "" {
		out.CheckpointRef = &checkpoint.Ref{ThreadID: out.ThreadID, CheckpointID: r.boundaryID}
		out.Revision = r.boundaryRev
	}
	return out
}

// RunDetailed drives a run to completion like Run and reports the structured
// outcome: identity, frozen result, and the checkpoint boundary this run committed.
// Errors still come back through error, with whatever identity is already known kept
// in the result for diagnosis.
//
// It does not record a terminal checkpoint — that stays an explicit RecordCompletion
// call (ADR-0033 D4) — and it does not change what Run or Stream do.
func (a *Agent) RunDetailed(ctx context.Context, input string, opts ...RunOption) (RunResult, error) {
	run := a.Stream(ctx, input, opts...)
	_, err := run.Wait()
	return run.DetailedResult(), err
}
