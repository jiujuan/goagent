package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// Terminal identity errors. RecordCompletion only accepts a run this agent
// created, that has finished, that actually committed something in this run
// (ADR-0033 D4), and a store that can commit with a revision check.
var (
	// ErrNoRun means no run was given to record.
	ErrNoRun = errors.New("agent: record completion requires a run")
	// ErrRunNotOwned means the run was created by a different agent.
	ErrRunNotOwned = errors.New("agent: run belongs to another agent")
	// ErrRunNotFinished means the run has not settled yet.
	ErrRunNotFinished = errors.New("agent: run has not finished")
	// ErrRunPaused means the run ended by pausing, so it has no terminal state.
	ErrRunPaused = errors.New("agent: run is paused, not terminal")
	// ErrNoCommittedBoundary means the run made no durable commit of its own —
	// it failed before the first one, or its runnable never checkpointed. An
	// inherited head is not a boundary, so nothing is recorded and no earlier
	// run's terminal record is rewritten.
	ErrNoCommittedBoundary = errors.New("agent: run has no committed checkpoint boundary of its own")
)

// terminalOutcome maps the runnable's outcome and the claim cleanup result onto
// the run's terminal state: result, error, and whether the run paused. A failed
// claim release is a failure even when the run itself completed or paused, so
// every branch reports it.
func terminalOutcome(out runOutcome, cleanupErr error) (core.Result, error, bool) {
	switch {
	case out.Err != nil:
		return core.Result{}, errors.Join(out.Err, cleanupErr), false
	case out.Control.Kind == core.Interrupt:
		if cleanupErr != nil {
			return core.Result{}, errors.Join(cleanupErr), false
		}
		return core.Result{}, nil, true
	default:
		if cleanupErr != nil {
			return core.Result{}, errors.Join(cleanupErr), false
		}
		return out.Result, nil, false
	}
}

// RecordCompletion writes this run's terminal record onto the boundary the run
// last committed. It neither starts nor waits for the run: call it after the run
// has settled, and only accepts a run this agent created that ended completed or
// failed. A paused run, a run that never committed anything in this run, or a
// store without atomic revision-checked commits is refused with a matching error
// and writes nothing.
//
// The record is appended with the store's compare-and-swap: the revision frozen
// at the end of the run is the expected head, so a run that finished a while ago
// cannot overwrite the state of a newer one. Calling it twice for the same run
// reuses the same checkpoint ID and content, so the retry confirms the first
// commit instead of appending a duplicate. Committing the record still moves the
// thread's head forward; that is the point of an append-only terminal record.
func (a *Agent) RecordCompletion(ctx context.Context, run *Run) error {
	if run == nil {
		return ErrNoRun
	}
	if run.agent != a {
		return ErrRunNotOwned
	}
	select {
	case <-run.done:
	default:
		return ErrRunNotFinished
	}

	run.terminalMu.Lock()
	frozen, paused := run.terminalFrozen, run.terminalPaused
	res, runErr := run.terminalResult, run.terminalErr
	boundaryID, boundaryRevision, boundaryOK := run.boundaryID, run.boundaryRev, run.boundaryOK
	run.terminalMu.Unlock()

	if !frozen {
		return ErrRunNotFinished
	}
	if paused {
		return ErrRunPaused
	}
	// Capability first: a store that only implements Checkpointer can neither
	// report a revision nor commit with one, so it is refused rather than
	// silently downgraded to Save.
	durable, ok := a.store.(checkpoint.Durable)
	if !ok {
		return fmt.Errorf("agent: record completion: %w", checkpoint.ErrDurableUnavailable)
	}
	if !boundaryOK {
		return ErrNoCommittedBoundary
	}

	completion := &checkpoint.Completion{RunID: run.ID, Kind: checkpoint.CompletionCompleted}
	if runErr != nil {
		completion.Kind = checkpoint.CompletionFailed
		completion.Err = runErr.Error()
	} else {
		message, err := core.CloneMessage(res.Message)
		if err != nil {
			return fmt.Errorf("agent: record completion: clone result: %w", err)
		}
		completion.Result = &core.Result{Message: message}
	}

	// The record is built once per run and reused verbatim, so concurrent or
	// repeated calls cannot append two terminals for the same run.
	run.recordMu.Lock()
	defer run.recordMu.Unlock()
	record := run.recorded
	if record == nil {
		if run.terminalID == "" {
			run.terminalID = core.NewID("completion")
		}
		built, err := buildTerminalRecord(ctx, durable, run.ThreadID, run.terminalID, boundaryID, boundaryRevision, completion, run.rc.runTurnID())
		if err != nil {
			return err
		}
		run.recorded = built
		record = built
	}

	expected := boundaryRevision
	if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
		Checkpoint:       record,
		ExpectedRevision: &expected,
	}); err != nil {
		return fmt.Errorf("agent: record completion: commit at revision %d: %w", boundaryRevision, err)
	}
	return nil
}

// buildTerminalRecord reads the frozen boundary back and copies it into an
// append-only record: same parent, same step, the boundary's full state and file
// snapshot, no pause or pending work, and this run's completion.
//
// The record keeps the identity of the run it terminates — that run's ID and turn
// — and its parent stays the boundary this run committed, read from the frozen
// pair rather than from whatever the thread's head has become since.
func buildTerminalRecord(
	ctx context.Context,
	durable checkpoint.Durable,
	threadID, recordID, boundaryID string,
	boundaryRevision uint64,
	completion *checkpoint.Completion,
	turnID string,
) (*checkpoint.Checkpoint, error) {
	base, err := durable.Load(ctx, threadID, boundaryID)
	if err != nil {
		return nil, fmt.Errorf("agent: record completion: load boundary %s: %w", boundaryID, err)
	}
	if base == nil || base.ID != boundaryID || base.ThreadID != threadID || base.Revision != boundaryRevision {
		return nil, fmt.Errorf("agent: record completion: boundary %s does not match the committed state (want revision %d)", boundaryID, boundaryRevision)
	}
	if base.Pause != nil || base.Pending != nil {
		return nil, fmt.Errorf("agent: record completion: boundary %s is a pause point, not a committed step", boundaryID)
	}
	state, err := core.CloneStateData(base.State)
	if err != nil {
		return nil, fmt.Errorf("agent: record completion: clone boundary state: %w", err)
	}
	if turnID == "" {
		turnID = base.TurnID // a boundary committed before run identity keeps its own record empty
	}
	return &checkpoint.Checkpoint{
		ID:        recordID,
		ThreadID:  threadID,
		ParentID:  boundaryID,
		ParentRef: &checkpoint.Ref{ThreadID: threadID, CheckpointID: boundaryID},
		RunID:     completion.RunID,
		TurnID:    turnID,
		CreatedAt: lineageNowUTC(),
		Step:      base.Step,
		State:     state,

		FileSnapshot: cloneRunFileSnapshot(base.FileSnapshot),
		Completion:   completion,
	}, nil
}

// cloneRunFileSnapshot copies a file snapshot whole. The distinction between no
// snapshot at all (nil) and an explicitly empty file view is part of the record.
func cloneRunFileSnapshot(in map[string][]byte) map[string][]byte {
	if in == nil {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for path, data := range in {
		if data == nil {
			out[path] = nil
			continue
		}
		copied := make([]byte, len(data))
		copy(copied, data)
		out[path] = copied
	}
	return out
}
