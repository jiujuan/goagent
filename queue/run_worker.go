package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/runstore"
)

// managedRunLease is how long a worker's ownership of a task record is meant to last. It is
// recorded, not enforced here: passing it makes a record stale for a human to recover, and
// only a heartbeat loop renews it (ADR-0034 D6, TASK-SM-09).
const managedRunLease = 30 * time.Second

// defaultCancelPollInterval is how often a running worker looks at its own task record for a
// cancellation request. A request is written by somebody else's RunClient, so looking is the
// only way a run can honour it.
const defaultCancelPollInterval = 250 * time.Millisecond

// ManagedRunWorker executes one recorded run from a queue delivery: it claims the task
// record, runs the agent under the identity that record carries, then publishes the frozen
// outcome back to it (ADR-0034 D4, D5).
//
// Claiming is a compare-and-swap, so redelivery of a run that is already running or settled
// cannot start a second execution — the model and the tools are not called again. The task
// record and the checkpoint history stay two separate durable facts: this worker writes the
// checkpoint's terminal record with Agent.RecordCompletion and the task record with the
// store, and reports each of their failures on its own.
type ManagedRunWorker struct {
	agent         *agent.Agent
	store         runstore.Store
	workerID      string
	lease         time.Duration
	recordTimeout time.Duration
	cancelPoll    time.Duration
}

// WorkerOption tunes a ManagedRunWorker.
type WorkerOption func(*ManagedRunWorker)

// WithWorkerLease sets how long a claim is meant to last. It must be positive.
func WithWorkerLease(d time.Duration) WorkerOption {
	return func(w *ManagedRunWorker) { w.lease = d }
}

// WithCancelPollInterval sets how often a running worker checks its task record for a
// cancellation request. It must be positive.
func WithCancelPollInterval(d time.Duration) WorkerOption {
	return func(w *ManagedRunWorker) { w.cancelPoll = d }
}

// WithManagedRecordTimeout bounds each recording phase: the terminal checkpoint write
// and the task-store publication. It must be positive.
func WithManagedRecordTimeout(d time.Duration) WorkerOption {
	return func(w *ManagedRunWorker) { w.recordTimeout = d }
}

// NewManagedRunWorker binds one worker to an agent and a task store. workerID names this
// worker in every record it claims, so a later reader can tell who wrote what.
func NewManagedRunWorker(a *agent.Agent, store runstore.Store, workerID string, opts ...WorkerOption) (*ManagedRunWorker, error) {
	if a == nil {
		return nil, fmt.Errorf("queue: managed run worker needs an agent")
	}
	if store == nil {
		return nil, fmt.Errorf("queue: managed run worker needs a run store")
	}
	if workerID == "" {
		return nil, fmt.Errorf("queue: managed run worker needs a worker id")
	}
	w := &ManagedRunWorker{
		agent:         a,
		store:         store,
		workerID:      workerID,
		lease:         managedRunLease,
		recordTimeout: terminalRecordTimeout,
		cancelPoll:    defaultCancelPollInterval,
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.lease <= 0 {
		return nil, fmt.Errorf("queue: managed run worker lease must be positive, got %v", w.lease)
	}
	if w.recordTimeout <= 0 {
		return nil, fmt.Errorf("queue: managed run record timeout must be positive, got %v", w.recordTimeout)
	}
	if w.cancelPoll <= 0 {
		return nil, fmt.Errorf("queue: managed run cancel poll interval must be positive, got %v", w.cancelPoll)
	}
	return w, nil
}

// ManagedRunOutcome reports what one delivery of a run did. The three flags are exclusive: a
// delivery either executed the run, found it already claimed or settled, or found the queued
// record cancelled before anything started.
type ManagedRunOutcome struct {
	// Record is the last task record read or successfully written by this delivery.
	// If a later read fails it may be stale; RecordError reports that failure.
	Record runstore.RunRecord
	// Result is the frozen execution result, even if task-store publication failed.
	// It is nil for deliveries that did not execute.
	Result *agent.RunResult
	// RunError is the execution error, independent of both recording errors.
	RunError error
	// CompletionRef identifies a successfully committed terminal checkpoint.
	CompletionRef *checkpoint.Ref
	// CompletionError is the checkpoint terminal-record error.
	CompletionError error
	// Executed means this delivery claimed the record and ran the model and tools.
	Executed bool
	// Duplicate means another delivery already owns or has settled this run, so nothing
	// executed again. At-least-once queues redeliver; this is how that stays harmless.
	Duplicate bool
	// CancelledBeforeStart means a cancellation was persisted while the run was still
	// queued, so the run ends here and never starts.
	CancelledBeforeStart bool
	// RecordError is the task-store write error, separate from the run's own failure and from
	// the checkpoint terminal record's failure. A non-nil value means the published outcome is
	// not durable, so nobody may treat the run as bookkept.
	RecordError error
}

// Execute claims and runs the recorded run identified by runID, then publishes its frozen
// outcome. input is the user text for this turn: the task record carries identity, not the
// prompt, so whoever resolved the delivery supplies it (see ManagedRunPayload).
//
// The returned error is the run's own failure, joined with the checkpoint terminal record's
// failure when a run that reached an outcome could not be recorded. A task-store write
// failure never hides behind a nil error: it comes back in outcome.RecordError and is joined
// into the returned error.
//
// ctx drives execution. Cancelling it stops the run, and the record then says the run failed
// — or was cancelled, when a cancellation request was persisted first. A request persisted
// while the run is going is noticed by this worker's own polling and then stops the run, so
// CancelRun works without any caller holding a handle.
func (w *ManagedRunWorker) Execute(ctx context.Context, runID string, input string, opts ...agent.RunOption) (ManagedRunOutcome, error) {
	record, err := w.store.Get(ctx, runID)
	if err != nil {
		return ManagedRunOutcome{}, err
	}
	if decision, settled := deliveryDecision(record); settled {
		return decision, nil
	}

	claimed, err := w.claim(ctx, record)
	if errors.Is(err, runstore.ErrConflict) {
		// Another write landed between the read and the swap: a cancellation that got there
		// first ends this run, anything else means another delivery holds it.
		latest, getErr := w.store.Get(ctx, runID)
		if getErr != nil {
			return ManagedRunOutcome{}, errors.Join(err, getErr)
		}
		if decision, settled := deliveryDecision(latest); settled {
			return decision, nil
		}
		return ManagedRunOutcome{Record: latest, Duplicate: true}, nil
	}
	if err != nil {
		return ManagedRunOutcome{}, err
	}

	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()
	stopWatching := make(chan struct{})
	defer close(stopWatching)
	go w.watchCancellation(execCtx, claimed.RunID, stopWatching, cancelExec)

	run, detail, runErr := w.drive(execCtx, claimed, input, opts...)
	completionRef, completionErr := w.recordCheckpoint(ctx, run, detail)
	return w.publish(ctx, claimed, detail, runErr, completionRef, completionErr)
}

// deliveryDecision reports what a delivery should do with a record it has just read. Only a
// queued record is its own to run; a cancelled one ended before anything started, and any
// other non-queued state belongs to a delivery that got there first.
func deliveryDecision(record runstore.RunRecord) (ManagedRunOutcome, bool) {
	if record.Status == runstore.StatusQueued {
		return ManagedRunOutcome{}, false
	}
	if record.Status == runstore.StatusCancelled {
		return ManagedRunOutcome{Record: record, CancelledBeforeStart: true}, true
	}
	return ManagedRunOutcome{Record: record, Duplicate: true}, true
}

// watchCancellation looks at this run's task record while the run is executing and cancels the
// execution context when it finds a persisted cancellation request. The request is written by
// somebody else's CancelRun, so a run that never looks cannot honour it (ADR-0034 D4).
//
// A read that fails is ignored: a store outage is not a cancellation, and the next tick tries
// again. Once the record has ended the watch stops — there is nothing left to cancel here.
//
// This loop neither renews the claim lease nor writes heartbeats; those stay separate duties
// (ADR-0034 D6, TASK-SM-09).
func (w *ManagedRunWorker) watchCancellation(ctx context.Context, runID string, stop <-chan struct{}, cancel context.CancelFunc) {
	ticker := time.NewTicker(w.cancelPoll)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		record, err := w.store.Get(ctx, runID)
		if err != nil {
			continue
		}
		if record.Status.Terminal() {
			return
		}
		if !record.CancelRequestedAt.IsZero() || record.Status == runstore.StatusCancelling {
			cancel()
			return
		}
	}
}

// claim moves a queued record to running under this worker's identity. The epoch advances
// from whatever the record last carried, so a run that comes back for another attempt is
// fenced with a new token instead of reusing an old one.
func (w *ManagedRunWorker) claim(ctx context.Context, record runstore.RunRecord) (runstore.RunRecord, error) {
	next := record
	next.Status = runstore.StatusRunning
	next.WorkerID = w.workerID
	next.WorkerEpoch = record.WorkerEpoch + 1
	next.WorkerLeaseUntil = time.Now().UTC().Add(w.lease)
	claimed, err := w.store.CompareAndSwap(ctx, record.RunID, record.Version, next)
	if err != nil {
		return runstore.RunRecord{}, err
	}
	return claimed, nil
}

// drive runs the agent with the identity the claimed record carries and returns the handle
// plus what the run froze when it settled.
func (w *ManagedRunWorker) drive(ctx context.Context, claimed runstore.RunRecord, input string, opts ...agent.RunOption) (*agent.Run, agent.RunResult, error) {
	spec := agent.ManagedRunSpec{
		RunID:    claimed.RunID,
		ThreadID: claimed.ThreadID,
		TurnID:   claimed.TurnID,
	}
	run, err := w.agent.StreamManaged(ctx, spec, input, opts...)
	if err != nil {
		return nil, agent.RunResult{ThreadID: spec.ThreadID, RunID: spec.RunID, TurnID: spec.TurnID}, err
	}
	_, runErr := run.Wait()
	return run, run.DetailedResult(), runErr
}

// recordCheckpoint appends this run's terminal checkpoint record on a context that keeps the
// run's values but not its cancellation: cancelling execution must not cancel the record that
// says the run was cancelled (ADR-0033 D2).
//
// A paused run has nothing terminal to record; the same holds for a run that never got a
// handle because the record's identity is one the agent refuses.
// A run that committed nothing of its own is refused by RecordCompletion with
// ErrNoCommittedBoundary, which is not a write failure — the missing boundary is the truth,
// and no checkpoint is invented for it.
func (w *ManagedRunWorker) recordCheckpoint(ctx context.Context, run *agent.Run, detail agent.RunResult) (*checkpoint.Ref, error) {
	if run == nil || detail.Paused {
		return nil, nil
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.recordTimeout)
	defer cancel()
	if err := w.agent.RecordCompletion(recordCtx, run); err != nil {
		if errors.Is(err, agent.ErrNoCommittedBoundary) {
			return nil, nil
		}
		return nil, err
	}
	ref, ok := run.RecordedBoundary()
	if !ok {
		return nil, fmt.Errorf("queue: run %q recorded its terminal checkpoint without reporting its boundary", detail.RunID)
	}
	return ref, nil
}

// publish writes the task record's terminal state. Execution outcome, checkpoint record error
// and task-store write error stay three separate facts (ADR-0034 D5).
//
// The record is re-read before it is written: a cancellation request can be persisted under a
// running record, which moves its version, and publishing from the claim's version would then
// be refused rather than report what happened.
func (w *ManagedRunWorker) publish(ctx context.Context, claimed runstore.RunRecord, detail agent.RunResult, runErr error, completionRef *checkpoint.Ref, completionErr error) (ManagedRunOutcome, error) {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.recordTimeout)
	defer cancel()
	out := ManagedRunOutcome{
		Record: claimed, Executed: true, Result: &detail, RunError: runErr,
		CompletionRef: cloneRef(completionRef), CompletionError: completionErr,
	}
	joined := errors.Join(runErr, completionErr)
	current, err := w.store.Get(recordCtx, claimed.RunID)
	if err != nil {
		out.RecordError = err
		return out, errors.Join(joined, err)
	}
	out.Record = current
	next := terminalTaskRecord(current, detail, runErr, completionRef, completionErr)
	published, writeErr := w.store.CompareAndSwap(recordCtx, current.RunID, current.Version, next)
	if writeErr == nil {
		out.Record = published
		return out, joined
	}
	// Keep the frozen result separate from the last known persisted state. A failed
	// read must not erase either the execution evidence or the record's identity.
	latest, getErr := w.store.Get(recordCtx, claimed.RunID)
	out.RecordError = errors.Join(writeErr, getErr)
	if getErr == nil {
		out.Record = latest
	}
	return out, errors.Join(joined, out.RecordError)
}

// terminalTaskRecord turns a frozen run outcome into the task record's terminal state. stored
// is the record as the store holds it now, so a cancellation request persisted during
// execution is part of what gets published.
func terminalTaskRecord(stored runstore.RunRecord, detail agent.RunResult, runErr error, completionRef *checkpoint.Ref, completionErr error) runstore.RunRecord {
	next := stored
	next.LatestCheckpointRef = cloneRef(detail.CheckpointRef)
	next.CheckpointRevision = detail.Revision
	next.CompletionRef = cloneRef(completionRef)
	if completionErr != nil {
		next.RecordError = &runstore.RunError{
			Code:      runstore.ErrCodeCompletionRecord,
			Message:   completionErr.Error(),
			Retryable: true,
		}
	} else {
		next.RecordError = nil
	}

	switch {
	case detail.Paused:
		// A pause ends this run; continuing it creates a new record whose ResumeOfRunID names
		// this one, and no terminal checkpoint record exists for a pause.
		next.Status = runstore.StatusPaused
		next.Error = nil
		next.FinishReason = runstore.FinishReasonPaused
		next.Result = cloneResult(detail.Result)
	case runErr != nil && honouredCancellation(stored, runErr):
		// The run stopped on the cancellation request the record already carries: that is
		// cancelled, not failure, and the request time stays on the record.
		next.Status = runstore.StatusCancelled
		next.FinishReason = runstore.FinishReasonCancelled
		next.Result = nil
		next.Error = nil
	case runErr != nil:
		next.Status = runstore.StatusFailed
		next.FinishReason = runstore.FinishReasonFailed
		next.Result = nil
		next.Error = &runstore.RunError{
			Code:      failureCode(runErr),
			Message:   runErr.Error(),
			Retryable: false,
		}
	default:
		// A cancellation request that arrives while the run is still executing does not
		// rewrite an outcome the run reached on its own (ADR-0034 D4).
		next.Status = runstore.StatusSucceeded
		next.Error = nil
		next.FinishReason = runstore.FinishReasonCompleted
		next.Result = cloneResult(detail.Result)
	}
	return next
}

// honouredCancellation reports a run that ended because it obeyed a persisted cancellation
// request. A request the run ignored is not honoured, and a deadline is a failure rather than
// a user cancellation.
func honouredCancellation(stored runstore.RunRecord, runErr error) bool {
	return stored.Status == runstore.StatusCancelling && errors.Is(runErr, context.Canceled)
}

// failureCode separates a deadline from an ordinary failure. A deadline is a failure and not
// a user cancellation (ADR-0034 D4).
func failureCode(runErr error) string {
	if errors.Is(runErr, context.DeadlineExceeded) {
		return runstore.ErrCodeDeadlineExceeded
	}
	return runstore.ErrCodeExecutionFailed
}

func cloneResult(res core.Result) *core.Result {
	cloned, err := core.CloneMessage(res.Message)
	if err != nil {
		return nil
	}
	return &core.Result{Message: cloned}
}

func cloneRef(ref *checkpoint.Ref) *checkpoint.Ref {
	if ref == nil {
		return nil
	}
	copied := *ref
	return &copied
}
