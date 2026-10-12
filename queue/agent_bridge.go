package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

// terminalRecordTimeout bounds the terminal write alone (ADR-0033 D2): it is a
// cooperative deadline for the record's store I/O, not a deadline for the run,
// and it cannot interrupt a store that ignores context.
const terminalRecordTimeout = 30 * time.Second

// Handle is the producer-side reference to a background agent run. The worker
// drives the run; the holder can stream its events (Events) or block for the
// result (Wait) whenever — both fan out from the agent's bus, so calling them is
// independent of the worker.
//
// The run executes on a context the queue owns, not the submitter's: canceling
// the context passed to EnqueueAgent only stops submission, and an already
// enqueued run keeps going. Handle.Cancel is the explicit way to stop execution.
type Handle struct {
	ID  string
	run *agent.Run

	// owner records the agent so the worker can commit this run's terminal record
	// after the run settles; RecordCompletion rejects a run from another agent.
	owner *agent.Agent
	// cancelWork releases the queue-owned execution context. Enqueue failure,
	// Cancel, and the worker closure (defer) all call it; it is idempotent.
	cancelWork context.CancelFunc
	// workDone observes that same context so the package's own tests can check
	// release on every settlement path.
	workDone <-chan struct{}

	// settle closes after the worker has finished the run AND attempted its
	// terminal record, so WaitRecorded never reports a run whose terminal state is
	// still in flight. The stored result is the run's frozen outcome; each waiter
	// gets its own deep copy.
	settle     chan struct{}
	settleOnce sync.Once
	settleMu   sync.Mutex
	settleRes  core.Result
	settleErr  error
}

// Wait blocks until the background run settles and returns its result. Success
// does not mean the terminal record has been written — that is WaitRecorded.
func (h *Handle) Wait() (core.Result, error) { return h.run.Wait() }

// WaitRecorded blocks until the worker has settled the run and attempted to
// commit its terminal record, then returns the run's result with
// errors.Join(runErr, recordErr).
//
// It waits on the worker and never starts the run itself: if no worker picks the
// job up, this blocks (cancelling the Handle does not settle an unconsumed job —
// see EnqueueAgent's lifecycle note). A run that paused for a human decision has
// no terminal state to record, so it returns its result and no record error.
//
// For a completed run a nil error means the terminal record was confirmed by the
// store. A failed run still returns its run error, and the record outcome is
// checked with errors.Is / errors.As. Each caller receives its own deep copy of
// the result.
func (h *Handle) WaitRecorded() (core.Result, error) {
	<-h.settle

	h.settleMu.Lock()
	res, err := h.settleRes, h.settleErr
	h.settleMu.Unlock()

	// Waiters must not share mutable parts: one caller editing the result cannot
	// reach another's copy.
	if cloned, cloneErr := core.CloneMessage(res.Message); cloneErr == nil {
		res = core.Result{Message: cloned}
	}
	return res, err
}

// Events subscribes to the run's live events (e.g. bus.Lossy for a UI).
func (h *Handle) Events(mode bus.DeliveryMode) (<-chan core.Event, func()) {
	return h.run.Events(mode)
}

// Cancel aborts the background run and releases the queue-owned execution
// context. The run's own context is a child of that context, so cancelling the
// run alone would leave the parent alive; both layers are cancelled here. It is
// idempotent.
func (h *Handle) Cancel() {
	h.run.Cancel()
	h.cancelWork()
}

// EnqueueAgent submits an agent run as a background job and returns immediately
// with a Handle. The run is created lazily (not driven until a worker picks the
// job up), so nothing executes on the caller's goroutine. Watch progress via
// Handle.Events / Handle.Wait or the agent's own bus.
//
// ctx owns submission only: it must not already be cancelled, and a cancel that
// arrives after Enqueue has accepted the job does not stop the run. A cancel
// racing the enqueue itself is decided by Queue.Enqueue's return value. Once the
// job is accepted, its resources stay with the queue until the worker settles it
// — a Pool whose context is cancelled stops dequeuing and waits for in-flight
// jobs, and a job that is never dequeued keeps its execution context until its
// Handle is cancelled.
//
// The caller must also run a worker: only the worker's closure ends the run and
// attempts the terminal record, so Handle.WaitRecorded waits on it and a job that
// is never picked up leaves that call blocked.
func EnqueueAgent(ctx context.Context, q Queue, a *agent.Agent, input string, opts ...agent.RunOption) (*Handle, error) {
	h, err := submitAgentRun(ctx, q, a, input, opts...)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// submitAgentRun is EnqueueAgent's body. It keeps the Handle on the failure path
// so this package can assert that the derived execution context is released when
// the queue rejects the job; exported callers only see (nil, err).
func submitAgentRun(ctx context.Context, q Queue, a *agent.Agent, input string, opts ...agent.RunOption) (*Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The run belongs to the queue from here on: WithoutCancel keeps ctx's values
	// but drops its cancellation, so a disconnecting submitter cannot abort work
	// that has already been handed over.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	h := &Handle{
		ID:         core.NewID("job"),
		run:        a.Stream(workCtx, input, opts...),
		owner:      a,
		cancelWork: cancelWork,
		workDone:   workCtx.Done(),
		settle:     make(chan struct{}),
	}
	job := Job{
		ID: h.ID,
		Run: func(context.Context) error {
			// The worker's ctx governs job fetching, not this run; the run settles
			// on workCtx. Releasing it here covers every closure exit, including a
			// run that ended in error or pause.
			defer h.cancelWork()

			// Wait only drives the run to its own settlement. The outcome reported
			// below comes from what drive froze, because Wait hands out the run's live
			// result object: a caller editing that — while the record is still being
			// written, or before this job was even picked up — must not change what
			// WaitRecorded returns, and it must keep agreeing with the terminal record.
			h.run.Wait()
			res, runErr := h.run.FrozenOutcome()
			err := errors.Join(runErr, h.commitTerminal(workCtx))
			// The settled outcome is stored and WaitRecorded unblocked before the
			// same joined error goes back to the Pool: the pool has no channel that
			// a holder of the Handle can read, so this is how a record failure stays
			// visible.
			h.recordSettled(res, err)
			return err
		},
	}
	if err := q.Enqueue(ctx, job); err != nil {
		h.cancelWork()
		return h, err
	}
	return h, nil
}

// commitTerminal asks the agent to append this run's terminal record. A paused
// run has no terminal state, so it is skipped here (RecordCompletion refuses a
// pause independently as well). The write runs on its own context: it inherits
// workCtx's values but not its cancellation, because cancelling execution must not
// cancel the record that says the run was cancelled (ADR-0033 D2).
func (h *Handle) commitTerminal(workCtx context.Context) error {
	if h.run.Paused() {
		return nil
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), terminalRecordTimeout)
	defer cancel()
	return h.owner.RecordCompletion(recordCtx, h.run)
}

// recordSettled freezes the worker's joined outcome and releases WaitRecorded.
// The result it stores is already a copy independent of the run (FrozenOutcome),
// and each waiter receives another. It runs once per Handle even if a consumer
// redelivers the job, so a later attempt cannot close the channel twice or
// replace the reported outcome.
func (h *Handle) recordSettled(res core.Result, err error) {
	h.settleOnce.Do(func() {
		h.settleMu.Lock()
		h.settleRes, h.settleErr = res, err
		h.settleMu.Unlock()
		close(h.settle)
	})
}
