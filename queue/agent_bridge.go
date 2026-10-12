package queue

import (
	"context"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

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

	// cancelWork releases the queue-owned execution context. Enqueue failure,
	// Cancel, and the worker closure (defer) all call it; it is idempotent.
	cancelWork context.CancelFunc
	// workDone observes that same context so the package's own tests can check
	// release on every settlement path.
	workDone <-chan struct{}
}

// Wait blocks until the background run settles and returns its result.
func (h *Handle) Wait() (core.Result, error) { return h.run.Wait() }

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
		cancelWork: cancelWork,
		workDone:   workCtx.Done(),
	}
	job := Job{
		ID: h.ID,
		Run: func(context.Context) error {
			// The worker's ctx governs job fetching, not this run; the run settles
			// on workCtx. Releasing it here covers every closure exit, including a
			// run that ended in error or pause.
			defer h.cancelWork()
			_, err := h.run.Wait()
			return err
		},
	}
	if err := q.Enqueue(ctx, job); err != nil {
		h.cancelWork()
		return h, err
	}
	return h, nil
}
