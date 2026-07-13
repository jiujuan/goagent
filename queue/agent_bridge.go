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
type Handle struct {
	ID  string
	run *agent.Run
}

// Wait blocks until the background run settles and returns its result.
func (h *Handle) Wait() (core.Result, error) { return h.run.Wait() }

// Events subscribes to the run's live events (e.g. bus.Lossy for a UI).
func (h *Handle) Events(mode bus.DeliveryMode) (<-chan core.Event, func()) {
	return h.run.Events(mode)
}

// Cancel aborts the background run.
func (h *Handle) Cancel() { h.run.Cancel() }

// EnqueueAgent submits an agent run as a background job and returns immediately
// with a Handle. The run is created lazily (not driven until a worker picks the
// job up), so nothing executes on the caller's goroutine. Watch progress via
// Handle.Events / Handle.Wait or the agent's own bus.
func EnqueueAgent(ctx context.Context, q Queue, a *agent.Agent, input string, opts ...agent.RunOption) (*Handle, error) {
	run := a.Stream(ctx, input, opts...) // lazy: not driven yet
	id := core.NewID("job")
	job := Job{
		ID: id,
		Run: func(context.Context) error {
			_, err := run.Wait() // drives the run to completion
			return err
		},
	}
	if err := q.Enqueue(ctx, job); err != nil {
		return nil, err
	}
	return &Handle{ID: id, run: run}, nil
}
