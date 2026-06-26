package plan

import (
	"context"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/queue"
)

// Backend selects how the scheduler runs ready steps concurrently. The DAG and
// dependency logic are identical across backends; only the execution pool
// differs — so OnError/Retry/Timeout behave the same whichever you pick.
type Backend string

const (
	// BackendGoroutines (default) runs each ready step's executor inline on its
	// own goroutine, bounding concurrency with a semaphore. It is hand-rolled in
	// the style of agent.ParallelAgent (what users call the "errgroup" backend)
	// and pulls in no extra infrastructure.
	BackendGoroutines Backend = ""

	// BackendQueue runs step executors through a queue.Worker pool, reusing the
	// framework's background execution layer. Pick it when you want plan steps to
	// share the same bounded worker infrastructure as other background work, or
	// to build on queue's Queue/Bus abstractions. Step results never travel over
	// the (lossy) Bus — the scheduler collects them directly — so correctness and
	// resume are unaffected by the choice.
	BackendQueue Backend = "queue"
)

// execPool bounds how many step executors run at once. The scheduler owns the
// dependency graph and only ever submits a step whose dependencies are already
// satisfied, so the pool never holds a slot for a blocked step.
type execPool interface {
	// run executes fn under the pool's concurrency bound, blocking the caller
	// until fn returns. It returns false (without ever calling fn) when ctx is
	// canceled before fn can start — the scheduler then marks the step Blocked.
	run(ctx context.Context, fn func()) bool
	// close releases pool resources. The scheduler calls it once every step has
	// settled, so no work is in flight.
	close()
}

// newPool builds the execution pool for the chosen backend. steps is the plan's
// step count, used to size the queue backend's buffer so submission never blocks.
func newPool(backend Backend, maxConc, steps int) execPool {
	if backend == BackendQueue {
		return newQueuePool(maxConc, steps)
	}
	return newGoroutinePool(maxConc)
}

// --- goroutine backend (default) --------------------------------------------

// goroutinePool runs fn inline on the submitting goroutine, gated by a semaphore.
type goroutinePool struct{ sem chan struct{} }

func newGoroutinePool(maxConc int) *goroutinePool {
	return &goroutinePool{sem: make(chan struct{}, maxConc)}
}

func (p *goroutinePool) run(ctx context.Context, fn func()) bool {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	defer func() { <-p.sem }()
	fn()
	return true
}

func (p *goroutinePool) close() {}

// --- queue backend ----------------------------------------------------------

// queuePool runs fn on a queue.Worker pool. Each submission is a Job whose Run
// invokes fn; the pool waits for that Job to run (or be skipped because the plan
// was canceled while it queued) via per-call signal channels, never the Bus.
type queuePool struct {
	q      *queue.MemQueue
	cancel context.CancelFunc
	done   chan struct{}
}

func newQueuePool(maxConc, steps int) *queuePool {
	if steps < 1 {
		steps = 1
	}
	q := queue.NewMemQueue(steps) // buffer every step so Enqueue never blocks
	w := queue.NewWorker(queue.Config{Consumer: q, Bus: queue.NewMemBus(), MaxConcurrent: maxConc})
	wctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(wctx); close(done) }()
	return &queuePool{q: q, cancel: cancel, done: done}
}

func (p *queuePool) run(ctx context.Context, fn func()) bool {
	ran := make(chan struct{})
	skipped := make(chan struct{})
	job := queue.Job{
		ID:  core.NewID("job"),
		Key: "plan",
		Run: func(context.Context, func(*core.Event)) (*core.Event, error) {
			// The plan may have been canceled (PolicyFail abort) while this job
			// waited for a worker slot; if so, don't run — the step is Blocked.
			if ctx.Err() != nil {
				close(skipped)
				return nil, nil
			}
			fn()
			close(ran)
			return nil, nil
		},
	}
	if err := p.q.Enqueue(ctx, job); err != nil {
		return false // canceled before the job was accepted
	}
	select {
	case <-ran:
		return true
	case <-skipped:
		return false
	}
}

func (p *queuePool) close() {
	p.cancel() // stop the worker (the queue is already drained: all steps settled)
	<-p.done
}
