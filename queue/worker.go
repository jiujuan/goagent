package queue

import (
	"context"
	"fmt"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// Worker runs queued jobs in the background. For each job it invokes the work
// (Job.Run for in-process jobs, or the Registry Handler named by Job.Type for
// serialized ones), publishing every emitted progress event (as transient/
// Partial) and the returned final event (non-Partial) to the Bus. It does not
// persist anything — that is the job's own concern — which keeps the worker free
// of any session or model dependency.
//
// Acknowledgement policy (matters only for at-least-once backends like Redis,
// where a non-acked job is later reclaimed and retried):
//
//   - success: ack — the job is done.
//   - business failure (the work returns an error): publish a failed event and
//     ack. The work ran to completion; retrying a deterministic failure would
//     only burn a redelivery, so we do not.
//   - infrastructure failure (the worker panics or the process dies before ack):
//     do not ack. The job stays pending and is reclaimed/retried by the backend,
//     bounded by its max-deliveries poison cap.
type Worker struct {
	consumer Consumer
	bus      Bus
	registry Registry
	max      int
}

// Config configures a Worker.
type Config struct {
	// Consumer is the job source (a *MemQueue or a Redis stream queue).
	Consumer Consumer
	Bus      Bus
	// Registry resolves Job.Type to a Handler for serialized (Redis) jobs. It may
	// be nil when every job carries a Run closure (in-process only).
	Registry Registry
	// MaxConcurrent bounds how many jobs run at once (default 4).
	MaxConcurrent int
}

// NewWorker builds a Worker.
func NewWorker(cfg Config) *Worker {
	max := cfg.MaxConcurrent
	if max < 1 {
		max = 4
	}
	return &Worker{consumer: cfg.Consumer, bus: cfg.Bus, registry: cfg.Registry, max: max}
}

// Run drains the consumer until it is closed or ctx is cancelled, dispatching
// each job to a bounded pool of goroutines. It blocks; call it in its own
// goroutine.
func (w *Worker) Run(ctx context.Context) {
	sem := make(chan struct{}, w.max)
	var wg sync.WaitGroup
	for {
		job, ack, ok, err := w.consumer.Dequeue(ctx)
		if err != nil || !ok {
			wg.Wait()
			return
		}
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Shutting down before this job could start. Leave it unacked so an
			// at-least-once backend reclaims it; in-process ack is a no-op.
			wg.Done()
			wg.Wait()
			return
		}
		go func(job Job, ack func() error) {
			defer wg.Done()
			defer func() { <-sem }()
			w.runJob(ctx, job, ack)
		}(job, ack)
	}
}

func (w *Worker) runJob(ctx context.Context, job Job, ack func() error) {
	run, err := w.resolve(job)
	if err != nil {
		// Unknown job type is a deterministic failure: ack so it is not retried.
		w.publishFailed(job, err.Error())
		_ = ack()
		return
	}
	if run == nil {
		_ = ack() // nothing to do
		return
	}

	emit := func(ev *core.Event) {
		if ev == nil {
			return
		}
		ev.Partial = true
		if ev.InvocationID == "" {
			ev.InvocationID = job.ID
		}
		if ev.Progress != nil && ev.Progress.JobID == "" {
			ev.Progress.JobID = job.ID
		}
		w.bus.Publish(job.Key, ev)
	}

	// A panic is an infrastructure failure: report it but do NOT ack, so an
	// at-least-once backend reclaims and retries the job (its poison cap stops a
	// crash loop). This also keeps one bad job from taking down the worker.
	defer func() {
		if r := recover(); r != nil {
			w.publishFailed(job, fmt.Sprintf("panic: %v", r))
		}
	}()

	final, err := run(ctx, emit)
	if err != nil {
		w.publishFailed(job, err.Error())
		_ = ack() // business failure: deterministic, don't retry
		return
	}
	if final != nil {
		final.Partial = false
		if final.InvocationID == "" {
			final.InvocationID = job.ID
		}
		if final.Progress != nil && final.Progress.JobID == "" {
			final.Progress.JobID = job.ID
		}
		w.bus.Publish(job.Key, final)
	}
	_ = ack()
}

// resolve picks the work for a job: its Run closure, or the Registry Handler
// named by its Type. It returns an error only when a Type names no handler.
func (w *Worker) resolve(job Job) (func(context.Context, func(*core.Event)) (*core.Event, error), error) {
	if job.Run != nil {
		return job.Run, nil
	}
	if job.Type == "" {
		return nil, nil
	}
	h := w.registry[job.Type]
	if h == nil {
		return nil, fmt.Errorf("queue: no handler registered for job type %q", job.Type)
	}
	payload := job.Payload
	return func(ctx context.Context, emit func(*core.Event)) (*core.Event, error) {
		return h(ctx, payload, emit)
	}, nil
}

func (w *Worker) publishFailed(job Job, msg string) {
	w.bus.Publish(job.Key, &core.Event{
		InvocationID: job.ID,
		Partial:      true,
		Progress:     &core.Progress{JobID: job.ID, Status: "failed", Err: msg},
	})
}
