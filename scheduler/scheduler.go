// Package scheduler is goagent's background execution layer: a job queue plus a
// bounded worker pool that runs work off the caller's goroutine. Use it to
// fire-and-forget agent runs (or any work) and process many of them N-at-a-time.
//
// Two backends share one Queue/Consumer interface:
//   - MemQueue: in-process (channel), for jobs carrying a Run closure.
//   - Redis Streams (New(WithRedis(url))): durable, cross-process, at-least-once
//     delivery with retry + dead-letter. A closure cannot cross a process, so
//     Redis jobs carry Type+Payload and the worker Pool rebuilds them from a
//     Registry of Handlers.
package scheduler

import (
	"context"
	"fmt"
	"sync"
)

// Job is one unit of background work. It carries its work in one of two forms:
//   - In-process (MemQueue): set Run, a closure the worker calls directly.
//   - Cross-process (Redis): set Type and Payload; the worker looks Type up in
//     its Registry and runs the Handler. Run must be nil for these.
type Job struct {
	// ID identifies the job (for correlation/logging).
	ID string
	// Key is an opaque routing label (e.g. "app/user/session"); optional.
	Key string
	// Run performs the work in-process. Never serialized; nil for Redis jobs.
	Run func(ctx context.Context) error
	// Type names a Handler in the worker's Registry (cross-process). Empty for
	// Run-based jobs.
	Type string
	// Payload is the serialized argument blob for the Type's Handler.
	Payload []byte
}

// Handler reconstructs and performs the work for a Job.Type from its Payload —
// the cross-process counterpart of Job.Run.
type Handler func(ctx context.Context, payload []byte) error

// Registry maps a Job.Type to its Handler.
type Registry map[string]Handler

// Queue is the producer side: Enqueue hands a job off without blocking on it
// running.
type Queue interface {
	Enqueue(ctx context.Context, job Job) error
}

// Consumer is the worker-facing side. Dequeue blocks until a job is available,
// ctx is done, or the queue is closed; it returns the job plus an ack callback
// the worker invokes once the job has settled (a no-op for in-process backends,
// an XACK for Redis). ok is false (nil error) when the queue is closed and
// drained.
type Consumer interface {
	Dequeue(ctx context.Context) (job Job, ack func() error, ok bool, err error)
}

// MemQueue is an in-process, channel-backed Queue and Consumer.
type MemQueue struct {
	ch     chan Job
	closed bool
	mu     sync.Mutex
}

// NewMemQueue creates a queue buffering up to size pending jobs (min 1).
func NewMemQueue(size int) *MemQueue {
	if size < 1 {
		size = 1
	}
	return &MemQueue{ch: make(chan Job, size)}
}

// Enqueue implements Queue. It blocks (until ctx is done) when full.
func (q *MemQueue) Enqueue(ctx context.Context, job Job) error {
	select {
	case q.ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Dequeue implements Consumer. The ack is a no-op (nothing to confirm in-process).
func (q *MemQueue) Dequeue(ctx context.Context) (Job, func() error, bool, error) {
	select {
	case job, ok := <-q.ch:
		if !ok {
			return Job{}, nil, false, nil
		}
		return job, func() error { return nil }, true, nil
	case <-ctx.Done():
		return Job{}, nil, false, ctx.Err()
	}
}

// Close stops accepting jobs; a Pool draining it returns once the buffer empties.
func (q *MemQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}

var (
	_ Queue    = (*MemQueue)(nil)
	_ Consumer = (*MemQueue)(nil)
)

// Pool is a bounded worker pool draining a Consumer.
type Pool struct {
	c           Consumer
	concurrency int
	registry    Registry
}

// NewPool builds a worker pool running up to concurrency jobs at once (min 1).
func NewPool(c Consumer, concurrency int) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Pool{c: c, concurrency: concurrency}
}

// WithRegistry supplies the Type -> Handler map used to run serialized (Redis)
// jobs. Unused for in-process Run jobs. Returns the pool for chaining.
func (p *Pool) WithRegistry(reg Registry) *Pool {
	p.registry = reg
	return p
}

// Run drains the queue, running jobs concurrently up to the pool's limit. It
// acks a job only on success, so a failed Redis job is redelivered. It blocks
// until ctx is cancelled or the queue is closed and drained, then waits for
// in-flight jobs.
func (p *Pool) Run(ctx context.Context) {
	sem := make(chan struct{}, p.concurrency)
	var wg sync.WaitGroup
	for {
		// Acquire a slot before dequeuing, so in-flight work never exceeds the limit.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		job, ack, ok, err := p.c.Dequeue(ctx)
		if err != nil || !ok {
			<-sem
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(job Job, ack func() error) {
			defer wg.Done()
			defer func() { <-sem }()
			if execErr := p.exec(ctx, job); execErr == nil && ack != nil {
				_ = ack()
			}
		}(job, ack)
	}
}

func (p *Pool) exec(ctx context.Context, job Job) error {
	if job.Run != nil {
		return job.Run(ctx)
	}
	if h := p.registry[job.Type]; h != nil {
		return h(ctx, job.Payload)
	}
	return fmt.Errorf("scheduler: no handler for job type %q", job.Type)
}
