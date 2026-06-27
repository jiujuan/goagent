// Package scheduler is goagent's background execution layer: a job queue plus a
// bounded worker pool that runs work off the caller's goroutine. Use it to
// fire-and-forget agent runs (or any work) and process many of them N-at-a-time.
//
// It is in-memory (MemQueue); the Queue/Consumer split leaves room for a durable
// backend (Redis/DB) behind the same interfaces later. Progress is observed
// through the agent's own bus (see EnqueueAgent), so scheduler needs no bus of
// its own.
package scheduler

import (
	"context"
	"sync"
)

// Job is one unit of background work. Run performs it and returns its terminal
// error (nil on success). It is opaque to the worker — a closure that may
// capture an agent, stores, anything.
type Job struct {
	ID  string
	Run func(ctx context.Context) error
}

// Queue is the producer side: Enqueue hands a job off without blocking on it
// running.
type Queue interface {
	Enqueue(ctx context.Context, job Job) error
}

// Consumer is the worker-facing side: a source of jobs. Kept separate from Queue
// so a Pool can drain an in-process MemQueue or (later) a durable backend.
type Consumer interface {
	// Jobs returns the receive channel of pending jobs; it is closed when the
	// queue is closed and drained.
	Jobs() <-chan Job
}

// MemQueue is an in-process, channel-backed Queue.
type MemQueue struct {
	ch     chan Job
	closed bool
	mu     sync.Mutex
}

// NewMemQueue creates a queue buffering up to size pending jobs (min 1).
// Enqueue blocks (until ctx is done) when the buffer is full — natural
// back-pressure.
func NewMemQueue(size int) *MemQueue {
	if size < 1 {
		size = 1
	}
	return &MemQueue{ch: make(chan Job, size)}
}

// Enqueue implements Queue.
func (q *MemQueue) Enqueue(ctx context.Context, job Job) error {
	select {
	case q.ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Jobs implements Consumer.
func (q *MemQueue) Jobs() <-chan Job { return q.ch }

// Close stops accepting jobs; a Pool draining it returns once the buffer empties.
func (q *MemQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}

// Pool is a bounded worker pool draining a Consumer.
type Pool struct {
	c           Consumer
	concurrency int
}

// NewPool builds a worker pool that runs up to concurrency jobs at once
// (min 1).
func NewPool(c Consumer, concurrency int) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Pool{c: c, concurrency: concurrency}
}

// Run drains the queue, running jobs concurrently up to the pool's limit. It
// blocks until ctx is cancelled or the queue is closed and drained, then waits
// for in-flight jobs before returning.
func (p *Pool) Run(ctx context.Context) {
	sem := make(chan struct{}, p.concurrency)
	var wg sync.WaitGroup
	jobs := p.c.Jobs()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case job, ok := <-jobs:
			if !ok {
				wg.Wait()
				return
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return
			}
			wg.Add(1)
			go func(job Job) {
				defer wg.Done()
				defer func() { <-sem }()
				if job.Run != nil {
					_ = job.Run(ctx)
				}
			}(job)
		}
	}
}
