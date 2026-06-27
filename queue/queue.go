// Package queue is goagent's background execution layer: a job queue plus a
// bounded worker pool that runs work off the caller's goroutine. Use it to
// fire-and-forget agent runs (or any work) and process many of them N-at-a-time.
//
// Two backends share one Queue/Consumer interface:
//   - MemQueue: in-process (channel), for jobs carrying a Run closure.
//   - Redis Streams (New(WithRedis(url))): durable, cross-process, at-least-once
//     delivery with retry + dead-letter. A closure cannot cross a process, so
//     Redis jobs carry Type+Payload and the worker Pool rebuilds them from a
//     Registry of Handlers.
package queue

import (
	"context"
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
