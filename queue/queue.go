// Package queue is a standalone, modality-agnostic background execution layer.
// It runs Jobs off the caller's goroutine and streams their events to a
// session-scoped Bus, so long-running work (e.g. image or video generation
// driven by an agent) does not block the current turn or request.
//
// The package depends only on core: a Job carries a Run closure that performs
// the work and emits core.Events; the worker only orchestrates (run, publish).
// It knows nothing about agents, models, or sessions — persistence and the
// meaning of a Job are the caller's concern (see runner.EnqueueAgent for the
// agent bridge). This keeps queue reusable for any background work and free of
// the rest of goagent's dependency graph.
package queue

import (
	"context"

	"github.com/jiujuan/goagent/core"
)

// Job is one unit of background work. It is intentionally opaque: Run holds the
// execution logic supplied by the caller, so the worker need not understand it.
//
// A Job carries its work in one of two forms, depending on the backend:
//
//   - In-process (MemQueue): set Run, a closure that may capture anything
//     (agents, stores, the surrounding request). The worker calls it directly.
//   - Cross-process (RedisStreamQueue): set Type and Payload. A closure cannot
//     be serialized onto a Redis stream, so the producer names a Handler by Type
//     and ships its arguments as a serialized Payload; the consuming worker looks
//     Type up in its Registry and rebuilds the work. Run must be nil for these.
type Job struct {
	// ID identifies the job; it is stamped onto every emitted progress event so
	// a subscriber can correlate updates with the final result.
	ID string

	// Key routes the job's events on the Bus. It is an opaque string; callers
	// that target a session typically use "app/user/session".
	Key string

	// Run performs the work. It calls emit for each intermediate progress event
	// and returns the final event to deliver (carrying the result in Message), or
	// an error. The worker marks emitted events Partial and publishes them; it
	// publishes the returned final event as non-Partial. Run owns any persistence
	// of its result.
	//
	// Run is for in-process backends only; it is never serialized. For Redis,
	// leave Run nil and use Type/Payload instead.
	Run func(ctx context.Context, emit func(*core.Event)) (*core.Event, error)

	// Type names a Handler in the worker's Registry. It is the serializable
	// stand-in for Run used by cross-process backends. Empty for Run-based jobs.
	Type string

	// Payload is the serialized argument blob passed to the Type's Handler. It is
	// opaque to the queue; the Handler owns its encoding (typically JSON).
	Payload []byte
}

// Handler is the cross-process counterpart of Job.Run: it reconstructs and
// performs the work for a Job.Type from its Payload. Its contract matches Run.
type Handler func(ctx context.Context, payload []byte, emit func(*core.Event)) (*core.Event, error)

// Registry maps a Job.Type to the Handler that performs it. A worker draining a
// serialized backend (Redis) is built with the Registry that knows how to run
// the job types its producers enqueue.
type Registry map[string]Handler

// Queue accepts jobs for background execution. Enqueue hands the job off; it
// must not block on the job running.
type Queue interface {
	Enqueue(ctx context.Context, job Job) error
}

// Consumer is the worker-facing side of a queue: a source of jobs to run. It is
// separate from Queue (the producer side) so the Worker can drain either an
// in-process MemQueue or a Redis stream without knowing which.
type Consumer interface {
	// Dequeue blocks until a job is available, ctx is done, or the queue is
	// closed. It returns the job and an ack callback the worker invokes once the
	// job has settled (so an at-least-once backend can mark it done); ok is false
	// with a nil error when the queue is closed and drained. For in-process
	// backends ack is a no-op.
	Dequeue(ctx context.Context) (job Job, ack func() error, ok bool, err error)
}

// MemQueue is an in-process, channel-backed Queue. Workers consume from Jobs().
type MemQueue struct {
	ch chan Job
}

// NewMemQueue creates a queue buffering up to size pending jobs. Enqueue blocks
// (until ctx is done) when the buffer is full, giving natural back-pressure.
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

// Jobs exposes the receive side for workers to range over.
func (q *MemQueue) Jobs() <-chan Job { return q.ch }

// Dequeue implements Consumer. It receives the next job from the channel; the
// returned ack is a no-op because an in-process queue has nothing to confirm.
// ok is false (with nil error) once the queue is closed and drained.
func (q *MemQueue) Dequeue(ctx context.Context) (Job, func() error, bool, error) {
	select {
	case job, ok := <-q.ch:
		if !ok {
			return Job{}, nil, false, nil
		}
		return job, noAck, true, nil
	case <-ctx.Done():
		return Job{}, nil, false, ctx.Err()
	}
}

// noAck is the ack callback for backends that have nothing to confirm.
func noAck() error { return nil }

// Close stops the queue; ranging workers exit once it is drained.
func (q *MemQueue) Close() { close(q.ch) }

var (
	_ Queue    = (*MemQueue)(nil)
	_ Consumer = (*MemQueue)(nil)
)
