package agent

import (
	"context"
	"errors"
	"sync"

	"github.com/jiujuan/goagent/core"
)

var (
	// ErrRunClosed means the run has ended or its input queue is closed, so the
	// steering message was not accepted.
	ErrRunClosed = errors.New("agent: run is not accepting steering messages")

	// ErrRunInterrupted means the run paused, so a steering message left in its
	// queue was never delivered.
	ErrRunInterrupted = errors.New("agent: run paused; steering message was not delivered")

	// ErrRunStopped means the run was stopped, escalated, transferred or canceled,
	// so a steering message left in its queue was never delivered.
	ErrRunStopped = errors.New("agent: run stopped; steering message was not delivered")
)

// SteerOutcome reports what happened to one steering message after the run
// settled its ticket.
type SteerOutcome struct {
	// Delivered is true when the message entered the model context of Step.
	Delivered bool
	// Step is the step whose model request carried the message. It is only
	// meaningful when Delivered is true.
	Step int
	// Err explains why the message was not delivered. It is nil when Delivered.
	Err error
}

// SteerTicket is the delivery receipt for one message accepted by Steer. A
// ticket settles exactly once; after that Done is closed and Outcome is final.
type SteerTicket struct {
	mu      sync.Mutex
	settled bool
	outcome SteerOutcome
	done    chan struct{}
}

func newSteerTicket() *SteerTicket {
	return &SteerTicket{done: make(chan struct{})}
}

// Done is closed when the ticket settles.
func (t *SteerTicket) Done() <-chan struct{} { return t.done }

// Outcome returns the settlement result, or the zero value while the ticket is
// still pending. It never blocks; use Wait to block until settlement.
func (t *SteerTicket) Outcome() SteerOutcome {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.outcome
}

// Wait blocks until the ticket settles or ctx ends, whichever comes first.
func (t *SteerTicket) Wait(ctx context.Context) (SteerOutcome, error) {
	select {
	case <-t.done:
		return t.Outcome(), nil
	case <-ctx.Done():
		return SteerOutcome{}, ctx.Err()
	}
}

func (t *SteerTicket) settle(out SteerOutcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.settled {
		return
	}
	t.settled = true
	t.outcome = out
	close(t.done)
}

// steerEntry is one queued message paired with the ticket its caller waits on.
type steerEntry struct {
	msg    core.Message
	ticket *SteerTicket
}

// steeringQueue is a goroutine-safe FIFO of injected messages with a terminal
// closed state. Once closed it accepts no more messages, and closing settles
// every message still waiting with the close reason.
type steeringQueue struct {
	mu      sync.Mutex
	entries []steerEntry
	closed  bool
	err     error
}

// push queues a message and returns its ticket, or nil and the close reason
// when the queue is already closed.
func (q *steeringQueue) push(msg core.Message) (*SteerTicket, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, q.err
	}
	ticket := newSteerTicket()
	q.entries = append(q.entries, steerEntry{msg: msg, ticket: ticket})
	return ticket, nil
}

// drain removes and returns every queued entry. Settlement is the caller's
// job, because only the loop knows which step received the messages.
func (q *steeringQueue) drain() []steerEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	out := q.entries
	q.entries = nil
	return out
}

// drainOrClose atomically either takes the queued entries (leaving the queue
// open) or closes an empty queue with err. A caller that gets (nil, false) has
// closed the queue and must stop accepting input.
func (q *steeringQueue) drainOrClose(err error) ([]steerEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) > 0 {
		out := q.entries
		q.entries = nil
		return out, true
	}
	if !q.closed {
		q.closed = true
		q.err = err
	}
	return nil, false
}

// close marks the queue closed, settles every remaining entry as undelivered
// with err, and keeps the first close reason on repeated calls.
func (q *steeringQueue) close(err error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.err = err
	pending := q.entries
	q.entries = nil
	q.mu.Unlock()
	settleUndelivered(pending, err)
}

// settleDelivered marks entries as delivered by the given step.
func settleDelivered(entries []steerEntry, step int) {
	for _, e := range entries {
		e.ticket.settle(SteerOutcome{Delivered: true, Step: step})
	}
}

// settleUndelivered marks entries as never delivered, with err as the reason.
func settleUndelivered(entries []steerEntry, err error) {
	for _, e := range entries {
		e.ticket.settle(SteerOutcome{Err: err})
	}
}
