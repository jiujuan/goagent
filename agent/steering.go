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

	// ErrPreemptionDisabled means the message asked to preempt a model call but
	// the agent was built without WithInputPreemption.
	ErrPreemptionDisabled = errors.New("agent: input preemption is disabled")

	// ErrPreemptionPhase means a message asked to preempt a model call with a
	// phase that must wait anyway; only before_model input may interrupt.
	ErrPreemptionPhase = errors.New("agent: only before_model input may preempt a model call")
)

// SteerOptions carries the delivery instructions for one steering message.
type SteerOptions struct {
	// Phase is the latest point at which the message may enter the run. The
	// empty value means before_model.
	Phase core.InputPhase
	// Preempt asks the run to cancel the model call in flight so the message
	// reaches the retry of the same step. It requires WithInputPreemption and a
	// before_model phase; otherwise pushWith refuses the message. When no model
	// call is in flight, the message simply queues.
	Preempt bool
	// Reason labels the request in the ModelPreempted event. It is advisory.
	Reason string
}

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
	// State is the receipt state after settlement: delivered or rejected. It is
	// empty before the ticket settles; use SteerTicket.State for a live read.
	State core.InputState
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

// State reports the receipt state: queued while the ticket is unsettled, then
// delivered or rejected. Unlike Outcome, it distinguishes "not yet" from
// "refused", so a caller can poll without blocking.
func (t *SteerTicket) State() core.InputState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.settled {
		return core.InputStateQueued
	}
	if t.outcome.Err != nil {
		return core.InputStateRejected
	}
	return core.InputStateDelivered
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

// steerEntry is one queued message paired with the ticket its caller waits on,
// plus the delivery phase the caller asked for.
type steerEntry struct {
	msg    core.Message
	ticket *SteerTicket
	phase  core.InputPhase
}

// steeringQueue is a goroutine-safe FIFO of injected messages with a terminal
// closed state. Once closed it accepts no more messages, and closing settles
// every message still waiting with the close reason.
type steeringQueue struct {
	mu                sync.Mutex
	entries           []steerEntry
	closed            bool
	err               error
	preemptionEnabled bool // WithInputPreemption: preempt requests may cancel a model call
}

// push queues a message and returns its ticket, or nil and the close reason
// when the queue is already closed.
func (q *steeringQueue) push(msg core.Message) (*SteerTicket, error) {
	return q.pushWith(msg, SteerOptions{})
}

// pushWith queues a message with delivery instructions. A closed queue refuses
// it; an unknown phase or a preempt request the agent cannot honor is refused
// before anything is queued, so the caller never holds a ticket for a message
// the run would not deliver.
func (q *steeringQueue) pushWith(msg core.Message, opts SteerOptions) (*SteerTicket, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, q.err
	}
	phase, err := core.NormalizeInputPhase(opts.Phase)
	if err != nil {
		return nil, err
	}
	if opts.Preempt {
		if !q.preemptionEnabled {
			return nil, ErrPreemptionDisabled
		}
		if phase != core.InputBeforeModel {
			return nil, ErrPreemptionPhase
		}
	}
	ticket := newSteerTicket()
	q.entries = append(q.entries, steerEntry{msg: msg, ticket: ticket, phase: phase})
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

// takeReady removes and returns the entries this delivery point may deliver.
// A new turn takes everything, including next_turn messages; any other point
// keeps them queued until the turn actually restarts. Kept entries hold their
// relative order.
func (q *steeringQueue) takeReady(newTurn bool) []steerEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	if newTurn {
		out := q.entries
		q.entries = nil
		return out
	}
	taken := make([]steerEntry, 0, len(q.entries))
	kept := make([]steerEntry, 0, len(q.entries))
	for _, e := range q.entries {
		if e.phase == core.InputNextTurn {
			kept = append(kept, e)
			continue
		}
		taken = append(taken, e)
	}
	if len(kept) == 0 {
		q.entries = nil
	} else {
		q.entries = kept
	}
	if len(taken) == 0 {
		return nil
	}
	return taken
}

// takePhase removes and returns only the entries with the given phase, keeping
// the rest queued in order. Preemption uses it to deliver exactly the input the
// retry was asked to wait for.
func (q *steeringQueue) takePhase(phase core.InputPhase) []steerEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	taken := make([]steerEntry, 0, len(q.entries))
	kept := make([]steerEntry, 0, len(q.entries))
	for _, e := range q.entries {
		if e.phase == phase {
			taken = append(taken, e)
			continue
		}
		kept = append(kept, e)
	}
	if len(kept) == 0 {
		q.entries = nil
	} else {
		q.entries = kept
	}
	if len(taken) == 0 {
		return nil
	}
	return taken
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
		e.ticket.settle(SteerOutcome{Delivered: true, Step: step, State: core.InputStateDelivered})
	}
}

// settleUndelivered marks entries as never delivered, with err as the reason.
func settleUndelivered(entries []steerEntry, err error) {
	for _, e := range entries {
		e.ticket.settle(SteerOutcome{Err: err, State: core.InputStateRejected})
	}
}
