package agent

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// runIdentity is a run's immutable identity, assigned when the handle is created
// and never rewritten.
type runIdentity struct {
	runID    string
	threadID string
}

// Run is a non-blocking handle to one in-flight (or paused) run. The loop is
// driven lazily on first observation, so subscribing via Iter/Events before
// driving means no events are missed.
//
// Stream with Iter() (which also records the terminal result for Wait), or call
// Wait() for just the result. On an Interrupted (HITL) pause, record decisions
// with Decide(...) then call Resume(ctx) to continue.
type Run struct {
	// ID and ThreadID are the run's identifiers as created. They are exported and
	// therefore writable by any caller holding the handle, so nothing internal reads
	// them back as a credential: the identity used for claims, committed checkpoints,
	// results and terminal records is the private copy fixed at creation.
	ID       string
	ThreadID string

	// identity is fixed by newRunHandle and never reassigned. Everything that must
	// agree with the RunID and ThreadID written into checkpoints — preparation and
	// its resume claim, the frozen result, the terminal record, lifecycle events and
	// resume — reads these fields, so rewriting the exported ones cannot reattribute
	// a run (ADR-0034 D3).
	identity runIdentity

	agent       *Agent
	bus         *bus.Bus
	topic       bus.Topic
	rc          *RunContext
	runnable    Runnable
	cancel      context.CancelFunc
	startErr    error
	preparation *runPreparation
	// admission is the same-thread admission policy resolved when the handle
	// was created; prepare reads it before touching the thread.
	admission AdmissionOptions

	done chan struct{}

	mu        sync.Mutex
	decisions []Approval

	observationMu sync.Mutex
	eventsSealed  bool
	eventTail     []core.Event

	lifecycleMu        sync.Mutex
	phase              runPhase
	preparedSettlement bool
	activated          bool
	stopParent         func() bool
	// gateRelease frees the thread slot this run holds. It is nil until
	// prepare acquires the slot (or a resume hands one over); lifecycleMu
	// guards it.
	gateRelease func()

	// terminalMu guards the frozen end-of-run data. drive() fills it before it
	// publishes any lifecycle event, so an observer that sees the terminal event
	// (or a Wait that returns) always finds a complete record. The result is
	// deep-copied here, so a caller mutating what Wait returned cannot reach it.
	// The identity is frozen along with it: Run.ID and Run.ThreadID are exported
	// and a caller can rewrite them after the fact, so a result record never reads
	// them back as its own credential (ADR-0034 D3).
	terminalMu       sync.Mutex
	terminalFrozen   bool
	terminalPaused   bool
	terminalResult   core.Result
	terminalErr      error
	terminalThreadID string
	terminalRunID    string
	terminalTurnID   string
	boundaryID       string
	boundaryRev      uint64
	boundaryOK       bool

	// recordMu serializes building and committing this run's terminal record;
	// recorded keeps the exact content so a retry is an identical commit, and
	// recordedSet says a commit of it actually succeeded.
	recordMu    sync.Mutex
	terminalID  string
	recorded    *checkpoint.Checkpoint
	recordedSet bool
}

// Iter streams the run's events as a pull iterator. The run starts on first
// iteration (after subscribing), so no events are missed. It records the
// terminal result/error for Wait().
func (r *Run) Iter() iter.Seq2[core.Event, error] {
	return func(yield func(core.Event, error) bool) {
		ch, cancel := r.subscribeRun(bus.Lossless)
		defer cancel()
		r.Start()
		for ev, err := range bus.Adapt(ch) {
			if !yield(ev, err) {
				return
			}
		}
	}
}

// Events returns a raw subscription channel and cancel func for a chosen
// DeliveryMode (e.g. Lossy for a UI). Call Iter or Wait to begin driving.
func (r *Run) Events(mode bus.DeliveryMode) (<-chan core.Event, func()) {
	return r.subscribeRun(mode)
}

// Start requests execution once and returns without waiting for it. It does
// not subscribe; register Events first when the initial events matter.
func (r *Run) Start() {
	r.lifecycleMu.Lock()
	if r.phase != runPrepared {
		r.lifecycleMu.Unlock()
		return
	}
	r.phase = runStarting
	r.lifecycleMu.Unlock()
	go r.execute()
}

// Await waits for settlement without starting or canceling the run. Canceling
// waitCtx only stops this waiter. A settled result takes precedence when both
// settlement and waitCtx are ready. Every caller receives an independent copy.
func (r *Run) Await(waitCtx context.Context) (core.Result, error) {
	select {
	case <-r.done:
		return r.settledCopy()
	default:
	}
	select {
	case <-r.done:
		return r.settledCopy()
	case <-waitCtx.Done():
		select {
		case <-r.done:
			return r.settledCopy()
		default:
			return core.Result{}, waitCtx.Err()
		}
	}
}

// Wait starts the run if necessary, then waits for settlement. It creates no
// observation subscription and does not wait for observers to drain the tail.
func (r *Run) Wait() (core.Result, error) {
	r.Start()
	return r.Await(context.Background())
}

// Steer injects a message to be delivered before the next model call. The
// returned ticket settles once the message reaches a step or the run ends
// without delivering it.
func (r *Run) Steer(msg core.Message) (*SteerTicket, error) { return r.SteerWith(msg, SteerOptions{}) }

// SteerWith injects a message with delivery instructions. See
// RunContext.SteerWith; the options are per message, so one run can mix a
// message that waits for the next turn with one that arrives now.
func (r *Run) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error) {
	return r.rc.SteerWith(msg, opts)
}

// Task reports this run's runtime task identity. The id, thread, kind and
// depth are fixed when the handle is created; the turn id is filled once
// prepare resolves it, so it may be empty before the run is driven.
func (r *Run) Task() TaskInfo { return r.rc.taskInfo() }

// Paused reports whether this run ended by pausing for a human decision instead
// of finishing. It reads the outcome drive() froze, not the store: while the run
// is still going (or before anything has driven it) it reports false, so the
// answer only means something once Wait returned or a terminal event was seen.
func (r *Run) Paused() bool {
	r.terminalMu.Lock()
	defer r.terminalMu.Unlock()
	return r.terminalFrozen && r.terminalPaused
}

// FrozenOutcome returns the outcome drive() froze when the run ended — the same
// result and error RecordCompletion writes — as a copy independent of the run, so
// editing what Wait returned cannot reach what a later reader sees here. Like
// Paused it only means something once the run has settled: before that it returns
// a zero result and a nil error, and a paused run reports the same zero result
// with no error.
func (r *Run) FrozenOutcome() (core.Result, error) {
	r.terminalMu.Lock()
	defer r.terminalMu.Unlock()
	if !r.terminalFrozen {
		return core.Result{}, nil
	}
	out, err := cloneResult(r.terminalResult)
	if err != nil {
		return core.Result{}, errors.Join(r.terminalErr, err)
	}
	return out, r.terminalErr
}

// drive remains an internal alias for fixtures and legacy package helpers.
func (r *Run) drive() { r.Start() }

func (r *Run) execute() {
	r.rc.publish(core.RunStarted{RunID: r.identity.runID, ThreadID: r.identity.threadID})
	if r.startErr != nil {
		r.settleExecution(runOutcome{Err: r.startErr}, false, false)
		return
	}
	if err := r.prepare(); err != nil {
		r.settleExecution(runOutcome{Err: err}, false, false)
		return
	}
	r.lifecycleMu.Lock()
	r.phase = runRunning
	r.lifecycleMu.Unlock()
	task := r.rc.taskInfo()
	r.rc.publish(core.TaskStarted{TaskID: task.ID, ParentID: task.ParentID, ThreadID: task.ThreadID, Kind: string(task.Kind), Depth: task.Depth})
	for _, ev := range r.rc.drainApprovalEvents() {
		r.rc.publish(ev)
	}
	r.settleExecution(r.runnable.run(r.rc), true, true)
}
