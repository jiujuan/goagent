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

	startOnce sync.Once
	doneOnce  sync.Once
	done      chan struct{}
	result    core.Result
	err       error

	mu        sync.Mutex
	decisions []Approval

	lifecycleMu  sync.Mutex
	driveStarted bool
	cancelErr    error

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
		ch, cancel := r.bus.Subscribe(r.topic, bus.Lossless)
		defer cancel()
		r.drive()
		for ev, err := range bus.Adapt(ch) {
			r.capture(ev)
			if !yield(ev, err) {
				return
			}
		}
	}
}

// Events returns a raw subscription channel and cancel func for a chosen
// DeliveryMode (e.g. Lossy for a UI). Call Iter or Wait to begin driving.
func (r *Run) Events(mode bus.DeliveryMode) (<-chan core.Event, func()) {
	return r.bus.Subscribe(r.topic, mode)
}

// Wait blocks until the run settles and returns its result or error. It drives
// and drains the run itself if no Iter consumer is active.
func (r *Run) Wait() (core.Result, error) {
	select {
	case <-r.done:
		return r.result, r.err
	default:
	}
	ch, cancel := r.bus.Subscribe(r.topic, bus.Lossless)
	defer cancel()
	r.drive()
	for {
		select {
		case <-r.done:
			return r.result, r.err
		case ev, ok := <-ch:
			if !ok {
				<-r.done
				return r.result, r.err
			}
			if r.capture(ev) {
				return r.result, r.err
			}
		}
	}
}

// Steer injects a message to be delivered before the next model call.
func (r *Run) Steer(msg core.Message) { r.rc.Steer(msg) }

// Cancel aborts the run. A Resume owns a durable claim before lazy execution
// starts, so canceling that prepared handle releases the claim synchronously.
func (r *Run) Cancel() {
	r.lifecycleMu.Lock()
	r.cancel()
	if !r.driveStarted {
		r.cancelErr = errors.Join(r.cancelErr, r.rc.releaseDurableClaim())
	}
	r.lifecycleMu.Unlock()
}

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
	out := r.terminalResult
	if cloned, err := core.CloneMessage(r.terminalResult.Message); err == nil {
		out = core.Result{Message: cloned}
	}
	return out, r.terminalErr
}

// freezeTerminal records how this run ended together with the boundary it last
// committed and the identity that produced it. drive() calls it before publishing
// any lifecycle event, so an observer that sees the terminal result always finds a
// complete record, and the stored result is a deep copy — a caller editing what
// Wait returned cannot reach it. The thread, run and turn identifiers are copied
// here rather than read back from the exported Run fields later, so rewriting those
// fields cannot reattribute a finished run.
func (r *Run) freezeTerminal(boundaryID string, boundaryRev uint64, boundaryOK bool, res core.Result, err error, paused bool) {
	frozen := res
	if cloned, cloneErr := core.CloneMessage(res.Message); cloneErr == nil {
		frozen = core.Result{Message: cloned}
	}
	// Read the turn before taking terminalMu so the lineage mutex is never nested
	// under it.
	turnID := r.rc.runTurnID()

	r.terminalMu.Lock()
	defer r.terminalMu.Unlock()
	if r.terminalFrozen {
		return
	}
	r.terminalFrozen = true
	r.terminalPaused = paused
	r.terminalResult = frozen
	r.terminalErr = err
	r.terminalThreadID = r.identity.threadID
	r.terminalRunID = r.identity.runID
	r.terminalTurnID = turnID
	r.boundaryID, r.boundaryRev, r.boundaryOK = boundaryID, boundaryRev, boundaryOK
}

// freezeFailure records a run that ended without executing (or before it could
// commit anything), so its boundary is whatever the run did commit by that point.
func (r *Run) freezeFailure(err error) {
	id, revision, ok := r.rc.committedBoundary()
	r.freezeTerminal(id, revision, ok, core.Result{}, err, false)
}

// drive launches the run exactly once. It owns the lifecycle events: RunStarted
// before, and the single terminal event (RunDone/RunFailed/Interrupted) derived
// from the runnable's outcome. Runnables (including nested workflow children)
// never publish lifecycle, so the shared topic carries exactly one of each.
func (r *Run) drive() {
	r.startOnce.Do(func() {
		r.lifecycleMu.Lock()
		r.driveStarted = true
		r.lifecycleMu.Unlock()
		go func() {
			r.bus.Publish(r.topic, core.RunStarted{RunID: r.identity.runID, ThreadID: r.identity.threadID})
			if r.startErr != nil {
				err := errors.Join(r.startErr, r.rc.releaseDurableClaim())
				r.freezeFailure(err)
				r.bus.Publish(r.topic, core.RunFailed{Err: err})
				r.finish(core.Result{}, err)
				return
			}
			if err := r.prepare(); err != nil {
				r.lifecycleMu.Lock()
				cancelErr := r.cancelErr
				r.lifecycleMu.Unlock()
				err = errors.Join(err, cancelErr, r.rc.releaseDurableClaim())
				r.freezeFailure(err)
				r.bus.Publish(r.topic, core.RunFailed{Err: err})
				r.finish(core.Result{}, err)
				return
			}
			for _, ev := range r.rc.drainApprovalEvents() {
				r.bus.Publish(r.topic, ev)
			}
			out := r.runnable.run(r.rc)
			// The boundary is captured while the claim is still held, and the
			// outcome is frozen with it before any terminal event goes out.
			boundaryID, boundaryRev, boundaryOK := r.rc.committedBoundary()
			cleanupErr := r.rc.releaseDurableClaim()
			res, err, paused := terminalOutcome(out, cleanupErr)
			r.freezeTerminal(boundaryID, boundaryRev, boundaryOK, res, err, paused)

			switch {
			case err != nil:
				r.agent.finishRun(r.rc, core.Result{}, err)
				r.bus.Publish(r.topic, core.RunFailed{Err: err})
				r.finish(core.Result{}, err)
			case paused:
				// Paused, not finished: run-end hooks wait for the eventual
				// completion (or failure) after a resume.
				r.bus.Publish(r.topic, core.Interrupted{
					Pending:  out.Pending,
					Phase:    out.Phase,
					Reason:   out.Reason,
					Recovery: out.Recovery,
				})
				r.finish(core.Result{}, nil)
			default:
				r.agent.finishRun(r.rc, out.Result, nil)
				r.bus.Publish(r.topic, core.RunDone{Result: out.Result})
				r.finish(out.Result, nil)
			}
		}()
	})
}

// capture records a terminal event and reports whether it was terminal.
func (r *Run) capture(ev core.Event) bool {
	switch e := ev.(type) {
	case core.RunDone:
		r.finish(e.Result, nil)
		return true
	case core.RunFailed:
		r.finish(core.Result{}, e.Err)
		return true
	case core.Interrupted:
		r.finish(core.Result{}, nil)
		return true
	default:
		return false
	}
}

func (r *Run) finish(res core.Result, err error) {
	r.doneOnce.Do(func() {
		r.result, r.err = res, err
		close(r.done)
	})
}
