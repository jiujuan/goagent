package runtime

import (
	"context"
	"iter"
	"sync"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

// Run is a non-blocking handle to one in-flight (or pending) agent run. The
// loop is driven lazily on first observation, so subscribing via Iter/Events
// before driving means no events are missed.
//
// Usage is single-consumer: stream with Iter() (which also records the terminal
// result for Wait), or call Wait() for just the result. Mixing both works but
// prefer Iter() first.
type Run struct {
	ID       string
	ThreadID string

	bus      *bus.Bus
	topic    bus.Topic
	rc       *agent.RunContext
	compiled agent.Runnable
	cancel   context.CancelFunc

	startOnce sync.Once
	doneOnce  sync.Once
	done      chan struct{}
	result    core.Result
	err       error
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

// Events returns a raw subscription channel and a cancel func, for callers that
// want a specific DeliveryMode (e.g. Lossy for a UI). It does not start the run;
// call Iter or Wait, or drive() indirectly, to begin.
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

// Cancel aborts the run.
func (r *Run) Cancel() { r.cancel() }

// drive launches the loop exactly once.
func (r *Run) drive() {
	r.startOnce.Do(func() {
		go agent.Drive(r.rc, r.compiled)
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
