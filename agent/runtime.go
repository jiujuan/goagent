package agent

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// RunContext is the runtime — the execution environment carried through one
// whole run. There is no separate runtime package; this type IS the runtime
// concept. It bundles the "equipment" the loop needs end to end: where to
// publish observation events (Bus/Topic), where to snapshot for pause/resume
// (Store), the live State, the steering inbox, and cancellation (via the
// embedded context.Context). Each step's transient view is the LoopContext
// (loopctx.go), which embeds this.
type RunContext struct {
	context.Context

	RunID    string
	ThreadID string
	Bus      *bus.Bus
	Topic    bus.Topic
	Store    checkpoint.Checkpointer
	State    *core.State

	steering steeringQueue
}

// Steer injects a message to be delivered before the next model call. Safe to
// call from another goroutine while the run is in flight.
func (rc *RunContext) Steer(msg core.Message) { rc.steering.push(msg) }

// publish is the loop's single-goroutine event sink.
func (rc *RunContext) publish(ev core.Event) { rc.Bus.Publish(rc.Topic, ev) }

// steeringQueue is a tiny goroutine-safe FIFO of injected messages.
type steeringQueue struct {
	mu   sync.Mutex
	msgs []core.Message
}

func (q *steeringQueue) push(m core.Message) {
	q.mu.Lock()
	q.msgs = append(q.msgs, m)
	q.mu.Unlock()
}

func (q *steeringQueue) drain() []core.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.msgs) == 0 {
		return nil
	}
	out := q.msgs
	q.msgs = nil
	return out
}

// cloneState makes a safe copy of a checkpoint's State so a run cannot mutate a
// stored snapshot. Files is a backend handle and is shared intentionally.
func cloneState(s core.State) core.State {
	out := core.State{Files: s.Files}
	if s.Messages != nil {
		out.Messages = append([]core.Message(nil), s.Messages...)
	}
	if s.Todos != nil {
		out.Todos = append([]core.Todo(nil), s.Todos...)
	}
	if s.KV != nil {
		out.KV = make(map[string]any, len(s.KV))
		for k, v := range s.KV {
			out.KV[k] = v
		}
	}
	return out
}
