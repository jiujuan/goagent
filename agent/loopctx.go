package agent

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// RunContext is the per-run environment. It embeds context.Context so it passes
// directly to model and tool calls. The loop publishes observational events to
// Bus on Topic, snapshots State to Checkpointer, and drains the steering queue
// between turns. It depends only on the bus/checkpoint abstractions, never on
// the runtime engine — that is what keeps the dependency graph acyclic.
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

// LoopContext is the mutable per-step view the loop threads through its phases
// and hands to middleware.
type LoopContext struct {
	*RunContext

	Step    int
	Request *llm.Request
	History []core.Message
}

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
