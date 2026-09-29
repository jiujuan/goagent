package agent

import (
	"context"
	"slices"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
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
	branch   string // set for a parallel sub-branch (isolation label)

	transferDepth int // delegation chain depth, bounded in transfer.go

	// resumed carries the tool batch a HITL pause left behind, set by Agent.Resume
	// and consumed once by the loop before its first model call (see hitl.go).
	resumed *resumeBatch

	steering steeringQueue

	// dynamic holds tools injected into this run by middleware
	// (LoopContext.AddTool). It is consulted before the agent's static table, so an
	// injection can replace an agent-level tool of the same name for this run only.
	dynamic dynamicTools
}

// deeper returns a child execution environment one delegation level down,
// sharing the same State (so the delegate continues the conversation) and
// Bus/Topic/Store. A fresh steering queue is intentional.
func (rc *RunContext) deeper() *RunContext {
	return &RunContext{
		Context:       rc.Context,
		RunID:         rc.RunID,
		ThreadID:      rc.ThreadID,
		Bus:           rc.Bus,
		Topic:         rc.Topic,
		Store:         rc.Store,
		State:         rc.State,
		branch:        rc.branch,
		transferDepth: rc.transferDepth + 1,
	}
}

// subRun derives an isolated child execution environment seeded with a single
// input message (fresh State, shared Files / Bus / Topic / Store). Used by
// AsTool and the DAG plan executor to run a unit on just its input, returning
// only its final text — the parent's conversation is not shared.
func (rc *RunContext) subRun(input core.Message) *RunContext {
	st := &core.State{Messages: []core.Message{input}}
	if rc.State != nil {
		st.Files = rc.State.Files
	}
	return &RunContext{
		Context:  rc.Context,
		RunID:    rc.RunID,
		ThreadID: rc.ThreadID,
		Bus:      rc.Bus,
		Topic:    rc.Topic,
		Store:    rc.Store,
		State:    st,
	}
}

// forBranch derives a child execution environment for a parallel sub-agent: a
// cloned State (so concurrent branches don't race), but the same Bus/Topic/Store
// (so events merge into one observable stream and snapshots share a thread). A
// fresh steering queue is intentional. It does NOT struct-copy rc (that would
// copy the steering mutex).
func (rc *RunContext) forBranch(branch string) *RunContext {
	st := cloneState(*rc.State)
	return &RunContext{
		Context:  rc.Context,
		RunID:    rc.RunID,
		ThreadID: rc.ThreadID,
		Bus:      rc.Bus,
		Topic:    rc.Topic,
		Store:    rc.Store,
		State:    &st,
		branch:   branch,
	}
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

// dynamicTools is a goroutine-safe name-to-tool table of run-scoped tools. The loop
// writes it while preparing a step and reads it from the tool goroutines, so both
// sides lock. Its zero value is an empty table.
type dynamicTools struct {
	mu     sync.Mutex
	byName map[string]tool.Tool
}

func (d *dynamicTools) set(t tool.Tool) {
	d.mu.Lock()
	if d.byName == nil {
		d.byName = make(map[string]tool.Tool, 4)
	}
	d.byName[t.Name()] = t
	d.mu.Unlock()
}

// lookup resolves one injected tool. ok is false when the table holds nothing for
// the name, which the caller reads as "not injected, use the agent's own tool".
func (d *dynamicTools) lookup(name string) (tool.Tool, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.byName[name]
	return t, ok
}

// advertised lists every injected tool for the model, in name order so the
// advertisement is stable across steps and runs.
func (d *dynamicTools) advertised() []llm.ToolSchema {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.byName) == 0 {
		return nil
	}
	names := make([]string, 0, len(d.byName))
	for name := range d.byName {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]llm.ToolSchema, 0, len(names))
	for _, name := range names {
		out = append(out, tool.SchemaOf(d.byName[name]))
	}
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
