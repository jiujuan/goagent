package agent

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
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

	// inFlight carries the tool batch an in-flight checkpoint left behind, set by
	// Agent.Resume and consumed once by the loop before its first model call.
	// Derived contexts leave it nil: only the resumed top-level run settles it.
	inFlight *inFlightBatch

	// continuedTurn marks a run that restarts a turn already begun — the only
	// kind Agent.Resume produces. It fixes the first step's delivery phase: a
	// next_turn message waits for the turn to actually restart, and a resume is
	// the same turn still finishing, not a new one.
	continuedTurn bool

	// replayBaseline is the step snapshot a resume without a pending batch
	// compares its first frozen step against; the loop takes it once. It is
	// deliberately not copied by deeper, subRun or forBranch: a derived run
	// freezes its own view, and the parent's baseline describes a conversation
	// the child does not share.
	replayBaseline *checkpoint.StepSnapshot

	// durable holds state that must travel with a resumed execution until its
	// next checkpoint commit. It is shared by nested contexts so a child cannot
	// bypass the parent's inbox acknowledgements or resume fencing token.
	durable *durableRunState

	steering steeringQueue

	// topLevel marks the execution environment of a top-level LLM run, the only
	// kind allowed to continue across turns. Derived contexts (deeper, subRun,
	// forBranch) leave it false.
	topLevel bool

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
		durable:       rc.durable,
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
		durable:  rc.durable,
	}
}

// forBranch derives a child execution environment for a parallel sub-agent: a
// cloned State (so concurrent branches don't race), but the same Bus/Topic/Store
// (so events merge into one observable stream and snapshots share a thread). A
// fresh steering queue is intentional. It does NOT struct-copy rc (that would
// copy the steering mutex).
func (rc *RunContext) forBranch(branch string) (*RunContext, error) {
	st, err := cloneState(*rc.State)
	if err != nil {
		return nil, err
	}
	return &RunContext{
		Context:  rc.Context,
		RunID:    rc.RunID,
		ThreadID: rc.ThreadID,
		Bus:      rc.Bus,
		Topic:    rc.Topic,
		Store:    rc.Store,
		State:    &st,
		branch:   branch,
		durable:  rc.durable,
	}, nil
}

// Steer injects a message to be delivered before the next model call. Safe to
// call from another goroutine while the run is in flight. The returned ticket
// settles when the message reaches a step's model context or the run ends
// without delivering it.
func (rc *RunContext) Steer(msg core.Message) (*SteerTicket, error) {
	return rc.SteerWith(msg, SteerOptions{})
}

// SteerWith injects a message with delivery instructions: a phase, and an
// optional request to preempt the model call in flight. The queue is the
// message's only home until a delivery point takes it.
func (rc *RunContext) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error) {
	return rc.steering.pushWith(msg, opts)
}

// publish is the loop's single-goroutine event sink.
func (rc *RunContext) publish(ev core.Event) { rc.Bus.Publish(rc.Topic, ev) }

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

// snapshot copies the injected table so one step builds its tool view from a
// consistent read while other goroutines may still write injections for later
// steps.
func (d *dynamicTools) snapshot() map[string]tool.Tool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.byName) == 0 {
		return nil
	}
	out := make(map[string]tool.Tool, len(d.byName))
	for name, t := range d.byName {
		out[name] = t
	}
	return out
}

// cloneState makes a safe copy of a checkpoint's State so a run cannot mutate a
// stored snapshot. Files is a backend handle and is shared intentionally.
func cloneState(s core.State) (core.State, error) {
	return core.CloneStateData(s)
}
