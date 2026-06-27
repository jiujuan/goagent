// Package agent is goagent's single execution package. It exposes a small
// façade — New + Run/Stream/Resume — and contains the runtime (execution
// environment) underneath: the AgentLoop phase machine, the RunContext it
// carries, middleware hooks, tool execution, and the human-in-the-loop
// pause/continue closure. There is no separate "runtime" package; that concept
// lives here, in runtime.go / loop.go / hitl.go.
//
// The package depends only on lower layers (core, llm, tool, bus, checkpoint,
// vfs); nothing imports agent back, so the dependency graph stays acyclic.
package agent

import (
	"context"
	"errors"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// Agent is both the declaration of an agent (its config) and a runnable handle.
// Build one with New, then call Run (blocking, returns the answer) or Stream
// (non-blocking, returns a *Run for live events and control).
type Agent struct {
	cfg      config
	runnable Runnable
	bus      *bus.Bus
	store    checkpoint.Checkpointer
}

// New builds an Agent from functional options. WithModel is required.
func New(opts ...Option) (*Agent, error) {
	c := config{maxSteps: defaultMaxSteps}
	for _, o := range opts {
		o(&c)
	}
	if c.model == nil {
		return nil, errors.New("agent: WithModel is required")
	}
	if c.bus == nil {
		c.bus = bus.New()
	}
	if c.store == nil {
		c.store = checkpoint.NewMemory()
	}
	a := &Agent{cfg: c, bus: c.bus, store: c.store}
	a.runnable = newLoop(c)
	return a, nil
}

// Bus exposes the agent's event bus (for extra subscribers / tracing).
func (a *Agent) Bus() *bus.Bus { return a.bus }

// Name reports the configured name.
func (a *Agent) Name() string { return a.cfg.name }

// RunConfig holds run-scoped settings, set with RunOptions.
type RunConfig struct {
	ThreadID string
	Message  core.Message
	Files    core.FileStore
}

// RunOption configures a single Run/Stream invocation.
type RunOption func(*RunConfig)

// OnThread runs on a specific thread, so state and checkpoints accumulate
// across calls. Defaults to a fresh ephemeral thread.
func OnThread(id string) RunOption { return func(r *RunConfig) { r.ThreadID = id } }

// WithMessage overrides the user message (e.g. multimodal content) instead of
// the plain string passed to Run/Stream.
func WithMessage(m core.Message) RunOption { return func(r *RunConfig) { r.Message = m } }

// WithRunFiles supplies a virtual filesystem backend for this run.
func WithRunFiles(f core.FileStore) RunOption { return func(r *RunConfig) { r.Files = f } }

// Run drives the agent loop to completion and returns the final answer text. It
// is the convenience entry: internally it calls the model and tools in a loop
// until the model replies without tool calls.
func (a *Agent) Run(ctx context.Context, input string, opts ...RunOption) (string, error) {
	res, err := a.Stream(ctx, input, opts...).Wait()
	return res.Message.Text(), err
}

// Stream starts a run and returns a non-blocking *Run handle for live events
// (Iter/Events), settlement (Wait), and control (Steer/Cancel/Decide). The loop
// is driven lazily on first observation, so no events are missed.
func (a *Agent) Stream(ctx context.Context, input string, opts ...RunOption) *Run {
	rc := RunConfig{ThreadID: core.NewID("thread"), Message: core.UserText(input)}
	for _, o := range opts {
		o(&rc)
	}
	state, err := a.restore(ctx, rc.ThreadID)
	if rc.Files != nil {
		state.Files = rc.Files
	} else if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	if rc.Message.Role != "" {
		state.Messages = append(state.Messages, rc.Message)
	}
	run := a.newRunHandle(ctx, rc.ThreadID, state)
	run.startErr = err
	return run
}

// restore loads the latest checkpoint's state for a thread, or a fresh State.
// On store error it returns an empty state plus the error (surfaced via the run).
func (a *Agent) restore(ctx context.Context, threadID string) (*core.State, error) {
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return &core.State{}, err
	}
	if cp == nil {
		return &core.State{}, nil
	}
	st := cloneState(cp.State)
	return &st, nil
}

// newRunHandle builds the per-run execution environment (RunContext) and its
// Run handle. Shared by Stream and Resume.
func (a *Agent) newRunHandle(ctx context.Context, threadID string, state *core.State) *Run {
	runCtx, cancel := context.WithCancel(ctx)
	runID := core.NewID("run")
	topic := bus.Topic(runID)
	rc := &RunContext{
		Context:  runCtx,
		RunID:    runID,
		ThreadID: threadID,
		Bus:      a.bus,
		Topic:    topic,
		Store:    a.store,
		State:    state,
	}
	return &Run{
		ID:       runID,
		ThreadID: threadID,
		agent:    a,
		bus:      a.bus,
		topic:    topic,
		rc:       rc,
		runnable: a.runnable,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}
