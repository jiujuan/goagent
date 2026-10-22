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
	"fmt"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// Agent is both the declaration of an agent (its config) and a runnable handle.
// Build one with New, then call Run (blocking, returns the answer), RunDetailed
// (blocking, returns the structured outcome and committed boundary) or Stream
// (non-blocking, returns a *Run for live events and control).
type Agent struct {
	cfg      config
	loop     *AgentLoop // the LLM loop (nil for workflow agents)
	parent   *Agent     // set when this agent is a sub-agent of another
	runnable Runnable
	bus      *bus.Bus
	store    checkpoint.Checkpointer
}

// New builds an Agent from functional options. WithModel is required.
func New(opts ...Option) (*Agent, error) {
	c := config{maxTurns: defaultMaxTurns, contextOverflowRetry: 1, claimLease: checkpoint.DefaultLease, inFlightCheckpoint: true}
	for _, o := range opts {
		o(&c)
	}
	if c.model == nil {
		return nil, errors.New("agent: WithModel is required")
	}
	if c.claimLease <= 0 {
		return nil, fmt.Errorf("agent: claim lease must be positive, got %v", c.claimLease)
	}
	switch c.stepSnapshotPolicy {
	case StepSnapshotWarn, StepSnapshotStrict, StepSnapshotOff:
	default:
		return nil, fmt.Errorf("agent: unknown step snapshot policy %d", c.stepSnapshotPolicy)
	}
	if c.bus == nil {
		c.bus = bus.New()
	}
	if c.store == nil {
		c.store = checkpoint.NewMemory()
	}
	if c.requireThreadCoordination {
		if _, ok := c.store.(checkpoint.ThreadCoordinator); !ok {
			return nil, fmt.Errorf("agent: checkpointer %T: %w", c.store, checkpoint.ErrCoordinationUnavailable)
		}
	}
	a := &Agent{cfg: c, bus: c.bus, store: c.store}
	a.loop = newLoop(c)
	a.runnable = &llmRunner{agent: a, loop: a.loop}
	// Wire the delegation tree: become the parent of each sub-agent, then
	// advertise the transfer tool for the resolved targets.
	for _, s := range c.subAgents {
		s.parent = a
	}
	if tt := transferToolFor(a.transferTargets()); tt != nil {
		a.loop.addTool(tt)
	}
	return a, nil
}

// Bus exposes the agent's event bus (for extra subscribers / tracing).
func (a *Agent) Bus() *bus.Bus { return a.bus }

// finishRun notifies the run-end capability of each configured middleware, in
// registration order. Workflow agents have no loop, so the hook walks the
// configured middleware list rather than the loop's stack.
func (a *Agent) finishRun(rc *RunContext, res core.Result, err error) {
	for _, m := range a.cfg.middleware {
		if f, ok := m.(RunFinisher); ok {
			f.FinishRun(rc, res, err)
		}
	}
}

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
// across calls. Defaults to a fresh ephemeral thread. The id is used verbatim as
// one file name in the File checkpointer and one directory name in a workspace's
// artifact store, so it must be [A-Za-z0-9_-], 1..core.MaxThreadIDLen bytes
// (ADR-0028): an id outside that set fails the run with core.CheckThreadID's
// error instead of being rewritten into a name.
func OnThread(id string) RunOption { return func(r *RunConfig) { r.ThreadID = id } }

// WithMessage overrides the user message (e.g. multimodal content) instead of
// the plain string passed to Run/Stream.
func WithMessage(m core.Message) RunOption { return func(r *RunConfig) { r.Message = m } }

// WithRunFiles supplies a virtual filesystem backend for this run. It takes
// precedence over anything restored from the thread's checkpoint. Durability:
// a backend implementing core.Snapshottable (like vfs.InState) is persisted by
// the File checkpointer and rehydrated on resume automatically; a backend that
// does not (a real directory like vfs.DirStore, a remote store) is externally
// managed — the checkpoint carries none of its bytes, so resuming from the
// checkpoint alone recovers nothing. Every process that continues such a thread
// must pass an equivalent backend here again, and the files it points at are not
// rolled back by time travel (ADR-0026, ADR-0027; pinned by the
// TestDurableDirStore* cases in this package's tests).
//
// The backend has to match the thread it is attached to: workspace.RunFiles
// names its directory after the thread id, so handing a store built for one id to
// a run on another reads and writes someone else's artifacts. Both halves key off
// the same id, which is why that id is validated as a file name (ADR-0028).
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
	return a.streamConfigured(ctx, resolveRunConfig(input, core.NewID("thread"), opts...))
}

func resolveRunConfig(input, threadID string, opts ...RunOption) RunConfig {
	rc := RunConfig{ThreadID: threadID, Message: core.UserText(input)}
	for _, o := range opts {
		o(&rc)
	}
	return rc
}

func (a *Agent) streamConfigured(ctx context.Context, rc RunConfig) *Run {
	// Validation and message freezing happen at handle creation. Store access is
	// deferred until drive so lazily-created runs restore the current head.
	err := core.CheckThreadID(rc.ThreadID)
	message := core.Message{}
	if err == nil {
		message, err = core.CloneMessage(rc.Message)
	}
	run := a.newRunHandle(ctx, rc.ThreadID, &core.State{})
	run.startErr = err
	run.preparation = &runPreparation{message: message, files: rc.Files, restoreCurrent: true}
	return run
}

// restore loads the latest checkpoint's state for a thread, or a fresh State.
// On store error it returns an empty state plus the error (surfaced via the run).
// An explicit runFiles backend wins without restoring a snapshot that would be
// discarded immediately. It also reports the head checkpoint it read, which is
// nil for a thread that has never committed: a run without thread coordination
// starts its parent chain from that head.
func (a *Agent) restore(ctx context.Context, threadID string, runFiles core.FileStore) (*core.State, *checkpoint.Checkpoint, error) {
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return &core.State{}, nil, err
	}
	state, err := restoreCheckpoint(cp, runFiles)
	if err != nil {
		return state, cp, err
	}
	return state, cp, nil
}

func restoreCheckpoint(cp *checkpoint.Checkpoint, runFiles core.FileStore) (*core.State, error) {
	if cp == nil {
		return &core.State{Files: runFiles}, nil
	}
	st, err := cloneState(cp.State)
	if err != nil {
		return &core.State{}, err
	}
	if runFiles != nil {
		st.Files = runFiles
		return &st, nil
	}
	if err := applyFileSnapshot(&st, cp.FileSnapshot); err != nil {
		return &core.State{}, err
	}
	return &st, nil
}

// applyFileSnapshot rehydrates a restored State's virtual filesystem from a
// checkpoint's persisted file snapshot (ADR-0026): the File checkpointer stored
// the contents of any core.Snapshottable backend as content-addressed blobs and
// served them back on Checkpoint.FileSnapshot. A nil snapshot (nothing ever
// written, an externally managed backend, or an in-memory checkpointer whose
// live handle already travelled with the state) leaves Files untouched for the
// caller's fallback rules. A non-nil snapshot, including an empty one, replaces
// any retained handle with an independent in-memory view.
func applyFileSnapshot(st *core.State, snap map[string][]byte) error {
	if st == nil || snap == nil {
		return nil
	}
	fs := vfs.NewInState()
	if err := fs.Restore(snap); err != nil {
		return fmt.Errorf("agent: restore file snapshot: %w", err)
	}
	st.Files = fs
	return nil
}

// newRunHandle builds the per-run execution environment (RunContext) and its
// Run handle. Shared by Stream and Resume.
func (a *Agent) newRunHandle(ctx context.Context, threadID string, state *core.State) *Run {
	return a.newRunHandleWithID(ctx, core.NewID("run"), threadID, state)
}

// newRunHandleWithID is newRunHandle with a caller-assigned run id, used by the managed
// entry point so the identity a queue recorded is the identity the run executes under.
// The id is fixed here and never regenerated or reassigned.
func (a *Agent) newRunHandleWithID(ctx context.Context, runID, threadID string, state *core.State) *Run {
	runCtx, cancel := context.WithCancel(ctx)
	topic := bus.Topic(runID)
	rc := &RunContext{
		Context:  runCtx,
		RunID:    runID,
		ThreadID: threadID,
		Bus:      a.bus,
		Topic:    topic,
		Store:    a.store,
		State:    state,
		durable:  newDurableRunState(),
		// The queue owns the preemption switch so a message is refused at
		// enqueue time when the agent cannot honor it.
		steering:      steeringQueue{preemptionEnabled: a.cfg.inputPreemption},
		preemptedStep: -1,
		// Only an LLM agent's own run may continue after a final answer. Workflow
		// roots and derived child contexts keep the single-turn semantics.
		topLevel: a.loop != nil,
	}
	// The lease lives on the durable state every nested run shares, so one run cannot
	// commit under one lease while its claim was renewed under another.
	rc.durable.claimLease = a.cfg.claimLease
	return &Run{
		ID:       runID,
		ThreadID: threadID,
		identity: runIdentity{runID: runID, threadID: threadID},
		agent:    a,
		bus:      a.bus,
		topic:    topic,
		rc:       rc,
		runnable: a.runnable,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}
