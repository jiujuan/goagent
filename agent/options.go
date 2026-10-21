package agent

import (
	"time"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

// config is the unexported settings bag an Agent is built from. Users never
// construct it directly; they pass With* Options to New. It is the former Spec,
// turned into functional-options form.
type config struct {
	name                      string
	description               string
	instruction               string
	prompt                    *prompt.Builder
	model                     llm.Model
	tools                     []tool.Tool
	middleware                []Middleware
	subAgents                 []*Agent
	maxTurns                  int
	toolExec                  ToolExecMode
	toolTimeout               time.Duration
	claimLease                time.Duration
	modelOpts                 []llm.Option
	outputKey                 string
	requireThreadCoordination bool
	stepSnapshotPolicy        StepSnapshotPolicy

	// delegation toggles (see transfer.go)
	noTransfer       bool
	noTransferParent bool
	noTransferPeers  bool

	// Execution-environment infrastructure. Each defaults to a fresh instance;
	// inject shared ones (e.g. across a multi-agent run) via WithBus /
	// WithCheckpointer.
	bus   *bus.Bus
	store checkpoint.Checkpointer
}

// Option configures an Agent at construction (functional-options pattern).
type Option func(*config)

// WithModel sets the chat model. Required.
func WithModel(m llm.Model) Option { return func(c *config) { c.model = m } }

// WithName sets the agent's name (used in delegation and tracing).
func WithName(s string) Option { return func(c *config) { c.name = s } }

// WithDescription sets a human/peer-facing description.
func WithDescription(s string) Option { return func(c *config) { c.description = s } }

// WithInstruction sets the system prompt.
func WithInstruction(s string) Option { return func(c *config) { c.instruction = s } }

// WithPrompt builds the system prompt from composable prompt.Sections (Identity,
// Environment, ToolGuidance, SessionState, custom). It is rendered once per run
// and takes precedence over WithInstruction.
func WithPrompt(b *prompt.Builder) Option { return func(c *config) { c.prompt = b } }

// WithTools adds tools (additive across calls).
func WithTools(ts ...tool.Tool) Option {
	return func(c *config) { c.tools = append(c.tools, ts...) }
}

// WithMiddleware adds loop middleware, outermost first (additive across calls).
func WithMiddleware(mw ...Middleware) Option {
	return func(c *config) { c.middleware = append(c.middleware, mw...) }
}

// WithSubAgents registers delegation targets (additive). Sub-agents are full
// *Agent values; the model can hand off to them via the auto-injected
// transfer_to_agent tool.
func WithSubAgents(subs ...*Agent) Option {
	return func(c *config) { c.subAgents = append(c.subAgents, subs...) }
}

// WithoutTransfer disables the auto-injected transfer_to_agent tool even when
// sub-agents are configured.
func WithoutTransfer() Option { return func(c *config) { c.noTransfer = true } }

// WithMaxTurns caps how many model<->tool turns one run may take (default 16).
// A "turn" is one model call plus any tools it triggers — it matches the
// TurnStarted/TurnDone events. This is a safety bound on a single run, NOT a
// "best of N" refinement count; for iterative refinement use a Loop workflow:
//
//	agent.Loop("refine", 3, drafter, critic)   // run up to 3 rounds
func WithMaxTurns(n int) Option { return func(c *config) { c.maxTurns = n } }

// WithToolExecution selects concurrent (default) or sequential tool execution.
// Note: a tool wrapped with tool.AsSequential still gives its own call a serial
// segment regardless of this mode; the consecutive calls around it remain
// concurrent in their own segments.
func WithToolExecution(m ToolExecMode) Option { return func(c *config) { c.toolExec = m } }

// WithToolTimeout bounds how long a single tool call may run (default 0 = no
// bound). It is per call, not per segment: each slow call in a serial segment
// can use the full duration. When it fires, the context handed to the tool is
// cancelled, so a tool that watches its context returns early; Go cannot stop a
// function that is already running, so a tool that ignores the context keeps
// executing. Total wall-clock for a whole run is RunBudget.MaxDuration's job,
// not this one's.
func WithToolTimeout(d time.Duration) Option { return func(c *config) { c.toolTimeout = d } }

// WithClaimLease sets how long this agent's checkpoint claims are held: the thread claim
// taken when a run starts, the resume claim claimed by Agent.Resume, the renewal interval
// derived from it (lease/2), and the lease each commit asks the store to keep. One value
// covers all of those, so a run cannot hold a claim shorter than the one it renewed, and a
// sub-agent built by its own New uses its own setting.
//
// It must be positive; the default is checkpoint.DefaultLease (30 seconds). This does not
// change the store's package-level default for anybody else, and it is not the queue's task
// record lease: that bounds a worker's ownership of a run record and is configured on
// queue.ManagedRunWorker. A heartbeat on the task record never renews this claim, and this
// claim's renewal never writes a task record (ADR-0034 D6).
func WithClaimLease(d time.Duration) Option { return func(c *config) { c.claimLease = d } }

// WithStepSnapshotPolicy selects how a resume reacts when its first frozen step
// differs from the step view recorded by the resumed checkpoint. The default
// StepSnapshotWarn publishes core.StepViewDrifted and continues;
// StepSnapshotStrict fails with ErrStepViewDrift before the model call;
// StepSnapshotOff skips the comparison. A resume that carries a pending tool
// batch never compares, because the batch executes before any new view freezes.
func WithStepSnapshotPolicy(p StepSnapshotPolicy) Option {
	return func(c *config) { c.stepSnapshotPolicy = p }
}

// WithOutputKey writes this agent's final answer text into State.KV under key,
// for inter-stage coordination in a workflow. Later stages reference it in their
// instruction via a {{key}} placeholder (rendered before each model call).
func WithOutputKey(key string) Option { return func(c *config) { c.outputKey = key } }

// WithModelOptions applies llm options to every model request (additive).
func WithModelOptions(o ...llm.Option) Option {
	return func(c *config) { c.modelOpts = append(c.modelOpts, o...) }
}

// WithBus injects a shared event bus (default: a fresh one). Use to let several
// agents publish to one observable stream.
func WithBus(b *bus.Bus) Option { return func(c *config) { c.bus = b } }

// WithCheckpointer injects a shared checkpointer (default: in-memory).
func WithCheckpointer(s checkpoint.Checkpointer) Option {
	return func(c *config) { c.store = s }
}

// WithThreadCoordinationRequired rejects checkpointers that cannot serialize
// writers for one thread. The default remains compatible with legacy stores.
func WithThreadCoordinationRequired(required bool) Option {
	return func(c *config) { c.requireThreadCoordination = required }
}

// ToolExecMode selects how a step's tool calls are executed.
type ToolExecMode int

const (
	// ToolParallel runs a step's tool calls concurrently (default).
	ToolParallel ToolExecMode = iota
	// ToolSequential runs them one at a time, in the model's call order.
	ToolSequential
)
