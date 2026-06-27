package agent

import (
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// config is the unexported settings bag an Agent is built from. Users never
// construct it directly; they pass With* Options to New. It is the former Spec,
// turned into functional-options form.
type config struct {
	name        string
	description string
	instruction string
	model       llm.Model
	tools       []tool.Tool
	middleware  []Middleware
	subAgents   []*Agent
	maxSteps    int
	toolExec    ToolExecMode
	modelOpts   []llm.Option
	outputKey   string

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

// WithMaxSteps caps the model<->tool loop (default 16).
func WithMaxSteps(n int) Option { return func(c *config) { c.maxSteps = n } }

// WithToolExecution selects concurrent (default) or sequential tool execution.
func WithToolExecution(m ToolExecMode) Option { return func(c *config) { c.toolExec = m } }

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

// ToolExecMode selects how a step's tool calls are executed.
type ToolExecMode int

const (
	// ToolParallel runs a step's tool calls concurrently (default).
	ToolParallel ToolExecMode = iota
	// ToolSequential runs them one at a time, in the model's call order.
	ToolSequential
)
