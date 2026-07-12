// Package agent defines the Agent contract and its built-in implementations:
// the LLM-driven turn engine (LLMAgent) and the deterministic workflow agents
// (Sequential, Parallel, Loop). An Agent decides; it does not persist. The
// Runner owns persistence and commits the events an Agent streams.
package agent

import (
	"context"
	"slices"
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// Agent is a streaming decision unit. Run yields a Stream of events for one
// invocation. Composition is expressed through SubAgents, which the workflow
// agents and the transfer mechanism walk.
type Agent interface {
	Name() string
	Description() string
	Run(ictx InvocationContext) core.Stream
	SubAgents() []Agent
}

// InvocationContext carries the per-invocation environment an Agent needs. It
// embeds context.Context so it can be passed directly to model and tool calls.
type InvocationContext struct {
	context.Context

	InvocationID string
	Agent        Agent
	// Root is the top of the agent tree for this run, set by the Runner. It
	// lets any agent locate its parent and peers for transfer direction rules.
	Root    Agent
	Session *session.Session
	// SessionSnapshot is the immutable history/state view this execution unit
	// must read. Workflow agents control when it is refreshed: parallel children
	// share one baseline, while sequential stages refresh between stages.
	SessionSnapshot *session.Snapshot
	Branch          string
	UserContent     core.Message

	// transferDepth counts how many delegations have chained in this run, used
	// to bound runaway agent-to-agent ping-pong.
	transferDepth int
}

// withAgent returns a copy of ictx rebound to a different agent (used when a
// workflow agent runs a sub-agent).
func (ictx InvocationContext) withAgent(a Agent, branch string) InvocationContext {
	ictx.Agent = a
	if branch != "" {
		ictx.Branch = branch
	}
	return ictx
}

// refreshSnapshot returns a copy bound to the Session's latest committed
// revision. The Session remains available for live tool state and persistence;
// model history and prompt sections read from the immutable snapshot.
func (ictx InvocationContext) refreshSnapshot() InvocationContext {
	if ictx.Session == nil {
		ictx.SessionSnapshot = nil
		return ictx
	}
	snapshot := ictx.Session.Snapshot()
	ictx.SessionSnapshot = &snapshot
	return ictx
}

func (ictx InvocationContext) snapshot() session.Snapshot {
	if ictx.SessionSnapshot != nil {
		return *ictx.SessionSnapshot
	}
	if ictx.Session != nil {
		return ictx.Session.Snapshot()
	}
	return session.Snapshot{}
}

// ForSubAgent returns a copy of ictx rebound to run sub, on an optional branch
// (empty keeps the current branch). It is the exported form of withAgent, used
// by out-of-package orchestrators (e.g. the plan package's AgentExecutor) that
// invoke a sub-agent's Run directly.
func (ictx InvocationContext) ForSubAgent(sub Agent, branch string) InvocationContext {
	return ictx.withAgent(sub, branch).refreshSnapshot()
}

// transferTo returns a copy of ictx handed to a transfer target, incrementing
// the delegation depth.
func (ictx InvocationContext) transferTo(target Agent) InvocationContext {
	ictx.Agent = target
	ictx.transferDepth++
	return ictx
}

// parentOf returns the agent in root's tree whose SubAgents contain target, or
// nil if target is the root or not found. Agent names are unique, so name
// equality identifies a node.
func parentOf(root, target Agent) Agent {
	for _, sub := range root.SubAgents() {
		if sub.Name() == target.Name() {
			return root
		}
		if p := parentOf(sub, target); p != nil {
			return p
		}
	}
	return nil
}

// defaultMaxSteps bounds the tool-use loop so a misbehaving model cannot spin
// forever.
const defaultMaxSteps = 16

// Config configures an LLMAgent.
type Config struct {
	Name        string
	Description string
	Model       llm.Model
	Instruction string
	Tools       []tool.Tool
	SubAgents   []Agent

	// Prompt, if set, builds the system prompt from composable sections and
	// takes precedence over Instruction (which is then ignored). Put the base
	// persona in a prompt.Identity section. It is rendered once per invocation.
	Prompt *prompt.Builder

	// OutputKey, if set, writes the agent's final text reply into session state
	// under this key, for inter-agent coordination.
	OutputKey string

	// DisableTransfer turns off the auto-injected transfer_to_agent tool
	// entirely for this agent (no delegation to sub-agents, parent, or peers).
	DisableTransfer bool

	// DisallowTransferToParent prevents this agent from delegating back up to
	// its parent. By default an agent may return control to its parent.
	DisallowTransferToParent bool

	// DisallowTransferToPeers prevents this agent from delegating sideways to
	// its siblings. By default an agent may transfer to peers when its parent
	// is itself delegation-capable.
	DisallowTransferToPeers bool

	// MaxSteps caps the model<->tool loop (default 16).
	MaxSteps int

	// ModelOptions are applied to every model request.
	ModelOptions []llm.Option

	// Middleware runs before each model call, in order (e.g. context
	// compaction). It may rewrite the request.
	Middleware []middleware.Middleware
}

// LLMAgent is the LLM-driven turn engine. Each Run repeatedly calls the model,
// executes any requested tools (concurrently, but preserving the model's call
// order in the transcript), and loops until the model replies without tool
// calls or MaxSteps is reached.
type LLMAgent struct {
	cfg      Config
	maxSteps int
}

// New constructs an LLMAgent, the default LLM-driven agent.
func New(cfg Config) *LLMAgent {
	ms := cfg.MaxSteps
	if ms <= 0 {
		ms = defaultMaxSteps
	}
	return &LLMAgent{cfg: cfg, maxSteps: ms}
}

func (a *LLMAgent) Name() string        { return a.cfg.Name }
func (a *LLMAgent) Description() string { return a.cfg.Description }
func (a *LLMAgent) SubAgents() []Agent  { return a.cfg.SubAgents }

// Run implements Agent.
func (a *LLMAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		// Direct Agent callers may not have gone through Runner. Bind one snapshot
		// here so model history and prompt sections still read the same revision.
		if ictx.SessionSnapshot == nil {
			ictx = ictx.refreshSnapshot()
		}
		// Decorate the model with the configured middleware chain once per run
		// (compaction, RAG, retry, rate limit, steering, ...).
		model := middleware.Chain(a.cfg.Model, a.cfg.Middleware...)

		// Seed working history from committed session messages. The agent keeps
		// its own copy and appends in-turn messages locally, so correctness does
		// not depend on when the Runner commits.
		history := slices.Clone(ictx.snapshot().Messages())

		// Advertise the agent's own tools, plus a synthetic transfer tool when
		// delegation is enabled and at least one target (sub-agent, parent, or
		// peer) is allowed by the direction rules.
		schemas := tool.Schemas(a.cfg.Tools)
		byName := tool.ByName(a.cfg.Tools)
		transferTargets := a.transferableTargets(ictx)
		if len(transferTargets) > 0 {
			tt := newTransferTool(a.orderedTargets(ictx, transferTargets))
			schemas = append(schemas, llm.ToolSchema{Name: tt.Name(), Description: tt.Description(), Parameters: tt.Schema()})
			byName[tt.Name()] = tt
		}

		// Render the system prompt once per invocation: env/state don't change
		// mid-turn. A configured Prompt builder wins over the static Instruction.
		system := a.cfg.Instruction
		if a.cfg.Prompt != nil {
			s, err := a.cfg.Prompt.Build(a.promptContext(ictx))
			if err != nil {
				yield(&core.Event{Author: a.cfg.Name, InvocationID: ictx.InvocationID, Err: err}, err)
				return
			}
			system = s
		}

		for range a.maxSteps {
			req := &llm.Request{
				System:   system,
				Messages: history,
				Tools:    schemas,
			}
			req.Options.Apply(a.cfg.ModelOptions...)

			final, ok := a.streamModel(ictx, model, req, yield)
			if !ok {
				return // consumer stopped or model errored (event already sent)
			}
			history = append(history, final)

			calls := final.ToolCalls()
			if len(calls) == 0 {
				a.maybeEmitOutput(ictx, final, yield)
				return
			}

			results, acts := a.execTools(ictx, byName, calls)
			toolMsg := core.Message{Role: core.RoleTool, Parts: results}
			history = append(history, toolMsg)

			toolEvt := a.event(ictx, &toolMsg, false, nil)
			toolEvt.Actions = core.Actions{StateDelta: acts.StateDelta, Escalate: acts.Escalate, Stop: acts.Stop}
			if !yield(toolEvt, nil) {
				return
			}

			// LLM-driven delegation: hand control to the requested agent, if it
			// is an allowed target (sub-agent, parent, or peer per the rules).
			if acts.TransferToAgent != "" {
				if target := transferTargets[acts.TransferToAgent]; target != nil {
					if ictx.transferDepth >= maxTransferDepth {
						return // bound runaway delegation chains
					}
					core.Pipe(target.Run(ictx.transferTo(target).refreshSnapshot()), yield)
					return
				}
				// Unknown or disallowed target: fall through; the model retries.
			}
			if acts.Stop {
				return
			}
		}
	}
}

// streamModel runs one model call, forwarding every response as an event and
// returning the final (non-partial) assistant message. The bool reports whether
// to continue (false on consumer-stop or model error).
func (a *LLMAgent) streamModel(ictx InvocationContext, model llm.Model, req *llm.Request, yield func(*core.Event, error) bool) (core.Message, bool) {
	var final core.Message
	for resp, err := range model.Generate(ictx, req) {
		if err != nil {
			yield(&core.Event{Author: a.cfg.Name, InvocationID: ictx.InvocationID, Err: err}, err)
			return core.Message{}, false
		}
		msg := resp.Message
		if !yield(a.event(ictx, &msg, resp.Partial, resp.Usage), nil) {
			return core.Message{}, false
		}
		if !resp.Partial {
			final = resp.Message
		}
	}
	return final, true
}

// execTools invokes the requested tools concurrently, returning their results
// in the model's original call order plus the merged side effects each tool
// requested via its Context.Actions.
func (a *LLMAgent) execTools(ictx InvocationContext, byName map[string]tool.Tool, calls []core.ToolCall) ([]core.Part, core.Actions) {
	results := make([]core.Part, len(calls))
	actions := make([]*core.Actions, len(calls))
	var wg sync.WaitGroup
	for i, c := range calls {
		t, ok := byName[c.Name]
		if !ok {
			results[i] = core.ToolResult{
				CallID: c.ID, Name: c.Name, IsError: true,
				Content: []core.Part{core.Text{Text: "unknown tool: " + c.Name}},
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			acts := &core.Actions{}
			actions[i] = acts
			tctx := &tool.Context{
				Context: ictx,
				State:   ictx.Session.State(),
				Actions: acts,
				CallID:  c.ID,
			}
			res, err := t.Call(tctx, c.Args)
			if err != nil {
				results[i] = core.ToolResult{
					CallID: c.ID, Name: c.Name, IsError: true,
					Content: []core.Part{core.Text{Text: err.Error()}},
				}
				return
			}
			results[i] = core.ToolResult{
				CallID: c.ID, Name: c.Name,
				Content: res.Content, IsError: res.IsError,
			}
		}()
	}
	wg.Wait()
	return results, mergeActions(actions)
}

// mergeActions folds the per-tool Actions into one. StateDelta entries are
// merged; TransferToAgent takes the last non-empty value; Escalate/Stop are
// OR-ed.
func mergeActions(list []*core.Actions) core.Actions {
	var m core.Actions
	for _, a := range list {
		if a == nil {
			continue
		}
		for k, v := range a.StateDelta {
			if m.StateDelta == nil {
				m.StateDelta = map[string]any{}
			}
			m.StateDelta[k] = v
		}
		if a.TransferToAgent != "" {
			m.TransferToAgent = a.TransferToAgent
		}
		m.Escalate = m.Escalate || a.Escalate
		m.Stop = m.Stop || a.Stop
	}
	return m
}

func (a *LLMAgent) maybeEmitOutput(ictx InvocationContext, final core.Message, yield func(*core.Event, error) bool) {
	if a.cfg.OutputKey == "" {
		return
	}
	yield(&core.Event{
		ID:           core.NewID("evt"),
		InvocationID: ictx.InvocationID,
		Author:       a.cfg.Name,
		Actions:      core.Actions{StateDelta: map[string]any{a.cfg.OutputKey: final.Text()}},
	}, nil)
}

func (a *LLMAgent) event(ictx InvocationContext, msg *core.Message, partial bool, usage *core.Usage) *core.Event {
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: ictx.InvocationID,
		Author:       a.cfg.Name,
		Branch:       ictx.Branch,
		Message:      msg,
		Partial:      partial,
		Usage:        usage,
	}
}

var _ Agent = (*LLMAgent)(nil)

// promptContext builds the prompt.Context DTO from the invocation, decoupling
// the prompt package from agent (it never sees InvocationContext directly).
func (a *LLMAgent) promptContext(ictx InvocationContext) prompt.Context {
	peers := make([]prompt.Peer, len(a.cfg.SubAgents))
	for i, sub := range a.cfg.SubAgents {
		peers[i] = prompt.Peer{Name: sub.Name(), Description: sub.Description()}
	}
	return prompt.Context{
		Context:         ictx,
		Session:         ictx.Session,
		SessionSnapshot: ictx.SessionSnapshot,
		UserContent:     ictx.UserContent,
		AgentName:       a.cfg.Name,
		AgentDesc:       a.cfg.Description,
		Tools:           a.cfg.Tools,
		SubAgents:       peers,
	}
}
