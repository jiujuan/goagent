package agent

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

// ErrMaxTurnsExceeded is the terminal error when the loop runs MaxTurns without
// the model producing a tool-call-free reply.
var ErrMaxTurnsExceeded = errors.New("agent: loop exceeded MaxTurns")

const defaultMaxTurns = 16

// AgentLoop is the runtime's controllable loop, an explicit phase machine. One
// step = PrepareTurn (drain steering, BeforeModel) → CallModel (ModifyRequest,
// stream, AfterModel) → ExecuteTools (BeforeTool gate, run, AfterTool) →
// Checkpoint → ApplyDirectives. It publishes internal events to the Bus and
// returns a runOutcome (lifecycle events are the Run wrapper's job).
type AgentLoop struct {
	model       llm.Model
	instruction string
	prompt      *prompt.Builder
	name        string
	description string
	outputKey   string
	modelOpts   []llm.Option
	toolExec    ToolExecMode
	toolTimeout time.Duration
	mw          *Stack
	tools       []tool.Tool
	byName      map[string]tool.Tool
	seqTool     map[string]bool // tools declaring the SequentialTool capability
	schemas     []llm.ToolSchema
	maxTurns    int
}

func newLoop(c config) *AgentLoop {
	ms := c.maxTurns
	if ms <= 0 {
		ms = defaultMaxTurns
	}
	seq := make(map[string]bool, len(c.tools))
	for _, t := range c.tools {
		if s, ok := t.(tool.SequentialTool); ok && s.SequentialExecution() {
			seq[t.Name()] = true
		}
	}
	return &AgentLoop{
		model:       c.model,
		instruction: c.instruction,
		prompt:      c.prompt,
		name:        c.name,
		description: c.description,
		outputKey:   c.outputKey,
		modelOpts:   c.modelOpts,
		toolExec:    c.toolExec,
		toolTimeout: c.toolTimeout,
		mw:          NewStack(c.middleware...),
		tools:       c.tools,
		byName:      tool.ByName(c.tools),
		seqTool:     seq,
		schemas:     tool.Schemas(c.tools),
		maxTurns:    ms,
	}
}

var _ Runnable = (*AgentLoop)(nil)

// addTool registers an extra tool (e.g. the synthetic transfer_to_agent) after
// construction, advertising it to the model.
func (l *AgentLoop) addTool(t tool.Tool) {
	l.byName[t.Name()] = t
	if s, ok := t.(tool.SequentialTool); ok && s.SequentialExecution() {
		l.seqTool[t.Name()] = true
	}
	l.schemas = mergeSchema(l.schemas, tool.SchemaOf(t))
}

// advertised lists the tools offered to the model for one step: the agent's own,
// then any tool the run's middleware injected. An injected name is never listed
// twice; if the agent already had a tool of that name the static slot stays and
// callOne resolves the call against the injected tool instead (see exectools.go).
func (l *AgentLoop) advertised(rc *RunContext) []llm.ToolSchema {
	out := slices.Clone(l.schemas)
	for _, s := range rc.dynamic.advertised() {
		out = mergeSchema(out, s)
	}
	return out
}

// mergeSchema appends s unless a schema of that name is already listed, so a tool
// injected mid-run keeps exactly one advertisement slot across steps.
func mergeSchema(list []llm.ToolSchema, s llm.ToolSchema) []llm.ToolSchema {
	for _, have := range list {
		if have.Name == s.Name {
			return list
		}
	}
	return append(list, s)
}

func (l *AgentLoop) run(rc *RunContext) runOutcome {
	history := append([]core.Message(nil), rc.State.Messages...)

	// A resumed run carries the tool batch its HITL pause left behind. Execute it
	// before consulting the model, so approved calls behave exactly like a batch
	// run inside a step (events, AfterTool, state, directives). A directive from
	// one of them ends the run there, as it would mid-step.
	if rb := rc.resumed; rb != nil {
		rc.resumed = nil
		parts, d := l.runResumed(rc, rb)
		if len(parts) > 0 {
			history = append(history, core.Message{Role: core.RoleTool, Parts: parts})
			rc.State.Messages = history
			l.checkpoint(rc, rb.step, nil)
		}
		if d.Kind != core.Continue {
			return runOutcome{Result: core.Result{Message: rb.final}, Control: d}
		}
	}

	// Render the system prompt once per run: a prompt.Builder (if set) wins over
	// the static instruction. The builder sees the tools, state and identity.
	usePrompt := l.prompt != nil
	system := l.instruction
	if usePrompt {
		s, err := l.prompt.Build(prompt.Context{
			Context:   rc,
			State:     rc.State,
			AgentName: l.name,
			AgentDesc: l.description,
			Tools:     l.tools,
		})
		if err != nil {
			return l.fail(rc, -1, history, err)
		}
		system = s
	}

	for step := 0; step < l.maxTurns; step++ {
		lc := &LoopContext{RunContext: rc, Step: step, MaxTurns: l.maxTurns, History: history}
		rc.publish(core.TurnStarted{Step: step})

		// Phase 1 — PrepareTurn: drain steering, then BeforeModel.
		if steers := rc.steering.drain(); len(steers) > 0 {
			history = append(history, steers...)
		}
		// Let history-compacters (durable compaction) rewrite the working history
		// before the model is consulted. The replacement flows into this step's
		// request and, at step end, its checkpoint. A no-op when no middleware
		// implements HistoryCompacter.
		history = l.mw.CompactHistory(lc, history)
		lc.History = history
		if d, err := l.mw.BeforeModel(lc); err != nil {
			return l.fail(rc, step, history, err)
		} else if out, stop := terminalFromDirective(d, core.Message{}); stop {
			return out
		}

		// Phase 2 — CallModel: ModifyRequest → stream → AfterModel.
		sys := system
		if !usePrompt {
			sys = renderTemplate(l.instruction, rc.State.KV)
		}
		req := &llm.Request{System: sys, Messages: history, Tools: l.advertised(rc)}
		req.Options.Apply(l.modelOpts...)
		lc.Request = req
		if err := l.mw.ModifyRequest(lc, req); err != nil {
			return l.fail(rc, step, history, err)
		}

		// Derive the context for this model call. An observability middleware
		// (ModelContexter) injects its span here so the provider call — and any
		// downstream traceparent — nests under it. With no such middleware,
		// genCtx == rc.Context.
		genCtx := l.mw.ModelContext(lc, rc.Context)

		finalResp, ok, err := l.streamModel(genCtx, rc, lc, req)
		if !ok {
			return l.fail(rc, step, history, err)
		}
		final := finalResp.Message

		if d, err := l.mw.AfterModel(lc, finalResp); err != nil {
			return l.fail(rc, step, history, err)
		} else if out, stop := terminalFromDirective(d, final); stop {
			return out
		}

		history = append(history, final)

		calls := final.ToolCalls()
		if len(calls) == 0 {
			if l.outputKey != "" {
				rc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: l.outputKey, Value: final.Text()})
			}
			rc.State.Messages = history
			l.checkpoint(rc, step, nil)
			rc.publish(core.TurnDone{Step: step})
			return runOutcome{Result: core.Result{Message: final}}
		}

		// A max_tokens stop means the reply was cut off by the output cap, so
		// every tool call in it may carry silently truncated arguments (they can
		// still parse). None are safe to execute — and none reach the BeforeTool
		// gate, so a truncated call never enters HITL approval — instead each
		// becomes an error result asking the model to re-issue it.
		if finalResp.StopReason == llm.StopMaxTokens {
			history = append(history, core.Message{Role: core.RoleTool, Parts: truncatedResults(rc, calls)})
			rc.State.Messages = history
			l.checkpoint(rc, step, nil)
			rc.publish(core.TurnDone{Step: step})
			continue
		}

		// Phase 3 — ExecuteTools: BeforeTool gate (HITL/permission) first.
		for i := range calls {
			d, err := l.mw.BeforeTool(lc, &calls[i])
			if err != nil {
				return l.fail(rc, step, history, err)
			}
			switch d.Kind {
			case core.Interrupt:
				// Persist history (incl. the assistant tool-call message) plus the
				// still-pending calls, so Resume can apply approvals.
				rc.State.Messages = history
				l.checkpoint(rc, step, &checkpoint.PendingHITL{Step: step, Pending: calls[i:]})
				return runOutcome{Control: core.Directive{Kind: core.Interrupt}, Pending: pendingFrom(calls[i:])}
			case core.Stop, core.Escalate, core.Transfer:
				// A gate denied/redirected before any tool ran; end this unit with
				// that control directive.
				rc.State.Messages = history
				l.checkpoint(rc, step, nil)
				return runOutcome{Result: core.Result{Message: final}, Control: d}
			}
		}

		results, dirs := l.execTools(rc, lc, calls)
		history = append(history, core.Message{Role: core.RoleTool, Parts: results})

		// Phase 4 — Checkpoint the step's state.
		rc.State.Messages = history
		l.checkpoint(rc, step, nil)
		rc.publish(core.TurnDone{Step: step})

		// Phase 5 — ApplyDirectives. A tool/AfterTool directive (Stop/Escalate/
		// Transfer) ends this unit and propagates up via the outcome.
		if d := core.Resolve(dirs...); d.Kind != core.Continue {
			return runOutcome{Result: core.Result{Message: final}, Control: d}
		}
	}

	// The budget ran out mid-conversation: the seam is still worth keeping, so
	// Resume can continue the thread with a larger budget instead of rewinding.
	return l.fail(rc, l.maxTurns-1, history, ErrMaxTurnsExceeded)
}

// terminalFromDirective turns a Before/AfterModel directive into a terminal
// outcome, reporting whether the loop should stop.
func terminalFromDirective(d core.Directive, final core.Message) (runOutcome, bool) {
	switch d.Kind {
	case core.Interrupt:
		return runOutcome{Control: core.Directive{Kind: core.Interrupt}}, true
	case core.Stop, core.Escalate, core.Transfer:
		return runOutcome{Result: core.Result{Message: final}, Control: d}, true
	default:
		return runOutcome{}, false
	}
}

// streamModel runs one model call, publishing MessageDelta for partials and
// MessageDone for the final message. ok is false (with err) if the model errored.
// genCtx is the (possibly span-carrying) context to invoke the provider with;
// rc remains the run environment for publishing and state. It returns the final
// *llm.Response (carrying Usage and StopReason) so AfterModel can observe them.
func (l *AgentLoop) streamModel(genCtx context.Context, rc *RunContext, lc *LoopContext, req *llm.Request) (*llm.Response, bool, error) {
	final := &llm.Response{}
	for resp, err := range l.model.Generate(genCtx, req) {
		if err != nil {
			_, _ = l.mw.OnError(lc, err) // retry middleware consulted; real retry lands later
			return nil, false, err
		}
		if resp.Partial {
			rc.publish(core.MessageDelta{Delta: resp.Message})
			continue
		}
		final = resp
		rc.publish(core.MessageDone{Message: resp.Message, Usage: resp.Usage})
	}
	return final, true, nil
}

// checkpoint snapshots the current State for resume/branch/time-travel. A nil
// Store is a no-op.
func (l *AgentLoop) checkpoint(rc *RunContext, step int, pending *checkpoint.PendingHITL) {
	if rc.Store == nil {
		return
	}
	_ = rc.Store.Save(rc, &checkpoint.Checkpoint{
		ID:       core.NewID("cp"),
		ThreadID: rc.ThreadID,
		Step:     step,
		State:    *rc.State,
		Pending:  pending,
	})
}

// fail ends the run on an error, first persisting the conversation up to the last
// point a provider would accept. Without that, the thread's newest snapshot is
// whichever step last succeeded, so a provider failure silently rewinds the run —
// losing this step's steering and any approved HITL batch — and Resume replays from
// there. step < 0 means nothing changed during this run, so nothing is written.
//
// The RunFailed event deliberately stays unchanged: the snapshot belongs to the
// checkpointer, recovery is a call to Agent.Resume(thread), and the error on the
// wire remains a single value.
func (l *AgentLoop) fail(rc *RunContext, step int, history []core.Message, err error) runOutcome {
	if err == nil {
		return runOutcome{}
	}
	if step >= 0 {
		rc.State.Messages = seam(history)
		l.checkpoint(rc, step, nil)
	}
	return runOutcome{Err: err}
}

// seam trims a conversation to its last replayable point: a trailing assistant
// message whose tool calls were never answered is dropped, because handing the
// provider a tool call with no matching result is invalid input. A partial reply
// lost this way is recovered by re-asking, not patched.
func seam(history []core.Message) []core.Message {
	if len(history) == 0 {
		return history
	}
	last := history[len(history)-1]
	if last.Role == core.RoleAssistant && len(last.ToolCalls()) > 0 {
		return history[:len(history)-1]
	}
	return history
}

func pendingFrom(calls []core.ToolCall) []core.ApprovalRequest {
	out := make([]core.ApprovalRequest, len(calls))
	for i, c := range calls {
		out[i] = core.ApprovalRequest{CallID: c.ID, Tool: c.Name, Args: c.Args}
	}
	return out
}

// truncatedResults fails a batch of tool calls from a max_tokens-truncated
// reply, publishing ToolStarted/ToolDone pairs (mirroring execTools' event
// shape) without invoking any handler.
func truncatedResults(rc *RunContext, calls []core.ToolCall) []core.Part {
	parts := make([]core.Part, 0, len(calls))
	for _, c := range calls {
		rc.publish(core.ToolStarted{Call: c})
		tr := core.ToolResult{
			CallID:  c.ID,
			Name:    c.Name,
			IsError: true,
			Content: []core.Part{core.Text{Text: `tool call "` + c.Name + `" was not executed: the reply hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.`}},
		}
		rc.publish(core.ToolDone{Result: tr})
		parts = append(parts, tr)
	}
	return parts
}
