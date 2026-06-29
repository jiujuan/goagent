package agent

import (
	"context"
	"errors"

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
	mw          *Stack
	tools       []tool.Tool
	byName      map[string]tool.Tool
	schemas     []llm.ToolSchema
	maxTurns    int
}

func newLoop(c config) *AgentLoop {
	ms := c.maxTurns
	if ms <= 0 {
		ms = defaultMaxTurns
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
		mw:          NewStack(c.middleware...),
		tools:       c.tools,
		byName:      tool.ByName(c.tools),
		schemas:     tool.Schemas(c.tools),
		maxTurns:    ms,
	}
}

var _ Runnable = (*AgentLoop)(nil)

// addTool registers an extra tool (e.g. the synthetic transfer_to_agent) after
// construction, advertising it to the model.
func (l *AgentLoop) addTool(t tool.Tool) {
	l.byName[t.Name()] = t
	l.schemas = append(l.schemas, llm.ToolSchema{
		Name:        t.Name(),
		Description: t.Description(),
		Parameters:  t.Schema(),
	})
}

func (l *AgentLoop) run(rc *RunContext) runOutcome {
	history := append([]core.Message(nil), rc.State.Messages...)

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
			return runOutcome{Err: err}
		}
		system = s
	}

	for step := 0; step < l.maxTurns; step++ {
		lc := &LoopContext{RunContext: rc, Step: step, History: history}
		rc.publish(core.TurnStarted{Step: step})

		// Phase 1 — PrepareTurn: drain steering, then BeforeModel.
		if steers := rc.steering.drain(); len(steers) > 0 {
			history = append(history, steers...)
			lc.History = history
		}
		if d, err := l.mw.BeforeModel(lc); err != nil {
			return runOutcome{Err: err}
		} else if out, stop := terminalFromDirective(d, core.Message{}); stop {
			return out
		}

		// Phase 2 — CallModel: ModifyRequest → stream → AfterModel.
		sys := system
		if !usePrompt {
			sys = renderTemplate(l.instruction, rc.State.KV)
		}
		req := &llm.Request{System: sys, Messages: history, Tools: l.schemas}
		req.Options.Apply(l.modelOpts...)
		lc.Request = req
		if err := l.mw.ModifyRequest(lc, req); err != nil {
			return runOutcome{Err: err}
		}

		// Derive the context for this model call. An observability middleware
		// (ModelContexter) injects its span here so the provider call — and any
		// downstream traceparent — nests under it. With no such middleware,
		// genCtx == rc.Context.
		genCtx := l.mw.ModelContext(lc, rc.Context)

		finalResp, ok, err := l.streamModel(genCtx, rc, lc, req)
		if !ok {
			return runOutcome{Err: err}
		}
		final := finalResp.Message

		if d, err := l.mw.AfterModel(lc, finalResp); err != nil {
			return runOutcome{Err: err}
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

		// Phase 3 — ExecuteTools: BeforeTool gate (HITL/permission) first.
		for i := range calls {
			d, err := l.mw.BeforeTool(lc, &calls[i])
			if err != nil {
				return runOutcome{Err: err}
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

	return runOutcome{Err: ErrMaxTurnsExceeded}
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

func pendingFrom(calls []core.ToolCall) []core.ApprovalRequest {
	out := make([]core.ApprovalRequest, len(calls))
	for i, c := range calls {
		out[i] = core.ApprovalRequest{CallID: c.ID, Tool: c.Name, Args: c.Args}
	}
	return out
}
