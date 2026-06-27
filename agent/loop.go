package agent

import (
	"errors"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// ErrMaxStepsExceeded is the terminal error when the loop runs MaxSteps without
// the model producing a tool-call-free reply.
var ErrMaxStepsExceeded = errors.New("agent: loop exceeded MaxSteps")

const defaultMaxSteps = 16

// AgentLoop is the runtime's controllable loop, an explicit phase machine. One
// step = PrepareTurn (drain steering, BeforeModel) → CallModel (ModifyRequest,
// stream, AfterModel) → ExecuteTools (BeforeTool gate, run, AfterTool) →
// Checkpoint → ApplyDirectives. It publishes internal events to the Bus and
// returns a runOutcome (lifecycle events are the Run wrapper's job).
type AgentLoop struct {
	model       llm.Model
	instruction string
	modelOpts   []llm.Option
	toolExec    ToolExecMode
	mw          *Stack
	byName      map[string]tool.Tool
	schemas     []llm.ToolSchema
	maxSteps    int
}

func newLoop(c config) *AgentLoop {
	ms := c.maxSteps
	if ms <= 0 {
		ms = defaultMaxSteps
	}
	return &AgentLoop{
		model:       c.model,
		instruction: c.instruction,
		modelOpts:   c.modelOpts,
		toolExec:    c.toolExec,
		mw:          NewStack(c.middleware...),
		byName:      tool.ByName(c.tools),
		schemas:     tool.Schemas(c.tools),
		maxSteps:    ms,
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

	for step := 0; step < l.maxSteps; step++ {
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
		req := &llm.Request{System: l.instruction, Messages: history, Tools: l.schemas}
		req.Options.Apply(l.modelOpts...)
		lc.Request = req
		if err := l.mw.ModifyRequest(lc, req); err != nil {
			return runOutcome{Err: err}
		}

		final, ok, err := l.streamModel(rc, lc, req)
		if !ok {
			return runOutcome{Err: err}
		}

		if d, err := l.mw.AfterModel(lc, &llm.Response{Message: final}); err != nil {
			return runOutcome{Err: err}
		} else if out, stop := terminalFromDirective(d, final); stop {
			return out
		}

		history = append(history, final)

		calls := final.ToolCalls()
		if len(calls) == 0 {
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
			if d.Kind == core.Interrupt {
				// Persist history (incl. the assistant tool-call message) plus the
				// still-pending calls, so Resume can apply approvals.
				rc.State.Messages = history
				l.checkpoint(rc, step, &checkpoint.PendingHITL{Step: step, Pending: calls[i:]})
				return runOutcome{Control: core.Directive{Kind: core.Interrupt}, Pending: pendingFrom(calls[i:])}
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

	return runOutcome{Err: ErrMaxStepsExceeded}
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
func (l *AgentLoop) streamModel(rc *RunContext, lc *LoopContext, req *llm.Request) (core.Message, bool, error) {
	var final core.Message
	for resp, err := range l.model.Generate(rc, req) {
		if err != nil {
			_, _ = l.mw.OnError(lc, err) // retry middleware consulted; real retry lands later
			return core.Message{}, false, err
		}
		if resp.Partial {
			rc.publish(core.MessageDelta{Delta: resp.Message})
			continue
		}
		final = resp.Message
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
