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
// Checkpoint → ApplyDirectives. Each phase publishes observational events to
// the run's Bus. It is built from a config by newLoop and holds no Spec.
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

func (l *AgentLoop) run(rc *RunContext) {
	rc.publish(core.RunStarted{RunID: rc.RunID, ThreadID: rc.ThreadID})

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
			rc.publish(core.RunFailed{Err: err})
			return
		} else if l.handleTerminal(rc, d) {
			return
		}

		// Phase 2 — CallModel: ModifyRequest → stream → AfterModel.
		req := &llm.Request{System: l.instruction, Messages: history, Tools: l.schemas}
		req.Options.Apply(l.modelOpts...)
		lc.Request = req
		if err := l.mw.ModifyRequest(lc, req); err != nil {
			rc.publish(core.RunFailed{Err: err})
			return
		}

		final, ok := l.streamModel(rc, lc, req)
		if !ok {
			return // model errored; RunFailed already published
		}

		if d, err := l.mw.AfterModel(lc, &llm.Response{Message: final}); err != nil {
			rc.publish(core.RunFailed{Err: err})
			return
		} else if l.handleTerminal(rc, d) {
			return
		}

		history = append(history, final)

		calls := final.ToolCalls()
		if len(calls) == 0 {
			rc.State.Messages = history
			l.checkpoint(rc, step, nil)
			rc.publish(core.TurnDone{Step: step})
			rc.publish(core.RunDone{Result: core.Result{Message: final}})
			return
		}

		// Phase 3 — ExecuteTools: BeforeTool gate (HITL/permission) first.
		for i := range calls {
			d, err := l.mw.BeforeTool(lc, &calls[i])
			if err != nil {
				rc.publish(core.RunFailed{Err: err})
				return
			}
			if d.Kind == core.Interrupt {
				// Persist history including the assistant tool-call message, plus
				// the still-pending calls, so Resume can apply approvals.
				rc.State.Messages = history
				l.checkpoint(rc, step, &checkpoint.PendingHITL{Step: step, Pending: calls[i:]})
				rc.publish(core.Interrupted{Pending: pendingFrom(calls[i:])})
				return
			}
		}

		results, dirs := l.execTools(rc, lc, calls)
		history = append(history, core.Message{Role: core.RoleTool, Parts: results})

		// Phase 4 — Checkpoint the step's state.
		rc.State.Messages = history
		l.checkpoint(rc, step, nil)
		rc.publish(core.TurnDone{Step: step})

		// Phase 5 — ApplyDirectives.
		switch core.Resolve(dirs...).Kind {
		case core.Stop, core.Escalate, core.Transfer:
			// Transfer/Escalate wiring (sub-agent run, enclosing Loop) lands in
			// a later stage; for now any of these ends the run cleanly.
			rc.publish(core.RunDone{Result: core.Result{Message: final}})
			return
		}
	}

	rc.publish(core.RunFailed{Err: ErrMaxStepsExceeded})
}

// handleTerminal acts on a directive from a Before/AfterModel phase and reports
// whether the loop should stop.
func (l *AgentLoop) handleTerminal(rc *RunContext, d core.Directive) bool {
	switch d.Kind {
	case core.Interrupt:
		rc.publish(core.Interrupted{})
		return true
	case core.Stop, core.Escalate, core.Transfer:
		rc.publish(core.RunDone{})
		return true
	default:
		return false
	}
}

// streamModel runs one model call, publishing MessageDelta for partials and
// MessageDone for the final message. The bool is false if the model errored.
func (l *AgentLoop) streamModel(rc *RunContext, lc *LoopContext, req *llm.Request) (core.Message, bool) {
	var final core.Message
	for resp, err := range l.model.Generate(rc, req) {
		if err != nil {
			_, _ = l.mw.OnError(lc, err) // retry middleware consulted; real retry lands later
			rc.publish(core.RunFailed{Err: err})
			return core.Message{}, false
		}
		if resp.Partial {
			rc.publish(core.MessageDelta{Delta: resp.Message})
			continue
		}
		final = resp.Message
		rc.publish(core.MessageDone{Message: resp.Message, Usage: resp.Usage})
	}
	return final, true
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
