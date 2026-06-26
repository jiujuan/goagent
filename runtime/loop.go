package runtime

import (
	"errors"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/event"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// ErrMaxStepsExceeded is the terminal error when the loop runs MaxSteps without
// the model producing a tool-call-free reply.
var ErrMaxStepsExceeded = errors.New("runtime: agent loop exceeded MaxSteps")

const defaultMaxSteps = 16

// AgentLoop is v2's controllable loop, expressed as an explicit phase machine.
// One step = PrepareTurn (BeforeModel) → CallModel (ModifyRequest, stream,
// AfterModel) → ExecuteTools (BeforeTool gate, run, AfterTool) → ApplyDirectives.
// Each phase publishes observational events to the run's Bus.
type AgentLoop struct {
	spec     AgentSpec
	mw       *Stack
	byName   map[string]tool.Tool
	schemas  []llm.ToolSchema
	maxSteps int
}

func newLoop(spec AgentSpec) *AgentLoop {
	ms := spec.Loop.MaxSteps
	if ms <= 0 {
		ms = defaultMaxSteps
	}
	return &AgentLoop{
		spec:     spec,
		mw:       NewStack(spec.Middleware...),
		byName:   tool.ByName(spec.Tools),
		schemas:  tool.Schemas(spec.Tools),
		maxSteps: ms,
	}
}

// Drive runs an agent loop synchronously, publishing observational events to
// rc.Bus on rc.Topic. It is the step-3 entry point; the non-blocking
// Runtime/Agent/Run wrapper (Start/Resume, checkpointing, steering) arrives in
// later ADR-0023 steps.
func Drive(rc *RunContext, spec AgentSpec) {
	newLoop(spec).run(rc)
}

func (l *AgentLoop) run(rc *RunContext) {
	pub := func(e event.Event) { rc.Bus.Publish(rc.Topic, e) }
	pub(event.RunStarted{RunID: rc.RunID, ThreadID: rc.ThreadID})

	history := append([]core.Message(nil), rc.State.Messages...)
	system := l.spec.Instruction

	for step := 0; step < l.maxSteps; step++ {
		lc := &LoopContext{RunContext: rc, Step: step, History: history}
		pub(event.TurnStarted{Step: step})

		// Phase 1 — PrepareTurn: BeforeModel.
		if d, err := l.mw.BeforeModel(lc); err != nil {
			pub(event.RunFailed{Err: err})
			return
		} else if l.handleTerminal(rc, d) {
			return
		}

		// Phase 2 — CallModel: ModifyRequest → stream → AfterModel.
		req := &llm.Request{System: system, Messages: history, Tools: l.schemas}
		req.Options.Apply(l.spec.modelOptions()...)
		lc.Request = req
		if err := l.mw.ModifyRequest(lc, req); err != nil {
			pub(event.RunFailed{Err: err})
			return
		}

		final, ok := l.streamModel(rc, lc, req)
		if !ok {
			return // model errored; RunFailed already published
		}

		if d, err := l.mw.AfterModel(lc, &llm.Response{Message: final}); err != nil {
			pub(event.RunFailed{Err: err})
			return
		} else if l.handleTerminal(rc, d) {
			return
		}

		history = append(history, final)

		calls := final.ToolCalls()
		if len(calls) == 0 {
			pub(event.TurnDone{Step: step})
			pub(event.RunDone{Result: event.Result{Message: final}})
			return
		}

		// Phase 3 — ExecuteTools: BeforeTool gate (HITL/permission) first.
		for i := range calls {
			d, err := l.mw.BeforeTool(lc, &calls[i])
			if err != nil {
				pub(event.RunFailed{Err: err})
				return
			}
			if d.Kind == core.Interrupt {
				pub(event.Interrupted{Pending: pendingFrom(calls[i:])})
				return // step 4 will checkpoint the Pending snapshot here
			}
		}

		results, dirs := l.execTools(rc, lc, calls)
		history = append(history, core.Message{Role: core.RoleTool, Parts: results})
		pub(event.TurnDone{Step: step})

		// Phase 5 — ApplyDirectives.
		switch core.Resolve(dirs...).Kind {
		case core.Stop, core.Escalate, core.Transfer:
			// Transfer/Escalate wiring (sub-agent run, enclosing Loop) lands in
			// later steps; for now any of these ends the run cleanly.
			pub(event.RunDone{Result: event.Result{Message: final}})
			return
		}
	}

	pub(event.RunFailed{Err: ErrMaxStepsExceeded})
}

// handleTerminal acts on a directive from a Before/AfterModel phase and reports
// whether the loop should stop.
func (l *AgentLoop) handleTerminal(rc *RunContext, d core.Directive) bool {
	pub := func(e event.Event) { rc.Bus.Publish(rc.Topic, e) }
	switch d.Kind {
	case core.Interrupt:
		pub(event.Interrupted{})
		return true
	case core.Stop, core.Escalate, core.Transfer:
		pub(event.RunDone{})
		return true
	default:
		return false
	}
}

// streamModel runs one model call, publishing MessageDelta for partials and
// MessageDone for the final message. The bool is false if the model errored
// (RunFailed already published).
func (l *AgentLoop) streamModel(rc *RunContext, lc *LoopContext, req *llm.Request) (core.Message, bool) {
	pub := func(e event.Event) { rc.Bus.Publish(rc.Topic, e) }
	var final core.Message
	for resp, err := range l.spec.Model.Generate(rc, req) {
		if err != nil {
			// Consult OnError (retry middleware lives here); a real retry loop
			// arrives with the retry middleware port. For now, errors are terminal.
			_, _ = l.mw.OnError(lc, err)
			pub(event.RunFailed{Err: err})
			return core.Message{}, false
		}
		if resp.Partial {
			pub(event.MessageDelta{Delta: resp.Message})
			continue
		}
		final = resp.Message
		pub(event.MessageDone{Message: resp.Message, Usage: resp.Usage})
	}
	return final, true
}

func pendingFrom(calls []core.ToolCall) []event.ApprovalRequest {
	out := make([]event.ApprovalRequest, len(calls))
	for i, c := range calls {
		out[i] = event.ApprovalRequest{CallID: c.ID, Tool: c.Name, Args: c.Args}
	}
	return out
}

// modelOptions is a hook for per-spec llm options; empty for now (kept so the
// loop body reads the same as v1's request assembly).
func (s AgentSpec) modelOptions() []llm.Option { return nil }
