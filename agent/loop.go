package agent

import (
	"context"
	"errors"
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

// ErrCheckpointUnavailable is returned when a path that promises a recoverable
// pause has no checkpointer. Ordinary in-memory work may omit persistence, but
// an Interrupted event must always correspond to a saved checkpoint.
var ErrCheckpointUnavailable = errors.New("agent: checkpoint store is required for a recoverable pause")

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
	schemas     []llm.ToolSchema
	maxTurns    int

	stepSnapshotPolicy StepSnapshotPolicy
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
		toolTimeout: c.toolTimeout,
		mw:          NewStack(c.middleware...),
		tools:       c.tools,
		byName:      tool.ByName(c.tools),
		schemas:     tool.Schemas(c.tools),
		maxTurns:    ms,

		stepSnapshotPolicy: c.stepSnapshotPolicy,
	}
}

var _ Runnable = (*AgentLoop)(nil)

// addTool registers an extra tool (e.g. the synthetic transfer_to_agent) after
// construction, advertising it to the model. A name registered twice keeps one
// slot and the last implementation.
func (l *AgentLoop) addTool(t tool.Tool) {
	l.byName[t.Name()] = t
	l.schemas = upsertSchema(l.schemas, tool.SchemaOf(t))
}

// taskState is one task's execution state. The step budget is fixed when the
// task starts, so every step — including one that continues a resumed batch —
// shares the same maxStep.
type taskState struct {
	step      int
	maxStep   int
	history   []core.Message
	system    string
	usePrompt bool
}

func (l *AgentLoop) run(rc *RunContext) (out runOutcome) {
	if rc.topLevel {
		// This context owns the queue. Once the task loop returns, no step can
		// deliver a message, so close it with the run's ending reason. The close
		// is idempotent, so an earlier close keeps its own reason.
		defer func() { rc.steering.close(out.inputCloseReason()) }()
	}
	st, prep, ok := l.prepareRun(rc)
	if !ok {
		return prep
	}
	return l.runTask(rc, st)
}

// prepareRun does the work before the first step: replay a resumed tool batch,
// render the system prompt, and fix the step budget. ok is false when the
// resumed batch already ended the run.
func (l *AgentLoop) prepareRun(rc *RunContext) (taskState, runOutcome, bool) {
	history := append([]core.Message(nil), rc.State.Messages...)
	startStep := 0

	// A resumed run carries the tool batch its HITL pause left behind. Execute it
	// before consulting the model, so approved calls behave exactly like a batch
	// run inside a step (events, AfterTool, state, directives). A directive from
	// one of them ends the run there, as it would mid-step.
	if rb := rc.resumed; rb != nil {
		rc.resumed = nil
		rlc := &LoopContext{RunContext: rc, Step: rb.step, MaxTurns: l.maxTurns, History: history}
		rlc.view = l.resumeStepView(rb)
		if err := l.mw.CheckToolPolicy(rlc, rb.calls); err != nil {
			return taskState{}, l.fail(rc, rb.step, history, rb.snapshot, err), false
		}
		resumed := l.runResumed(rb, rlc)
		// The rejected calls in a resumed batch are reported here rather than inside
		// runResumed, so every phase ordering stays in the loop. Same position as a
		// step's: batch joined, snapshot not yet written.
		l.mw.ToolReject(rlc, resumed.rejects)
		if resumed.pause {
			return taskState{}, l.interrupt(rc, rb.step, history, rb.snapshot, rb.calls, "before_tool", resumed.reason, checkpoint.RecoveryResumeTools), false
		}
		if len(resumed.parts) > 0 {
			history = append(history, core.Message{Role: core.RoleTool, Parts: resumed.parts})
		}
		if resumed.err != nil {
			return taskState{}, l.fail(rc, rb.step, history, rb.snapshot, resumed.err), false
		}
		if len(resumed.parts) > 0 || resumed.control.Kind != core.Continue {
			rc.State.Messages = history
			if err := l.checkpoint(rc, commitOptions{
				Step:     rb.step,
				Snapshot: rb.snapshot,
				Release:  resumed.control.Kind != core.Continue,
			}); err != nil {
				return taskState{}, runOutcome{Err: err}, false
			}
		}
		if resumed.control.Kind != core.Continue {
			return taskState{}, runOutcome{Result: core.Result{Message: rb.final}, Control: resumed.control}, false
		}
		// The recovered batch belongs to rb.step. Start fresh model work at the
		// next logical step so a provider that reuses a tool-call ID cannot replay
		// the batch's durable result.
		startStep = rb.step + 1
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
			return taskState{}, l.fail(rc, -1, history, nil, err), false
		}
		system = s
	}

	return taskState{
		step:      startStep,
		maxStep:   startStep + l.maxTurns,
		history:   history,
		system:    system,
		usePrompt: usePrompt,
	}, runOutcome{}, true
}

// runTask drives steps until one ends the task or the budget runs out.
func (l *AgentLoop) runTask(rc *RunContext, st taskState) runOutcome {
	for st.step < st.maxStep {
		out, next, again := l.runStep(rc, st)
		if !again {
			return out
		}
		st = next
	}
	// The budget ran out mid-conversation: the seam is still worth keeping, so
	// Resume can continue the thread with a larger budget instead of rewinding.
	return l.fail(rc, st.maxStep-1, st.history, nil, ErrMaxTurnsExceeded)
}

// settleTurn decides whether a final answer ends the run or another turn starts
// because input arrived while the model was answering. The order is deliberate:
// a spent budget ends the turn before anything can be queued for a step that
// will never run; volatile messages force the next turn without touching the
// (possibly slow) inbox; and the last drain-or-close is one atomic step, so a
// message racing with the close is either delivered or refused, never stranded.
func (l *AgentLoop) settleTurn(rc *RunContext, st taskState, history []core.Message) (nextHistory []core.Message, entries []steerEntry, continuing bool, err error) {
	if st.step+1 >= st.maxStep {
		settleUndelivered(rc.steering.drain(), ErrMaxTurnsExceeded)
		rc.steering.close(ErrRunClosed)
		return history, nil, false, nil
	}
	if entries := rc.steering.drain(); len(entries) > 0 {
		for _, e := range entries {
			history = append(history, e.msg)
		}
		return history, entries, true, nil
	}
	if durable, ok := rc.durableStore(); ok {
		inbox, err := durable.Inbox(rc, rc.ThreadID)
		if err != nil {
			return history, nil, false, err
		}
		pending := rc.pendingInboxAcks()
		for _, msg := range inbox {
			if _, consumed := pending[msg.ID]; consumed {
				continue
			}
			// The next turn's PrepareTurn delivers it and registers the ack.
			return history, nil, true, nil
		}
	}
	late, _ := rc.steering.drainOrClose(ErrRunClosed)
	if len(late) > 0 {
		for _, e := range late {
			history = append(history, e.msg)
		}
		return history, late, true, nil
	}
	return history, nil, false, nil
}

// runStep runs one PrepareTurn → CallModel → ExecuteTools → Checkpoint →
// ApplyDirectives step. again reports whether the task has another step, with
// next carrying the advanced state when it does.
func (l *AgentLoop) runStep(rc *RunContext, st taskState) (runOutcome, taskState, bool) {
	step := st.step
	history := st.history
	system := st.system
	usePrompt := st.usePrompt

	lc := &LoopContext{RunContext: rc, Step: step, MaxTurns: l.maxTurns, History: history}
	rc.publish(core.TurnStarted{Step: step})

	// Phase 1 — PrepareTurn: drain volatile steering, then the durable inbox.
	if steers := rc.steering.drain(); len(steers) > 0 {
		for _, e := range steers {
			history = append(history, e.msg)
		}
		settleDelivered(steers, step)
	}
	if durable, ok := rc.durableStore(); ok {
		inbox, err := durable.Inbox(rc, rc.ThreadID)
		if err != nil {
			return l.fail(rc, step, history, nil, err), st, false
		}
		if len(inbox) > 0 {
			ids := make([]string, 0, len(inbox))
			for _, msg := range inbox {
				history = append(history, msg.Message)
				ids = append(ids, msg.ID)
			}
			rc.addInboxAcks(ids)
		}
	}
	// Let history-compacters (durable compaction) rewrite the working history
	// before the model is consulted. The replacement flows into this step's
	// request and, at step end, its checkpoint. A no-op when no middleware
	// implements HistoryCompacter.
	history = l.mw.CompactHistory(lc, history)
	lc.History = history
	if d, err := l.mw.BeforeModel(lc); err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	} else {
		switch d.Kind {
		case core.Interrupt:
			return l.interrupt(rc, step, history, lc.viewSnapshot(), nil, "before_model", d.Reason, checkpoint.RecoveryReplayModel), st, false
		case core.Stop, core.Escalate, core.Transfer:
			rc.State.Messages = history
			if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: true}); err != nil {
				return runOutcome{Err: err}, st, false
			}
			return runOutcome{Control: d}, st, false
		}
	}

	// Phase 2 — CallModel: ModifyRequest → stream → AfterModel.
	sys := system
	if !usePrompt {
		sys = renderTemplate(l.instruction, rc.State.KV)
	}
	req := &llm.Request{System: sys, Messages: history, Tools: l.snapshotTools(rc).schemas}
	req.Options.Apply(l.modelOpts...)
	lc.Request = req
	if err := l.mw.ModifyRequest(lc, req); err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	}
	view, err := l.freezeStepView(lc, req)
	if err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	}
	lc.view = view
	if err := l.checkReplayDrift(lc, view); err != nil {
		return l.fail(rc, step, history, view.snapshot(), err), st, false
	}

	// Derive the context for this model call. An observability middleware
	// (ModelContexter) injects its span here so the provider call — and any
	// downstream traceparent — nests under it. With no such middleware,
	// genCtx == rc.Context.
	genCtx := l.mw.ModelContext(lc, rc.Context)

	finalResp, ok, err := l.streamModel(genCtx, rc, lc, req)
	if !ok {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	}
	final := finalResp.Message

	if d, err := l.mw.AfterModel(lc, finalResp); err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	} else {
		switch d.Kind {
		case core.Interrupt:
			// Persist the model-request boundary, not final: a resumed run must
			// re-ask the model rather than continue an uncheckpointed reply.
			return l.interrupt(rc, step, history, lc.viewSnapshot(), nil, "after_model", d.Reason, checkpoint.RecoveryReplayModel), st, false
		case core.Stop, core.Escalate, core.Transfer:
			history = append(history, final)
			rc.State.Messages = history
			if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: true}); err != nil {
				return runOutcome{Err: err}, st, false
			}
			return runOutcome{Result: core.Result{Message: final}, Control: d}, st, false
		}
	}

	history = append(history, final)

	calls := final.ToolCalls()
	if len(calls) == 0 {
		if l.outputKey != "" {
			rc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: l.outputKey, Value: final.Text()})
		}

		if !rc.topLevel {
			// A workflow member keeps single-turn semantics: its final answer
			// returns from this unit, and the shared queue stays open for the next
			// member or loop round.
			rc.State.Messages = history
			if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: true}); err != nil {
				return runOutcome{Err: err}, st, false
			}
			rc.publish(core.TurnDone{Step: step})
			return runOutcome{Result: core.Result{Message: final}}, st, false
		}

		nextHistory, entries, continuing, err := l.settleTurn(rc, st, history)
		if err != nil {
			return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
		}
		rc.State.Messages = nextHistory
		if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: !continuing}); err != nil {
			settleUndelivered(entries, ErrRunClosed)
			return runOutcome{Err: err}, st, false
		}
		if continuing {
			settleDelivered(entries, step+1)
		}
		rc.publish(core.TurnDone{Step: step})
		if continuing {
			return runOutcome{}, taskState{
				step:      step + 1,
				maxStep:   st.maxStep,
				history:   nextHistory,
				system:    st.system,
				usePrompt: st.usePrompt,
			}, true
		}
		return runOutcome{Result: core.Result{Message: final}}, st, false
	}

	// A max_tokens stop means the reply was cut off by the output cap, so
	// every tool call in it may carry silently truncated arguments (they can
	// still parse). None are safe to execute — and none reach the BeforeTool
	// gate, so a truncated call never enters HITL approval — instead each
	// becomes an error result asking the model to re-issue it.
	if finalResp.StopReason == llm.StopMaxTokens {
		history = append(history, core.Message{Role: core.RoleTool, Parts: truncatedResults(rc, calls)})
		rc.State.Messages = history
		if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot()}); err != nil {
			return runOutcome{Err: err}, st, false
		}
		rc.publish(core.TurnDone{Step: step})
		return runOutcome{}, taskState{
			step:      step + 1,
			maxStep:   st.maxStep,
			history:   history,
			system:    st.system,
			usePrompt: st.usePrompt,
		}, true
	}

	// Phase 3 — ExecuteTools: BeforeTool gate (HITL/permission) first, then the
	// batch, then the rejections it collected. Reporting them after the batch joined
	// (not from a tool's worker) and before this step's snapshot is what lets a
	// ToolRejecter write State.KV without locking and have the write checkpointed.
	if err := l.mw.CheckToolPolicy(lc, calls); err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	}
	for i := range calls {
		d, err := l.mw.BeforeTool(lc, &calls[i])
		if err != nil {
			return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
		}
		switch d.Kind {
		case core.Interrupt:
			// No handler has started yet. Save the whole original batch: calls
			// checked before this gate were only preflighted, not executed.
			return l.interrupt(rc, step, history, lc.viewSnapshot(), calls, "before_tool", d.Reason, checkpoint.RecoveryResumeTools), st, false
		case core.Stop, core.Escalate, core.Transfer:
			// A gate denied/redirected before any tool ran; end this unit with
			// that control directive.
			rc.State.Messages = history
			if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: true}); err != nil {
				return runOutcome{Err: err}, st, false
			}
			return runOutcome{Result: core.Result{Message: final}, Control: d}, st, false
		}
	}

	results, dirs, rejects, execErr := l.execTools(rc, lc, calls)
	l.mw.ToolReject(lc, rejects)
	if len(results) > 0 {
		history = append(history, core.Message{Role: core.RoleTool, Parts: results})
	}
	if execErr != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), execErr), st, false
	}

	// Phase 4 — Checkpoint the step's state.
	rc.State.Messages = history
	next := core.Resolve(dirs...)
	if err := l.checkpoint(rc, commitOptions{Step: step, Snapshot: lc.viewSnapshot(), Release: next.Kind != core.Continue}); err != nil {
		return runOutcome{Err: err}, st, false
	}
	rc.publish(core.TurnDone{Step: step})

	// Phase 5 — ApplyDirectives. A tool/AfterTool directive (Stop/Escalate/
	// Transfer) ends this unit and propagates up via the outcome.
	if next.Kind != core.Continue {
		return runOutcome{Result: core.Result{Message: final}, Control: next}, st, false
	}

	return runOutcome{}, taskState{
		step:      step + 1,
		maxStep:   st.maxStep,
		history:   history,
		system:    st.system,
		usePrompt: st.usePrompt,
	}, true
}

// streamModel runs one model call, publishing MessageDelta for partials and
// MessageDone for the final message. ok is false (with err) if the model errored.
// genCtx is the (possibly span-carrying) context to invoke the provider with;
// rc remains the run environment for publishing and state. It returns the final
// *llm.Response (carrying Usage and StopReason) so AfterModel can observe them.
func (l *AgentLoop) streamModel(genCtx context.Context, rc *RunContext, lc *LoopContext, req *llm.Request) (*llm.Response, bool, error) {
	if err := rc.checkWriteAccess(); err != nil {
		return nil, false, err
	}
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

// commitOptions is one checkpoint write. Snapshot is the frozen step view the
// write belongs to, or nil when the event happened before a view existed (or
// where no view describes the write, such as a resumed legacy batch).
type commitOptions struct {
	Step       int
	Pending    *checkpoint.PendingHITL
	Pause      *checkpoint.Pause
	Snapshot   *checkpoint.StepSnapshot
	Release    bool
	Executions []checkpoint.ToolExecution
}

// checkpoint snapshots the current State for resume/branch/time-travel. A nil
// Store is allowed for ordinary best-effort computation, but never for a
// recoverable pause.
func (l *AgentLoop) checkpoint(rc *RunContext, opts commitOptions) error {
	if rc.Store == nil {
		if opts.Pending != nil || opts.Pause != nil {
			return ErrCheckpointUnavailable
		}
		return nil
	}
	return rc.commitCheckpoint(&checkpoint.Checkpoint{
		ID:       core.NewID("cp"),
		ThreadID: rc.ThreadID,
		Step:     opts.Step,
		State:    *rc.State,
		Pending:  opts.Pending,
		Pause:    opts.Pause,
		Snapshot: opts.Snapshot,
	}, opts.Release, opts.Executions...)
}

func (l *AgentLoop) interrupt(rc *RunContext, step int, history []core.Message, snapshot *checkpoint.StepSnapshot, pending []core.ToolCall, phase, reason, recovery string) runOutcome {
	rc.State.Messages = history
	var hitl *checkpoint.PendingHITL
	if len(pending) > 0 {
		hitl = &checkpoint.PendingHITL{Step: step, Pending: pending}
	}
	pause := &checkpoint.Pause{ID: core.NewID("pause"), Phase: phase, Reason: reason, Recovery: recovery}
	if err := l.checkpoint(rc, commitOptions{
		Step:       step,
		Pending:    hitl,
		Pause:      pause,
		Snapshot:   snapshot,
		Release:    true,
		Executions: pendingExecutions(rc.ThreadID, step, pending),
	}); err != nil {
		return runOutcome{Err: err}
	}
	return runOutcome{
		Control:  core.Directive{Kind: core.Interrupt},
		Pending:  pendingFrom(pending),
		Phase:    phase,
		Reason:   reason,
		Recovery: recovery,
	}
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
func (l *AgentLoop) fail(rc *RunContext, step int, history []core.Message, snapshot *checkpoint.StepSnapshot, err error) runOutcome {
	if err == nil {
		return runOutcome{}
	}
	if step >= 0 {
		rc.State.Messages = seam(history)
		if checkpointErr := l.checkpoint(rc, commitOptions{Step: step, Snapshot: snapshot, Release: true}); checkpointErr != nil {
			err = errors.Join(err, checkpointErr)
		}
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
//
// It reports no ToolRejection on purpose. Nothing was wrong with the call — the
// reply was cut off before it finished — so counting it against a tool's argument
// quality would let an output-cap problem trip an argument guard.
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
