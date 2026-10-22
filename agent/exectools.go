package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// execTools runs a step's tool calls and returns their results in the model's
// original call order, plus the directives gathered from each tool's
// Result.Control and the AfterTool middleware. State mutations a tool requests
// (Result.State) are applied to the run State immediately. Calls run in
// segments planned by buildToolCallSegments: consecutive calls run concurrently
// (the default WithToolExecution), while ToolSequential makes every call serial
// and a tool declaring the SequentialTool capability (tool.AsSequential) gives
// its own call a serial segment — consecutive calls around it still run
// concurrently within their own segments.
//
// ToolStarted is published in call order; ToolDone as each result lands, and a
// serial call's ToolDone is published before the next segment's ToolStarted.
// The Bus is concurrency-safe, so parallel publishes do not race. State
// mutations from parallel tools are serialized under a mutex.
//
// A call bounded by WithToolTimeout (or by a middleware context bound) stops
// being waited for when its deadline passes: the loop reports a timeout result
// and moves on. The tool's late result, Control and State ops are then dropped,
// so its external side effects may continue unseen — see abandonedAtBound.
//
// The third return value lists every call this batch rejected, with the class the
// loop assigned it. The final error joins all AfterTool failures after the whole
// batch has settled, so completed results are still available for the failure
// checkpoint instead of silently disappearing with the hook error.
func (l *AgentLoop) execTools(rc *RunContext, lc *LoopContext, calls []core.ToolCall) ([]core.Part, []core.Directive, []ToolRejection, error) {
	if lc.view == nil {
		return nil, nil, nil, errors.New("agent: tool execution requires a frozen step view")
	}
	if err := rc.checkWriteAccess(); err != nil {
		return nil, nil, nil, err
	}
	results := make([]core.Part, len(calls))
	reported := make([]bool, len(calls))
	dirs := make([]core.Directive, len(calls))
	var stateMu sync.Mutex
	var rejects []ToolRejection
	var rejectMu sync.Mutex
	var afterErrs []error
	var afterErrMu sync.Mutex
	var runErrs []error
	var runErrMu sync.Mutex
	recordRunErr := func(err error) {
		if err == nil {
			return
		}
		runErrMu.Lock()
		runErrs = append(runErrs, err)
		runErrMu.Unlock()
	}

	run := func(i int, c core.ToolCall) {
		var out toolOutcome
		if err := rc.checkWriteAccess(); err != nil {
			if !errors.Is(err, context.Canceled) {
				recordRunErr(err)
				return
			}
			out = toolOutcome{tr: cancelledResult(c), err: err}
		} else {
			callCtx, cancel := l.toolCallCtx(lc, &c)
			defer cancel()

			// The handler runs on its own goroutine so the loop can stop waiting on
			// it: Go cannot interrupt a function that is already running, so an
			// uncooperative tool would otherwise hold the batch — and the run —
			// forever. The channel is buffered by one, which is what lets an
			// abandoned worker write its result and exit rather than blocking on a
			// reader that has moved on.
			ch := make(chan toolOutcome, 1)
			go func() { ch <- l.callOne(lc, callCtx, c) }()

			start := time.Now()
			select {
			case out = <-ch:
			case <-callCtx.Done():
				out = awaitAbandoned(ch, callCtx, c, start)
			}
		}
		lc.stats.add(out.class)
		tr := out.tr
		if out.err != nil {
			recordRunErr(out.err)
		}
		if out.noResult {
			return
		}

		// Anything the abandoned handler returns afterwards reaches nobody: the
		// write went to a channel no one reads again, so its ops, Control and
		// AfterTool pass are deliberately skipped here — that is the cost of
		// bounding a call, and the timeout text tells the model to assume nothing.
		if len(out.ops) > 0 {
			stateMu.Lock()
			rc.State.Apply(out.ops...)
			stateMu.Unlock()
		}
		// A tool's own Control wins over an AfterTool directive only if higher
		// precedence; Resolve folds both.
		ds := []core.Directive{}
		if out.control != nil {
			ds = append(ds, *out.control)
		}
		if !out.skipAfter {
			if d, err := l.mw.AfterTool(lc, &tr); err != nil {
				afterErrMu.Lock()
				afterErrs = append(afterErrs, err)
				afterErrMu.Unlock()
			} else {
				ds = append(ds, d)
			}
		}
		if out.execution != nil {
			ex := *out.execution
			ex.State = checkpoint.ToolCompleted
			result := core.Message{Role: core.RoleTool, Parts: []core.Part{tr}}
			ex.Result = &result
			rc.addCompletedTool(ex)
		}
		// Store the result AFTER AfterTool so a hook that rewrites tr (e.g. an
		// eval ToolGuard marking a bad result IsError) is what the model sees in
		// history — consistent with the ToolDone event published below.
		results[i] = tr
		reported[i] = true
		dirs[i] = core.Resolve(ds...)
		if out.rejection != nil {
			rejectMu.Lock()
			rejects = append(rejects, *out.rejection)
			rejectMu.Unlock()
		}
		rc.publish(core.ToolDone{Result: tr})
	}

	segments := buildToolCallSegments(calls, lc.view.Tools(), l.toolExec)
	dispatchStopped := false
	for _, seg := range segments {
		if dispatchStopped {
			break
		}

		if seg.serial {
			i := seg.indexes[0]
			if err := rc.checkWriteAccess(); err != nil {
				recordRunErr(err)
				break
			}
			rc.publish(core.ToolStarted{Call: calls[i]})
			run(i, calls[i])
			continue
		}

		var wg sync.WaitGroup
		for _, i := range seg.indexes {
			if err := rc.checkWriteAccess(); err != nil {
				recordRunErr(err)
				dispatchStopped = true
				break
			}
			rc.publish(core.ToolStarted{Call: calls[i]})
			wg.Add(1)
			go func(i int, c core.ToolCall) {
				defer wg.Done()
				run(i, c)
			}(i, calls[i])
		}
		wg.Wait()
	}
	return reportedToolResults(results, reported), dirs, rejects, errors.Join(append(afterErrs, runErrs...)...)
}

func reportedToolResults(results []core.Part, reported []bool) []core.Part {
	out := make([]core.Part, 0, len(results))
	for i := range results {
		if reported[i] {
			out = append(out, results[i])
		}
	}
	return out
}

// abandonGrace bounds how long the loop still listens to a call whose context has
// just ended. A tool that watched the cancellation has its own error ready within
// microseconds of it; measured without this window the loop won the race every
// single time and threw the handler's better report away in favour of its own
// wording. The cost is bounded and only falls on calls that are already stuck.
const abandonGrace = 2 * time.Millisecond

// awaitAbandoned decides what a bounded call reports once its context ended: the
// handler's own outcome if it arrives within abandonGrace, otherwise the loop's
// word for the abandonment.
func awaitAbandoned(ch <-chan toolOutcome, callCtx context.Context, c core.ToolCall, start time.Time) toolOutcome {
	timer := time.NewTimer(abandonGrace)
	defer timer.Stop()
	select {
	case out := <-ch:
		return out
	case <-timer.C:
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return abandonedAtBound(c, time.Since(start))
		}
		// The run itself was cancelled, not the call out of time. That is not a
		// rejection the loop is reporting on the tool's behalf — there is no later
		// step left to act on it — so it carries no rejection.
		return toolOutcome{tr: cancelledResult(c), class: classExecuted}
	}
}

// toolCallCtx derives the context one tool call runs under: middleware first (a
// span, or a per-tool bound), the agent's default deadline last. Only tightening
// is expressible — a context can carry an earlier deadline, never a later one, so
// a tool needing more time than WithToolTimeout grants means setting that default
// to 0 and bounding per tool with middleware instead.
//
// The returned CancelFunc releases both layers; execTools defers it per call,
// which is also what tells an abandoned handler its context is done.
func (l *AgentLoop) toolCallCtx(lc *LoopContext, c *core.ToolCall) (context.Context, context.CancelFunc) {
	ctx, mwCancel := l.mw.ToolContext(lc, lc.RunContext.Context, c)
	if l.toolTimeout <= 0 {
		return ctx, mwCancel
	}
	callCtx, deadlineCancel := context.WithTimeout(ctx, l.toolTimeout)
	return callCtx, func() { deadlineCancel(); mwCancel() }
}

// updaterContext carries the run's tool.Updater capability through a derived call
// context. Context.Update looks the capability up on the context a tool was
// handed (tool.Context.Update), so wrapping the run context in a deadline — or in
// whatever a ToolContexter middleware returns — would otherwise hide it and drop
// every partial result the long-running tool reports.
type updaterContext struct {
	context.Context
	up tool.Updater
}

func (u updaterContext) UpdateTool(callID string, p core.Part) { u.up.UpdateTool(callID, p) }

// keepToolUpdates re-attaches the run's Update capability when ctx is no longer
// the run context itself, and leaves an unchanged context alone.
func keepToolUpdates(ctx context.Context, rc *RunContext) context.Context {
	if _, ok := ctx.(tool.Updater); ok {
		return ctx
	}
	return updaterContext{Context: ctx, up: rc}
}

// outcomeClass says how a call settled, for the recovery report. Only a
// recovered batch keeps counters; every other batch leaves lc.stats nil and the
// classification is discarded.
type outcomeClass uint8

const (
	classUncounted outcomeClass = iota
	classReplayed
	classExecuted
	classUnknown
	classRejected
)

// batchStats accumulates how the calls of one recovered batch settled. It is
// safe for concurrent use because a batch may run its calls in parallel
// segments. Nil receivers accept every add, so ordinary steps need no guard.
type batchStats struct {
	mu       sync.Mutex
	replayed int
	executed int
	unknown  int
	rejected int
}

func (s *batchStats) add(c outcomeClass) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c {
	case classReplayed:
		s.replayed++
	case classExecuted:
		s.executed++
	case classUnknown:
		s.unknown++
	case classRejected:
		s.rejected++
	}
}

func (s *batchStats) snapshot() (replayed, executed, unknown, rejected int) {
	if s == nil {
		return 0, 0, 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replayed, s.executed, s.unknown, s.rejected
}

// toolOutcome is everything one tool call produces: the result the model sees,
// plus the control directive and state mutations its handler requested, plus the
// rejection the loop reports when the call never got a usable answer.
type toolOutcome struct {
	tr        core.ToolResult
	control   *core.Directive
	ops       []core.StateOp
	rejection *ToolRejection
	err       error
	noResult  bool
	class     outcomeClass

	// skipAfter is used for a result restored from the durable journal. It has
	// already passed AfterTool in the run that first completed it, and replaying
	// that hook can duplicate its own side effects.
	skipAfter bool
	execution *checkpoint.ToolExecution
}

// abandonedAtBound is the loop's own report for a call it stopped waiting on: the
// error result the model gets and the rejection for ToolRejecter, both carrying the
// same sentence. d is measured, not the configured bound, so an agent default and a
// middleware limit word the same way. The tool may still be running: the loop has
// already moved on, so the model is told to assume nothing about its effects.
func abandonedAtBound(c core.ToolCall, d time.Duration) toolOutcome {
	msg := fmt.Sprintf(
		"tool %q timed out after %s and its result was discarded: it may still be running, "+
			"so assume nothing about what it did. Retry with narrower arguments or another tool, "+
			"or say that this step could not complete.", c.Name, d.Round(time.Millisecond))
	return toolOutcome{
		tr:        errResult(c, msg),
		rejection: &ToolRejection{Call: c, Class: RejectTimedOut, Detail: msg},
		class:     classExecuted,
	}
}

// cancelledResult reports the same abandonment when the run itself was cancelled
// rather than out of time.
func cancelledResult(c core.ToolCall) core.ToolResult {
	return errResult(c, fmt.Sprintf(
		"tool %q did not finish: the run was cancelled while it was executing.", c.Name))
}

// callOne dispatches a single tool call, returning its result plus any control
// directive and state ops the tool requested. The name resolves against the
// step's frozen view: a tool that was not advertised is refused, and an
// advertised name runs the implementation frozen with it. Unknown tools,
// rejected or schema-invalid arguments, and handler errors all become error
// ToolResults reported back to the model — the handler never runs for a bad
// call — and each one carries the rejection class alongside, which is what
// ToolRejecter observes.
func (l *AgentLoop) callOne(lc *LoopContext, callCtx context.Context, c core.ToolCall) toolOutcome {
	// The frozen view is the single source of truth: only a tool this step
	// advertised may run, and it runs the implementation frozen for that name.
	tv := lc.view.Tools()
	if !tv.Advertised(c.Name) {
		if tv.knownName(c.Name) {
			return rejected(c, RejectNotAdvertised, fmt.Sprintf(
				"tool %q is not advertised in this step, so it cannot be called; use only the tools listed for this step", c.Name))
		}
		return rejected(c, RejectUnknownTool, "unknown tool: "+c.Name)
	}
	t, ok := tv.Resolve(c.Name)
	if !ok {
		// Only a legacy resumed batch can advertise a name whose implementation
		// has since disappeared. Restore preflight rejects that before it runs.
		return rejected(c, RejectUnknownTool, "unknown tool: "+c.Name)
	}
	raw := c.Args
	if p, ok := t.(tool.ArgumentPreparer); ok {
		prepared, err := p.PrepareArguments(raw)
		if err != nil {
			return rejected(c, RejectPrepareFailed, "invalid arguments: "+err.Error())
		}
		raw = prepared
	}
	if err := tool.Validate(t.Schema(), raw); err != nil {
		return rejected(c, RejectSchemaInvalid, "invalid arguments: "+err.Error())
	}
	if err := lc.RunContext.checkWriteAccess(); err != nil {
		out := toolOutcome{err: fmt.Errorf("tool %q dispatch: %w", c.Name, err)}
		if errors.Is(err, context.Canceled) {
			out.tr = cancelledResult(c)
		} else {
			out.noResult = true
		}
		return out
	}

	invocationID, idempotencyKey := toolInvocation(lc.ThreadID, lc.Step, c)
	var execution *checkpoint.ToolExecution
	if durable, ok := lc.durableStore(); ok {
		ex, err := durable.ClaimTool(callCtx, checkpoint.ToolClaimRequest{
			ThreadID:       lc.ThreadID,
			InvocationID:   invocationID,
			CallID:         c.ID,
			Tool:           c.Name,
			IdempotencyKey: idempotencyKey,
			Claim:          lc.durableClaim(),
		})
		switch {
		case errors.Is(err, checkpoint.ErrToolOutcomeUnknown):
			return toolOutcome{tr: unknownToolResult(c, ex.IdempotencyKey), class: classUnknown}
		case err != nil:
			return toolOutcome{
				tr:  errResult(c, "tool execution could not be fenced: "+err.Error()),
				err: fmt.Errorf("tool %q durable claim: %w", c.Name, err),
			}
		case ex.State == checkpoint.ToolCompleted:
			tr, ok := resultFromExecution(ex, c)
			if !ok {
				return toolOutcome{
					tr:  errResult(c, "stored tool result is invalid and cannot be replayed"),
					err: fmt.Errorf("tool %q has invalid durable result for invocation %q", c.Name, invocationID),
				}
			}
			return toolOutcome{tr: tr, skipAfter: true, class: classReplayed}
		default:
			execution = &ex
		}
	}
	if err := lc.RunContext.checkWriteAccess(); err != nil {
		out := toolOutcome{err: fmt.Errorf("tool %q dispatch: %w", c.Name, err)}
		if errors.Is(err, context.Canceled) {
			out.tr = cancelledResult(c)
			out.execution = execution
		} else {
			out.noResult = true
		}
		return out
	}

	tctx := &tool.Context{
		Context:        keepToolUpdates(callCtx, lc.RunContext),
		State:          lc.State,
		CallID:         c.ID,
		InvocationID:   invocationID,
		IdempotencyKey: idempotencyKey,
		Attempt:        toolAttempt(execution),
	}
	res, err := t.Call(tctx, raw)
	if err != nil {
		out := rejected(c, RejectHandlerError, err.Error())
		out.execution = execution
		// The handler was entered, so this counts as executed even though the
		// call is also reported as rejected.
		out.class = classExecuted
		return out
	}
	if res == nil {
		out := rejected(c, RejectHandlerError, "tool returned no result")
		out.execution = execution
		out.class = classExecuted
		return out
	}
	// A handler that ran and reported its own failure (tool.ErrorResult, hence
	// IsError) is not a rejection: the call was well-formed and answered, and the
	// model may know what to do with the answer. Same for an IsError set later by
	// an AfterTool hook — the loop never sees that pass here.
	return toolOutcome{
		tr:        core.ToolResult{CallID: c.ID, Name: c.Name, Content: res.Content, IsError: res.IsError},
		control:   res.Control,
		ops:       res.State,
		execution: execution,
		class:     classExecuted,
	}
}

// toolInvocation identifies one logical tool-call occurrence. Provider call IDs
// are only unique within a response in practice, so the loop step is part of
// the identity. A paused batch resumes at that same step; a later model turn
// cannot accidentally replay its completed result merely by reusing a call ID.
func toolInvocation(threadID string, step int, c core.ToolCall) (string, string) {
	h := sha256.New()
	_, _ = h.Write([]byte(threadID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(strconv.AppendInt(nil, int64(step), 10))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(c.ID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(c.Name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(c.Args)
	sum := hex.EncodeToString(h.Sum(nil))
	return "tool-" + sum, "idemp-" + sum
}

func pendingExecutions(threadID string, step int, calls []core.ToolCall) []checkpoint.ToolExecution {
	if len(calls) == 0 {
		return nil
	}
	out := make([]checkpoint.ToolExecution, 0, len(calls))
	for _, c := range calls {
		invocationID, idempotencyKey := toolInvocation(threadID, step, c)
		out = append(out, checkpoint.ToolExecution{
			InvocationID:   invocationID,
			CallID:         c.ID,
			Tool:           c.Name,
			IdempotencyKey: idempotencyKey,
			State:          checkpoint.ToolPending,
		})
	}
	return out
}

func toolAttempt(execution *checkpoint.ToolExecution) int {
	if execution == nil || execution.Attempt < 1 {
		return 1
	}
	return execution.Attempt
}

func resultFromExecution(ex checkpoint.ToolExecution, call core.ToolCall) (core.ToolResult, bool) {
	if ex.Result == nil {
		return core.ToolResult{}, false
	}
	for _, p := range ex.Result.Parts {
		tr, ok := p.(core.ToolResult)
		if ok && tr.CallID == call.ID && tr.Name == call.Name {
			return tr, true
		}
	}
	return core.ToolResult{}, false
}

func unknownToolResult(c core.ToolCall, idempotencyKey string) core.ToolResult {
	msg := fmt.Sprintf("tool %q was started before recovery but its outcome was not durably recorded; do not retry it automatically because an external side effect may already have occurred", c.Name)
	if idempotencyKey != "" {
		msg += "; query the external system or request human confirmation using idempotency key " + idempotencyKey
	}
	return errResult(c, msg)
}

// rejected is one call the loop would not run (or could not finish): the error
// result the model sees plus the fact reported to ToolRejecter. Detail is that same
// text, so a guard can quote what the model was told without re-reading history.
func rejected(c core.ToolCall, class RejectClass, msg string) toolOutcome {
	return toolOutcome{
		tr:        errResult(c, msg),
		rejection: &ToolRejection{Call: c, Class: class, Detail: msg},
		class:     classRejected,
	}
}

func errResult(c core.ToolCall, msg string) core.ToolResult {
	return core.ToolResult{
		CallID: c.ID, Name: c.Name, IsError: true,
		Content: []core.Part{core.Text{Text: msg}},
	}
}

// UpdateTool implements tool.Updater for the run: a running tool's
// Context.Update call arrives here and goes out on the run's event stream as a
// ToolUpdate tagged with the CallID that started it. Every tool a run invokes is
// handed a context that carries this capability (callOne, via keepToolUpdates),
// so this is the only path a partial result takes. Like any other publish it is
// safe from a tool's own goroutines; a ToolUpdate arriving after the batch's
// ToolDone is a late report from a tool that did not join its workers, not a
// runtime event. A bounded call (WithToolTimeout) makes that ordinary: the loop
// reports the timeout and moves on, so a partial arriving afterwards belongs to
// a call the run has already answered — observers drop it by CallID, the loop
// does not intercept it.
func (rc *RunContext) UpdateTool(callID string, p core.Part) {
	rc.publish(core.ToolUpdate{CallID: callID, Partial: p})
}

var _ tool.Updater = (*RunContext)(nil)
