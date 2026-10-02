package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// execTools runs a step's tool calls and returns their results in the model's
// original call order, plus the directives gathered from each tool's
// Result.Control and the AfterTool middleware. State mutations a tool requests
// (Result.State) are applied to the run State immediately. Execution is
// concurrent by default (WithToolExecution); ToolSequential forces serial, and
// so does any tool declaring the SequentialTool capability (tool.AsSequential),
// which downgrades the whole batch it appears in.
//
// ToolStarted is published in call order; ToolDone as each result lands. The
// Bus is concurrency-safe, so parallel publishes do not race. State mutations
// from parallel tools are serialized under a mutex.
//
// A call bounded by WithToolTimeout (or by a middleware context bound) stops
// being waited for when its deadline passes: the loop reports a timeout result
// and moves on. The tool's late result, Control and State ops are then dropped,
// so its external side effects may continue unseen — see timeoutResult.
func (l *AgentLoop) execTools(rc *RunContext, lc *LoopContext, calls []core.ToolCall) ([]core.Part, []core.Directive) {
	results := make([]core.Part, len(calls))
	dirs := make([]core.Directive, len(calls))
	var stateMu sync.Mutex

	run := func(i int, c core.ToolCall) {
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
		var out toolOutcome
		select {
		case out = <-ch:
		case <-callCtx.Done():
			out = awaitAbandoned(ch, callCtx, c, start)
		}
		tr := out.tr

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
		if d, err := l.mw.AfterTool(lc, &tr); err == nil {
			ds = append(ds, d)
		}
		// Store the result AFTER AfterTool so a hook that rewrites tr (e.g. an
		// eval ToolGuard marking a bad result IsError) is what the model sees in
		// history — consistent with the ToolDone event published below.
		results[i] = tr
		dirs[i] = core.Resolve(ds...)
		rc.publish(core.ToolDone{Result: tr})
	}

	// Serial if the agent forces it globally, or if any call in this batch
	// targets a tool declaring the SequentialTool capability — one such tool
	// downgrades the whole batch, keeping the model's call order. Tools injected
	// for this run count the same way (isSequential).
	needSeq := l.toolExec == ToolSequential
	if !needSeq {
		for _, c := range calls {
			if l.isSequential(rc, c.Name) {
				needSeq = true
				break
			}
		}
	}
	if needSeq {
		for i := range calls {
			rc.publish(core.ToolStarted{Call: calls[i]})
			run(i, calls[i])
		}
		return results, dirs
	}

	var wg sync.WaitGroup
	for i := range calls {
		rc.publish(core.ToolStarted{Call: calls[i]})
		wg.Add(1)
		go func(i int, c core.ToolCall) {
			defer wg.Done()
			run(i, c)
		}(i, calls[i])
	}
	wg.Wait()
	return results, dirs
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
			return toolOutcome{tr: timeoutResult(c, time.Since(start))}
		}
		return toolOutcome{tr: cancelledResult(c)}
	}
}

// toolCallCtx derives the context one tool call runs under: middleware first (a
// span, or a per-tool bound), the agent's default deadline last. Only tightening
// is expressible — a context can carry an earlier deadline, never a later one, so
// a tool needing more time than WithToolTimeout grants means setting that default
// to 0 and bounding per tool with middleware instead.
func (l *AgentLoop) toolCallCtx(lc *LoopContext, c *core.ToolCall) (context.Context, context.CancelFunc) {
	ctx := l.mw.ToolContext(lc, lc.RunContext.Context, c)
	if l.toolTimeout <= 0 {
		return ctx, func() {}
	}
	callCtx, cancel := context.WithTimeout(ctx, l.toolTimeout)
	return callCtx, cancel
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

// toolOutcome is everything one tool call produces: the result the model sees,
// plus the control directive and state mutations its handler requested.
type toolOutcome struct {
	tr      core.ToolResult
	control *core.Directive
	ops     []core.StateOp
}

// timeoutResult reports a call the loop stopped waiting for. d is measured, not
// the configured bound, so an agent default and a middleware limit word the same
// way. The tool may still be running: the loop has already moved on, so the
// model is told to assume nothing about its effects.
func timeoutResult(c core.ToolCall, d time.Duration) core.ToolResult {
	return errResult(c, fmt.Sprintf(
		"tool %q timed out after %s and its result was discarded: it may still be running, "+
			"so assume nothing about what it did. Retry with narrower arguments or another tool, "+
			"or say that this step could not complete.", c.Name, d.Round(time.Millisecond)))
}

// cancelledResult reports the same abandonment when the run itself was cancelled
// rather than out of time.
func cancelledResult(c core.ToolCall) core.ToolResult {
	return errResult(c, fmt.Sprintf(
		"tool %q did not finish: the run was cancelled while it was executing.", c.Name))
}

// callOne dispatches a single tool call, returning its result plus any control
// directive and state ops the tool requested. The name resolves against the
// run's injected tools first, then the agent's own table. Unknown tools, rejected
// or schema-invalid arguments, and handler errors all become error ToolResults
// reported back to the model — the handler never runs for a bad call.
func (l *AgentLoop) callOne(lc *LoopContext, callCtx context.Context, c core.ToolCall) toolOutcome {
	// Run-scoped injections resolve first, so a middleware can replace an
	// agent-level tool by name or supply one the agent was built without.
	t, ok := lc.dynamic.lookup(c.Name)
	if !ok {
		t, ok = l.byName[c.Name]
	}
	if !ok {
		return toolOutcome{tr: errResult(c, "unknown tool: "+c.Name)}
	}
	raw := c.Args
	if p, ok := t.(tool.ArgumentPreparer); ok {
		prepared, err := p.PrepareArguments(raw)
		if err != nil {
			return toolOutcome{tr: errResult(c, "invalid arguments: "+err.Error())}
		}
		raw = prepared
	}
	if err := tool.Validate(t.Schema(), raw); err != nil {
		return toolOutcome{tr: errResult(c, "invalid arguments: "+err.Error())}
	}
	tctx := &tool.Context{Context: keepToolUpdates(callCtx, lc.RunContext), State: lc.State, CallID: c.ID}
	res, err := t.Call(tctx, raw)
	if err != nil {
		return toolOutcome{tr: errResult(c, err.Error())}
	}
	return toolOutcome{
		tr:      core.ToolResult{CallID: c.ID, Name: c.Name, Content: res.Content, IsError: res.IsError},
		control: res.Control,
		ops:     res.State,
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

// isSequential reports whether the tool behind name must not share a batch with others.
// Both tables count: the agent's own capability flags recorded at construction,
// and the run's injected tools resolved by name.
func (l *AgentLoop) isSequential(rc *RunContext, name string) bool {
	if l.seqTool[name] {
		return true
	}
	t, ok := rc.dynamic.lookup(name)
	if !ok {
		return false
	}
	s, ok := t.(tool.SequentialTool)
	return ok && s.SequentialExecution()
}
