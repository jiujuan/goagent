package agent

import (
	"context"
	"sync"

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
func (l *AgentLoop) execTools(rc *RunContext, lc *LoopContext, calls []core.ToolCall) ([]core.Part, []core.Directive) {
	results := make([]core.Part, len(calls))
	dirs := make([]core.Directive, len(calls))
	var stateMu sync.Mutex

	run := func(i int, c core.ToolCall) {
		callCtx, cancel := l.toolCallCtx(lc, &c)
		defer cancel()
		tr, control, ops := l.callOne(lc, callCtx, c)
		if len(ops) > 0 {
			stateMu.Lock()
			rc.State.Apply(ops...)
			stateMu.Unlock()
		}
		// A tool's own Control wins over an AfterTool directive only if higher
		// precedence; Resolve folds both.
		ds := []core.Directive{}
		if control != nil {
			ds = append(ds, *control)
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

// callOne dispatches a single tool call, returning its result plus any control
// directive and state ops the tool requested. The name resolves against the
// run's injected tools first, then the agent's own table. Unknown tools, rejected
// or schema-invalid arguments, and handler errors all become error ToolResults
// reported back to the model — the handler never runs for a bad call.
func (l *AgentLoop) callOne(lc *LoopContext, callCtx context.Context, c core.ToolCall) (core.ToolResult, *core.Directive, []core.StateOp) {
	// Run-scoped injections resolve first, so a middleware can replace an
	// agent-level tool by name or supply one the agent was built without.
	t, ok := lc.dynamic.lookup(c.Name)
	if !ok {
		t, ok = l.byName[c.Name]
	}
	if !ok {
		return errResult(c, "unknown tool: "+c.Name), nil, nil
	}
	raw := c.Args
	if p, ok := t.(tool.ArgumentPreparer); ok {
		prepared, err := p.PrepareArguments(raw)
		if err != nil {
			return errResult(c, "invalid arguments: "+err.Error()), nil, nil
		}
		raw = prepared
	}
	if err := tool.Validate(t.Schema(), raw); err != nil {
		return errResult(c, "invalid arguments: "+err.Error()), nil, nil
	}
	tctx := &tool.Context{Context: keepToolUpdates(callCtx, lc.RunContext), State: lc.State, CallID: c.ID}
	res, err := t.Call(tctx, raw)
	if err != nil {
		return errResult(c, err.Error()), nil, nil
	}
	tr := core.ToolResult{CallID: c.ID, Name: c.Name, Content: res.Content, IsError: res.IsError}
	return tr, res.Control, res.State
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
// ToolDone is a late report from a tool that did not join its workers, not a runtime event.
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
