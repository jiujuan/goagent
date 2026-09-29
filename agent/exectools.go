package agent

import (
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
		tr, control, ops := l.callOne(rc, c)
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
	// downgrades the whole batch, keeping the model's call order.
	needSeq := l.toolExec == ToolSequential
	if !needSeq {
		for _, c := range calls {
			if l.seqTool[c.Name] {
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

// callOne dispatches a single tool call, returning its result plus any control
// directive and state ops the tool requested. Unknown tools, rejected or
// schema-invalid arguments, and handler errors all become error ToolResults
// reported back to the model — the handler never runs for a bad call.
func (l *AgentLoop) callOne(rc *RunContext, c core.ToolCall) (core.ToolResult, *core.Directive, []core.StateOp) {
	t, ok := l.byName[c.Name]
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
	tctx := &tool.Context{Context: rc, State: rc.State, CallID: c.ID}
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
// handed rc as its context (callOne), so this is the only path a partial result
// takes. Like any other publish it is safe from a tool's own goroutines; a
// ToolUpdate arriving after the batch's ToolDone is a late report from a tool
// that did not join its workers, not a runtime event.
func (rc *RunContext) UpdateTool(callID string, p core.Part) {
	rc.publish(core.ToolUpdate{CallID: callID, Partial: p})
}

var _ tool.Updater = (*RunContext)(nil)
