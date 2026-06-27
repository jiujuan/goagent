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
// concurrent by default (LoopPolicy.ToolExecution); Sequential runs one at a
// time.
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
		results[i] = tr
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
		dirs[i] = core.Resolve(ds...)
		rc.publish(core.ToolDone{Result: tr})
	}

	if l.toolExec == Sequential {
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
// directive and state ops the tool requested. Unknown tools and handler errors
// become error ToolResults reported back to the model.
func (l *AgentLoop) callOne(rc *RunContext, c core.ToolCall) (core.ToolResult, *core.Directive, []core.StateOp) {
	t, ok := l.byName[c.Name]
	if !ok {
		return core.ToolResult{
			CallID: c.ID, Name: c.Name, IsError: true,
			Content: []core.Part{core.Text{Text: "unknown tool: " + c.Name}},
		}, nil, nil
	}
	tctx := &tool.Context{Context: rc, State: rc.State, CallID: c.ID}
	res, err := t.Call(tctx, c.Args)
	if err != nil {
		return core.ToolResult{
			CallID: c.ID, Name: c.Name, IsError: true,
			Content: []core.Part{core.Text{Text: err.Error()}},
		}, nil, nil
	}
	tr := core.ToolResult{CallID: c.ID, Name: c.Name, Content: res.Content, IsError: res.IsError}
	return tr, res.Control, res.State
}
