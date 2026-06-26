package runtime

import (
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/event"
	"github.com/jiujuan/goagent/tool"
)

// execTools runs a step's tool calls and returns their results in the model's
// original call order, plus the per-call directives the AfterTool middleware
// produced. Execution is concurrent by default (LoopPolicy.ToolExecution); any
// Sequential policy runs them one at a time.
//
// Tool events: ToolStarted is published in call order from this goroutine;
// ToolDone is published as each result lands (completion order under Parallel).
// The Bus is concurrency-safe, so parallel publishes do not race.
func (l *AgentLoop) execTools(rc *RunContext, lc *LoopContext, calls []core.ToolCall) ([]core.Part, []core.Directive) {
	results := make([]core.Part, len(calls))
	dirs := make([]core.Directive, len(calls))

	run := func(i int, c core.ToolCall) {
		tr := l.callOne(rc, c)
		results[i] = tr
		if d, err := l.mw.AfterTool(lc, &tr); err == nil {
			dirs[i] = d
		}
		rc.Bus.Publish(rc.Topic, event.ToolDone{Result: tr})
	}

	if l.spec.Loop.ToolExecution == Sequential {
		for i := range calls {
			rc.Bus.Publish(rc.Topic, event.ToolStarted{Call: calls[i]})
			run(i, calls[i])
		}
		return results, dirs
	}

	var wg sync.WaitGroup
	for i := range calls {
		rc.Bus.Publish(rc.Topic, event.ToolStarted{Call: calls[i]})
		wg.Add(1)
		go func(i int, c core.ToolCall) {
			defer wg.Done()
			run(i, c)
		}(i, calls[i])
	}
	wg.Wait()
	return results, dirs
}

// callOne dispatches a single tool call to its handler, mapping unknown tools
// and handler errors into error ToolResults (reported back to the model, not
// propagated as Go errors).
func (l *AgentLoop) callOne(rc *RunContext, c core.ToolCall) core.ToolResult {
	t, ok := l.byName[c.Name]
	if !ok {
		return core.ToolResult{
			CallID: c.ID, Name: c.Name, IsError: true,
			Content: []core.Part{core.Text{Text: "unknown tool: " + c.Name}},
		}
	}
	tctx := &tool.Context{
		Context: rc,
		State:   stateAdapter{rc.State},
		Actions: &core.Actions{}, // v1 Actions ignored in v2; tools signal via Result later
		CallID:  c.ID,
	}
	res, err := t.Call(tctx, c.Args)
	if err != nil {
		return core.ToolResult{
			CallID: c.ID, Name: c.Name, IsError: true,
			Content: []core.Part{core.Text{Text: err.Error()}},
		}
	}
	return core.ToolResult{CallID: c.ID, Name: c.Name, Content: res.Content, IsError: res.IsError}
}
