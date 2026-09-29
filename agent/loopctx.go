package agent

import (
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// LoopContext is the per-step view of the runtime that the loop threads through
// its phases and hands to middleware. It embeds *RunContext (the run-wide
// execution environment), so a hook reaches both the current step and the whole
// run (publish events, read/write State, reach the checkpointer).
type LoopContext struct {
	*RunContext

	Step    int
	Request *llm.Request
	History []core.Message
}

// AddTool makes a tool callable for the rest of this run and advertises it on the
// step's request, so a middleware can supply a capability the agent was not built
// with: a per-run data source, a mock, or an implementation that replaces an
// agent-level tool of the same name. Injected tools resolve ahead of the agent's
// own table (see exectools.go), so replacing by name is deliberate and visible.
//
// It is scoped to this run: the agent and any other run of it are untouched, and
// a sub-run (a delegated agent, a DAG node) gets its own table. The system prompt
// is rendered once per run before the first step, so it still describes the
// agent's original tools; call this from ModifyRequest, where the advertisement
// is picked up for that step and every later one.
func (lc *LoopContext) AddTool(t tool.Tool) {
	lc.RunContext.dynamic.set(t)
	if lc.Request == nil {
		return
	}
	lc.Request.Tools = mergeSchema(lc.Request.Tools, tool.SchemaOf(t))
}
