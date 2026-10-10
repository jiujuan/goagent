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

	Step     int
	MaxTurns int // the loop's step cap, for budget/wrap-up middleware to observe
	Request  *llm.Request
	History  []core.Message
	approved map[toolApprovalKey]struct{}
}

// IsApproved reports whether this exact persisted tool call was explicitly
// approved for the resumed batch currently being checked. It intentionally does
// not provide a generic "skip gates" switch: middleware that does not opt into
// this narrow capability still sees the resumed call and may interrupt it again.
func (lc *LoopContext) IsApproved(c *core.ToolCall) bool {
	if lc == nil || c == nil {
		return false
	}
	_, ok := lc.approved[toolApprovalKeyFor(*c)]
	return ok
}

type toolApprovalKey struct {
	ID   string
	Name string
	Args string
}

func toolApprovalKeyFor(c core.ToolCall) toolApprovalKey {
	return toolApprovalKey{ID: c.ID, Name: c.Name, Args: string(c.Args)}
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
