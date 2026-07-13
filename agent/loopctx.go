package agent

import (
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
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
