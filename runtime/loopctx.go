package runtime

import (
	"context"
	"maps"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/session"
)

// RunContext is the per-run environment. It embeds context.Context so it passes
// directly to model and tool calls. The loop publishes observational events to
// Bus on Topic and reads/writes the run's State.
type RunContext struct {
	context.Context

	RunID    string
	ThreadID string
	Bus      *bus.Bus
	Topic    bus.Topic
	State    *core.State
}

// LoopContext is the mutable per-step view the loop threads through its phases
// and hands to middleware. It carries the step index, the in-flight request,
// and the working message history.
type LoopContext struct {
	*RunContext

	Step    int
	Request *llm.Request
	History []core.Message
}

// stateAdapter exposes a *core.State's KV map as the v1 session.State interface,
// so existing tools can read/write v2 state without a tool-package change yet
// (the tool.Result.Control/State path arrives in a later step). All() returns a
// clone, matching v1 semantics.
type stateAdapter struct{ s *core.State }

func (a stateAdapter) Get(k string) (any, bool) {
	v, ok := a.s.KV[k]
	return v, ok
}

func (a stateAdapter) Set(k string, v any) {
	if a.s.KV == nil {
		a.s.KV = map[string]any{}
	}
	a.s.KV[k] = v
}

func (a stateAdapter) Delete(k string) { delete(a.s.KV, k) }

func (a stateAdapter) All() map[string]any { return maps.Clone(a.s.KV) }

var _ session.State = stateAdapter{}
