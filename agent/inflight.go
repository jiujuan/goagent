package agent

import (
	"encoding/json"

	"github.com/jiujuan/goagent/core"
)

// inFlightCallsAdvertised reports whether every call names a tool this step
// offered the model. A call outside that table never runs, so a batch holding
// one has no side effect to recover; writing it would also record work the
// storage layer rejects (an in-flight call must belong to the step's
// advertised table). Such a batch skips the write and takes the ordinary
// per-call rejection path.
func inFlightCallsAdvertised(lc *LoopContext, calls []core.ToolCall) bool {
	tv := lc.view.Tools()
	for _, c := range calls {
		if !tv.Advertised(c.Name) {
			return false
		}
	}
	return true
}

// cloneToolCalls deep-copies a batch so a checkpoint, an event and the handlers
// cannot share argument bytes. The gate may rewrite Args in place, each record
// keeps the bytes as they were at the moment it was taken.
func cloneToolCalls(calls []core.ToolCall) []core.ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]core.ToolCall, len(calls))
	for i, c := range calls {
		if c.Args != nil {
			c.Args = append(json.RawMessage(nil), c.Args...)
		}
		out[i] = c
	}
	return out
}
