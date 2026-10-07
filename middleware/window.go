package middleware

import (
	"github.com/jiujuan/goagent/core"
)

// Context-window eviction. The window strategies (RecentN, SlidingWindow,
// ImportanceWeighted) drop messages instead of summarizing them, and the whole
// point of this file is that they drop *units*, never single messages: an
// assistant tool call and the tool results that answer it travel together, or
// the request reaches a provider with an orphan result it cannot match.
//
// The unit rules and the pairing repair below implement ADR-0031 §3.

// unit is one message or message group that eviction decides about as a whole.
type unit struct {
	start int // first message index, inclusive
	end   int // last message index, exclusive
}

// groupUnits splits msgs into eviction units. An assistant message carrying tool
// calls binds with the tool messages that immediately follow it and report those
// calls; everything else is a unit of its own.
//
// Two facts make this both safe and necessary. Providers copy ToolResult.CallID
// straight into the wire format without checking that the call is present
// (llm/openaicompat/openaicompat.go:178-187, llm/anthropic/anthropic.go:173-185),
// so a result whose call was evicted is a rejected request. And the loop keeps
// the pair adjacent: the assistant message is appended at agent/loop.go:203 and
// its results as one tool message right after it at :256, with the max_tokens
// path at :222 keeping the same shape.
func groupUnits(msgs []core.Message) []unit {
	units := make([]unit, 0, len(msgs))
	for i := 0; i < len(msgs); {
		end := i + 1
		if ids := callIDs(msgs[i]); len(ids) > 0 {
			for j := i + 1; j < len(msgs) && reportsAny(msgs[j], ids); j++ {
				end = j + 1
			}
		}
		units = append(units, unit{start: i, end: end})
		i = end
	}
	return units
}

// callIDs returns the set of tool-call ids m carries, nil for anything but an
// assistant message with calls.
func callIDs(m core.Message) map[string]bool {
	if m.Role != core.RoleAssistant {
		return nil
	}
	calls := m.ToolCalls()
	if len(calls) == 0 {
		return nil
	}
	ids := make(map[string]bool, len(calls))
	for _, c := range calls {
		ids[c.ID] = true
	}
	return ids
}

// reportsAny is true for a tool message carrying at least one result of ids. A
// tool message reporting none of them ends the unit rather than being absorbed:
// it answers a call from further back or one that is gone, and repairPairing
// decides its fate.
func reportsAny(m core.Message, ids map[string]bool) bool {
	if m.Role != core.RoleTool {
		return false
	}
	for _, p := range m.Parts {
		if tr, ok := p.(core.ToolResult); ok && ids[tr.CallID] {
			return true
		}
	}
	return false
}

// keptMessages flattens the chosen units back into a message slice in the
// original order. The fresh slice is deliberate: a window over the input's
// backing array would let a later append overwrite evicted messages.
func keptMessages(msgs []core.Message, keep []unit) []core.Message {
	n := 0
	for _, u := range keep {
		n += u.end - u.start
	}
	out := make([]core.Message, 0, n)
	for _, u := range keep {
		out = append(out, msgs[u.start:u.end]...)
	}
	return out
}

// repairPairing removes tool results whose originating call is not among msgs.
// It is a guard for histories this package did not build — the unit rules
// already keep calls and results together — and it runs once after a strategy
// has chosen what survives.
//
// It deliberately does not synthesize a result for a call that has none. A
// pending human approval checkpoints exactly that shape: agent/loop.go:236-244
// persists the assistant tool-call message together with the still-pending
// calls, and Resume appends the real results afterwards (agent/loop.go:119-128,
// agent/hitl.go:116). A fabricated "result dropped" note would tell the resumed
// run that a call was already answered.
func repairPairing(msgs []core.Message) []core.Message {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, c := range m.ToolCalls() {
			calls[c.ID] = true
		}
	}
	out := make([]core.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Role != core.RoleTool {
			out = append(out, m)
			continue
		}
		parts := make([]core.Part, 0, len(m.Parts))
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && !calls[tr.CallID] {
				continue
			}
			parts = append(parts, p)
		}
		if len(parts) == 0 {
			continue
		}
		m.Parts = parts // m is a copy; the input message keeps its own parts
		out = append(out, m)
	}
	return out
}
