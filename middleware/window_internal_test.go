package middleware

import (
	"encoding/json"
	"testing"

	"github.com/jiujuan/goagent/core"
)

// winAsst is an assistant message carrying one tool call per id.
func winAsst(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolCall{ID: id, Name: "t_" + id, Args: json.RawMessage(`{}`)})
	}
	return core.Message{Role: core.RoleAssistant, Parts: parts}
}

// winTool is one tool message carrying one result per id, the shape the loop
// appends (all results of a step in a single message, agent/loop.go:256).
func winTool(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolResult{CallID: id, Name: "t_" + id, Content: []core.Part{core.Text{Text: "r"}}})
	}
	return core.Message{Role: core.RoleTool, Parts: parts}
}

func winUser(s string) core.Message { return core.UserText(s) }

// winRoles renders a history as a role string for readable failure output.
func winRoles(msgs []core.Message) string {
	out := ""
	for _, m := range msgs {
		out += string(m.Role[0])
	}
	return out
}

func TestGroupUnits(t *testing.T) {
	cases := []struct {
		name string
		msgs []core.Message
		want [][2]int
	}{
		{
			name: "no calls: every message is its own unit",
			msgs: []core.Message{winUser("a"), winAsst(), winUser("b")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "call binds its result",
			msgs: []core.Message{winUser("q"), winAsst("c1"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 3}},
		},
		{
			name: "two calls, results split across two tool messages: one unit",
			msgs: []core.Message{winUser("q"), winAsst("c1", "c2"), winTool("c1"), winTool("c2"), winAsst()},
			want: [][2]int{{0, 1}, {1, 4}, {4, 5}},
		},
		{
			name: "dangling call with no results is a unit of one",
			msgs: []core.Message{winUser("q"), winAsst("c1")},
			want: [][2]int{{0, 1}, {1, 2}},
		},
		{
			name: "leading orphan result stands alone",
			msgs: []core.Message{winTool("gone"), winUser("q"), winAsst("c1"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 4}},
		},
		{
			name: "a user message between call and result breaks adjacency",
			msgs: []core.Message{winUser("q"), winAsst("c1"), winUser("steer"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}},
		},
		{
			name: "absorb on at least one match, stop at a message reporting none",
			msgs: []core.Message{winAsst("c1"), winTool("c1", "c9"), winTool("c9"), winUser("q")},
			want: [][2]int{{0, 2}, {2, 3}, {3, 4}},
		},
		{
			name: "empty history has no units",
			msgs: nil,
			want: [][2]int{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := groupUnits(tc.msgs)
			if len(got) != len(tc.want) {
				t.Fatalf("units = %d %v over [%s], want %d %v", len(got), gotUnits(got), winRoles(tc.msgs), len(tc.want), tc.want)
			}
			for i, u := range got {
				if u.start != tc.want[i][0] || u.end != tc.want[i][1] {
					t.Fatalf("unit %d = [%d,%d), want %v over [%s]", i, u.start, u.end, tc.want[i], winRoles(tc.msgs))
				}
			}
			// Units tile the input exactly: contiguous, non-overlapping, full cover.
			prev := 0
			for _, u := range got {
				if u.start != prev || u.end <= u.start {
					t.Fatalf("units not contiguous at [%d,%d) (prev %d)", u.start, u.end, prev)
				}
				prev = u.end
			}
			if prev != len(tc.msgs) {
				t.Fatalf("units cover %d of %d messages", prev, len(tc.msgs))
			}
		})
	}
}

func gotUnits(us []unit) [][2]int {
	out := make([][2]int, len(us))
	for i, u := range us {
		out[i] = [2]int{u.start, u.end}
	}
	return out
}

func TestRepairPairingDropsOrphanResults(t *testing.T) {
	// A result whose call was evicted, next to one that survives.
	in := []core.Message{
		winUser("q"),
		winTool("gone"),
		winAsst("c1"),
		winTool("c1", "gone2"),
	}
	out := repairPairing(in)
	if len(out) != 3 {
		t.Fatalf("kept %d messages [%s], want 3", len(out), winRoles(out))
	}
	if out[0].Role != core.RoleUser || out[1].Role != core.RoleAssistant {
		t.Fatalf("kept wrong messages: [%s]", winRoles(out))
	}
	tr, ok := out[2].Parts[0].(core.ToolResult)
	if !ok || len(out[2].Parts) != 1 || tr.CallID != "c1" {
		t.Fatalf("surviving tool message keeps %+v, want only c1", out[2].Parts)
	}
	// The input must not be mutated: history is shared with State.Messages.
	if len(in[1].Parts) != 1 || len(in[3].Parts) != 2 {
		t.Fatalf("repairPairing mutated its input: %d then %d parts", len(in[1].Parts), len(in[3].Parts))
	}
}

func TestRepairPairingKeepsPendingCallUntouched(t *testing.T) {
	// The shape a pending HITL interrupt checkpoints (agent/loop.go:236-244):
	// an assistant tool call with no result yet. Repair must leave it alone —
	// synthesizing a result would make Resume see an already-answered call.
	in := []core.Message{winUser("q"), winAsst("c1")}
	out := repairPairing(in)
	if len(out) != 2 {
		t.Fatalf("kept %d messages, want 2 unchanged", len(out))
	}
	if out[1].Role != core.RoleAssistant || len(out[1].Parts) != 1 {
		t.Fatalf("pending call was altered: %+v", out[1].Parts)
	}
}

func TestKeptMessagesNeverSplitsAUnit(t *testing.T) {
	// A history in which every result has its call: two call/result pairs, one
	// dangling-free assistant reply, one user turn in between.
	msgs := []core.Message{
		winUser("q"),
		winAsst("c1", "c2"), winTool("c1"), winTool("c2"),
		winUser("follow-up"),
		winAsst("c3"), winTool("c3"),
		winAsst(),
	}
	units := groupUnits(msgs)
	if len(units) != 5 {
		t.Fatalf("units = %d %v, want 5 ([0,1) [1,4) [4,5) [5,7) [7,8))", len(units), gotUnits(units))
	}

	// No selection of whole units can create an orphan, before any repair:
	// that is the whole point of deciding at unit granularity.
	subsets := [][]int{{}, {0}, {2}, {4}, {0, 2, 4}, {1, 3}, {1, 4}, {0, 1, 2, 3, 4}}
	for _, pick := range subsets {
		var keep []unit
		for _, idx := range pick {
			keep = append(keep, units[idx])
		}
		sel := keptMessages(msgs, keep)
		if orphans := winOrphanCallIDs(sel); len(orphans) > 0 {
			t.Fatalf("subset %v produced orphans %v: [%s]", pick, orphans, winRoles(sel))
		}
		// So repair is a no-op on a history that is already well-formed.
		if got := repairPairing(sel); len(got) != len(sel) {
			t.Fatalf("subset %v: repair dropped %d of %d messages from a well-formed selection", pick, len(sel)-len(got), len(sel))
		}
		// The result is a copy, never a window over the input's backing array:
		// a later append to sel would otherwise overwrite live history.
		if len(sel) > 0 {
			if first := units[pick[0]].start; &sel[0] == &msgs[first] {
				t.Fatalf("subset %v aliases the input at message %d", pick, first)
			}
		}
	}

	// A history that already carries an orphan (its call was never in the input)
	// is where repair earns its keep: the orphan is its own unit, and selecting
	// it alone must leave nothing behind.
	dirty := append([]core.Message{}, msgs...)
	dirty = append(dirty, winTool("gone"))
	dirtyUnits := groupUnits(dirty)
	if len(dirtyUnits) != 6 {
		t.Fatalf("dirty units = %d %v, want 6 (the orphan stands alone)", len(dirtyUnits), gotUnits(dirtyUnits))
	}
	orphanUnit := dirtyUnits[5]
	sel := keptMessages(dirty, []unit{dirtyUnits[0], orphanUnit})
	if orphans := winOrphanCallIDs(sel); len(orphans) != 1 {
		t.Fatalf("orphan selection should carry 1 orphan before repair, got %v", orphans)
	}
	if got := repairPairing(sel); len(got) != 1 || got[0].Role != core.RoleUser {
		t.Fatalf("repair kept [%s], want only the user message", winRoles(got))
	}
}

// winOrphanCallIDs lists the tool results in msgs whose call is absent.
func winOrphanCallIDs(msgs []core.Message) []string {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, c := range m.ToolCalls() {
			calls[c.ID] = true
		}
	}
	var orphans []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && !calls[tr.CallID] {
				orphans = append(orphans, tr.CallID)
			}
		}
	}
	return orphans
}
