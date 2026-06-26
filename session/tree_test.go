package session

import (
	"testing"

	"github.com/jiujuan/goagent/core"
)

// TestCommitLinksParentAndAdvancesLeaf verifies a linear sequence of commits
// forms a chain: each event's parent is its predecessor, and Leaf tracks the
// tip. This is the degenerate tree that keeps existing linear usage unchanged.
func TestCommitLinksParentAndAdvancesLeaf(t *testing.T) {
	s := newSession("app", "u", "s")

	e1 := &core.Event{ID: "e1", Message: msg(core.RoleUser, "hi")}
	s.commit(e1)
	if e1.ParentID != "" {
		t.Fatalf("root parent = %q, want empty", e1.ParentID)
	}
	if s.Leaf() != "e1" {
		t.Fatalf("leaf = %q, want e1", s.Leaf())
	}

	e2 := &core.Event{ID: "e2", Message: msg(core.RoleAssistant, "yo")}
	s.commit(e2)
	if e2.ParentID != "e1" {
		t.Fatalf("e2 parent = %q, want e1", e2.ParentID)
	}
	if s.Leaf() != "e2" {
		t.Fatalf("leaf = %q, want e2", s.Leaf())
	}

	if got := len(s.Messages()); got != 2 {
		t.Fatalf("messages = %d, want 2", got)
	}
}

// TestActivePathFollowsLeafAcrossBranches builds a fork off the root and checks
// that Messages/Events project only the active branch, and that switching the
// leaf changes what history is visible.
func TestActivePathFollowsLeafAcrossBranches(t *testing.T) {
	s := newSession("app", "u", "s")

	root := &core.Event{ID: "root", Message: msg(core.RoleUser, "root")}
	s.commit(root)

	// Branch A: root -> a
	a := &core.Event{ID: "a", ParentID: "root", Message: msg(core.RoleAssistant, "A")}
	s.commit(a)

	// Branch B: also off root -> b. Committing it moves the active leaf to b.
	b := &core.Event{ID: "b", ParentID: "root", Message: msg(core.RoleAssistant, "B")}
	s.commit(b)

	if s.Leaf() != "b" {
		t.Fatalf("leaf = %q, want b", s.Leaf())
	}
	// Active path is root -> b only (branch A's "a" is not visible).
	got := texts(s.Messages())
	if want := []string{"root", "B"}; !equal(got, want) {
		t.Fatalf("active messages = %v, want %v", got, want)
	}
	if n := len(s.Events()); n != 2 {
		t.Fatalf("active events = %d, want 2", n)
	}

	// pathTo on the other branch still resolves independently.
	if got := texts(messagesOf(s.pathTo("a"))); !equal(got, []string{"root", "A"}) {
		t.Fatalf("branch A path = %v, want [root A]", got)
	}
}

// TestStateAlongReplaysPerPath verifies state is path-dependent: each branch
// sees only the deltas on its own path back to the root, which is the mechanism
// branch switching (phase 2) relies on.
func TestStateAlongReplaysPerPath(t *testing.T) {
	s := newSession("app", "u", "s")

	root := &core.Event{ID: "root", Actions: core.Actions{StateDelta: map[string]any{"k": "root"}}}
	s.commit(root)
	a := &core.Event{ID: "a", ParentID: "root", Actions: core.Actions{StateDelta: map[string]any{"k": "A"}}}
	s.commit(a)
	b := &core.Event{ID: "b", ParentID: "root", Actions: core.Actions{StateDelta: map[string]any{"k": "B"}}}
	s.commit(b)

	if v, _ := s.stateAlong("a").Get("k"); v != "A" {
		t.Fatalf("stateAlong(a)[k] = %v, want A", v)
	}
	if v, _ := s.stateAlong("b").Get("k"); v != "B" {
		t.Fatalf("stateAlong(b)[k] = %v, want B", v)
	}
	// Invariant: live (incrementally maintained) state matches stateAlong(leaf).
	live, _ := s.State().Get("k")
	along, _ := s.stateAlong(s.Leaf()).Get("k")
	if live != along {
		t.Fatalf("live state %v != stateAlong(leaf) %v", live, along)
	}
}

// TestStateAlongDropsTempKeys verifies temp:-scoped keys are not retained when
// replaying state along a path, matching commit's behavior.
func TestStateAlongDropsTempKeys(t *testing.T) {
	s := newSession("app", "u", "s")
	e := &core.Event{ID: "e", Actions: core.Actions{StateDelta: map[string]any{
		"keep":      1,
		"temp:gone": 2,
	}}}
	s.commit(e)

	st := s.stateAlong("e")
	if _, ok := st.Get("keep"); !ok {
		t.Fatal("stateAlong dropped non-temp key")
	}
	if _, ok := st.Get("temp:gone"); ok {
		t.Fatal("stateAlong retained temp: key")
	}
}

// --- helpers ----------------------------------------------------------------

func msg(role core.Role, text string) *core.Message {
	return &core.Message{Role: role, Parts: []core.Part{core.Text{Text: text}}}
}

func messagesOf(events []*core.Event) []core.Message {
	out := make([]core.Message, 0, len(events))
	for _, e := range events {
		if e.Message != nil {
			out = append(out, *e.Message)
		}
	}
	return out
}

func texts(msgs []core.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Text()
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
