package session

import (
	"testing"

	"github.com/jiujuan/goagent/core"
)

func TestDetachedCommitDoesNotAdvanceActiveView(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{ID: "base", Message: msg(core.RoleUser, "base"), Actions: core.Actions{StateDelta: map[string]any{"k": "base"}}})
	s.commit(&core.Event{
		ID:       "branch",
		ParentID: "base",
		Detached: true,
		Message:  msg(core.RoleAssistant, "branch"),
		Actions:  core.Actions{StateDelta: map[string]any{"k": "branch"}},
	})

	if got := s.Leaf(); got != "base" {
		t.Fatalf("leaf = %q, want base", got)
	}
	if got := texts(s.Messages()); !equal(got, []string{"base"}) {
		t.Fatalf("active messages = %v, want [base]", got)
	}
	if got, _ := s.State().Get("k"); got != "base" {
		t.Fatalf("live state k = %v, want base", got)
	}
	if got, _ := s.stateAlong("branch").Get("k"); got != "branch" {
		t.Fatalf("branch state k = %v, want branch", got)
	}
}

func TestMergeProjectsBranchesInDeclaredOrder(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{ID: "base", Message: msg(core.RoleUser, "base")})

	// Commit B first to prove physical completion order does not control the
	// logical history exposed after merge.
	s.commit(&core.Event{ID: "b1", ParentID: "base", Detached: true, Branch: "p.b", Message: msg(core.RoleAssistant, "B")})
	s.commit(&core.Event{ID: "a1", ParentID: "base", Detached: true, Branch: "p.a", Message: msg(core.RoleAssistant, "A")})
	s.commit(&core.Event{
		ID:           "merge",
		ParentID:     "base",
		MergeParents: []string{"a1", "b1"},
		Actions:      core.Actions{StateDelta: map[string]any{"merged": true}},
	})

	if got := texts(s.Messages()); !equal(got, []string{"base", "A", "B"}) {
		t.Fatalf("merged messages = %v, want [base A B]", got)
	}
	if got := s.Leaf(); got != "merge" {
		t.Fatalf("leaf = %q, want merge", got)
	}
	if got, _ := s.State().Get("merged"); got != true {
		t.Fatalf("merged state = %v, want true", got)
	}
	if tips := s.tips(); len(tips) != 1 || tips[0] != "merge" {
		t.Fatalf("tips = %v, want [merge]", tips)
	}
}

func TestMergeAppliesStateDeletes(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{ID: "base", Actions: core.Actions{StateDelta: map[string]any{"remove": 1, "keep": 2}}})
	s.commit(&core.Event{
		ID:       "merge",
		ParentID: "base",
		Actions: core.Actions{
			StateDelta:  map[string]any{"added": 3},
			StateDelete: []string{"remove"},
		},
	})

	if _, ok := s.State().Get("remove"); ok {
		t.Fatal("removed state key is still present")
	}
	if got, _ := s.State().Get("keep"); got != 2 {
		t.Fatalf("keep = %v, want 2", got)
	}
	if got, _ := s.State().Get("added"); got != 3 {
		t.Fatalf("added = %v, want 3", got)
	}
}

func TestCheckoutDetachedBranchRestoresBranchState(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{ID: "base", Actions: core.Actions{StateDelta: map[string]any{"k": "base"}}})
	s.commit(&core.Event{ID: "branch", ParentID: "base", Detached: true, Actions: core.Actions{StateDelta: map[string]any{"k": "branch"}}})

	if err := s.checkout("branch"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.State().Get("k"); got != "branch" {
		t.Fatalf("checked-out state k = %v, want branch", got)
	}
}

func TestStoreRejectsDanglingGraphEdges(t *testing.T) {
	store := InMemory()
	s, _ := store.GetOrCreate(t.Context(), "app", "user", "session")
	err := store.Append(t.Context(), s, &core.Event{ID: "bad", ParentID: "missing"})
	if err == nil {
		t.Fatal("append with unknown parent returned nil error")
	}
}

func TestSummaryCanCutInsideMergedProjection(t *testing.T) {
	store := InMemory()
	s, _ := store.GetOrCreate(t.Context(), "app", "user", "session")
	for _, event := range []*core.Event{
		{ID: "base", Message: msg(core.RoleUser, "base")},
		{ID: "a", ParentID: "base", Detached: true, Message: msg(core.RoleAssistant, "A")},
		{ID: "b", ParentID: "base", Detached: true, Message: msg(core.RoleAssistant, "B")},
		{ID: "merge", ParentID: "base", MergeParents: []string{"a", "b"}},
	} {
		if err := store.Append(t.Context(), s, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := Summarize(t.Context(), store, s, "a", "summary-through-A"); err != nil {
		t.Fatal(err)
	}
	if got := texts(s.Messages()); !equal(got, []string{"summary-through-A", "B"}) {
		t.Fatalf("summarized merge messages = %v, want [summary-through-A B]", got)
	}
}

func TestBranchesIncludesActiveBaseWithUnmergedChildren(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{ID: "base"})
	s.commit(&core.Event{ID: "detached", ParentID: "base", Detached: true})

	refs := s.branchRefs()
	if len(refs) != 2 {
		t.Fatalf("branch refs = %+v, want active base plus detached tip", refs)
	}
	var active string
	for _, ref := range refs {
		if ref.Active {
			active = ref.LeafEventID
		}
	}
	if active != "base" {
		t.Fatalf("active ref = %q, want base", active)
	}
}
