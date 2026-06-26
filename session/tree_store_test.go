package session_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

func appendEvt(t *testing.T, st session.Store, s *session.Session, text string, delta map[string]any) string {
	t.Helper()
	m := core.UserText(text)
	e := &core.Event{Author: "x", Message: &m}
	if delta != nil {
		e.Actions.StateDelta = delta
	}
	if err := st.Append(context.Background(), s, e); err != nil {
		t.Fatal(err)
	}
	return e.ID
}

func msgTexts(msgs []core.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Text()
	}
	return out
}

func eqStr(a, b []string) bool {
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

// TestCheckoutBranchesAndState verifies Checkout moves the active leaf so a
// subsequent Append branches from there, rebuilds state along the new path, and
// that Branches enumerates the tips with the active one marked.
func TestCheckoutBranchesAndState(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")

	ida := appendEvt(t, st, s, "a", map[string]any{"k": "a"})
	_ = appendEvt(t, st, s, "b", map[string]any{"k": "b"})
	idc := appendEvt(t, st, s, "c", map[string]any{"k": "c"})

	if v, _ := s.State().Get("k"); v != "c" {
		t.Fatalf("state before checkout = %v, want c", v)
	}

	// Go back to a and branch: a -> d.
	if err := st.Checkout(ctx, s, ida); err != nil {
		t.Fatal(err)
	}
	if s.Leaf() != ida {
		t.Fatalf("leaf = %q, want %q", s.Leaf(), ida)
	}
	if v, _ := s.State().Get("k"); v != "a" {
		t.Fatalf("state after checkout(a) = %v, want a (path-replayed)", v)
	}

	idd := appendEvt(t, st, s, "d", map[string]any{"k": "d"})
	if got := msgTexts(s.Messages()); !eqStr(got, []string{"a", "d"}) {
		t.Fatalf("active messages = %v, want [a d]", got)
	}
	if v, _ := s.State().Get("k"); v != "d" {
		t.Fatalf("state on branch = %v, want d", v)
	}

	// Tips are c (original) and d (new branch); d is active.
	brs, err := st.Branches(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	tips := map[string]bool{}
	var active string
	for _, b := range brs {
		tips[b.LeafEventID] = true
		if b.Active {
			active = b.LeafEventID
		}
	}
	if !tips[idc] || !tips[idd] || len(brs) != 2 {
		t.Fatalf("branches = %+v, want tips {%s,%s}", brs, idc, idd)
	}
	if active != idd {
		t.Fatalf("active tip = %q, want %q", active, idd)
	}
}

// TestForkCopiesPathAndIsolates verifies Fork seeds a new session with the
// root..fromEventID path and that mutating either session leaves the other
// untouched.
func TestForkCopiesPathAndIsolates(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "orig")

	ida := appendEvt(t, st, s, "a", nil)
	idb := appendEvt(t, st, s, "b", nil)
	_ = appendEvt(t, st, s, "c", nil)

	fork, err := st.Fork(ctx, s, idb, "forked")
	if err != nil {
		t.Fatal(err)
	}
	if got := msgTexts(fork.Messages()); !eqStr(got, []string{"a", "b"}) {
		t.Fatalf("fork messages = %v, want [a b]", got)
	}
	if fork.Leaf() != idb {
		t.Fatalf("fork leaf = %q, want %q", fork.Leaf(), idb)
	}

	// Appending to the fork must not change the original.
	appendEvt(t, st, fork, "z", nil)
	if got := msgTexts(fork.Messages()); !eqStr(got, []string{"a", "b", "z"}) {
		t.Fatalf("fork after append = %v", got)
	}
	s2, _ := st.GetOrCreate(ctx, "app", "u", "orig")
	if got := msgTexts(s2.Messages()); !eqStr(got, []string{"a", "b", "c"}) {
		t.Fatalf("original mutated by fork: %v", got)
	}

	_ = ida
}

// TestFileStoreCheckoutPersists verifies a Checkout (active leaf moved to an
// interior node) survives a reload from a fresh FileStore.
func TestFileStoreCheckoutPersists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st1, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := st1.GetOrCreate(ctx, "app", "u", "sess")
	_ = appendEvt(t, st1, s, "a", map[string]any{"k": "a"})
	idb := appendEvt(t, st1, s, "b", map[string]any{"k": "b"})
	_ = appendEvt(t, st1, s, "c", map[string]any{"k": "c"})
	if err := st1.Checkout(ctx, s, idb); err != nil {
		t.Fatal(err)
	}

	// Fresh store over the same dir must restore the checked-out leaf + state.
	st2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := st2.GetOrCreate(ctx, "app", "u", "sess")
	if err != nil {
		t.Fatal(err)
	}
	if s2.Leaf() != idb {
		t.Fatalf("recovered leaf = %q, want %q", s2.Leaf(), idb)
	}
	if got := msgTexts(s2.Messages()); !eqStr(got, []string{"a", "b"}) {
		t.Fatalf("recovered messages = %v, want [a b]", got)
	}
	if v, _ := s2.State().Get("k"); v != "b" {
		t.Fatalf("recovered state = %v, want b", v)
	}

	// Appending now branches from b and the new tip persists across reload.
	idd := appendEvt(t, st2, s2, "d", map[string]any{"k": "d"})
	st3, _ := session.NewFileStore(dir)
	s3, _ := st3.GetOrCreate(ctx, "app", "u", "sess")
	if s3.Leaf() != idd {
		t.Fatalf("after branch+reload leaf = %q, want %q", s3.Leaf(), idd)
	}
	if got := msgTexts(s3.Messages()); !eqStr(got, []string{"a", "b", "d"}) {
		t.Fatalf("after branch+reload messages = %v, want [a b d]", got)
	}
}

// TestFileStoreForkWritesNewFile verifies Fork materializes a new session file
// recoverable by an independent store.
func TestFileStoreForkWritesNewFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st1, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := st1.GetOrCreate(ctx, "app", "u", "orig")
	_ = appendEvt(t, st1, s, "a", nil)
	idb := appendEvt(t, st1, s, "b", nil)
	_ = appendEvt(t, st1, s, "c", nil)

	if _, err := st1.Fork(ctx, s, idb, "forked"); err != nil {
		t.Fatal(err)
	}

	st2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := st2.GetOrCreate(ctx, "app", "u", "forked")
	if err != nil {
		t.Fatal(err)
	}
	if got := msgTexts(f2.Messages()); !eqStr(got, []string{"a", "b"}) {
		t.Fatalf("recovered fork = %v, want [a b]", got)
	}
	if f2.Leaf() != idb {
		t.Fatalf("recovered fork leaf = %q, want %q", f2.Leaf(), idb)
	}
}

// TestTreeStoreUnknownEventErrors verifies Checkout/Fork reject unknown IDs.
func TestTreeStoreUnknownEventErrors(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")
	appendEvt(t, st, s, "a", nil)

	if err := st.Checkout(ctx, s, "nope"); err == nil {
		t.Fatal("Checkout(unknown) = nil, want error")
	}
	if _, err := st.Fork(ctx, s, "nope", "new"); err == nil {
		t.Fatal("Fork(unknown) = nil, want error")
	}
}
