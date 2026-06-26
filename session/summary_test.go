package session_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/session"
)

// TestSummarizeProjectsAndPreservesState verifies a summary node replaces the
// covered prefix in the projected messages while leaving derived State (which
// replays over every event) untouched.
func TestSummarizeProjectsAndPreservesState(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")

	_ = appendEvt(t, st, s, "a", map[string]any{"k": "a"})
	_ = appendEvt(t, st, s, "b", nil)
	idc := appendEvt(t, st, s, "c", map[string]any{"k": "c"})
	_ = appendEvt(t, st, s, "d", nil)
	_ = appendEvt(t, st, s, "e", nil)

	if err := session.Summarize(ctx, st, s, idc, "SUM(a..c)"); err != nil {
		t.Fatal(err)
	}

	// Projection: summary stands in for a,b,c; d,e kept.
	if got := msgTexts(s.Messages()); !eqStr(got, []string{"SUM(a..c)", "d", "e"}) {
		t.Fatalf("projected messages = %v, want [SUM(a..c) d e]", got)
	}
	// State is unaffected by summarization: full path still replays k=c.
	if v, _ := s.State().Get("k"); v != "c" {
		t.Fatalf("state after summarize = %v, want c", v)
	}
	// The raw event log still holds every event (append-only, nothing dropped).
	if n := len(s.Events()); n != 6 {
		t.Fatalf("raw events = %d, want 6 (5 + summary node)", n)
	}
}

// TestResummarizeSupersedes verifies a later summary node nearest the leaf wins,
// folding the earlier summary into its replaced prefix.
func TestResummarizeSupersedes(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")

	_ = appendEvt(t, st, s, "a", nil)
	_ = appendEvt(t, st, s, "b", nil)
	idc := appendEvt(t, st, s, "c", nil)
	idd := appendEvt(t, st, s, "d", nil)
	_ = appendEvt(t, st, s, "e", nil)

	if err := session.Summarize(ctx, st, s, idc, "SUM1(a..c)"); err != nil {
		t.Fatal(err)
	}
	if got := msgTexts(s.Messages()); !eqStr(got, []string{"SUM1(a..c)", "d", "e"}) {
		t.Fatalf("after first summary = %v", got)
	}

	// Re-summarize further (cut at d): the new node supersedes SUM1.
	if err := session.Summarize(ctx, st, s, idd, "SUM2(a..d)"); err != nil {
		t.Fatal(err)
	}
	if got := msgTexts(s.Messages()); !eqStr(got, []string{"SUM2(a..d)", "e"}) {
		t.Fatalf("after re-summary = %v, want [SUM2(a..d) e]", got)
	}
}

// TestSummarizeRejectsOffPathCut verifies the cut must be on the active path.
func TestSummarizeRejectsOffPathCut(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")
	appendEvt(t, st, s, "a", nil)

	if err := session.Summarize(ctx, st, s, "nope", "x"); err == nil {
		t.Fatal("Summarize(off-path cut) = nil, want error")
	}
}

// TestSummaryNodePersists verifies a summary node and its projection survive a
// reload from a fresh FileStore.
func TestSummaryNodePersists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st1, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := st1.GetOrCreate(ctx, "app", "u", "sess")
	_ = appendEvt(t, st1, s, "a", nil)
	idb := appendEvt(t, st1, s, "b", nil)
	_ = appendEvt(t, st1, s, "c", nil)
	if err := session.Summarize(ctx, st1, s, idb, "SUM(a..b)"); err != nil {
		t.Fatal(err)
	}

	st2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := st2.GetOrCreate(ctx, "app", "u", "sess")
	if err != nil {
		t.Fatal(err)
	}
	if got := msgTexts(s2.Messages()); !eqStr(got, []string{"SUM(a..b)", "c"}) {
		t.Fatalf("recovered projection = %v, want [SUM(a..b) c]", got)
	}
	if !strings.HasPrefix(msgTexts(s2.Messages())[0], "SUM") {
		t.Fatal("summary not recovered as first message")
	}
}
