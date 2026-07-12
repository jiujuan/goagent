package session_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

func TestFileStoreReloadAndForkPreserveMergeGraph(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := store.GetOrCreate(ctx, "app", "user", "source")
	appendGraphEvent(t, store, s, &core.Event{ID: "base", Message: message("base")})
	appendGraphEvent(t, store, s, &core.Event{ID: "b", ParentID: "base", Detached: true, Message: message("B")})
	appendGraphEvent(t, store, s, &core.Event{ID: "a", ParentID: "base", Detached: true, Message: message("A")})
	appendGraphEvent(t, store, s, &core.Event{
		ID:           "merge",
		ParentID:     "base",
		MergeParents: []string{"a", "b"},
		Actions:      core.Actions{StateDelta: map[string]any{"merged": true}},
	})

	reloaded, _ := session.NewFileStore(dir)
	s2, err := reloaded.GetOrCreate(ctx, "app", "user", "source")
	if err != nil {
		t.Fatal(err)
	}
	if got := messageTextsForMerge(s2.Messages()); !equalMergeStrings(got, []string{"base", "A", "B"}) {
		t.Fatalf("reloaded messages = %v, want [base A B]", got)
	}
	if got, _ := s2.State().Get("merged"); got != true {
		t.Fatalf("reloaded merged state = %v, want true", got)
	}

	var tree session.TreeStore = reloaded
	fork, err := tree.Fork(ctx, s2, "merge", "fork")
	if err != nil {
		t.Fatal(err)
	}
	if got := messageTextsForMerge(fork.Messages()); !equalMergeStrings(got, []string{"base", "A", "B"}) {
		t.Fatalf("fork messages = %v, want [base A B]", got)
	}
	if len(fork.Events()) != 4 {
		t.Fatalf("fork event count = %d, want 4", len(fork.Events()))
	}
}

func appendGraphEvent(t *testing.T, store session.Store, s *session.Session, event *core.Event) {
	t.Helper()
	if err := store.Append(context.Background(), s, event); err != nil {
		t.Fatal(err)
	}
}

func message(text string) *core.Message {
	msg := core.AssistantText(text)
	return &msg
}

func messageTextsForMerge(messages []core.Message) []string {
	out := make([]string, len(messages))
	for i, message := range messages {
		out[i] = message.Text()
	}
	return out
}

func equalMergeStrings(a, b []string) bool {
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
