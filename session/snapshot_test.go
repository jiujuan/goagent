package session

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/core"
)

func TestSnapshotRemainsStableAfterSessionChanges(t *testing.T) {
	s := newSession("app", "user", "session")
	s.commit(&core.Event{
		ID:      "first",
		Message: msg(core.RoleUser, "before"),
		Actions: core.Actions{StateDelta: map[string]any{"phase": "before"}},
	})

	snap := s.Snapshot()
	s.commit(&core.Event{
		ID:      "second",
		Message: msg(core.RoleAssistant, "after"),
		Actions: core.Actions{StateDelta: map[string]any{"phase": "after"}},
	})

	if got := texts(snap.Messages()); !equal(got, []string{"before"}) {
		t.Fatalf("snapshot messages = %v, want [before]", got)
	}
	if got, _ := snap.State().Get("phase"); got != "before" {
		t.Fatalf("snapshot state phase = %v, want before", got)
	}
	if snap.Leaf() != "first" {
		t.Fatalf("snapshot leaf = %q, want first", snap.Leaf())
	}
	if snap.Revision() == s.Revision() {
		t.Fatalf("snapshot revision = live revision = %d after another commit", snap.Revision())
	}
}

func TestBeginInvocationWaitHonorsCancellation(t *testing.T) {
	s := newSession("app", "user", "session")
	release, err := s.BeginInvocation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.BeginInvocation(ctx); err != context.Canceled {
		t.Fatalf("BeginInvocation error = %v, want context.Canceled", err)
	}
}

func TestSnapshotReturnsDetachedFrameworkValues(t *testing.T) {
	s := newSession("app", "user", "session")
	e := &core.Event{ID: "event", Message: msg(core.RoleUser, "original")}
	s.commit(e)

	// Mutating the caller-owned event after commit must not mutate the event log.
	e.Message.Parts[0] = core.Text{Text: "caller mutation"}

	snap := s.Snapshot()
	msgs := snap.Messages()
	msgs[0].Parts[0] = core.Text{Text: "snapshot mutation"}
	events := snap.Events()
	events[0].Message.Parts[0] = core.Text{Text: "event mutation"}

	if got := s.Messages()[0].Text(); got != "original" {
		t.Fatalf("live message = %q, want original", got)
	}
	if got := snap.Messages()[0].Text(); got != "original" {
		t.Fatalf("snapshot message = %q, want original", got)
	}
}

func TestSessionConcurrentAppendStateAndSnapshot(t *testing.T) {
	s := newSession("app", "user", "session")
	const workers = 8
	const iterations = 100

	var wg sync.WaitGroup
	for worker := range workers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				id := fmt.Sprintf("%d-%d", worker, i)
				s.commit(&core.Event{
					ID:      id,
					Message: msg(core.RoleAssistant, id),
					Actions: core.Actions{StateDelta: map[string]any{id: i}},
				})
				s.State().Set("last-reader", id)
				_ = s.Snapshot()
				_ = s.Messages()
				_ = s.Events()
				_ = s.Leaf()
			}
		}()
	}
	wg.Wait()

	if got := len(s.allEvents()); got != workers*iterations {
		t.Fatalf("event count = %d, want %d", got, workers*iterations)
	}
	if got := s.Revision(); got < workers*iterations {
		t.Fatalf("revision = %d, want at least %d", got, workers*iterations)
	}
}
