package session_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

func TestFileStoreConcurrentAppendReloadsValidSessions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := store.GetOrCreate(ctx, "app", "user", "left")
	right, _ := store.GetOrCreate(ctx, "app", "user", "right")

	const perSession = 75
	errCh := make(chan error, perSession*2)
	var wg sync.WaitGroup
	appendMany := func(s *session.Session, prefix string) {
		defer wg.Done()
		for i := range perSession {
			msg := core.AssistantText(fmt.Sprintf("%s-%d", prefix, i))
			errCh <- store.Append(ctx, s, &core.Event{Message: &msg})
		}
	}

	wg.Add(2)
	go appendMany(left, "left")
	go appendMany(right, "right")
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	reloaded, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"left", "right"} {
		s, err := reloaded.GetOrCreate(ctx, "app", "user", id)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(s.Messages()); got != perSession {
			t.Fatalf("session %s message count = %d, want %d", id, got, perSession)
		}
		if s.Leaf() == "" {
			t.Fatalf("session %s reloaded with an empty leaf", id)
		}
	}
}
