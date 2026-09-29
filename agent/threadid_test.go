package agent_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// A thread id is a file name, so an id that cannot be one fails the run instead
// of being rewritten: neither Run nor Resume reaches the store or the model.
func TestUnsafeThreadIDFailsTheRun(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	calls := 0
	model := mock.New("m", func(*llm.Request) *llm.Response {
		calls++
		return mock.Text("should not be reached")
	})
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Run(ctx, "hello", agent.OnThread("tenant/a")); err == nil {
		t.Fatal("Run with an unsafe thread id succeeded")
	} else if !strings.Contains(err.Error(), "A-Z") {
		t.Fatalf("error should name the allowed characters, got %v", err)
	}
	if _, err := a.Run(ctx, "hello", agent.OnThread("")); err == nil {
		t.Fatal("Run with an empty thread id succeeded")
	}
	if calls != 0 {
		t.Fatalf("the model ran %d times for rejected ids, want 0", calls)
	}
	// Nothing was checkpointed either: the run failed before touching the store.
	if entries, err := os.ReadDir(dir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Errorf("rejected runs wrote %v to the store directory", entries)
	}

	if _, err := a.Resume(ctx, "tenant/a", agent.Allow("d1")); err == nil || !strings.Contains(err.Error(), "thread id") {
		t.Fatalf("Resume(\"tenant/a\") = %v, want the thread id error", err)
	}
}

// A generated id (what a run gets when OnThread is omitted) satisfies the same
// rule, so ordinary use never meets the new error.
func TestGeneratedThreadIDPassesCheck(t *testing.T) {
	ctx := context.Background()
	a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text("done")
	})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "hello"); err != nil {
		t.Fatalf("run without OnThread failed the thread id check: %v", err)
	}
}
