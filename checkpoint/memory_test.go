package checkpoint_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func TestMemorySaveLoadLatest(t *testing.T) {
	ctx := context.Background()
	cp := checkpoint.NewMemory()

	for i := 0; i < 3; i++ {
		err := cp.Save(ctx, &checkpoint.Checkpoint{
			ID:       core.NewID("cp"),
			ThreadID: "t1",
			Step:     i,
			State:    core.State{KV: map[string]any{"step": i}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	latest, err := cp.Latest(ctx, "t1")
	if err != nil || latest == nil {
		t.Fatalf("Latest err=%v cp=%v", err, latest)
	}
	if latest.Step != 2 {
		t.Fatalf("Latest step = %d, want 2", latest.Step)
	}

	hist, _ := cp.History(ctx, "t1")
	if len(hist) != 3 {
		t.Fatalf("History len = %d, want 3", len(hist))
	}

	loaded, err := cp.Load(ctx, "t1", hist[0].ID)
	if err != nil || loaded.Step != 0 {
		t.Fatalf("Load err=%v cp=%v", err, loaded)
	}
}

func TestLatestEmptyThread(t *testing.T) {
	got, err := checkpoint.NewMemory().Latest(context.Background(), "none")
	if err != nil || got != nil {
		t.Fatalf("empty thread should be (nil,nil), got (%v,%v)", got, err)
	}
}

func TestForkBranchesWithParentLink(t *testing.T) {
	parent := &checkpoint.Checkpoint{
		ID:       "cp_parent",
		ThreadID: "t1",
		Step:     5,
		State:    core.State{KV: map[string]any{"k": "v"}, Todos: []core.Todo{{ID: "1"}}},
	}
	child := checkpoint.Fork(parent, "t2", "cp_child")

	if child.ParentID != "cp_parent" {
		t.Fatalf("ParentID = %q, want cp_parent", child.ParentID)
	}
	if child.ThreadID != "t2" || child.Step != 5 {
		t.Fatalf("fork = thread %q step %d, want t2/5", child.ThreadID, child.Step)
	}

	// Mutating the fork must not touch the parent's snapshot.
	child.State.KV["k"] = "changed"
	child.State.Todos = append(child.State.Todos, core.Todo{ID: "2"})
	if parent.State.KV["k"] != "v" {
		t.Fatalf("parent KV mutated: %v", parent.State.KV)
	}
	if len(parent.State.Todos) != 1 {
		t.Fatalf("parent Todos mutated: %v", parent.State.Todos)
	}
}
