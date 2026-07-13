package checkpoint_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func TestMemorySaveLoadLatestHistory(t *testing.T) {
	ctx := context.Background()
	cp := checkpoint.NewMemory()
	for i := 0; i < 3; i++ {
		if err := cp.Save(ctx, &checkpoint.Checkpoint{
			ID: core.NewID("cp"), ThreadID: "t1", Step: i,
			State: core.State{KV: map[string]any{"step": i}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	latest, _ := cp.Latest(ctx, "t1")
	if latest == nil || latest.Step != 2 {
		t.Fatalf("latest = %v", latest)
	}
	hist, _ := cp.History(ctx, "t1")
	if len(hist) != 3 {
		t.Fatalf("history len = %d", len(hist))
	}
	if got, err := cp.Load(ctx, "t1", hist[0].ID); err != nil || got.Step != 0 {
		t.Fatalf("load = %v %v", got, err)
	}
}

func TestForkIsolatesParent(t *testing.T) {
	parent := &checkpoint.Checkpoint{
		ID: "p", ThreadID: "t1", Step: 5,
		State: core.State{KV: map[string]any{"k": "v"}, Todos: []core.Todo{{ID: "1"}}},
	}
	child := checkpoint.Fork(parent, "t2", "c")
	if child.ParentID != "p" || child.ThreadID != "t2" {
		t.Fatalf("fork link wrong: %+v", child)
	}
	child.State.KV["k"] = "changed"
	child.State.Todos = append(child.State.Todos, core.Todo{ID: "2"})
	if parent.State.KV["k"] != "v" || len(parent.State.Todos) != 1 {
		t.Fatalf("parent mutated: %+v", parent.State)
	}
}
