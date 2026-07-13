package checkpoint_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func TestFileCrossInstanceResume(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// "Process 1" writes two checkpoints.
	c1, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Save(ctx, &checkpoint.Checkpoint{
		ID: "cp1", ThreadID: "job-1", Step: 0,
		State: core.State{Messages: []core.Message{core.UserText("记住:代号天狼星")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c1.Save(ctx, &checkpoint.Checkpoint{
		ID: "cp2", ThreadID: "job-1", Step: 1,
		State: core.State{KV: map[string]any{"k": "v"}},
	}); err != nil {
		t.Fatal(err)
	}

	// "Process 2": a fresh instance over the same dir sees the persisted state.
	c2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := c2.Latest(ctx, "job-1")
	if err != nil || latest == nil {
		t.Fatalf("latest err=%v cp=%v", err, latest)
	}
	if latest.ID != "cp2" || latest.Step != 1 {
		t.Fatalf("latest = %+v, want cp2/step1", latest)
	}
	hist, _ := c2.History(ctx, "job-1")
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	loaded, err := c2.Load(ctx, "job-1", "cp1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Messages[0].Text() != "记住:代号天狼星" {
		t.Fatalf("message did not round-trip through the file: %q", loaded.State.Messages[0].Text())
	}
}

func TestFileLatestEmpty(t *testing.T) {
	c, _ := checkpoint.NewFile(t.TempDir())
	got, err := c.Latest(context.Background(), "none")
	if err != nil || got != nil {
		t.Fatalf("empty thread should be (nil,nil), got (%v,%v)", got, err)
	}
}
