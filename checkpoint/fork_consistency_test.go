package checkpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

func TestSnapshotConsistencyForkNested(t *testing.T) {
	parent := &checkpoint.Checkpoint{
		ID: "parent", ThreadID: "parent-thread", Step: 5, Revision: 7,
		State: core.State{
			Messages: []core.Message{{
				Role: core.RoleUser,
				Parts: []core.Part{
					core.Image{MIME: "image/png", Data: []byte("image")},
					core.ToolCall{ID: "call1", Name: "lookup", Args: json.RawMessage(`{"key":"before"}`)},
				},
			}},
			Todos: []core.Todo{{ID: "todo1", Title: "before", Status: "pending"}},
			KV: map[string]any{
				"nested": map[string]any{"value": "before"},
				"bytes":  []byte("bytes"),
			},
		},
		FileSnapshot: map[string][]byte{"note.txt": []byte("file")},
	}

	child, err := checkpoint.Fork(parent, "child-thread", "child")
	if err != nil {
		t.Fatal(err)
	}
	if child.ID != "child" || child.ThreadID != "child-thread" || child.ParentID != "parent" {
		t.Fatalf("fork link = %+v", child)
	}
	if child.Step != parent.Step || child.Revision != 0 {
		t.Fatalf("fork position = step %d revision %d, want step %d revision 0", child.Step, child.Revision, parent.Step)
	}

	child.State.KV["nested"].(map[string]any)["value"] = "changed"
	child.State.KV["bytes"].([]byte)[0] = 'X'
	image := child.State.Messages[0].Parts[0].(core.Image)
	image.Data[0] = 'X'
	call := child.State.Messages[0].Parts[1].(core.ToolCall)
	call.Args[0] = 'X'
	child.State.Todos[0].Title = "changed"
	child.FileSnapshot["note.txt"][0] = 'X'

	if got := parent.State.KV["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("parent nested KV = %v, want before", got)
	}
	if got := string(parent.State.KV["bytes"].([]byte)); got != "bytes" {
		t.Fatalf("parent KV bytes = %q, want bytes", got)
	}
	if got := string(parent.State.Messages[0].Parts[0].(core.Image).Data); got != "image" {
		t.Fatalf("parent image data = %q, want image", got)
	}
	if got := string(parent.State.Messages[0].Parts[1].(core.ToolCall).Args); got != `{"key":"before"}` {
		t.Fatalf("parent tool args = %q", got)
	}
	if got := parent.State.Todos[0].Title; got != "before" {
		t.Fatalf("parent todo title = %q, want before", got)
	}
	if got := string(parent.FileSnapshot["note.txt"]); got != "file" {
		t.Fatalf("parent file snapshot = %q, want file", got)
	}
}

func TestSnapshotConsistencyForkFiles(t *testing.T) {
	ctx := context.Background()
	active := vfs.NewInState()
	if err := active.Write("note.txt", []byte("active-parent")); err != nil {
		t.Fatal(err)
	}
	activeChild, err := checkpoint.Fork(&checkpoint.Checkpoint{
		ID: "active-parent", ThreadID: "active-parent-thread", State: core.State{Files: active},
	}, "active-child-thread", "active-child")
	if err != nil {
		t.Fatal(err)
	}
	if activeChild.State.Files != nil || string(activeChild.FileSnapshot["note.txt"]) != "active-parent" {
		t.Fatalf("active files were not frozen: %+v", activeChild)
	}
	if err := active.Write("note.txt", []byte("active-after-fork")); err != nil {
		t.Fatal(err)
	}
	if got := string(activeChild.FileSnapshot["note.txt"]); got != "active-parent" {
		t.Fatalf("forked active file = %q, want active-parent", got)
	}
	activeChild.FileSnapshot["note.txt"][0] = 'X'
	activeData, err := active.Read("note.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(activeData); got != "active-after-fork" {
		t.Fatalf("active parent file = %q, want active-after-fork", got)
	}

	dir := t.TempDir()
	store := newFileStore(t, dir)
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("parent")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, &checkpoint.Checkpoint{
		ID: "parent", ThreadID: "parent-thread", State: core.State{Files: files},
	}); err != nil {
		t.Fatal(err)
	}

	parent, err := store.Latest(ctx, "parent-thread")
	if err != nil {
		t.Fatal(err)
	}
	child, err := checkpoint.Fork(parent, "child-thread", "child")
	if err != nil {
		t.Fatal(err)
	}
	if child.State.Files != nil || len(child.FileSnapshot) != 1 {
		t.Fatalf("forked files before save = %+v", child)
	}
	child.FileSnapshot["note.txt"] = []byte("child")
	if err := store.Save(ctx, child); err != nil {
		t.Fatal(err)
	}

	reopened := newFileStore(t, dir)
	storedParent, err := reopened.Latest(ctx, "parent-thread")
	if err != nil {
		t.Fatal(err)
	}
	storedChild, err := reopened.Latest(ctx, "child-thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(storedParent.FileSnapshot) != 1 || string(storedParent.FileSnapshot["note.txt"]) != "parent" {
		t.Fatalf("parent files = %v, want one unchanged file", storedParent.FileSnapshot)
	}
	if len(storedChild.FileSnapshot) != 1 || string(storedChild.FileSnapshot["note.txt"]) != "child" {
		t.Fatalf("child files = %v, want one independent file", storedChild.FileSnapshot)
	}
}

func TestSnapshotConsistencyForkInvalid(t *testing.T) {
	tests := []struct {
		name         string
		parent       func() *checkpoint.Checkpoint
		threadID     string
		checkpointID string
		wantCloneErr bool
	}{
		{name: "nil parent", parent: func() *checkpoint.Checkpoint { return nil }, threadID: "child-thread", checkpointID: "child"},
		{name: "empty target thread", parent: forkTestParent, checkpointID: "child"},
		{name: "invalid target thread", parent: forkTestParent, threadID: "child/thread", checkpointID: "child"},
		{name: "same target thread", parent: forkTestParent, threadID: "parent-thread", checkpointID: "child"},
		{name: "empty checkpoint ID", parent: forkTestParent, threadID: "child-thread"},
		{name: "same checkpoint ID", parent: forkTestParent, threadID: "child-thread", checkpointID: "parent"},
		{
			name: "uncloneable state",
			parent: func() *checkpoint.Checkpoint {
				parent := forkTestParent()
				parent.State.KV["bad"] = make(chan int)
				return parent
			},
			threadID: "child-thread", checkpointID: "child", wantCloneErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parent := tc.parent()
			child, err := checkpoint.Fork(parent, tc.threadID, tc.checkpointID)
			if err == nil {
				t.Fatal("Fork returned nil error")
			}
			if child != nil {
				t.Fatalf("Fork returned partial checkpoint: %+v", child)
			}
			if tc.wantCloneErr && !errors.Is(err, core.ErrStateNotCloneable) {
				t.Fatalf("error = %v, want ErrStateNotCloneable", err)
			}
			if parent != nil {
				assertForkTestParentUnchanged(t, parent)
			}
		})
	}
}

func TestSnapshotConsistencyForkDropsPause(t *testing.T) {
	parent := forkTestParent()
	child, err := checkpoint.Fork(parent, "child-thread", "child")
	if err != nil {
		t.Fatal(err)
	}
	if child.Pending != nil || child.Pause != nil {
		t.Fatalf("fork inherited pause state: pending=%+v pause=%+v", child.Pending, child.Pause)
	}
	if child.Revision != 0 {
		t.Fatalf("fork revision = %d, want 0", child.Revision)
	}
	assertForkTestParentUnchanged(t, parent)
}

func forkTestParent() *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID: "parent", ThreadID: "parent-thread", Step: 3, Revision: 9,
		State: core.State{KV: map[string]any{"nested": map[string]any{"value": "before"}}},
		Pending: &checkpoint.PendingHITL{
			Step: 3,
			Pending: []core.ToolCall{{
				ID: "call1", Name: "danger", Args: json.RawMessage(`{"approved":false}`),
			}},
		},
		Pause: &checkpoint.Pause{
			ID: "pause1", Phase: "before_tool", Reason: "approval required", Recovery: checkpoint.RecoveryResumeTools,
		},
		FileSnapshot: map[string][]byte{"note.txt": []byte("parent")},
	}
}

func assertForkTestParentUnchanged(t *testing.T, parent *checkpoint.Checkpoint) {
	t.Helper()
	if parent.ID != "parent" || parent.ThreadID != "parent-thread" || parent.Step != 3 || parent.Revision != 9 {
		t.Fatalf("parent metadata changed: %+v", parent)
	}
	if got := parent.State.KV["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("parent nested KV = %v, want before", got)
	}
	if parent.Pending == nil || parent.Pending.Step != 3 || len(parent.Pending.Pending) != 1 {
		t.Fatalf("parent pending changed: %+v", parent.Pending)
	}
	if got := string(parent.Pending.Pending[0].Args); got != `{"approved":false}` {
		t.Fatalf("parent pending args = %q", got)
	}
	if parent.Pause == nil || parent.Pause.ID != "pause1" || parent.Pause.Reason != "approval required" {
		t.Fatalf("parent pause changed: %+v", parent.Pause)
	}
	if got := string(parent.FileSnapshot["note.txt"]); got != "parent" {
		t.Fatalf("parent file snapshot = %q, want parent", got)
	}
}

// TestForkRejectsInFlight keeps a branch from copying a batch that was planned
// but never produced results: the branch would inherit work whose outcome only
// the parent thread's ledger can explain.
func TestForkRejectsInFlight(t *testing.T) {
	parent := inFlightCheckpoint("parent-thread", "parent")
	child, err := checkpoint.Fork(parent, "child-thread", "child")
	if !errors.Is(err, checkpoint.ErrInFlightConflict) {
		t.Fatalf("Fork error = %v, want ErrInFlightConflict", err)
	}
	if child != nil {
		t.Fatalf("Fork returned a child for an in-flight parent: %+v", child)
	}
	if parent.InFlight == nil || len(parent.InFlight.Calls) != 1 {
		t.Fatalf("rejected fork changed the parent batch: %+v", parent.InFlight)
	}

	plain := inFlightCheckpoint("parent-thread", "parent")
	plain.InFlight = nil
	forked, err := checkpoint.Fork(plain, "child-thread", "child")
	if err != nil {
		t.Fatalf("Fork without an in-flight batch: %v", err)
	}
	if forked.InFlight != nil {
		t.Fatalf("fork inherited an in-flight batch: %+v", forked.InFlight)
	}
}
