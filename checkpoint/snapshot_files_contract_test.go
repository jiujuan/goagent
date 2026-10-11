package checkpoint_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

func TestSnapshotConsistencyFreezeFiles(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			store, _ := newSnapshotContractStore(t, backend)
			files := vfs.NewInState()
			if err := files.Write("note.txt", []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := store.Save(context.Background(), &checkpoint.Checkpoint{
				ID:           "cp1",
				ThreadID:     "thread1",
				State:        core.State{Files: files},
				FileSnapshot: map[string][]byte{"stale.txt": []byte("stale")},
			}); err != nil {
				t.Fatal(err)
			}

			if err := files.Write("note.txt", []byte("after")); err != nil {
				t.Fatal(err)
			}
			if err := files.Write("later.txt", []byte("new")); err != nil {
				t.Fatal(err)
			}

			got, err := store.Latest(context.Background(), "thread1")
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("missing checkpoint")
			}
			if got.State.Files != nil {
				t.Fatal("snapshottable active files must be cleared from the frozen state")
			}
			if len(got.FileSnapshot) != 1 || string(got.FileSnapshot["note.txt"]) != "before" {
				t.Fatalf("frozen files = %v, want only note.txt=before", got.FileSnapshot)
			}
		})
	}
}

func TestSnapshotConsistencyResaveFileSnapshot(t *testing.T) {
	dir := t.TempDir()
	store := newFileStore(t, dir)
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("durable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), &checkpoint.Checkpoint{
		ID:       "source",
		ThreadID: "source-thread",
		State:    core.State{Files: files},
	}); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(context.Background(), "source-thread", "source")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Files != nil || string(loaded.FileSnapshot["note.txt"]) != "durable" {
		t.Fatalf("loaded source = %+v", loaded)
	}
	loaded.ID = "resaved"
	loaded.ThreadID = "target-thread"
	loaded.ParentID = ""
	loaded.Revision = 0
	if err := store.Save(context.Background(), loaded); err != nil {
		t.Fatal(err)
	}

	reopened := newFileStore(t, dir)
	got, err := reopened.Load(context.Background(), "target-thread", "resaved")
	if err != nil {
		t.Fatal(err)
	}
	if got.State.Files != nil || len(got.FileSnapshot) != 1 || string(got.FileSnapshot["note.txt"]) != "durable" {
		t.Fatalf("resaved snapshot = %+v", got)
	}
}

func TestSnapshotConsistencyEmptyFiles(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			store, reopen := newSnapshotContractStore(t, backend)
			ctx := context.Background()
			cases := []struct {
				thread  string
				cp      *checkpoint.Checkpoint
				wantNil bool
			}{
				{
					thread: "active-empty",
					cp: &checkpoint.Checkpoint{
						ID: "cp1", ThreadID: "active-empty", State: core.State{Files: vfs.NewInState()},
						FileSnapshot: map[string][]byte{"stale.txt": []byte("stale")},
					},
				},
				{
					thread: "carried-empty",
					cp: &checkpoint.Checkpoint{
						ID: "cp1", ThreadID: "carried-empty", FileSnapshot: map[string][]byte{},
					},
				},
				{
					thread:  "no-files",
					cp:      &checkpoint.Checkpoint{ID: "cp1", ThreadID: "no-files"},
					wantNil: true,
				},
			}
			for _, tc := range cases {
				if err := store.Save(ctx, tc.cp); err != nil {
					t.Fatalf("Save(%s): %v", tc.thread, err)
				}
			}

			store = reopen()
			for _, tc := range cases {
				got, err := store.Latest(ctx, tc.thread)
				if err != nil {
					t.Fatalf("Latest(%s): %v", tc.thread, err)
				}
				if got == nil {
					t.Fatalf("Latest(%s) returned nil", tc.thread)
				}
				if (got.FileSnapshot == nil) != tc.wantNil {
					t.Fatalf("Latest(%s) FileSnapshot nil = %v, want %v", tc.thread, got.FileSnapshot == nil, tc.wantNil)
				}
				if got.FileSnapshot != nil && len(got.FileSnapshot) != 0 {
					t.Fatalf("Latest(%s) FileSnapshot = %v, want empty", tc.thread, got.FileSnapshot)
				}
			}
		})
	}
}

func TestSnapshotConsistencyFileReadOwnership(t *testing.T) {
	dir := t.TempDir()
	store := newFileStore(t, dir)
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), &checkpoint.Checkpoint{
		ID: "cp1", ThreadID: "thread1", State: core.State{Files: files},
	}); err != nil {
		t.Fatal(err)
	}

	first, err := store.Latest(context.Background(), "thread1")
	if err != nil {
		t.Fatal(err)
	}
	first.FileSnapshot["note.txt"][0] = 'X'
	first.FileSnapshot["added.txt"] = []byte("added")

	second, err := store.Latest(context.Background(), "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.FileSnapshot) != 1 || string(second.FileSnapshot["note.txt"]) != "before" {
		t.Fatalf("stored files changed through read: %v", second.FileSnapshot)
	}
}

func TestSnapshotConsistencyExternalFiles(t *testing.T) {
	ctx := context.Background()
	external := &snapshotExternalFiles{data: map[string][]byte{"note.txt": []byte("external")}}

	memory := checkpoint.NewMemory()
	if err := memory.Save(ctx, &checkpoint.Checkpoint{
		ID: "cp1", ThreadID: "memory-thread", State: core.State{Files: external},
	}); err != nil {
		t.Fatal(err)
	}
	memoryCheckpoint, err := memory.Latest(ctx, "memory-thread")
	if err != nil {
		t.Fatal(err)
	}
	if memoryCheckpoint.FileSnapshot != nil {
		t.Fatalf("external files were presented as a portable snapshot: %v", memoryCheckpoint.FileSnapshot)
	}
	if memoryCheckpoint.State.Files != external {
		t.Fatal("Memory must retain the externally managed file handle")
	}

	fileStore := newFileStore(t, t.TempDir())
	if err := fileStore.Save(ctx, &checkpoint.Checkpoint{
		ID: "cp1", ThreadID: "file-thread", State: core.State{Files: external},
	}); err != nil {
		t.Fatal(err)
	}
	fileCheckpoint, err := fileStore.Latest(ctx, "file-thread")
	if err != nil {
		t.Fatal(err)
	}
	if fileCheckpoint.State.Files != nil || fileCheckpoint.FileSnapshot != nil {
		t.Fatalf("File persisted an external handle as snapshot data: %+v", fileCheckpoint)
	}
}

func newSnapshotContractStore(t *testing.T, backend string) (checkpoint.Checkpointer, func() checkpoint.Checkpointer) {
	t.Helper()
	if backend == "memory" {
		store := checkpoint.NewMemory()
		return store, func() checkpoint.Checkpointer { return store }
	}
	dir := t.TempDir()
	store := newFileStore(t, dir)
	return store, func() checkpoint.Checkpointer { return newFileStore(t, dir) }
}

func newFileStore(t *testing.T, dir string) *checkpoint.File {
	t.Helper()
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type snapshotExternalFiles struct {
	data map[string][]byte
}

func (s *snapshotExternalFiles) Read(path string) ([]byte, error) { return s.data[path], nil }

func (s *snapshotExternalFiles) Write(path string, data []byte) error {
	s.data[path] = data
	return nil
}

func (s *snapshotExternalFiles) List(string) ([]string, error) { return nil, nil }
