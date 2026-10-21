package checkpoint_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func stepSnapshotFixture() *checkpoint.StepSnapshot {
	return &checkpoint.StepSnapshot{
		Step:           3,
		Advertised:     []string{"alpha", "beta"},
		ToolDigest:     "tool-digest-3",
		SettingsDigest: "settings-digest-3",
		Model:          "snapshot-model",
	}
}

func assertStepSnapshot(t *testing.T, got, want *checkpoint.StepSnapshot) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("step snapshot = %+v, want %+v", got, want)
	}
}

func TestStepSnapshotRoundTrip(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store, reopen := newSnapshotContractStore(t, backend)
			want := stepSnapshotFixture()
			cp := &checkpoint.Checkpoint{
				ID:       "snap1",
				ThreadID: "thread1",
				Step:     3,
				State:    core.State{},
				Snapshot: want,
			}
			if err := store.Save(ctx, cp); err != nil {
				t.Fatal(err)
			}

			latest, err := reopen().Latest(ctx, "thread1")
			if err != nil {
				t.Fatal(err)
			}
			if latest == nil {
				t.Fatal("Latest returned no checkpoint")
			}
			assertStepSnapshot(t, latest.Snapshot, want)

			loaded, err := reopen().Load(ctx, "thread1", "snap1")
			if err != nil {
				t.Fatal(err)
			}
			assertStepSnapshot(t, loaded.Snapshot, want)
		})
	}
}

func TestStepSnapshotCloneIsolation(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store, _ := newSnapshotContractStore(t, backend)
			want := stepSnapshotFixture()
			if err := store.Save(ctx, &checkpoint.Checkpoint{
				ID:       "snap1",
				ThreadID: "thread1",
				State:    core.State{},
				Snapshot: want,
			}); err != nil {
				t.Fatal(err)
			}

			first, err := store.Latest(ctx, "thread1")
			if err != nil {
				t.Fatal(err)
			}
			second, err := store.Latest(ctx, "thread1")
			if err != nil {
				t.Fatal(err)
			}
			first.Snapshot.Advertised[0] = "changed"
			first.Snapshot.ToolDigest = "changed"
			first.Snapshot.SettingsDigest = "changed"

			assertStepSnapshot(t, second.Snapshot, want)
			stored, err := store.Latest(ctx, "thread1")
			if err != nil {
				t.Fatal(err)
			}
			assertStepSnapshot(t, stored.Snapshot, want)
		})
	}
}

func stepSnapshotRetryCheckpoint(snapshot *checkpoint.StepSnapshot) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID:       "snap-retry",
		ThreadID: "thread",
		Step:     3,
		State:    core.State{KV: map[string]any{"value": "same"}},
		Snapshot: snapshot,
	}
}

func TestStepSnapshotDigestRetry(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store, _ := newSnapshotContractStore(t, backend)
			durable, ok := store.(checkpoint.Durable)
			if !ok {
				t.Fatalf("%s store does not implement Durable", backend)
			}
			snapshot := stepSnapshotFixture()
			if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
				Checkpoint: stepSnapshotRetryCheckpoint(snapshot),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
				Checkpoint: stepSnapshotRetryCheckpoint(stepSnapshotFixture()),
			}); err != nil {
				t.Fatalf("matching snapshot retry: %v", err)
			}

			changed := stepSnapshotFixture()
			changed.ToolDigest = "tool-digest-changed"
			if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
				Checkpoint: stepSnapshotRetryCheckpoint(changed),
			}); !errors.Is(err, checkpoint.ErrCheckpointConflict) {
				t.Fatalf("changed snapshot retry error = %v, want ErrCheckpointConflict", err)
			}
		})
	}
}

func legacyCheckpoint(value string) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID:       "legacy",
		ThreadID: "thread",
		Step:     2,
		State:    core.State{KV: map[string]any{"value": value}},
	}
}

func TestLegacyCheckpointDigestUnchanged(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store, _ := newSnapshotContractStore(t, backend)
			durable, ok := store.(checkpoint.Durable)
			if !ok {
				t.Fatalf("%s store does not implement Durable", backend)
			}
			if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
				Checkpoint: legacyCheckpoint("same"),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
				Checkpoint: legacyCheckpoint("same"),
			}); err != nil {
				t.Fatalf("legacy retry: %v", err)
			}
		})
	}
}
