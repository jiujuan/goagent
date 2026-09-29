package vfs_test

import (
	"testing"

	"github.com/jiujuan/goagent/vfs"
)

func TestInStateSnapshotRestore(t *testing.T) {
	s := vfs.NewInState()
	if err := s.Write("a.txt", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := s.Write("b.bin", []byte{0, 7}); err != nil {
		t.Fatal(err)
	}

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot = %v", snap)
	}
	// Snapshot is a defensive copy: mutating it must not affect the store.
	snap["a.txt"][0] = 'X'
	if got, _ := s.Read("a.txt"); string(got) != "one" {
		t.Fatalf("store aliased into snapshot: %q", got)
	}

	other := vfs.NewInState()
	if err := other.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if got, _ := other.Read("a.txt"); string(got) != "Xne" {
		t.Fatalf("restore missed contents: %q", got)
	}
	// Restore replaces wholesale: pre-existing keys vanish.
	if err := other.Write("stale", []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := other.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Read("stale"); err == nil {
		t.Fatal("Restore must replace, not merge")
	}
	// nil map empties.
	if err := other.Restore(nil); err != nil {
		t.Fatal(err)
	}
	if got, err := other.List(""); err == nil && len(got) != 0 {
		t.Fatalf("nil restore should empty the store: %v", got)
	}
}
