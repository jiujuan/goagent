package agent

import (
	"reflect"
	"testing"

	"github.com/jiujuan/goagent/core"
)

// These are white-box tests for the deterministic branch-merge helpers that
// back Parallel. They exercise diffKV, mergeKVPatches and the conflict policies
// directly, without spinning up agents.

func TestDiffKV(t *testing.T) {
	base := map[string]any{"keep": 1, "change": "old", "drop": true}
	branch := map[string]any{"keep": 1, "change": "new", "add": 42}

	got := diffKV(base, branch)

	wantDelta := map[string]any{"change": "new", "add": 42}
	if !reflect.DeepEqual(got.Delta, wantDelta) {
		t.Errorf("delta = %v, want %v", got.Delta, wantDelta)
	}
	if !reflect.DeepEqual(got.Delete, []string{"drop"}) {
		t.Errorf("delete = %v, want [drop]", got.Delete)
	}
}

func TestDiffKV_NoChange(t *testing.T) {
	base := map[string]any{"a": 1}
	got := diffKV(base, map[string]any{"a": 1})
	if got.Delta != nil || got.Delete != nil {
		t.Errorf("expected empty patch, got %+v", got)
	}
}

func TestMergeKVPatches_DisjointKeys(t *testing.T) {
	patches := []statePatch{
		{Delta: map[string]any{"a": 1}},
		{Delta: map[string]any{"b": 2}},
	}
	merged, err := mergeKVPatches(patches, RejectStateConflicts)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"a": 1, "b": 2}
	if !reflect.DeepEqual(merged.Delta, want) {
		t.Errorf("merged delta = %v, want %v", merged.Delta, want)
	}
}

func TestMergeKVPatches_IdenticalWriteNoConflict(t *testing.T) {
	// Two branches setting the same key to the SAME value is not a conflict,
	// even under the strict Reject policy.
	patches := []statePatch{
		{Delta: map[string]any{"shared": "same"}},
		{Delta: map[string]any{"shared": "same"}},
	}
	merged, err := mergeKVPatches(patches, RejectStateConflicts)
	if err != nil {
		t.Fatalf("identical writes should not conflict: %v", err)
	}
	if merged.Delta["shared"] != "same" {
		t.Errorf("shared = %v, want same", merged.Delta["shared"])
	}
}

func TestMergeKVPatches_RejectConflict(t *testing.T) {
	patches := []statePatch{
		{Delta: map[string]any{"k": "from0"}},
		{Delta: map[string]any{"k": "from1"}},
	}
	if _, err := mergeKVPatches(patches, RejectStateConflicts); err == nil {
		t.Fatal("expected conflict error under RejectStateConflicts")
	}
}

func TestMergeKVPatches_PreferEarlier(t *testing.T) {
	patches := []statePatch{
		{Delta: map[string]any{"k": "from0"}},
		{Delta: map[string]any{"k": "from1"}},
	}
	merged, err := mergeKVPatches(patches, PreferEarlierBranch)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Delta["k"] != "from0" {
		t.Errorf("k = %v, want from0 (earlier branch wins)", merged.Delta["k"])
	}
}

func TestMergeKVPatches_PreferLater(t *testing.T) {
	patches := []statePatch{
		{Delta: map[string]any{"k": "from0"}},
		{Delta: map[string]any{"k": "from1"}},
	}
	merged, err := mergeKVPatches(patches, PreferLaterBranch)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Delta["k"] != "from1" {
		t.Errorf("k = %v, want from1 (later branch wins)", merged.Delta["k"])
	}
}

func TestMergeKVPatches_DeleteVsSetConflict(t *testing.T) {
	// One branch deletes a key, another sets it: a conflict under Reject.
	patches := []statePatch{
		{Delete: []string{"k"}},
		{Delta: map[string]any{"k": "v"}},
	}
	if _, err := mergeKVPatches(patches, RejectStateConflicts); err == nil {
		t.Fatal("expected delete-vs-set conflict under RejectStateConflicts")
	}
	// PreferLater keeps the set.
	merged, err := mergeKVPatches(patches, PreferLaterBranch)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Delta["k"] != "v" || len(merged.Delete) != 0 {
		t.Errorf("prefer-later delete-vs-set = %+v, want set k=v", merged)
	}
}

func TestApplyKVPatch(t *testing.T) {
	st := &core.State{KV: map[string]any{"old": 1, "gone": 2}}
	applyKVPatch(st, statePatch{Delta: map[string]any{"new": 3}, Delete: []string{"gone"}})
	want := map[string]any{"old": 1, "new": 3}
	if !reflect.DeepEqual(st.KV, want) {
		t.Errorf("KV = %v, want %v", st.KV, want)
	}
}
