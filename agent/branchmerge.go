package agent

import (
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/jiujuan/goagent/core"
)

// This file holds the deterministic state-merge used when a Parallel workflow
// folds its branches back together. Each branch runs on an isolated clone of
// State (see RunContext.forBranch), so a branch's KV writes are invisible to its
// siblings. When the branches finish, parallelRunner diffs each branch's KV
// against the pre-fork baseline and folds the resulting patches — in branch
// declaration order — into one deterministic result, resolving same-key
// conflicts by the configured StateConflictPolicy.
//
// v1's port note: main modeled this on an event-sourced Session with per-branch
// Overlays and StatePatches. v2 has no Session; branch state IS core.State.KV,
// so the "overlay" is just the cloned KV map and the "patch" is its diff against
// the baseline. The merge/conflict semantics are otherwise identical.

// statePatch is a branch's KV mutation relative to the pre-fork baseline. Delta
// carries set/changed keys; Delete carries keys the branch removed. Delete is
// kept sorted so merge diagnostics and results are deterministic.
type statePatch struct {
	Delta  map[string]any
	Delete []string
}

// cloneKV returns a shallow copy of a KV map (nil-safe), used to snapshot the
// pre-fork baseline so each branch's writes can be diffed against it.
func cloneKV(kv map[string]any) map[string]any {
	if kv == nil {
		return nil
	}
	out := make(map[string]any, len(kv))
	for k, v := range kv {
		out[k] = v
	}
	return out
}

// diffKV computes the patch that turns baseline into branch. A key present in
// branch with a value not deep-equal to baseline's (or absent from baseline) is
// a Delta; a key in baseline missing from branch is a Delete.
func diffKV(baseline, branch map[string]any) statePatch {
	patch := statePatch{Delta: map[string]any{}}
	for k, v := range branch {
		if old, ok := baseline[k]; !ok || !reflect.DeepEqual(old, v) {
			patch.Delta[k] = v
		}
	}
	for k := range baseline {
		if _, ok := branch[k]; !ok {
			patch.Delete = append(patch.Delete, k)
		}
	}
	slices.Sort(patch.Delete)
	if len(patch.Delta) == 0 {
		patch.Delta = nil
	}
	return patch
}

// branchMutation records which branch last owned a key's value, so a conflict
// error can name the two colliding branches.
type branchMutation struct {
	branch int
	delete bool
	value  any
}

// mergeKVPatches folds per-branch patches in declaration order into a single
// deterministic patch, resolving same-key conflicts per policy. Identical
// writes (same value, or both deletes) never conflict.
func mergeKVPatches(patches []statePatch, policy StateConflictPolicy) (statePatch, error) {
	mutations := map[string]branchMutation{}
	for branch, patch := range patches {
		for _, key := range patch.Delete {
			if err := foldMutation(mutations, key, branchMutation{branch: branch, delete: true}, policy); err != nil {
				return statePatch{}, err
			}
		}
		// Sort keys so folding is order-independent within a branch.
		for _, key := range slices.Sorted(maps.Keys(patch.Delta)) {
			m := branchMutation{branch: branch, value: patch.Delta[key]}
			if err := foldMutation(mutations, key, m, policy); err != nil {
				return statePatch{}, err
			}
		}
	}
	merged := statePatch{Delta: map[string]any{}}
	for _, key := range slices.Sorted(maps.Keys(mutations)) {
		m := mutations[key]
		if m.delete {
			merged.Delete = append(merged.Delete, key)
		} else {
			merged.Delta[key] = m.value
		}
	}
	if len(merged.Delta) == 0 {
		merged.Delta = nil
	}
	return merged, nil
}

// foldMutation merges one branch's mutation for a key into the accumulator,
// applying the conflict policy when two branches touch the same key differently.
func foldMutation(dst map[string]branchMutation, key string, next branchMutation, policy StateConflictPolicy) error {
	current, exists := dst[key]
	if !exists {
		dst[key] = next
		return nil
	}
	// Agreeing writes (both delete, or same value) are not conflicts.
	if current.delete == next.delete && (current.delete || reflect.DeepEqual(current.value, next.value)) {
		return nil
	}
	switch policy {
	case PreferEarlierBranch:
		return nil // keep current (earlier branch)
	case PreferLaterBranch:
		dst[key] = next
		return nil
	default: // RejectStateConflicts
		return fmt.Errorf("parallel: state key %q conflicts between branches %d and %d", key, current.branch, next.branch)
	}
}

// applyKVPatch writes a merged patch into st.KV. Deletes are applied before
// deltas so a key that is both (never produced by mergeKVPatches, but cheap to
// be safe) resolves to the set value.
func applyKVPatch(st *core.State, patch statePatch) {
	if len(patch.Delta) == 0 && len(patch.Delete) == 0 {
		return
	}
	if st.KV == nil && len(patch.Delta) > 0 {
		st.KV = make(map[string]any, len(patch.Delta))
	}
	for _, key := range patch.Delete {
		delete(st.KV, key)
	}
	for key, value := range patch.Delta {
		st.KV[key] = value
	}
}
