package agent

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

// --- Sequential -------------------------------------------------------------

// SequentialAgent runs its sub-agents in order, forwarding every event. It is
// the deterministic counterpart to LLM-driven delegation.
type SequentialAgent struct {
	name string
	subs []Agent
}

// Sequential builds a SequentialAgent.
func Sequential(name string, subs ...Agent) *SequentialAgent {
	return &SequentialAgent{name: name, subs: subs}
}

func (a *SequentialAgent) Name() string        { return a.name }
func (a *SequentialAgent) Description() string { return "runs sub-agents in sequence" }
func (a *SequentialAgent) SubAgents() []Agent  { return a.subs }

func (a *SequentialAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for _, sub := range a.subs {
			if !core.Pipe(sub.Run(ictx.withAgent(sub, "").refreshSnapshot()), yield) {
				return
			}
		}
	}
}

// --- Loop -------------------------------------------------------------------

// LoopAgent runs its sub-agents in sequence repeatedly until a sub-agent emits
// an event with Actions.Escalate set, or MaxIterations is reached (0 = until
// escalation).
type LoopAgent struct {
	name    string
	maxIter int
	subs    []Agent
}

// Loop builds a LoopAgent.
func Loop(name string, maxIterations int, subs ...Agent) *LoopAgent {
	return &LoopAgent{name: name, maxIter: maxIterations, subs: subs}
}

func (a *LoopAgent) Name() string        { return a.name }
func (a *LoopAgent) Description() string { return "runs sub-agents in a loop until escalation" }
func (a *LoopAgent) SubAgents() []Agent  { return a.subs }

func (a *LoopAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for iter := 0; a.maxIter == 0 || iter < a.maxIter; iter++ {
			escalated := false
			for _, sub := range a.subs {
				for ev, err := range sub.Run(ictx.withAgent(sub, "").refreshSnapshot()) {
					if !yield(ev, err) {
						return
					}
					if err == nil && ev != nil && ev.Actions.Escalate {
						escalated = true
					}
				}
				if escalated {
					return
				}
			}
		}
	}
}

// --- Parallel ---------------------------------------------------------------

// ParallelAgent runs sub-agents on isolated event and state branches. Branch
// events are persisted as detached chains, then one merge event publishes their
// logical history and resolved state in declaration order. Per-event ACKs keep
// each branch cursor aligned with Runner persistence.
type ParallelAgent struct {
	name string
	subs []Agent
	opts ParallelOptions
}

// StateConflictPolicy controls deterministic resolution when multiple branches
// change the same state key to different values.
type StateConflictPolicy int

const (
	RejectStateConflicts StateConflictPolicy = iota
	PreferEarlierBranch
	PreferLaterBranch
)

// ParallelOptions configures merge behavior without changing the simple
// Parallel constructor. The zero value rejects conflicting state writes.
type ParallelOptions struct {
	StateConflict StateConflictPolicy
}

// Parallel builds a ParallelAgent.
func Parallel(name string, subs ...Agent) *ParallelAgent {
	return ParallelWithOptions(name, ParallelOptions{}, subs...)
}

// ParallelWithOptions builds a ParallelAgent with explicit merge policy.
func ParallelWithOptions(name string, opts ParallelOptions, subs ...Agent) *ParallelAgent {
	return &ParallelAgent{name: name, subs: subs, opts: opts}
}

func (a *ParallelAgent) Name() string        { return a.name }
func (a *ParallelAgent) Description() string { return "runs sub-agents in parallel" }
func (a *ParallelAgent) SubAgents() []Agent  { return a.subs }

type parallelItem struct {
	ev  *core.Event
	err error
	ack chan struct{}
}

type parallelBranchResult struct {
	tip   string
	patch session.StatePatch
}

func (a *ParallelAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		// Capture one immutable baseline before any child starts. A slow child
		// must not observe events already emitted by a faster sibling.
		base := ictx.refreshSnapshot()
		if len(a.subs) == 0 {
			return
		}
		merged := make(chan parallelItem)
		var wg sync.WaitGroup
		done := make(chan struct{})
		results := make([]parallelBranchResult, len(a.subs))

		for index, sub := range a.subs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				scope := newBranchScope(base.snapshot())
				branch := ictx.Branch
				if branch == "" {
					branch = a.name
				}
				branch += "." + sub.Name()
				child := base.withAgent(sub, branch)
				child.branchScope = scope
				for ev, err := range sub.Run(child) {
					managedHere := ev != nil && !ev.GraphManaged
					if managedHere {
						if ev.ID == "" {
							ev.ID = core.NewID("evt")
						}
						if ev.Branch == "" {
							ev.Branch = branch
						}
						ev.ParentID = scope.parentID()
						ev.Detached = true
						ev.GraphManaged = true
					}
					ack := make(chan struct{})
					select {
					case merged <- parallelItem{ev: ev, err: err, ack: ack}:
					case <-done:
						return
					}
					select {
					case <-ack:
					case <-done:
						return
					}
					if managedHere && err == nil {
						scope.accept(ev)
					}
					if err != nil {
						return
					}
				}
				results[index] = parallelBranchResult{tip: scope.tipID(), patch: scope.patch()}
			}()
		}

		go func() {
			wg.Wait()
			close(merged)
		}()

		defer close(done)
		for it := range merged {
			ok := yield(it.ev, it.err)
			close(it.ack)
			if !ok || it.err != nil {
				return
			}
		}

		actions, err := mergeBranchPatches(results, a.opts.StateConflict)
		if err != nil {
			yield(&core.Event{ID: core.NewID("evt"), InvocationID: ictx.InvocationID, Author: a.name, Err: err}, err)
			return
		}
		parents := make([]string, 0, len(results))
		for _, result := range results {
			if result.tip != "" && result.tip != base.snapshot().Leaf() {
				parents = append(parents, result.tip)
			}
		}
		merge := &core.Event{
			ID:           core.NewID("evt"),
			InvocationID: ictx.InvocationID,
			Author:       a.name,
			Branch:       ictx.Branch,
			ParentID:     base.snapshot().Leaf(),
			Detached:     ictx.branchScope != nil,
			MergeParents: parents,
			Actions:      actions,
			GraphManaged: true,
		}
		if !yield(merge, nil) {
			return
		}
		if ictx.branchScope != nil {
			ictx.branchScope.accept(merge)
		}
	}
}

type branchMutation struct {
	branch int
	delete bool
	value  any
}

func mergeBranchPatches(results []parallelBranchResult, policy StateConflictPolicy) (core.Actions, error) {
	mutations := map[string]branchMutation{}
	for branch, result := range results {
		deletes := map[string]bool{}
		for _, key := range result.patch.Delete {
			deletes[key] = true
			if err := mergeMutation(mutations, key, branchMutation{branch: branch, delete: true}, policy); err != nil {
				return core.Actions{}, err
			}
		}
		keys := slices.Sorted(maps.Keys(result.patch.Delta))
		for _, key := range keys {
			if deletes[key] {
				continue
			}
			if err := mergeMutation(mutations, key, branchMutation{branch: branch, value: result.patch.Delta[key]}, policy); err != nil {
				return core.Actions{}, err
			}
		}
	}
	actions := core.Actions{StateDelta: map[string]any{}}
	keys := slices.Sorted(maps.Keys(mutations))
	for _, key := range keys {
		mutation := mutations[key]
		if mutation.delete {
			actions.StateDelete = append(actions.StateDelete, key)
		} else {
			actions.StateDelta[key] = mutation.value
		}
	}
	if len(actions.StateDelta) == 0 {
		actions.StateDelta = nil
	}
	return actions, nil
}

func mergeMutation(dst map[string]branchMutation, key string, next branchMutation, policy StateConflictPolicy) error {
	current, exists := dst[key]
	if !exists {
		dst[key] = next
		return nil
	}
	if current.delete == next.delete && (current.delete || reflect.DeepEqual(current.value, next.value)) {
		return nil
	}
	switch policy {
	case PreferEarlierBranch:
		return nil
	case PreferLaterBranch:
		dst[key] = next
		return nil
	default:
		return fmt.Errorf("parallel: state key %q conflicts between branches %d and %d", key, current.branch, next.branch)
	}
}

var (
	_ Agent = (*SequentialAgent)(nil)
	_ Agent = (*LoopAgent)(nil)
	_ Agent = (*ParallelAgent)(nil)
)
