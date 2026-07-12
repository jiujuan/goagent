package session

import (
	"maps"
	"slices"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// StatePatch is the durable mutation produced by a branch-local Overlay.
// Delta replaces values; Delete removes keys. Keys are sorted when exported so
// conflict diagnostics and persisted merge events remain deterministic.
type StatePatch struct {
	Delta  map[string]any
	Delete []string
}

// Overlay is an isolated mutable State initialized from a read-only baseline.
// Parallel branches receive separate overlays, so direct tool writes and event
// actions cannot leak into siblings or the live Session before merge.
type Overlay struct {
	mu      sync.RWMutex
	values  map[string]any
	changed map[string]any
	deleted map[string]struct{}
}

// NewOverlay creates a branch-local state overlay from base.
func NewOverlay(base StateReader) *Overlay {
	values := map[string]any{}
	if base != nil {
		values = base.All()
	}
	return &Overlay{
		values:  values,
		changed: map[string]any{},
		deleted: map[string]struct{}{},
	}
}

func (o *Overlay) Get(key string) (any, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	value, ok := o.values[key]
	return value, ok
}

func (o *Overlay) Set(key string, value any) {
	o.mu.Lock()
	o.values[key] = value
	o.changed[key] = value
	delete(o.deleted, key)
	o.mu.Unlock()
}

func (o *Overlay) Delete(key string) {
	o.mu.Lock()
	delete(o.values, key)
	delete(o.changed, key)
	o.deleted[key] = struct{}{}
	o.mu.Unlock()
}

func (o *Overlay) All() map[string]any {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return maps.Clone(o.values)
}

// Apply folds committed event actions into the overlay before the branch
// continues. This mirrors Session commit semantics without publishing changes.
func (o *Overlay) Apply(actions core.Actions) {
	for _, key := range actions.StateDelete {
		o.Delete(key)
	}
	for key, value := range actions.StateDelta {
		o.Set(key, value)
	}
}

// Patch returns a detached, deterministic description of overlay mutations.
func (o *Overlay) Patch() StatePatch {
	o.mu.RLock()
	defer o.mu.RUnlock()
	deleted := make([]string, 0, len(o.deleted))
	for key := range o.deleted {
		deleted = append(deleted, key)
	}
	slices.Sort(deleted)
	return StatePatch{Delta: maps.Clone(o.changed), Delete: deleted}
}

var _ State = (*Overlay)(nil)
