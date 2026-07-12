package agent

import (
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

// branchScope is the mutable runtime cursor for one isolated parallel branch.
// Its event tip advances only after Runner has acknowledged persistence.
type branchScope struct {
	mu      sync.RWMutex
	tip     string
	overlay *session.Overlay
}

func newBranchScope(base session.Snapshot) *branchScope {
	return &branchScope{tip: base.Leaf(), overlay: session.NewOverlay(base.State())}
}

func (b *branchScope) parentID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tip
}

func (b *branchScope) tipID() string { return b.parentID() }

func (b *branchScope) state() session.State { return b.overlay }

func (b *branchScope) snapshot(s *session.Session) session.Snapshot {
	b.mu.RLock()
	tip := b.tip
	b.mu.RUnlock()
	return s.SnapshotAt(tip).WithState(b.overlay)
}

func (b *branchScope) accept(event *core.Event) {
	b.mu.Lock()
	if event != nil && !event.Partial {
		b.tip = event.ID
		b.overlay.Apply(event.Actions)
	}
	b.mu.Unlock()
}

func (b *branchScope) patch() session.StatePatch { return b.overlay.Patch() }
