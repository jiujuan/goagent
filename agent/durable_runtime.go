package agent

import (
	"context"
	"sort"
	"sync"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// durableRunState is the in-flight half of a durable commit. The store remains
// authoritative; this object only remembers work that must be included in the
// next successful checkpoint commit.
type durableRunState struct {
	mu sync.Mutex

	ackMessageIDs  []string
	executions     map[string]checkpoint.ToolExecution
	claim          *checkpoint.Claim
	approvalEvents []core.ApprovalDecided
}

func newDurableRunState() *durableRunState {
	return &durableRunState{executions: make(map[string]checkpoint.ToolExecution)}
}

func (rc *RunContext) durableStore() (checkpoint.Durable, bool) {
	if rc == nil || rc.Store == nil {
		return nil, false
	}
	d, ok := rc.Store.(checkpoint.Durable)
	return d, ok
}

func (rc *RunContext) ensureDurableState() *durableRunState {
	if rc.durable == nil {
		rc.durable = newDurableRunState()
	}
	return rc.durable
}

func (rc *RunContext) addInboxAcks(ids []string) {
	if len(ids) == 0 {
		return
	}
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := make(map[string]struct{}, len(d.ackMessageIDs)+len(ids))
	for _, id := range d.ackMessageIDs {
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		d.ackMessageIDs = append(d.ackMessageIDs, id)
	}
}

func (rc *RunContext) addCompletedTool(ex checkpoint.ToolExecution) {
	if ex.InvocationID == "" {
		return
	}
	d := rc.ensureDurableState()
	d.mu.Lock()
	if d.executions == nil {
		d.executions = make(map[string]checkpoint.ToolExecution)
	}
	d.executions[ex.InvocationID] = ex
	d.mu.Unlock()
}

func (rc *RunContext) durableClaim() *checkpoint.Claim {
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	return copyClaim(d.claim)
}

func (rc *RunContext) setDurableClaim(claim *checkpoint.Claim) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	d.claim = copyClaim(claim)
	d.mu.Unlock()
}

func (rc *RunContext) queueApprovalEvent(ev core.ApprovalDecided) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	d.approvalEvents = append(d.approvalEvents, ev)
	d.mu.Unlock()
}

func (rc *RunContext) drainApprovalEvents() []core.ApprovalDecided {
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	out := append([]core.ApprovalDecided(nil), d.approvalEvents...)
	d.approvalEvents = nil
	return out
}

// commitCheckpoint chooses the store's atomic commit path when available. It
// deliberately leaves in-flight acknowledgements and tool outcomes intact on
// any error so a retry can make the exact same durable transition.
func (rc *RunContext) commitCheckpoint(cp *checkpoint.Checkpoint, releaseClaim bool, extra ...checkpoint.ToolExecution) error {
	if rc.Store == nil {
		return nil
	}
	durable, ok := rc.durableStore()
	if !ok {
		return rc.Store.Save(rc.Context, cp)
	}

	d := rc.ensureDurableState()
	d.mu.Lock()
	acks := append([]string(nil), d.ackMessageIDs...)
	claim := copyClaim(d.claim)
	executions := make(map[string]checkpoint.ToolExecution, len(d.executions)+len(extra))
	for id, ex := range d.executions {
		executions[id] = ex
	}
	d.mu.Unlock()
	for _, ex := range extra {
		if ex.InvocationID != "" {
			executions[ex.InvocationID] = ex
		}
	}
	ids := make([]string, 0, len(executions))
	for id := range executions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	committed := make([]checkpoint.ToolExecution, 0, len(ids))
	for _, id := range ids {
		committed = append(committed, executions[id])
	}

	ctx := rc.Context
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	receipt, err := durable.Commit(ctx, checkpoint.CommitRequest{
		Checkpoint:     cp,
		AckMessageIDs:  acks,
		Claim:          claim,
		Lease:          checkpoint.DefaultLease,
		ReleaseClaim:   releaseClaim,
		ToolExecutions: committed,
	})
	if err != nil {
		return err
	}

	d.mu.Lock()
	ackSet := make(map[string]struct{}, len(acks))
	for _, id := range acks {
		ackSet[id] = struct{}{}
	}
	if len(ackSet) > 0 {
		remaining := d.ackMessageIDs[:0]
		for _, id := range d.ackMessageIDs {
			if _, committed := ackSet[id]; !committed {
				remaining = append(remaining, id)
			}
		}
		d.ackMessageIDs = remaining
	}
	for _, id := range ids {
		delete(d.executions, id)
	}
	if releaseClaim {
		d.claim = nil
	} else {
		d.claim = copyClaim(receipt.Claim)
	}
	d.mu.Unlock()
	return nil
}

func (rc *RunContext) releaseDurableClaim() {
	durable, ok := rc.durableStore()
	if !ok {
		return
	}
	claim := rc.durableClaim()
	if claim == nil {
		return
	}
	ctx := rc.Context
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	if err := durable.ReleaseClaim(ctx, *claim); err == nil {
		rc.setDurableClaim(nil)
	}
}

func copyClaim(in *checkpoint.Claim) *checkpoint.Claim {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
