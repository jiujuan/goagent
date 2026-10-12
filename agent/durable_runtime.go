package agent

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"

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
	guard          *writeGuard
	approvalEvents []core.ApprovalDecided

	// boundary is the highest-revision checkpoint this run committed
	// successfully, with the revision the store assigned it. A failed commit
	// never touches it, so the pair always identifies a visible state of THIS
	// run (ADR-0033 D4).
	boundaryID       string
	boundaryRevision uint64
	boundarySet      bool

	// lineageMu is this run's shared commit mutex. One commit at a time reads the
	// run's visible sequence, stamps its own parent edge, identity and creation
	// time, and — only on success — advances that sequence, so parallel branches
	// extend one history instead of each inventing a sibling from a stale head
	// (ADR-0034 D2). Lock order is lineageMu, then the write guard's own store
	// mutex, then the store; claim renewal never takes lineageMu, so a renewal
	// cannot queue behind a commit that is still assembling.
	lineageMu sync.Mutex
	headRef   *checkpoint.Ref

	// headRevision is the store revision of headRef, when the store reports one.
	// An idempotent confirmation of an older record returns that older revision,
	// so the sequence only moves forward on a strictly higher one.
	headRevision    uint64
	headRevisionSet bool

	// assembled holds the checkpoints this run has already given a lineage. An
	// empty parent is a determined state for a root record, so re-submitting one
	// must not be read as "not assembled yet". The map keeps the pointers it
	// stamped, so an address cannot be recycled behind its back.
	assembled map[*checkpoint.Checkpoint]struct{}
	turnID    string
}

// lineageNowUTC is the clock a run stamps a freshly constructed checkpoint with.
// Tests replace it so a constructed record stays deterministic.
var lineageNowUTC = func() time.Time { return time.Now().UTC() }

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

// noteCommittedBoundary records the checkpoint the store just made visible and
// the revision it assigned. Only a successful durable commit reaches this.
// Parallel branches share one durable state and commit concurrently, so a
// receipt can arrive after a later commit has already been recorded: the
// boundary only moves forward to a higher revision, and the ID and revision are
// replaced together so no reader can pair one commit's ID with another's
// revision.
func (rc *RunContext) noteCommittedBoundary(id string, revision uint64) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.boundarySet && revision <= d.boundaryRevision {
		return
	}
	d.boundaryID, d.boundaryRevision, d.boundarySet = id, revision, true
}

// committedBoundary returns the highest-revision durable commit this run made —
// inherited head checkpoints are not part of it, so a run that never committed
// anything reports no boundary instead of pretending one.
func (rc *RunContext) committedBoundary() (id string, revision uint64, ok bool) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.boundaryID, d.boundaryRevision, d.boundarySet
}

// setLineageStart pins the parent edge and turn a run continues from. The
// coordinator path passes the head BeginRun resolved, the plain-store path the
// head it restored, and a resume the paused checkpoint's own ref together with
// the turn that pause was recorded under. A fresh thread has no head, so its
// first commit stays a root.
func (rc *RunContext) setLineageStart(head *checkpoint.Ref, turnID string) {
	d := rc.ensureDurableState()
	d.lineageMu.Lock()
	defer d.lineageMu.Unlock()
	if head != nil && head.ThreadID != "" && head.CheckpointID != "" {
		copied := *head
		d.headRef = &copied
	}
	d.turnID = turnID
}

// runTurnID reports the turn this run stamps its commits with, which is "" until
// the run actually starts and an empty value on a checkpoint written before run
// identity existed.
func (rc *RunContext) runTurnID() string {
	d := rc.ensureDurableState()
	d.lineageMu.Lock()
	defer d.lineageMu.Unlock()
	return d.turnID
}

// applyLineageLocked gives a checkpoint about to be committed its parent edge,
// identity and creation time. The caller holds lineageMu. A record this run has
// already assembled — a retry of a committed one — is left exactly as it is, so
// the store still sees the original content, and a parent edge the caller chose
// up front, such as a fork's cross-thread source, is never overwritten.
func (rc *RunContext) applyLineageLocked(cp *checkpoint.Checkpoint) {
	d := rc.ensureDurableState()
	if _, done := d.assembled[cp]; done {
		return
	}
	// A record is never its own parent: the head this run just advanced to is the
	// record itself when an already committed checkpoint is rebuilt from the same
	// request, and that retry must keep the content the store already has.
	if cp.ParentID == "" && cp.ParentRef == nil && d.headRef != nil && d.headRef.CheckpointID != cp.ID {
		cp.ParentID = d.headRef.CheckpointID
		cp.ParentRef = &checkpoint.Ref{ThreadID: d.headRef.ThreadID, CheckpointID: d.headRef.CheckpointID}
	}
	if cp.RunID == "" {
		cp.RunID = rc.RunID
	}
	if cp.TurnID == "" {
		cp.TurnID = d.turnID
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = lineageNowUTC()
	}
	if d.assembled == nil {
		d.assembled = make(map[*checkpoint.Checkpoint]struct{}, 4)
	}
	d.assembled[cp] = struct{}{}
}

// advanceLineageLocked records the checkpoint the store just made visible as the
// parent for this run's next commit. Only a successful commit reaches it, so a
// failed one leaves the sequence pointing at the previous visible state and can
// never be used as a parent. A receipt carrying a revision this run has already
// passed is an idempotent confirmation of an older record, so the sequence does
// not move back onto it.
func (rc *RunContext) advanceLineageLocked(cp *checkpoint.Checkpoint, receipt checkpoint.CommitReceipt) {
	if cp.ID == "" {
		return
	}
	d := rc.ensureDurableState()
	if receipt.Revision > 0 {
		if d.headRevisionSet && receipt.Revision <= d.headRevision {
			return
		}
		d.headRevision = receipt.Revision
		d.headRevisionSet = true
	}
	d.headRef = &checkpoint.Ref{ThreadID: cp.ThreadID, CheckpointID: cp.ID}
}

func (rc *RunContext) setDurableClaim(claim *checkpoint.Claim) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	d.claim = copyClaim(claim)
	d.mu.Unlock()
}

func (rc *RunContext) setDurableGuard(guard *writeGuard) {
	d := rc.ensureDurableState()
	d.mu.Lock()
	d.guard = guard
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
//
// The whole commit runs under this run's lineage mutex: the parent edge, run and
// turn identity and creation time are chosen from the sequence as it stands, and
// only a commit the store accepted advances that sequence.
func (rc *RunContext) commitCheckpoint(cp *checkpoint.Checkpoint, releaseClaim bool, extra ...checkpoint.ToolExecution) error {
	if rc.Store == nil {
		return nil
	}
	d := rc.ensureDurableState()
	d.lineageMu.Lock()
	defer d.lineageMu.Unlock()
	rc.applyLineageLocked(cp)
	receipt, err := rc.commitCheckpointLocked(cp, releaseClaim, extra...)
	if err != nil {
		return err
	}
	rc.advanceLineageLocked(cp, receipt)
	return nil
}

func (rc *RunContext) commitCheckpointLocked(cp *checkpoint.Checkpoint, releaseClaim bool, extra ...checkpoint.ToolExecution) (checkpoint.CommitReceipt, error) {
	durable, ok := rc.durableStore()
	if !ok {
		if err := rc.Store.Save(rc.Context, cp); err != nil {
			return checkpoint.CommitReceipt{}, err
		}
		// A store without receipts cannot report which revision became visible, so
		// the sequence just follows the write order.
		return checkpoint.CommitReceipt{}, nil
	}

	d := rc.ensureDurableState()
	d.mu.Lock()
	acks := append([]string(nil), d.ackMessageIDs...)
	claim := copyClaim(d.claim)
	guard := d.guard
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
	req := checkpoint.CommitRequest{
		Checkpoint:     cp,
		AckMessageIDs:  acks,
		Claim:          claim,
		Lease:          checkpoint.DefaultLease,
		ReleaseClaim:   releaseClaim,
		ToolExecutions: committed,
	}
	var (
		receipt checkpoint.CommitReceipt
		err     error
	)
	if guard != nil {
		receipt, err = guard.commit(ctx, req)
	} else {
		receipt, err = durable.Commit(ctx, req)
	}
	if err != nil {
		return checkpoint.CommitReceipt{}, err
	}
	rc.noteCommittedBoundary(cp.ID, receipt.Revision)

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
		if current, ok := d.executions[id]; ok && reflect.DeepEqual(current, executions[id]) {
			delete(d.executions, id)
		}
	}
	if guard != nil {
		if receipt.Claim != nil {
			d.claim = copyClaim(receipt.Claim)
		}
	} else if releaseClaim {
		d.claim = nil
	} else {
		d.claim = copyClaim(receipt.Claim)
	}
	d.mu.Unlock()
	return receipt, nil
}

func (rc *RunContext) releaseDurableClaim() error {
	durable, ok := rc.durableStore()
	if !ok {
		return nil
	}
	d := rc.ensureDurableState()
	d.mu.Lock()
	guard := d.guard
	d.mu.Unlock()
	if guard != nil {
		err := guard.stopAndRelease(rc.Context)
		if err == nil {
			d.mu.Lock()
			if d.guard == guard {
				d.guard = nil
				d.claim = nil
			}
			d.mu.Unlock()
		}
		return err
	}
	claim := rc.durableClaim()
	if claim == nil {
		return nil
	}
	ctx := rc.Context
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	if err := durable.ReleaseClaim(ctx, *claim); err != nil {
		return err
	}
	rc.setDurableClaim(nil)
	return nil
}

func copyClaim(in *checkpoint.Claim) *checkpoint.Claim {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
