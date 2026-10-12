package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jiujuan/goagent/core"
)

// memoryDurableState is deliberately owned by Memory rather than a second
// in-memory store. One mutex protects snapshots, inbox messages, claims,
// decisions and tool execution boundaries so Commit is atomic for tests and
// single-process users.
type memoryDurableState struct {
	revision   uint64
	fencing    uint64
	inbox      map[string]InboxMessage
	inboxOrder []string
	claim      *Claim
	decisions  map[string]core.ApprovalDecision
	executions map[string]ToolExecution
	commits    map[string]memoryCommitRecord
}

type memoryCommitRecord struct {
	digest       string
	storedDigest string
	revision     uint64
}

func (r memoryCommitRecord) matches(digest string) bool {
	return digest != "" && (digest == r.digest || digest == r.storedDigest)
}

func (m *Memory) durableLocked(threadID string) *memoryDurableState {
	if st := m.durable[threadID]; st != nil {
		return st
	}
	st := &memoryDurableState{
		inbox:      map[string]InboxMessage{},
		decisions:  map[string]core.ApprovalDecision{},
		executions: map[string]ToolExecution{},
		commits:    map[string]memoryCommitRecord{},
	}
	for _, cp := range m.byThread[threadID] {
		if cp.Revision == 0 {
			st.revision++
			cp.Revision = st.revision
			continue
		}
		if cp.Revision > st.revision {
			st.revision = cp.Revision
		}
	}
	m.durable[threadID] = st
	return st
}

// Deliver records a message once by its producer-provided ID. A retry with the
// same payload is a no-op; a different payload under that ID is rejected rather
// than silently delivering the wrong instruction.
func (m *Memory) Deliver(_ context.Context, threadID string, msg InboxMessage) (DeliveryReceipt, error) {
	if threadID == "" || msg.ID == "" {
		return DeliveryReceipt{}, fmt.Errorf("checkpoint: durable delivery requires thread ID and message ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(threadID)
	if old, ok := st.inbox[msg.ID]; ok {
		same, err := sameInboxMessage(old, msg)
		if err != nil {
			return DeliveryReceipt{}, err
		}
		if !same {
			return DeliveryReceipt{}, fmt.Errorf("checkpoint: inbox message ID %q conflicts with existing payload", msg.ID)
		}
		return DeliveryReceipt{ID: msg.ID, Duplicate: true}, nil
	}
	st.inbox[msg.ID] = msg
	st.inboxOrder = append(st.inboxOrder, msg.ID)
	return DeliveryReceipt{ID: msg.ID}, nil
}

func (m *Memory) Inbox(_ context.Context, threadID string) ([]InboxMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(threadID)
	out := make([]InboxMessage, 0, len(st.inboxOrder))
	for _, id := range st.inboxOrder {
		if msg, ok := st.inbox[id]; ok {
			out = append(out, msg)
		}
	}
	return out, nil
}

func (m *Memory) Commit(_ context.Context, req CommitRequest) (CommitReceipt, error) {
	if req.Checkpoint == nil || req.Checkpoint.ThreadID == "" {
		return CommitReceipt{}, fmt.Errorf("checkpoint: durable commit requires a checkpoint with ThreadID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitLocked(req)
}

func (m *Memory) commitLocked(req CommitRequest) (CommitReceipt, error) {
	cp := req.Checkpoint
	for _, ex := range req.ToolExecutions {
		if ex.InvocationID == "" {
			return CommitReceipt{}, fmt.Errorf("checkpoint: tool execution requires invocation ID")
		}
	}
	stored, err := freezeCheckpoint(cp)
	if err != nil {
		return CommitReceipt{}, err
	}
	executions, err := cloneToolExecutions(req.ToolExecutions)
	if err != nil {
		return CommitReceipt{}, err
	}
	digest, err := commitRequestDigest(stored, req.AckMessageIDs, req.Claim, executions)
	if err != nil {
		return CommitReceipt{}, err
	}

	st := m.durableLocked(cp.ThreadID)
	now := m.nowTime()
	if cp.ID != "" {
		if committed, ok := st.commits[cp.ID]; ok {
			if !committed.matches(digest) {
				return CommitReceipt{}, ErrCheckpointConflict
			}
			cp.Revision = committed.revision
			return CommitReceipt{Revision: committed.revision, Claim: retryReceiptClaim(st, req.Claim, now)}, nil
		}
		for _, old := range m.byThread[cp.ThreadID] {
			if old.ID == cp.ID {
				return CommitReceipt{}, ErrCheckpointConflict
			}
		}
	}
	if err := validateCommitClaim(st, req.Claim, now); err != nil {
		return CommitReceipt{}, err
	}
	if req.ExpectedRevision != nil && currentRevision(m.byThread[cp.ThreadID]) != *req.ExpectedRevision {
		return CommitReceipt{}, ErrRevisionConflict
	}

	revision := st.revision + 1
	stored.Revision = revision
	// A record constructed without a creation time gets one when it first becomes
	// visible. An idempotent retry above returned before this point, so the stored
	// record keeps the time it was first committed with.
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = m.nowTime().UTC()
	}
	storedDigest, err := commitRequestDigest(stored, req.AckMessageIDs, req.Claim, executions)
	if err != nil {
		return CommitReceipt{}, err
	}
	claim := renewedClaim(st, req, now)

	st.revision = revision
	m.byThread[cp.ThreadID] = append(m.byThread[cp.ThreadID], stored)
	for _, id := range req.AckMessageIDs {
		delete(st.inbox, id)
	}
	for _, ex := range executions {
		st.executions[ex.InvocationID] = ex
	}
	if st.claim != nil && claimExpired(st.claim, now) && req.Claim == nil {
		st.claim = nil
	}
	if claim != nil {
		st.claim = cloneClaim(claim)
	}
	if req.ReleaseClaim && req.Claim != nil {
		st.claim = nil
		claim = nil
	}
	if cp.ID != "" {
		st.commits[cp.ID] = memoryCommitRecord{digest: digest, storedDigest: storedDigest, revision: revision}
	}
	cp.Revision = revision
	return CommitReceipt{Revision: revision, Claim: claim}, nil
}

func (m *Memory) Claim(_ context.Context, req ClaimRequest) (Claim, error) {
	if req.ThreadID == "" || req.PauseID == "" || req.Owner == "" {
		return Claim{}, fmt.Errorf("checkpoint: claim requires thread ID, pause ID and owner")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(req.ThreadID)
	cp := latestLocked(m.byThread[req.ThreadID])
	if cp == nil || cp.Revision != req.Revision || EffectivePauseID(cp) != req.PauseID {
		return Claim{}, ErrClaimConflict
	}
	now := m.nowTime()
	if st.claim != nil && !claimExpired(st.claim, now) {
		if st.claim.Owner != req.Owner || st.claim.Revision != req.Revision || st.claim.PauseID != req.PauseID {
			return Claim{}, ErrClaimConflict
		}
		st.claim.ExpiresAt = now.Add(normalizeLease(req.Lease))
		return *st.claim, nil
	}
	st.fencing++
	st.claim = &Claim{
		ThreadID:     req.ThreadID,
		Revision:     req.Revision,
		PauseID:      req.PauseID,
		Owner:        req.Owner,
		FencingToken: st.fencing,
		ExpiresAt:    now.Add(normalizeLease(req.Lease)),
	}
	return *st.claim, nil
}

func (m *Memory) ReleaseClaim(_ context.Context, claim Claim) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(claim.ThreadID)
	if st.claim == nil {
		return nil
	}
	if !sameClaim(*st.claim, claim) {
		return ErrStaleFencingToken
	}
	st.claim = nil
	return nil
}

func (m *Memory) RecordDecision(_ context.Context, threadID string, decision core.ApprovalDecision) (DecisionReceipt, error) {
	if threadID == "" || decision.DecisionID == "" || decision.PauseID == "" || decision.Digest == "" {
		return DecisionReceipt{}, fmt.Errorf("checkpoint: structured decision requires thread ID, decision ID, pause ID and digest")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(threadID)
	if old, ok := st.decisions[decision.DecisionID]; ok {
		if old.Digest != decision.Digest {
			return DecisionReceipt{}, ErrDecisionConflict
		}
		return DecisionReceipt{Decision: old, Duplicate: true}, nil
	}
	st.decisions[decision.DecisionID] = decision
	return DecisionReceipt{Decision: decision}, nil
}

func (m *Memory) Decisions(_ context.Context, threadID, pause string) ([]core.ApprovalDecision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(threadID)
	out := make([]core.ApprovalDecision, 0, len(st.decisions))
	for _, d := range st.decisions {
		if d.PauseID == pause {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DecisionID < out[j].DecisionID })
	return out, nil
}

func (m *Memory) ClaimTool(_ context.Context, req ToolClaimRequest) (ToolExecution, error) {
	if req.ThreadID == "" || req.InvocationID == "" || req.CallID == "" || req.Tool == "" {
		return ToolExecution{}, fmt.Errorf("checkpoint: tool claim requires thread, invocation, call and tool IDs")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(req.ThreadID)
	if err := validateCommitClaim(st, req.Claim, m.nowTime()); err != nil {
		return ToolExecution{}, err
	}
	if old, ok := st.executions[req.InvocationID]; ok {
		switch old.State {
		case ToolCompleted:
			return cloneToolExecution(old)
		case ToolClaimed:
			next := old
			next.State = ToolUnknown
			next.Result = nil
			result, err := cloneToolExecution(next)
			if err != nil {
				return ToolExecution{}, err
			}
			st.executions[req.InvocationID] = next
			return result, ErrToolOutcomeUnknown
		case ToolUnknown:
			result, err := cloneToolExecution(old)
			if err != nil {
				return ToolExecution{}, err
			}
			return result, ErrToolOutcomeUnknown
		case ToolPending:
			next := old
			next.Attempt++
			next.State = ToolClaimed
			if next.IdempotencyKey == "" {
				next.IdempotencyKey = req.IdempotencyKey
			}
			result, err := cloneToolExecution(next)
			if err != nil {
				return ToolExecution{}, err
			}
			st.executions[req.InvocationID] = next
			return result, nil
		}
	}
	ex := ToolExecution{
		InvocationID:   req.InvocationID,
		CallID:         req.CallID,
		Tool:           req.Tool,
		IdempotencyKey: req.IdempotencyKey,
		Attempt:        1,
		State:          ToolClaimed,
	}
	result, err := cloneToolExecution(ex)
	if err != nil {
		return ToolExecution{}, err
	}
	st.executions[req.InvocationID] = ex
	return result, nil
}

func (m *Memory) ToolExecution(_ context.Context, threadID, invocationID string) (ToolExecution, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ex, ok := m.durableLocked(threadID).executions[invocationID]
	if !ok {
		return ToolExecution{}, false, nil
	}
	cloned, err := cloneToolExecution(ex)
	return cloned, true, err
}

func validateCommitClaim(st *memoryDurableState, got *Claim, now time.Time) error {
	if st.claim == nil {
		if got != nil {
			return ErrStaleFencingToken
		}
		return nil
	}
	if claimExpired(st.claim, now) {
		if got != nil {
			return ErrStaleFencingToken
		}
		return nil
	}
	if got == nil {
		return ErrClaimRequired
	}
	if !sameClaim(*st.claim, *got) {
		return ErrStaleFencingToken
	}
	return nil
}

func renewedClaim(st *memoryDurableState, req CommitRequest, now time.Time) *Claim {
	if req.Claim == nil || st.claim == nil {
		return nil
	}
	c := *st.claim
	c.ExpiresAt = now.Add(normalizeLease(req.Lease))
	return &c
}

func retryReceiptClaim(st *memoryDurableState, requested *Claim, now time.Time) *Claim {
	if requested == nil || st.claim == nil || claimExpired(st.claim, now) || !sameClaim(*st.claim, *requested) {
		return nil
	}
	return cloneClaim(st.claim)
}

func currentRevision(checkpoints []*Checkpoint) uint64 {
	head := latestLocked(checkpoints)
	if head == nil {
		return 0
	}
	return head.Revision
}

func normalizeLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return DefaultLease
	}
	return lease
}

func claimExpired(c *Claim, now time.Time) bool { return !now.Before(c.ExpiresAt) }

func sameClaim(a, b Claim) bool {
	return a.ThreadID == b.ThreadID && a.Owner == b.Owner && a.FencingToken == b.FencingToken &&
		a.Revision == b.Revision && a.PauseID == b.PauseID
}

func latestLocked(cps []*Checkpoint) *Checkpoint {
	if len(cps) == 0 {
		return nil
	}
	return cps[len(cps)-1]
}

func cloneClaim(in *Claim) *Claim {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func sameInboxMessage(a, b InboxMessage) (bool, error) {
	aa, err := json.Marshal(a.Message)
	if err != nil {
		return false, err
	}
	bb, err := json.Marshal(b.Message)
	if err != nil {
		return false, err
	}
	return string(aa) == string(bb), nil
}

var _ Durable = (*Memory)(nil)
