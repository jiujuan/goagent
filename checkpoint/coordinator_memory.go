package checkpoint

import (
	"context"
	"fmt"
	"time"
)

const threadRootPauseID = "thread-root"

func (m *Memory) BeginRun(_ context.Context, req BeginRunRequest) (BeginRunResult, error) {
	if req.ThreadID == "" || req.Owner == "" {
		return BeginRunResult{}, fmt.Errorf("checkpoint: begin run requires thread ID and owner")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(req.ThreadID)
	now := m.nowTime()
	liveClaim := st.claim != nil && !claimExpired(st.claim, now)
	if liveClaim && st.claim.Owner != req.Owner {
		return BeginRunResult{}, ErrClaimConflict
	}

	head := latestLocked(m.byThread[req.ThreadID])
	if RequiresResume(head) {
		return BeginRunResult{}, ErrResumeRequired
	}
	cloned, err := cloneCheckpoint(head)
	if err != nil {
		return BeginRunResult{}, err
	}

	if liveClaim {
		claim := *st.claim
		claim.ExpiresAt = now.Add(normalizeLease(req.Lease))
		st.claim = &claim
		return BeginRunResult{Claim: claim, Checkpoint: cloned}, nil
	}

	revision := uint64(0)
	pauseID := threadRootPauseID
	if head != nil {
		revision = head.Revision
		pauseID = EffectivePauseID(head)
	}
	st.fencing++
	claim := Claim{
		ThreadID:     req.ThreadID,
		Revision:     revision,
		PauseID:      pauseID,
		Owner:        req.Owner,
		FencingToken: st.fencing,
		ExpiresAt:    now.Add(normalizeLease(req.Lease)),
	}
	st.claim = &claim
	return BeginRunResult{Claim: claim, Checkpoint: cloned}, nil
}

func (m *Memory) RenewClaim(_ context.Context, claim Claim, lease time.Duration) (Claim, error) {
	if claim.ThreadID == "" || claim.Owner == "" {
		return Claim{}, fmt.Errorf("checkpoint: renew claim requires thread ID and owner")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.durableLocked(claim.ThreadID)
	now := m.nowTime()
	if st.claim == nil || claimExpired(st.claim, now) || !sameClaim(*st.claim, claim) {
		return Claim{}, ErrStaleFencingToken
	}
	renewed := *st.claim
	renewed.ExpiresAt = now.Add(normalizeLease(lease))
	st.claim = &renewed
	return renewed, nil
}

var _ ThreadCoordinator = (*Memory)(nil)
