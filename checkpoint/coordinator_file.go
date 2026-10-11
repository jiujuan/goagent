package checkpoint

import (
	"context"
	"fmt"
	"time"
)

func (f *File) BeginRun(ctx context.Context, req BeginRunRequest) (BeginRunResult, error) {
	if req.ThreadID == "" || req.Owner == "" {
		return BeginRunResult{}, fmt.Errorf("checkpoint: begin run requires thread ID and owner")
	}

	var result BeginRunResult
	err := f.withThread(ctx, req.ThreadID, func(st *fileState) error {
		now := f.nowTime()
		liveClaim := st.claim != nil && !claimExpired(st.claim, now)
		if liveClaim && st.claim.Owner != req.Owner {
			return ErrClaimConflict
		}

		head := latestFileCheckpoint(st)
		if head != nil && (head.Pending != nil || head.Pause != nil) {
			return ErrResumeRequired
		}
		cloned, err := fileCheckpoint(head, st.blobs)
		if err != nil {
			return err
		}

		var claim Claim
		if liveClaim {
			claim = *st.claim
			claim.ExpiresAt = now.Add(normalizeLease(req.Lease))
		} else {
			revision := uint64(0)
			pauseID := threadRootPauseID
			if head != nil {
				revision = head.Revision
				pauseID = EffectivePauseID(head.Checkpoint)
			}
			claim = Claim{
				ThreadID:     req.ThreadID,
				Revision:     revision,
				PauseID:      pauseID,
				Owner:        req.Owner,
				FencingToken: st.fencing + 1,
				ExpiresAt:    now.Add(normalizeLease(req.Lease)),
			}
		}

		rec := journalRecord{Version: durableFormatVersion, Kind: recordClaim, Claim: &claim}
		if err := f.appendRecordLocked(req.ThreadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		result = BeginRunResult{Claim: claim, Checkpoint: cloned}
		return nil
	})
	return result, err
}

func (f *File) RenewClaim(ctx context.Context, claim Claim, lease time.Duration) (Claim, error) {
	if claim.ThreadID == "" || claim.Owner == "" {
		return Claim{}, fmt.Errorf("checkpoint: renew claim requires thread ID and owner")
	}

	var renewed Claim
	err := f.withThread(ctx, claim.ThreadID, func(st *fileState) error {
		now := f.nowTime()
		if st.claim == nil || claimExpired(st.claim, now) || !sameClaim(*st.claim, claim) {
			return ErrStaleFencingToken
		}
		renewed = *st.claim
		renewed.ExpiresAt = now.Add(normalizeLease(lease))
		rec := journalRecord{Version: durableFormatVersion, Kind: recordClaim, Claim: &renewed}
		if err := f.appendRecordLocked(claim.ThreadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		return nil
	})
	if err != nil {
		return Claim{}, err
	}
	return renewed, nil
}

var _ ThreadCoordinator = (*File)(nil)
