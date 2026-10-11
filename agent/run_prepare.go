package agent

import (
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

type runPreparation struct {
	message        core.Message
	files          core.FileStore
	restoreCurrent bool
	claim          *checkpoint.Claim
	headRevision   uint64
}

func (r *Run) prepare() error {
	if err := r.rc.Context.Err(); err != nil {
		return err
	}
	if r.preparation == nil {
		return nil
	}

	var (
		state        *core.State
		err          error
		coordinator  checkpoint.ThreadCoordinator
		claim        = copyClaim(r.preparation.claim)
		headRevision = r.preparation.headRevision
	)
	if coordinated, ok := r.agent.store.(checkpoint.ThreadCoordinator); ok {
		coordinator = coordinated
	}
	if r.preparation.restoreCurrent {
		if coordinator != nil {
			begun, beginErr := coordinator.BeginRun(r.rc.Context, checkpoint.BeginRunRequest{
				ThreadID: r.ThreadID,
				Owner:    r.ID,
				Lease:    checkpoint.DefaultLease,
			})
			if beginErr != nil {
				return beginErr
			}
			claim = &begun.Claim
			r.rc.setDurableClaim(claim)
			if begun.Checkpoint != nil {
				headRevision = begun.Checkpoint.Revision
			}
			state, err = restoreCheckpoint(begun.Checkpoint, r.preparation.files)
		} else {
			state, err = r.agent.restore(r.rc.Context, r.ThreadID, r.preparation.files)
		}
		if err != nil {
			return err
		}
		if r.preparation.files != nil {
			state.Files = r.preparation.files
		} else if state.Files == nil {
			state.Files = vfs.NewInState()
		}
		if r.preparation.message.Role != "" {
			state.Messages = append(state.Messages, r.preparation.message)
		}
		r.rc.State = state
	}
	if coordinator != nil && claim != nil {
		guard := newWriteGuard(coordinator, *claim, headRevision, checkpoint.DefaultLease)
		r.rc.setDurableGuard(guard)
		if err := guard.start(r.rc.Context, r.cancel); err != nil {
			return err
		}
	}
	return nil
}
