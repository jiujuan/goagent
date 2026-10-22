package agent

import (
	"fmt"

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

	// head is the checkpoint this run continues from, and turnID the turn it
	// belongs to. A resume fills both when it claims the paused checkpoint; an
	// ordinary run resolves the head from the store below and opens a new turn.
	head   *checkpoint.Ref
	turnID string
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
		head         = r.preparation.head
		plainHead    *checkpoint.Checkpoint
	)
	if coordinated, ok := r.agent.store.(checkpoint.ThreadCoordinator); ok {
		coordinator = coordinated
	}
	if r.preparation.restoreCurrent {
		if coordinator != nil {
			begun, beginErr := coordinator.BeginRun(r.rc.Context, checkpoint.BeginRunRequest{
				ThreadID: r.identity.threadID,
				Owner:    r.identity.runID,
				Lease:    r.agent.cfg.claimLease,
			})
			if beginErr != nil {
				return beginErr
			}
			claim = &begun.Claim
			r.rc.setDurableClaim(claim)
			if begun.Checkpoint != nil {
				headRevision = begun.Checkpoint.Revision
				head = checkpointRef(begun.Checkpoint)
			}
			state, err = restoreCheckpoint(begun.Checkpoint, r.preparation.files)
		} else {
			state, plainHead, err = r.agent.restore(r.rc.Context, r.identity.threadID, r.preparation.files)
			head = checkpointRef(plainHead)
		}
		if err != nil {
			return err
		}
		// A store without BeginRun cannot refuse a thread whose recorded work is
		// still unfinished, so this path has to refuse it itself: the in-flight
		// batch must be settled by Resume before a new input may start.
		if plainHead != nil && plainHead.InFlight != nil {
			return fmt.Errorf("agent: prepare run on thread %q: head %s carries an unresolved tool batch; resume the thread before starting a new run: %w",
				r.identity.threadID, plainHead.ID, checkpoint.ErrResumeRequired)
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
	// The head and the turn are resolved when execution actually starts, not when
	// the handle was created, so a run never parents onto a head that has since
	// moved. A new input opens a new turn.
	turnID := r.preparation.turnID
	if turnID == "" {
		turnID = core.NewID("turn")
	}
	r.rc.setLineageStart(head, turnID)
	r.rc.setTaskTurn(turnID)
	if coordinator != nil && claim != nil {
		guard := newWriteGuard(coordinator, *claim, headRevision, r.agent.cfg.claimLease)
		r.rc.setDurableGuard(guard)
		if err := guard.start(r.rc.Context, r.cancel); err != nil {
			return err
		}
	}
	return nil
}

// checkpointRef locates a resolved head on its own thread, or reports no head at
// all for a thread that has not committed anything yet.
func checkpointRef(cp *checkpoint.Checkpoint) *checkpoint.Ref {
	if cp == nil || cp.ID == "" {
		return nil
	}
	return &checkpoint.Ref{ThreadID: cp.ThreadID, CheckpointID: cp.ID}
}
