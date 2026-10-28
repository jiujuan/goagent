package agent

import (
	"errors"
	"slices"

	"github.com/jiujuan/goagent/core"
)

type runFinishCall struct {
	hook   RunFinisher
	result core.Result
}

func cloneResult(res core.Result) (core.Result, error) {
	message, err := core.CloneMessage(res.Message)
	if err != nil {
		return core.Result{}, err
	}
	return core.Result{Message: message}, nil
}

// All fallible copies precede freezing and every hook sees its own result.
func (r *Run) prepareOutcomeCopies(res core.Result, hooksEligible bool) (core.Result, core.Result, []runFinishCall, error) {
	frozen, err := cloneResult(res)
	tail, tailErr := cloneResult(res)
	err = errors.Join(err, tailErr)
	var calls []runFinishCall
	if hooksEligible {
		for _, middleware := range r.agent.cfg.middleware {
			if hook, ok := middleware.(RunFinisher); ok {
				copy, copyErr := cloneResult(res)
				err = errors.Join(err, copyErr)
				calls = append(calls, runFinishCall{hook: hook, result: copy})
			}
		}
	}
	if err != nil {
		frozen, tail = core.Result{}, core.Result{}
		for i := range calls {
			calls[i].result = core.Result{}
		}
	}
	return frozen, tail, calls, err
}

// settleExecution belongs exclusively to the producer (or the prepared cancel
// owner). Resource I/O, hooks and observation delivery never hold Run locks.
func (r *Run) settleExecution(out runOutcome, rootStarted, hooksEligible bool) {
	r.rc.steering.close(out.inputCloseReason())
	boundaryID, boundaryRev, boundaryOK := r.rc.committedBoundary()
	cleanupErr := r.rc.releaseDurableClaim()
	r.releaseAdmission()
	res, err, paused := terminalOutcome(out, cleanupErr)
	frozen, tailResult, calls, copyErr := r.prepareOutcomeCopies(res, hooksEligible && !paused)
	if copyErr != nil {
		err = errors.Join(err, copyErr)
		paused = false
	}
	tail := r.terminalTail(out, tailResult, err, paused, rootStarted)
	r.freezeTerminal(boundaryID, boundaryRev, boundaryOK, frozen, err, paused)
	r.agent.finishRun(r.rc, calls, err)
	r.sealEvents(tail)
	close(r.done)
}

func (r *Run) terminalTail(out runOutcome, res core.Result, err error, paused, rootStarted bool) []core.Event {
	tail := make([]core.Event, 0, 2)
	if rootStarted {
		task := r.rc.taskInfo()
		statusOut := out
		statusOut.Err = err
		if paused {
			statusOut.Control = core.Directive{Kind: core.Interrupt}
		}
		tail = append(tail, core.TaskDone{TaskID: task.ID, Kind: string(task.Kind), Status: taskStatus(statusOut), Err: err})
	}
	switch {
	case err != nil:
		tail = append(tail, core.RunFailed{Err: err})
	case paused:
		pending := slices.Clone(out.Pending)
		for i := range pending {
			pending[i].Args = slices.Clone(pending[i].Args)
		}
		tail = append(tail, core.Interrupted{Pending: pending, Phase: out.Phase, Reason: out.Reason, Recovery: out.Recovery})
	default:
		tail = append(tail, core.RunDone{Result: res})
	}
	return tail
}

// res is already normalized and exclusively owned by the settlement pipeline.
func (r *Run) freezeTerminal(boundaryID string, boundaryRev uint64, boundaryOK bool, res core.Result, err error, paused bool) {
	turnID := r.rc.runTurnID()
	r.terminalMu.Lock()
	defer r.terminalMu.Unlock()
	r.terminalFrozen = true
	r.terminalPaused = paused
	r.terminalResult = res
	r.terminalErr = err
	r.terminalThreadID = r.identity.threadID
	r.terminalRunID = r.identity.runID
	r.terminalTurnID = turnID
	r.boundaryID, r.boundaryRev, r.boundaryOK = boundaryID, boundaryRev, boundaryOK
}

func (r *Run) settledCopy() (core.Result, error) { return r.FrozenOutcome() }
