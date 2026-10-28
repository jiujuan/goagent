package agent

import "context"

type runPhase uint8

const (
	runPrepared runPhase = iota
	runStarting
	runRunning
	runSettling
	runSettled
)

// Cancel requests cancellation of started work. For a prepared run it owns
// synchronous cleanup and memory settlement; it does not execute the runnable.
// Started work remains in flight until its execution owner actually settles.
func (r *Run) Cancel() {
	r.lifecycleMu.Lock()
	phase := r.phase
	if phase == runSettled {
		r.lifecycleMu.Unlock()
		return
	}
	// Register the request before canceling the context outside the lock.
	// The execution owner may reach settlement before that call runs.
	if phase < runSettling && r.cancelErr == nil {
		r.cancelErr = r.rc.Context.Err()
		if r.cancelErr == nil {
			r.cancelErr = context.Canceled
		}
	}
	owner := phase == runPrepared
	if owner {
		r.phase = runSettling
		r.preparedSettlement = true
	}
	wait := !owner && phase == runSettling && r.preparedSettlement
	r.lifecycleMu.Unlock()
	r.cancel()
	r.rc.steering.close(ErrRunStopped)
	if owner {
		r.settleExecution(runOutcome{Err: r.rc.Context.Err()}, false, false)
	} else if wait {
		<-r.done
	}
}

// beginSettlement fixes the cancellation cutoff under the same lock as Cancel.
// Parent context cancellation is visible even before its AfterFunc is scheduled.
func (r *Run) beginSettlement() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.cancelErr == nil {
		r.cancelErr = r.rc.Context.Err()
	}
	r.phase = runSettling
	return r.cancelErr
}
func (r *Run) completeSettlement() {
	r.lifecycleMu.Lock()
	stop := r.stopParent
	r.stopParent = nil
	r.lifecycleMu.Unlock()
	if stop != nil {
		stop()
	}
	r.cancel()
	r.rc.timing.Settled()
	// A callback can be waiting on done. Never join it before publishing done.
	r.lifecycleMu.Lock()
	r.phase = runSettled
	close(r.done)
	r.lifecycleMu.Unlock()
}

// activateLifecycle is the final construction step, after all preparation,
// claims and admission resources have been handed to the handle.
func (r *Run) activateLifecycle() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.activated {
		return
	}
	r.activated = true
	if r.phase == runSettled {
		return
	}
	r.stopParent = context.AfterFunc(r.rc.Context, r.Cancel)
}
