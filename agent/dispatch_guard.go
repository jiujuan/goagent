package agent

import (
	"fmt"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

// checkWriteAccess rejects new external work once the root run is cancelled or
// its durable writer can no longer prove ownership. It only samples state locks;
// model and tool calls never run while a guard lock is held.
func (rc *RunContext) checkWriteAccess() error {
	if rc == nil {
		return nil
	}

	var contextErr error
	if rc.Context != nil {
		contextErr = rc.Context.Err()
	}
	if rc.durable == nil {
		return contextErr
	}

	rc.durable.mu.Lock()
	guard := rc.durable.guard
	rc.durable.mu.Unlock()
	if guard == nil {
		return contextErr
	}

	guard.mu.Lock()
	guardErr := guard.firstErr
	stopped := guard.stopped
	expiresAt := guard.claim.ExpiresAt
	guard.mu.Unlock()

	if guardErr != nil {
		return guardErr
	}
	if !expiresAt.IsZero() && !time.Now().Before(expiresAt) {
		return fmt.Errorf("agent: write lease expired at %s: %w", expiresAt.Format(time.RFC3339Nano), checkpoint.ErrStaleFencingToken)
	}
	if stopped {
		return errWriteGuardStopped
	}
	return contextErr
}
