package runstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// lockPollInterval and lockStealAfter mirror the checkpoint File backend: a lock is
// created with O_EXCL and polled, and only a lock older than a generous operation TTL may
// be stolen. Keeping the same numbers means one operation takes comparably long on both
// stores (ADR-0034 D5).
const (
	lockPollInterval = 5 * time.Millisecond
	lockStealAfter   = 2 * time.Minute
)

// acquireProcessLock serializes one run's journal across processes. It uses O_EXCL rather
// than flock because the run store must work on Windows too, where advisory file locks are
// not available in the same way.
//
// The wait is interruptible: a caller that gives up on a held lock gets its context error
// rather than an unbounded wait, because a queued worker may be cancelled while another
// process is mid-write.
func (f *File) acquireProcessLock(ctx context.Context, runID string) (func(), error) {
	path, err := f.lockPath(runID)
	if err != nil {
		return nil, err
	}
	for {
		fh, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fh.WriteString(fmt.Sprintf("%d", time.Now().UnixNano()))
			_ = fh.Close()
			return func() { _ = os.Remove(path) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > lockStealAfter {
			if removeErr := os.Remove(path); removeErr == nil || os.IsNotExist(removeErr) {
				continue
			}
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("runstore: waiting for run %q lock: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}
