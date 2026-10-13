package runstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

const lockPollInterval = 5 * time.Millisecond

// acquireProcessLock holds an OS lock on a stable file until release or process exit.
// Never unlink the file: another handle could otherwise lock a different inode while
// the old owner is still in its critical section. File age does not imply ownership loss.
func (f *File) acquireProcessLock(ctx context.Context, runID string) (func(), error) {
	path, err := f.lockPath(runID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			fh.Close()
			return nil, err
		}
		acquired, err := tryRunFileLock(fh)
		if err != nil {
			fh.Close()
			return nil, err
		}
		if acquired {
			var once sync.Once
			release := func() { once.Do(func() { _ = fh.Close() }) }
			if err := ctx.Err(); err != nil {
				release()
				return nil, err
			}
			return release, nil
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			fh.Close()
			return nil, fmt.Errorf("runstore: waiting for run %q lock: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}
