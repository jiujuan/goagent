package runstore

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

func tryRunFileLock(fh *os.File) (bool, error) {
	err := windows.LockFileEx(windows.Handle(fh.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return false, nil
	}
	return err == nil, err
}
