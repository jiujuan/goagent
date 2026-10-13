//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package runstore

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

func tryRunFileLock(fh *os.File) (bool, error) {
	err := unix.Flock(int(fh.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
		return false, nil
	}
	return err == nil, err
}
