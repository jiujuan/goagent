//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package runstore

import (
	"fmt"
	"os"
	"runtime"
)

func tryRunFileLock(*os.File) (bool, error) {
	return false, fmt.Errorf("runstore: OS file locking is unsupported on %s", runtime.GOOS)
}
