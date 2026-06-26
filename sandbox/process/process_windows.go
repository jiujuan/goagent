//go:build windows

package process

import (
	"os/exec"
	"syscall"
)

// configureSysProcAttr starts the child in a new process group so a Ctrl-Break
// would target it and not the parent. Note: killing the full descendant tree on
// Windows with only the standard library is best-effort — see killProcessTree.
func configureSysProcAttr(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}

// killProcessTree terminates the child process. Recursively killing
// grandchildren on Windows requires job objects or taskkill, neither of which
// is available through the pure standard library, so this kills the direct
// child only (best-effort, as documented in docs/SANDBOX.md).
func killProcessTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
