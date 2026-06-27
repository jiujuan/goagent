//go:build !windows

package process

import (
	"os/exec"
	"syscall"
)

// configureSysProcAttr puts the child in its own process group so the whole
// tree can be signalled together on timeout or output overflow.
func configureSysProcAttr(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessTree kills the child's entire process group. The negative PID
// targets the group whose leader is the child (established via Setpgid).
func killProcessTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pgid := cmd.Process.Pid
	// Best effort: signal the group, then the leader directly as a fallback.
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
}
