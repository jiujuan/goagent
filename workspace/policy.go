package workspace

import (
	"fmt"

	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/sandbox/process"
)

// SandboxPolicy returns base with WorkDir filled in as the workspace root, so
// commands start where the file tools work. Every other field is carried over
// untouched — this is a default for one dimension, not a rewrite of the policy.
//
// A base that already names a different WorkDir is an error rather than a
// silent override: the two paths disagreeing is exactly the drift the
// workspace exists to prevent.
func (w *Workspace) SandboxPolicy(base sandbox.Policy) (sandbox.Policy, error) {
	if base.WorkDir != "" && base.WorkDir != w.root {
		// Paths unquoted: %q would double every Windows separator in a message a
		// human has to read.
		return sandbox.Policy{}, fmt.Errorf("workspace: policy WorkDir %s conflicts with workspace root %s", base.WorkDir, w.root)
	}
	base.WorkDir = w.root
	return base, nil
}

// Sandbox builds the process backend for SandboxPolicy's result, giving callers
// commands that run in the same directory the workspace reports.
func (w *Workspace) Sandbox(base sandbox.Policy) (*process.Sandbox, error) {
	p, err := w.SandboxPolicy(base)
	if err != nil {
		return nil, err
	}
	sb, err := process.New(p)
	if err != nil {
		return nil, fmt.Errorf("workspace: sandbox for %s: %w", w.root, err)
	}
	return sb, nil
}
