// Package process is the default Sandbox backend. It runs commands as OS
// processes under the portable limits described in sandbox.Policy: timeout,
// output cap, working-directory confinement, environment allow-listing, and a
// command allow-list. It uses only the standard library; the only
// platform-specific code is the process-group plumbing in process_unix.go /
// process_windows.go, which remains pure syscall.
package process

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/jiujuan/goagent/sandbox"
)

// Sandbox is the process-based implementation of sandbox.Sandbox.
type Sandbox struct {
	policy  sandbox.Policy
	allowed map[string]bool // command base-name allow set; nil means "allow all"
}

var _ sandbox.Sandbox = (*Sandbox)(nil)

// New builds a process sandbox for the given policy. It validates WorkDir
// eagerly (absolute and existing) so misconfiguration fails at construction
// rather than on the first Run.
func New(policy sandbox.Policy) (*Sandbox, error) {
	if !filepath.IsAbs(policy.WorkDir) {
		return nil, sandbox.ErrInvalidWorkDir
	}
	info, err := os.Stat(policy.WorkDir)
	if err != nil || !info.IsDir() {
		return nil, sandbox.ErrInvalidWorkDir
	}
	var allowed map[string]bool
	if len(policy.AllowedCommands) > 0 {
		allowed = make(map[string]bool, len(policy.AllowedCommands))
		for _, c := range policy.AllowedCommands {
			allowed[filepath.Base(c)] = true
		}
	}
	return &Sandbox{policy: policy, allowed: allowed}, nil
}

// Run executes spec under the sandbox's policy. Configuration and policy
// violations (empty command, disallowed command, failure to start) return a Go
// error. A command that starts but fails (non-zero exit, timeout, truncation)
// returns a populated Outcome with a nil error.
func (s *Sandbox) Run(ctx context.Context, spec sandbox.Spec) (*sandbox.Outcome, error) {
	if spec.Command == "" {
		return nil, sandbox.ErrInvalidSpec
	}
	if s.allowed != nil && !s.allowed[filepath.Base(spec.Command)] {
		return nil, sandbox.ErrCommandNotAllowed
	}

	if s.policy.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.policy.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	cmd.Dir = s.policy.WorkDir
	cmd.Env = buildEnv(s.policy.Env)
	if len(spec.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(spec.Stdin)
	}
	configureSysProcAttr(cmd)

	// One budget shared by stdout and stderr caps the combined output; blowing
	// it kills the process tree once.
	b := newBudget(s.policy.MaxOutputBytes, func() { killProcessTree(cmd) })
	outW := &capWriter{budget: b}
	errW := &capWriter{budget: b}
	cmd.Stdout = outW
	cmd.Stderr = errW

	start := time.Now()
	runErr := cmd.Run()
	duration := time.Since(start)

	out := &sandbox.Outcome{
		Stdout:    outW.buf.Bytes(),
		Stderr:    errW.buf.Bytes(),
		Truncated: b.exceeded(),
		Duration:  duration,
	}

	// Timeout is observed via the context; CommandContext kills the process
	// when it fires, surfacing as a run error we reinterpret here.
	if ctx.Err() == context.DeadlineExceeded {
		out.TimedOut = true
		out.ExitCode = -1
		return out, nil
	}

	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			out.ExitCode = exitErr.ExitCode()
			return out, nil
		}
		// A truncation kill manifests as a non-ExitError run failure; treat it
		// as a completed-but-truncated outcome rather than an infra error.
		if out.Truncated {
			out.ExitCode = -1
			return out, nil
		}
		// Anything else (binary not found, permission denied) is infrastructure.
		return nil, runErr
	}

	out.ExitCode = 0
	return out, nil
}

// buildEnv renders the env allow-list into the KEY=VALUE form exec expects. A
// nil/empty map yields a non-nil empty slice so exec uses an empty environment
// rather than inheriting the parent's.
func buildEnv(allow map[string]string) []string {
	env := make([]string, 0, len(allow))
	for k, v := range allow {
		env = append(env, k+"="+v)
	}
	return env
}

// budget is a combined output ceiling shared by the stdout and stderr writers.
// It is concurrency-safe because exec copies the two streams on separate
// goroutines.
type budget struct {
	mu       sync.Mutex
	limit    int64 // <=0 means unlimited
	used     int64
	cut      bool
	onExceed func()
}

func newBudget(limit int64, onExceed func()) *budget {
	return &budget{limit: limit, onExceed: onExceed}
}

// take reserves up to len(p) bytes against the budget, returning how many may
// be written. It fires onExceed exactly once the first time the limit is hit.
func (b *budget) take(p []byte) int {
	if b.limit <= 0 {
		return len(p)
	}
	b.mu.Lock()
	remaining := b.limit - b.used
	if remaining <= 0 {
		b.markCutLocked()
		b.mu.Unlock()
		return 0
	}
	n := int64(len(p))
	if n > remaining {
		b.used += remaining
		b.markCutLocked()
		b.mu.Unlock()
		return int(remaining)
	}
	b.used += n
	b.mu.Unlock()
	return len(p)
}

func (b *budget) markCutLocked() {
	if !b.cut {
		b.cut = true
		if b.onExceed != nil {
			go b.onExceed()
		}
	}
}

func (b *budget) exceeded() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cut
}

// capWriter buffers output, writing only what the shared budget allows and
// discarding the rest (while reporting success so the child keeps draining).
type capWriter struct {
	budget *budget
	buf    bytes.Buffer
}

func (w *capWriter) Write(p []byte) (int, error) {
	n := w.budget.take(p)
	if n > 0 {
		w.buf.Write(p[:n])
	}
	return len(p), nil
}
