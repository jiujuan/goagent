// Package sandbox defines a small, portable contract for running external
// commands under controlled constraints. The contract decides nothing about
// how isolation is achieved; backends (see sandbox/process) implement it.
//
// The default process backend enforces five portable, deterministic limits:
// wall-clock timeout, combined output cap, working-directory confinement,
// environment allow-listing (empty by default), and a command allow-list.
// Stronger isolation (memory/CPU caps, network namespaces) is intentionally
// left to future container backends; Policy and Sandbox are shaped to admit
// them without breaking this contract.
package sandbox

import (
	"context"
	"errors"
	"time"
)

// Sandbox runs a single command under a controlled environment. The interface
// is deliberately small: one method, one Spec in, one Outcome out.
type Sandbox interface {
	Run(ctx context.Context, spec Spec) (*Outcome, error)
}

// Spec is what to run on a given invocation.
type Spec struct {
	// Command is the executable name or path to run.
	Command string
	// Args are the command arguments (not including Command itself).
	Args []string
	// Stdin, if non-empty, is fed to the process's standard input.
	Stdin []byte
}

// Policy is the set of constraints a backend enforces. It is fixed when the
// backend is constructed, not per-call. A zero-valued field means "do not
// constrain this dimension" — except WorkDir, which is required.
type Policy struct {
	// WorkDir is the absolute, existing directory every command runs in.
	// Required.
	WorkDir string
	// Timeout is the wall-clock limit for a command. 0 means no timeout.
	Timeout time.Duration
	// MaxOutputBytes caps the combined size of stdout and stderr. 0 means no
	// cap. When exceeded, the command is killed and Outcome.Truncated is set.
	MaxOutputBytes int64
	// AllowedCommands is a command allow-list matched by base name. An empty
	// list means no command restriction; a non-empty list rejects anything
	// whose base name is not present.
	AllowedCommands []string
	// Env is the environment-variable allow-list passed to the command. A nil
	// or empty map means a completely empty environment. Only the keys listed
	// here are exposed to the child process.
	Env map[string]string
}

// Outcome is the result of a command that actually started. Failure modes that
// occur after the process launched (non-zero exit, timeout, output truncation)
// are reported here rather than as Go errors, mirroring the framework's
// "tool errors are data, not Go errors" stance.
type Outcome struct {
	// ExitCode is the process exit code (0 on success).
	ExitCode int
	// Stdout is the captured standard output (possibly truncated).
	Stdout []byte
	// Stderr is the captured standard error (possibly truncated).
	Stderr []byte
	// TimedOut is true if the command was killed for exceeding Policy.Timeout.
	TimedOut bool
	// Truncated is true if output hit Policy.MaxOutputBytes and was cut off.
	Truncated bool
	// Duration is how long the command ran.
	Duration time.Duration
}

// Errors returned before a process starts (configuration or policy
// violations). Failures after launch are reported via Outcome instead.
var (
	// ErrInvalidWorkDir means WorkDir is empty, not absolute, or not an
	// existing directory.
	ErrInvalidWorkDir = errors.New("sandbox: work dir must be an absolute existing directory")
	// ErrCommandNotAllowed means the command's base name is not in the
	// non-empty AllowedCommands list.
	ErrCommandNotAllowed = errors.New("sandbox: command not in allow list")
	// ErrInvalidSpec means the Spec is unusable (e.g. empty Command).
	ErrInvalidSpec = errors.New("sandbox: empty command")
)
