package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/core"
)

// ManagedRunSpec is the identity a queue assigns to one recorded run before that run
// executes. A background worker receives it after the submitting process is gone, so the
// identity cannot be generated inside the run: RunID, ThreadID and TurnID have to agree
// with what the task record and the committed checkpoints say.
//
// TurnID may be empty, which means the run opens a new turn when it starts executing,
// exactly as an ordinary run does. A queue that wants its record to carry the turn assigns
// one here.
type ManagedRunSpec struct {
	// RunID is the execution identity. It is the task record's key, so it must be
	// usable as a file name.
	RunID string
	// ThreadID is the thread the run executes on.
	ThreadID string
	// TurnID is the user turn this run belongs to, or empty to open a new one.
	TurnID string
}

// ErrInvalidManagedRun reports a spec that cannot identify a run: an empty run id, or a
// thread id that is not a safe file name.
var ErrInvalidManagedRun = errors.New("agent: invalid managed run spec")

// Validate checks the spec without touching the store.
func (s ManagedRunSpec) Validate() error {
	if s.RunID == "" {
		return fmt.Errorf("agent: managed run has no run id: %w", ErrInvalidManagedRun)
	}
	// The thread id becomes a file name in every checkpoint store, so it keeps the store's
	// own rule. The run id has no such requirement here: it only has to be a globally unique
	// execution identity, and the task store refuses one that cannot name a journal when the
	// record is created — before any worker executes it.
	if err := core.CheckThreadID(s.ThreadID); err != nil {
		return fmt.Errorf("agent: managed run: %w: %w", ErrInvalidManagedRun, err)
	}
	return nil
}

// StreamManaged creates a lazy run whose identity comes from the caller instead of being
// generated, and returns the handle without driving it. It is the entry point for a queue
// worker: the identity, the thread and the run id stamped into every checkpoint this run
// commits are the ones in spec, fixed when the handle is created.
//
// Rewriting the exported Run.ID or Run.ThreadID afterwards does not make a run into some
// other task: claims, committed checkpoints, the frozen result and the terminal record all
// read the identity frozen here (ADR-0034 D3), so a caller cannot fake a managed identity
// after creation.
//
// An OnThread option that names another thread than spec.ThreadID is rejected and nothing
// runs, because continuing someone else's history under this run's identity is never what
// a task record means. Other run options pass through unchanged, including WithRunFiles: an
// externally managed file backend has to be supplied per run by whoever resolves the agent.
//
// Unlike Stream, which cannot report a bad argument until the run fails, this returns an
// error for an invalid spec or a conflicting thread before creating the handle.
func (a *Agent) StreamManaged(ctx context.Context, spec ManagedRunSpec, input string, opts ...RunOption) (*Run, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	rc := resolveRunConfig(input, spec.ThreadID, opts...)
	if rc.ThreadID != spec.ThreadID {
		return nil, fmt.Errorf(
			"agent: managed run %q on thread %q: OnThread(%q) conflicts with the assigned thread",
			spec.RunID, spec.ThreadID, rc.ThreadID)
	}
	return a.streamManaged(ctx, rc, spec), nil
}

// RunManaged is the convenience form of StreamManaged: it drives the run to completion and
// reports the structured outcome, without recording a terminal checkpoint.
func (a *Agent) RunManaged(ctx context.Context, spec ManagedRunSpec, input string, opts ...RunOption) (RunResult, error) {
	run, err := a.StreamManaged(ctx, spec, input, opts...)
	if err != nil {
		return RunResult{ThreadID: spec.ThreadID, RunID: spec.RunID, TurnID: spec.TurnID}, err
	}
	_, runErr := run.Wait()
	return run.DetailedResult(), runErr
}

func (a *Agent) streamManaged(ctx context.Context, rc RunConfig, spec ManagedRunSpec) *Run {
	// The spec's thread id is already validated, so the only handle-creation failure left
	// is a message that cannot be frozen.
	message, err := core.CloneMessage(rc.Message)
	if err != nil {
		message = core.Message{}
	}
	run := a.newRunHandleWithID(ctx, spec.RunID, spec.ThreadID, &core.State{})
	run.startErr = err
	run.preparation = &runPreparation{
		message:        message,
		files:          rc.Files,
		restoreCurrent: true,
		turnID:         spec.TurnID,
	}
	run.activateLifecycle()
	return run
}
