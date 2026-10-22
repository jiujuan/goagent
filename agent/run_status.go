package agent

import (
	"context"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// RunStatus is what one thread looks like in the checkpoint store right now:
// a read-only snapshot of its head checkpoint. It is deliberately not a live
// process view — see Agent.RunStatus for what it can and cannot say.
//
// Paused and InFlight are separate facts: a paused head waits for a person, an
// in-flight head waits for a resume. README's "后台运行的终态与状态查询"
// section has the comparison table.
type RunStatus struct {
	// ThreadID is the thread this status was read from.
	ThreadID string
	// Exists reports whether the thread has any checkpoint at all.
	Exists bool
	// Paused reports whether the head carries pause or pending-work metadata
	// instead of a terminal record. It does not promise the run can be resumed
	// successfully: approvals, claims, recovery kind and unknown tool outcomes
	// still decide that.
	Paused bool
	// InFlight reports whether the head is the boundary written before a tool
	// batch ran. Nothing is waiting for a human decision here: the next step is
	// Agent.Resume, which settles the batch from its ledger. It is independent of
	// Paused; the store rejects a checkpoint carrying both.
	InFlight bool
	// Revision is the head checkpoint's store revision, 0 when the thread has no
	// checkpoint.
	Revision uint64
	// Step is the head checkpoint's loop step.
	Step int
	// Completion is the terminal record on the head, nil when it has none.
	Completion *checkpoint.Completion
	// Latest is the head checkpoint as read from the store, nil when the thread
	// does not exist. The built-in stores hand out copies, so editing it does not
	// reach the thread's history; a custom store defines its own boundary here.
	Latest *checkpoint.Checkpoint
}

// RunStatus reads a thread's head checkpoint. It is a pure read: no Save, no
// Commit, no claim, no lease renewal, so it never moves a thread forward and can
// be called while a run is in flight.
//
// An unknown thread is not an error: Exists is false and Latest is nil. Store
// failures are returned unchanged, and thread IDs are validated before the store
// is touched.
//
// What it cannot tell you is whether a run is happening now. A Completion is a
// record of the run that wrote it: a newer run can already be underway on the
// same thread without having committed anything yet, so the head still shows the
// previous run's terminal record. Never read this as "no run is active".
func (a *Agent) RunStatus(ctx context.Context, threadID string) (RunStatus, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return RunStatus{}, err
	}
	latest, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return RunStatus{}, err
	}

	status := RunStatus{ThreadID: threadID}
	if latest == nil {
		return status, nil
	}
	status.Exists = true
	status.Revision = latest.Revision
	status.Step = latest.Step
	status.Latest = latest
	status.Completion = latest.Completion
	status.Paused = latest.Completion == nil && (latest.Pause != nil || latest.Pending != nil)
	status.InFlight = latest.Completion == nil && latest.InFlight != nil
	return status, nil
}
