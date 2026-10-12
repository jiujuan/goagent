// Package runstore keeps the mutable task record of one run: where it is in its
// lifecycle, which worker owns it, whether a cancellation has been requested, and
// which checkpoint boundary and outcome it produced.
//
// It is deliberately separate from the checkpoint package. A checkpoint describes an
// immutable committed state; a run record describes execution progress, and writing
// heartbeats or status into checkpoints would inflate the history and move a thread's
// head without any state change behind it (ADR-0034 D2). A record references
// checkpoints by Ref and revision but never owns them, and the package depends only on
// core and checkpoint — the queue drives execution, so neither checkpoint nor agent is
// imported here.
//
// A RunID is a globally unique execution identity assigned by the caller and is the
// only key. JobID is recorded separately: it names a queue delivery, and the same run
// may be delivered more than once. Resume never revives a record — continuing a paused
// run creates a new record whose ResumeOfRunID names the previous one.
package runstore

import (
	"errors"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// Status is the lifecycle state of a run record. The set is closed on purpose: a
// reader must never guess what an unfamiliar state means for scheduling or for the
// thread's checkpoint history.
type Status string

const (
	// StatusQueued means the run was recorded but no worker has started it.
	StatusQueued Status = "queued"
	// StatusRunning means a worker holds the run.
	StatusRunning Status = "running"
	// StatusCancelling means cancellation was requested while a worker still held it.
	StatusCancelling Status = "cancelling"
	// StatusPaused ends this run: it stopped at a durable pause awaiting approval, and
	// a resume creates a new record rather than reviving this one.
	StatusPaused Status = "paused"
	// StatusSucceeded ends this run with a produced result.
	StatusSucceeded Status = "succeeded"
	// StatusFailed ends this run with an error.
	StatusFailed Status = "failed"
	// StatusCancelled ends this run because a persisted cancellation request was
	// observed before any worker started it.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether the status ends the run, so no further ordinary update is
// allowed on it.
func (s Status) Terminal() bool {
	switch s {
	case StatusPaused, StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// FinishReason classifies how a run ended, separately from its error code.
type FinishReason string

const (
	// FinishReasonCompleted means the run produced a result.
	FinishReasonCompleted FinishReason = "completed"
	// FinishReasonFailed means the run ended with an error.
	FinishReasonFailed FinishReason = "failed"
	// FinishReasonCancelled means the run ended because a cancellation request was
	// honored.
	FinishReasonCancelled FinishReason = "cancelled"
	// FinishReasonPaused means the run stopped at a durable pause awaiting approval.
	FinishReasonPaused FinishReason = "paused"
)

// Error codes carried by RunError.Code. Callers must keep the specific code of an
// unknown outcome rather than collapsing it into a generic failure, because automatic
// replay of an external tool effect is not safe (ADR-0034 D4).
const (
	// ErrCodeEnqueueFailed means delivery to the queue was known to have failed.
	ErrCodeEnqueueFailed = "enqueue_failed"
	// ErrCodeDispatchUnknown means the queue accepted the job but the outcome of the
	// delivery call is not known.
	ErrCodeDispatchUnknown = "dispatch_unknown"
	// ErrCodeDeadlineExceeded means the run ended by exceeding its deadline, which is
	// a failure and not a user cancellation.
	ErrCodeDeadlineExceeded = "deadline_exceeded"
	// ErrCodeExecutionFailed means the run itself ended with an error.
	ErrCodeExecutionFailed = "execution_failed"
)

// Store errors.
var (
	// ErrInvalidRecord means the write itself is malformed: no run id, a status the
	// lifecycle does not define, or a terminal state without the evidence that state
	// requires. It is reported before the stored record is consulted.
	ErrInvalidRecord = errors.New("runstore: invalid run record")
	// ErrNotFound means no record exists under the requested RunID.
	ErrNotFound = errors.New("runstore: run record not found")
	// ErrConflict means the record changed since the caller read it, or a create was
	// replayed with different content.
	ErrConflict = errors.New("runstore: run record conflicts with stored state")
	// ErrInvalidTransition means the requested state change is not one the lifecycle
	// allows, including any attempt to rewrite a terminal record.
	ErrInvalidTransition = errors.New("runstore: invalid run record transition")
	// ErrOwnershipLost means the write came from a worker that no longer owns the run.
	ErrOwnershipLost = errors.New("runstore: worker ownership lost")
)

// RunError is why a run failed, with enough detail to decide what is safe next.
type RunError struct {
	Code      string `json:"code"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

// RunRecord is the task record of one run. Every field is written through a Store,
// which returns deep copies so a caller cannot reach stored state (ADR-0034 D4).
//
// Version is this record's own compare-and-swap counter and is unrelated to a
// checkpoint's thread revision: the two advance for different reasons and a caller must
// never read one as the other.
type RunRecord struct {
	// RunID is the execution identity and the only key. It matches the RunID the run
	// stamps onto the checkpoints it commits.
	RunID string `json:"run_id"`
	// ThreadID is the thread the run executes on.
	ThreadID string `json:"thread_id"`
	// TurnID is the user turn this run belongs to.
	TurnID string `json:"turn_id,omitempty"`
	// JobID names the queue delivery of this run, when it came from a queue.
	JobID string `json:"job_id,omitempty"`
	// ResumeOfRunID is the run this one continues, empty for a first run.
	ResumeOfRunID string `json:"resume_of_run_id,omitempty"`

	// Status is the lifecycle state.
	Status Status `json:"status"`
	// FinishReason classifies a terminal outcome.
	FinishReason FinishReason `json:"finish_reason,omitempty"`
	// Error explains a failed run, nil otherwise.
	Error *RunError `json:"error,omitempty"`
	// Result is the run's frozen final message, nil unless it produced one.
	Result *core.Result `json:"result,omitempty"`

	// LatestCheckpointRef locates the last checkpoint this run committed.
	LatestCheckpointRef *checkpoint.Ref `json:"latest_checkpoint_ref,omitempty"`
	// CheckpointRevision is the store revision of that checkpoint, 0 when unknown.
	CheckpointRevision uint64 `json:"checkpoint_revision,omitempty"`
	// CompletionRef locates the checkpoint carrying this run's terminal record, nil
	// when none was recorded. A run can finish successfully with this nil, which is how
	// execution success and durable bookkeeping stay separate outcomes (ADR-0033 D4).
	CompletionRef *checkpoint.Ref `json:"completion_ref,omitempty"`
	// RecordError says the terminal checkpoint record itself failed to store.
	RecordError *RunError `json:"record_error,omitempty"`

	// CreatedAt is when the record was first created, preserved across updates.
	CreatedAt time.Time `json:"created_at,omitzero"`
	// UpdatedAt is when the store last accepted a change.
	UpdatedAt time.Time `json:"updated_at,omitzero"`
	// WorkerID is the worker that claimed the run, empty until one starts it.
	WorkerID string `json:"worker_id,omitempty"`
	// HeartbeatAt is the last liveness signal from that worker. It never moves a
	// checkpoint head and never changes status on its own.
	HeartbeatAt time.Time `json:"heartbeat_at,omitzero"`
	// CancelRequestedAt is when a cancellation was persisted. Requesting cancellation
	// is a durable fact, not a claim that external side effects were undone.
	CancelRequestedAt time.Time `json:"cancel_requested_at,omitzero"`
	// WorkerEpoch fences writes to the worker that claimed the run.
	WorkerEpoch uint64 `json:"worker_epoch,omitempty"`
	// WorkerLeaseUntil bounds that ownership. Passing it makes a record stale; it does
	// not by itself mean the run failed or was cancelled (ADR-0034 D6).
	WorkerLeaseUntil time.Time `json:"worker_lease_until,omitzero"`

	// Version is this record's CAS counter, assigned by the store starting at 1.
	Version uint64 `json:"version"`
}

// terminalDecisionFields are the parts of a terminal record that no later write may
// change: identity and creation time are checked separately as immutable.
func (r RunRecord) terminalDecision() terminalDecision {
	return terminalDecision{
		Status:             r.Status,
		FinishReason:       r.FinishReason,
		Error:              r.Error,
		Result:             r.Result,
		LatestCheckpointID: refCheckpointID(r.LatestCheckpointRef),
		CheckpointRevision: r.CheckpointRevision,
		CompletionID:       refCheckpointID(r.CompletionRef),
		RecordError:        r.RecordError,
	}
}

type terminalDecision struct {
	Status             Status
	FinishReason       FinishReason
	Error              *RunError
	Result             *core.Result
	LatestCheckpointID string
	CheckpointRevision uint64
	CompletionID       string
	RecordError        *RunError
}

func refCheckpointID(ref *checkpoint.Ref) string {
	if ref == nil {
		return ""
	}
	return ref.ThreadID + "/" + ref.CheckpointID
}

// allowedTransitions is the lifecycle: queued starts, running settles or is cancelled
// mid-flight, and a cancelling record may still reach any frozen outcome. Terminal
// states appear nowhere on the right-hand side, because a terminal record accepts only
// an identical retry.
var allowedTransitions = map[Status]map[Status]bool{
	StatusQueued: {
		StatusQueued:     true,
		StatusRunning:    true,
		StatusCancelling: true,
		StatusCancelled:  true,
		// A delivery that failed before any worker started still ends the run.
		StatusFailed: true,
	},
	StatusRunning: {
		StatusRunning:    true,
		StatusPaused:     true,
		StatusSucceeded:  true,
		StatusFailed:     true,
		StatusCancelling: true,
	},
	StatusCancelling: {
		StatusCancelling: true,
		StatusCancelled:  true,
		StatusPaused:     true,
		StatusSucceeded:  true,
		StatusFailed:     true,
	},
	StatusPaused:    {},
	StatusSucceeded: {},
	StatusFailed:    {},
	StatusCancelled: {},
}
