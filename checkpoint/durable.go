package checkpoint

import (
	"context"
	"errors"
	"time"

	"github.com/jiujuan/goagent/core"
)

// Durable is an optional extension to Checkpointer. It keeps durable inbox
// delivery, checkpoint commits, resume coordination and approval decisions in
// the same per-thread history as ordinary snapshots. Code that only needs
// Checkpointer remains compatible with stores that do not implement Durable.
type Durable interface {
	Checkpointer

	Deliver(context.Context, string, InboxMessage) (DeliveryReceipt, error)
	Inbox(context.Context, string) ([]InboxMessage, error)
	Commit(context.Context, CommitRequest) (CommitReceipt, error)

	Claim(context.Context, ClaimRequest) (Claim, error)
	ReleaseClaim(context.Context, Claim) error

	RecordDecision(context.Context, string, core.ApprovalDecision) (DecisionReceipt, error)
	Decisions(context.Context, string, string) ([]core.ApprovalDecision, error)

	ClaimTool(context.Context, ToolClaimRequest) (ToolExecution, error)
	ToolExecution(context.Context, string, string) (ToolExecution, bool, error)
}

var (
	// ErrDurableUnavailable means the configured Checkpointer only supports the
	// base checkpoint API. Durable APIs must not pretend an in-memory fallback
	// was persisted.
	ErrDurableUnavailable = errors.New("checkpoint: durable capability unavailable")
	// ErrClaimConflict means another, still-live owner holds the requested
	// checkpoint/pause claim.
	ErrClaimConflict = errors.New("checkpoint: resume claim is held by another owner")
	// ErrStaleFencingToken means a lease expired and a newer owner has acquired
	// the thread. A stale worker must never commit state after this error.
	ErrStaleFencingToken = errors.New("checkpoint: stale fencing token")
	// ErrClaimRequired means a thread currently has a live resume owner and an
	// ordinary Save/Commit tried to bypass its fencing token.
	ErrClaimRequired = errors.New("checkpoint: active resume claim requires a fencing token")
	// ErrDecisionConflict means one decision ID was reused for different
	// pause/call/arguments/policy/decision content.
	ErrDecisionConflict = errors.New("checkpoint: approval decision ID conflicts with existing content")
	// ErrToolOutcomeUnknown means a tool was known to have started but no
	// completed result was atomically committed. Replaying it may duplicate an
	// external side effect, so callers must not execute it automatically.
	ErrToolOutcomeUnknown = errors.New("checkpoint: tool outcome is unknown")
)

const DefaultLease = 30 * time.Second

// InboxMessage is one durable, redeliverable message. ID is supplied by the
// producer and is the idempotency key for delivery; it must remain stable when
// a producer retries after an ambiguous transport failure.
type InboxMessage struct {
	ID      string       `json:"id"`
	Message core.Message `json:"message"`
}

type DeliveryReceipt struct {
	ID        string
	Duplicate bool
}

// ClaimRequest uses the latest checkpoint revision and pause ID as a CAS
// target. Owner should be unique per worker attempt; a successful Claim carries
// the fencing token that must accompany every later Commit.
type ClaimRequest struct {
	ThreadID string
	Revision uint64
	PauseID  string
	Owner    string
	Lease    time.Duration
}

// Claim is a leased ownership record for a resumed thread.
type Claim struct {
	ThreadID     string    `json:"thread_id"`
	Revision     uint64    `json:"revision"`
	PauseID      string    `json:"pause_id"`
	Owner        string    `json:"owner"`
	FencingToken uint64    `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// CommitRequest makes the checkpoint history and inbox acknowledgement visible
// as one durable operation. ToolExecutions completed in the same step are also
// committed with the snapshot, preventing a result from being remembered while
// the history that explains it is lost (or vice versa).
type CommitRequest struct {
	Checkpoint     *Checkpoint
	AckMessageIDs  []string
	Claim          *Claim
	Lease          time.Duration
	ReleaseClaim   bool
	ToolExecutions []ToolExecution
}

type CommitReceipt struct {
	Revision uint64
	Claim    *Claim
}

// ToolExecutionState describes the durable boundary around an external tool
// invocation. Claimed without a completed commit is deliberately not replayed.
type ToolExecutionState string

const (
	ToolPending   ToolExecutionState = "pending"
	ToolClaimed   ToolExecutionState = "claimed"
	ToolCompleted ToolExecutionState = "completed"
	ToolUnknown   ToolExecutionState = "unknown"
)

// ToolExecution is keyed by InvocationID, which is stable across resume. A
// completed Result is stored as a core.Message so it keeps core's tagged Part
// JSON encoding instead of serializing an interface slice directly.
type ToolExecution struct {
	InvocationID   string             `json:"invocation_id"`
	CallID         string             `json:"call_id"`
	Tool           string             `json:"tool"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	Attempt        int                `json:"attempt"`
	State          ToolExecutionState `json:"state"`
	Result         *core.Message      `json:"result,omitempty"`
}

type ToolClaimRequest struct {
	ThreadID       string
	InvocationID   string
	CallID         string
	Tool           string
	IdempotencyKey string
	Claim          *Claim
}

// DecisionReceipt reports whether RecordDecision returned a prior equivalent
// decision instead of adding another one.
type DecisionReceipt struct {
	Decision  core.ApprovalDecision
	Duplicate bool
}
