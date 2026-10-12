package runstore

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

// Store keeps run records keyed by RunID.
//
// Every implementation must return deep copies and must route every write through the
// same lifecycle validation in this package, so no caller — including a worker that
// believes it knows the current state — can move a record to a state the machine
// forbids or rewrite a frozen outcome (ADR-0034 D4).
type Store interface {
	// Create inserts a record. Creating the same RunID again with the same content is
	// idempotent and returns the stored record; the same RunID with different content
	// reports ErrConflict. A first insert gets Version 1 and, when the caller left it
	// zero, a CreatedAt from the store clock.
	Create(ctx context.Context, record RunRecord) (RunRecord, error)

	// Get reads one record. It is a pure read: it never reconciles, renews a lease or
	// repairs state. An unknown RunID reports ErrNotFound.
	Get(ctx context.Context, runID string) (RunRecord, error)

	// CompareAndSwap replaces the record under runID only if its current Version equals
	// expectedVersion. It never creates a missing record. The caller passes the whole
	// record it wants stored: a field it leaves out is dropped, except the ones the store
	// maintains — CreatedAt, the identity fields, the persisted cancellation request,
	// ownership and the heartbeat, none of which a write may erase or move backwards. The
	// store advances Version by one and stamps UpdatedAt from its clock. On success the
	// accepted record is returned.
	//
	// After a claim, worker mutations require matching WorkerID and WorkerEpoch.
	// Omitting both permits only unchanged content or a pure cancellation request
	// (running/cancelling -> cancelling); all other fields must be restated unchanged.
	// Reconciliation of execution outcomes is not an ownerless CAS operation.
	//
	// A retry that matches the stored terminal outcome exactly is idempotent: the stored
	// record comes back unchanged, without a new Version.
	CompareAndSwap(ctx context.Context, runID string, expectedVersion uint64, next RunRecord) (RunRecord, error)
}

// storageClock is the package-internal injection point for the store clock. Tests in
// this package replace it so timestamps are deterministic; production paths leave it
// alone. A Store implementation takes one stamp per write attempt and passes it to the
// shared validation functions, so the record it publishes and the one it stores cannot
// carry different times.
var storageClock = func() time.Time { return time.Now().UTC() }

func nowStamp() time.Time { return storageClock() }

// validStatus reports whether s is one of the lifecycle states this package defines.
// An unknown status is a malformed write, not a transition.
func validStatus(s Status) bool {
	switch s {
	case StatusQueued, StatusRunning, StatusCancelling, StatusPaused, StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// validateShape checks the parts of a record that must hold in every stored state,
// independent of which state it came from.
func validateShape(r RunRecord) error {
	if r.RunID == "" {
		return fmt.Errorf("runstore: record has no run id: %w", ErrInvalidRecord)
	}
	if !validStatus(r.Status) {
		return fmt.Errorf("runstore: unknown run status %q: %w", r.Status, ErrInvalidRecord)
	}
	switch r.Status {
	case StatusFailed:
		if r.Error == nil || r.Error.Code == "" {
			return fmt.Errorf("runstore: failed run %q must carry an error code: %w", r.RunID, ErrInvalidRecord)
		}
	case StatusCancelled:
		if r.CancelRequestedAt.IsZero() {
			return fmt.Errorf("runstore: cancelled run %q must carry the cancellation request time: %w", r.RunID, ErrInvalidRecord)
		}
	case StatusCancelling:
		if r.CancelRequestedAt.IsZero() {
			return fmt.Errorf("runstore: cancelling run %q must carry the cancellation request time: %w", r.RunID, ErrInvalidRecord)
		}
	}
	return nil
}

// prepareCreate validates a create against the record already stored under the same
// RunID, which may be nil. It returns the record to publish.
func prepareCreate(stored *RunRecord, next RunRecord, now time.Time) (RunRecord, error) {
	if err := validateShape(next); err != nil {
		return RunRecord{}, err
	}
	if stored != nil {
		if createContentEqual(*stored, next) {
			return stored.clone()
		}
		return RunRecord{}, fmt.Errorf("runstore: run %q already recorded with different content: %w", next.RunID, ErrConflict)
	}
	created := next
	created.CreatedAt = storedTime(next.CreatedAt, now)
	created.UpdatedAt = now
	created.Version = 1
	return created.clone()
}

// createContentEqual compares all caller-owned content. A missing CreatedAt means
// the caller accepts the creation time assigned by the store on the first attempt.
// Version and UpdatedAt are always assigned by the store.
func createContentEqual(stored, next RunRecord) bool {
	next.CreatedAt = storedTime(next.CreatedAt, stored.CreatedAt)
	stored.Version, next.Version = 0, 0
	stored.UpdatedAt, next.UpdatedAt = time.Time{}, time.Time{}
	// Compare timestamps as instants, independent of location and monotonic metadata.
	if !stored.CreatedAt.Equal(next.CreatedAt) ||
		!stored.HeartbeatAt.Equal(next.HeartbeatAt) ||
		!stored.CancelRequestedAt.Equal(next.CancelRequestedAt) ||
		!stored.WorkerLeaseUntil.Equal(next.WorkerLeaseUntil) {
		return false
	}
	stored.CreatedAt, next.CreatedAt = time.Time{}, time.Time{}
	stored.HeartbeatAt, next.HeartbeatAt = time.Time{}, time.Time{}
	stored.CancelRequestedAt, next.CancelRequestedAt = time.Time{}, time.Time{}
	stored.WorkerLeaseUntil, next.WorkerLeaseUntil = time.Time{}, time.Time{}
	// Message payloads use the same encoded-content equality as terminal retries.
	if stored.Result == nil || next.Result == nil {
		if stored.Result != next.Result {
			return false
		}
	} else if !equalMessage(stored.Result.Message, next.Result.Message) {
		return false
	}
	stored.Result, next.Result = nil, nil
	return reflect.DeepEqual(stored, next)
}

// prepareCAS applies the lifecycle to one compare-and-swap attempt against the stored
// record, which may be nil when the RunID is unknown. Every Store implementation calls
// this and only this.
func prepareCAS(stored *RunRecord, runID string, expectedVersion uint64, next RunRecord, now time.Time) (RunRecord, error) {
	if runID == "" {
		return RunRecord{}, fmt.Errorf("runstore: compare-and-swap without a run id: %w", ErrInvalidRecord)
	}
	if next.RunID != "" && next.RunID != runID {
		return RunRecord{}, fmt.Errorf("runstore: compare-and-swap on %q carries run id %q: %w", runID, next.RunID, ErrInvalidRecord)
	}
	if next.RunID == "" {
		next.RunID = runID
	}
	// Versions start at 1, so an expectedVersion of zero can never match: a caller that
	// never read the record gets the same conflict as one whose read went stale.
	if stored == nil {
		return RunRecord{}, runNotFound(runID)
	}
	if stored.Version != expectedVersion {
		return RunRecord{}, fmt.Errorf("runstore: run %q is at version %d, expected %d: %w", runID, stored.Version, expectedVersion, ErrConflict)
	}
	if err := checkImmutableIdentity(*stored, next); err != nil {
		return RunRecord{}, err
	}
	merged, err := mergeOwnership(*stored, next)
	if err != nil {
		return RunRecord{}, err
	}
	// The merged record is the one that would be stored, so it is the one shape-checked:
	// a write that leaves out a fact the store already holds is repaired before it is
	// judged, not rejected for the omission.
	return applyLifecycle(merged, *stored, now)
}

// checkImmutableIdentity rejects a write that rewrites who the run is or when it began.
func checkImmutableIdentity(stored, next RunRecord) error {
	if stored.RunID != next.RunID {
		return fmt.Errorf("runstore: run %q cannot change its run id to %q: %w", stored.RunID, next.RunID, ErrConflict)
	}
	if stored.ThreadID != next.ThreadID {
		return fmt.Errorf("runstore: run %q cannot move from thread %q to %q: %w", stored.RunID, stored.ThreadID, next.ThreadID, ErrConflict)
	}
	if stored.TurnID != next.TurnID {
		return fmt.Errorf("runstore: run %q cannot change turn from %q to %q: %w", stored.RunID, stored.TurnID, next.TurnID, ErrConflict)
	}
	if stored.JobID != next.JobID {
		return fmt.Errorf("runstore: run %q cannot change job from %q to %q: %w", stored.RunID, stored.JobID, next.JobID, ErrConflict)
	}
	if stored.ResumeOfRunID != next.ResumeOfRunID {
		return fmt.Errorf("runstore: run %q cannot change resume-of from %q to %q: %w", stored.RunID, stored.ResumeOfRunID, next.ResumeOfRunID, ErrConflict)
	}
	if !stored.CreatedAt.IsZero() && !next.CreatedAt.IsZero() && !stored.CreatedAt.Equal(next.CreatedAt) {
		return fmt.Errorf("runstore: run %q cannot change its created at: %w", stored.RunID, ErrConflict)
	}
	return nil
}

// mergeOwnership reconciles the worker fields. Ownership is assigned once — by the CAS
// that starts the run — and from then on a write that names a worker must name the one
// that owns the run. An ownerless write may only restate content or request
// cancellation; it cannot publish worker progress or an execution outcome.
//
// It returns the record whose ownership fields are reconciled with the stored ones.
func mergeOwnership(stored, next RunRecord) (RunRecord, error) {
	if stored.WorkerID == "" && stored.WorkerEpoch == 0 {
		// Unclaimed. A claim may set both; anything else leaves them empty.
		if (next.WorkerID != "") != (next.WorkerEpoch != 0) {
			return RunRecord{}, fmt.Errorf("runstore: run %q claim must give worker %q with epoch %d together: %w",
				stored.RunID, next.WorkerID, next.WorkerEpoch, ErrOwnershipLost)
		}
		return next, nil
	}
	// Claimed: ownership never changes.
	if next.WorkerID == "" && next.WorkerEpoch == 0 {
		out := next
		out.WorkerID = stored.WorkerID
		out.WorkerEpoch = stored.WorkerEpoch
		comparable := out
		if (stored.Status == StatusRunning || stored.Status == StatusCancelling) &&
			out.Status == StatusCancelling && !out.CancelRequestedAt.IsZero() {
			comparable.Status = stored.Status
			comparable.CancelRequestedAt = stored.CancelRequestedAt
		}
		if !createContentEqual(stored, comparable) {
			return RunRecord{}, fmt.Errorf("runstore: run %q worker mutation requires its owner and epoch: %w", stored.RunID, ErrOwnershipLost)
		}
		return out, nil
	}
	if next.WorkerID != stored.WorkerID || next.WorkerEpoch != stored.WorkerEpoch {
		return RunRecord{}, fmt.Errorf("runstore: run %q is owned by worker %q epoch %d, write came from %q epoch %d: %w",
			stored.RunID, stored.WorkerID, stored.WorkerEpoch, next.WorkerID, next.WorkerEpoch, ErrOwnershipLost)
	}
	return next, nil
}

// applyLifecycle gates the state change itself, then stamps the accepted record.
func applyLifecycle(base, stored RunRecord, now time.Time) (RunRecord, error) {
	// A cancellation request, once persisted, is a durable fact: no write may drop it or
	// date it earlier than the request already on record.
	if base.CancelRequestedAt.IsZero() {
		base.CancelRequestedAt = stored.CancelRequestedAt
	} else if !stored.CancelRequestedAt.IsZero() && base.CancelRequestedAt.Before(stored.CancelRequestedAt) {
		return RunRecord{}, fmt.Errorf("runstore: run %q cannot move its cancellation request from %s to %s: %w",
			stored.RunID, stored.CancelRequestedAt.Format(time.RFC3339Nano), base.CancelRequestedAt.Format(time.RFC3339Nano), ErrConflict)
	}
	if err := validateShape(base); err != nil {
		return RunRecord{}, err
	}
	// Liveness never rewinds either.
	if base.HeartbeatAt.Before(stored.HeartbeatAt) {
		return RunRecord{}, fmt.Errorf("runstore: run %q heartbeat at %s is older than stored %s: %w",
			stored.RunID, base.HeartbeatAt.Format(time.RFC3339Nano), stored.HeartbeatAt.Format(time.RFC3339Nano), ErrConflict)
	}

	if stored.Status.Terminal() {
		if base.terminalDecision().equal(stored.terminalDecision()) {
			// Idempotent retry of a settled outcome: publish what is stored, unchanged.
			return stored.clone()
		}
		return RunRecord{}, fmt.Errorf("runstore: run %q already settled as %q, cannot rewrite it to %q: %w",
			stored.RunID, stored.Status, base.Status, ErrInvalidTransition)
	}
	if !allowedTransitions[stored.Status][base.Status] {
		return RunRecord{}, fmt.Errorf("runstore: run %q cannot go from %q to %q: %w",
			stored.RunID, stored.Status, base.Status, ErrInvalidTransition)
	}
	base.CreatedAt = stored.CreatedAt
	base.Version = stored.Version + 1
	base.UpdatedAt = now
	return base.clone()
}

// storedTime returns the caller's timestamp when it set one, otherwise the store clock.
func storedTime(want, now time.Time) time.Time {
	if want.IsZero() {
		return now
	}
	return want
}
