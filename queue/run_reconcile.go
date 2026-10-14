package queue

// SM-12 (ADR-0034 D5): the explicit repair of a run record that stopped short of its
// terminal state. Execution finished, its terminal snapshot was committed, and the task-store
// write that publishes the outcome never landed — the shape a crash between those two steps
// leaves behind, and the reason a reconnecting client can watch a run stay running forever.
//
// ReconcileRun is the only way that state moves on its own, and it moves it only as far as the
// durable evidence carries it: the committed Completion for this RunID. Nothing here is
// inferred from the thread's newest snapshot, from a lease that expired, or from the fact that
// a run "looks finished". With no matching Completion the record is reported as stale and left
// exactly as it stands — recovery is a human decision, not a guess by this package.

import (
	"context"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/runstore"
)

// ReconcileReason is how a reconcile call ended, in one of the five states a caller has to
// distinguish.
type ReconcileReason string

const (
	// ReconcileRepaired means this call wrote the terminal state the committed Completion
	// describes.
	ReconcileRepaired ReconcileReason = "repaired_from_completion"
	// ReconcileAlreadySettled means the record already ended, so nothing was written and the
	// frozen outcome stands.
	ReconcileAlreadySettled ReconcileReason = "already_settled"
	// ReconcileStale means the run has no committed Completion this store can find: the record
	// is not the reconciler's to finish, and it is reported unchanged.
	ReconcileStale ReconcileReason = "stale_no_completion"
	// ReconcileContradictory means evidence was found but does not fit the record — the wrong
	// thread, the wrong run, more than one terminal record for one run, or a named snapshot
	// that is not there. Repairs are refused; Detail says which.
	ReconcileContradictory ReconcileReason = "contradictory_evidence"
	// ReconcileRefused means the durable outcome is known but the record's own lifecycle
	// forbids publishing it, such as a record no worker ever claimed. The store error comes
	// back too, so a caller cannot mistake this for a repair.
	ReconcileRefused ReconcileReason = "transition_refused"
)

// maxReconcileAttempts bounds how many times one call re-reads and retries after a compare-
// and-swap conflict. A heartbeat or a cancellation can land between the read and the write;
// retrying re-decides from the new state, and giving up after a few rounds is honest about a
// store that will not settle.
const maxReconcileAttempts = 4

// ReconcileReport is the outcome of one ReconcileRun call.
type ReconcileReport struct {
	RunID string
	// Record is the task record as the store holds it when this call returns: repaired in the
	// Repaired case, untouched otherwise.
	Record runstore.RunRecord
	// Repaired is true only when this call advanced the record into its terminal state.
	Repaired bool
	Reason   ReconcileReason
	// Detail explains a refusal or a contradiction, in the store's or this package's own words.
	Detail string
	// Evidence names the committed terminal snapshot the decision was made from, nil when no
	// snapshot was consulted.
	Evidence *checkpoint.Ref
	// Attempts is how many compare-and-swap writes this call made, including the ones another
	// writer got in front of.
	Attempts int
}

// RunReconciler repairs run records from committed checkpoint evidence. It reads three
// things and writes one: Query to find a run's terminal snapshot, MetadataReader to confirm the
// snapshot a record already names, and Load to read the frozen outcome itself. A store that
// cannot do all three is refused at construction, because the honest answer there is "I cannot
// tell", not a silent fallback that reads whole histories.
type RunReconciler struct {
	runs      runstore.Store
	snapshots checkpoint.Checkpointer
	query     checkpoint.Query
	metadata  checkpoint.MetadataReader
}

// NewRunReconciler binds one reconciler to a task store and a checkpoint store.
func NewRunReconciler(runs runstore.Store, snapshots checkpoint.Checkpointer) (*RunReconciler, error) {
	if runs == nil {
		return nil, fmt.Errorf("queue: a run reconciler needs a task store")
	}
	if snapshots == nil {
		return nil, fmt.Errorf("queue: a run reconciler needs a checkpoint store")
	}
	query, ok := snapshots.(checkpoint.Query)
	if !ok {
		return nil, fmt.Errorf("queue: reconciler over %T cannot list checkpoint summaries: %w", snapshots, checkpoint.ErrCapabilityUnavailable)
	}
	metadata, ok := snapshots.(checkpoint.MetadataReader)
	if !ok {
		return nil, fmt.Errorf("queue: reconciler over %T cannot read checkpoint summaries: %w", snapshots, checkpoint.ErrCapabilityUnavailable)
	}
	return &RunReconciler{runs: runs, snapshots: snapshots, query: query, metadata: metadata}, nil
}

// ReconcileRun reads the record, looks for this run's committed Completion, and publishes the
// frozen outcome it finds. It is a write operation: a caller that only wants to know where a
// run stands uses GetRun.
//
// A terminal record comes back unchanged — a settled outcome is never rewritten, and a second
// call is not a second version. Errors from the task store are returned as they are, including
// runstore.ErrNotFound for a RunID that was never recorded.
func (r *RunReconciler) ReconcileRun(ctx context.Context, runID string) (ReconcileReport, error) {
	var last ReconcileReport
	for attempt := 1; attempt <= maxReconcileAttempts; attempt++ {
		record, err := r.runs.Get(ctx, runID)
		if err != nil {
			return ReconcileReport{RunID: runID}, err
		}
		if record.Status.Terminal() {
			return ReconcileReport{RunID: runID, Record: record, Reason: ReconcileAlreadySettled}, nil
		}

		found, err := r.findCompletion(ctx, record)
		if err != nil {
			return ReconcileReport{RunID: runID, Record: record, Attempts: attempt - 1}, err
		}
		if found.report.Reason != "" {
			// No usable evidence: the record stays as it is, and the reason says why.
			found.report.RunID = runID
			found.report.Record = record
			found.report.Attempts = attempt - 1
			return found.report, nil
		}

		next := terminalFromCompletion(record, found.completion)
		updated, err := r.runs.CompareAndSwap(ctx, runID, record.Version, next)
		if err == nil {
			return ReconcileReport{
				RunID: runID, Record: updated, Repaired: true,
				Reason: ReconcileRepaired, Evidence: found.ref, Attempts: attempt,
			}, nil
		}
		if errors.Is(err, runstore.ErrConflict) {
			// Somebody advanced the record between the read and the write — a heartbeat, a
			// cancellation, another reconciler. Re-reading is the only way to know whether the
			// evidence still says the same thing.
			last = ReconcileReport{RunID: runID, Record: record, Evidence: found.ref, Attempts: attempt}
			if err := ctx.Err(); err != nil {
				return last, err
			}
			continue
		}
		if errors.Is(err, runstore.ErrInvalidTransition) || errors.Is(err, runstore.ErrOwnershipLost) {
			return ReconcileReport{
				RunID: runID, Record: record, Reason: ReconcileRefused, Detail: err.Error(),
				Evidence: found.ref, Attempts: attempt,
			}, err
		}
		return ReconcileReport{RunID: runID, Record: record, Attempts: attempt}, err
	}
	return last, fmt.Errorf("queue: run %q did not settle after %d reconcile attempts: %w",
		runID, maxReconcileAttempts, runstore.ErrConflict)
}

// evidence is one located terminal snapshot, already checked against the record.
type evidence struct {
	completion *checkpoint.Checkpoint
	ref        *checkpoint.Ref
	// report is set when the search produced no usable evidence: Reason and Detail explain the
	// stale or contradictory answer, and completion stays nil.
	report ReconcileReport
}

// findCompletion locates the committed Completion of this run and only this run. A record that
// already names its terminal snapshot is checked through that name; otherwise the run's own
// timeline is searched, so a newer snapshot from another run on the same thread cannot be
// mistaken for this one's outcome.
func (r *RunReconciler) findCompletion(ctx context.Context, record runstore.RunRecord) (evidence, error) {
	var located *checkpoint.CheckpointMetadata
	if record.CompletionRef != nil {
		metadata, ok, err := r.metadata.ReadCheckpointMetadata(ctx, *record.CompletionRef)
		if err != nil {
			return evidence{}, err
		}
		if !ok {
			// The record points at a snapshot this store does not have. That is a
			// contradiction between two durable facts, not an absence of evidence, and the
			// difference is what an operator needs: nothing is written either way.
			return evidence{report: ReconcileReport{
				Reason: ReconcileContradictory,
				Detail: fmt.Sprintf("the record names completion %s/%s, which this store does not hold",
					record.CompletionRef.ThreadID, record.CompletionRef.CheckpointID),
			}}, nil
		}
		located = &metadata
	} else {
		candidates, err := r.completionsFor(ctx, record)
		if err != nil {
			return evidence{}, err
		}
		switch len(candidates) {
		case 0:
			return evidence{report: ReconcileReport{
				Reason: ReconcileStale,
				Detail: fmt.Sprintf("no committed terminal snapshot carries run %q", record.RunID),
			}}, nil
		case 1:
			located = &candidates[0]
		default:
			return evidence{report: ReconcileReport{
				Reason: ReconcileContradictory,
				Detail: fmt.Sprintf("%d terminal snapshots carry run %q; which one is the outcome is not this call's to decide",
					len(candidates), record.RunID),
			}}, nil
		}
	}

	if located.Ref.ThreadID != record.ThreadID {
		return evidence{report: ReconcileReport{
			Reason: ReconcileContradictory,
			Detail: fmt.Sprintf("the terminal snapshot lives on thread %q, the record belongs to %q",
				located.Ref.ThreadID, record.ThreadID),
		}}, nil
	}
	if located.RunID != record.RunID {
		return evidence{report: ReconcileReport{
			Reason: ReconcileContradictory,
			Detail: fmt.Sprintf("the terminal snapshot carries run %q, not %q", located.RunID, record.RunID),
		}}, nil
	}
	if located.CompletionKind == "" {
		return evidence{report: ReconcileReport{
			Reason: ReconcileStale,
			Detail: fmt.Sprintf("snapshot %s carries no terminal record for run %q", located.Ref.CheckpointID, record.RunID),
		}}, nil
	}

	// The summary says a terminal record exists; the frozen outcome itself is read from the
	// snapshot, because a list row never carries the result body.
	cp, err := r.snapshots.Load(ctx, located.Ref.ThreadID, located.Ref.CheckpointID)
	if err != nil {
		return evidence{}, fmt.Errorf("queue: load completion %s/%s: %w", located.Ref.ThreadID, located.Ref.CheckpointID, err)
	}
	if cp == nil || cp.Completion == nil {
		return evidence{report: ReconcileReport{
			Reason: ReconcileContradictory,
			Detail: fmt.Sprintf("snapshot %s/%s is listed as terminal but carries no terminal record",
				located.Ref.ThreadID, located.Ref.CheckpointID),
		}}, nil
	}
	if cp.Completion.RunID != record.RunID {
		return evidence{report: ReconcileReport{
			Reason: ReconcileContradictory,
			Detail: fmt.Sprintf("the terminal record belongs to run %q, not %q", cp.Completion.RunID, record.RunID),
		}}, nil
	}
	if cp.Completion.Kind != located.CompletionKind {
		return evidence{report: ReconcileReport{
			Reason: ReconcileContradictory,
			Detail: fmt.Sprintf("the summary says %q and the snapshot says %q", located.CompletionKind, cp.Completion.Kind),
		}}, nil
	}
	switch cp.Completion.Kind {
	case checkpoint.CompletionCompleted, checkpoint.CompletionFailed:
	default:
		// An unknown kind is not a reason to write a status this package invented.
		return evidence{report: ReconcileReport{
			Reason:   ReconcileContradictory,
			Evidence: &located.Ref,
			Detail:   fmt.Sprintf("terminal record kind %q is not one this package knows", cp.Completion.Kind),
		}}, nil
	}

	ref := located.Ref
	return evidence{completion: cp, ref: &ref}, nil
}

// completionsFor walks this run's own timeline and returns the snapshots carrying a terminal
// record. The search is by RunID and deliberately not by thread: a terminal snapshot committed
// on another thread is still this run's claim to make, and finding it is what lets the check
// below say "that outcome belongs to a different thread" instead of reporting that nothing was
// found. The walk is bounded by one run's history and pages through it, so a long thread does
// not turn a repair into a full read.
func (r *RunReconciler) completionsFor(ctx context.Context, record runstore.RunRecord) ([]checkpoint.CheckpointMetadata, error) {
	opts := checkpoint.ListOptions{
		Limit: checkpoint.MaxQueryLimit,
		RunID: record.RunID,
	}
	var found []checkpoint.CheckpointMetadata
	for {
		page, err := r.query.QueryCheckpoints(ctx, opts)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if item.CompletionKind != "" {
				found = append(found, item)
			}
		}
		if page.NextCursor == "" {
			return found, nil
		}
		opts.Cursor = page.NextCursor
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// terminalFromCompletion publishes the outcome the terminal snapshot froze, onto the record as
// the store holds it now. Identity, ownership, the cancellation request and the heartbeat come
// from that record and are restated unchanged: the CAS is authorised for the worker that owns
// the run, and a reconcile is that write on its behalf — it does not take ownership, and it
// does not clear a request the run ignored.
//
// What it says about the outcome is exactly what the snapshot says. A run cancelled after it
// had already committed a boundary records a failed terminal snapshot, so the repair reports a
// failure with the cancellation request time still on the record; nothing here upgrades or
// downgrades that into a user cancellation the checkpoint never recorded.
func terminalFromCompletion(record runstore.RunRecord, cp *checkpoint.Checkpoint) runstore.RunRecord {
	next := record
	ref := checkpoint.Ref{ThreadID: cp.ThreadID, CheckpointID: cp.ID}
	next.CompletionRef = &ref
	// The terminal snapshot is durable, which is precisely what the recording error said was
	// missing. Clearing it is not optimism about the write path; it is the evidence itself.
	next.RecordError = nil

	switch cp.Completion.Kind {
	case checkpoint.CompletionFailed:
		next.Status = runstore.StatusFailed
		next.FinishReason = runstore.FinishReasonFailed
		next.Result = nil
		message := cp.Completion.Err
		if message == "" {
			message = "the run's committed terminal record reports a failure without a message"
		}
		next.Error = &runstore.RunError{
			Code:      runstore.ErrCodeExecutionFailed,
			Message:   message,
			Retryable: false,
		}
	default:
		next.Status = runstore.StatusSucceeded
		next.FinishReason = runstore.FinishReasonCompleted
		next.Error = nil
		if cp.Completion.Result != nil {
			next.Result = cloneResult(*cp.Completion.Result)
		} else {
			next.Result = nil
		}
	}
	return next
}
