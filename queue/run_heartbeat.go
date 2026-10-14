package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jiujuan/goagent/runstore"
)

// This file is the task-record half of a running managed delivery: one loop that keeps the
// record's liveness fields current, notices a cancellation somebody else persisted, and stops
// trusting this worker's ownership when the record says otherwise.
//
// It deliberately does not touch a checkpoint claim. Renewing that is the agent's write guard
// (see agent.WithClaimLease), and a heartbeat on the task record cannot keep a thread claim
// alive — one is a queue's bookkeeping write, the other is the store's fencing token
// (ADR-0034 D6).

// defaultHeartbeatInterval is how often a running worker rewrites its record's liveness
// fields. A heartbeat only says "this worker is still here"; it never changes the run's
// status, moves a checkpoint head or renews a claim.
const defaultHeartbeatInterval = 5 * time.Second

// minHeartbeatsPerLease is how many heartbeat intervals one worker lease must cover. A lease
// shorter than that would let a single slow write make a live run look abandoned.
const minHeartbeatsPerLease = 3

// runTicker is the ticker a task watch waits on. Tests inject their own so a run can cross
// several leases without real time passing.
type runTicker interface {
	Chan() <-chan time.Time
	Stop()
}

type runtimeRunTicker struct{ *time.Ticker }

func (t runtimeRunTicker) Chan() <-chan time.Time { return t.C }

// taskWatch supervises one claimed record while its run executes. Everything it reports is
// about the task record; a run keeps going when a write fails, because a queue hiccup is not
// a cancellation and not a loss of ownership.
type taskWatch struct {
	store    runstore.Store
	runID    string
	workerID string
	epoch    uint64

	lease     time.Duration
	heartbeat time.Duration
	poll      time.Duration

	now       func() time.Time
	newTicker func(time.Duration) runTicker

	mu              sync.Mutex
	lastRecorded    time.Time // last successful look at the record, read or write
	lastHeartbeatAt time.Time // liveness the store actually carries
	lost            error     // set when this delivery may no longer write the record
	heartbeats      int
	failures        int
	lastErr         error
}

// newTaskWatch builds the watch for one claim. The claim itself already recorded ownership
// and a lease, so the first heartbeat is due one heartbeat interval after it.
func (w *ManagedRunWorker) newTaskWatch(claimed runstore.RunRecord) *taskWatch {
	started := w.now()
	return &taskWatch{
		store:           w.store,
		runID:           claimed.RunID,
		workerID:        w.workerID,
		epoch:           claimed.WorkerEpoch,
		lease:           w.lease,
		heartbeat:       w.heartbeat,
		poll:            w.cancelPoll,
		now:             w.now,
		newTicker:       w.newTicker,
		lastRecorded:    started,
		lastHeartbeatAt: started,
	}
}

// run is the loop: wait, look, act. It exits with the execution it is watching — when the
// delivery settles or its context ends — so no heartbeat is written after a terminal state.
func (t *taskWatch) run(ctx context.Context, stop <-chan struct{}, cancelExec context.CancelFunc) {
	ticker := t.newTicker(t.poll)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.Chan():
		}
		t.tick(ctx, cancelExec)
	}
}

// tick does one look at the record. It is separated from run so a test can step through
// several leases without waiting on a ticker. Once this delivery has stopped trusting its own
// ownership it writes nothing more, even if the store answers again.
func (t *taskWatch) tick(ctx context.Context, cancelExec context.CancelFunc) {
	if t.ownershipLost() != nil {
		return
	}
	now := t.now()
	current, err := t.store.Get(ctx, t.runID)
	if err != nil {
		t.noteFailure(err, now)
		return
	}
	if current.WorkerID != t.workerID || current.WorkerEpoch != t.epoch {
		// The record belongs to somebody else now: stop this run and never write another
		// terminal state over the new owner's.
		t.noteLost(fmt.Errorf("queue: run %q is held by worker %q epoch %d, not %q epoch %d: %w",
			t.runID, current.WorkerID, current.WorkerEpoch, t.workerID, t.epoch, runstore.ErrOwnershipLost))
		cancelExec()
		return
	}
	t.noteSuccess(now)

	if current.Status.Terminal() {
		cancelExec()
		return
	}
	if !current.CancelRequestedAt.IsZero() || current.Status == runstore.StatusCancelling {
		// The request is already durable; the worker's job is to stop, and the terminal
		// publish then records what the run actually reached.
		cancelExec()
		return
	}
	if now.Sub(t.lastHeartbeat()) < t.heartbeat {
		return
	}

	next := current
	next.HeartbeatAt = now
	next.WorkerLeaseUntil = now.Add(t.lease)
	updated, err := t.store.CompareAndSwap(ctx, current.RunID, current.Version, next)
	if err != nil {
		if errors.Is(err, runstore.ErrOwnershipLost) {
			t.noteLost(err)
			cancelExec()
			return
		}
		// A conflict or a failed write leaves the stored liveness where it was: the next
		// tick re-reads and tries again, and the lease keeps running down.
		t.noteFailure(err, now)
		return
	}
	t.noteSuccess(now)
	t.mu.Lock()
	t.heartbeats++
	t.lastHeartbeatAt = updated.HeartbeatAt
	t.mu.Unlock()
}

// lastHeartbeat is the liveness time the store is known to carry: the claim's own stamp, or
// the last heartbeat write that landed.
func (t *taskWatch) lastHeartbeat() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastHeartbeatAt
}

// ownershipLost reports why this delivery may no longer publish, nil while it still owns the
// record.
func (t *taskWatch) ownershipLost() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lost
}

// stats reports what the loop did: heartbeats written, failed accesses and the last error.
func (t *taskWatch) stats() (heartbeats, failures int, lastErr error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.heartbeats, t.failures, t.lastErr
}

func (t *taskWatch) noteSuccess(now time.Time) {
	t.mu.Lock()
	t.lastRecorded = now
	t.mu.Unlock()
}

func (t *taskWatch) noteFailure(err error, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failures++
	t.lastErr = err
	// A record this worker has not managed to touch for a whole lease is no longer
	// evidence that it owns the run: stop publishing rather than risk overwriting a
	// recovered record with a stale epoch.
	if t.lost == nil && now.Sub(t.lastRecorded) >= t.lease {
		t.lost = fmt.Errorf("queue: run %q liveness unrecorded for %v past its %v lease: %w",
			t.runID, now.Sub(t.lastRecorded).Round(time.Millisecond), t.lease, err)
	}
}

func (t *taskWatch) noteLost(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lost == nil {
		t.lost = err
	}
}
