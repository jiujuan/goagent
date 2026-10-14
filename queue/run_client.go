package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jiujuan/goagent/runstore"
)

// RunClient waits on and cancels a managed run through its task record alone, so a caller
// that has no *agent.Run — a different process, a reconnecting UI — can still follow the run
// it submitted (ADR-0034 D5). It is bound to a runstore.Store; the agent, the queue and the
// checkpoint history stay somebody else's business.
//
// The three methods read and write the record only. Nothing here drives a run, reconciles a
// stale record, renews a lease or repairs a checkpoint: a queued run stays queued no matter
// how long a client waits on it.
type RunClient struct {
	store   runstore.Store
	minWait time.Duration
	maxWait time.Duration
}

// Polling bounds for WaitRun. A run is executed by a worker on its own schedule, so waiting
// is a poll, not a subscription; the interval backs off from minWait up to maxWait.
const (
	defaultRunPollInterval = 250 * time.Millisecond
	defaultRunPollMax      = 2 * time.Second
)

// RunClientOption tunes a RunClient's polling.
type RunClientOption func(*RunClient)

// WithRunPollInterval sets the first wait between two reads. It must be positive.
func WithRunPollInterval(d time.Duration) RunClientOption {
	return func(c *RunClient) { c.minWait = d }
}

// WithRunPollMax caps the wait between two reads. It must be at least the poll interval.
func WithRunPollMax(d time.Duration) RunClientOption {
	return func(c *RunClient) { c.maxWait = d }
}

// NewRunClient binds a client to one task store.
func NewRunClient(store runstore.Store, opts ...RunClientOption) (*RunClient, error) {
	if store == nil {
		return nil, fmt.Errorf("queue: run client needs a run store")
	}
	c := &RunClient{store: store, minWait: defaultRunPollInterval, maxWait: defaultRunPollMax}
	for _, opt := range opts {
		opt(c)
	}
	if c.minWait <= 0 {
		return nil, fmt.Errorf("queue: run poll interval must be positive, got %v", c.minWait)
	}
	if c.maxWait < c.minWait {
		return nil, fmt.Errorf("queue: run poll max %v is below the interval %v", c.maxWait, c.minWait)
	}
	return c, nil
}

// GetRun reads one record as the store holds it. It never changes anything: a record whose
// worker lease passed is reported as it stands, not as failed or stale. An unknown RunID
// reports runstore.ErrNotFound.
func (c *RunClient) GetRun(ctx context.Context, runID string) (runstore.RunRecord, error) {
	return c.store.Get(ctx, runID)
}

// WaitRun returns once the record reaches a state that ends this run: paused, succeeded,
// failed or cancelled. A pause is an end for this run — a resume is a new record — so waiting
// here never means waiting for an approval.
//
// ctx bounds the wait. On cancellation or deadline the record last read is returned together
// with ctx's error, so a caller can report where the run stood when it gave up. An unknown
// RunID reports runstore.ErrNotFound without waiting, and a read that fails mid-wait is
// returned rather than retried blindly: the client cannot tell a store outage from a
// deleted run.
func (c *RunClient) WaitRun(ctx context.Context, runID string) (runstore.RunRecord, error) {
	var last runstore.RunRecord
	delay := c.minWait
	for {
		record, err := c.store.Get(ctx, runID)
		if err != nil {
			return runstore.RunRecord{}, err
		}
		last = record
		if record.Status.Terminal() {
			return record, nil
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, ctx.Err()
		case <-timer.C:
		}
		delay = c.nextWait(delay)
	}
}

// nextWait is the pause after the one just waited: doubling until the cap. A run that has
// not started yet is usually a run no worker has picked up, so reading faster cannot make it
// go sooner.
func (c *RunClient) nextWait(waited time.Duration) time.Duration {
	if next := waited * 2; next < c.maxWait {
		return next
	}
	return c.maxWait
}

// CancelRun persists a cancellation request and reports the record as it then stands. It is
// a request, not a stop: the run keeps executing until a worker observes it, and nothing here
// undoes effects the run already produced (ADR-0034 D4).
//
// queued goes straight to cancelled, because no worker holds it; running and cancelling carry
// the request as cancelling, and the worker that holds the run decides the final state — a
// run that reaches its outcome before noticing the request keeps that outcome.
//
// A record that already ended, or already carries the request, comes back unchanged: asking
// twice does not advance the version. A record that changed under this call is re-read and
// re-decided, so a concurrent heartbeat or claim cannot make the request get lost.
func (c *RunClient) CancelRun(ctx context.Context, runID string) (runstore.RunRecord, error) {
	for {
		record, err := c.store.Get(ctx, runID)
		if err != nil {
			return runstore.RunRecord{}, err
		}
		if record.Status.Terminal() || record.Status == runstore.StatusCancelling {
			return record, nil
		}

		next := record
		if next.CancelRequestedAt.IsZero() {
			next.CancelRequestedAt = time.Now().UTC()
		}
		if record.Status == runstore.StatusQueued {
			next.Status = runstore.StatusCancelled
			next.FinishReason = runstore.FinishReasonCancelled
			next.Result = nil
			next.Error = nil
		} else {
			next.Status = runstore.StatusCancelling
		}

		updated, err := c.store.CompareAndSwap(ctx, runID, record.Version, next)
		if err == nil {
			return updated, nil
		}
		if !errors.Is(err, runstore.ErrConflict) {
			return runstore.RunRecord{}, err
		}
		if err := ctx.Err(); err != nil {
			return runstore.RunRecord{}, err
		}
		// Somebody else advanced the record between the read and the swap. Reading again is
		// the only way to know whether the request still needs writing.
	}
}
