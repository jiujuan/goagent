package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

var errWriteGuardStopped = errors.New("agent: write guard stopped")

const writeGuardReleaseTimeout = 5 * time.Second

type writeGuardTicker interface {
	Chan() <-chan time.Time
	Stop()
}

type runtimeWriteGuardTicker struct{ *time.Ticker }

func (t runtimeWriteGuardTicker) Chan() <-chan time.Time { return t.C }

type writeGuard struct {
	store checkpoint.ThreadCoordinator

	opMu sync.Mutex
	mu   sync.Mutex

	claim        checkpoint.Claim
	headRevision uint64
	lease        time.Duration
	renewCancel  context.CancelFunc
	renewDone    chan struct{}
	firstErr     error
	started      bool
	stopped      bool

	newTicker func(time.Duration) writeGuardTicker
	stopOnce  sync.Once
	stopErr   error
}

func newWriteGuard(store checkpoint.ThreadCoordinator, claim checkpoint.Claim, headRevision uint64, lease time.Duration) *writeGuard {
	if lease <= 0 {
		lease = checkpoint.DefaultLease
	}
	return &writeGuard{
		store:        store,
		claim:        claim,
		headRevision: headRevision,
		lease:        lease,
		newTicker: func(interval time.Duration) writeGuardTicker {
			return runtimeWriteGuardTicker{Ticker: time.NewTicker(interval)}
		},
	}
}

func (g *writeGuard) start(ctx context.Context, cancel context.CancelFunc) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if cancel == nil {
		cancel = func() {}
	}

	g.opMu.Lock()
	defer g.opMu.Unlock()

	g.mu.Lock()
	if g.stopped {
		err := g.firstErr
		g.mu.Unlock()
		if err != nil {
			return err
		}
		return errWriteGuardStopped
	}
	if g.started {
		err := g.firstErr
		g.mu.Unlock()
		return err
	}
	g.started = true
	claim := g.claim
	g.mu.Unlock()

	renewed, err := g.store.RenewClaim(ctx, claim, g.lease)
	if err != nil {
		recorded := g.recordErrorUnlessStopped(err)
		if ctx.Err() == nil && recorded {
			cancel()
		}
		return err
	}

	renewCtx, renewCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	g.mu.Lock()
	g.claim = renewed
	if g.stopped {
		g.mu.Unlock()
		renewCancel()
		return errWriteGuardStopped
	}
	g.renewCancel = renewCancel
	g.renewDone = done
	g.mu.Unlock()

	ticker := g.newTicker(writeGuardRenewInterval(g.lease))
	go g.renewLoop(renewCtx, cancel, ticker, done)
	return nil
}

func (g *writeGuard) commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.opMu.Lock()
	defer g.opMu.Unlock()

	g.mu.Lock()
	if g.firstErr != nil {
		err := g.firstErr
		g.mu.Unlock()
		return checkpoint.CommitReceipt{}, err
	}
	if g.stopped {
		g.mu.Unlock()
		return checkpoint.CommitReceipt{}, errWriteGuardStopped
	}
	headRevision := g.headRevision
	claim := g.claim
	g.mu.Unlock()

	req.ExpectedRevision = &headRevision
	req.Claim = &claim
	req.Lease = g.lease
	req.ReleaseClaim = false
	receipt, err := g.store.Commit(ctx, req)
	if err != nil {
		return checkpoint.CommitReceipt{}, err
	}

	g.mu.Lock()
	g.headRevision = receipt.Revision
	if receipt.Claim != nil {
		g.claim = *receipt.Claim
	}
	g.mu.Unlock()
	return receipt, nil
}

func (g *writeGuard) err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.firstErr
}

func (g *writeGuard) stopAndRelease(ctx context.Context) error {
	g.stopOnce.Do(func() {
		g.stopErr = g.stop(ctx)
	})
	return g.stopErr
}

func (g *writeGuard) stop(ctx context.Context) error {
	g.mu.Lock()
	g.stopped = true
	renewCancel := g.renewCancel
	g.mu.Unlock()
	if renewCancel != nil {
		renewCancel()
	}

	// Synchronize with start or an in-flight operation, then cancel any renewal
	// loop that start installed after the first cancellation check.
	g.opMu.Lock()
	g.mu.Lock()
	renewCancel = g.renewCancel
	renewDone := g.renewDone
	g.mu.Unlock()
	if renewCancel != nil {
		renewCancel()
	}
	g.opMu.Unlock()

	if renewDone != nil {
		<-renewDone
	}

	g.opMu.Lock()
	defer g.opMu.Unlock()
	g.mu.Lock()
	claim := g.claim
	g.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeGuardReleaseTimeout)
	err := g.store.ReleaseClaim(releaseCtx, claim)
	cancel()
	if err != nil {
		g.recordError(err)
	}
	return g.err()
}

func (g *writeGuard) renewLoop(ctx context.Context, cancel context.CancelFunc, ticker writeGuardTicker, done chan<- struct{}) {
	defer close(done)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if err := g.renew(ctx); err != nil {
				if ctx.Err() != nil || g.isStopped() {
					return
				}
				cancel()
				return
			}
		}
	}
}

func (g *writeGuard) renew(ctx context.Context) error {
	g.opMu.Lock()
	defer g.opMu.Unlock()

	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return errWriteGuardStopped
	}
	if g.firstErr != nil {
		err := g.firstErr
		g.mu.Unlock()
		return err
	}
	claim := g.claim
	g.mu.Unlock()

	renewed, err := g.store.RenewClaim(ctx, claim, g.lease)
	if err != nil {
		if ctx.Err() == nil {
			g.recordErrorUnlessStopped(err)
		}
		return err
	}
	g.mu.Lock()
	g.claim = renewed
	g.mu.Unlock()
	return nil
}

func (g *writeGuard) recordError(err error) error {
	if err == nil {
		return nil
	}
	g.mu.Lock()
	if g.firstErr == nil {
		g.firstErr = err
	}
	first := g.firstErr
	g.mu.Unlock()
	return first
}

func (g *writeGuard) recordErrorUnlessStopped(err error) bool {
	if err == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	if g.firstErr == nil {
		g.firstErr = err
	}
	return true
}

func (g *writeGuard) isStopped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped
}

func writeGuardRenewInterval(lease time.Duration) time.Duration {
	interval := lease / 2
	if interval <= 0 {
		return time.Nanosecond
	}
	return interval
}
