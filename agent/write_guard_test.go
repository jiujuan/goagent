package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

func TestSnapshotConsistencyGuardRenewal(t *testing.T) {
	ticker := newGuardTestTicker()
	store := &guardTestCoordinator{}
	guard := newWriteGuard(store, guardTestClaim(3), 7, time.Minute)
	guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := guard.start(ctx, cancel); err != nil {
		t.Fatal(err)
	}
	if got := store.renewCount(); got != 1 {
		t.Fatalf("initial renew calls = %d, want 1", got)
	}

	ticker.tick()
	waitForGuard(t, func() bool { return store.renewCount() == 2 })
	if err := guard.stopAndRelease(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ticker.stopped:
	default:
		t.Fatal("renew ticker was not stopped")
	}
	if got := store.releaseCount(); got != 1 {
		t.Fatalf("release calls = %d, want 1", got)
	}
	if err := guard.stopAndRelease(ctx); err != nil {
		t.Fatalf("second stop error = %v", err)
	}
	if got := store.releaseCount(); got != 1 {
		t.Fatalf("release calls after second stop = %d, want 1", got)
	}
}

func TestSnapshotConsistencyGuardLostLease(t *testing.T) {
	t.Run("initial renewal", func(t *testing.T) {
		store := &guardTestCoordinator{
			renewFn: func(context.Context, checkpoint.Claim, time.Duration) (checkpoint.Claim, error) {
				return checkpoint.Claim{}, checkpoint.ErrStaleFencingToken
			},
		}
		guard := newWriteGuard(store, guardTestClaim(1), 4, time.Minute)
		ctx, cancel := context.WithCancel(context.Background())
		if err := guard.start(ctx, cancel); !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("start error = %v, want ErrStaleFencingToken", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("initial renewal failure did not cancel root context")
		}
		if _, err := guard.commit(context.Background(), guardTestCommit("after-loss")); !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("commit error = %v, want ErrStaleFencingToken", err)
		}
		if got := store.commitCount(); got != 0 {
			t.Fatalf("commit calls = %d, want 0", got)
		}
		_ = guard.stopAndRelease(ctx)
	})

	t.Run("initial renewal cancellation remains terminal", func(t *testing.T) {
		store := &guardTestCoordinator{
			renewFn: func(ctx context.Context, _ checkpoint.Claim, _ time.Duration) (checkpoint.Claim, error) {
				<-ctx.Done()
				return checkpoint.Claim{}, ctx.Err()
			},
		}
		guard := newWriteGuard(store, guardTestClaim(1), 4, time.Minute)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := guard.start(ctx, cancel); !errors.Is(err, context.Canceled) {
			t.Fatalf("start error = %v, want context.Canceled", err)
		}
		if _, err := guard.commit(context.Background(), guardTestCommit("after-canceled-start")); !errors.Is(err, context.Canceled) {
			t.Fatalf("commit error = %v, want context.Canceled", err)
		}
		if got := store.commitCount(); got != 0 {
			t.Fatalf("commit calls = %d, want 0", got)
		}
		_ = guard.stopAndRelease(ctx)
	})

	t.Run("background renewal", func(t *testing.T) {
		ticker := newGuardTestTicker()
		var renews int
		store := &guardTestCoordinator{
			renewFn: func(_ context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
				renews++
				if renews > 1 {
					return checkpoint.Claim{}, checkpoint.ErrStaleFencingToken
				}
				claim.ExpiresAt = time.Now().Add(lease)
				return claim, nil
			},
		}
		guard := newWriteGuard(store, guardTestClaim(2), 8, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		ctx, cancel := context.WithCancel(context.Background())
		if err := guard.start(ctx, cancel); err != nil {
			t.Fatal(err)
		}
		ticker.tick()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("background renewal failure did not cancel root context")
		}
		if err := guard.err(); !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("guard error = %v, want ErrStaleFencingToken", err)
		}
		if _, err := guard.commit(context.Background(), guardTestCommit("after-background-loss")); !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("commit error = %v, want ErrStaleFencingToken", err)
		}
		if got := store.commitCount(); got != 0 {
			t.Fatalf("commit calls = %d, want 0", got)
		}
		_ = guard.stopAndRelease(ctx)
	})
}

func TestSnapshotConsistencyGuardCommitSerial(t *testing.T) {
	t.Run("serializes commits and advances revision", func(t *testing.T) {
		ticker := newGuardTestTicker()
		firstEntered := make(chan struct{})
		releaseFirst := make(chan struct{})
		var once sync.Once
		store := &guardTestCoordinator{
			commitFn: func(_ context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
				once.Do(func() {
					close(firstEntered)
					<-releaseFirst
				})
				revision := *req.ExpectedRevision + 1
				claim := *req.Claim
				claim.ExpiresAt = time.Now().Add(req.Lease)
				return checkpoint.CommitReceipt{Revision: revision, Claim: &claim}, nil
			},
		}
		guard := newWriteGuard(store, guardTestClaim(4), 5, 2*time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := guard.start(ctx, cancel); err != nil {
			t.Fatal(err)
		}

		results := make(chan checkpoint.CommitReceipt, 2)
		errs := make(chan error, 2)
		go func() {
			receipt, err := guard.commit(ctx, guardTestCommit("first"))
			results <- receipt
			errs <- err
		}()
		<-firstEntered
		go func() {
			receipt, err := guard.commit(ctx, guardTestCommit("second"))
			results <- receipt
			errs <- err
		}()
		ticker.tick()
		select {
		case <-time.After(30 * time.Millisecond):
		case <-store.secondCommitEntered():
			t.Fatal("second commit entered store before first completed")
		}
		if got := store.renewCount(); got != 1 {
			t.Fatalf("renew calls during commit = %d, want only initial renewal", got)
		}
		close(releaseFirst)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		gotReceipts := map[uint64]bool{(<-results).Revision: true, (<-results).Revision: true}
		if !gotReceipts[6] || !gotReceipts[7] {
			t.Fatalf("commit revisions = %v, want 6 and 7", gotReceipts)
		}
		waitForGuard(t, func() bool { return store.renewCount() == 2 })

		requests := store.commitRequests()
		if len(requests) != 2 {
			t.Fatalf("commit requests = %d, want 2", len(requests))
		}
		for i, wantRevision := range []uint64{5, 6} {
			req := requests[i]
			if req.ExpectedRevision == nil || *req.ExpectedRevision != wantRevision {
				t.Fatalf("commit %d expected revision = %v, want %d", i, req.ExpectedRevision, wantRevision)
			}
			if req.Claim == nil || req.Claim.FencingToken != 4 {
				t.Fatalf("commit %d claim = %+v", i, req.Claim)
			}
			if req.ReleaseClaim {
				t.Fatalf("commit %d released root claim", i)
			}
			if req.Lease != 2*time.Minute {
				t.Fatalf("commit %d lease = %s, want 2m", i, req.Lease)
			}
		}
		if err := guard.stopAndRelease(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed commit keeps revision", func(t *testing.T) {
		commitErr := errors.New("commit failed")
		var attempts int
		store := &guardTestCoordinator{
			commitFn: func(_ context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
				attempts++
				if attempts == 1 {
					return checkpoint.CommitReceipt{}, commitErr
				}
				return checkpoint.CommitReceipt{Revision: *req.ExpectedRevision + 1, Claim: req.Claim}, nil
			},
		}
		guard := newWriteGuard(store, guardTestClaim(5), 11, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return newGuardTestTicker() }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := guard.start(ctx, cancel); err != nil {
			t.Fatal(err)
		}
		if _, err := guard.commit(ctx, guardTestCommit("failed")); !errors.Is(err, commitErr) {
			t.Fatalf("first commit error = %v, want %v", err, commitErr)
		}
		if _, err := guard.commit(ctx, guardTestCommit("retry")); err != nil {
			t.Fatal(err)
		}
		requests := store.commitRequests()
		if len(requests) != 2 || *requests[0].ExpectedRevision != 11 || *requests[1].ExpectedRevision != 11 {
			t.Fatalf("expected revisions after failure = %v, %v", requests[0].ExpectedRevision, requests[1].ExpectedRevision)
		}
		_ = guard.stopAndRelease(ctx)
	})
}

func TestSnapshotConsistencyGuardStop(t *testing.T) {
	t.Run("cancels renewal and releases once with a live context", func(t *testing.T) {
		ticker := newGuardTestTicker()
		renewEntered := make(chan struct{})
		var renews int
		store := &guardTestCoordinator{
			renewFn: func(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
				renews++
				if renews == 1 {
					claim.ExpiresAt = time.Now().Add(lease)
					return claim, nil
				}
				close(renewEntered)
				<-ctx.Done()
				return checkpoint.Claim{}, ctx.Err()
			},
			releaseFn: func(ctx context.Context, _ checkpoint.Claim) error {
				if err := ctx.Err(); err != nil {
					t.Errorf("release context error = %v, want live context", err)
				}
				return nil
			},
		}
		guard := newWriteGuard(store, guardTestClaim(6), 2, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		root, cancelRoot := context.WithCancel(context.Background())
		if err := guard.start(root, cancelRoot); err != nil {
			t.Fatal(err)
		}
		ticker.tick()
		<-renewEntered
		cancelRoot()
		if err := guard.stopAndRelease(root); err != nil {
			t.Fatalf("stop error = %v", err)
		}
		if err := guard.stopAndRelease(root); err != nil {
			t.Fatalf("second stop error = %v", err)
		}
		if got := store.releaseCount(); got != 1 {
			t.Fatalf("release calls = %d, want 1", got)
		}
		if err := guard.err(); err != nil {
			t.Fatalf("guard error after intentional stop = %v", err)
		}
		if _, err := guard.commit(context.Background(), guardTestCommit("stopped")); err == nil {
			t.Fatal("commit after stop succeeded")
		}
	})

	t.Run("returns release failure", func(t *testing.T) {
		releaseErr := errors.New("release failed")
		store := &guardTestCoordinator{
			releaseFn: func(context.Context, checkpoint.Claim) error { return releaseErr },
		}
		guard := newWriteGuard(store, guardTestClaim(7), 0, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return newGuardTestTicker() }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := guard.start(ctx, cancel); err != nil {
			t.Fatal(err)
		}
		if err := guard.stopAndRelease(ctx); !errors.Is(err, releaseErr) {
			t.Fatalf("stop error = %v, want release failure", err)
		}
		if err := guard.stopAndRelease(ctx); !errors.Is(err, releaseErr) {
			t.Fatalf("second stop error = %v, want release failure", err)
		}
		if got := store.releaseCount(); got != 1 {
			t.Fatalf("release calls = %d, want 1", got)
		}
	})
}

type guardTestCoordinator struct {
	checkpoint.ThreadCoordinator

	mu         sync.Mutex
	renews     []checkpoint.Claim
	commits    []checkpoint.CommitRequest
	releases   []checkpoint.Claim
	renewFn    func(context.Context, checkpoint.Claim, time.Duration) (checkpoint.Claim, error)
	commitFn   func(context.Context, checkpoint.CommitRequest) (checkpoint.CommitReceipt, error)
	releaseFn  func(context.Context, checkpoint.Claim) error
	secondSeen chan struct{}
}

func (s *guardTestCoordinator) RenewClaim(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
	s.mu.Lock()
	s.renews = append(s.renews, claim)
	fn := s.renewFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, claim, lease)
	}
	claim.ExpiresAt = time.Now().Add(lease)
	return claim, nil
}

func (s *guardTestCoordinator) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	stored := req
	if req.ExpectedRevision != nil {
		revision := *req.ExpectedRevision
		stored.ExpectedRevision = &revision
	}
	stored.Claim = copyClaim(req.Claim)
	s.mu.Lock()
	s.commits = append(s.commits, stored)
	if len(s.commits) == 2 && s.secondSeen != nil {
		close(s.secondSeen)
	}
	fn := s.commitFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return checkpoint.CommitReceipt{}, nil
}

func (s *guardTestCoordinator) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	s.mu.Lock()
	s.releases = append(s.releases, claim)
	fn := s.releaseFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, claim)
	}
	return nil
}

func (s *guardTestCoordinator) renewCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.renews)
}

func (s *guardTestCoordinator) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

func (s *guardTestCoordinator) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.releases)
}

func (s *guardTestCoordinator) commitRequests() []checkpoint.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.CommitRequest(nil), s.commits...)
}

func (s *guardTestCoordinator) secondCommitEntered() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.secondSeen == nil {
		s.secondSeen = make(chan struct{})
		if len(s.commits) >= 2 {
			close(s.secondSeen)
		}
	}
	return s.secondSeen
}

type guardTestTicker struct {
	ch      chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func newGuardTestTicker() *guardTestTicker {
	return &guardTestTicker{ch: make(chan time.Time, 1), stopped: make(chan struct{})}
}

func (t *guardTestTicker) Chan() <-chan time.Time { return t.ch }

func (t *guardTestTicker) Stop() { t.once.Do(func() { close(t.stopped) }) }

func (t *guardTestTicker) tick() { t.ch <- time.Now() }

func guardTestClaim(fencing uint64) checkpoint.Claim {
	return checkpoint.Claim{
		ThreadID:     "guard-thread",
		Revision:     1,
		PauseID:      "thread-root",
		Owner:        "guard-owner",
		FencingToken: fencing,
		ExpiresAt:    time.Now().Add(time.Minute),
	}
}

func guardTestCommit(id string) checkpoint.CommitRequest {
	return checkpoint.CommitRequest{
		Checkpoint:   &checkpoint.Checkpoint{ID: id, ThreadID: "guard-thread"},
		ReleaseClaim: true,
	}
}

func waitForGuard(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for guard condition")
		}
		time.Sleep(time.Millisecond)
	}
}
