package agent

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

type cleanupContextKey struct{}

func checkCleanupContext(t *testing.T, ctx context.Context, deadline time.Time) {
	t.Helper()
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Errorf("cleanup deadline = %v, %v; want %v", got, ok, deadline)
	}
	if got := ctx.Value(cleanupContextKey{}); got != "cleanup-value" {
		t.Errorf("cleanup context value = %v, want cleanup-value", got)
	}
}

func cleanupRunContext(ctx context.Context, store *guardTestCoordinator, guarded bool) *RunContext {
	rc := &RunContext{Context: ctx, Store: store}
	claim := guardTestClaim(1)
	rc.setDurableClaim(&claim)
	if guarded {
		rc.ensureDurableState().guard = newWriteGuard(store, claim, 0, time.Minute)
	}
	return rc
}

func TestGuardedReleaseUsesExistingTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent := context.WithValue(context.Background(), cleanupContextKey{}, "cleanup-value")
		deadline := time.Now().Add(time.Second)
		ctx, cancel := context.WithDeadline(parent, deadline)
		defer cancel()
		store := &guardTestCoordinator{releaseFn: func(got context.Context, _ checkpoint.Claim) error {
			checkCleanupContext(t, got, deadline)
			if got != ctx {
				t.Error("stop replaced its existing cleanup context")
			}
			return nil
		}}
		guard := newWriteGuard(store, guardTestClaim(1), 0, time.Minute)
		if err := guard.stop(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUnguardedReleaseUsesCleanupTimeout(t *testing.T) {
	for _, parentKind := range []string{"live", "short execution deadline", "nil"} {
		t.Run(parentKind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var parent context.Context = context.WithValue(context.Background(), cleanupContextKey{}, "cleanup-value")
				if parentKind == "short execution deadline" {
					var cancel context.CancelFunc
					parent, cancel = context.WithTimeout(parent, time.Second)
					defer cancel()
				} else if parentKind == "nil" {
					parent = nil
				}
				deadline := time.Now().Add(writeGuardReleaseTimeout)
				var releaseCtx context.Context
				store := &guardTestCoordinator{releaseFn: func(ctx context.Context, _ checkpoint.Claim) error {
					releaseCtx = ctx
					if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
						t.Errorf("release deadline = %v, %v; want %v", got, ok, deadline)
					}
					if parent != nil {
						checkCleanupContext(t, ctx, deadline)
					}
					return ctx.Err()
				}}
				rc := cleanupRunContext(parent, store, false)
				if err := rc.releaseDurableClaim(); err != nil {
					t.Fatal(err)
				}
				if releaseCtx.Err() != context.Canceled {
					t.Errorf("cleanup context after release = %v, want canceled", releaseCtx.Err())
				}
				if err := rc.releaseDurableClaim(); err != nil || store.releaseCount() != 1 || rc.durableClaim() != nil {
					t.Fatalf("second cleanup: error=%v releases=%d claim=%v", err, store.releaseCount(), rc.durableClaim())
				}
			})
		})
	}
}

func TestCanceledCleanupStillReleasesClaim(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "unguarded", true: "guarded"}[guarded], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.WithValue(context.Background(), cleanupContextKey{}, "cleanup-value"))
				cancel()
				deadline := time.Now().Add(writeGuardReleaseTimeout)
				store := &guardTestCoordinator{releaseFn: func(ctx context.Context, _ checkpoint.Claim) error {
					checkCleanupContext(t, ctx, deadline)
					return ctx.Err()
				}}
				rc := cleanupRunContext(parent, store, guarded)
				if err := rc.releaseDurableClaim(); err != nil {
					t.Fatal(err)
				}
				if store.releaseCount() != 1 || rc.durableClaim() != nil {
					t.Fatalf("releases=%d claim=%v", store.releaseCount(), rc.durableClaim())
				}
			})
		})
	}
}

type cleanupBarrierTicker struct {
	*guardTestTicker
	stopping chan struct{}
	finish   chan struct{}
}

func (t *cleanupBarrierTicker) Stop() {
	close(t.stopping)
	<-t.finish
	t.guardTestTicker.Stop()
}

func TestCleanupWaitsForRenewalBeforeRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ticker := &cleanupBarrierTicker{guardTestTicker: newGuardTestTicker(), stopping: make(chan struct{}), finish: make(chan struct{})}
		renewEntered := make(chan struct{})
		renewCanceled := make(chan struct{})
		var renews atomic.Int32
		var deadline time.Time
		store := &guardTestCoordinator{renewFn: func(ctx context.Context, claim checkpoint.Claim, _ time.Duration) (checkpoint.Claim, error) {
			if renews.Add(1) == 1 {
				return claim, nil
			}
			close(renewEntered)
			<-ctx.Done()
			close(renewCanceled)
			return checkpoint.Claim{}, ctx.Err()
		}}
		parent := context.WithValue(context.Background(), cleanupContextKey{}, "cleanup-value")
		rc := cleanupRunContext(parent, store, true)
		guard := rc.ensureDurableState().guard
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		store.releaseFn = func(ctx context.Context, _ checkpoint.Claim) error {
			checkCleanupContext(t, ctx, deadline)
			select {
			case <-guard.renewDone:
			default:
				t.Error("claim released before renewal exited")
			}
			select {
			case <-ticker.stopped:
			default:
				t.Error("claim released before renewal ticker stopped")
			}
			return ctx.Err()
		}
		if err := guard.start(parent, func() { t.Error("intentional cleanup canceled execution") }); err != nil {
			t.Fatal(err)
		}
		ticker.tick()
		<-renewEntered
		deadline = time.Now().Add(writeGuardReleaseTimeout)
		done := make(chan error, 1)
		go func() { done <- rc.releaseDurableClaim() }()
		<-renewCanceled
		<-ticker.stopping
		synctest.Wait()
		if store.releaseCount() != 0 {
			t.Error("cleanup released claim while renewal was blocked")
		}
		// Advance virtual time while renewal is still stopping. Release must use
		// the original budget, including the time spent waiting for renewal.
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		<-timer.C
		close(ticker.finish)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := guard.stopAndRelease(parent); err != nil || store.releaseCount() != 1 {
			t.Fatalf("repeated stop: error=%v releases=%d", err, store.releaseCount())
		}
	})
}

func TestCleanupFailureIncludedInFrozenOutcome(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "unguarded", true: "guarded"}[guarded], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runErr := errors.New("execution failed")
				releaseErr := errors.New("release failed")
				var hookErr error
				r := outcomeFixture(runOutcome{Err: runErr}, &lifecycleFinisher{fn: func(_ *RunContext, _ core.Result, err error) { hookErr = err }})
				deadline := time.Now().Add(writeGuardReleaseTimeout)
				store := &guardTestCoordinator{releaseFn: func(ctx context.Context, _ checkpoint.Claim) error {
					checkCleanupContext(t, ctx, deadline)
					return releaseErr
				}}
				r.rc.Context = context.WithValue(r.rc.Context, cleanupContextKey{}, "cleanup-value")
				r.rc.Store = store
				claim := guardTestClaim(1)
				r.rc.setDurableClaim(&claim)
				if guarded {
					r.rc.ensureDurableState().guard = newWriteGuard(store, claim, 0, time.Minute)
				}
				_, waitErr := r.Wait()
				_, frozenErr := r.FrozenOutcome()
				for name, err := range map[string]error{"Wait": waitErr, "FrozenOutcome": frozenErr, "FinishRun": hookErr} {
					if !errors.Is(err, runErr) || !errors.Is(err, releaseErr) {
						t.Errorf("%s error = %v, want execution and cleanup failures", name, err)
					}
				}
			})
		})
	}
}

type cleanupResumeStore struct {
	checkpoint.ThreadCoordinator
	head      *checkpoint.Checkpoint
	decisions []core.ApprovalDecision
	decideErr error
	cancel    context.CancelFunc
	releaseFn func(context.Context, checkpoint.Claim) error
	releases  atomic.Int32
}

func (s *cleanupResumeStore) Latest(context.Context, string) (*checkpoint.Checkpoint, error) {
	// Return the malformed head directly: a real store would reject invalid
	// clone data before the construction-failure cleanup path is reached.
	return s.head, nil
}

func (s *cleanupResumeStore) Claim(_ context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	if s.cancel != nil {
		s.cancel()
	}
	return checkpoint.Claim{ThreadID: req.ThreadID, Owner: req.Owner, PauseID: req.PauseID, FencingToken: 1}, nil
}

func (s *cleanupResumeStore) Decisions(context.Context, string, string) ([]core.ApprovalDecision, error) {
	return s.decisions, s.decideErr
}

func (s *cleanupResumeStore) ToolExecution(context.Context, string, string) (checkpoint.ToolExecution, bool, error) {
	return checkpoint.ToolExecution{State: checkpoint.ToolCompleted}, true, nil
}

func (s *cleanupResumeStore) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	s.releases.Add(1)
	return s.releaseFn(ctx, claim)
}

func checkResumeCleanup(t *testing.T, a *Agent, store *cleanupResumeStore, original error, fragment string, canceled bool) {
	t.Helper()
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), cleanupContextKey{}, "cleanup-value"))
	defer cancel()
	if canceled {
		store.cancel = cancel
	}
	deadline := time.Now().Add(writeGuardReleaseTimeout)
	releasing, finishRelease := make(chan struct{}), make(chan struct{})
	releaseErr := errors.New("resume release failed")
	var releaseCtx context.Context
	store.releaseFn = func(ctx context.Context, claim checkpoint.Claim) error {
		releaseCtx = ctx
		checkCleanupContext(t, ctx, deadline)
		if err := ctx.Err(); err != nil {
			t.Errorf("cleanup inherited execution cancellation: %v", err)
		}
		if claim.ThreadID != store.head.ThreadID || claim.Owner == "" || claim.FencingToken != 1 {
			t.Errorf("released claim = %+v", claim)
		}
		close(releasing)
		<-finishRelease
		return releaseErr
	}
	type resumeResult struct {
		run *Run
		err error
	}
	done := make(chan resumeResult, 1)
	go func() {
		run, err := a.Resume(parent, store.head.ThreadID)
		done <- resumeResult{run: run, err: err}
	}()
	<-releasing
	synctest.Wait()
	if release, err := a.gate.acquire(context.Background(), store.head.ThreadID, AdmissionFail, defaultAdmissionWaiters); !errors.Is(err, checkpoint.ErrClaimConflict) {
		if release != nil {
			release()
		}
		t.Errorf("gate while claim release blocked = %v, want claim conflict", err)
	}
	select {
	case result := <-done:
		t.Errorf("Resume returned before release completed: %+v", result)
	default:
	}
	close(finishRelease)
	result := <-done
	if result.run != nil || !errors.Is(result.err, releaseErr) || (original != nil && !errors.Is(result.err, original)) || !strings.Contains(result.err.Error(), fragment) {
		t.Errorf("Resume = (%v, %v), want construction error %q joined with release error", result.run, result.err, fragment)
	}
	if releaseCtx.Err() != context.Canceled {
		t.Errorf("cleanup context after release = %v, want canceled", releaseCtx.Err())
	}
	if got := store.releases.Load(); got != 1 {
		t.Errorf("release attempts = %d, want 1", got)
	}
	release, err := a.gate.acquire(context.Background(), store.head.ThreadID, AdmissionFail, defaultAdmissionWaiters)
	if err != nil {
		t.Fatalf("gate after failed Resume: %v", err)
	}
	release()
}

func TestResumeConstructionFailureUsesBoundedCleanup(t *testing.T) {
	for _, failure := range []string{"decisions", "approvals", "clone state"} {
		for _, canceled := range []bool{false, true} {
			t.Run(failure+map[bool]string{false: "/live", true: "/canceled"}[canceled], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					store := &cleanupResumeStore{ThreadCoordinator: checkpoint.NewMemory(), head: &checkpoint.Checkpoint{ID: "head", ThreadID: "resume-cleanup", Revision: 1}}
					var original error
					var fragment string
					switch failure {
					case "decisions":
						original = errors.New("decisions unavailable")
						store.decideErr, fragment = original, original.Error()
					case "approvals":
						store.decisions = []core.ApprovalDecision{{DecisionID: "invalid"}}
						fragment = "durable approvals exist without pending tool calls"
					case "clone state":
						store.head.State.KV = map[string]any{"invalid": make(chan int)}
						original, fragment = core.ErrStateNotCloneable, ""
					}
					checkResumeCleanup(t, &Agent{store: store}, store, original, fragment, canceled)
				})
			})
		}
	}
}

func TestResumeInFlightConstructionFailureUsesBoundedCleanup(t *testing.T) {
	for _, failure := range []string{"clone state", "match message"} {
		for _, canceled := range []bool{false, true} {
			t.Run(failure+map[bool]string{false: "/live", true: "/canceled"}[canceled], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					rec := internalRecTool("rec")
					calls := []core.ToolCall{{ID: "c1", Name: rec.Name()}}
					store := &cleanupResumeStore{ThreadCoordinator: checkpoint.NewMemory(), head: &checkpoint.Checkpoint{
						ID: "inflight", ThreadID: "inflight-cleanup", Revision: 1,
						State:    core.State{Messages: []core.Message{internalPair(calls)}},
						InFlight: &checkpoint.InFlightBatch{Calls: calls}, Snapshot: internalInFlightSnapshot(0, rec),
					}}
					original := core.ErrStateNotCloneable
					if failure == "clone state" {
						store.head.State.KV = map[string]any{"invalid": make(chan int)}
					} else {
						store.head.State.Messages = nil
						original = ErrInFlightUnrecoverable
					}
					a := &Agent{store: store, loop: &AgentLoop{byName: map[string]tool.Tool{rec.Name(): rec}}}
					checkResumeCleanup(t, a, store, original, "", canceled)
				})
			})
		}
	}
}
