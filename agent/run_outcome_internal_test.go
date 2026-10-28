package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

type lifecycleRunnable func(*RunContext) runOutcome

func (f lifecycleRunnable) run(rc *RunContext) runOutcome { return f(rc) }

type lifecycleFinisher struct {
	BaseMiddleware
	fn func(*RunContext, core.Result, error)
}

func (f *lifecycleFinisher) FinishRun(rc *RunContext, res core.Result, err error) { f.fn(rc, res, err) }

// Raw fixtures deliberately remain unactivated: they control execution directly.
func outcomeFixture(out runOutcome, hooks ...Middleware) *Run {
	a := &Agent{bus: bus.New(), store: checkpoint.NewMemory(), cfg: config{middleware: hooks}}
	a.runnable = lifecycleRunnable(func(*RunContext) runOutcome { return out })
	return a.newRunHandle(context.Background(), "outcome-thread", &core.State{})
}

func TestFreezeFailurePrecedesFinishRun(t *testing.T) {
	var hookErr error
	calls := 0
	hook := &lifecycleFinisher{fn: func(_ *RunContext, res core.Result, err error) {
		calls++
		hookErr = err
		if len(res.Message.Parts) != 0 {
			t.Error("failure hook received uncloneable result")
		}
	}}
	// Pointer parts satisfy core.Part but are rejected by CloneMessage.
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.Message{Parts: []core.Part{&core.Text{Text: "invalid"}}}}}, hook)
	events, stop := r.Events(bus.Lossless)
	defer stop()
	_, err := r.Wait()
	if !errors.Is(err, core.ErrStateNotCloneable) {
		t.Fatalf("Wait error = %v", err)
	}
	if calls != 1 || !errors.Is(hookErr, core.ErrStateNotCloneable) {
		t.Fatalf("hook calls=%d error=%v", calls, hookErr)
	}
	frozen, frozenErr := r.FrozenOutcome()
	if len(frozen.Message.Parts) != 0 || !errors.Is(frozenErr, core.ErrStateNotCloneable) {
		t.Fatalf("frozen = %#v, %v", frozen, frozenErr)
	}
	for ev := range events {
		if failed, ok := ev.(core.RunFailed); ok {
			if !errors.Is(failed.Err, core.ErrStateNotCloneable) {
				t.Fatal(failed.Err)
			}
			return
		}
		if _, ok := ev.(core.RunDone); ok {
			t.Fatal("freeze failure reported success")
		}
	}
	t.Fatal("missing failure event")
}

func TestFinishRunMutationDoesNotChangeOutcome(t *testing.T) {
	message := core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Image{Data: []byte{1}}}}
	first := &lifecycleFinisher{fn: func(_ *RunContext, res core.Result, _ error) { res.Message.Parts[0].(core.Image).Data[0] = 9 }}
	second := &lifecycleFinisher{fn: func(_ *RunContext, res core.Result, _ error) {
		if res.Message.Parts[0].(core.Image).Data[0] != 1 {
			t.Error("hooks share mutable result")
		}
	}}
	r := outcomeFixture(runOutcome{Result: core.Result{Message: message}}, first, second)
	ch, stop := r.Events(bus.Lossless)
	defer stop()
	result, err := r.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if result.Message.Parts[0].(core.Image).Data[0] != 1 {
		t.Fatal("hook changed Wait result")
	}
	frozen, _ := r.FrozenOutcome()
	if frozen.Message.Parts[0].(core.Image).Data[0] != 1 || r.DetailedResult().Result.Message.Parts[0].(core.Image).Data[0] != 1 {
		t.Fatal("hook changed frozen result")
	}
	for ev := range ch {
		if done, ok := ev.(core.RunDone); ok {
			if done.Result.Message.Parts[0].(core.Image).Data[0] != 1 {
				t.Fatal("hook changed event")
			}
			return
		}
	}
	t.Fatal("missing terminal event")
}

func TestPrepareFailurePreservesHookEligibility(t *testing.T) {
	called := false
	r := outcomeFixture(runOutcome{}, &lifecycleFinisher{fn: func(*RunContext, core.Result, error) { called = true }})
	r.startErr = errors.New("invalid configuration")
	if _, err := r.Wait(); err == nil {
		t.Fatal("expected preparation error")
	}
	if called {
		t.Fatal("preparation error invoked FinishRun")
	}
}

func TestPausedRunSkipsFinishRun(t *testing.T) {
	called := false
	r := outcomeFixture(runOutcome{Control: core.Directive{Kind: core.Interrupt}}, &lifecycleFinisher{fn: func(*RunContext, core.Result, error) { called = true }})
	if _, err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	if !r.Paused() || called {
		t.Fatalf("paused=%v hook=%v", r.Paused(), called)
	}
}

type preparationBarrierStore struct {
	checkpoint.ThreadCoordinator
	calls     atomic.Int32
	releasing chan struct{}
	release   chan struct{}
}

func (s *preparationBarrierStore) BeginRun(ctx context.Context, req checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	if s.calls.Add(1) == 1 {
		return checkpoint.BeginRunResult{Claim: checkpoint.Claim{Owner: req.Owner}, Checkpoint: &checkpoint.Checkpoint{State: core.State{KV: map[string]any{"invalid": make(chan int)}}}}, nil
	}
	return checkpoint.BeginRunResult{}, errors.New("second preparation")
}
func (s *preparationBarrierStore) ReleaseClaim(context.Context, checkpoint.Claim) error {
	close(s.releasing)
	<-s.release
	return nil
}
func TestPrepareFailureReleasesClaimBeforeAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &preparationBarrierStore{ThreadCoordinator: checkpoint.NewMemory(), releasing: make(chan struct{}), release: make(chan struct{})}
		a := &Agent{bus: bus.New(), store: store}
		first := a.newRunHandle(context.Background(), "same", &core.State{})
		first.preparation = &runPreparation{restoreCurrent: true}
		second := a.newRunHandle(context.Background(), "same", &core.State{})
		second.preparation = &runPreparation{restoreCurrent: true}
		second.admission = AdmissionOptions{Policy: AdmissionQueue, MaxWaiters: 1}
		firstDone := make(chan struct{})
		go func() { first.Wait(); close(firstDone) }()
		<-store.releasing
		secondDone := make(chan struct{})
		go func() { second.Wait(); close(secondDone) }()
		synctest.Wait()
		if store.calls.Load() != 1 {
			t.Errorf("next run entered BeginRun before cleanup: calls=%d", store.calls.Load())
		}
		close(store.release)
		<-firstDone
		<-secondDone
		if store.calls.Load() != 2 {
			t.Fatal("gate never handed over")
		}
	})
}

func TestCleanupFailureTurnsPauseTaskIntoFailure(t *testing.T) {
	releaseErr := errors.New("claim cleanup failed")
	r := outcomeFixture(runOutcome{Control: core.Directive{Kind: core.Interrupt}})
	store := &guardTestCoordinator{ThreadCoordinator: checkpoint.NewMemory(), releaseFn: func(context.Context, checkpoint.Claim) error { return releaseErr }}
	r.rc.Store = store
	r.agent.store = store
	claim := guardTestClaim(1)
	r.rc.setDurableClaim(&claim)
	ch, stop := r.Events(bus.Lossless)
	defer stop()
	if _, err := r.Wait(); !errors.Is(err, releaseErr) {
		t.Fatal(err)
	}
	if r.Paused() {
		t.Fatal("cleanup failure retained pause outcome")
	}
	tasks, failures := 0, 0
	for ev := range ch {
		switch value := ev.(type) {
		case core.TaskDone:
			tasks++
			if value.Status != "failed" || !errors.Is(value.Err, releaseErr) {
				t.Fatalf("task=%+v", value)
			}
		case core.RunFailed:
			failures++
			if !errors.Is(value.Err, releaseErr) {
				t.Fatal(value.Err)
			}
		case core.Interrupted:
			t.Fatal("failed pause emitted Interrupted")
		}
	}
	if tasks != 1 || failures != 1 {
		t.Fatalf("tasks=%d failures=%d", tasks, failures)
	}
}
