package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestExpiredContextBeforeFirstConsumptionSettles(t *testing.T) {
	for _, managed := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			a, err := New(WithModel(mock.New("never", nil)))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var r *Run
			if managed {
				r, err = a.StreamManaged(ctx, ManagedRunSpec{RunID: "managed", ThreadID: "thread"}, "hi")
				if err != nil {
					t.Fatal(err)
				}
			} else {
				r = a.Stream(ctx, "hi")
			}
			synctest.Wait()
			select {
			case <-r.done:
			default:
				t.Fatal("expired prepared factory run did not settle")
			}
			if _, err := r.Await(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
func TestActivationAfterResumeResourcesAttached(t *testing.T) {
	for _, branch := range []string{"no-pending", "workflow-pending", "llm-pending"} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			head := &checkpoint.Checkpoint{ID: "pause", ThreadID: "resume", State: core.State{}}
			if branch != "no-pending" {
				calls := []core.ToolCall{{ID: "c", Name: "rec", Args: []byte(`{}`)}}
				head.Pending = &checkpoint.PendingHITL{Pending: calls}
				head.State.Messages = []core.Message{internalPair(calls)}
			}
			store := &cleanupResumeStore{ThreadCoordinator: checkpoint.NewMemory(), head: head, cancel: cancel}
			var a *Agent
			store.releaseFn = func(context.Context, checkpoint.Claim) error {
				release, err := a.gate.acquire(context.Background(), "resume", AdmissionFail, 1)
				if err == nil {
					release()
					t.Error("claim released after gate was already handed over")
				}
				return nil
			}
			if branch == "llm-pending" {
				var err error
				a, err = New(WithModel(mock.New("never", nil)), WithTools(internalRecTool("rec")), WithCheckpointer(store))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				a = &Agent{bus: outcomeFixture(runOutcome{}).bus, store: store}
			}
			r, err := a.Resume(ctx, "resume")
			if err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			select {
			case <-r.done:
			default:
				t.Fatal("cancel during Resume construction was not activated")
			}
			if store.releases.Load() != 1 {
				t.Fatalf("releases=%d", store.releases.Load())
			}
			release, err := a.gate.acquire(context.Background(), "resume", AdmissionFail, 1)
			if err != nil {
				t.Fatal(err)
			}
			release()
		})
	}
}
func TestAfterFuncRegistrationHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		rcCtx, rcCancel := context.WithCancel(parent)
		r.rc.Context = rcCtx
		r.cancel = rcCancel
		store := &guardTestCoordinator{ThreadCoordinator: checkpoint.NewMemory(), releaseFn: func(context.Context, checkpoint.Claim) error {
			r.lifecycleMu.Lock()
			defer r.lifecycleMu.Unlock()
			if !r.activated || r.stopParent == nil {
				t.Error("callback ran before activation published stop")
			}
			return nil
		}}
		r.agent.store = store
		r.rc.Store = store
		claim := guardTestClaim(1)
		r.rc.setDurableClaim(&claim)
		r.activateLifecycle()
		synctest.Wait()
		select {
		case <-r.done:
		default:
			t.Fatal("activation callback did not complete")
		}
		r.activateLifecycle()
		r.Cancel()
		if store.releaseCount() != 1 {
			t.Fatal("repeated activation repeated cleanup")
		}
	})
}
func TestConcurrentContextCancelAndStart(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		a := outcomeFixture(runOutcome{}).agent
		r := a.newRunHandle(ctx, "race", &core.State{})
		r.activateLifecycle()
		var wg sync.WaitGroup
		wg.Go(r.Start)
		wg.Go(cancel)
		wg.Wait()
		r.Await(context.Background())
		r.Cancel()
	}
}
func TestCompletedRunIgnoresLaterParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}}).agent
	r := a.newRunHandle(ctx, "completed", &core.State{})
	r.activateLifecycle()
	r.Wait()
	cancel()
	result, err := r.Await(context.Background())
	if err != nil || result.Message.Text() != "done" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopParent != nil {
		t.Fatal("settled run retains AfterFunc stop")
	}
}
func TestResumeInFlightActivatesAfterConstruction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		memory := checkpoint.NewMemory()
		rec := internalRecTool("rec")
		head := seedInternalInFlight(t, memory, "inflight", 0, []core.ToolCall{{ID: "c", Name: "rec", Args: []byte(`{}`)}}, rec)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &cleanupResumeStore{ThreadCoordinator: memory, head: head, cancel: cancel, releaseFn: func(context.Context, checkpoint.Claim) error { return nil }}
		a, err := New(WithModel(mock.New("never", nil)), WithTools(rec), WithCheckpointer(store))
		if err != nil {
			t.Fatal(err)
		}
		r, err := a.Resume(ctx, "inflight")
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-r.done:
		default:
			t.Fatal("inflight factory did not activate cancellation")
		}
		if r.rc.inFlight == nil || r.preparation == nil || store.releases.Load() != 1 {
			t.Fatal("inflight resources not handed over before cleanup")
		}
	})
}
