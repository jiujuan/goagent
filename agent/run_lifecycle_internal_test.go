package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func TestPreparedCancelSettlesWithoutStarting(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	var calls atomic.Int32
	r.runnable = lifecycleRunnable(func(*RunContext) runOutcome { calls.Add(1); return runOutcome{} })
	r.Cancel()
	select {
	case <-r.done:
	default:
		t.Fatal("prepared cancellation did not settle")
	}
	r.Start()
	_, err := r.Await(context.Background())
	if !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		t.Fatalf("calls=%d error=%v", calls.Load(), err)
	}
}
func TestConcurrentPreparedCancelHasSingleOwner(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	var releases atomic.Int32
	r.setGateRelease(func() { releases.Add(1) })
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(r.Cancel)
	}
	wg.Wait()
	select {
	case <-r.done:
	default:
		t.Fatal("not settled")
	}
	if releases.Load() != 1 {
		t.Fatalf("gate releases=%d", releases.Load())
	}
}
func TestStartCancelRaceHasOneOwner(t *testing.T) {
	for range 100 {
		r := outcomeFixture(runOutcome{})
		var calls, releases atomic.Int32
		r.runnable = lifecycleRunnable(func(*RunContext) runOutcome { calls.Add(1); return runOutcome{} })
		r.setGateRelease(func() { releases.Add(1) })
		var wg sync.WaitGroup
		wg.Go(r.Start)
		wg.Go(r.Cancel)
		wg.Wait()
		r.Await(context.Background())
		if calls.Load() > 1 || releases.Load() != 1 {
			t.Fatalf("executions=%d releases=%d", calls.Load(), releases.Load())
		}
	}
}
func TestStartedCancelAwaitsExternalExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		entered := make(chan struct{})
		release := make(chan struct{})
		r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome { close(entered); <-release; return runOutcome{Err: rc.Context.Err()} })
		r.Start()
		<-entered
		r.Cancel()
		synctest.Wait()
		select {
		case <-r.done:
			t.Fatal("cancel pretended external runnable exited")
		default:
		}
		close(release)
		if _, err := r.Await(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
func TestCancelClosesSteeringOnce(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	ticket, err := r.Steer(core.UserText("pending"))
	if err != nil {
		t.Fatal(err)
	}
	r.Cancel()
	r.Cancel()
	<-ticket.Done()
	if _, err := r.Steer(core.UserText("later")); err == nil {
		t.Fatal("canceled steering still accepts input")
	}
}
func TestUnstartedCancelHasNoTaskOrStartedEvent(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	ch, stop := r.Events(bus.Lossless)
	defer stop()
	r.Cancel()
	n := 0
	for ev := range ch {
		n++
		if _, ok := ev.(core.RunFailed); !ok {
			t.Fatalf("unstarted event=%T", ev)
		}
	}
	if n != 1 {
		t.Fatalf("events=%d", n)
	}
}

func TestPreparedCancelReleasesClaimBeforeGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		releasing := make(chan struct{})
		release := make(chan struct{})
		var released atomic.Bool
		var gates atomic.Int32
		store := &guardTestCoordinator{ThreadCoordinator: checkpoint.NewMemory(), releaseFn: func(context.Context, checkpoint.Claim) error {
			close(releasing)
			<-release
			released.Store(true)
			return nil
		}}
		r.agent.store = store
		r.rc.Store = store
		claim := guardTestClaim(1)
		r.rc.setDurableClaim(&claim)
		r.setGateRelease(func() {
			if !released.Load() {
				t.Error("gate released before claim")
			}
			gates.Add(1)
		})
		first := make(chan struct{})
		go func() { r.Cancel(); close(first) }()
		<-releasing
		second := make(chan struct{})
		go func() { r.Cancel(); close(second) }()
		synctest.Wait()
		select {
		case <-second:
			t.Error("concurrent prepared cancel returned before settlement")
		default:
		}
		if gates.Load() != 0 {
			t.Fatal("early gate release")
		}
		close(release)
		<-first
		<-second
		if store.releaseCount() != 1 || gates.Load() != 1 {
			t.Fatalf("claim=%d gate=%d", store.releaseCount(), gates.Load())
		}
	})
}
