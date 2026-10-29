package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/core"
)

func TestStartIsIdempotentAndNonBlocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		release := make(chan struct{})
		var calls atomic.Int32
		r.runnable = lifecycleRunnable(func(*RunContext) runOutcome { calls.Add(1); <-release; return runOutcome{} })
		for range 100 {
			r.Start()
		}
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("calls=%d", calls.Load())
		}
		close(release)
		if _, err := r.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
func TestAwaitDoesNotStartPreparedRun(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	var calls atomic.Int32
	r.runnable = lifecycleRunnable(func(*RunContext) runOutcome { calls.Add(1); return runOutcome{} })
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if _, err := r.Await(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls.Load() != 0 || r.bus.Subscribers(r.topic) != 0 {
		t.Fatal("Await started or subscribed")
	}
	r.cancel()
}
func TestAwaitContextDoesNotCancelExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
		entered := make(chan struct{})
		release := make(chan struct{})
		original := r.runnable
		r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome { close(entered); <-release; return original.run(rc) })
		r.Start()
		<-entered
		ctx, stop := context.WithCancel(context.Background())
		stop()
		if _, err := r.Await(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := r.rc.Context.Err(); err != nil {
			t.Fatalf("wait canceled execution: %v", err)
		}
		close(release)
		res, err := r.Await(context.Background())
		if err != nil || res.Message.Text() != "done" {
			t.Fatalf("result=%v error=%v", res, err)
		}
	})
}
func TestAwaitPrefersSettledResultOnContextRace(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
	r.Wait()
	ctx, stop := context.WithCancel(context.Background())
	stop()
	for range 100 {
		res, err := r.Await(ctx)
		if err != nil || res.Message.Text() != "done" {
			t.Fatalf("result=%v error=%v", res, err)
		}
	}
}
func TestManyAwaitersDoNotSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		entered := make(chan struct{})
		release := make(chan struct{})
		r.runnable = lifecycleRunnable(func(*RunContext) runOutcome { close(entered); <-release; return runOutcome{} })
		r.Start()
		<-entered
		var wg sync.WaitGroup
		for i := range 100 {
			wg.Go(func() {
				if i%2 == 0 {
					r.Await(context.Background())
				} else {
					r.Wait()
				}
			})
		}
		synctest.Wait()
		n := r.bus.Subscribers(r.topic)
		close(release)
		wg.Wait()
		if n != 0 {
			t.Fatalf("waiters created %d observers", n)
		}
	})
}
func TestWaitStartsThenAwaits(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
	res, err := r.Wait()
	if err != nil || res.Message.Text() != "done" {
		t.Fatalf("result=%v error=%v", res, err)
	}
}
func TestAwaitResultsAreIndependent(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.Message{Parts: []core.Part{core.Image{Data: []byte{1}}}}}})
	r.Start()
	a, err := r.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := r.Await(context.Background())
	a.Message.Parts[0].(core.Image).Data[0] = 9
	frozen, _ := r.FrozenOutcome()
	if b.Message.Parts[0].(core.Image).Data[0] != 1 || frozen.Message.Parts[0].(core.Image).Data[0] != 1 {
		t.Fatal("await results alias")
	}
}
func TestIterSubscribesBeforeStart(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	r.runnable = lifecycleRunnable(func(*RunContext) runOutcome {
		if n := r.bus.Subscribers(r.topic); n != 1 {
			t.Errorf("observers at execution=%d", n)
		}
		return runOutcome{}
	})
	for _, err := range r.Iter() {
		if err != nil {
			t.Fatal(err)
		}
	}
}
