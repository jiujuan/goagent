package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

func TestIterAfterSuccessfulWaitReturnsTerminalTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
		var calls atomic.Int32
		original := r.runnable
		r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome { calls.Add(1); return original.run(rc) })
		if _, err := r.Wait(); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			count := 0
			for ev, err := range r.Iter() {
				if err != nil {
					t.Fatal(err)
				}
				count++
				switch ev.(type) {
				case core.TaskDone, core.RunDone:
				default:
					t.Fatalf("late event %T", ev)
				}
			}
			if count != 2 {
				t.Fatalf("late tail count=%d", count)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("executions=%d", calls.Load())
		}
	})
}
func TestEventsAfterFailureAreFinite(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	r.startErr = errors.New("invalid")
	r.Wait()
	ch, stop := r.Events(bus.Lossless)
	defer stop()
	ev, ok := <-ch
	if !ok {
		t.Fatal("missing failure")
	}
	if _, ok := ev.(core.RunFailed); !ok {
		t.Fatalf("event=%T", ev)
	}
	if _, ok := <-ch; ok {
		t.Fatal("late channel open")
	}
}
func TestIterAfterPauseDoesNotRestart(t *testing.T) {
	r := outcomeFixture(runOutcome{Control: core.Directive{Kind: core.Interrupt}, Pending: []core.ApprovalRequest{{CallID: "call", Args: []byte{1}}}})
	r.Wait()
	n := 0
	for ev, err := range r.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ev.(core.Interrupted); ok {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("interrupts=%d", n)
	}
}
func TestSlowTerminalObserverDoesNotBlockWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
		r.bus = bus.New(bus.WithBufferSize(2))
		r.rc.Bus = r.bus
		ch, stop := r.Events(bus.Lossless)
		defer stop()
		returned := make(chan struct{})
		go func() { r.Wait(); close(returned) }()
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Fatal("slow terminal observer blocked settlement")
		}
		events := []core.Event{}
		for ev := range ch {
			events = append(events, ev)
		}
		if len(events) != 4 {
			t.Fatalf("ordered events=%d", len(events))
		}
		if _, ok := events[2].(core.TaskDone); !ok {
			t.Fatal("missing root task tail")
		}
		if _, ok := events[3].(core.RunDone); !ok {
			t.Fatal("missing run tail")
		}
	})
}
func TestSubscribeAndSealRace(t *testing.T) {
	for range 30 {
		r := outcomeFixture(runOutcome{})
		var wg sync.WaitGroup
		wg.Go(func() { r.sealEvents([]core.Event{core.TaskDone{}, core.RunDone{}}) })
		for range 10 {
			wg.Go(func() {
				ch, stop := r.Events(bus.Lossless)
				defer stop()
				n := 0
				for range ch {
					n++
				}
				if n != 2 {
					t.Errorf("tail=%d", n)
				}
			})
		}
		wg.Wait()
		if r.bus.Subscribers(r.topic) != 0 {
			t.Fatal("sealed subscription retained")
		}
		r.cancel()
	}
}
func TestEarlyBreakUnsubscribesTailSender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		for range r.Iter() {
			break
		}
		r.Wait()
		if r.bus.Subscribers(r.topic) != 0 {
			t.Fatal("iterator subscription leaked")
		}
	})
}
func TestTerminalCopiesAreIndependent(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.Message{Parts: []core.Part{core.Image{Data: []byte{1}}}}}})
	r.Wait()
	a, stopA := r.Events(bus.Lossless)
	defer stopA()
	b, stopB := r.Events(bus.Lossless)
	defer stopB()
	<-a
	<-b
	x := (<-a).(core.RunDone)
	y := (<-b).(core.RunDone)
	x.Result.Message.Parts[0].(core.Image).Data[0] = 9
	frozen, _ := r.FrozenOutcome()
	if y.Result.Message.Parts[0].(core.Image).Data[0] != 1 || frozen.Message.Parts[0].(core.Image).Data[0] != 1 {
		t.Fatal("terminal payload aliases")
	}
}
func TestLateSubscriberGetsNoProgressReplay(t *testing.T) {
	r := outcomeFixture(runOutcome{})
	first := make(chan struct{})
	proceed := make(chan struct{})
	r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome {
		rc.publish(core.TurnStarted{Step: 1})
		close(first)
		<-proceed
		rc.publish(core.TurnStarted{Step: 2})
		return runOutcome{}
	})
	done := make(chan struct{})
	go func() { r.Wait(); close(done) }()
	<-first
	ch, stop := r.Events(bus.Lossless)
	defer stop()
	close(proceed)
	<-done
	n := 0
	for ev := range ch {
		if step, ok := ev.(core.TurnStarted); ok {
			if step.Step != 2 {
				t.Fatal("replayed progress")
			}
			n++
		}
	}
	if n != 1 {
		t.Fatalf("subsequent progress=%d", n)
	}
}
func TestWorkflowChildTaskDoneRemainsCancelable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		r.bus = bus.New(bus.WithBufferSize(2))
		r.rc.Bus = r.bus
		_, stop := r.Events(bus.Lossless)
		defer stop()
		entered := make(chan struct{})
		exited := make(chan struct{})
		r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome {
			close(entered)
			rc.publish(core.TaskDone{TaskID: "child"})
			close(exited)
			return runOutcome{Err: rc.Context.Err()}
		})
		done := make(chan struct{})
		go func() { r.Wait(); close(done) }()
		<-entered
		synctest.Wait()
		select {
		case <-exited:
			t.Fatal("child TaskDone lost ordinary back-pressure")
		default:
		}
		r.Cancel()
		<-exited
		<-done
		if _, err := r.FrozenOutcome(); !errors.Is(err, context.Canceled) {
			t.Fatalf("outcome=%v", err)
		}
	})
}
