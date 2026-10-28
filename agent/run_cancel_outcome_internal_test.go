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
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestCancelAtFinalTurnDoneFreezesCanceledOutcome(t *testing.T) {
	for _, source := range []string{"Run.Cancel", "parent context"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancelParent := context.WithCancel(context.Background())
				defer cancelParent()
				shared := bus.New(bus.WithBufferSize(1))
				memory := checkpoint.NewMemory()
				var hookCalls int
				var hookResult core.Result
				var hookErr error
				hook := &lifecycleFinisher{fn: func(_ *RunContext, res core.Result, err error) {
					hookCalls++
					hookResult, hookErr = res, err
				}}
				model := mock.New("final-answer", func(*llm.Request) *llm.Response { return mock.Text("done") })
				a, err := New(WithBus(shared), WithCheckpointer(memory), WithModel(model), WithMiddleware(hook))
				if err != nil {
					t.Fatal(err)
				}
				run := a.Stream(ctx, "hello")
				events, unsubscribe := run.Events(bus.Lossless)
				defer unsubscribe()
				run.Start()
				// Leave MessageDone in the single-slot buffer. With all other
				// goroutines durably blocked, the next publish is TurnDone,
				// after the real loop's final checkpoint has committed.
				for ev := range events {
					if _, ok := ev.(core.TurnStarted); ok {
						break
					}
				}
				synctest.Wait()
				_, _, committed := run.rc.committedBoundary()
				if !committed || len(events) != cap(events) {
					t.Fatal("loop did not reach final TurnDone backpressure after committing")
				}
				select {
				case <-run.done:
					t.Fatal("run settled before canceling final publication")
				default:
				}
				if source == "Run.Cancel" {
					run.Cancel()
				} else {
					cancelParent()
				}
				result, runErr := run.Await(context.Background())
				if !errors.Is(runErr, context.Canceled) || result.Message.Text() != "" {
					t.Fatalf("Await result=%+v err=%v, want canceled with zero result", result, runErr)
				}
				if hookCalls != 1 || !errors.Is(hookErr, context.Canceled) || hookResult.Message.Text() != "" {
					t.Fatalf("hook calls=%d result=%+v err=%v", hookCalls, hookResult, hookErr)
				}
				var taskDone, runFailed int
				for ev := range events {
					switch ev := ev.(type) {
					case core.MessageDone:
						// Confirms the buffer held the final model response,
						// rather than an earlier unrelated event.
						if ev.Message.Text() != "done" {
							t.Fatalf("message=%+v", ev.Message)
						}
					case core.TaskDone:
						taskDone++
						if ev.Status != "canceled" || !errors.Is(ev.Err, context.Canceled) || ev.Err.Error() != runErr.Error() {
							t.Fatalf("TaskDone=%+v runErr=%v", ev, runErr)
						}
					case core.RunFailed:
						runFailed++
						if !errors.Is(ev.Err, context.Canceled) || ev.Err.Error() != runErr.Error() {
							t.Fatalf("RunFailed=%+v", ev)
						}
					case core.RunDone, core.Interrupted:
						t.Fatalf("canceled run emitted %T", ev)
					}
				}
				if taskDone != 1 || runFailed != 1 {
					t.Fatalf("TaskDone=%d RunFailed=%d", taskDone, runFailed)
				}
				if err := a.RecordCompletion(context.Background(), run); err != nil {
					t.Fatal(err)
				}
				head, err := memory.Latest(context.Background(), run.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				if head.Completion == nil || head.Completion.Kind != checkpoint.CompletionFailed || head.Completion.Err != runErr.Error() || head.Completion.Result != nil {
					t.Fatalf("completion=%+v runErr=%v", head.Completion, runErr)
				}
			})
		})
	}
}

func TestCancelRequestPrecedesContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := outcomeFixture(runOutcome{})
		runnableEntered, releaseRunnable := make(chan struct{}), make(chan struct{})
		run.runnable = lifecycleRunnable(func(*RunContext) runOutcome {
			close(runnableEntered)
			<-releaseRunnable
			return runOutcome{Result: core.Result{Message: core.AssistantText("done")}}
		})
		cancelEntered, releaseCancel := make(chan struct{}), make(chan struct{})
		originalCancel := run.cancel
		var calls atomic.Int32
		run.cancel = func() {
			if calls.Add(1) == 1 {
				close(cancelEntered)
				<-releaseCancel
			}
			originalCancel()
		}
		run.Start()
		<-runnableEntered
		cancelDone := make(chan struct{})
		go func() { run.Cancel(); close(cancelDone) }()
		<-cancelEntered
		if err := run.rc.Context.Err(); err != nil {
			t.Fatalf("context canceled before request barrier: %v", err)
		}
		close(releaseRunnable)
		_, err := run.Await(context.Background())
		close(releaseCancel)
		<-cancelDone
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("registered Cancel lost to settlement: %v", err)
		}
	})
}

func TestCancelBeforeSettlementPreservesRunnableError(t *testing.T) {
	original := errors.New("runnable failed")
	run := outcomeFixture(runOutcome{})
	run.runnable = lifecycleRunnable(func(*RunContext) runOutcome {
		run.Cancel()
		return runOutcome{Err: original}
	})
	_, err := run.Wait()
	if !errors.Is(err, original) || !errors.Is(err, context.Canceled) {
		t.Fatalf("outcome lost an error: %v", err)
	}
}

func TestCancelDuringSettlementDoesNotRewriteOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var hookErr error
		hook := &lifecycleFinisher{fn: func(_ *RunContext, _ core.Result, err error) { hookErr = err }}
		run := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}}, hook)
		cleanupEntered, releaseCleanup := make(chan struct{}), make(chan struct{})
		run.setGateRelease(func() { close(cleanupEntered); <-releaseCleanup })
		run.Start()
		<-cleanupEntered
		run.Cancel()
		close(releaseCleanup)
		result, err := run.Await(context.Background())
		if err != nil || hookErr != nil || result.Message.Text() != "done" {
			t.Fatalf("Settling cancellation rewrote outcome: result=%+v err=%v hook=%v", result, err, hookErr)
		}
		events, unsubscribe := run.Events(bus.Lossless)
		defer unsubscribe()
		var completed int
		for ev := range events {
			switch ev := ev.(type) {
			case core.TaskDone:
				if ev.Err != nil || ev.Status != "done" {
					t.Fatalf("TaskDone=%+v", ev)
				}
			case core.RunDone:
				completed++
			case core.RunFailed:
				t.Fatalf("unexpected failure: %v", ev.Err)
			}
		}
		if completed != 1 {
			t.Fatalf("RunDone=%d", completed)
		}
	})
}
