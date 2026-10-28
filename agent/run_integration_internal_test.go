package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestFrameworkPublishCancelledWhenFull(t *testing.T) {
	for _, name := range []string{"RunStarted", "TaskStarted", "RunContext.publish"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				shared := bus.New(bus.WithBufferSize(1))
				a, err := New(WithBus(shared), WithModel(mock.New("stub", nil)))
				if err != nil {
					t.Fatal(err)
				}
				run := a.newRunHandle(ctx, "publish-cancel", &core.State{})
				defer run.cancel()
				run.runnable = steeringStaticRunnable{}
				events, unsubscribe := run.Events(bus.Lossless)
				defer unsubscribe()

				var unblocked <-chan struct{}
				if name == "RunContext.publish" {
					shared.Publish(run.topic, core.RunStarted{})
					done := make(chan struct{})
					go func() {
						run.rc.publish(core.TaskDone{TaskID: "child"})
						close(done)
					}()
					unblocked = done
				} else {
					ticket, err := run.Steer(core.UserText("pending"))
					if err != nil {
						t.Fatal(err)
					}
					unblocked = ticket.Done()
					if name == "RunStarted" {
						shared.Publish(run.topic, core.TaskStarted{})
					}
					run.drive()
					defer func() {
						// Root terminal publication remains blocking until T05.
						unsubscribe()
						synctest.Wait()
						select {
						case <-run.done:
						default:
							t.Error("run did not settle after observer cleanup")
						}
					}()
				}

				// With no timers in the bubble, the publisher must now be
				// blocked on the full observer channel.
				synctest.Wait()
				select {
				case <-unblocked:
					t.Fatal("publication returned before cancellation with a full observer buffer")
				default:
				}
				if len(events) != cap(events) {
					t.Fatal("observer buffer is not full")
				}
				cancel()
				synctest.Wait()
				select {
				case <-unblocked:
				default:
					t.Fatal("ordinary publication stayed blocked after execution context cancellation")
				}
			})
		})
	}
}

type approvalReceiptObserverStore struct {
	checkpoint.Durable
	recorded chan checkpoint.DecisionReceipt
}

func (s *approvalReceiptObserverStore) RecordDecision(ctx context.Context, threadID string, decision core.ApprovalDecision) (checkpoint.DecisionReceipt, error) {
	receipt, err := s.Durable.RecordDecision(ctx, threadID, decision)
	if err == nil {
		s.recorded <- receipt
	}
	return receipt, err
}

func TestApprovalReceiptSurvivesObserverCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &approvalReceiptObserverStore{
			Durable:  checkpoint.NewMemory(),
			recorded: make(chan checkpoint.DecisionReceipt, 1),
		}
		cp := &checkpoint.Checkpoint{
			ID: "approval-pause", ThreadID: "approval-receipt",
			Pending: &checkpoint.PendingHITL{Pending: []core.ToolCall{{ID: "call-1", Name: "danger", Args: []byte(`{}`)}}},
		}
		if err := store.Save(context.Background(), cp); err != nil {
			t.Fatal(err)
		}
		shared := bus.New(bus.WithBufferSize(1))
		a, err := New(WithBus(shared), WithCheckpointer(store), WithModel(mock.New("stub", nil)))
		if err != nil {
			t.Fatal(err)
		}
		// Keep execution alive: approval notification must use its caller's context.
		run := a.newRunHandle(context.Background(), cp.ThreadID, &core.State{})
		defer run.cancel()
		events, unsubscribe := run.Events(bus.Lossless)
		defer unsubscribe()
		shared.Publish(run.topic, core.RunStarted{})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type outcome struct {
			receipt checkpoint.DecisionReceipt
			err     error
		}
		done := make(chan outcome, 1)
		go func() {
			receipt, err := run.RecordApproval(ctx, ApprovalDecision{
				DecisionID: "decision-1", CallID: "call-1", PolicyVersion: "policy-v1", Approved: true,
			})
			done <- outcome{receipt: receipt, err: err}
		}()
		synctest.Wait()
		var committed checkpoint.DecisionReceipt
		select {
		case committed = <-store.recorded:
		default:
			t.Fatal("approval was not durably recorded")
		}
		select {
		case got := <-done:
			t.Fatalf("approval returned before notification cancellation: %+v", got)
		default:
		}
		if len(events) != cap(events) {
			t.Fatal("observer buffer is not full")
		}
		cancel()
		synctest.Wait()
		select {
		case got := <-done:
			if got.err != nil || got.receipt != committed {
				t.Fatalf("committed approval = %+v, %v; want receipt %+v and success", got.receipt, got.err, committed)
			}
		default:
			t.Fatal("committed approval stayed blocked after notification context cancellation")
		}
		if err := run.rc.Context.Err(); err != nil {
			t.Fatalf("execution context was canceled: %v", err)
		}
		decisions, err := store.Decisions(context.Background(), cp.ThreadID, checkpoint.EffectivePauseID(cp))
		if err != nil || len(decisions) != 1 || decisions[0] != committed.Decision {
			t.Fatalf("query committed approval = %+v, %v; want %+v", decisions, err, committed.Decision)
		}
		retried, err := a.RecordApproval(context.Background(), cp.ThreadID, committed.Decision)
		if err != nil || !retried.Duplicate || retried.Decision != committed.Decision {
			t.Fatalf("retry receipt = %+v, %v; want duplicate committed decision", retried, err)
		}
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("approval caller error = %v, want context.Canceled", ctx.Err())
		}
	})
}

func TestAfterFuncAlreadyRunningDoesNotBlockSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := outcomeFixture(runOutcome{})
		callbackEntered := make(chan struct{})
		callbackDone := make(chan struct{})
		r.lifecycleMu.Lock()
		r.activated = true
		r.stopParent = context.AfterFunc(r.rc.Context, func() { close(callbackEntered); r.Cancel(); close(callbackDone) })
		r.lifecycleMu.Unlock()
		cleanupEntered := make(chan struct{})
		release := make(chan struct{})
		store := &guardTestCoordinator{ThreadCoordinator: checkpoint.NewMemory(), releaseFn: func(context.Context, checkpoint.Claim) error { close(cleanupEntered); <-release; return nil }}
		r.rc.Store = store
		r.agent.store = store
		claim := guardTestClaim(1)
		r.rc.setDurableClaim(&claim)
		returned := make(chan struct{})
		go func() { r.Cancel(); close(returned) }()
		<-cleanupEntered
		<-callbackEntered
		synctest.Wait()
		select {
		case <-callbackDone:
			t.Fatal("prepared callback returned before settlement")
		default:
		}
		close(release)
		<-returned
		<-callbackDone
		select {
		case <-r.done:
		default:
			t.Fatal("settlement did not publish done")
		}
	})
}

func TestLossyObserverReceivesReliableRunTail(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.AssistantText("done")}})
	r.bus = bus.New(bus.WithBufferSize(1))
	r.rc.Bus = r.bus
	ch, stop := r.Events(bus.Lossy)
	defer stop()
	r.Wait()
	events := []core.Event{}
	for ev := range ch {
		events = append(events, ev)
	}
	if len(events) != 3 {
		t.Fatalf("events=%d", len(events))
	}
	if _, ok := events[0].(core.RunStarted); !ok {
		t.Fatal("missing buffered progress")
	}
	if _, ok := events[1].(core.TaskDone); !ok {
		t.Fatal("missing root tail")
	}
	if _, ok := events[2].(core.RunDone); !ok {
		t.Fatal("missing success tail")
	}
}

func TestConcurrentTerminalReadersOwnPayloads(t *testing.T) {
	r := outcomeFixture(runOutcome{Result: core.Result{Message: core.Message{Parts: []core.Part{core.ToolResult{Content: []core.Part{core.Image{Data: []byte{1}}}}}}}})
	r.Wait()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			var result core.Result
			switch i % 5 {
			case 0:
				result, _ = r.Wait()
			case 1:
				result, _ = r.FrozenOutcome()
			case 2:
				result = r.DetailedResult().Result
			case 3:
				for ev := range r.Iter() {
					if done, ok := ev.(core.RunDone); ok {
						result = done.Result
					}
				}
			case 4:
				ch, stop := r.Events(bus.Lossless)
				defer stop()
				for ev := range ch {
					if done, ok := ev.(core.RunDone); ok {
						result = done.Result
					}
				}
			}
			data := result.Message.Parts[0].(core.ToolResult).Content[0].(core.Image).Data
			if data[0] != 1 {
				t.Errorf("reader received mutated payload=%d", data[0])
			}
			data[0] = byte(i + 2)
		})
	}
	wg.Wait()
	frozen, _ := r.FrozenOutcome()
	if frozen.Message.Parts[0].(core.ToolResult).Content[0].(core.Image).Data[0] != 1 {
		t.Fatal("readers changed frozen outcome")
	}
}

func TestHookAndWaitEditsDoNotChangeCompletionRecord(t *testing.T) {
	message := core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Image{Data: []byte{1}}}}
	hook := &lifecycleFinisher{fn: func(_ *RunContext, result core.Result, _ error) { result.Message.Parts[0].(core.Image).Data[0] = 9 }}
	r := outcomeFixture(runOutcome{}, hook)
	r.runnable = lifecycleRunnable(func(rc *RunContext) runOutcome {
		rc.State.Messages = []core.Message{message}
		err := rc.commitCheckpoint(&checkpoint.Checkpoint{ID: "hook-boundary", ThreadID: r.identity.threadID, State: *rc.State}, false)
		return runOutcome{Result: core.Result{Message: message}, Err: err}
	})
	result, err := r.Wait()
	if err != nil {
		t.Fatal(err)
	}
	result.Message.Parts[0].(core.Image).Data[0] = 8
	if err := r.agent.RecordCompletion(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	head, err := r.agent.store.Latest(context.Background(), r.identity.threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head.Completion == nil || head.Completion.Result == nil || head.Completion.Result.Message.Parts[0].(core.Image).Data[0] != 1 {
		t.Fatalf("completion=%+v", head.Completion)
	}
}
