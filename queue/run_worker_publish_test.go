package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/runstore"
)

type terminalContextStore struct {
	runstore.Store
	t     *testing.T
	block bool
}

func (s *terminalContextStore) Get(ctx context.Context, id string) (runstore.RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return runstore.RunRecord{}, err
	}
	return s.Store.Get(ctx, id)
}
func (s *terminalContextStore) CompareAndSwap(ctx context.Context, id string, v uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	if next.Status.Terminal() {
		if err := ctx.Err(); err != nil {
			return runstore.RunRecord{}, err
		}
		if ctx.Value(managedCtxKey{}) != "trusted" {
			s.t.Error("record context lost values")
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			s.t.Error("record context has no deadline")
			return runstore.RunRecord{}, errors.New("unbounded record context")
		}
		if left := time.Until(deadline); left <= 0 || left > time.Second {
			s.t.Errorf("unexpected record timeout: %v", left)
		}
		if s.block {
			<-ctx.Done()
			return runstore.RunRecord{}, ctx.Err()
		}
	}
	return s.Store.CompareAndSwap(ctx, id, v, next)
}
func TestManagedRunPublishContext(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled execution", true: "bounded write"}[block], func(t *testing.T) {
			store := &terminalContextStore{Store: runstore.NewMemory(), t: t, block: block}
			w := managedWorker(t, managedAgent(t, checkpoint.NewMemory(), "done", nil), store, "worker", WithManagedRecordTimeout(50*time.Millisecond))
			r := managedRecord("publish-context", "thread", "turn")
			r.Status = runstore.StatusRunning
			r.WorkerID = "worker"
			r.WorkerEpoch = 1
			claimed := managedCreate(t, store, r)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), managedCtxKey{}, "trusted"))
			cancel()
			out, err := w.publish(ctx, claimed, agent.RunResult{RunID: r.RunID, ThreadID: r.ThreadID}, nil, nil, nil)
			if block {
				if !errors.Is(err, context.DeadlineExceeded) || out.RecordError == nil {
					t.Fatalf("bounded write: %v / %+v", err, out)
				}
			} else if err != nil || out.Record.Status != runstore.StatusSucceeded {
				t.Fatalf("cancelled execution blocked publication: %v / %+v", err, out)
			}
		})
	}
}
func TestManagedRunClearsDispatchError(t *testing.T) {
	store := runstore.NewMemory()
	w := managedWorker(t, managedAgent(t, checkpoint.NewMemory(), "done", nil), store, "worker")
	r := managedRecord("dispatch-error", "thread", "turn")
	r.Error = &runstore.RunError{Code: runstore.ErrCodeDispatchUnknown}
	managedCreate(t, store, r)
	out, err := w.Execute(context.Background(), r.RunID, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if out.Record.Error != nil {
		t.Fatalf("success retains dispatch error: %+v", out.Record.Error)
	}
	paused := terminalTaskRecord(r, agent.RunResult{Paused: true}, nil, nil, nil)
	if paused.Error != nil {
		t.Fatalf("pause retains dispatch error: %+v", paused.Error)
	}
}

// TestQueueWorkerDoesNotMarkInFlightAsPaused：在途不是暂停。真实的在途头只有
// 进程在批次中途被杀才会留下，队列夹具不制造进程崩溃，因此这里直接验证映射：
// Paused=false 的运行无论成功、失败还是取消都不会写成 paused；真正的暂停仍走
// paused 分支，证明这不是恒真断言。RunResult 没有在途字段，队列不拿
// RunStatus.InFlight 推断暂停。
func TestQueueWorkerDoesNotMarkInFlightAsPaused(t *testing.T) {
	stored := managedRecord("inflight-mapping", "thread-inflight-mapping", "turn-1")
	stored.Status = runstore.StatusRunning
	for _, tc := range []struct {
		name   string
		runErr error
		want   runstore.Status
	}{
		{"succeeded", nil, runstore.StatusSucceeded},
		{"failed", errors.New("tool failed"), runstore.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := terminalTaskRecord(stored, agent.RunResult{RunID: stored.RunID, Paused: false}, tc.runErr, nil, nil)
			if got.Status != tc.want {
				t.Fatalf("status = %s, want %s", got.Status, tc.want)
			}
			if got.Status == runstore.StatusPaused || got.FinishReason == runstore.FinishReasonPaused {
				t.Fatalf("非暂停运行被记成暂停: %+v", got)
			}
		})
	}

	cancelling := stored
	cancelling.Status = runstore.StatusCancelling
	cancelled := terminalTaskRecord(cancelling, agent.RunResult{Paused: false}, context.Canceled, nil, nil)
	if cancelled.Status != runstore.StatusCancelled || cancelled.FinishReason == runstore.FinishReasonPaused {
		t.Fatalf("取消的运行被记错: %+v", cancelled)
	}

	paused := terminalTaskRecord(stored, agent.RunResult{Paused: true}, nil, nil, nil)
	if paused.Status != runstore.StatusPaused || paused.FinishReason != runstore.FinishReasonPaused {
		t.Fatalf("对照用例：真正的暂停没有记成 paused: %+v", paused)
	}
}

type failingPublicationStore struct {
	runstore.Store
	failRead          int
	reads             int
	readErr, writeErr error
}

func (s *failingPublicationStore) Get(ctx context.Context, id string) (runstore.RunRecord, error) {
	s.reads++
	if s.reads == s.failRead {
		return runstore.RunRecord{}, s.readErr
	}
	return s.Store.Get(ctx, id)
}
func (s *failingPublicationStore) CompareAndSwap(context.Context, string, uint64, runstore.RunRecord) (runstore.RunRecord, error) {
	return runstore.RunRecord{}, s.writeErr
}

func TestManagedRunRetainsOutcomeOnPublicationFailure(t *testing.T) {
	for _, failRead := range []int{0, 1, 2} {
		t.Run(map[int]string{0: "write", 1: "initial read", 2: "read after write"}[failRead], func(t *testing.T) {
			readErr, writeErr := errors.New("read unavailable"), errors.New("write unavailable")
			store := &failingPublicationStore{Store: runstore.NewMemory(), failRead: failRead, readErr: readErr, writeErr: writeErr}
			w := managedWorker(t, managedAgent(t, checkpoint.NewMemory(), "done", nil), store, "worker")
			r := managedRecord("result-retained", "thread", "turn")
			r.Status = runstore.StatusRunning
			claimed := managedCreate(t, store, r)
			detail := agent.RunResult{RunID: r.RunID, ThreadID: r.ThreadID, TurnID: r.TurnID, Revision: 5, CheckpointRef: &checkpoint.Ref{ThreadID: r.ThreadID, CheckpointID: "boundary"}}
			executionErr, completionErr := errors.New("execution failed"), errors.New("checkpoint failed")
			out, err := w.publish(context.Background(), claimed, detail, executionErr, nil, completionErr)
			if out.Result == nil || out.Result.RunID != detail.RunID || out.Result.Revision != 5 || out.Result.CheckpointRef.CheckpointID != "boundary" {
				t.Fatalf("frozen result lost: %+v", out)
			}
			if out.Record.RunID != r.RunID || out.Record.Status != runstore.StatusRunning {
				t.Fatalf("last known record lost: %+v", out.Record)
			}
			if !errors.Is(out.RunError, executionErr) || !errors.Is(out.CompletionError, completionErr) || out.RecordError == nil {
				t.Fatalf("independent errors lost: %+v", out)
			}
			if !errors.Is(err, executionErr) || !errors.Is(err, completionErr) {
				t.Fatalf("returned error lost cause: %v", err)
			}
			if failRead != 0 && !errors.Is(err, readErr) {
				t.Fatalf("read error missing: %v", err)
			}
			if failRead != 1 && !errors.Is(err, writeErr) {
				t.Fatalf("write error missing: %v", err)
			}
		})
	}
}

func TestManagedRunRetainsSuccessfulResultOnWriteFailure(t *testing.T) {
	store := &refuseTerminalStore{Store: runstore.NewMemory()}
	w := managedWorker(t, managedAgent(t, checkpoint.NewMemory(), "retained answer", nil), store, "worker")
	r := managedCreate(t, store, managedRecord("success-retained", "thread-success", "turn"))
	out, err := w.Execute(context.Background(), r.RunID, "hello")
	if err == nil || out.RecordError == nil {
		t.Fatal("expected write failure")
	}
	if out.Result == nil || out.Result.Result.Message.Text() != "retained answer" {
		t.Fatalf("successful answer lost: %+v", out.Result)
	}
	if out.CompletionRef == nil || out.CompletionError != nil || out.RunError != nil {
		t.Fatalf("completion evidence lost: %+v", out)
	}
}
