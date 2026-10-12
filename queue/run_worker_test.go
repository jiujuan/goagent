package queue

// SM-07（ADR-0034 D4、D5）：受管运行的 worker 生命周期。领取是一次 CAS，所以重复投递不
// 会第二次启动模型；执行结局、checkpoint 终态记录的失败、任务存储的写失败是三件分开报出
// 的事，最后一条永远不会被一个成功返回掩盖。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runstore"
)

// --- 夹具 -------------------------------------------------------------------

func managedTextModel(text string, calls *atomic.Int64) llm.Model {
	return mock.New("m", func(*llm.Request) *llm.Response {
		if calls != nil {
			calls.Add(1)
		}
		return mock.Text(text)
	})
}

func managedAgent(t *testing.T, store checkpoint.Checkpointer, text string, calls *atomic.Int64) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithCheckpointer(store), agent.WithModel(managedTextModel(text, calls)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// managedBlockingAgent 调一次 gate 工具，工具卡到执行 context 取消为止。
func managedBlockingAgent(t *testing.T, store checkpoint.Checkpointer, entered chan struct{}, release <-chan struct{}) *agent.Agent {
	t.Helper()
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("工具之后完成")
		}
		return mock.CallTool("c1", "gate", `{}`)
	})
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(model),
		agent.WithTools(blockingGateTool(entered, release)),
		agent.WithToolTimeout(time.Minute),
		agent.WithMaxTurns(4),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func managedRecord(runID, threadID, turnID string) runstore.RunRecord {
	return runstore.RunRecord{
		RunID:    runID,
		ThreadID: threadID,
		TurnID:   turnID,
		JobID:    "job-" + runID,
		Status:   runstore.StatusQueued,
	}
}

func managedCreate(t *testing.T, store runstore.Store, rec runstore.RunRecord) runstore.RunRecord {
	t.Helper()
	created, err := store.Create(context.Background(), rec)
	if err != nil {
		t.Fatalf("建任务记录 %s: %v", rec.RunID, err)
	}
	return created
}

func managedGet(t *testing.T, store runstore.Store, runID string) runstore.RunRecord {
	t.Helper()
	rec, err := store.Get(context.Background(), runID)
	if err != nil {
		t.Fatalf("读任务记录 %s: %v", runID, err)
	}
	return rec
}

func managedCAS(t *testing.T, store runstore.Store, rec runstore.RunRecord, mutate func(*runstore.RunRecord)) runstore.RunRecord {
	t.Helper()
	next := rec
	mutate(&next)
	updated, err := store.CompareAndSwap(context.Background(), rec.RunID, rec.Version, next)
	if err != nil {
		t.Fatalf("改写任务记录 %s: %v", rec.RunID, err)
	}
	return updated
}

func managedWorker(t *testing.T, a *agent.Agent, store runstore.Store, workerID string, opts ...WorkerOption) *ManagedRunWorker {
	t.Helper()
	w, err := NewManagedRunWorker(a, store, workerID, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// readLatchStore 让前两次 Get 互相等待，用来把两个 worker 的领取压到同一个版本上。
type readLatchStore struct {
	runstore.Store

	mu       sync.Mutex
	arrivals int
	both     chan struct{}
}

func (l *readLatchStore) Get(ctx context.Context, runID string) (runstore.RunRecord, error) {
	rec, err := l.Store.Get(ctx, runID)
	if err != nil {
		return rec, err
	}
	l.mu.Lock()
	l.arrivals++
	if l.arrivals == 2 {
		close(l.both)
	}
	l.mu.Unlock()
	<-l.both
	return rec, nil
}

// refuseTerminalStore 拒绝任何把记录带入终态的写入，模拟任务存储在收尾时不可用。
type refuseTerminalStore struct {
	runstore.Store
}

func (s *refuseTerminalStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	if next.Status.Terminal() {
		return runstore.RunRecord{}, fmt.Errorf("测试设定：任务存储收尾写入不可用: %w", runstore.ErrConflict)
	}
	return s.Store.CompareAndSwap(ctx, runID, expected, next)
}

// --- 领取与发布 -------------------------------------------------------------

// TestManagedRunWorkerClaimAndPublish：queued 记录被领取、执行、发布终态，任务记录/运行/
// checkpoint 三处的 RunID 是同一条。
func TestManagedRunWorkerClaimAndPublish(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "受管回答", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-ok", "thread-wk-ok", "turn-wk-ok"))
	out, err := w.Execute(ctx, created.RunID, "问题")
	if err != nil {
		t.Fatalf("受管执行失败: %v", err)
	}
	if !out.Executed || out.Duplicate || out.CancelledBeforeStart {
		t.Fatalf("本次投递的判定不对: %+v", out)
	}
	if out.RecordError != nil {
		t.Fatalf("任务存储写入正常却报了记录错误: %v", out.RecordError)
	}

	rec := out.Record
	if rec.Status != runstore.StatusSucceeded || rec.FinishReason != runstore.FinishReasonCompleted {
		t.Fatalf("终态 = %s/%s", rec.Status, rec.FinishReason)
	}
	if rec.RunID != created.RunID || rec.ThreadID != created.ThreadID || rec.TurnID != created.TurnID {
		t.Fatalf("发布改动了身份: %+v", rec)
	}
	if rec.JobID != created.JobID {
		t.Fatalf("JobID 丢了: %q", rec.JobID)
	}
	if rec.Result == nil || rec.Result.Message.Text() != "受管回答" {
		t.Fatalf("结果没落进记录: %+v", rec.Result)
	}
	if rec.WorkerID != "worker-A" || rec.WorkerEpoch != 1 {
		t.Fatalf("领取信息 = %q epoch %d", rec.WorkerID, rec.WorkerEpoch)
	}
	if rec.WorkerLeaseUntil.IsZero() {
		t.Fatal("领取没写租期")
	}
	if rec.Version != created.Version+2 {
		t.Fatalf("Version = %d, 期望领取+发布共 %d", rec.Version, created.Version+2)
	}
	if rec.Error != nil || rec.RecordError != nil {
		t.Fatalf("成功的运行不该带错误: %+v %+v", rec.Error, rec.RecordError)
	}
	if calls.Load() != 1 {
		t.Fatalf("模型调用 %d 次", calls.Load())
	}

	// 运行自己提交的边界，与终态记录，是两个不同的 checkpoint。
	if rec.LatestCheckpointRef == nil || rec.CheckpointRevision == 0 {
		t.Fatalf("没有记下本次运行的边界: %+v rev=%d", rec.LatestCheckpointRef, rec.CheckpointRevision)
	}
	if rec.CompletionRef == nil {
		t.Fatal("成功运行应记下终态 checkpoint")
	}
	boundary, err := cps.Load(ctx, rec.LatestCheckpointRef.ThreadID, rec.LatestCheckpointRef.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.RunID != rec.RunID || boundary.TurnID != rec.TurnID {
		t.Fatalf("运行提交的 checkpoint 身份与记录不符: run=%q turn=%q", boundary.RunID, boundary.TurnID)
	}
	completion, err := cps.Load(ctx, rec.CompletionRef.ThreadID, rec.CompletionRef.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	if completion.Completion == nil || completion.Completion.RunID != rec.RunID {
		t.Fatalf("终态记录的 RunID 与任务记录不符: %+v", completion.Completion)
	}
	if completion.RunID != rec.RunID || completion.Revision != rec.CheckpointRevision+1 {
		t.Fatalf("终态记录不是接在本次边界之后: rev=%d, 记录 rev=%d", completion.Revision, rec.CheckpointRevision)
	}
	if got := managedGet(t, store, created.RunID); got.Version != rec.Version {
		t.Fatalf("返回的记录与落库的不一致: %d vs %d", rec.Version, got.Version)
	}
}

// TestManagedRunWorkerSkipsSettledRecords：已被领取、已暂停、已终结的记录再投递一次，
// 模型一次都不调用，记录也不推进版本。
func TestManagedRunWorkerSkipsSettledRecords(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		settle func(t *testing.T, store runstore.Store, created runstore.RunRecord) runstore.Status
	}{
		{
			name: "another_worker_running",
			settle: func(t *testing.T, store runstore.Store, created runstore.RunRecord) runstore.Status {
				managedCAS(t, store, created, func(r *runstore.RunRecord) {
					r.Status = runstore.StatusRunning
					r.WorkerID, r.WorkerEpoch = "worker-other", 1
				})
				return runstore.StatusRunning
			},
		},
		{
			name: "paused",
			settle: func(t *testing.T, store runstore.Store, created runstore.RunRecord) runstore.Status {
				running := managedCAS(t, store, created, func(r *runstore.RunRecord) {
					r.Status = runstore.StatusRunning
					r.WorkerID, r.WorkerEpoch = "worker-other", 1
				})
				managedCAS(t, store, running, func(r *runstore.RunRecord) { r.Status = runstore.StatusPaused })
				return runstore.StatusPaused
			},
		},
		{
			name: "settled",
			settle: func(t *testing.T, store runstore.Store, created runstore.RunRecord) runstore.Status {
				running := managedCAS(t, store, created, func(r *runstore.RunRecord) { r.Status = runstore.StatusRunning })
				managedCAS(t, store, running, func(r *runstore.RunRecord) {
					r.Status = runstore.StatusSucceeded
					r.FinishReason = runstore.FinishReasonCompleted
				})
				return runstore.StatusSucceeded
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cps := checkpoint.NewMemory()
			store := runstore.NewMemory()
			var calls atomic.Int64
			w := managedWorker(t, managedAgent(t, cps, "不该再跑", &calls), store, "worker-A")

			created := managedCreate(t, store, managedRecord("run-wk-dup-"+tc.name, "thread-wk-dup-"+tc.name, "turn-1"))
			want := tc.settle(t, store, created)
			before := managedGet(t, store, created.RunID)

			out, err := w.Execute(ctx, created.RunID, "问题")
			if err != nil {
				t.Fatalf("重复投递不该报错: %v", err)
			}
			if !out.Duplicate || out.Executed || out.CancelledBeforeStart {
				t.Fatalf("重复投递判定不对: %+v", out)
			}
			if out.Record.Status != want {
				t.Fatalf("返回状态 = %s, 期望 %s", out.Record.Status, want)
			}
			if calls.Load() != 0 {
				t.Fatalf("重复投递调用了 %d 次模型", calls.Load())
			}
			if got := managedGet(t, store, created.RunID); got.Version != before.Version {
				t.Fatalf("重复投递推进了版本: %d → %d", before.Version, got.Version)
			}
			if count := terminalCount(t, cps, created.ThreadID); count != 0 {
				t.Fatalf("重复投递写出了 %d 条终态 checkpoint", count)
			}
		})
	}
}

// TestManagedRunWorkerClaimRace：两条投递压到同一个版本上竞争领取，只有一方执行。
func TestManagedRunWorkerClaimRace(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	latch := &readLatchStore{Store: runstore.NewMemory(), both: make(chan struct{})}
	var calls atomic.Int64

	created := managedCreate(t, latch, managedRecord("run-wk-race", "thread-wk-race", "turn-1"))
	workers := make([]*ManagedRunWorker, 0, 2)
	for _, id := range []string{"worker-A", "worker-B"} {
		workers = append(workers, managedWorker(t, managedAgent(t, cps, "只跑一次", &calls), latch, id))
	}
	winnerID := make(chan string, 2)
	for _, w := range workers {
		go func(w *ManagedRunWorker) {
			out, err := w.Execute(ctx, created.RunID, "问题")
			if err != nil {
				t.Errorf("worker %s 执行报错: %v", w.workerID, err)
			}
			winner := ""
			if out.Executed {
				winner = w.workerID
			}
			winnerID <- winner
		}(w)
	}

	var executed []string
	for range 2 {
		if id := <-winnerID; id != "" {
			executed = append(executed, id)
		}
	}
	if len(executed) != 1 {
		t.Fatalf("竞争领取后执行了 %d 次: %v", len(executed), executed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("模型调用 %d 次, 期望 1", got)
	}
	rec := managedGet(t, latch, created.RunID)
	if rec.Status != runstore.StatusSucceeded || rec.WorkerID != executed[0] {
		t.Fatalf("终态归属不对: worker=%q status=%s", rec.WorkerID, rec.Status)
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 1 {
		t.Fatalf("终态 checkpoint 出现 %d 条", count)
	}
}

// --- 取消 -------------------------------------------------------------------

// TestManagedRunWorkerQueuedCancellation：排队期间已持久化取消的记录直接结束，不启动执行。
func TestManagedRunWorkerQueuedCancellation(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "不该跑", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-queued-cancel", "thread-wk-queued-cancel", "turn-1"))
	cancelled := managedCAS(t, store, created, func(r *runstore.RunRecord) {
		r.Status = runstore.StatusCancelled
		r.FinishReason = runstore.FinishReasonCancelled
		r.CancelRequestedAt = time.Now().UTC()
	})

	out, err := w.Execute(ctx, created.RunID, "问题")
	if err != nil {
		t.Fatalf("排队取消不该报错: %v", err)
	}
	if !out.CancelledBeforeStart || out.Executed || out.Duplicate {
		t.Fatalf("判定不对: %+v", out)
	}
	if out.Record.Version != cancelled.Version {
		t.Fatalf("worker 推进了已取消记录: %d → %d", cancelled.Version, out.Record.Version)
	}
	if calls.Load() != 0 {
		t.Fatalf("已取消的排队记录调用了 %d 次模型", calls.Load())
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("为已取消的排队记录写出了 %d 条终态", count)
	}
}

// TestManagedRunWorkerHonoursCancelWhileRunning：运行中被持久化的取消请求赢下竞争——
// 记录进入 cancelling，运行因此结束时发布 cancelled，而不是 failed。
func TestManagedRunWorkerHonoursCancelWhileRunning(t *testing.T) {
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	entered := make(chan struct{})
	release := make(chan struct{})
	w := managedWorker(t, managedBlockingAgent(t, cps, entered, release), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-cancel", "thread-wk-cancel", "turn-1"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		out ManagedRunOutcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := w.Execute(ctx, created.RunID, "问题")
		done <- result{out, err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("工具没有开始运行")
	}

	// 等领取落库，然后把取消请求写在它上面。
	running := waitForRunning(t, store, created.RunID)
	requested := managedCAS(t, store, running, func(r *runstore.RunRecord) {
		r.Status = runstore.StatusCancelling
		r.CancelRequestedAt = time.Now().UTC()
	})
	cancel()

	res := <-done
	if res.err == nil || !errors.Is(res.err, context.Canceled) {
		t.Fatalf("取消的执行应带 context.Canceled, got %v", res.err)
	}
	rec := res.out.Record
	if rec.Status != runstore.StatusCancelled || rec.FinishReason != runstore.FinishReasonCancelled {
		t.Fatalf("发布状态 = %s/%s, 期望 cancelled", rec.Status, rec.FinishReason)
	}
	if rec.Error != nil {
		t.Fatalf("honoured 的取消不该带执行错误: %+v", rec.Error)
	}
	if !rec.CancelRequestedAt.Equal(requested.CancelRequestedAt) {
		t.Fatalf("取消请求时间被改写: %v → %v", requested.CancelRequestedAt, rec.CancelRequestedAt)
	}
	if rec.Result != nil {
		t.Fatalf("取消的运行不该有结果: %+v", rec.Result)
	}
}

func waitForRunning(t *testing.T, store runstore.Store, runID string) runstore.RunRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := store.Get(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Status == runstore.StatusRunning {
			return rec
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("运行 %s 没有在 5 秒内进入 running", runID)
	return runstore.RunRecord{}
}

// TestManagedRunWorkerUnusableIdentity：任务记录里的线程身份不被 agent 接受时，连手柄都
// 建不起来，记录仍然落成 failed，且不带任何 checkpoint 凭据。
func TestManagedRunWorkerUnusableIdentity(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "跑不起来", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-badthread", "bad/thread", "turn-1"))
	out, err := w.Execute(ctx, created.RunID, "问题")
	if err == nil {
		t.Fatal("身份不可用应返回错误")
	}
	if !errors.Is(err, agent.ErrInvalidManagedRun) {
		t.Fatalf("返回错误 = %v, 期望 ErrInvalidManagedRun", err)
	}
	if !out.Executed {
		t.Fatalf("确实领取过: %+v", out)
	}
	if out.Record.Status != runstore.StatusFailed || out.Record.Error == nil {
		t.Fatalf("状态 = %+v", out.Record)
	}
	if calls.Load() != 0 {
		t.Fatalf("入口拒绝仍调用了 %d 次模型", calls.Load())
	}
	if out.Record.CompletionRef != nil || out.Record.LatestCheckpointRef != nil {
		t.Fatalf("没有运行的记录却带 checkpoint 凭据: %+v %+v", out.Record.CompletionRef, out.Record.LatestCheckpointRef)
	}
	if hist, err := cps.History(ctx, created.ThreadID); err != nil {
		t.Fatal(err)
	} else if len(hist) != 0 {
		t.Fatalf("不可用身份写下了 %d 条 checkpoint", len(hist))
	}
}

// --- 三个分开的错误出口 -----------------------------------------------------

// TestManagedRunWorkerFailureWithoutBoundary：本次没有任何提交时，任务记录照样能写成
// failed，但不能造一条 checkpoint。
func TestManagedRunWorkerFailureWithoutBoundary(t *testing.T) {
	ctx := context.Background()
	settle := newSettleStore()
	blocked := beginRefusingStore{ThreadCoordinator: settle}
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, blocked, "跑不起来", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-noboundary", "thread-wk-noboundary", "turn-1"))
	out, err := w.Execute(ctx, created.RunID, "问题")
	if err == nil {
		t.Fatal("执行失败应返回错误")
	}
	if !out.Executed || out.Duplicate {
		t.Fatalf("确实领取并执行过: %+v", out)
	}
	if out.RecordError != nil {
		t.Fatalf("任务存储写入正常却报了记录错误: %v", out.RecordError)
	}
	rec := out.Record
	if rec.Status != runstore.StatusFailed || rec.FinishReason != runstore.FinishReasonFailed {
		t.Fatalf("状态 = %s/%s", rec.Status, rec.FinishReason)
	}
	if rec.Error == nil || rec.Error.Code != runstore.ErrCodeExecutionFailed {
		t.Fatalf("错误码 = %+v, 期望 %s", rec.Error, runstore.ErrCodeExecutionFailed)
	}
	if rec.CompletionRef != nil || rec.RecordError != nil {
		t.Fatalf("无本次边界却留下了收尾凭据: %+v %+v", rec.CompletionRef, rec.RecordError)
	}
	if rec.LatestCheckpointRef != nil || rec.CheckpointRevision != 0 {
		t.Fatalf("继承的 head 被当成本次边界: %+v rev=%d", rec.LatestCheckpointRef, rec.CheckpointRevision)
	}
	if calls.Load() != 0 {
		t.Fatalf("准备失败仍调用了 %d 次模型", calls.Load())
	}
	if count := terminalCount(t, settle, created.ThreadID); count != 0 {
		t.Fatalf("为无本次边界的运行造了 %d 条终态 checkpoint", count)
	}
}

// TestManagedRunWorkerCompletionRecordError：checkpoint 终态写失败时任务记录仍是 succeeded，
// 失败只出现在 RecordError 与返回的错误里。
func TestManagedRunWorkerCompletionRecordError(t *testing.T) {
	ctx := context.Background()
	settle := newSettleStore()
	settle.recordOn = true
	settle.record = errors.New("终态写不进去")
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, settle, "执行是成功的", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-recerror", "thread-wk-recerror", "turn-1"))
	out, err := w.Execute(ctx, created.RunID, "问题")
	if err == nil {
		t.Fatal("终态记录失败必须返回错误")
	}
	if !errors.Is(err, settle.record) {
		t.Fatalf("返回错误丢了存储原因: %v", err)
	}
	if !out.Executed || out.RecordError != nil {
		t.Fatalf("任务存储本身写成功了: %+v", out)
	}
	rec := out.Record
	if rec.Status != runstore.StatusSucceeded {
		t.Fatalf("执行成功应发布 succeeded, got %s", rec.Status)
	}
	if rec.Result == nil || rec.Result.Message.Text() != "执行是成功的" {
		t.Fatalf("冻结结果没落库: %+v", rec.Result)
	}
	if rec.Error != nil {
		t.Fatalf("执行没失败: %+v", rec.Error)
	}
	if rec.CompletionRef != nil {
		t.Fatalf("终态没落库却有引用: %+v", rec.CompletionRef)
	}
	if rec.RecordError == nil || rec.RecordError.Code != runstore.ErrCodeCompletionRecord {
		t.Fatalf("RecordError = %+v, 期望 %s", rec.RecordError, runstore.ErrCodeCompletionRecord)
	}
	if rec.RecordError.Message == "" || !rec.RecordError.Retryable {
		t.Fatalf("RecordError 丢了可重试信息: %+v", rec.RecordError)
	}
	if count := terminalCount(t, settle, created.ThreadID); count != 0 {
		t.Fatalf("被拒的终态落库了 %d 条", count)
	}
}

// TestManagedRunWorkerStoreWriteFailure：任务存储收尾写失败时，返回的是存储里真实的状态，
// 并把写错误同时放在返回值与 error 上。
func TestManagedRunWorkerStoreWriteFailure(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	durable := runstore.NewMemory()
	store := &refuseTerminalStore{Store: durable}
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "跑完了但记不上", &calls), store, "worker-A")

	created := managedCreate(t, store, managedRecord("run-wk-writefail", "thread-wk-writefail", "turn-1"))
	out, err := w.Execute(ctx, created.RunID, "问题")
	if err == nil {
		t.Fatal("任务存储写失败必须返回错误")
	}
	if out.RecordError == nil {
		t.Fatal("写失败必须单独报出来")
	}
	if !errors.Is(err, out.RecordError) {
		t.Fatalf("返回的错误没包含写错误: %v / %v", err, out.RecordError)
	}
	if !out.Executed {
		t.Fatalf("运行确实执行过: %+v", out)
	}
	if out.Record.Status != runstore.StatusRunning {
		t.Fatalf("应报回存储真实状态 running, got %s", out.Record.Status)
	}
	if out.Record.CompletionRef != nil {
		t.Fatalf("没写进去的终态不该出现在返回记录里: %+v", out.Record.CompletionRef)
	}
	if stored := managedGet(t, durable, created.RunID); stored.Status != runstore.StatusRunning {
		t.Fatalf("落库状态被改写: %s", stored.Status)
	}
	// checkpoint 的终态记录与任务记录是两件事，前者仍然落成了。
	if count := terminalCount(t, cps, created.ThreadID); count != 1 {
		t.Fatalf("checkpoint 终态记录 %d 条, 期望 1", count)
	}
}

// --- worker 自身的配置与收尾 context ---------------------------------------

// TestManagedRunWorkerConstruction：构造时把缺依赖与非法间隔挡住，默认值是 30 秒。
func TestManagedRunWorkerConstruction(t *testing.T) {
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	a := managedAgent(t, cps, "回答", nil)

	for name, err := range map[string]error{
		"no agent":       func() error { _, e := NewManagedRunWorker(nil, store, "w"); return e }(),
		"no store":       func() error { _, e := NewManagedRunWorker(a, nil, "w"); return e }(),
		"no worker id":   func() error { _, e := NewManagedRunWorker(a, store, ""); return e }(),
		"zero lease":     func() error { _, e := NewManagedRunWorker(a, store, "w", WithWorkerLease(0)); return e }(),
		"negative lease": func() error { _, e := NewManagedRunWorker(a, store, "w", WithWorkerLease(-time.Second)); return e }(),
		"zero record timeout": func() error {
			_, e := NewManagedRunWorker(a, store, "w", WithManagedRecordTimeout(0))
			return e
		}(),
	} {
		if err == nil {
			t.Fatalf("%s 应构造失败", name)
		}
	}

	w := managedWorker(t, a, store, "w")
	if w.lease != managedRunLease || w.recordTimeout != terminalRecordTimeout {
		t.Fatalf("默认值 = %v / %v", w.lease, w.recordTimeout)
	}
}

type managedCtxKey struct{}

// TestManagedRunWorkerRecordContext：终态写用独立 context——执行 context 已经取消，记录
// 照样落库；这个 context 保留可信 values 并带上可配置的期限。
func TestManagedRunWorkerRecordContext(t *testing.T) {
	settle := newSettleStore()
	settle.gateOn = true
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, settle, "独立收尾", &calls), store, "worker-A",
		WithManagedRecordTimeout(3*time.Second))

	created := managedCreate(t, store, managedRecord("run-wk-ctx", "thread-wk-ctx", "turn-1"))
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	recCtx := context.WithValue(parent, managedCtxKey{}, "可信值")

	done := make(chan error, 1)
	go func() {
		_, err := w.Execute(recCtx, created.RunID, "问题")
		done <- err
	}()

	select {
	case <-settle.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("终态提交没有到达闸门")
	}
	cancelParent()
	close(settle.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("执行 context 取消不该打断终态记录: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker 没有收尾")
	}

	recordCtx, errAt, deadline := settle.observedSettle()
	if recordCtx == nil {
		t.Fatal("没有观察到终态提交的 context")
	}
	if errAt != nil {
		t.Fatalf("终态记录的 context 带着取消: %v", errAt)
	}
	if recordCtx.Value(managedCtxKey{}) != "可信值" {
		t.Fatal("终态记录丢了可信 values")
	}
	if deadline <= 0 || deadline > 3*time.Second {
		t.Fatalf("终态记录的期限 = %v, 期望在 (0, 3s]", deadline)
	}
	if got := managedGet(t, store, created.RunID); got.Status != runstore.StatusSucceeded {
		t.Fatalf("记录状态 = %s, 期望 succeeded", got.Status)
	}
}
