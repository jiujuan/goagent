package queue

// SM-08（ADR-0034 D5）：RunClient 让调用方只凭 store + RunID 就能查询、等待、请求取消。
// 三个方法动的都是任务记录本身：等待不驱动执行，取消只是把请求持久化。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/runstore"
)

func TestRunClientConstruction(t *testing.T) {
	store := runstore.NewMemory()
	if _, err := NewRunClient(nil); err == nil {
		t.Fatal("没有 store 应构造失败")
	}

	c, err := NewRunClient(store)
	if err != nil {
		t.Fatal(err)
	}
	if c.minWait != defaultRunPollInterval || c.maxWait != defaultRunPollMax {
		t.Fatalf("默认退避 = %v/%v, 期望 %v/%v", c.minWait, c.maxWait, defaultRunPollInterval, defaultRunPollMax)
	}

	for name, err := range map[string]error{
		"zero interval": func() error {
			_, err := NewRunClient(store, WithRunPollInterval(0))
			return err
		}(),
		"negative interval": func() error {
			_, err := NewRunClient(store, WithRunPollInterval(-time.Millisecond))
			return err
		}(),
		"zero max": func() error {
			_, err := NewRunClient(store, WithRunPollMax(0))
			return err
		}(),
		"max below min": func() error {
			_, err := NewRunClient(store, WithRunPollInterval(time.Second), WithRunPollMax(time.Millisecond))
			return err
		}(),
	} {
		if err == nil {
			t.Fatalf("%s 应构造失败", name)
		}
	}

	custom, err := NewRunClient(store, WithRunPollInterval(20*time.Millisecond), WithRunPollMax(80*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if custom.minWait != 20*time.Millisecond || custom.maxWait != 80*time.Millisecond {
		t.Fatalf("配置没生效: %v/%v", custom.minWait, custom.maxWait)
	}
}

// TestRunClientBackoffCapped：等待从最小间隔倍增到上限为止，之后不再增长。
func TestRunClientBackoffCapped(t *testing.T) {
	c, err := NewRunClient(runstore.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		2 * time.Second,
		2 * time.Second,
	}
	delay := c.minWait
	for i, expect := range want {
		delay = c.nextWait(delay)
		if delay != expect {
			t.Fatalf("第 %d 次退避 = %v, 期望 %v", i+1, delay, expect)
		}
	}
}

// TestReconnectWaitDoesNotDrive：等待者只读记录。它既不会领取也不会驱动运行，暂停的记录
// 立刻返回而不继续等审批。
func TestReconnectWaitDoesNotDrive(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	var calls atomic.Int64
	// 有 agent 但没有 worker 被调用：等待者自己绝不驱动执行。
	_ = managedAgent(t, cps, "等待不该启动它", &calls)

	created := managedCreate(t, store, managedRecord("run-rc-wait", "thread-rc-wait", "turn-1"))
	client, err := NewRunClient(store, WithRunPollInterval(2*time.Millisecond), WithRunPollMax(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	got, err := client.GetRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Status != runstore.StatusQueued || got.Version != created.Version {
		t.Fatalf("GetRun 读到的不是原记录: %s v%d", got.Status, got.Version)
	}
	if got.WorkerID != "" || got.WorkerEpoch != 0 {
		t.Fatalf("纯读却拿到了所有权: %q epoch %d", got.WorkerID, got.WorkerEpoch)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	rec, err := client.WaitRun(waitCtx, created.RunID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("仍在排队的 WaitRun = %v, 期望 ctx 超时", err)
	}
	if rec.RunID != created.RunID || rec.Status != runstore.StatusQueued {
		t.Fatalf("超时仍应带回最后读到的记录: %+v", rec)
	}
	if calls.Load() != 0 {
		t.Fatalf("等待者驱动了 %d 次模型", calls.Load())
	}
	if after := managedGet(t, store, created.RunID); after.Version != created.Version {
		t.Fatalf("等待推进了记录版本: %d → %d", created.Version, after.Version)
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("等待写出了 %d 条 checkpoint", count)
	}

	// 未知 ID 是 ErrNotFound，不是"等一会儿再报超时"。
	unknownCtx, unknownCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer unknownCancel()
	if _, err := client.WaitRun(unknownCtx, "run-never-created"); !errors.Is(err, runstore.ErrNotFound) {
		t.Fatalf("等待未知 RunID = %v, 期望 ErrNotFound", err)
	}

	// 暂停对本次运行是终态：等到即返回，不等审批。
	paused := managedCAS(t, store, managedCAS(t, store, created, func(r *runstore.RunRecord) {
		r.Status = runstore.StatusRunning
		r.WorkerID, r.WorkerEpoch = "worker-A", 1
	}), func(r *runstore.RunRecord) {
		r.Status = runstore.StatusPaused
		r.FinishReason = runstore.FinishReasonPaused
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readyCancel()
	gotPaused, err := client.WaitRun(readyCtx, created.RunID)
	if err != nil {
		t.Fatalf("等待暂停记录不该超时: %v", err)
	}
	if gotPaused.Status != runstore.StatusPaused || gotPaused.Version != paused.Version {
		t.Fatalf("等到的不是暂停记录: %s v%d", gotPaused.Status, gotPaused.Version)
	}
}

// TestCancelBeforeStart：排队中的取消请求直接把记录落成 cancelled，重复取消幂等返回原记录，
// worker 之后拿到这次投递也只能判定"开始前已被取消"。
func TestCancelBeforeStart(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "不该跑", &calls), store, "worker-A")
	client := mustRunClient(t, store)

	created := managedCreate(t, store, managedRecord("run-rc-early", "thread-rc-early", "turn-1"))
	cancelled, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if cancelled.Status != runstore.StatusCancelled || cancelled.FinishReason != runstore.FinishReasonCancelled {
		t.Fatalf("排队取消 = %s/%s", cancelled.Status, cancelled.FinishReason)
	}
	if cancelled.CancelRequestedAt.IsZero() {
		t.Fatal("取消请求没带时间")
	}
	if cancelled.Version != created.Version+1 {
		t.Fatalf("Version = %d, 期望 %d", cancelled.Version, created.Version+1)
	}
	if cancelled.RunID != created.RunID || cancelled.ThreadID != created.ThreadID ||
		cancelled.TurnID != created.TurnID || cancelled.JobID != created.JobID {
		t.Fatalf("取消改动了身份: %+v", cancelled)
	}
	if cancelled.Result != nil || cancelled.Error != nil {
		t.Fatalf("取消不该带结果或错误: %+v %+v", cancelled.Result, cancelled.Error)
	}

	again, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("重复取消应幂等: %v", err)
	}
	if again.Version != cancelled.Version || again.Status != runstore.StatusCancelled {
		t.Fatalf("重复取消推进了记录: v%d %s", again.Version, again.Status)
	}

	out, err := w.Execute(ctx, created.RunID, "问题")
	if err != nil {
		t.Fatalf("worker 处理已取消记录不该报错: %v", err)
	}
	if !out.CancelledBeforeStart || out.Executed {
		t.Fatalf("worker 判定不对: %+v", out)
	}
	if calls.Load() != 0 {
		t.Fatalf("排队取消仍调用了 %d 次模型", calls.Load())
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("为排队取消写了 %d 条 checkpoint", count)
	}
}

// TestCancelTerminalIsIdempotent：已终结的记录只被读回，不会被取消改写，也不推版本。
func TestCancelTerminalIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	client := mustRunClient(t, store)

	created := managedCreate(t, store, managedRecord("run-rc-settled", "thread-rc-settled", "turn-1"))
	running := managedCAS(t, store, created, func(r *runstore.RunRecord) { r.Status = runstore.StatusRunning })
	settled := managedCAS(t, store, running, func(r *runstore.RunRecord) {
		r.Status = runstore.StatusSucceeded
		r.FinishReason = runstore.FinishReasonCompleted
	})

	got, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("对已终结记录取消应幂等: %v", err)
	}
	if got.Status != runstore.StatusSucceeded || got.Version != settled.Version {
		t.Fatalf("返回 = %s v%d, 期望 succeeded v%d", got.Status, got.Version, settled.Version)
	}
	after := managedGet(t, store, created.RunID)
	if !after.CancelRequestedAt.IsZero() {
		t.Fatalf("已终结的记录被写上取消时间: %v", after.CancelRequestedAt)
	}

	if _, err := client.CancelRun(ctx, "run-rc-missing"); !errors.Is(err, runstore.ErrNotFound) {
		t.Fatalf("取消未知 RunID = %v, 期望 ErrNotFound", err)
	}
}

// TestCancelRunning：worker 自己轮询到取消请求并停止执行，调用方不需要碰它的 context。
func TestCancelRunning(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	entered := make(chan struct{})
	release := make(chan struct{})
	w := managedWorker(t, managedBlockingAgent(t, cps, entered, release), store, "worker-A",
		WithCancelPollInterval(5*time.Millisecond))
	client := mustRunClient(t, store)

	created := managedCreate(t, store, managedRecord("run-cancel-running", "thread-cancel-running", "turn-1"))
	done := make(chan error, 1)
	go func() {
		_, err := w.Execute(ctx, created.RunID, "问题")
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("工具没有开始运行")
	}

	requested, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if requested.Status != runstore.StatusCancelling || requested.CancelRequestedAt.IsZero() {
		t.Fatalf("运行中的取消应落成 cancelling: %s %v", requested.Status, requested.CancelRequestedAt)
	}
	if requested.WorkerID != "worker-A" || requested.WorkerEpoch != 1 {
		t.Fatalf("取消请求丢了所有权: %q epoch %d", requested.WorkerID, requested.WorkerEpoch)
	}

	// 没有人取消执行 context：只有 worker 的轮询能停下来。
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("执行应带取消错误, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker 没有消费取消请求")
	}

	rec := managedGet(t, store, created.RunID)
	if rec.Status != runstore.StatusCancelled || rec.FinishReason != runstore.FinishReasonCancelled {
		t.Fatalf("发布状态 = %s/%s, 期望 cancelled", rec.Status, rec.FinishReason)
	}
	if rec.Error != nil || rec.Result != nil {
		t.Fatalf("取消的记录不该带错误或结果: %+v %+v", rec.Error, rec.Result)
	}
	if !rec.CancelRequestedAt.Equal(requested.CancelRequestedAt) {
		t.Fatalf("取消请求时间被改写: %v → %v", requested.CancelRequestedAt, rec.CancelRequestedAt)
	}
	// 取消的执行仍然记录了自己的终态。
	if count := terminalCount(t, cps, created.ThreadID); count != 1 {
		t.Fatalf("checkpoint 终态记录 %d 条, 期望 1", count)
	}

	again, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("取消结束后再取消应幂等: %v", err)
	}
	if again.Version != rec.Version || again.Status != runstore.StatusCancelled {
		t.Fatalf("幂等取消推进了记录: v%d %s", again.Version, again.Status)
	}
}

// TestCancelCompletionRace：取消请求落在"运行已结束、结局还没写"的窗口里。运行已经跑到
// 成功，就按成功的冻结结局发布；请求只留下时间，不改写结局。
func TestCancelCompletionRace(t *testing.T) {
	ctx := context.Background()
	settle := newSettleStore()
	settle.gateOn = true
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, settle, "已经答完了", &calls), store, "worker-A",
		WithCancelPollInterval(time.Hour))

	created := managedCreate(t, store, managedRecord("run-cancel-race", "thread-cancel-race", "turn-1"))
	done := make(chan ManagedRunOutcome, 1)
	go func() {
		out, err := w.Execute(ctx, created.RunID, "问题")
		if err != nil {
			t.Errorf("运行本身不该报错: %v", err)
		}
		done <- out
	}()

	// 运行结束、终态 checkpoint 卡在闸门上：此刻任务记录还是 running。
	select {
	case <-settle.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("终态提交没有到达闸门")
	}
	client := mustRunClient(t, store)
	requested, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if requested.Status != runstore.StatusCancelling {
		t.Fatalf("请求应落成 cancelling: %s", requested.Status)
	}
	// 请求已经在记录上：再问一次不能又写一遍。
	second, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatalf("重复请求取消: %v", err)
	}
	if second.Version != requested.Version || second.Status != runstore.StatusCancelling {
		t.Fatalf("重复请求推进了记录: v%d %s", second.Version, second.Status)
	}

	close(settle.release)
	out := <-done
	rec := out.Record
	if rec.Status != runstore.StatusSucceeded || rec.FinishReason != runstore.FinishReasonCompleted {
		t.Fatalf("冻结结局被迟到的取消改写了: %s/%s", rec.Status, rec.FinishReason)
	}
	if rec.Result == nil || rec.Result.Message.Text() != "已经答完了" {
		t.Fatalf("结果丢了: %+v", rec.Result)
	}
	if rec.CancelRequestedAt.IsZero() || !rec.CancelRequestedAt.Equal(requested.CancelRequestedAt) {
		t.Fatalf("取消请求时间没保留: %v", rec.CancelRequestedAt)
	}
	if rec.Error != nil || rec.RecordError != nil {
		t.Fatalf("成功的运行不该带错误: %+v %+v", rec.Error, rec.RecordError)
	}
	if out.RecordError != nil {
		t.Fatalf("发布不该报写失败: %v", out.RecordError)
	}
	if calls.Load() != 1 {
		t.Fatalf("模型调用 %d 次", calls.Load())
	}

	late, err := client.CancelRun(ctx, created.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if late.Status != runstore.StatusSucceeded || late.Version != rec.Version {
		t.Fatalf("结局冻结后取消应幂等返回: %s v%d", late.Status, late.Version)
	}
}

// TestManagedRunThroughPool 把整条链跑通：提交 → 队列 → Registry Handler → worker 领取
// 执行 → RunClient 只凭 RunID 等到终态；同一条载荷再送一次也不会重跑。
func TestManagedRunThroughPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	q := NewMemQueue(4)
	var calls atomic.Int64
	resolver := &managedResolver{agents: map[string]*agent.Agent{
		"support": managedAgent(t, cps, "后台完成", &calls),
	}}
	handler, err := ManagedRunHandler(store, "worker-A", resolver.resolve, WithCancelPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	pool := NewPool(q, 2).WithRegistry(Registry{ManagedRunJobType: handler})
	go pool.Run(ctx)
	defer q.Close()

	sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
		AgentKey: "support", RunID: "run-pool-1", ThreadID: "thread-pool-1", TurnID: "turn-pool-1", Input: "问题",
	})
	if err != nil {
		t.Fatalf("提交失败: %v", err)
	}

	client, err := NewRunClient(store, WithRunPollInterval(2*time.Millisecond), WithRunPollMax(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	rec, err := client.WaitRun(waitCtx, sub.RunID)
	if err != nil {
		t.Fatalf("等待终态: %v", err)
	}
	if rec.Status != runstore.StatusSucceeded || rec.Result == nil || rec.Result.Message.Text() != "后台完成" {
		t.Fatalf("记录 = %s %+v", rec.Status, rec.Result)
	}
	if rec.WorkerID != "worker-A" || rec.CompletionRef == nil {
		t.Fatalf("领取或终态引用不对: %q %+v", rec.WorkerID, rec.CompletionRef)
	}
	if calls.Load() != 1 {
		t.Fatalf("模型调用 %d 次", calls.Load())
	}
	if count := terminalCount(t, cps, "thread-pool-1"); count != 1 {
		t.Fatalf("checkpoint 终态 %d 条", count)
	}

	// 同一条载荷再送到同一个 Handler：队列至少一次投递的正常后果是无害。
	if err := handler(ctx, sub.job.Payload); err != nil {
		t.Fatalf("重复投递应让队列确认消息: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("重复投递后模型调用 %d 次", calls.Load())
	}
	if after := managedGet(t, store, sub.RunID); after.Version != rec.Version {
		t.Fatalf("重复投递推进了记录: %d → %d", rec.Version, after.Version)
	}
}

// TestRunClientOnFileStore 用真实的文件任务存储跑一遍"只看记录"：一个实例提交，另一个
// 实例只凭 RunID 等待、取消，worker 之后拿到这次投递也只能判定开始前已被取消。
func TestRunClientOnFileStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writer := mustOpenFileRunStore(t, dir)
	reader := mustOpenFileRunStore(t, dir)

	cps := checkpoint.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, cps, "文件里的运行", &calls), writer, "worker-A")
	// 投递出去但没人消费：记录只能停在排队。
	q := &captureQueue{}

	sub, err := EnqueueManagedAgent(ctx, q, writer, ManagedRunRequest{
		AgentKey: "support", RunID: "run-file-client", ThreadID: "thread-file-client",
		TurnID: "turn-1", Input: "问题",
	})
	if err != nil {
		t.Fatalf("提交失败: %v", err)
	}

	client := mustRunClient(t, reader)
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := client.WaitRun(waitCtx, sub.RunID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("没人消费投递时等待应超时: %v", err)
	}

	cancelled, err := client.CancelRun(ctx, sub.RunID)
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if cancelled.Status != runstore.StatusCancelled {
		t.Fatalf("状态 = %s", cancelled.Status)
	}

	// 换一个实例、换一个 RunClient，只凭 RunID 也能等到这条终态。
	readyCtx, readyCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readyCancel()
	got, err := client.WaitRun(readyCtx, sub.RunID)
	if err != nil {
		t.Fatalf("等待已取消的记录: %v", err)
	}
	if got.Status != runstore.StatusCancelled || got.Version != cancelled.Version {
		t.Fatalf("另一个实例读到 = %s v%d", got.Status, got.Version)
	}
	if got.RunID != sub.RunID || got.JobID != sub.JobID {
		t.Fatalf("跨实例读到的身份不对: %+v", got)
	}

	out, err := w.Execute(ctx, sub.RunID, "问题")
	if err != nil {
		t.Fatalf("worker 处理已取消记录: %v", err)
	}
	if !out.CancelledBeforeStart || calls.Load() != 0 {
		t.Fatalf("落盘的取消没被 worker 认出来: %+v calls=%d", out, calls.Load())
	}
}

func mustOpenFileRunStore(t *testing.T, dir string) runstore.Store {
	t.Helper()
	f, err := runstore.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustRunClient(t *testing.T, store runstore.Store) *RunClient {
	t.Helper()
	c, err := NewRunClient(store, WithRunPollInterval(2*time.Millisecond), WithRunPollMax(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
