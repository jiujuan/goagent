package queue

// 本文件是 BC-04（ADR-0033 D2、D5，V7）的内部测试：WaitRecorded 等待的是 worker
// 的收尾（运行结束 + 终态记录尝试），不是运行本身。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// --- 存储夹具 ---------------------------------------------------------------

// settleStore 包装 Memory 的线程协调能力，专门观察终态提交（Completion 非空）：
// 可以把它卡在闸门上、可以让它写入失败、并记下提交时使用的 context。
type settleStore struct {
	checkpoint.ThreadCoordinator

	mu sync.Mutex
	// 非终态提交计数与最近一次终态提交的观察值。
	settledReqs []checkpoint.CommitRequest
	settleCtx   []context.Context
	settleErrAt []error
	settleDead  []time.Duration

	gateOn   bool
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	recordOn bool
	record   error
}

func newSettleStore() *settleStore {
	return &settleStore{
		ThreadCoordinator: checkpoint.NewMemory(),
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
}

func (s *settleStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	terminal := req.Checkpoint != nil && req.Checkpoint.Completion != nil
	if terminal {
		s.mu.Lock()
		s.settledReqs = append(s.settledReqs, req)
		s.settleCtx = append(s.settleCtx, ctx)
		s.settleErrAt = append(s.settleErrAt, ctx.Err())
		if deadline, ok := ctx.Deadline(); ok {
			s.settleDead = append(s.settleDead, time.Until(deadline))
		} else {
			s.settleDead = append(s.settleDead, 0)
		}
		s.mu.Unlock()

		if s.gateOn {
			s.once.Do(func() { close(s.entered) })
			select {
			case <-s.release:
			case <-ctx.Done():
				return checkpoint.CommitReceipt{}, ctx.Err()
			}
		}
		if s.recordOn && s.record != nil {
			// 写入被拒绝：终态没有落盘，历史记录里不该出现 Completion。
			return checkpoint.CommitReceipt{}, s.record
		}
	}
	return s.ThreadCoordinator.Commit(ctx, req)
}

func (s *settleStore) settleCommits() []checkpoint.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.CommitRequest(nil), s.settledReqs...)
}

// observedSettle 返回第一次终态提交时记下的 context、当时的错误与剩余期限。
func (s *settleStore) observedSettle() (context.Context, error, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.settleCtx) == 0 {
		return nil, nil, 0
	}
	return s.settleCtx[0], s.settleErrAt[0], s.settleDead[0]
}

// beginRefusingStore 让 BeginRun 直接失败：运行一步都没执行，因此本次没有可记录
// 的提交边界。
type beginRefusingStore struct{ checkpoint.ThreadCoordinator }

func (s beginRefusingStore) BeginRun(context.Context, checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	return checkpoint.BeginRunResult{}, errors.New("线程已被占用")
}

// durableTextAgent 是"一次模型调用 + 可记录终态的存储"的正常完成夹具。
func durableTextAgent(t *testing.T, store checkpoint.Checkpointer, reply string) *agent.Agent {
	t.Helper()
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text(reply) })),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func durableErrAgent(t *testing.T, store checkpoint.Checkpointer) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithCheckpointer(store), agent.WithModel(errModel{}))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func durableGateAgent(t *testing.T, store checkpoint.Checkpointer, entered chan struct{}, release <-chan struct{}) *agent.Agent {
	t.Helper()
	var mu sync.Mutex
	turn := 0
	model := mock.New("m", func(*llm.Request) *llm.Response {
		mu.Lock()
		turn++
		first := turn == 1
		mu.Unlock()
		if first {
			return mock.CallTool("c1", "gate", `{}`)
		}
		return mock.Text("gate 之后完成")
	})
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(model),
		agent.WithTools(blockingGateTool(entered, release)),
		agent.WithToolTimeout(2*time.Second),
		agent.WithMaxTurns(4),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// blockingGateTool 是 BC-01 夹具里的 gate 工具形状：开始时关闭 entered，然后等 release
// 或自身 context 取消。entered 为 nil 时它直接返回。
func blockingGateTool(entered chan struct{}, release <-chan struct{}) tool.Tool {
	if entered == nil {
		return tool.New("gate", "立即返回", func(_ *tool.Context, _ struct{}) (string, error) {
			return "ok", nil
		})
	}
	var once sync.Once
	return tool.New("gate", "阻塞直到 context 取消或测试放行", func(ctx *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "released", nil
		}
	})
}

// durablePausingAgent 的工具本身立即返回并计数，审批闸门拦下它，用来覆盖"暂停收尾"。
func durablePausingAgent(t *testing.T, store checkpoint.Checkpointer, ran *atomic.Int64) *agent.Agent {
	t.Helper()
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("审批之后完成")
		}
		return mock.CallTool("c1", "write", `{}`)
	})
	w := tool.New("write", "被审批闸门拦下的工具", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return "ok", nil
	})
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(model),
		agent.WithTools(w),
		agent.WithMiddleware(approvalGate{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// terminalCount 数历史里带终态的记录条数。
func terminalCount(t *testing.T, store checkpoint.Checkpointer, threadID string) int {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, cp := range hist {
		if cp.Completion != nil {
			n++
		}
	}
	return n
}

// recordedWait 在后台调用 WaitRecorded，把结果送到 channel；调用方用它来判断
// "闸门还压着时没有返回"。
func recordedWait(h *Handle) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := h.WaitRecorded()
		ch <- err
	}()
	return ch
}

// --- V7：WaitRecorded 的等待与错误出口 --------------------------------------

// TestQueueWaitRecordedWaitsForCommit：终态提交被卡在闸门上时，Wait 已经返回（运行
// 本身结束了），WaitRecorded 仍必须等待；放行后它返回并带来已落盘的终态。
func TestQueueWaitRecordedWaitsForCommit(t *testing.T) {
	const reply = "后台完成并记录终态"
	store := newSettleStore()
	store.gateOn = true
	a := durableTextAgent(t, store, reply)

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-wait-recorded"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("终态提交没有到达闸门")
	}

	// 闸门还压着：运行已结束，所以 Wait 立即返回。
	res, err := h.Wait()
	if err != nil {
		t.Fatalf("运行失败: %v", err)
	}
	if got := res.Message.Text(); got != reply {
		t.Fatalf("运行结果 = %q，期望 %q", got, reply)
	}

	// WaitRecorded 不能越过记录中的提交：闸门未放行期间它不得返回。这里用一段
	// 有界的负向观察——提交被卡住，所以它根本没有完成的可能。
	got := recordedWait(h)
	select {
	case err := <-got:
		t.Fatalf("终态提交还卡在闸门上，WaitRecorded 就返回了: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(store.release)
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("WaitRecorded 返回错误: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("放行后 WaitRecorded 没有返回")
	}

	head, err := store.Latest(context.Background(), "q-wait-recorded")
	if err != nil {
		t.Fatal(err)
	}
	if head.Completion == nil || head.Completion.Kind != checkpoint.CompletionCompleted {
		t.Fatalf("终态没有落盘: %+v", head.Completion)
	}
	assertWorkReleased(t, h, "WaitRecorded 收尾")
}

// TestQueueRecordFailureVisible：记录失败必须通过 WaitRecorded 对外可见，并且不掩盖
// 运行自身的结果与错误。
func TestQueueRecordFailureVisible(t *testing.T) {
	injected := errors.New("终态写入失败")

	t.Run("运行成功但记录失败", func(t *testing.T) {
		store := newSettleStore()
		store.recordOn = true
		store.record = injected
		a := durableTextAgent(t, store, "结果仍可取回")

		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-record-fail-ok"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		q.Close()
		startPool(t, q, 1)

		res, err := h.Wait()
		if err != nil {
			t.Fatalf("运行本身应成功: %v", err)
		}
		if got := res.Message.Text(); got != "结果仍可取回" {
			t.Fatalf("Wait 结果 = %q", got)
		}
		settled, err := h.WaitRecorded()
		if !errors.Is(err, injected) {
			t.Fatalf("WaitRecorded 未暴露记录错误: %v", err)
		}
		if !strings.Contains(err.Error(), "record completion") {
			t.Fatalf("错误未说明是终态记录失败: %v", err)
		}
		if got := settled.Message.Text(); got != "结果仍可取回" {
			t.Fatalf("WaitRecorded 丢失了运行结果: %q", got)
		}
		if n := terminalCount(t, store, "q-record-fail-ok"); n != 0 {
			t.Fatalf("写入被拒绝却落盘了 %d 条终态", n)
		}
	})

	t.Run("运行失败且记录也失败", func(t *testing.T) {
		store := newSettleStore()
		store.recordOn = true
		store.record = injected
		a := durableErrAgent(t, store)

		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-record-fail-run"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		q.Close()
		startPool(t, q, 1)

		if _, err := h.Wait(); err == nil {
			t.Fatal("模型故障应让运行失败")
		}
		_, err = h.WaitRecorded()
		if !errors.Is(err, injected) {
			t.Fatalf("WaitRecorded 未暴露记录错误: %v", err)
		}
		if !strings.Contains(err.Error(), "后台任务模型故障") {
			t.Fatalf("WaitRecorded 掩盖了运行自身的错误: %v", err)
		}
	})
}

// TestQueueCancelStillRecords：Handle.Cancel 取消执行后，终态记录用的是不继承取消的
// 独立 context，已经写下本次提交边界的运行仍应记成 failed。
func TestQueueCancelStillRecords(t *testing.T) {
	store := newSettleStore()
	entered := make(chan struct{})
	release := make(chan struct{})
	a := durableGateAgent(t, store, entered, release)

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-cancel-records"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("gate 工具未开始执行")
	}
	h.Cancel()

	res, err := h.WaitRecorded()
	if err == nil {
		t.Fatal("取消后应带运行错误返回")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("错误未体现执行取消: %v", err)
	}
	if got := res.Message.Text(); got != "" {
		t.Fatalf("取消的运行不应带结果: %q", got)
	}

	head, err := store.Latest(context.Background(), "q-cancel-records")
	if err != nil {
		t.Fatal(err)
	}
	if head.Completion == nil || head.Completion.Kind != checkpoint.CompletionFailed {
		t.Fatalf("取消的运行终态不符: %+v", head.Completion)
	}
	if head.Completion.RunID != h.run.ID {
		t.Fatalf("终态归属另一个运行: %+v", head.Completion)
	}
	if head.Completion.Result != nil {
		t.Fatalf("失败运行不该有结果快照: %+v", head.Completion.Result)
	}
	if head.Completion.Err == "" {
		t.Fatal("失败终态缺少错误文本")
	}

	// 仓库自带的 Memory 在提交时忽略 context，所以"记录用的是未取消的 context"必须
	// 直接看提交那一刻的 context 状态，不能只看是否写成功。
	recordCtx, errAtCommit, _ := store.observedSettle()
	if recordCtx == nil {
		t.Fatal("没有观察到终态提交")
	}
	if errAtCommit != nil {
		t.Fatalf("记录 context 继承了执行的取消: %v", errAtCommit)
	}
}

// TestQueueRecordVisibleWithoutBoundary：取消发生在任务被消费之前，运行一步都没提交，
// 记录被"无可记录边界"拒绝——这个错误同样必须经 WaitRecorded 对外可见。
func TestQueueRecordVisibleWithoutBoundary(t *testing.T) {
	store := beginRefusingStore{ThreadCoordinator: checkpoint.NewMemory()}
	a := durableTextAgent(t, store, "不应执行")
	rq := &recordQueue{}

	h, err := EnqueueAgent(context.Background(), rq, a, "go", agent.OnThread("q-no-boundary"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	h.Cancel() // 任务还没被取走，执行 context 先结束

	_, accepted := rq.snapshot()
	settled := make(chan error, 1)
	go func() {
		_, err := h.WaitRecorded()
		settled <- err
	}()
	// 由测试自己消费这条任务，模拟 worker 迟到取任务。闭包交给 Pool 的就是同一个
	// 合并错误——队列没有别的通道把它送出来。
	if err := runQueueJob(accepted[0]); !errors.Is(err, agent.ErrNoCommittedBoundary) {
		t.Fatalf("闭包返回给 Pool 的错误 = %v，期望含 ErrNoCommittedBoundary", err)
	}

	err = <-settled
	if !errors.Is(err, agent.ErrNoCommittedBoundary) {
		t.Fatalf("无边界运行应暴露 ErrNoCommittedBoundary，实际: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("运行自身的取消错误被掩盖: %v", err)
	}
}

// runQueueJob 执行被队列接收的闭包并返回它的错误（模拟 worker 消费 recordQueue）。
func runQueueJob(job Job) error {
	if job.Run == nil {
		return errors.New("任务没有闭包")
	}
	return job.Run(context.Background())
}

// TestQueuePauseSkipsCompletion：暂停的运行正常收尾但不写终态，既有的审批恢复流程
// 不受影响。
func TestQueuePauseSkipsCompletion(t *testing.T) {
	store := newSettleStore()
	var ran atomic.Int64
	a := durablePausingAgent(t, store, &ran)
	threadID := "q-pause-skips"

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(threadID))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	if _, err := h.Wait(); err != nil {
		t.Fatalf("暂停是正常收尾: %v", err)
	}
	if !h.run.Paused() {
		t.Fatal("运行没有记下暂停结局")
	}
	if _, err := h.WaitRecorded(); err != nil {
		t.Fatalf("暂停的收尾不应报错: %v", err)
	}
	if n := terminalCount(t, store, threadID); n != 0 {
		t.Fatalf("暂停写了 %d 条终态记录", n)
	}
	if got := len(store.settleCommits()); got != 0 {
		t.Fatalf("暂停仍尝试了 %d 次终态提交", got)
	}
	head, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head.Pause == nil && head.Pending == nil {
		t.Fatal("暂停元数据丢失，线程无法恢复")
	}
	assertWorkReleased(t, h, "暂停收尾")

	// 既有审批流程照常：批准后恢复并能完成，恢复路径也不会自动写终态。
	resumed, err := a.Resume(context.Background(), threadID, agent.Allow("c1"))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	res, err := resumed.Wait()
	if err != nil {
		t.Fatalf("恢复后的运行失败: %v", err)
	}
	if got := res.Message.Text(); got != "审批之后完成" {
		t.Fatalf("恢复结果 = %q", got)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("工具执行次数 = %d，期望 1", got)
	}
	if n := terminalCount(t, store, threadID); n != 0 {
		t.Fatalf("恢复路径写了 %d 条终态记录", n)
	}
}

// TestQueueRecordContextDeadline：记录用的是独立 context——执行 context 已取消时它
// 仍可用、带约 30 秒的期限，并且在闭包返回后被释放。测试不真等 30 秒。
func TestQueueRecordContextDeadline(t *testing.T) {
	store := newSettleStore()
	entered := make(chan struct{})
	release := make(chan struct{})
	a := durableGateAgent(t, store, entered, release)

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-record-ctx"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("gate 工具未开始执行")
	}
	h.Cancel()
	assertWorkReleased(t, h, "取消后")

	if _, err := h.WaitRecorded(); err != nil && strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("记录 context 被取消或超时: %v", err)
	}
	recordCtx, errAtCommit, remaining := store.observedSettle()
	if recordCtx == nil {
		t.Fatal("没有观察到终态提交")
	}
	if errAtCommit != nil {
		t.Fatalf("记录 context 在提交时就已结束: %v", errAtCommit)
	}
	if remaining <= 20*time.Second || remaining > terminalRecordTimeout {
		t.Fatalf("记录 context 剩余期限 = %s，期望接近 %s", remaining, terminalRecordTimeout)
	}
	select {
	case <-recordCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("收尾结束后记录 context 未释放")
	}
}

// TestQueueWaitRecordedMultipleWaiters：多个等待者得到一致内容，改一份不影响另一份；
// 任务未被消费时 WaitRecorded 不驱动运行。
func TestQueueWaitRecordedMultipleWaiters(t *testing.T) {
	t.Run("三个等待者", func(t *testing.T) {
		store := newSettleStore()
		a := durableTextAgent(t, store, "共享的结果")

		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-multi-waiters"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		q.Close()
		startPool(t, q, 1)
		if _, err := h.Wait(); err != nil {
			t.Fatalf("运行失败: %v", err)
		}

		const waiters = 3
		results := make([]core.Result, waiters)
		errs := make([]error, waiters)
		var wg sync.WaitGroup
		for i := 0; i < waiters; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = h.WaitRecorded()
			}(i)
		}
		wg.Wait()

		for i := 0; i < waiters; i++ {
			if errs[i] != nil {
				t.Fatalf("等待者 %d 报错: %v", i, errs[i])
			}
			if got := results[i].Message.Text(); got != "共享的结果" {
				t.Fatalf("等待者 %d 的结果 = %q", i, got)
			}
		}
		// 改一份不污染其他份。
		results[0].Message.Parts[0] = core.Text{Text: "被等待者改写了"}
		for i := 1; i < waiters; i++ {
			if got := results[i].Message.Text(); got != "共享的结果" {
				t.Fatalf("等待者 %d 的结果被改写污染: %q", i, got)
			}
		}
		if again := results[2].Message.Text(); again != "共享的结果" {
			t.Fatalf("重复读取被污染: %q", again)
		}
	})

	t.Run("改过 Wait 返回值再等记录", func(t *testing.T) {
		store := newSettleStore()
		a := durableTextAgent(t, store, "原始结果")

		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("q-wait-mutation"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		q.Close()
		startPool(t, q, 1)

		res, err := h.Wait()
		if err != nil {
			t.Fatalf("运行失败: %v", err)
		}
		// 先确定收尾已经记下结果（此时才允许改写），再看第二次读取。
		if _, err := h.WaitRecorded(); err != nil {
			t.Fatalf("WaitRecorded 报错: %v", err)
		}
		// Wait 给出的是运行自己的结果对象；调用方就地改写它，不应影响收尾结果。
		res.Message.Parts[0] = core.Text{Text: "调用方改写"}
		settled, err := h.WaitRecorded()
		if err != nil {
			t.Fatalf("WaitRecorded 报错: %v", err)
		}
		if got := settled.Message.Text(); got != "原始结果" {
			t.Fatalf("收尾结果被 Wait 返回值的改写污染: %q", got)
		}
	})

	t.Run("未消费时不驱动运行", func(t *testing.T) {
		var calls atomic.Int32
		store := newSettleStore()
		a, err := agent.New(
			agent.WithCheckpointer(store),
			agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
				calls.Add(1)
				return mock.Text("未被消费")
			})),
		)
		if err != nil {
			t.Fatal(err)
		}
		rq := &recordQueue{} // 接收任务但没有 worker

		h, err := EnqueueAgent(context.Background(), rq, a, "go", agent.OnThread("q-unconsumed"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		_, accepted := rq.snapshot()

		got := recordedWait(h)
		select {
		case err := <-got:
			t.Fatalf("任务未被消费，WaitRecorded 却返回了: %v", err)
		case <-time.After(300 * time.Millisecond):
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("WaitRecorded 驱动了运行，模型调用 %d 次", n)
		}

		if err := runQueueJob(accepted[0]); err != nil {
			t.Fatalf("消费任务失败: %v", err)
		}
		select {
		case err := <-got:
			if err != nil {
				t.Fatalf("WaitRecorded 返回错误: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("消费后 WaitRecorded 没有返回")
		}
	})
}

// TestQueueWaitRecordedIsolatedFromWaitEdits：收尾结果必须取自运行自己冻结的结局，
// 不能是 Wait 递出去的那个活对象。两种时序都要挡住：提交期间被改写、worker 消费前被改写。
func TestQueueWaitRecordedIsolatedFromWaitEdits(t *testing.T) {
	const original = "原始结果"
	const edited = "调用方改写"

	// 断言收尾结果与已落盘的终态文本一致，都仍是运行冻结的内容。
	assertBothOriginal := func(t *testing.T, settled core.Result, store *settleStore, threadID string) {
		t.Helper()
		if got := settled.Message.Text(); got != original {
			t.Fatalf("WaitRecorded 返回 %q，期望 %q（收尾结果取自被改写的活对象）", got, original)
		}
		head, err := store.Latest(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if head.Completion == nil || head.Completion.Result == nil {
			t.Fatalf("终态结果缺失: %+v", head.Completion)
		}
		if got := head.Completion.Result.Message.Text(); got != original {
			t.Fatalf("已落盘终态 = %q，与冻结结果不一致", got)
		}
	}

	t.Run("提交期间被改写", func(t *testing.T) {
		threadID := "q-isolate-during-commit"
		store := newSettleStore()
		store.gateOn = true
		a := durableTextAgent(t, store, original)

		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(threadID))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		q.Close()
		startPool(t, q, 1)

		select {
		case <-store.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("终态提交没有到达闸门")
		}
		// 运行本身已经结束，Wait 立即可返回；调用方就地改写它拿到的结果。
		res, err := h.Wait()
		if err != nil {
			t.Fatalf("运行失败: %v", err)
		}
		res.Message.Parts[0] = core.Text{Text: edited}

		close(store.release)
		settled, err := h.WaitRecorded()
		if err != nil {
			t.Fatalf("WaitRecorded 报错: %v", err)
		}
		assertBothOriginal(t, settled, store, threadID)
	})

	t.Run("worker 消费前被改写", func(t *testing.T) {
		threadID := "q-isolate-before-consume"
		store := newSettleStore()
		a := durableTextAgent(t, store, original)
		rq := &recordQueue{} // 接收任务但没有 worker

		h, err := EnqueueAgent(context.Background(), rq, a, "go", agent.OnThread(threadID))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		_, accepted := rq.snapshot()

		// 调用方先自己把运行驱动到结束，再改写拿到的结果，然后才让 worker 消费。
		res, err := h.Wait()
		if err != nil {
			t.Fatalf("运行失败: %v", err)
		}
		res.Message.Parts[0] = core.Text{Text: edited}

		if err := runQueueJob(accepted[0]); err != nil {
			t.Fatalf("消费任务失败: %v", err)
		}
		settled, err := h.WaitRecorded()
		if err != nil {
			t.Fatalf("WaitRecorded 报错: %v", err)
		}
		assertBothOriginal(t, settled, store, threadID)
	})
}
