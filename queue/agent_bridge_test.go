package queue

// 本文件是 queue 包的内部测试：BC-01 要检查 EnqueueAgent 派生的执行 context
// 是否在约定路径上释放。派生 context 属于队列内部，所以这里直接读 Handle 的
// 非导出字段，不依赖 Go 并未提供的 context 反查能力。

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// --- 夹具 -------------------------------------------------------------------

// recordQueue 记录 Enqueue 的调用情况：fail 非空时模拟入队失败（后端不可用、
// 队列已满等），jobs 只保存真正被接收的任务。
type recordQueue struct {
	mu       sync.Mutex
	fail     error
	attempts int
	jobs     []Job
}

func (q *recordQueue) Enqueue(_ context.Context, job Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.attempts++
	if q.fail != nil {
		return q.fail
	}
	q.jobs = append(q.jobs, job)
	return nil
}

func (q *recordQueue) snapshot() (attempts int, accepted []Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.attempts, append([]Job(nil), q.jobs...)
}

// textAgent 用 mock 模型一次性回答，代表正常完成的后台运行。
func textAgent(t *testing.T, reply string) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text(reply)
	})))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// errModel 让运行以失败结束。
type errModel struct{}

func (errModel) Name() string { return "err" }
func (errModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, errors.New("后台任务模型故障"))
	}
}

// gateToolAgent 提供一个等待自身 context 的工具：模型先请求该工具，运行因此停在
// 工具调用里，直到 context 取消或测试放行。entered 在工具开始时关闭一次。
func gateToolAgent(t *testing.T, entered chan struct{}, release <-chan struct{}) *agent.Agent {
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
	gate := tool.New("gate", "阻塞直到 context 取消或测试放行", func(ctx *tool.Context, _ struct{}) (string, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "released", nil
		}
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(gate),
		// 有限工具超时：取消之后框架等待工具的时间受 ADR-0029 约束。
		agent.WithToolTimeout(2*time.Second),
		agent.WithMaxTurns(4),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// approvalGate 把首个未批准的工具调用变成 HITL 暂停，用于覆盖"暂停收尾"。
type approvalGate struct{ agent.BaseMiddleware }

func (approvalGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "需要审批"}, nil
}

// pausingAgent 的工具本身立即返回，但审批闸门会拦下它，ran 记录工具真正执行的次数。
func pausingAgent(t *testing.T) (*agent.Agent, *atomic.Int64) {
	t.Helper()
	var ran atomic.Int64
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
	a, err := agent.New(agent.WithModel(model), agent.WithTools(w), agent.WithMiddleware(approvalGate{}))
	if err != nil {
		t.Fatal(err)
	}
	return a, &ran
}

// terminalEvent 排空 Wait 之后仍缓冲着的订阅事件，返回其中最后出现的终态事件。
// 终态由 drive 的同一个 goroutine 发布，且先于 done 关闭，所以 Wait 返回后排空即可见。
func terminalEvent(ch <-chan core.Event) (core.Event, bool) {
	var last core.Event
	var found bool
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return last, found
			}
			switch ev.(type) {
			case core.RunDone, core.RunFailed, core.Interrupted:
				last, found = ev, true
			}
		default:
			return last, found
		}
	}
}

// assertTerminal 断言运行的终态事件类型。
func assertTerminal[T core.Event](t *testing.T, ch <-chan core.Event, want string) {
	t.Helper()
	ev, ok := terminalEvent(ch)
	if !ok {
		t.Fatalf("未观察到 %s 终态事件", want)
	}
	if _, matches := ev.(T); !matches {
		t.Fatalf("终态事件 = %T，期望 %s", ev, want)
	}
}

// assertWorkReleased 检查队列派生的执行 context 已释放（Handle.workDone 关闭）。
func assertWorkReleased(t *testing.T, h *Handle, path string) {
	t.Helper()
	select {
	case <-h.workDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s：执行 context 未释放", path)
	}
}

// assertWorkAlive 检查执行 context 未被无关的取消结束。
func assertWorkAlive(t *testing.T, h *Handle, path string) {
	t.Helper()
	select {
	case <-h.workDone:
		t.Fatalf("%s：执行 context 不应结束", path)
	default:
	}
}

// startPool 启动 worker，并在测试结束时停止该 pool 的取任务 context。
func startPool(t *testing.T, q *MemQueue, concurrency int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go NewPool(q, concurrency).Run(ctx)
}

// --- V1：context 归属与释放 -------------------------------------------------

// TestQueueCallerCancelDoesNotStopRun：提交方的 ctx 只控制入队。任务已经交给队列
// 之后，提交方断线（取消）不得中止后台运行。
func TestQueueCallerCancelDoesNotStopRun(t *testing.T) {
	const reply = "提交方已断线，后台仍完成"
	a := textAgent(t, reply)
	q := NewMemQueue(4)

	submitCtx, cancelSubmit := context.WithCancel(context.Background())
	h, err := EnqueueAgent(submitCtx, q, a, "go", agent.OnThread("t-caller-cancel"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	cancelSubmit() // 断线发生在入队成功之后

	assertWorkAlive(t, h, "提交方取消")

	q.Close()
	startPool(t, q, 1)

	res, err := h.Wait()
	if err != nil {
		t.Fatalf("提交方取消后后台运行失败: %v", err)
	}
	if got := res.Message.Text(); got != reply {
		t.Fatalf("后台结果 = %q，期望 %q", got, reply)
	}
	assertWorkReleased(t, h, "闭包正常收尾")
}

// TestQueueEnqueueFailsWithCanceledCaller：入口 ctx 已取消时 EnqueueAgent 直接失败，
// 不得把任务交给队列。
func TestQueueEnqueueFailsWithCanceledCaller(t *testing.T) {
	a := textAgent(t, "不应执行")
	rq := &recordQueue{}

	submitCtx, cancelSubmit := context.WithCancel(context.Background())
	cancelSubmit()

	h, err := EnqueueAgent(submitCtx, rq, a, "go", agent.OnThread("t-entry-canceled"))
	if err == nil {
		t.Fatal("入口 ctx 已取消，EnqueueAgent 必须返回错误")
	}
	if h != nil {
		t.Fatal("入队失败不得返回 Handle")
	}
	if attempts, accepted := rq.snapshot(); attempts != 0 || len(accepted) != 0 {
		t.Fatalf("已取消的提交不应触达队列：attempts=%d accepted=%d", attempts, len(accepted))
	}
}

// TestQueueEnqueueFailureReleasesContext：队列拒绝任务时，已经派生的执行 context
// 必须释放，且不会有任务被接收。
func TestQueueEnqueueFailureReleasesContext(t *testing.T) {
	a := textAgent(t, "不应执行")
	rq := &recordQueue{fail: errors.New("队列后端不可用")}

	h, err := submitAgentRun(context.Background(), rq, a, "go", agent.OnThread("t-enqueue-fail"))
	if err == nil {
		t.Fatal("入队失败必须返回错误")
	}
	if !strings.Contains(err.Error(), "队列后端不可用") {
		t.Fatalf("错误未保留队列原因: %v", err)
	}
	assertWorkReleased(t, h, "入队失败")

	if _, accepted := rq.snapshot(); len(accepted) != 0 {
		t.Fatalf("入队失败不应留下已接收任务: %d", len(accepted))
	}
}

// TestQueueHandleCancelStops：Handle.Cancel 显式中止执行，同时释放派生 context——
// run.Cancel 只取消其子 context，不会向上取消 workCtx，所以两层都要由它负责。
func TestQueueHandleCancelStops(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	a := gateToolAgent(t, entered, release)
	q := NewMemQueue(4)

	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("t-handle-cancel"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	events, unsub := h.Events(bus.Lossless)
	defer unsub()

	q.Close()
	startPool(t, q, 1)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("gate 工具未开始执行")
	}

	h.Cancel()

	if _, err := h.Wait(); err == nil {
		t.Fatal("显式取消后运行应以错误结束")
	}
	assertTerminal[core.RunFailed](t, events, "RunFailed")
	assertWorkReleased(t, h, "Handle.Cancel")

	// 重复取消无副作用：结果与已释放的 context 都不变。
	h.Cancel()
	if _, err := h.Wait(); err == nil {
		t.Fatal("重复 Cancel 改变了运行结果")
	}
}

// TestQueueHandleCancelReleasesUnstartedJob：任务还没被 worker 取走时，闭包不会
// 运行，Handle.Cancel 必须自己释放执行 context。
func TestQueueHandleCancelReleasesUnstartedJob(t *testing.T) {
	a := textAgent(t, "不应执行")
	rq := &recordQueue{} // 接收任务但没有 Pool 消费

	h, err := EnqueueAgent(context.Background(), rq, a, "go", agent.OnThread("t-cancel-unstarted"))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	assertWorkAlive(t, h, "取消前")

	h.Cancel()
	assertWorkReleased(t, h, "Handle.Cancel（任务未被消费）")

	if _, accepted := rq.snapshot(); len(accepted) != 1 {
		t.Fatalf("任务应仍由队列持有: %d", len(accepted))
	}
}

// TestQueueContextReleasedAfterSettlement：成功、失败、暂停三种收尾都由闭包 defer
// 释放执行 context，之后的 Cancel 不产生新影响。
func TestQueueContextReleasedAfterSettlement(t *testing.T) {
	t.Run("成功收尾", func(t *testing.T) {
		a := textAgent(t, "正常完成")
		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("t-settle-ok"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		events, unsub := h.Events(bus.Lossless)
		defer unsub()
		q.Close()
		startPool(t, q, 1)

		if _, err := h.Wait(); err != nil {
			t.Fatalf("运行失败: %v", err)
		}
		assertTerminal[core.RunDone](t, events, "RunDone")
		assertWorkReleased(t, h, "成功收尾")
		h.Cancel()
	})

	t.Run("失败收尾", func(t *testing.T) {
		a, err := agent.New(agent.WithModel(errModel{}))
		if err != nil {
			t.Fatal(err)
		}
		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("t-settle-fail"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		events, unsub := h.Events(bus.Lossless)
		defer unsub()
		q.Close()
		startPool(t, q, 1)

		if _, err := h.Wait(); err == nil {
			t.Fatal("模型故障应返回错误")
		}
		assertTerminal[core.RunFailed](t, events, "RunFailed")
		assertWorkReleased(t, h, "失败收尾")
		h.Cancel()
	})

	t.Run("暂停收尾", func(t *testing.T) {
		a, ran := pausingAgent(t)
		q := NewMemQueue(4)
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread("t-settle-pause"))
		if err != nil {
			t.Fatalf("EnqueueAgent: %v", err)
		}
		events, unsub := h.Events(bus.Lossless)
		defer unsub()
		q.Close()
		startPool(t, q, 1)

		if _, err := h.Wait(); err != nil {
			t.Fatalf("暂停是正常收尾，不应报错: %v", err)
		}
		assertTerminal[core.Interrupted](t, events, "Interrupted")
		if got := ran.Load(); got != 0 {
			t.Fatalf("审批闸门未生效，工具执行了 %d 次", got)
		}
		assertWorkReleased(t, h, "暂停收尾")
		h.Cancel()
	})
}
