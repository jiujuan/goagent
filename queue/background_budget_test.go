package queue

// BC-05（ADR-0033 D3，V2）：后台路径上的治理边界。这些用例只证明"框架会结束等待"，
// 不宣称整次运行有硬期限，也不宣称被放弃的工具 goroutine 已被杀死。

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// --- 夹具 -------------------------------------------------------------------

// countingToolModel 在有工具可用时不断调用它（收尾模式下工具被撤下，于是改用文本回答），
// 并统计模型被调用的次数。
func countingToolModel(name string) (llm.Model, *atomic.Int32) {
	var calls atomic.Int32
	m := mock.New("tool-caller", func(req *llm.Request) *llm.Response {
		n := int(calls.Add(1))
		if len(req.Tools) > 0 {
			return mock.CallTool(fmt.Sprintf("c%d", n), name, `{}`)
		}
		return mock.Text("收尾回答")
	})
	return m, &calls
}

func bgStore() checkpoint.Checkpointer { return checkpoint.NewMemory() }

func bgAgent(t *testing.T, opts ...agent.Option) *agent.Agent {
	t.Helper()
	opts = append([]agent.Option{agent.WithCheckpointer(bgStore())}, opts...)
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func paidTool() tool.Tool {
	return tool.New("fetch", "付费工具", func(_ *tool.Context, _ struct{}) (string, error) {
		return "ok", nil
	})
}

func heldTool(entered chan struct{}, release <-chan struct{}, waited *atomic.Bool) tool.Tool {
	var once sync.Once
	return tool.New("hold", "阻塞直到测试放行", func(ctx *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			// 记明这一次是真的被压住后放行，不是工具自己立刻返回。
			waited.Store(true)
			return "released", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
}

func ignoresContextTool(delay time.Duration, started chan struct{}) tool.Tool {
	var once sync.Once
	return tool.New("stuck", "不看 context，自己睡到点", func(_ *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(started) })
		time.Sleep(delay)
		return "too late", nil
	})
}

func toolCallModel(name string) llm.Model {
	var mu sync.Mutex
	n := 0
	return mock.New("caller", func(*llm.Request) *llm.Response {
		mu.Lock()
		n++
		id := fmt.Sprintf("c%d", n)
		mu.Unlock()
		return mock.CallTool(id, name, `{}`)
	})
}

func answerAfterToolModel(name string, text string) llm.Model {
	return mock.New("answerer", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text(text)
		}
		return mock.CallTool("c1", name, `{}`)
	})
}

func toolFreeModel(text string) llm.Model {
	return mock.New("talk", func(*llm.Request) *llm.Response { return mock.Text(text) })
}

// --- V2：后台路径上的治理边界 ------------------------------------------------

// runBackground 入队并跑完一个后台运行，返回收尾结果与线程状态。
func runBackground(t *testing.T, a *agent.Agent, threadID string) (core.Result, error, agent.RunStatus) {
	t.Helper()
	ctx := context.Background()
	q := NewMemQueue(4)
	h, err := EnqueueAgent(ctx, q, a, "go", agent.OnThread(threadID))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	type settled struct {
		res core.Result
		err error
	}
	done := make(chan settled, 1)
	go func() {
		res, err := h.WaitRecorded()
		done <- settled{res, err}
	}()
	select {
	case got := <-done:
		status, serr := a.RunStatus(ctx, threadID)
		if serr != nil {
			t.Fatal(serr)
		}
		return got.res, got.err, status
	case <-time.After(15 * time.Second):
		t.Fatal("后台运行没有收尾")
		return core.Result{}, nil, agent.RunStatus{}
	}
}

// TestBackgroundRunBudgetWrapsUp：注入时钟越过 MaxDuration 后，运行按正常收尾结束，
// 终态记成 completed——预算超限不是失败。
func TestBackgroundRunBudgetWrapsUp(t *testing.T) {
	const threadID = "bg-budget-wrapup"
	model, calls := countingToolModel("fetch")
	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxDuration: time.Second,
		Now:         fastClockAfter(2), // 第三次读数起就已经超出一秒
	})
	a := bgAgent(t, agent.WithModel(model), agent.WithTools(paidTool()), agent.WithMiddleware(guard))

	res, err, status := runBackground(t, a, threadID)
	if err != nil {
		t.Fatalf("预算超限应正常收尾，实际: %v", err)
	}
	if got := res.Message.Text(); got != "收尾回答" {
		t.Fatalf("结果 = %q，期望收尾回答", got)
	}
	if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionCompleted {
		t.Fatalf("终态不该是预算错误 failed: %+v", status.Completion)
	}
	if n := calls.Load(); n < 2 {
		t.Fatalf("模型调用 %d 次，期望先调工具再收尾", n)
	}
	if status.Paused {
		t.Fatal("收尾后的运行不该报暂停")
	}
}

// TestBackgroundToolTimeoutBoundsWait：配置了有限工具超时后，框架不再等待一个不看
// context 的工具；运行照常结束。这里不宣称那个工具 goroutine 已被杀死。
func TestBackgroundToolTimeoutBoundsWait(t *testing.T) {
	const threadID = "bg-tool-timeout"
	started := make(chan struct{})
	model := answerAfterToolModel("stuck", "超时后完成")
	a := bgAgent(t,
		agent.WithModel(model),
		agent.WithTools(ignoresContextTool(20*time.Second, started)),
		agent.WithToolTimeout(50*time.Millisecond),
		agent.WithMaxTurns(4),
	)

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(threadID))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("卡住的工具没有开始")
	}

	// 有限超时让框架不再等这个 20 秒的工具；没有超时，收尾要等满 20 秒。
	type settled struct {
		res core.Result
		err error
	}
	done := make(chan settled, 1)
	go func() {
		res, err := h.WaitRecorded()
		done <- settled{res, err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("运行没有正常收尾: %v", got.err)
		}
		if got2 := got.res.Message.Text(); got2 != "超时后完成" {
			t.Fatalf("结果 = %q", got2)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("框架仍在等待一个不看 context 的工具")
	}
	status, err := a.RunStatus(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionCompleted {
		t.Fatalf("终态不符: %+v", status.Completion)
	}
}

// TestBackgroundBudgetDoesNotCancelInflightTool：预算的时间上限已经越过，正在执行的工具
// 仍不会被预算打断（这条路径没有配置工具超时）。测试必须主动放行工具才能收尾。
func TestBackgroundBudgetDoesNotCancelInflightTool(t *testing.T) {
	const threadID = "bg-budget-inflight"
	entered := make(chan struct{})
	release := make(chan struct{})
	exceeded := make(chan string, 4)
	var waited atomic.Bool
	model, calls := countingToolModel("hold")
	// 时钟按"第一次模型回答之后"跨过上限：AfterModel 检查时预算越界，而这一轮的工具
	// 调用已经发出，于是形成"预算已过、工具仍在途"的状态。
	t0 := time.Now()
	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxDuration: time.Second,
		Now: func() time.Time {
			if calls.Load() >= 1 {
				return t0.Add(time.Hour)
			}
			return t0
		},
		OnExceed: func(r middleware.BudgetReport) { exceeded <- strings.Join(r.Exceeded, ",") },
	})
	a := bgAgent(t,
		agent.WithModel(model),
		agent.WithTools(heldTool(entered, release, &waited)),
		agent.WithMiddleware(guard),
		agent.WithMaxTurns(4),
	)

	q := NewMemQueue(4)
	h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(threadID))
	if err != nil {
		t.Fatalf("EnqueueAgent: %v", err)
	}
	q.Close()
	startPool(t, q, 1)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("在途工具没有开始")
	}

	// 先确认前提：时间上限确实越过了（否则下面的"没被打断"是空转）。
	var crossed string
	select {
	case crossed = <-exceeded:
	case <-time.After(5 * time.Second):
		t.Fatal("注入时钟没有让预算越过上限")
	}
	if !strings.Contains(crossed, "duration") {
		t.Fatalf("越过的资源 = %q，期望含 duration", crossed)
	}

	// 预算越过了，但工具还在执行且没有配工具超时：运行不能自行结束。
	settled := recordedWaitHandle(h)
	select {
	case <-settled:
		t.Fatal("预算打断了在途工具")
	case <-time.After(500 * time.Millisecond):
	}
	if waited.Load() {
		t.Fatal("工具在放行之前就返回了，夹具没能制造在途状态")
	}

	close(release)
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("放行后收尾失败: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("放行后运行没有收尾")
	}
	if !waited.Load() {
		t.Fatal("工具没有走到放行分支")
	}
	if n := calls.Load(); n < 2 {
		t.Fatalf("模型调用 %d 次，期望工具之后再收一次", n)
	}
	status, err := a.RunStatus(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionCompleted {
		t.Fatalf("终态不符: %+v", status.Completion)
	}
}

// TestBackgroundMaxTurnsBoundsModelLoop：轮次上限约束的是"有限、会返回"的模型循环——
// 每轮都以工具调用结束的模型被上限止住，运行以失败终态收尾。不推广到任意 Runnable
// 或阻塞 I/O。
func TestBackgroundMaxTurnsBoundsModelLoop(t *testing.T) {
	const threadID = "bg-max-turns"
	model, calls := countingToolModel("fetch")
	a := bgAgent(t, agent.WithModel(model), agent.WithTools(paidTool()), agent.WithMaxTurns(3))

	_, err, status := runBackground(t, a, threadID)
	if err == nil {
		t.Fatal("超出轮次上限应以上报错误收尾")
	}
	if !errors.Is(err, agent.ErrMaxTurnsExceeded) {
		t.Fatalf("错误 = %v，期望是轮次上限", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("模型调用 %d 次，期望等于上限 3", got)
	}
	if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionFailed {
		t.Fatalf("终态不符: %+v", status.Completion)
	}
	if status.Completion.Err == "" {
		t.Fatal("失败终态缺少错误文本")
	}
}

// recordedWaitHandle 在后台等收尾结果，只报告"是否已经返回"。
func recordedWaitHandle(h *Handle) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := h.WaitRecorded()
		ch <- err
	}()
	return ch
}

// fastClockAfter 让前 n 次读数返回起点，其后读数跳到一小时后：注入时钟跨过时间上限，
// 又不依赖真实等待。
func fastClockAfter(n int32) func() time.Time {
	var reads atomic.Int32
	t0 := time.Now()
	return func() time.Time {
		if reads.Add(1) <= n {
			return t0
		}
		return t0.Add(time.Hour)
	}
}
