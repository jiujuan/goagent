// Command runtime-hardening 演示运行时并发加固的四条可见行为，全程离线：
//
//  1. 任务树：根 Task 与 AsTool 子 Task 都有 TaskStarted/TaskDone。子运行有
//     自己的 topic，父运行的事件流里看不到它；示例用 checkpointer 在 BeginRun
//     时订阅子运行 topic，那时 TaskStarted 还没发布。
//  2. 预算：MaxUnits=1 时第二次 delegate 被拒，错误作为工具结果交回模型。
//  3. 失败取消：Parallel 中一个分支失败后，另一个在途分支收到取消，流上出现
//     BranchCanceled；默认策略是 CancelSiblings。
//  4. 同线程排队：同一个 Session 的两次 Send 先进先出，第二次的模型请求能看到
//     第一次的回答，模型调用峰值不超过 1。
//
// 只使用 mock 模型、内存存储和 channel；不访问网络，不用固定 sleep 制造顺序。
//
//	go run ./examples/runtime-hardening
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	ctx := context.Background()
	demoTaskTree(ctx)
	fmt.Println()
	demoParallelCancel(ctx)
	fmt.Println()
	demoSessionFIFO(ctx)
}

// childRunTap subscribes to a run's topic during BeginRun, which happens before
// the run publishes TaskStarted. AsTool never returns the child's Run handle, so
// this is how the child's own event stream can be observed from the outside.
type childRunTap struct {
	checkpoint.ThreadCoordinator
	bus *bus.Bus

	mu      sync.Mutex
	ch      <-chan core.Event
	cancel  func()
	events  []core.Event
	stopped chan struct{}
}

func (t *childRunTap) BeginRun(ctx context.Context, req checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	t.mu.Lock()
	if t.ch == nil {
		t.ch, t.cancel = t.bus.Subscribe(bus.Topic(req.Owner), bus.Lossless)
		go t.collect(t.ch)
	}
	t.mu.Unlock()
	return t.ThreadCoordinator.BeginRun(ctx, req)
}

func (t *childRunTap) collect(ch <-chan core.Event) {
	for ev := range ch {
		t.mu.Lock()
		t.events = append(t.events, ev)
		t.mu.Unlock()
	}
	close(t.stopped)
}

func (t *childRunTap) drain() []core.Event {
	t.close()
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]core.Event(nil), t.events...)
}

func (t *childRunTap) close() {
	t.mu.Lock()
	cancel := t.cancel
	t.cancel = nil
	t.mu.Unlock()
	if cancel != nil {
		cancel()
		<-t.stopped
	}
}

func demoTaskTree(ctx context.Context) {
	fmt.Println("== 1. 根 Task、AsTool 子 Task 与任务树预算 ==")

	sharedBus := bus.New()
	tap := &childRunTap{ThreadCoordinator: checkpoint.NewMemory(), bus: sharedBus, stopped: make(chan struct{})}
	defer tap.close()

	child, err := agent.New(
		agent.WithName("worker"),
		agent.WithModel(mock.New("child", func(*llm.Request) *llm.Response {
			return mock.Text("child finished")
		})),
		agent.WithBus(sharedBus),
		agent.WithCheckpointer(tap),
	)
	if err != nil {
		exit(err)
	}

	calls := 0
	parent, err := agent.New(
		agent.WithName("root"),
		agent.WithModel(mock.New("parent", func(*llm.Request) *llm.Response {
			calls++
			switch calls {
			case 1:
				return mock.CallTool("delegate-1", "delegate", `{"task":"first"}`)
			case 2:
				return mock.CallTool("delegate-2", "delegate", `{"task":"second"}`)
			default:
				return mock.Text("root finished")
			}
		})),
		agent.WithTools(agent.AsTool(child, "delegate", "delegate a task")),
		agent.WithTaskBudget(agent.TaskBudget{MaxUnits: 1}),
		agent.WithBus(sharedBus),
	)
	if err != nil {
		exit(err)
	}

	run := parent.Stream(ctx, "start")
	var parentEvents []core.Event
	for ev := range run.Iter() {
		parentEvents = append(parentEvents, ev)
	}
	if _, err := run.Wait(); err != nil {
		exit(err)
	}

	var rootTaskID string
	for _, ev := range parentEvents {
		switch e := ev.(type) {
		case core.TaskStarted:
			rootTaskID = e.TaskID
			fmt.Printf("root TaskStarted: kind=%s depth=%d parent=none\n", e.Kind, e.Depth)
		case core.ToolDone:
			if !e.Result.IsError {
				fmt.Printf("root ToolDone %s: call=%s is_error=false\n", e.Result.Name, e.Result.CallID)
				continue
			}
			prefix, _, _ := strings.Cut(toolText(e.Result), " at task ")
			fmt.Printf("root ToolDone %s: call=%s is_error=true error=%q\n", e.Result.Name, e.Result.CallID, prefix)
		case core.TaskDone:
			fmt.Printf("root TaskDone: kind=%s status=%s\n", e.Kind, e.Status)
		}
	}

	for _, ev := range tap.drain() {
		switch e := ev.(type) {
		case core.TaskStarted:
			fmt.Printf("child TaskStarted (child topic): kind=%s depth=%d parent=root:%v\n",
				e.Kind, e.Depth, e.ParentID == rootTaskID)
		case core.TaskDone:
			fmt.Printf("child TaskDone (child topic): kind=%s status=%s\n", e.Kind, e.Status)
		}
	}
}

var errBranchA = errors.New("branch a failed on purpose")

// branchAFailure waits until the sibling branch is actually in flight, then
// fails. That makes the cancellation deterministic without a timer.
type branchAFailure struct {
	agent.BaseMiddleware
	siblingInFlight <-chan struct{}
}

func (m branchAFailure) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	<-m.siblingInFlight
	return core.Directive{}, errBranchA
}

func demoParallelCancel(ctx context.Context) {
	fmt.Println("== 2. Parallel 一个分支失败，另一个收到 BranchCanceled ==")

	siblingInFlight := make(chan struct{})
	var once sync.Once

	failing, err := agent.New(
		agent.WithName("a-fails"),
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response {
			return mock.Text("unused")
		})),
		agent.WithMiddleware(branchAFailure{siblingInFlight: siblingInFlight}),
	)
	if err != nil {
		exit(err)
	}

	waitingTool := tool.New("wait_for_cancel", "wait until the branch context is canceled",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			once.Do(func() { close(siblingInFlight) })
			<-tctx.Done()
			return "", tctx.Err()
		})
	waiting, err := agent.New(
		agent.WithName("b-waits"),
		agent.WithTools(waitingTool),
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response {
			return mock.CallTool("b-call", "wait_for_cancel", `{}`)
		})),
	)
	if err != nil {
		exit(err)
	}

	run := agent.Parallel("fanout", failing, waiting).Stream(ctx, "run both")
	var events []core.Event
	for ev := range run.Iter() {
		events = append(events, ev)
	}
	_, runErr := run.Wait()

	for _, ev := range events {
		switch e := ev.(type) {
		case core.TaskStarted:
			fmt.Printf("TaskStarted: kind=%s depth=%d\n", e.Kind, e.Depth)
		case core.TaskDone:
			fmt.Printf("TaskDone: kind=%s status=%s\n", e.Kind, e.Status)
		case core.BranchCanceled:
			fmt.Printf("BranchCanceled: branch=%s reason=%v\n", e.Name, e.Reason)
		}
	}
	fmt.Printf("parallel run error: %v\n", runErr)
}

type sendOutcome struct {
	answer string
	err    error
}

func demoSessionFIFO(ctx context.Context) {
	fmt.Println("== 3. 同一个 Session 的两次 Send 先进先出 ==")

	const threadID = "fifo-demo"
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondHistory := make(chan []core.Message, 1)
	var enteredOnce sync.Once
	var calls, inFlight, peak atomic.Int32

	model := mock.New("session", func(req *llm.Request) *llm.Response {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			seen := peak.Load()
			if current <= seen || peak.CompareAndSwap(seen, current) {
				break
			}
		}
		if calls.Add(1) == 1 {
			enteredOnce.Do(func() { close(firstEntered) })
			<-releaseFirst
			return mock.Text("first answer")
		}
		secondHistory <- append([]core.Message(nil), req.Messages...)
		return mock.Text("second answer")
	})

	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		exit(err)
	}
	sess, err := a.OpenSession(ctx, threadID)
	if err != nil {
		exit(err)
	}

	firstDone := make(chan sendOutcome, 1)
	go func() {
		res, sendErr := sess.Send(ctx, "first question")
		firstDone <- sendOutcome{answer: res.Result.Message.Text(), err: sendErr}
	}()
	<-firstEntered

	secondDone := make(chan sendOutcome, 1)
	go func() {
		res, sendErr := sess.Send(ctx, "second question")
		secondDone <- sendOutcome{answer: res.Result.Message.Text(), err: sendErr}
	}()

	close(releaseFirst)
	first := <-firstDone
	if first.err != nil {
		exit(first.err)
	}
	second := <-secondDone
	if second.err != nil {
		exit(second.err)
	}
	history := <-secondHistory

	fmt.Printf("first Send answer: %s\n", first.answer)
	fmt.Printf("second Send answer: %s\n", second.answer)
	fmt.Printf("second request saw first answer: %v\n", hasText(history, "first answer"))
	fmt.Printf("peak concurrent model calls: %d\n", peak.Load())
}

func toolText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func hasText(msgs []core.Message, want string) bool {
	for _, m := range msgs {
		if m.Text() == want {
			return true
		}
	}
	return false
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
