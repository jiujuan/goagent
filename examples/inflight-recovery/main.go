// Command inflight-recovery 演示在途批次的崩溃恢复：第一次运行的批次执行前
// 提交已经落盘，进程随即消失（之后的提交全部失败），头部停在这个边界上。
// 重新打开同一目录后，RunStatus 报 InFlight 而不是 Paused；Agent.Resume 按
// 台账执行两条 pending 调用、结算这一批，再进入下一步，不会重新询问模型
// “这一批要做什么”。
//
// 全程 mock 模型与本机临时目录，无网络；退出前删除临时目录：
//
//	go run ./examples/inflight-recovery
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

var errProcessGone = errors.New("模拟进程消失：之后的提交全部失败")

// dyingStore lets the first commit land, then reports every later commit as
// failed. The first Commit still returns the receipt, together with the error:
// the boundary was made visible, but the process died before its caller could
// observe the result.
type dyingStore struct {
	checkpoint.Durable

	mu      sync.Mutex
	commits int
	dead    bool
}

func (s *dyingStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return checkpoint.CommitReceipt{}, errProcessGone
	}
	s.commits++
	last := s.commits == 1
	if last {
		s.dead = true
	}
	s.mu.Unlock()

	receipt, err := s.Durable.Commit(ctx, req)
	if err != nil {
		return receipt, err
	}
	if last {
		return receipt, errProcessGone
	}
	return receipt, nil
}

type recordingLookup struct {
	mu    sync.Mutex
	calls []string
}

func (l *recordingLookup) Name() string        { return "lookup" }
func (l *recordingLookup) Description() string { return "查询一行数据" }
func (l *recordingLookup) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}`)
}

func (l *recordingLookup) Call(tc *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.calls = append(l.calls, tc.CallID)
	l.mu.Unlock()
	return tool.TextResult("value-" + in.Key), nil
}

func (l *recordingLookup) invocations() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

func main() {
	ctx := context.Background()
	const threadID = "inflight-recovery-example"

	dir, err := os.MkdirTemp("", "goagent-inflight-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	file, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	lookup := &recordingLookup{}
	firstModel := mock.New("first-process", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return batch()
	})
	first, err := agent.New(
		agent.WithModel(firstModel),
		agent.WithTools(lookup),
		agent.WithCheckpointer(&dyingStore{Durable: file}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("第一次运行：模型规划两条工具调用，进程在批次执行前消失。")
	run := first.Stream(ctx, "查两行数据", agent.OnThread(threadID))
	var firstErr error
	for _, err := range run.Iter() {
		if err != nil {
			firstErr = err
		}
	}
	if _, err := run.Wait(); err != nil {
		firstErr = err
	}
	if !errors.Is(firstErr, errProcessGone) {
		log.Fatalf("第一次运行错误 = %v，want 模拟的进程消失", firstErr)
	}
	if got := lookup.invocations(); len(got) != 0 {
		log.Fatalf("进程消失前执行了 %d 次工具，want 0", len(got))
	}
	head, err := file.Latest(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	if head == nil || head.InFlight == nil {
		log.Fatalf("落盘头部 = %+v，want 在途边界", head)
	}
	fmt.Printf("第一次运行结束：%v；工具执行 0 次\n", firstErr)
	fmt.Printf("落盘的头部：in-flight=true calls=%d\n", len(head.InFlight.Calls))

	// 新进程：重新打开同一目录，换一个只回答终稿的模型。
	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	recoveryCalls := 0
	recoveryModel := mock.New("second-process", func(*llm.Request) *llm.Response {
		recoveryCalls++
		return mock.Text("完成")
	})
	second, err := agent.New(
		agent.WithModel(recoveryModel),
		agent.WithTools(lookup),
		agent.WithCheckpointer(reopened),
	)
	if err != nil {
		log.Fatal(err)
	}

	status, err := second.RunStatus(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("重开目录后的 RunStatus：exists=%v in-flight=%v paused=%v\n",
		status.Exists, status.InFlight, status.Paused)
	if !status.Exists || !status.InFlight || status.Paused || status.Latest == nil || status.Latest.InFlight == nil {
		log.Fatalf("RunStatus = %+v，want 头部停在在途边界", status)
	}

	resumed, err := second.Resume(ctx, threadID)
	if err != nil {
		log.Fatalf("Resume: %v", err)
	}
	var recovered *core.ToolBatchRecovered
	for ev, err := range resumed.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		if e, ok := ev.(core.ToolBatchRecovered); ok {
			copied := e
			recovered = &copied
		}
	}
	result, err := resumed.Wait()
	if err != nil {
		log.Fatalf("恢复运行失败：%v", err)
	}
	if recovered == nil || recovered.Executed != 2 || recovered.Replayed != 0 ||
		recovered.Unknown != 0 || recovered.Rejected != 0 {
		log.Fatalf("ToolBatchRecovered = %+v，want executed=2", recovered)
	}
	fmt.Printf("恢复：executed=%d replayed=%d unknown=%d rejected=%d\n",
		recovered.Executed, recovered.Replayed, recovered.Unknown, recovered.Rejected)

	got := lookup.invocations()
	seen := make(map[string]int, len(got))
	for _, id := range got {
		seen[id]++
	}
	if len(got) != 2 || seen["c1"] != 1 || seen["c2"] != 1 {
		log.Fatalf("工具调用 = %v，want 每个调用恰好一次", got)
	}
	fmt.Println("工具调用（并发执行，顺序不固定）：", got)
	if recoveryCalls != 1 {
		log.Fatalf("恢复进程的模型调用 = %d 次，want 只问下一步（1 次）", recoveryCalls)
	}
	fmt.Printf("恢复进程的模型调用：%d 次（没有重新规划这一批）\n", recoveryCalls)

	head, err = reopened.Latest(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	if head.InFlight != nil {
		log.Fatalf("最终头部仍带在途批次：%+v", head.InFlight)
	}
	if len(head.State.Messages) == 0 {
		log.Fatal("最终头部没有消息")
	}
	var toolMessages []core.Message
	for _, m := range head.State.Messages {
		if m.Role == core.RoleTool {
			toolMessages = append(toolMessages, m)
		}
	}
	if len(toolMessages) != 1 {
		log.Fatalf("工具消息 = %d 条，want 恰好一条批次结果", len(toolMessages))
	}
	last := toolMessages[0]
	if len(last.Parts) != 2 {
		log.Fatalf("最终工具消息 = %+v，want 两条结果", last)
	}
	for i, want := range []string{"c1", "c2"} {
		tr, ok := last.Parts[i].(core.ToolResult)
		if !ok || tr.CallID != want {
			log.Fatalf("结果 %d = %+v，want %s", i, last.Parts[i], want)
		}
	}
	fmt.Println("最终头部无在途批次，工具结果按 c1、c2 顺序各一条。")
	fmt.Println("最终回答：", result.Message.Text())
}

func batch() *llm.Response {
	parts := make([]core.Part, 2)
	for i, c := range []core.ToolCall{
		{ID: "c1", Name: "lookup", Args: []byte(`{"key":"a"}`)},
		{ID: "c2", Name: "lookup", Args: []byte(`{"key":"b"}`)},
	} {
		parts[i] = c
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: llm.StopToolUse,
	}
}
