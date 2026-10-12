package agent_test

// SM-03（ADR-0034 D3）：结构化运行结果。本文件覆盖边界稳定、身份冻结、无边界
// 与暂停四种结局，以及前台 Run 的既有契约不变。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func resultTextModel(name, text string) llm.Model {
	return mock.New(name, func(*llm.Request) *llm.Response { return mock.Text(text) })
}

func resultToolModel(name string, rounds int) llm.Model {
	var calls atomic.Int32
	return mock.New(name, func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			if int(calls.Add(1)) < rounds {
				return mock.CallTool("call-more", "echo", `{}`)
			}
			return mock.Text("收尾回答")
		}
		return mock.CallTool("call-1", "echo", `{}`)
	})
}

func resultEchoTool() tool.Tool {
	return tool.New("echo", "echo back", func(*tool.Context, struct{}) (string, error) {
		return "done", nil
	})
}

// TestRunDetailedBoundaryStable：A 结束后 B 推进线程 head，再读 A 的结果仍是 A 的
// 边界；改写导出的 Run.ID/ThreadID 不会重新归属这次运行。
func TestRunDetailedBoundaryStable(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "第一次回答"))
	threadID := "detailed-boundary"

	runA := a.Stream(ctx, "问题 A", agent.OnThread(threadID))
	if _, err := runA.Wait(); err != nil {
		t.Fatal(err)
	}
	first := runA.DetailedResult()
	if first.RunID != runA.ID || first.ThreadID != threadID {
		t.Fatalf("身份不符: run=%q thread=%q", first.RunID, first.ThreadID)
	}
	if first.TurnID == "" {
		t.Fatal("结果没有 TurnID")
	}
	if first.CheckpointRef == nil || first.Revision == 0 {
		t.Fatalf("没有本次提交的边界: %+v rev=%d", first.CheckpointRef, first.Revision)
	}
	head := headCheckpoint(t, base, threadID)
	if first.CheckpointRef.CheckpointID != head.ID || first.Revision != head.Revision {
		t.Fatalf("边界不是本次提交: %+v rev=%d, want %s rev=%d", first.CheckpointRef, first.Revision, head.ID, head.Revision)
	}
	if first.CheckpointRef.ThreadID != threadID {
		t.Fatalf("边界 Ref 的线程 = %q", first.CheckpointRef.ThreadID)
	}
	if first.Paused {
		t.Fatal("已完成的运行报成暂停")
	}
	if first.Result.Message.Text() != "第一次回答" {
		t.Fatalf("结果正文 = %q", first.Result.Message.Text())
	}

	// 同线程上的第二次运行推进 head。
	second, err := a.RunDetailed(ctx, "问题 B", agent.OnThread(threadID))
	if err != nil {
		t.Fatal(err)
	}
	if second.RunID == first.RunID {
		t.Fatal("两次运行共用 RunID")
	}
	if second.TurnID == first.TurnID {
		t.Fatal("新的用户输入沿用了旧 TurnID")
	}
	moved := headCheckpoint(t, base, threadID)
	if moved.ID != second.CheckpointRef.CheckpointID {
		t.Fatalf("B 的边界不是当前 head: %s vs %s", moved.ID, second.CheckpointRef.CheckpointID)
	}

	kept := runA.DetailedResult()
	if kept.CheckpointRef.CheckpointID != first.CheckpointRef.CheckpointID || kept.Revision != first.Revision {
		t.Fatalf("A 的边界随 head 移动了: %+v rev=%d", kept.CheckpointRef, kept.Revision)
	}
}

// TestRunDetailedIdentityIsFrozen：导出字段是调用方可写的，结果与终态记录都取冻结
// 下来的身份，不跟着改写。
func TestRunDetailedIdentityIsFrozen(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "第一次回答"))
	threadID := "detailed-frozen-identity"

	runA := a.Stream(ctx, "问题", agent.OnThread(threadID))
	if _, err := runA.Wait(); err != nil {
		t.Fatal(err)
	}
	first := runA.DetailedResult()
	runA.ID = "rewritten-run"
	runA.ThreadID = "rewritten-thread"

	if after := runA.DetailedResult(); after.RunID != first.RunID || after.ThreadID != threadID {
		t.Fatalf("结果跟着导出字段改写了: run=%q thread=%q", after.RunID, after.ThreadID)
	}
	if err := a.RecordCompletion(ctx, runA); err != nil {
		t.Fatalf("改写导出字段后仍应能记录本次终态: %v", err)
	}
	head := headCheckpoint(t, base, threadID)
	if head.Completion == nil || head.Completion.RunID != first.RunID {
		t.Fatalf("终态记录没有用冻结的 RunID: %+v", head.Completion)
	}
	if head.RunID != first.RunID || head.ThreadID != threadID {
		t.Fatalf("终态记录的谱系身份被改写: run=%q thread=%q", head.RunID, head.ThreadID)
	}
	if got := runA.DetailedResult(); got.Revision != first.Revision {
		t.Fatalf("记录终态后 A 的边界动了: %d vs %d", got.Revision, first.Revision)
	}
}

// TestRunDetailedNoBoundary：准备阶段失败、本次没有任何提交时 Ref=nil、Revision=0，
// 但已知的运行身份保留下来供诊断。
func TestRunDetailedNoBoundary(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	first := terminalAgent(t, base, resultTextModel("m", "第一次"))
	if _, err := first.RunDetailed(ctx, "建立 head", agent.OnThread("detailed-no-boundary")); err != nil {
		t.Fatal(err)
	}

	blocked := &refuseBeginStore{ThreadCoordinator: base}
	a := terminalAgent(t, blocked, resultTextModel("m", "第二次"))
	res, err := a.RunDetailed(ctx, "再来", agent.OnThread("detailed-no-boundary"))
	if err == nil {
		t.Fatal("准备失败应返回错误")
	}
	if res.CheckpointRef != nil || res.Revision != 0 {
		t.Fatalf("继承的 head 被当成本次边界: %+v rev=%d", res.CheckpointRef, res.Revision)
	}
	if res.ThreadID != "detailed-no-boundary" || res.RunID == "" {
		t.Fatalf("已知的运行身份丢失: run=%q thread=%q", res.RunID, res.ThreadID)
	}
	if res.TurnID != "" {
		t.Fatalf("没有启动的运行不该有 TurnID: %q", res.TurnID)
	}
	if res.Result.Message.Text() != "" || res.Paused {
		t.Fatalf("失败的运行不该带结果或暂停标志: %+v paused=%v", res.Result, res.Paused)
	}
}

// TestRunDetailedPartialFailureKeepsBoundary：部分提交之后失败，边界是本次最后
// 一次成功提交。
func TestRunDetailedPartialFailureKeepsBoundary(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	store := &terminalStore{
		ThreadCoordinator: base,
		beforeCommit:      func(checkpoint.CommitRequest) error { return nil },
	}
	// 第二次提交失败：第一次成功提交就是本次边界。
	var attempts atomic.Int32
	store.beforeCommit = func(checkpoint.CommitRequest) error {
		if attempts.Add(1) == 2 {
			return errors.New("存储不可用")
		}
		return nil
	}
	a := terminalAgent(t, store, resultToolModel("m", 3), resultEchoTool())
	res, err := a.RunDetailed(ctx, "带工具的问题", agent.OnThread("detailed-partial"))
	if err == nil {
		t.Fatal("提交失败应让运行失败")
	}
	if res.CheckpointRef == nil || res.Revision == 0 {
		t.Fatalf("部分提交后失败应保留本次边界: %+v rev=%d", res.CheckpointRef, res.Revision)
	}
	stored, err := base.Load(ctx, res.CheckpointRef.ThreadID, res.CheckpointRef.CheckpointID)
	if err != nil {
		t.Fatalf("边界记录不可定位: %v", err)
	}
	if stored.RunID != res.RunID || stored.TurnID != res.TurnID {
		t.Fatalf("边界身份与结果不符: %+v vs run=%q turn=%q", stored, res.RunID, res.TurnID)
	}
}

// TestRunDetailedPaused：暂停的运行报 Paused=true、没有错误，边界是那条暂停记录，
// 也不会自动写终态。
func TestRunDetailedPaused(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgentWith(t, base, resultToolModel("m", 2), gate{}, resultEchoTool())
	res, err := a.RunDetailed(ctx, "需要审批", agent.OnThread("detailed-paused"))
	if err != nil {
		t.Fatalf("暂停不是错误: %v", err)
	}
	if !res.Paused {
		t.Fatal("没有报成暂停")
	}
	if res.CheckpointRef == nil || res.Revision == 0 {
		t.Fatalf("暂停也是一次成功提交: %+v rev=%d", res.CheckpointRef, res.Revision)
	}
	head := headCheckpoint(t, base, "detailed-paused")
	if head.Pause == nil || head.ID != res.CheckpointRef.CheckpointID {
		t.Fatalf("边界不是暂停记录: %+v", head)
	}
	if head.Completion != nil {
		t.Fatal("暂停路径写了终态")
	}
	if res.RunID == "" || res.TurnID == "" {
		t.Fatalf("暂停记录应保留运行身份: %+v", res)
	}
}

// TestRunDetailedIsolation：改动画出来的结果不污染下一次读取。
func TestRunDetailedIsolation(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "原始回答"))
	run := a.Stream(ctx, "问题", agent.OnThread("detailed-isolation"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	first := run.DetailedResult()
	if len(first.Result.Message.Parts) == 0 {
		t.Fatal("结果没有内容")
	}
	first.Result.Message.Parts[0] = core.Text{Text: "改写过的内容"}
	first.Result.Message.Parts = append(first.Result.Message.Parts, core.Text{Text: "多出来的一段"})

	again := run.DetailedResult()
	if again.Result.Message.Text() != "原始回答" {
		t.Fatalf("结果被调用方改写污染: %q", again.Result.Message.Text())
	}
	if len(again.Result.Message.Parts) != 1 {
		t.Fatalf("结果段落数被改动: %d", len(again.Result.Message.Parts))
	}
}

// TestRunDetailedIdentityFixedAtCreation：在 Wait 之前改写导出字段也不能改变身份。
// 执行准备、已提交检查点、结构化结果与终态记录必须始终用创建时那一份。
func TestRunDetailedIdentityFixedAtCreation(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultToolModel("m", 1), resultEchoTool())
	threadID := "review-identity"

	run := a.Stream(ctx, "带工具的问题", agent.OnThread(threadID))
	createdRunID := run.ID
	run.ID = "rewritten-before-wait"
	run.ThreadID = "rewritten-thread"

	if _, err := run.Wait(); err != nil {
		t.Fatalf("改写导出字段不该让运行失败: %v", err)
	}
	res := run.DetailedResult()
	if res.RunID != createdRunID {
		t.Fatalf("结果跟改了导出字段: run=%q, want 创建时的 %q", res.RunID, createdRunID)
	}
	if res.ThreadID != threadID {
		t.Fatalf("结果线程 = %q, want %q", res.ThreadID, threadID)
	}
	if res.CheckpointRef == nil || res.CheckpointRef.ThreadID != threadID {
		t.Fatalf("边界 Ref 落到别的线程上: %+v", res.CheckpointRef)
	}
	boundary, err := base.Load(ctx, res.CheckpointRef.ThreadID, res.CheckpointRef.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.RunID != res.RunID || boundary.TurnID != res.TurnID || boundary.ThreadID != threadID {
		t.Fatalf("边界身份与结果不一致: %+v vs %+v", boundary, res)
	}
	if count := historyCount(t, base, "rewritten-thread"); count != 0 {
		t.Fatalf("被改写的线程名上出现了 %d 条记录", count)
	}

	if err := a.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("记录终态: %v", err)
	}
	record := headCheckpoint(t, base, threadID)
	if record.Completion == nil || record.Completion.RunID != res.RunID {
		t.Fatalf("终态记录用了改写后的身份: %+v", record.Completion)
	}
	if record.RunID != res.RunID || record.ThreadID != threadID {
		t.Fatalf("终态记录的谱系身份与结果不一致: run=%q thread=%q", record.RunID, record.ThreadID)
	}
	if got := run.DetailedResult(); got.RunID != createdRunID {
		t.Fatalf("记录终态后结果身份漂移: %q", got.RunID)
	}
}

// TestRunDetailedKeepsRunContract：Run 的字符串返回和 checkpoint 条数不变，
// RunDetailed 不额外追加记录。
func TestRunDetailedKeepsRunContract(t *testing.T) {
	ctx := context.Background()
	plain := checkpoint.NewMemory()
	runA := terminalAgent(t, plain, resultToolModel("m", 1), resultEchoTool())
	text, err := runA.Run(ctx, "带工具的问题", agent.OnThread("contract-run"))
	if err != nil {
		t.Fatal(err)
	}
	countPlain := historyCount(t, plain, "contract-run")

	detailed := checkpoint.NewMemory()
	runB := terminalAgent(t, detailed, resultToolModel("m", 1), resultEchoTool())
	res, err := runB.RunDetailed(ctx, "带工具的问题", agent.OnThread("contract-detailed"))
	if err != nil {
		t.Fatal(err)
	}
	countDetailed := historyCount(t, detailed, "contract-detailed")

	if text != "收尾回答" {
		t.Fatalf("Run 的字符串返回变了: %q", text)
	}
	if countDetailed != countPlain {
		t.Fatalf("RunDetailed 追加了记录: %d vs %d", countDetailed, countPlain)
	}
	if res.CheckpointRef == nil || res.Result.Message.Text() != "收尾回答" {
		t.Fatalf("结构化结果不符: %+v", res)
	}
}

// TestRunDetailedUnknownBeforeSettle：还没结束的 Run 读到的是没有结局的空记录。
func TestRunDetailedUnknownBeforeSettle(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	run := a.Stream(ctx, "问题", agent.OnThread("detailed-unknown"))
	res := run.DetailedResult()
	if res.CheckpointRef != nil || res.Revision != 0 || res.Paused || res.Result.Message.Text() != "" {
		t.Fatalf("未结束的运行不该有结局: %+v", res)
	}
	if res.RunID != run.ID || res.ThreadID != "detailed-unknown" {
		t.Fatalf("未结束的运行也应报出当前身份: run=%q thread=%q", res.RunID, res.ThreadID)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if settled := run.DetailedResult(); settled.CheckpointRef == nil || settled.Result.Message.Text() != "回答" {
		t.Fatalf("结束后应读到完整记录: %+v", settled)
	}
}
