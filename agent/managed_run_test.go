package agent_test

// SM-07（ADR-0034 D3、D4）：受管运行入口。queue 分配的 RunID/ThreadID/TurnID 在创建
// 手柄时冻结，运行、已提交检查点、结构化结果与终态记录用的是同一份身份；改写导出的
// Run.ID 不能冒充别的任务。RecordCompletion 成功后可以从运行上取到那条终态记录。

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func managedTextModel(text string, calls *atomic.Int32) llm.Model {
	return mock.New("m", func(*llm.Request) *llm.Response {
		if calls != nil {
			calls.Add(1)
		}
		return mock.Text(text)
	})
}

// TestManagedRunSpecValidation 检查入口在建手柄之前就把坏身份挡在门外：空的 RunID、
// 不能当文件名的 ThreadID，以及和分派线程冲突的 OnThread。
func TestManagedRunSpecValidation(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	var calls atomic.Int32
	a := terminalAgent(t, base, managedTextModel("回答", &calls))

	for _, spec := range []agent.ManagedRunSpec{
		{RunID: "", ThreadID: "managed-valid"},
		{RunID: "run-1", ThreadID: ""},
		{RunID: "run-1", ThreadID: "bad/thread"},
		{RunID: "run-1", ThreadID: "../escape"},
	} {
		if err := spec.Validate(); err == nil {
			t.Fatalf("spec %+v 应校验失败", spec)
		}
		if _, err := a.StreamManaged(ctx, spec, "问题"); !errors.Is(err, agent.ErrInvalidManagedRun) {
			t.Fatalf("StreamManaged(%+v) = %v, 期望 ErrInvalidManagedRun", spec, err)
		}
	}

	if err := (agent.ManagedRunSpec{RunID: "run-1", ThreadID: "managed-valid"}).Validate(); err != nil {
		t.Fatalf("合法 spec 被拒: %v", err)
	}

	// 冲突的 OnThread 同样是入口拒绝，而不是运行中途悄悄换线程。
	_, err := a.StreamManaged(ctx, agent.ManagedRunSpec{RunID: "run-1", ThreadID: "managed-valid"}, "问题",
		agent.OnThread("managed-other"))
	if err == nil {
		t.Fatal("OnThread 与分派线程冲突时应拒绝")
	}
	if !strings.Contains(err.Error(), "conflicts with the assigned thread") {
		t.Fatalf("冲突错误没说清: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("被拒绝的入口调用了 %d 次模型", got)
	}
	if count := historyCount(t, base, "managed-valid"); count != 0 {
		t.Fatalf("被拒绝的入口写下了 %d 条检查点", count)
	}
}

// TestManagedRunIdentity 证明分派身份一路一致：手柄的导出字段、已提交检查点、结构化
// 结果与终态记录都取创建时那一份，改写导出字段不影响任何一处。
func TestManagedRunIdentity(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultToolModel("m", 1), resultEchoTool())

	spec := agent.ManagedRunSpec{
		RunID:    "run-managed-1",
		ThreadID: "managed-identity",
		TurnID:   "turn-managed-1",
	}
	run, err := a.StreamManaged(ctx, spec, "带工具的问题")
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != spec.RunID || run.ThreadID != spec.ThreadID {
		t.Fatalf("手柄没用分派身份: run=%q thread=%q", run.ID, run.ThreadID)
	}
	if ref, ok := run.RecordedBoundary(); ok || ref != nil {
		t.Fatalf("还没记录终态就有边界: %+v ok=%v", ref, ok)
	}

	run.ID = "rewritten-run"
	run.ThreadID = "rewritten-thread"
	if _, err := run.Wait(); err != nil {
		t.Fatalf("改写导出字段不该让运行失败: %v", err)
	}

	res := run.DetailedResult()
	if res.RunID != spec.RunID || res.ThreadID != spec.ThreadID || res.TurnID != spec.TurnID {
		t.Fatalf("结果身份跟着导出字段改写: run=%q thread=%q turn=%q", res.RunID, res.ThreadID, res.TurnID)
	}
	if res.CheckpointRef == nil || res.CheckpointRef.ThreadID != spec.ThreadID {
		t.Fatalf("边界 Ref 落到了别的线程: %+v", res.CheckpointRef)
	}
	boundary, err := base.Load(ctx, res.CheckpointRef.ThreadID, res.CheckpointRef.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.RunID != spec.RunID || boundary.TurnID != spec.TurnID {
		t.Fatalf("已提交检查点的身份不是分派的那份: run=%q turn=%q", boundary.RunID, boundary.TurnID)
	}
	if count := historyCount(t, base, "rewritten-thread"); count != 0 {
		t.Fatalf("被改写的线程名上出现了 %d 条记录", count)
	}

	if err := a.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("记录终态: %v", err)
	}
	ref, ok := run.RecordedBoundary()
	if !ok || ref == nil {
		t.Fatal("记录成功后应能取到终态边界")
	}
	head := headCheckpoint(t, base, spec.ThreadID)
	if ref.CheckpointID != head.ID || ref.ThreadID != spec.ThreadID {
		t.Fatalf("终态边界指向别处: %+v, head=%s", ref, head.ID)
	}
	if head.RunID != spec.RunID || head.TurnID != spec.TurnID {
		t.Fatalf("终态记录的谱系身份被改写: run=%q turn=%q", head.RunID, head.TurnID)
	}
	if head.Completion == nil || head.Completion.RunID != spec.RunID {
		t.Fatalf("终态记录没用分派的 RunID: %+v", head.Completion)
	}
	if again := run.DetailedResult(); again.RunID != spec.RunID || again.Revision != res.Revision {
		t.Fatalf("记录终态后结果身份或边界漂移: %+v", again)
	}

	// TurnID 留空时，受管运行和前台运行一样开一个新回合，而不是共用上一个。
	second, err := a.StreamManaged(ctx, agent.ManagedRunSpec{RunID: "run-managed-2", ThreadID: spec.ThreadID}, "第二个问题")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := second.DetailedResult(); got.TurnID == "" || got.TurnID == spec.TurnID {
		t.Fatalf("未指定 TurnID 的运行没有开新回合: %q", got.TurnID)
	}
}

// TestManagedRunPreparationFailure 造一次线程占用：运行失败且本次没有任何提交，
// 身份仍然报出来供诊断，终态记录被拒而不是造一个假边界。
func TestManagedRunPreparationFailure(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	seedSpec := agent.ManagedRunSpec{RunID: "run-managed-seed", ThreadID: "managed-prepare-fail", TurnID: "turn-seed"}
	seedAgent := terminalAgent(t, base, managedTextModel("建立 head", nil))
	if _, err := seedAgent.RunManaged(ctx, seedSpec, "第一个问题"); err != nil {
		t.Fatal(err)
	}
	before := historyCount(t, base, seedSpec.ThreadID)

	blocked := &refuseBeginStore{ThreadCoordinator: base}
	a := terminalAgent(t, blocked, managedTextModel("第二次", nil))
	spec := agent.ManagedRunSpec{RunID: "run-managed-blocked", ThreadID: seedSpec.ThreadID, TurnID: "turn-managed-blocked"}
	run, err := a.StreamManaged(ctx, spec, "第二个问题")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err == nil {
		t.Fatal("准备失败应返回错误")
	}

	res := run.DetailedResult()
	if res.RunID != spec.RunID || res.ThreadID != spec.ThreadID {
		t.Fatalf("失败的运行丢了已知身份: run=%q thread=%q", res.RunID, res.ThreadID)
	}
	if res.TurnID != "" {
		t.Fatalf("没有启动的执行不该带 TurnID: %q", res.TurnID)
	}
	if res.CheckpointRef != nil || res.Revision != 0 || res.Paused {
		t.Fatalf("继承的 head 被当成本次边界: %+v rev=%d paused=%v", res.CheckpointRef, res.Revision, res.Paused)
	}
	if err := a.RecordCompletion(ctx, run); !errors.Is(err, agent.ErrNoCommittedBoundary) {
		t.Fatalf("无边界运行记录终态 = %v, 期望 ErrNoCommittedBoundary", err)
	}
	if ref, ok := run.RecordedBoundary(); ok || ref != nil {
		t.Fatalf("被拒的终态记录不该留下边界: %+v ok=%v", ref, ok)
	}
	if after := historyCount(t, base, seedSpec.ThreadID); after != before {
		t.Fatalf("无本次提交的运行额外写了 %d 条检查点", after-before)
	}
}

// TestManagedRunPaused 证明暂停不是错误：边界是那条暂停记录，终态记录被明确拒绝。
func TestManagedRunPaused(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgentWith(t, base, resultToolModel("m", 2), gate{}, resultEchoTool())

	spec := agent.ManagedRunSpec{RunID: "run-managed-pause", ThreadID: "managed-paused", TurnID: "turn-managed-pause"}
	run, err := a.StreamManaged(ctx, spec, "需要审批")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("暂停不是错误: %v", err)
	}
	res := run.DetailedResult()
	if !res.Paused {
		t.Fatal("没有报成暂停")
	}
	if res.RunID != spec.RunID || res.TurnID != spec.TurnID {
		t.Fatalf("暂停结果身份不符: run=%q turn=%q", res.RunID, res.TurnID)
	}
	if res.CheckpointRef == nil || res.Revision == 0 {
		t.Fatalf("暂停也是一次成功提交: %+v rev=%d", res.CheckpointRef, res.Revision)
	}
	head := headCheckpoint(t, base, spec.ThreadID)
	if head.Pause == nil || head.ID != res.CheckpointRef.CheckpointID {
		t.Fatalf("边界不是暂停记录: %+v", head)
	}
	if head.Completion != nil {
		t.Fatal("暂停路径写了终态")
	}

	if err := a.RecordCompletion(ctx, run); !errors.Is(err, agent.ErrRunPaused) {
		t.Fatalf("暂停运行记录终态 = %v, 期望 ErrRunPaused", err)
	}
	if ref, ok := run.RecordedBoundary(); ok || ref != nil {
		t.Fatalf("被拒的终态记录不该留下边界: %+v ok=%v", ref, ok)
	}
	if got := headCheckpoint(t, base, spec.ThreadID); got.Completion != nil {
		t.Fatalf("暂停运行最终写了终态: %+v", got.Completion)
	}
}

// TestManagedRunCompletionRecordError 证明终态记录写失败与执行成功是两件事：边界取不到，
// 放开故障后重试同一条记录，不出现第二条终态。
func TestManagedRunCompletionRecordError(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	store := &terminalStore{ThreadCoordinator: base}
	var refuse atomic.Bool
	store.beforeCommit = func(req checkpoint.CommitRequest) error {
		if refuse.Load() && req.Checkpoint != nil && req.Checkpoint.Completion != nil {
			return errors.New("终态存储不可用")
		}
		return nil
	}
	a := terminalAgent(t, store, managedTextModel("最终回答", nil))

	spec := agent.ManagedRunSpec{RunID: "run-managed-record", ThreadID: "managed-record-error", TurnID: "turn-managed-record"}
	run, err := a.StreamManaged(ctx, spec, "问题")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}

	res := run.DetailedResult()
	if res.CheckpointRef == nil {
		t.Fatal("运行本身应有本次提交的边界")
	}
	if got := headCheckpoint(t, base, spec.ThreadID); got.Completion != nil {
		t.Fatal("还没记录就有终态")
	}

	refuse.Store(true)
	err = a.RecordCompletion(ctx, run)
	if err == nil {
		t.Fatal("终态写入被拒时应返回错误")
	}
	if !strings.Contains(err.Error(), "终态存储不可用") {
		t.Fatalf("错误丢了存储的原因: %v", err)
	}
	if ref, ok := run.RecordedBoundary(); ok || ref != nil {
		t.Fatalf("写入失败的记录不该报出边界: %+v ok=%v", ref, ok)
	}
	if got := headCheckpoint(t, base, spec.ThreadID); got.Completion != nil {
		t.Fatalf("被拒的终态落库了: %+v", got.Completion)
	}
	if got := run.DetailedResult(); got.CheckpointRef.CheckpointID != res.CheckpointRef.CheckpointID {
		t.Fatalf("终态记录失败却推进了本次边界: %+v", got.CheckpointRef)
	}

	refuse.Store(false)
	if err := a.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("故障放开后重试: %v", err)
	}
	ref, ok := run.RecordedBoundary()
	if !ok {
		t.Fatal("重试成功后应报出边界")
	}
	head := headCheckpoint(t, base, spec.ThreadID)
	if ref.CheckpointID != head.ID || head.Completion == nil || head.Completion.RunID != spec.RunID {
		t.Fatalf("重试后的终态不对: %+v head=%+v", ref, head.Completion)
	}
	if count := len(store.terminalCommits()); count != 1 {
		t.Fatalf("终态提交出现 %d 次，期望只成功 1 次", count)
	}
}
