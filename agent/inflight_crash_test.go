package agent_test

// IF-07：在途边界的崩溃注入矩阵。每个用例先用真实运行把存储推进到某个提交
// 时刻，再让「进程」消失（之后的提交全部被拒），随后用同一份持久状态走
// Agent.Resume。统一断言：每个计划调用恰好一条结果、没有重复副作用、头部
// 最终不带 InFlight。
//
// 崩溃不是取消：取消会走失败补齐并写下一个常规检查点，崩溃则停在上一次
// 成功提交上。crashStore 用「之后所有 Commit 都被拒」表达这一点，onCrash
// 只负责让运行停在边界处，不写任何结算。

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

var errProcessGone = errors.New("crash harness: the process is gone")

// crashStore lets crashAt commits through, then refuses every later one. The
// last successful write stays visible, exactly as a process death would leave
// it. onCrash runs after that commit returns, so a test can cancel the run
// before any handler starts without writing a settlement.
type crashStore struct {
	checkpoint.Durable

	mu      sync.Mutex
	commits int
	crashAt int
	crashed bool
	onCrash func()
	err     error
}

func (s *crashStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	if s.crashed {
		err := s.err
		s.mu.Unlock()
		return checkpoint.CommitReceipt{}, err
	}
	s.commits++
	n := s.commits
	s.mu.Unlock()

	receipt, err := s.Durable.Commit(ctx, req)
	if err != nil {
		return receipt, err
	}
	if n == s.crashAt {
		s.mu.Lock()
		s.crashed = true
		s.mu.Unlock()
		if s.onCrash != nil {
			s.onCrash()
		}
	}
	return receipt, nil
}

// heal starts a new process on the same durable state.
func (s *crashStore) heal() {
	s.mu.Lock()
	s.crashed = false
	s.crashAt = 0
	s.mu.Unlock()
}

// cancelAfterFirstTool stops the run once one call finished. Nothing else is
// written, so the in-flight head survives.
type cancelAfterFirstTool struct {
	agent.BaseMiddleware
	once   sync.Once
	cancel context.CancelFunc
}

func (m *cancelAfterFirstTool) AfterTool(*agent.LoopContext, *core.ToolResult) (core.Directive, error) {
	m.once.Do(m.cancel)
	return core.Directive{}, nil
}

func batchResponse(calls ...core.ToolCall) *llm.Response {
	parts := make([]core.Part, len(calls))
	for i, c := range calls {
		parts[i] = c
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: llm.StopToolUse,
	}
}

func latestHead(t *testing.T, store checkpoint.Checkpointer, threadID string) *checkpoint.Checkpoint {
	t.Helper()
	head, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatalf("Latest(%s): %v", threadID, err)
	}
	if head == nil {
		t.Fatalf("thread %s has no checkpoint", threadID)
	}
	return head
}

// toolMessage returns the single tool-result message a settled batch writes,
// so a recovery that appends the same results twice is caught.
func toolMessage(t *testing.T, head *checkpoint.Checkpoint, wantParts int) core.Message {
	t.Helper()
	var found []core.Message
	for _, m := range head.State.Messages {
		if m.Role == core.RoleTool {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("tool messages = %d, want exactly one batch result", len(found))
	}
	if len(found[0].Parts) != wantParts {
		t.Fatalf("tool result parts = %d, want %d: %+v", len(found[0].Parts), wantParts, found[0])
	}
	return found[0]
}

func assertResultsInOrder(t *testing.T, msg core.Message, ids ...string) {
	t.Helper()
	for i, id := range ids {
		tr, ok := msg.Parts[i].(core.ToolResult)
		if !ok || tr.CallID != id {
			t.Fatalf("result %d = %+v, want call %s in order", i, msg.Parts[i], id)
		}
	}
}

func drainRecovered(t *testing.T, run *agent.Run) *core.ToolBatchRecovered {
	t.Helper()
	var recovered *core.ToolBatchRecovered
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatalf("recovery event stream: %v", err)
		}
		if e, ok := ev.(core.ToolBatchRecovered); ok {
			copied := e
			recovered = &copied
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("recovery run: %v", err)
	}
	return recovered
}

// TestCrashBeforeInFlightCommit：在途提交失败，handler 一个都没启动；失败
// 检查点里没有悬空的执行记录。重新运行从模型步开始，模型可以重新规划出
// 不同的调用，旧批次不会被补跑。
func TestCrashBeforeInFlightCommit(t *testing.T) {
	ctx := context.Background()
	const threadID = "crash-before-commit"
	commitErr := errors.New("in-flight commit unavailable")
	mem := checkpoint.NewMemory()
	store := &failCommitStore{Durable: mem, failAt: 1, err: commitErr}
	rec := newRecordingTool("rec")
	firstBatch := recCalls(2)

	var mu sync.Mutex
	requests := 0
	model := mock.New("crash-before-commit", func(*llm.Request) *llm.Response {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		switch n {
		case 1:
			return batchResponse(firstBatch...)
		case 2:
			return batchResponse(core.ToolCall{ID: "c3", Name: "rec", Args: json.RawMessage(`{}`)})
		default:
			return mock.Text("done")
		}
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); !errors.Is(err, commitErr) {
		t.Fatalf("run error = %v, want the in-flight commit failure", err)
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times after a failed in-flight commit", got)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("head = %+v, want the failure checkpoint with no batch", head.InFlight)
	}
	// The failed commit never reached the ledger, so no orphan invocation is
	// left behind for a later run to replay.
	orphan := pendingExecution(t, threadID, 0, firstBatch[0])
	if _, found, err := mem.ToolExecution(ctx, threadID, orphan.InvocationID); err != nil || found {
		t.Fatalf("failed in-flight commit left a ledger record: found=%v err=%v", found, err)
	}

	if _, err := a.Run(ctx, "retry", agent.OnThread(threadID)); err != nil {
		t.Fatalf("rerun after a failed in-flight commit: %v", err)
	}
	got := rec.invocations()
	if len(got) != 1 || got[0].CallID != "c3" {
		t.Fatalf("handlers = %+v, want only the replanned call c3", got)
	}
	head = latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("final head = %+v, want no in-flight batch", head.InFlight)
	}
}

// TestCrashAfterCommitBeforeHandlers：在途提交成功、handler 启动前进程退出。
// 恢复把两条 pending 调用各执行一次，头部结算后不再带 InFlight。
func TestCrashAfterCommitBeforeHandlers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const threadID = "crash-after-commit"
	mem := checkpoint.NewMemory()
	store := &crashStore{Durable: mem, crashAt: 1, onCrash: cancel, err: errProcessGone}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); err == nil {
		t.Fatal("the crashed run must not report success")
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times before the crash was observed", got)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight == nil || len(head.InFlight.Calls) != 2 {
		t.Fatalf("crashed head = %+v, want the two-call in-flight batch", head.InFlight)
	}
	for _, c := range calls {
		if state := ledgerState(t, mem, threadID, 0, c); state != checkpoint.ToolPending {
			t.Fatalf("ledger %s = %q, want pending before any handler", c.ID, state)
		}
	}

	store.heal()
	recovered := drainRecovered(t, mustResume(t, a, threadID))
	if recovered == nil || recovered.Executed != 2 || recovered.Unknown != 0 || recovered.Replayed != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want executed=2", recovered)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want each pending call once", got)
	}
	head = latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("settled head still carries in-flight: %+v", head.InFlight)
	}
	assertResultsInOrder(t, toolMessage(t, head, 2), "c1", "c2")
}

// TestCrashMidBatch：第一个调用完成后进程退出，第二个还没启动。完成结果
// 只在内存里、没有随任何提交落盘，所以台账仍把第一个显示为 claimed；恢复
// 对它报「结果未知」而不是重跑，第二个 pending 调用照常执行。
func TestCrashMidBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const threadID = "crash-mid-batch"
	mem := checkpoint.NewMemory()
	store := &crashStore{Durable: mem, crashAt: 1, err: errProcessGone}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterFirstTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); err == nil {
		t.Fatal("the crashed run must not report success")
	}
	before := rec.invocations()
	if len(before) != 1 || before[0].CallID != "c1" {
		t.Fatalf("handlers before the crash = %+v, want only c1", before)
	}
	if state := ledgerState(t, mem, threadID, 0, calls[0]); state != checkpoint.ToolClaimed {
		t.Fatalf("ledger c1 = %q, want claimed: its result never reached a commit", state)
	}
	if state := ledgerState(t, mem, threadID, 0, calls[1]); state != checkpoint.ToolPending {
		t.Fatalf("ledger c2 = %q, want pending: it never started", state)
	}
	if head := latestHead(t, mem, threadID); head.InFlight == nil {
		t.Fatalf("crashed head = %+v, want the in-flight batch", head)
	}

	store.heal()
	recovered := drainRecovered(t, mustResume(t, a, threadID))
	if recovered == nil || recovered.Executed != 1 || recovered.Unknown != 1 || recovered.Replayed != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want executed=1 unknown=1", recovered)
	}
	after := rec.invocations()
	if len(after) != 2 || after[0].CallID != "c1" || after[1].CallID != "c2" {
		t.Fatalf("handler invocations = %+v, want c1 once and c2 once", after)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("settled head still carries in-flight: %+v", head.InFlight)
	}
	msg := toolMessage(t, head, 2)
	assertResultsInOrder(t, msg, "c1", "c2")
	if tr := msg.Parts[0].(core.ToolResult); !tr.IsError {
		t.Fatalf("claimed c1 = %+v, want an unknown-outcome error result", tr)
	}
	if tr := msg.Parts[1].(core.ToolResult); tr.IsError {
		t.Fatalf("pending c2 = %+v, want the handler's result", tr)
	}
}

// TestCrashAfterBatchBeforePhase4：批次全部执行完，Phase 4 提交失败。每个
// 调用的完成记录同样只在内存，台账停在 claimed；恢复报两次未知，不重跑
// handler，也不重复追加工具消息。
func TestCrashAfterBatchBeforePhase4(t *testing.T) {
	ctx := context.Background()
	const threadID = "crash-before-phase4"
	mem := checkpoint.NewMemory()
	phase4Err := errors.New("phase 4 commit unavailable")
	store := &failCommitStore{Durable: mem, failAt: 2, err: phase4Err}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); !errors.Is(err, phase4Err) {
		t.Fatalf("run error = %v, want the Phase 4 commit failure", err)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want the batch executed once", got)
	}
	for _, c := range calls {
		if state := ledgerState(t, mem, threadID, 0, c); state != checkpoint.ToolClaimed {
			t.Fatalf("ledger %s = %q, want claimed: Phase 4 never committed the results", c.ID, state)
		}
	}
	if head := latestHead(t, mem, threadID); head.InFlight == nil {
		t.Fatalf("crashed head = %+v, want the in-flight batch", head)
	}

	recovered := drainRecovered(t, mustResume(t, a, threadID))
	if recovered == nil || recovered.Unknown != 2 || recovered.Executed != 0 || recovered.Replayed != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want unknown=2", recovered)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times after recovery, want no repeat", got)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("settled head still carries in-flight: %+v", head.InFlight)
	}
	msg := toolMessage(t, head, 2)
	assertResultsInOrder(t, msg, "c1", "c2")
	for i, part := range msg.Parts {
		if tr := part.(core.ToolResult); !tr.IsError {
			t.Fatalf("result %d = %+v, want unknown-outcome error", i, tr)
		}
	}
}

// TestCrashAfterPhase4：Phase 4 已成功，头部是普通检查点。恢复直接回到
// 模型步，不重新执行批次，也不再追加一条工具消息。
func TestCrashAfterPhase4(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const threadID = "crash-after-phase4"
	mem := checkpoint.NewMemory()
	store := &crashStore{Durable: mem, crashAt: 2, onCrash: cancel, err: errProcessGone}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); err == nil {
		t.Fatal("the crashed run must not report success")
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want the batch executed once", got)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("head after Phase 4 = %+v, want no in-flight batch", head.InFlight)
	}
	toolMessage(t, head, 2)
	if state := ledgerState(t, mem, threadID, 0, calls[0]); state != checkpoint.ToolCompleted {
		t.Fatalf("ledger c1 = %q, want completed: Phase 4 committed it", state)
	}

	store.heal()
	recovered := drainRecovered(t, mustResume(t, a, threadID))
	if recovered != nil {
		t.Fatalf("recovering a settled head published ToolBatchRecovered: %+v", recovered)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want the batch not re-executed", got)
	}
	head = latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("final head carries in-flight: %+v", head.InFlight)
	}
	toolMessage(t, head, 2)
}

// TestCrashDuringRecoveryCommit：恢复执行了 pending 调用，但结算提交失败。
// 第二次恢复按台账把这两条 claimed 报成未知，不重试 handler。
func TestCrashDuringRecoveryCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const threadID = "crash-recovery-commit"
	mem := checkpoint.NewMemory()
	crash := &crashStore{Durable: mem, crashAt: 1, onCrash: cancel, err: errProcessGone}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(crash),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); err == nil {
		t.Fatal("the crashed run must not report success")
	}
	crash.heal()

	recoveryErr := errors.New("recovery settlement unavailable")
	failing := &failCommitStore{Durable: mem, failAt: 1, err: recoveryErr}
	b, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(failing),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := b.Resume(context.Background(), threadID)
	if err != nil {
		t.Fatalf("first Resume: %v", err)
	}
	if _, err := phase1Drain(t, first); !errors.Is(err, recoveryErr) {
		t.Fatalf("first recovery = %v, want the settlement commit failure", err)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want both pending calls executed once", got)
	}
	if head := latestHead(t, mem, threadID); head.InFlight == nil {
		t.Fatalf("head after the failed settlement = %+v, want the in-flight batch", head)
	}
	for _, c := range calls {
		if state := ledgerState(t, mem, threadID, 0, c); state != checkpoint.ToolClaimed {
			t.Fatalf("ledger %s = %q, want claimed after the failed settlement", c.ID, state)
		}
	}

	recovered := drainRecovered(t, mustResume(t, b, threadID))
	if recovered == nil || recovered.Unknown != 2 || recovered.Executed != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want unknown=2 and no retry", recovered)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times after the second recovery, want no repeat", got)
	}
	head := latestHead(t, mem, threadID)
	if head.InFlight != nil {
		t.Fatalf("settled head still carries in-flight: %+v", head.InFlight)
	}
	toolMessage(t, head, 2)
}

// TestInFlightSurvivesFileStoreReopen：文件存储重开目录后，RunStatus 仍报
// 在途，Resume 能把批次结算掉。
func TestInFlightSurvivesFileStoreReopen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const threadID = "crash-file-reopen"
	dir := t.TempDir()
	file, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	store := &crashStore{Durable: file, crashAt: 1, onCrash: cancel, err: errProcessGone}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := phase1Drain(t, run); err == nil {
		t.Fatal("the crashed run must not report success")
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times before the crash was observed", got)
	}

	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(reopened),
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := b.RunStatus(ctx, threadID)
	if err != nil {
		t.Fatalf("RunStatus after reopen: %v", err)
	}
	if !status.Exists || !status.InFlight || status.Paused {
		t.Fatalf("RunStatus after reopen = %+v, want exists, in-flight, not paused", status)
	}
	if status.Latest == nil || status.Latest.InFlight == nil {
		t.Fatalf("RunStatus.Latest = %+v, want the raw in-flight head", status.Latest)
	}

	recovered := drainRecovered(t, mustResume(t, b, threadID))
	if recovered == nil || recovered.Executed != 2 {
		t.Fatalf("ToolBatchRecovered = %+v, want executed=2", recovered)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want each pending call once", got)
	}
	head := latestHead(t, reopened, threadID)
	if head.InFlight != nil {
		t.Fatalf("settled head still carries in-flight: %+v", head.InFlight)
	}
	assertResultsInOrder(t, toolMessage(t, head, 2), "c1", "c2")
}

// TestInFlightMatrixBackwardChecks 是 IF-07 反向验证的集中登记处：五个
// 「去掉守卫就应红」的行为各由一条既有用例拥有，这里把它们作为子用例统一
// 跑一遍，免得反向检查散落在各卡的执行记录里。断言不在这里重写。
func TestInFlightMatrixBackwardChecks(t *testing.T) {
	guards := []struct {
		name string
		run  func(*testing.T)
	}{
		{"in-flight commit precedes handlers", TestInFlightCheckpointWrittenBeforeHandlers},
		{"next_turn waits for the turn boundary", TestVolatilePhasesSurviveToolBatch},
		{"preemption runs at most once per step", TestPreemptTwiceInOneStep},
		{"summary drift only blocks possible execution", TestResumeInFlightSummaryDriftAllowsPureReplay},
		{"failure backfills every planned call", TestFailedInFlightBatchKeepsPairing},
	}
	for _, g := range guards {
		t.Run(g.name, g.run)
	}
}

func mustResume(t *testing.T, a *agent.Agent, threadID string) *agent.Run {
	t.Helper()
	run, err := a.Resume(context.Background(), threadID)
	if err != nil {
		t.Fatalf("Resume(%s): %v", threadID, err)
	}
	return run
}
