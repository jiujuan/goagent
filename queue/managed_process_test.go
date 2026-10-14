package queue

// SM-13（ADR-0034 D10）：受管运行的跨进程验收。父进程只做编排和断言；提交、执行、取消、
// 修复都发生在 os.Executable 重新拉起的子进程里，两边除了一个 File 目录什么都不共享。
// 取消因此不可能靠内存 Handle，崩溃后的事实也只能靠"重开磁盘"来看。
//
// 故障点是子进程打印的 barrier 行：工具进到了哪一步、外部效果落没了、终态 checkpoint 提交后
// 停在发布之前。父进程先看到行再决定杀进程或放行，等的永远是"某条持久事实"或"某行输出"，
// 不是碰运气的延时。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	"github.com/jiujuan/goagent/runstore"
	"github.com/jiujuan/goagent/tool"
)

const managedProcessHelperEnv = "GOAGENT_MANAGED_PROCESS_HELPER"

const (
	managedProcessRoleEnv    = "GOAGENT_MANAGED_ROLE"
	managedProcessModeEnv    = "GOAGENT_MANAGED_MODE"
	managedProcessStateEnv   = "GOAGENT_MANAGED_STATE"
	managedProcessJobsEnv    = "GOAGENT_MANAGED_JOBS"
	managedProcessEffectsEnv = "GOAGENT_MANAGED_EFFECTS"
	managedProcessRunEnv     = "GOAGENT_MANAGED_RUN"
	managedProcessThreadEnv  = "GOAGENT_MANAGED_THREAD"
	managedProcessTurnEnv    = "GOAGENT_MANAGED_TURN"
	managedProcessJobIDEnv   = "GOAGENT_MANAGED_JOB_ID"
	managedProcessWorkerEnv  = "GOAGENT_MANAGED_WORKER"
	managedProcessTextEnv    = "GOAGENT_MANAGED_TEXT"
)

// 子进程角色。
const (
	managedRoleProducer = "producer"
	managedRoleWorker   = "worker"
	managedRoleClient   = "client"
)

// 每个角色内的模式，就是这张表的故障点。
const (
	managedModeEnqueue     = "enqueue"      // producer：建排队记录并把作业载荷落到共享目录
	managedModeCreateOnly  = "create-only"  // producer：建完记录就停在入队之前，等被杀
	managedModeReply       = "reply"        // worker：一次模型调用后正常收尾
	managedModeGate        = "gate"         // worker：停在工具里，等执行 context 被取消
	managedModeEffect      = "effect"       // worker：落一份外部效果后停在任何保存之前，等被杀
	managedModeHoldPublish = "hold-publish" // worker：终态 checkpoint 已提交，停在发布记录之前，等被杀
	managedModeWait        = "wait"         // client：只靠 RunID 等到终态
	managedModeReconcile   = "reconcile"    // client：显式修复一条没终结的记录
)

// barrier 行：父进程等到这些行才动手。
const (
	managedLineCreated     = "created"
	managedLineToolEntered = "tool-entered"
	managedLineEffectDone  = "effect-written"
	managedLinePrePublish  = "pre-publish"
)

// 报告行前缀：后面跟一行 JSON。
const (
	managedReportSubmitted  = "submitted"
	managedReportCreated    = "created-report"
	managedReportHandled    = "handled"
	managedReportWaited     = "waited"
	managedReportReconciled = "reconciled"
)

// managedProcessAgentKey 是载荷里的 agent key：worker 侧由它重建配置，闭包不过进程边界。
const managedProcessAgentKey = "managed"

// managedProcessReport 是子进程回报给父进程的唯一事实来源。字段全平铺，出错时把错误文本
// 放进去而不是让子进程非零退出：父进程要的是"看到了什么"，堆栈仍在 stderr 里。
type managedProcessReport struct {
	RunID           string `json:"run_id,omitempty"`
	JobID           string `json:"job_id,omitempty"`
	Status          string `json:"status,omitempty"`
	FinishReason    string `json:"finish_reason,omitempty"`
	Version         uint64 `json:"version,omitempty"`
	RecordWorker    string `json:"record_worker,omitempty"`
	WorkerID        string `json:"worker_id,omitempty"`
	Executed        bool   `json:"executed,omitempty"`
	HasResult       bool   `json:"has_result,omitempty"`
	ResultText      string `json:"result_text,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	CancelRequested bool   `json:"cancel_requested,omitempty"`
	CompletionID    string `json:"completion_id,omitempty"`
	ModelCalls      int64  `json:"model_calls,omitempty"`
	HandlerError    string `json:"handler_error,omitempty"`
	WaitError       string `json:"wait_error,omitempty"`
	Repaired        bool   `json:"repaired,omitempty"`
	Reason          string `json:"reason,omitempty"`
	Detail          string `json:"detail,omitempty"`
	Attempts        int    `json:"attempts,omitempty"`
	Error           string `json:"error,omitempty"`
}

// --- 场景 A：取消穿过进程边界 ------------------------------------------------

// TestManagedRunCancelCrossesProcesses 让生产者子进程提交、worker 子进程执行并停在工具里，
// 父进程与另一个客户端子进程各自重开磁盘上的 File 存储：前者写取消请求，后者只凭 RunID
// 等到终态。整条链上没有任何内存 Handle，也没有共享 goroutine。
func TestManagedRunCancelCrossesProcesses(t *testing.T) {
	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("cancel")

	submit := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleProducer, mode: managedModeEnqueue, ids: ids, text: "跨进程取消的答案",
	})
	submitted := submit.report(t, managedReportSubmitted)
	if err := submit.finish(t); err != nil {
		t.Fatalf("生产者退出异常: %v\n%s", err, submit.output())
	}
	if submitted.JobID != ids.jobID || submitted.Version != 1 || submitted.Status != string(runstore.StatusQueued) {
		t.Fatalf("提交回报 = %+v", submitted)
	}

	// 磁盘重开：排队记录是一个进程写完就留给所有人的事实。
	queued := managedRecordOnDisk(t, dirs, ids.runID)
	if queued.Status != runstore.StatusQueued || queued.WorkerID != "" || queued.JobID != ids.jobID {
		t.Fatalf("重开后的记录 = %+v", queued)
	}

	worker := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeGate, ids: ids,
		workerID: "worker-cancel", text: "跨进程取消的答案",
	})
	worker.expectLine(t, managedLineToolEntered)

	// barrier 之后记录必然已经是 running：领取写在模型第一次调用之前。
	running := managedRecordOnDisk(t, dirs, ids.runID)
	if running.Status != runstore.StatusRunning || running.WorkerID != "worker-cancel" || running.Version != 2 {
		t.Fatalf("worker 领取后的记录 = %+v", running)
	}

	waiter := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleClient, mode: managedModeWait, ids: ids,
	})

	client, err := NewRunClient(managedRunsOnDisk(t, dirs), WithRunPollInterval(2*time.Millisecond), WithRunPollMax(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	requested, err := client.CancelRun(context.Background(), ids.runID)
	if err != nil {
		t.Fatalf("另一个进程请求取消: %v", err)
	}
	if requested.Status != runstore.StatusCancelling || requested.CancelRequestedAt.IsZero() {
		t.Fatalf("取消请求落盘后 = %+v", requested)
	}

	got := waiter.report(t, managedReportWaited)
	if err := waiter.finish(t); err != nil {
		t.Fatalf("等待客户端退出异常: %v\n%s", err, waiter.output())
	}
	if got.Status != string(runstore.StatusCancelled) || got.WaitError != "" {
		t.Fatalf("新客户端的 WaitRun = %+v", got)
	}
	if got.FinishReason != string(runstore.FinishReasonCancelled) || got.HasResult {
		t.Fatalf("被Honour 的取消不该带结果: %+v", got)
	}

	handled := worker.report(t, managedReportHandled)
	if err := worker.finish(t); err != nil {
		t.Fatalf("worker 退出异常: %v\n%s", err, worker.output())
	}
	if handled.HandlerError != "" && !strings.Contains(handled.HandlerError, context.Canceled.Error()) {
		t.Fatalf("worker 报的执行错误 = %q, 期望只有 context 取消", handled.HandlerError)
	}
	if handled.Status != string(runstore.StatusCancelled) || handled.ModelCalls != 1 {
		t.Fatalf("worker 回报 = %+v", handled)
	}

	// 终态同样是重开磁盘来看：请求时间保留、没有结果、没有错误、也没被迟到改写。
	final := managedRecordOnDisk(t, dirs, ids.runID)
	if final.Status != runstore.StatusCancelled || final.FinishReason != runstore.FinishReasonCancelled {
		t.Fatalf("磁盘上的终态 = %s/%s", final.Status, final.FinishReason)
	}
	if final.Result != nil || final.Error != nil || final.RecordError != nil {
		t.Fatalf("取消的运行带了不该有的内容: %+v", final)
	}
	if !final.CancelRequestedAt.Equal(requested.CancelRequestedAt) {
		t.Fatalf("取消请求时间被改写: %v → %v", requested.CancelRequestedAt, final.CancelRequestedAt)
	}
	if final.WorkerID != "worker-cancel" || final.WorkerEpoch != running.WorkerEpoch {
		t.Fatalf("归属被改写: %q epoch %d", final.WorkerID, final.WorkerEpoch)
	}
	// 被 Honour 的取消仍然留下一条终态快照：执行 context 被取消不该把"这次运行结束了"这条
	// 事实一起取消掉（ADR-0033 D2），但任务记录里绝不带结果。
	completions := managedCompletions(t, dirs, ids.runID)
	if len(completions) != 1 {
		t.Fatalf("取消后的终态快照条数 = %d, 期望 1: %+v", len(completions), completions)
	}
	if completions[0].CompletionKind != checkpoint.CompletionFailed {
		t.Fatalf("取消的终态快照 kind = %q, 期望 %q", completions[0].CompletionKind, checkpoint.CompletionFailed)
	}
}

// --- 场景 B：崩溃窗口 --------------------------------------------------------

// TestManagedRunRepairedAfterCrashBeforeRecord 把 worker 停在"终态 checkpoint 已提交、任务
// 记录还没发布"这一步，然后杀掉它。新客户端显式 ReconcileRun 只按磁盘上的 Completion 修复，
// 父进程再重开一次磁盘确认修复是持久事实，而不是又一次内存里的巧合。
func TestManagedRunRepairedAfterCrashBeforeRecord(t *testing.T) {
	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("repair")
	text := "提交完成但没写记录的答案"

	submit := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleProducer, mode: managedModeEnqueue, ids: ids, text: text,
	})
	submit.report(t, managedReportSubmitted)
	if err := submit.finish(t); err != nil {
		t.Fatalf("生产者退出异常: %v\n%s", err, submit.output())
	}

	worker := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeHoldPublish, ids: ids,
		workerID: "worker-crash", text: text,
	})
	worker.expectLine(t, managedLinePrePublish)
	worker.kill(t)

	// 崩溃留下的形状：执行已经做完，记录还停在 running，且没有任何终态引用。
	crashed := managedRecordOnDisk(t, dirs, ids.runID)
	if crashed.Status != runstore.StatusRunning || crashed.WorkerID != "worker-crash" {
		t.Fatalf("崩溃后的记录 = %+v", crashed)
	}
	if crashed.CompletionRef != nil || crashed.FinishReason != "" || crashed.Result != nil {
		t.Fatalf("崩溃后的记录不该带终态内容: %+v", crashed)
	}

	completionIDs := managedCompletionIDs(t, dirs, ids.runID)
	if len(completionIDs) != 1 {
		t.Fatalf("磁盘上的终态 checkpoint 条数 = %d, 期望 1", len(completionIDs))
	}

	reconcile := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleClient, mode: managedModeReconcile, ids: ids,
	})
	report := reconcile.report(t, managedReportReconciled)
	if err := reconcile.finish(t); err != nil {
		t.Fatalf("修复客户端退出异常: %v\n%s", err, reconcile.output())
	}
	if !report.Repaired || report.Reason != string(ReconcileRepaired) || report.Attempts != 1 {
		t.Fatalf("修复回报 = %+v", report)
	}
	if report.Status != string(runstore.StatusSucceeded) || report.ResultText != text {
		t.Fatalf("修复结果 = %+v", report)
	}
	if report.CompletionID != completionIDs[0] {
		t.Fatalf("修复引用的快照 %q, 磁盘上是 %q", report.CompletionID, completionIDs[0])
	}

	// 磁盘重开：修复是持久事实。
	repaired := managedRecordOnDisk(t, dirs, ids.runID)
	if repaired.Status != runstore.StatusSucceeded || repaired.FinishReason != runstore.FinishReasonCompleted {
		t.Fatalf("重开后的状态 = %s/%s", repaired.Status, repaired.FinishReason)
	}
	if repaired.Result == nil || repaired.Result.Message.Text() != text {
		t.Fatalf("重开后的结果 = %+v", repaired.Result)
	}
	if repaired.CompletionRef == nil || repaired.CompletionRef.CheckpointID != completionIDs[0] {
		t.Fatalf("重开后的终态引用 = %+v", repaired.CompletionRef)
	}
	if repaired.Error != nil || repaired.RecordError != nil {
		t.Fatalf("修复后的记录不该带错误: %+v %+v", repaired.Error, repaired.RecordError)
	}
	if repaired.WorkerID != "worker-crash" || repaired.Version != crashed.Version+1 {
		t.Fatalf("修复改动了归属或版本: %q v%d → v%d", repaired.WorkerID, crashed.Version, repaired.Version)
	}

	// 再修复一次只报告已终结，版本不再前进。
	second := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleClient, mode: managedModeReconcile, ids: ids,
	})
	settled := second.report(t, managedReportReconciled)
	if err := second.finish(t); err != nil {
		t.Fatalf("第二次修复退出异常: %v\n%s", err, second.output())
	}
	if settled.Repaired || settled.Reason != string(ReconcileAlreadySettled) || settled.Version != repaired.Version {
		t.Fatalf("第二次修复 = %+v", settled)
	}
}

// TestManagedRunUnknownOutcomeIsNotReplayed 把 worker 停在"工具效果已经落盘、结果还没保存"
// 处杀掉，然后把同一份载荷交给第二个 worker 和一个把租约推到过期的第三方写法：
// 没有任何进程重跑这次执行，也没有任何进程替它编一个结局。
func TestManagedRunUnknownOutcomeIsNotReplayed(t *testing.T) {
	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("unknown")

	submit := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleProducer, mode: managedModeEnqueue, ids: ids, text: "不会再问一遍的答案",
	})
	submit.report(t, managedReportSubmitted)
	if err := submit.finish(t); err != nil {
		t.Fatalf("生产者退出异常: %v\n%s", err, submit.output())
	}

	first := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeEffect, ids: ids,
		workerID: "worker-died", text: "不会再问一遍的答案",
	})
	first.expectLine(t, managedLineEffectDone)
	first.kill(t)

	affected := managedEffectFiles(t, dirs)
	if len(affected) != 1 || managedModelCalls(t, dirs) != 1 {
		t.Fatalf("崩溃后的外部效果 = %v, 模型调用 = %d", affected, managedModelCalls(t, dirs))
	}
	stopped := managedRecordOnDisk(t, dirs, ids.runID)
	if stopped.Status != runstore.StatusRunning || stopped.CompletionRef != nil {
		t.Fatalf("崩溃后的记录 = %+v", stopped)
	}

	// 租约过期由父进程写下：这是"第三方把记录标成陈旧"，不是重启的依据。
	stale := managedMarkStale(t, dirs, stopped)
	if !stale.WorkerLeaseUntil.Before(time.Now().UTC()) || stale.Version != stopped.Version+1 {
		t.Fatalf("陈旧标记没有把租约推到过去: %+v", stale)
	}

	// 同一份载荷再交给一个新 worker：领取失败，模型一次都不再调。
	second := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeReply, ids: ids,
		workerID: "worker-second", text: "不会再问一遍的答案",
	})
	handled := second.report(t, managedReportHandled)
	if err := second.finish(t); err != nil {
		t.Fatalf("第二个 worker 退出异常: %v\n%s", err, second.output())
	}
	if handled.Executed || handled.ModelCalls != 0 {
		t.Fatalf("第二个 worker 重跑了: %+v", handled)
	}
	if handled.Status != string(runstore.StatusRunning) || handled.Version != stale.Version {
		t.Fatalf("第二个 worker 改动了记录: %+v", handled)
	}
	after := managedRecordOnDisk(t, dirs, ids.runID)
	if !after.WorkerLeaseUntil.Equal(stale.WorkerLeaseUntil) || after.WorkerID != "worker-died" {
		t.Fatalf("陈旧记录的归属或租约被动过: %+v", after)
	}
	if len(managedEffectFiles(t, dirs)) != 1 || managedModelCalls(t, dirs) != 1 {
		t.Fatalf("重放留下了痕迹: effects=%v 模型调用=%d", managedEffectFiles(t, dirs), managedModelCalls(t, dirs))
	}

	// 也没有证据可以替它收尾：显式修复只报告陈旧，记录原样不动。
	reconcile := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleClient, mode: managedModeReconcile, ids: ids,
	})
	report := reconcile.report(t, managedReportReconciled)
	if err := reconcile.finish(t); err != nil {
		t.Fatalf("修复客户端退出异常: %v\n%s", err, reconcile.output())
	}
	if report.Repaired || report.Reason != string(ReconcileStale) {
		t.Fatalf("未知结局的修复回报 = %+v", report)
	}
	if report.Status != string(runstore.StatusRunning) || report.Version != stale.Version {
		t.Fatalf("修复改动了记录: %+v", report)
	}
	if len(managedCompletionIDs(t, dirs, ids.runID)) != 0 {
		t.Fatal("没有终态快照的运行不该被查出来")
	}
}

// --- 场景 C：重复投递与未入队的排队记录 ---------------------------------------

// TestManagedRunDuplicateDeliveryAcrossProcesses 把同一份载荷同时交给两个 worker 进程：
// 领取是一次 CAS，所以恰好一方执行；跨进程的模型调用总数仍然是 1。
func TestManagedRunDuplicateDeliveryAcrossProcesses(t *testing.T) {
	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("duplicate")
	text := "只跑一次的答案"

	submit := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleProducer, mode: managedModeEnqueue, ids: ids, text: text,
	})
	submit.report(t, managedReportSubmitted)
	if err := submit.finish(t); err != nil {
		t.Fatalf("生产者退出异常: %v\n%s", err, submit.output())
	}

	first := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeReply, ids: ids, workerID: "worker-one", text: text,
	})
	second := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleWorker, mode: managedModeReply, ids: ids, workerID: "worker-two", text: text,
	})
	left := first.report(t, managedReportHandled)
	right := second.report(t, managedReportHandled)
	if err := first.finish(t); err != nil {
		t.Fatalf("worker-one 退出异常: %v\n%s", err, first.output())
	}
	if err := second.finish(t); err != nil {
		t.Fatalf("worker-two 退出异常: %v\n%s", err, second.output())
	}

	if left.Executed == right.Executed {
		t.Fatalf("两个 worker 的执行标记没分出赢家: %+v %+v", left, right)
	}
	// 落败的一方要报"重复"而不是报错：队列据此确认这条投递，重复投递才是无害的。
	loser := left
	if loser.Executed {
		loser = right
	}
	if loser.HandlerError != "" {
		t.Fatalf("重复投递被当成失败: %q", loser.HandlerError)
	}
	if loser.Status != string(runstore.StatusSucceeded) {
		t.Fatalf("重复投递看到的记录 = %s", loser.Status)
	}
	if calls := managedModelCalls(t, dirs); calls != 1 {
		t.Fatalf("重复投递后模型调用 %d 次, 期望 1", calls)
	}
	if workers := managedModelCallWorkers(t, dirs); len(workers) != 1 {
		t.Fatalf("模型调用来自 %v, 期望恰好一个进程", workers)
	}
	record := managedRecordOnDisk(t, dirs, ids.runID)
	if record.Status != runstore.StatusSucceeded || record.Result == nil || record.Result.Message.Text() != text {
		t.Fatalf("重复投递后的记录 = %+v", record)
	}
	winner := left
	if !winner.Executed {
		winner = right
	}
	if record.WorkerID != winner.WorkerID {
		t.Fatalf("记录归属 %q 与执行方 %q 不符", record.WorkerID, winner.WorkerID)
	}
}

// TestManagedRunQueuedRecordAfterProducerCrash 让生产者在建完记录之后、投递之前被杀：
// 留下的排队记录可诊断，重开的存储能读到它，而等它的客户端不会替它编结局。
func TestManagedRunQueuedRecordAfterProducerCrash(t *testing.T) {
	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("never-dispatched")

	producer := startManagedProcess(t, dirs, managedProcessSpec{
		role: managedRoleProducer, mode: managedModeCreateOnly, ids: ids, text: "从没开始的运行",
	})
	producer.expectLine(t, managedLineCreated)
	producer.kill(t)

	record := managedRecordOnDisk(t, dirs, ids.runID)
	if record.Status != runstore.StatusQueued || record.Version != 1 {
		t.Fatalf("崩溃留下的记录 = %s v%d", record.Status, record.Version)
	}
	if record.WorkerID != "" || record.Error != nil || record.Result != nil || record.CompletionRef != nil {
		t.Fatalf("没投递的运行不该有任何进度内容: %+v", record)
	}

	// 列表里看得见它，才算可诊断：重开存储用查询找,而不是靠生产者留下的变量。
	page, err := managedRunFileOnDisk(t, dirs).ListRuns(context.Background(), runstore.RunListOptions{
		ThreadID: ids.threadID,
	})
	if err != nil {
		t.Fatalf("列出排队记录: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].RunID != ids.runID || page.Items[0].Status != runstore.StatusQueued {
		t.Fatalf("查询结果 = %+v", page.Items)
	}

	// 等待有边界：超时把记录原样交回来，等待本身不会推动它。
	client, err := NewRunClient(managedRunsOnDisk(t, dirs), WithRunPollInterval(2*time.Millisecond), WithRunPollMax(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waited, err := client.WaitRun(waitCtx, ids.runID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等待排队记录应超时: %v", err)
	}
	if waited.Status != runstore.StatusQueued || waited.Version != record.Version {
		t.Fatalf("超时回报 = %+v", waited)
	}

	// 没有执行证据时显式修复也拒绝动它。
	report, err := mustRunReconciler(t, dirs).ReconcileRun(context.Background(), ids.runID)
	if err != nil {
		t.Fatalf("修复排队记录: %v", err)
	}
	if report.Repaired || report.Reason != ReconcileStale || report.Record.Version != record.Version {
		t.Fatalf("排队记录的修复回报 = %+v", report)
	}
}

// --- Redis：可选集成，同一条 Type/Payload 路径 --------------------------------

// TestManagedRunThroughRedisQueue 用生产入口 queue.New(WithRedis(url)) 跑同一条
// Type/Payload 链路。地址只从环境注入；没有环境或连不上都明确 skip —— Memory/File 跑绿
// 不代表 Redis 验过。
func TestManagedRunThroughRedisQueue(t *testing.T) {
	url := redisTestURL()
	if url == "" {
		t.Skip("Redis 集成测试需要环境变量 GOAGENT_REDIS_URL 或 REDIS_URL；本次只验证了 Memory/File 后端")
	}
	stream := "goagent:sm13:" + core.NewID("sm13")
	q, consumer, err := New(WithRedis(url), WithStream(stream), WithGroup("sm13-workers"))
	if err != nil {
		t.Skipf("Redis 地址无法建客户端: %v", err)
	}

	dirs := newManagedProcessDirs(t)
	ids := managedRunIDsFor("redis")
	text := "经 Redis 投递的答案"

	store := managedRunsOnDisk(t, dirs)
	cps := managedSnapshotsOnDisk(t, dirs)
	calls := &atomic.Int64{}
	handler, err := ManagedRunHandler(store, "worker-redis", fixedResolver(t, cps, text, calls),
		WithCancelPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewPool(consumer, 1).WithRegistry(Registry{ManagedRunJobType: handler})
	go pool.Run(ctx)

	sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
		AgentKey: managedProcessAgentKey, RunID: ids.runID, ThreadID: ids.threadID, TurnID: ids.turnID, Input: "跨后端提交",
	})
	if err != nil {
		cancel()
		t.Skipf("Redis 投递没有成功（实例不可用？）: %v", err)
	}

	client, err := NewRunClient(store, WithRunPollInterval(5*time.Millisecond), WithRunPollMax(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	record, err := client.WaitRun(waitCtx, sub.RunID)
	if err != nil {
		t.Fatalf("等待经 Redis 投递的运行: %v", err)
	}
	if record.Status != runstore.StatusSucceeded || record.Result == nil || record.Result.Message.Text() != text {
		t.Fatalf("经 Redis 的运行回报 = %+v", record)
	}
	if record.WorkerID != "worker-redis" || record.CompletionRef == nil {
		t.Fatalf("经 Redis 的运行归属或终态引用不对: %q %+v", record.WorkerID, record.CompletionRef)
	}
	if calls.Load() != 1 {
		t.Fatalf("模型调用 %d 次", calls.Load())
	}

	// 重开磁盘再看一遍，Redis 这条路径不留"只在内存里成功"的说法。
	reopened := managedRecordOnDisk(t, dirs, ids.runID)
	if reopened.Version != record.Version || reopened.Status != runstore.StatusSucceeded {
		t.Fatalf("重开后的记录 = %s v%d, 内存里是 v%d", reopened.Status, reopened.Version, record.Version)
	}
}

func redisTestURL() string {
	if url := os.Getenv("GOAGENT_REDIS_URL"); url != "" {
		return url
	}
	return os.Getenv("REDIS_URL")
}

// --- 子进程入口 --------------------------------------------------------------

func TestManagedRunProcessHelper(t *testing.T) {
	if os.Getenv(managedProcessHelperEnv) != "1" {
		return
	}
	env := readManagedProcessEnv()
	switch env.role {
	case managedRoleProducer:
		managedProcessProducer(t, env)
	case managedRoleWorker:
		managedProcessWorker(t, env)
	case managedRoleClient:
		managedProcessClient(t, env)
	default:
		t.Fatalf("unknown managed run helper role %q", env.role)
	}
}

type managedProcessEnv struct {
	role, mode, state, jobs, effects string
	ids                              managedRunIDs
	workerID, text                   string
}

func readManagedProcessEnv() managedProcessEnv {
	return managedProcessEnv{
		role:    os.Getenv(managedProcessRoleEnv),
		mode:    os.Getenv(managedProcessModeEnv),
		state:   os.Getenv(managedProcessStateEnv),
		jobs:    os.Getenv(managedProcessJobsEnv),
		effects: os.Getenv(managedProcessEffectsEnv),
		ids: managedRunIDs{
			runID:    os.Getenv(managedProcessRunEnv),
			threadID: os.Getenv(managedProcessThreadEnv),
			turnID:   os.Getenv(managedProcessTurnEnv),
			jobID:    os.Getenv(managedProcessJobIDEnv),
		},
		workerID: os.Getenv(managedProcessWorkerEnv),
		text:     os.Getenv(managedProcessTextEnv),
	}
}

func (e managedProcessEnv) jobFile() string {
	return filepath.Join(e.jobs, e.ids.jobID+".json")
}

func managedProcessProducer(t *testing.T, env managedProcessEnv) {
	t.Helper()
	runs, err := runstore.NewFile(env.state)
	if err != nil {
		t.Fatalf("worker 端打开任务存储: %v", err)
	}
	ctx := context.Background()
	req := ManagedRunRequest{
		AgentKey: managedProcessAgentKey,
		RunID:    env.ids.runID,
		ThreadID: env.ids.threadID,
		TurnID:   env.ids.turnID,
		JobID:    env.ids.jobID,
		Input:    "跨进程提交",
	}

	switch env.mode {
	case managedModeEnqueue:
		sub, err := EnqueueManagedAgent(ctx, fileJobQueue{dir: env.jobs}, runs, req)
		if err != nil {
			t.Fatalf("提交受管运行: %v", err)
		}
		record, err := runs.Get(ctx, sub.RunID)
		if err != nil {
			t.Fatalf("读回排队记录: %v", err)
		}
		managedPrintReport(managedReportSubmitted, managedReportOf(record))
	case managedModeCreateOnly:
		created, err := runs.Create(ctx, runstore.RunRecord{
			RunID: env.ids.runID, ThreadID: env.ids.threadID, TurnID: env.ids.turnID,
			JobID: env.ids.jobID, Status: runstore.StatusQueued,
		})
		if err != nil {
			t.Fatalf("建排队记录: %v", err)
		}
		managedPrintReport(managedReportCreated, managedReportOf(created))
		fmt.Fprintln(os.Stdout, managedLineCreated)
		holdUntilKilled()
	default:
		t.Fatalf("unknown producer mode %q", env.mode)
	}
}

func managedProcessWorker(t *testing.T, env managedProcessEnv) {
	t.Helper()
	runs, err := runstore.NewFile(env.state)
	if err != nil {
		t.Fatalf("worker 端打开任务存储: %v", err)
	}
	job := readManagedProcessJob(t, env.jobFile())

	recorder := &managedCallRecorder{effects: env.effects, workerID: env.workerID}
	a := managedProcessAgent(t, env, recorder)
	resolve := func(_ context.Context, key string) (*agent.Agent, error) {
		if key != managedProcessAgentKey {
			return nil, fmt.Errorf("queue test: resolver does not know agent %q", key)
		}
		return a, nil
	}

	var store runstore.Store = runs
	if env.mode == managedModeHoldPublish {
		store = &publishHoldStore{Store: runs}
	}
	handler, err := ManagedRunHandler(store, env.workerID, resolve,
		WithCancelPollInterval(5*time.Millisecond),
		WithHeartbeatInterval(time.Minute),
		WithWorkerLease(5*time.Minute),
		WithManagedRecordTimeout(time.Minute),
	)
	if err != nil {
		t.Fatalf("建 handler: %v", err)
	}

	handlerErr := handler(context.Background(), job.Payload)
	record, getErr := runs.Get(context.Background(), env.ids.runID)
	report := managedProcessReport{
		RunID: env.ids.runID, WorkerID: env.workerID, ModelCalls: recorder.count(),
	}
	if handlerErr != nil {
		report.HandlerError = handlerErr.Error()
	}
	if getErr != nil {
		report.Error = getErr.Error()
	} else {
		merged := managedReportOf(record)
		merged.WorkerID = env.workerID
		merged.ModelCalls = recorder.count()
		merged.HandlerError = report.HandlerError
		merged.Executed = record.WorkerID == env.workerID
		report = merged
	}
	managedPrintReport(managedReportHandled, report)
}

func managedProcessClient(t *testing.T, env managedProcessEnv) {
	t.Helper()
	runs, err := runstore.NewFile(env.state)
	if err != nil {
		t.Fatalf("客户端打开任务存储: %v", err)
	}
	ctx := context.Background()

	switch env.mode {
	case managedModeWait:
		client, err := NewRunClient(runs, WithRunPollInterval(5*time.Millisecond), WithRunPollMax(100*time.Millisecond))
		if err != nil {
			t.Fatalf("建 RunClient: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		record, err := client.WaitRun(waitCtx, env.ids.runID)
		report := managedReportOf(record)
		if err != nil {
			report.WaitError = err.Error()
		}
		managedPrintReport(managedReportWaited, report)
	case managedModeReconcile:
		cps, err := checkpoint.NewFile(env.state)
		if err != nil {
			t.Fatalf("客户端打开快照存储: %v", err)
		}
		reconciler, err := NewRunReconciler(runs, cps)
		if err != nil {
			managedPrintReport(managedReportReconciled, managedProcessReport{RunID: env.ids.runID, Error: err.Error()})
			return
		}
		report, err := reconciler.ReconcileRun(ctx, env.ids.runID)
		out := managedReportOf(report.Record)
		out.Repaired = report.Repaired
		out.Reason = string(report.Reason)
		out.Detail = report.Detail
		out.Attempts = report.Attempts
		if report.Evidence != nil {
			out.CompletionID = report.Evidence.CheckpointID
		}
		if err != nil {
			out.Error = err.Error()
		}
		managedPrintReport(managedReportReconciled, out)
	default:
		t.Fatalf("unknown client mode %q", env.mode)
	}
}

// managedCallRecorder 把"模型被调了一次"写成磁盘上的一个文件，而不是攒在进程内存里：
// 被杀掉的 worker 也要留下它跑过几次的事实，否则崩溃窗口无从断言。
type managedCallRecorder struct {
	effects  string
	workerID string
	calls    atomic.Int64
}

func (r *managedCallRecorder) note() {
	if r == nil {
		return
	}
	n := r.calls.Add(1)
	path := filepath.Join(r.effects, fmt.Sprintf("model-call-%s-%d.txt", r.workerID, n))
	if err := os.WriteFile(path, []byte(r.workerID+"\n"), 0o644); err != nil {
		fmt.Fprintf(os.Stdout, "model-call-record-failed %v\n", err)
	}
}

func (r *managedCallRecorder) count() int64 {
	if r == nil {
		return 0
	}
	return r.calls.Load()
}

func managedProcessAgent(t *testing.T, env managedProcessEnv, recorder *managedCallRecorder) *agent.Agent {
	t.Helper()
	cps, err := checkpoint.NewFile(env.state)
	if err != nil {
		t.Fatalf("worker 端打开快照存储: %v", err)
	}

	var model llm.Model
	switch env.mode {
	case managedModeReply, managedModeHoldPublish:
		model = mock.New("managed-process", func(*llm.Request) *llm.Response {
			recorder.note()
			return mock.Text(env.text)
		})
	default:
		model = mock.New("managed-process", func(req *llm.Request) *llm.Response {
			recorder.note()
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text(env.text)
			}
			return mock.CallTool("call-1", "gate", `{}`)
		})
	}

	opts := []agent.Option{
		agent.WithCheckpointer(cps),
		agent.WithModel(model),
		agent.WithMaxTurns(4),
		agent.WithToolTimeout(10 * time.Minute),
	}
	switch env.mode {
	case managedModeGate:
		opts = append(opts, agent.WithTools(processGateTool()))
	case managedModeEffect:
		opts = append(opts, agent.WithTools(processEffectTool(env.effects, env.workerID)))
	}

	a, err := agent.New(opts...)
	if err != nil {
		t.Fatalf("建 agent: %v", err)
	}
	return a
}

// processGateTool 打印 barrier 后停在工具里，直到执行 context 被取消：worker 自己的轮询
// 看到落盘的取消请求才会取消执行，所以这一停正好证明取消来自另一个进程。
func processGateTool() tool.Tool {
	var once sync.Once
	return tool.New("gate", "打印 barrier 后停在工具里", func(ctx *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { fmt.Fprintln(os.Stdout, managedLineToolEntered) })
		<-ctx.Done()
		return "", ctx.Err()
	})
}

// processEffectTool 先把外部效果落盘，再停在任何保存之前：父进程杀掉它，就得到一个"效果
// 已经发生、结局没人知道"的窗口。
func processEffectTool(effectsDir, workerID string) tool.Tool {
	var once sync.Once
	return tool.New("gate", "落一份外部效果后停在保存之前", func(_ *tool.Context, _ struct{}) (string, error) {
		once.Do(func() {
			path := filepath.Join(effectsDir, "effect-"+workerID+".txt")
			if err := os.WriteFile(path, []byte(workerID+"\n"), 0o644); err != nil {
				fmt.Fprintln(os.Stdout, "effect-failed")
				return
			}
			fmt.Fprintln(os.Stdout, managedLineEffectDone)
			holdUntilKilled()
		})
		return "", nil
	})
}

// publishHoldStore 把终态发布这一步卡在门外，非终态写入照常放行：留下的形状就是
// "快照已经提交、任务记录还没终结"。
type publishHoldStore struct {
	runstore.Store
}

func (s *publishHoldStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	if next.Status.Terminal() {
		fmt.Fprintln(os.Stdout, managedLinePrePublish)
		holdUntilKilled()
	}
	return s.Store.CompareAndSwap(ctx, runID, expected, next)
}

// holdUntilKilled 让子进程停在故障点上直到父进程杀它。计时器保证运行时不会把它判成死锁，
// 心跳协程也就照常活着。
func holdUntilKilled() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
	}
}

func managedPrintReport(tag string, report managedProcessReport) {
	data, err := json.Marshal(report)
	if err != nil {
		data = []byte(`{"error":"cannot encode report"}`)
	}
	fmt.Fprintf(os.Stdout, "%s %s\n", tag, data)
}

// --- 测试里的作业搬运 ---------------------------------------------------------

// fileJobQueue 只把 Type/Payload 原样落到共享目录，让另一个进程读到完全相同的字节。
// 跨进程搬运在生产代码里是 Redis Streams（见 TestManagedRunThroughRedisQueue），这里
// 不为测试再造一份后端，只造一条能落盘的传递。
type fileJobQueue struct {
	dir string
}

type fileJobRecord struct {
	ID      string `json:"id"`
	Key     string `json:"key"`
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}

func (q fileJobQueue) Enqueue(_ context.Context, job Job) error {
	if job.Run != nil {
		return fmt.Errorf("queue test: file job transport accepts no closure job")
	}
	data, err := json.Marshal(fileJobRecord{ID: job.ID, Key: job.Key, Type: job.Type, Payload: job.Payload})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(q.dir, "job-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, filepath.Join(q.dir, job.ID+".json")); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

var _ Queue = fileJobQueue{}

func readManagedProcessJob(t *testing.T, path string) Job {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读作业文件: %v", err)
	}
	var raw fileJobRecord
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("解作业文件: %v", err)
	}
	if raw.Type != ManagedRunJobType {
		t.Fatalf("作业类型 = %q, want %q", raw.Type, ManagedRunJobType)
	}
	return Job{ID: raw.ID, Key: raw.Key, Type: raw.Type, Payload: raw.Payload}
}

// --- 父进程侧的编排 -----------------------------------------------------------

type managedRunIDs struct {
	runID, threadID, turnID, jobID string
}

func managedRunIDsFor(name string) managedRunIDs {
	return managedRunIDs{
		runID:    "run-sm13-" + name,
		threadID: "thread-sm13-" + name,
		turnID:   "turn-sm13-" + name,
		jobID:    "job-sm13-" + name,
	}
}

type managedProcessDirs struct {
	state   string
	jobs    string
	effects string
}

func newManagedProcessDirs(t *testing.T) managedProcessDirs {
	t.Helper()
	root := t.TempDir()
	dirs := managedProcessDirs{
		state:   filepath.Join(root, "state"),
		jobs:    filepath.Join(root, "jobs"),
		effects: filepath.Join(root, "effects"),
	}
	for _, dir := range []string{dirs.state, dirs.jobs, dirs.effects} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("建 %s: %v", dir, err)
		}
	}
	return dirs
}

// managedRunsOnDisk 每次都新开一个存储句柄：断言的是磁盘，不是上一个句柄的缓存。
func managedRunsOnDisk(t *testing.T, dirs managedProcessDirs) runstore.Store {
	t.Helper()
	return managedRunFileOnDisk(t, dirs)
}

func managedRunFileOnDisk(t *testing.T, dirs managedProcessDirs) *runstore.File {
	t.Helper()
	store, err := runstore.NewFile(dirs.state)
	if err != nil {
		t.Fatalf("重开任务存储: %v", err)
	}
	return store
}

func managedSnapshotsOnDisk(t *testing.T, dirs managedProcessDirs) checkpoint.Checkpointer {
	t.Helper()
	store, err := checkpoint.NewFile(dirs.state)
	if err != nil {
		t.Fatalf("重开快照存储: %v", err)
	}
	return store
}

func managedRecordOnDisk(t *testing.T, dirs managedProcessDirs, runID string) runstore.RunRecord {
	t.Helper()
	record, err := managedRunsOnDisk(t, dirs).Get(context.Background(), runID)
	if err != nil {
		t.Fatalf("重开磁盘读记录 %s: %v", runID, err)
	}
	return record
}

// managedCompletions 按 RunID 查这条运行留下的终态摘要。返回摘要本身： kinds 和快照 id
// 都要能各自断言，不能只给一个计数。
func managedCompletions(t *testing.T, dirs managedProcessDirs, runID string) []checkpoint.CheckpointMetadata {
	t.Helper()
	store, err := checkpoint.NewFile(dirs.state)
	if err != nil {
		t.Fatalf("重开快照存储: %v", err)
	}
	page, err := store.QueryCheckpoints(context.Background(), checkpoint.ListOptions{
		RunID: runID, Limit: checkpoint.MaxQueryLimit,
	})
	if err != nil {
		t.Fatalf("按 RunID 查快照: %v", err)
	}
	var out []checkpoint.CheckpointMetadata
	for _, item := range page.Items {
		if item.CompletionKind != "" {
			out = append(out, item)
		}
	}
	return out
}

func managedCompletionIDs(t *testing.T, dirs managedProcessDirs, runID string) []string {
	t.Helper()
	var out []string
	for _, item := range managedCompletions(t, dirs, runID) {
		out = append(out, item.Ref.CheckpointID)
	}
	return out
}

func managedEffectFiles(t *testing.T, dirs managedProcessDirs) []string {
	t.Helper()
	entries, err := os.ReadDir(dirs.effects)
	if err != nil {
		t.Fatalf("读效果目录: %v", err)
	}
	var out []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "effect-") {
			out = append(out, entry.Name())
		}
	}
	sort.Strings(out)
	return out
}

// managedModelCalls 数磁盘上的模型调用凭据：每次调用落一个文件，谁调的写在名字里。
// 被杀掉的进程来不及回报，但已经发生过一次调用就必然留下一个文件。
func managedModelCalls(t *testing.T, dirs managedProcessDirs) int64 {
	t.Helper()
	entries, err := os.ReadDir(dirs.effects)
	if err != nil {
		t.Fatalf("读效果目录: %v", err)
	}
	var total int64
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "model-call-") {
			total++
		}
	}
	return total
}

func managedModelCallWorkers(t *testing.T, dirs managedProcessDirs) []string {
	t.Helper()
	entries, err := os.ReadDir(dirs.effects)
	if err != nil {
		t.Fatalf("读效果目录: %v", err)
	}
	seen := map[string]bool{}
	var out []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "model-call-") {
			continue
		}
		trimmed := strings.TrimPrefix(name, "model-call-")
		worker := trimmed[:strings.LastIndex(trimmed, "-")]
		if !seen[worker] {
			seen[worker] = true
			out = append(out, worker)
		}
	}
	sort.Strings(out)
	return out
}

// managedMarkStale 由父进程替一个已经死掉的 worker 把租约推到过去：这是第三条腿写下的事实，
// 用来检查"陈旧"会不会被谁当成重启的依据。
func managedMarkStale(t *testing.T, dirs managedProcessDirs, current runstore.RunRecord) runstore.RunRecord {
	t.Helper()
	next := current
	next.WorkerLeaseUntil = time.Now().UTC().Add(-time.Hour)
	updated, err := managedRunsOnDisk(t, dirs).CompareAndSwap(context.Background(), current.RunID, current.Version, next)
	if err != nil {
		t.Fatalf("把记录标成陈旧: %v", err)
	}
	return updated
}

func mustRunReconciler(t *testing.T, dirs managedProcessDirs) *RunReconciler {
	t.Helper()
	reconciler, err := NewRunReconciler(managedRunsOnDisk(t, dirs), managedSnapshotsOnDisk(t, dirs))
	if err != nil {
		t.Fatalf("建修复器: %v", err)
	}
	return reconciler
}

func managedReportOf(record runstore.RunRecord) managedProcessReport {
	out := managedProcessReport{
		RunID:        record.RunID,
		JobID:        record.JobID,
		Status:       string(record.Status),
		FinishReason: string(record.FinishReason),
		Version:      record.Version,
		RecordWorker: record.WorkerID,
	}
	if record.Result != nil {
		out.HasResult = true
		out.ResultText = record.Result.Message.Text()
	}
	if record.Error != nil {
		out.ErrorCode = record.Error.Code
	}
	out.CancelRequested = !record.CancelRequestedAt.IsZero()
	if record.CompletionRef != nil {
		out.CompletionID = record.CompletionRef.CheckpointID
	}
	return out
}

// fixedResolver 给 Redis 集成用例在同一个进程里返回一个固定配置的 agent。
func fixedResolver(t *testing.T, cps checkpoint.Checkpointer, text string, calls *atomic.Int64) AgentResolver {
	t.Helper()
	return func(_ context.Context, key string) (*agent.Agent, error) {
		if key != managedProcessAgentKey {
			return nil, fmt.Errorf("queue test: resolver does not know agent %q", key)
		}
		a, err := agent.New(
			agent.WithCheckpointer(cps),
			agent.WithModel(mock.New("managed-redis", func(*llm.Request) *llm.Response {
				calls.Add(1)
				return mock.Text(text)
			})),
		)
		if err != nil {
			return nil, err
		}
		return a, nil
	}
}

// --- 子进程句柄 ---------------------------------------------------------------

type managedProcessSpec struct {
	role, mode, workerID, text string
	ids                        managedRunIDs
}

type managedProcess struct {
	cmd     *exec.Cmd
	lines   chan string
	stderr  *bytes.Buffer
	mu      sync.Mutex
	seen    []string
	pending bool
}

func startManagedProcess(t *testing.T, dirs managedProcessDirs, spec managedProcessSpec) *managedProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("定位测试二进制失败: %v", err)
	}

	args := []string{"-test.run=^TestManagedRunProcessHelper$", "-test.v=false", "-test.timeout=120s"}
	env := []string{
		managedProcessHelperEnv + "=1",
		managedProcessRoleEnv + "=" + spec.role,
		managedProcessModeEnv + "=" + spec.mode,
		managedProcessStateEnv + "=" + dirs.state,
		managedProcessJobsEnv + "=" + dirs.jobs,
		managedProcessEffectsEnv + "=" + dirs.effects,
		managedProcessRunEnv + "=" + spec.ids.runID,
		managedProcessThreadEnv + "=" + spec.ids.threadID,
		managedProcessTurnEnv + "=" + spec.ids.turnID,
		managedProcessJobIDEnv + "=" + spec.ids.jobID,
		managedProcessWorkerEnv + "=" + spec.workerID,
		managedProcessTextEnv + "=" + spec.text,
	}
	sort.Strings(env)

	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), env...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("接子进程 stdout: %v", err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 %s/%s 子进程: %v", spec.role, spec.mode, err)
	}

	p := &managedProcess{cmd: cmd, stderr: stderr, lines: make(chan string, 256), pending: true}
	go p.scan(stdout)
	t.Cleanup(p.stop)
	return p
}

func (p *managedProcess) scan(r io.Reader) {
	buffer := bufio.NewScanner(r)
	buffer.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for buffer.Scan() {
		line := buffer.Text()
		p.mu.Lock()
		p.seen = append(p.seen, line)
		p.mu.Unlock()
		p.lines <- line
	}
	close(p.lines)
}

// expectLine 等到一行完全等于 want 的 barrier。父进程只在看到它之后才动手，所以停在哪一步
// 是确定的。
func (p *managedProcess) expectLine(t *testing.T, want string) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("子进程在打印 %q 之前退出了:\n%s", want, p.output())
			}
			if line == want {
				return
			}
		case <-deadline.C:
			t.Fatalf("等 barrier %q 超时:\n%s", want, p.output())
		}
	}
}

// report 等到带 tag 的那一行 JSON 并解出来。
func (p *managedProcess) report(t *testing.T, tag string) managedProcessReport {
	t.Helper()
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("子进程没打印 %q:\n%s", tag, p.output())
			}
			if !strings.HasPrefix(line, tag+" ") {
				continue
			}
			var out managedProcessReport
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, tag+" ")), &out); err != nil {
				t.Fatalf("解析 %q 回报 %q: %v\n%s", tag, line, err, p.output())
			}
			return out
		case <-deadline.C:
			t.Fatalf("等 %q 回报超时:\n%s", tag, p.output())
		}
	}
}

// kill 在 barrier 之后杀掉子进程：租约、锁、未完成的写都由操作系统回收。被杀的一方退出码
// 非零是预期内的，所以这里只确认进程真的没了，不把它当成断言失败。
func (p *managedProcess) kill(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil || p.cmd.ProcessState != nil {
		t.Fatalf("子进程没有在跑，无法注入崩溃")
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("杀掉子进程失败: %v", err)
	}
	p.drain(t)
	if err := p.cmd.Wait(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("被杀的子进程返回值不是退出码: %v\n%s", err, p.output())
		}
	}
	p.markDone()
}

// finish 正常收尾：收完剩余输出再 Wait，返回进程的退出错误。
func (p *managedProcess) finish(t *testing.T) error {
	t.Helper()
	p.drain(t)
	err := p.cmd.Wait()
	p.markDone()
	return err
}

func (p *managedProcess) markDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = false
}

// drain 读到 EOF 为止：子进程退出后扫描协程必然结束。
func (p *managedProcess) drain(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, ok := <-p.lines:
			if !ok {
				return
			}
		case <-deadline.C:
			t.Fatalf("子进程输出没读完:\n%s", p.output())
		}
	}
}

func (p *managedProcess) stop() {
	p.mu.Lock()
	running := p.pending
	p.mu.Unlock()
	if !running {
		return
	}
	if p.cmd.Process != nil && p.cmd.ProcessState == nil {
		_ = p.cmd.Process.Kill()
	}
	_ = p.cmd.Wait()
}

func (p *managedProcess) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var builder strings.Builder
	builder.WriteString("stdout:\n")
	for _, line := range p.seen {
		builder.WriteString("  " + line + "\n")
	}
	builder.WriteString("stderr:\n")
	builder.WriteString(p.stderr.String())
	return builder.String()
}
