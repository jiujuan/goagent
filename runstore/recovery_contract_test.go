package runstore

// SM-13（ADR-0034 D4、D10）：崩溃之后的任务记录契约。
//
// 这里只问一件事：一个进程写了什么、死在哪里，另一个进程重开磁盘之后能看见什么、能改什么。
// 归属是一个令牌而不是"进程还活着"，所以死掉的主人留下的记录既不能被新人抢走，也不会被谁
// 自动判死；一次没投递出去的运行留下的排队记录，既不缺证也不越证。
//
// 沿用本包既有的做法：用 os.Executable 重跑测试二进制当第二个进程，Windows 与 Unix 都不跳过，
// 因为后端两边都用 OS 锁。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const runRecoveryHelperEnv = "GOAGENT_RUN_RECOVERY_HELPER"

const (
	runRecoveryDirEnv    = "GOAGENT_RUN_RECOVERY_DIR"
	runRecoveryRunEnv    = "GOAGENT_RUN_RECOVERY_RUN"
	runRecoveryWorkerEnv = "GOAGENT_RUN_RECOVERY_WORKER"
	runRecoveryActionEnv = "GOAGENT_RUN_RECOVERY_ACTION"
)

// 子进程要做的那一次写。
const (
	runRecoveryActionClaim   = "claim"
	runRecoveryActionSteal   = "steal"
	runRecoveryActionRestate = "restate"
)

type runRecoveryOutcome struct {
	Action  string `json:"action"`
	Worker  string `json:"worker"`
	Version uint64 `json:"version"`
	Status  string `json:"status"`
	Err     string `json:"err"`
}

// TestRunFileRecoveryProcessHelper 是子进程一侧：读记录、等父进程放行、做它那一种写，
// 然后把结果打成一行 JSON。
func TestRunFileRecoveryProcessHelper(t *testing.T) {
	if os.Getenv(runRecoveryHelperEnv) != "1" {
		return
	}
	store, err := NewFile(os.Getenv(runRecoveryDirEnv))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	runID := os.Getenv(runRecoveryRunEnv)
	worker := os.Getenv(runRecoveryWorkerEnv)
	action := os.Getenv(runRecoveryActionEnv)

	latest, err := store.Get(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	command, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if command != "go\n" {
		t.Fatalf("helper command = %q, want go", command)
	}

	next := latest
	out := runRecoveryOutcome{Action: action, Worker: worker}
	switch action {
	case runRecoveryActionClaim:
		next.Status = StatusRunning
		next.WorkerID = worker
		next.WorkerEpoch = 1
		next.WorkerLeaseUntil = latest.CreatedAt.Add(time.Minute)
		next.HeartbeatAt = latest.CreatedAt
	case runRecoveryActionSteal:
		// 换一个主人：归属是令牌，不是"前任已经死了"就能改的东西。
		next.WorkerID = worker
		next.WorkerEpoch = latest.WorkerEpoch + 1
	case runRecoveryActionRestate:
		// 原样带上死掉主人的身份，只推进心跳：这是修复者的写法。
		next.WorkerID = worker
		next.WorkerEpoch = latest.WorkerEpoch
		next.HeartbeatAt = latest.CreatedAt.Add(2 * time.Minute)
		next.WorkerLeaseUntil = latest.CreatedAt.Add(3 * time.Minute)
	default:
		t.Fatalf("unknown recovery action %q", action)
	}

	updated, err := store.CompareAndSwap(ctx, runID, latest.Version, next)
	out.Status = string(updated.Status)
	out.Version = updated.Version
	if err != nil {
		out.Err = err.Error()
	}
	if encodeErr := json.NewEncoder(os.Stdout).Encode(out); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	// 成功的一方停在这里，等父进程把它杀掉：死在哪一步必须是确定的。
	if err == nil && action != runRecoveryActionRestate {
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	}
}

// TestRunFileDeadOwnerCannotBeReplaced 让子进程领取并停在 running 后被杀掉，父进程重开磁盘：
// 记录还是那个死主人的，别的 worker 抢不走；带着同一身份重写可以；两种写法各自在 journal 上
// 留下恰好一条事件。
func TestRunFileDeadOwnerCannotBeReplaced(t *testing.T) {
	stubClock(t, testBase(), time.Minute)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)
	created := mustCreate(t, store, recordIn("dead-owner", StatusQueued, testBase()))
	if created.Version != 1 {
		t.Fatalf("初始版本 = %d", created.Version)
	}

	claimed := runRecoveryChild(t, dir, "dead-owner", "dead-worker", runRecoveryActionClaim)
	if claimed.Err != "" || claimed.Version != 2 || claimed.Status != string(StatusRunning) {
		t.Fatalf("子进程领取失败: %+v", claimed)
	}
	if lines := journalLines(t, dir, "dead-owner"); lines != 2 {
		t.Fatalf("领取后 journal 行数 = %d, 期望 2", lines)
	}

	// 抢归属：必须被拒，而且拒完磁盘上一个字节都没多。
	stolen := runRecoveryChild(t, dir, "dead-owner", "thief-worker", runRecoveryActionSteal)
	if stolen.Err == "" {
		t.Fatalf("别的 worker 抢到了死主人的记录: %+v", stolen)
	}
	if !strings.Contains(stolen.Err, ErrOwnershipLost.Error()) {
		t.Fatalf("抢归属的错误 = %q, 期望带 %q", stolen.Err, ErrOwnershipLost.Error())
	}
	if after := journalLines(t, dir, "dead-owner"); after != 2 {
		t.Fatalf("被拒的写还是落了一条事件: %d 行", after)
	}

	// 带上原主人身份的重写是被允许的：这就是修复者的形状。
	restate := newFileStore(t, dir)
	latest, err := restate.Get(ctx, "dead-owner")
	if err != nil {
		t.Fatal(err)
	}
	if latest.WorkerID != "dead-worker" || latest.WorkerEpoch != 1 {
		t.Fatalf("死主人的归属被改动: %q epoch %d", latest.WorkerID, latest.WorkerEpoch)
	}
	next := latest
	next.HeartbeatAt = testBase().Add(5 * time.Minute)
	next.WorkerLeaseUntil = testBase().Add(6 * time.Minute)
	updated, err := restate.CompareAndSwap(ctx, "dead-owner", latest.Version, next)
	if err != nil {
		t.Fatalf("带原主人身份重写: %v", err)
	}
	if updated.Version != 3 || updated.Status != StatusRunning || updated.WorkerID != "dead-worker" {
		t.Fatalf("重写后的记录 = %s %q v%d", updated.Status, updated.WorkerID, updated.Version)
	}

	// 重开一次磁盘：状态与 journal 行数都是同一件事。
	reopened := newFileStore(t, dir)
	got, err := reopened.Get(ctx, "dead-owner")
	if err != nil {
		t.Fatalf("重开读记录: %v", err)
	}
	if got.Version != updated.Version || !got.HeartbeatAt.Equal(next.HeartbeatAt) {
		t.Fatalf("重开后的记录 = %+v", got)
	}
	if lines := journalLines(t, dir, "dead-owner"); lines != 3 {
		t.Fatalf("journal 行数 = %d, 期望 3", lines)
	}
}

// TestRunFileUndispatchedRunStaysQueued 覆盖"建了记录却没投递"这一格：另一个进程重开只能
// 看见排队事实本身——没归属、没心跳、没结局；照抄同一内容再建一次是幂等，换个内容建则被拒。
// 取消请求仍然写得进去，因为排队中的运行没有主人要尊重。
func TestRunFileUndispatchedRunStaysQueued(t *testing.T) {
	stubClock(t, testBase(), time.Minute)
	dir := t.TempDir()
	ctx := context.Background()

	first := newFileStore(t, dir)
	wanted := recordIn("never-picked", StatusQueued, testBase())
	created := mustCreate(t, first, wanted)

	// 重开的第二个进程：看到的就是排队本身。
	second := newFileStore(t, dir)
	got, err := second.Get(ctx, "never-picked")
	if err != nil {
		t.Fatalf("重开读排队记录: %v", err)
	}
	if got.Status != StatusQueued || got.WorkerID != "" || got.WorkerEpoch != 0 || !got.HeartbeatAt.IsZero() {
		t.Fatalf("排队记录带了进度: %+v", got)
	}
	if got.Error != nil || got.Result != nil || got.CompletionRef != nil || got.RecordError != nil {
		t.Fatalf("排队记录编造了结局: %+v", got)
	}

	// 同一内容再建一次：幂等回放，版本不前进。
	replay := mustCreate(t, second, wanted)
	if replay.Version != created.Version || !replay.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("同内容重建改写了记录: v%d vs v%d", replay.Version, created.Version)
	}

	// 换个内容建同一个 RunID：必须拒。改身份（TurnID）也一律拒。
	different := wanted
	different.TurnID = "another-turn"
	if _, err := second.Create(ctx, different); !errors.Is(err, ErrConflict) {
		t.Fatalf("同 RunID 换内容建记录应被拒: %v", err)
	}
	renameAttempt := got
	renameAttempt.TurnID = "another-turn"
	if _, err := second.CompareAndSwap(ctx, "never-picked", got.Version, renameAttempt); !errors.Is(err, ErrConflict) {
		t.Fatalf("改写 TurnID 应被拒: %v", err)
	}
	untouched := mustGet(t, second, "never-picked")
	if untouched.Version != created.Version || untouched.TurnID != created.TurnID {
		t.Fatalf("被拒的写改动了磁盘: %+v", untouched)
	}

	// 排队记录接受一个取消请求：没有主人要尊重，请求写得进去且不会被谁自动判成终态。
	cancelling := newFileStore(t, dir)
	latest := mustGet(t, cancelling, "never-picked")
	next := latest
	next.CancelRequestedAt = testBase().Add(10 * time.Minute)
	requested, err := cancelling.CompareAndSwap(ctx, "never-picked", latest.Version, next)
	if err != nil {
		t.Fatalf("请求取消排队记录: %v", err)
	}
	if requested.CancelRequestedAt.IsZero() || requested.Status != StatusQueued {
		t.Fatalf("取消请求后的记录 = %s at %v", requested.Status, requested.CancelRequestedAt)
	}
	if lines := journalLines(t, dir, "never-picked"); lines != 2 {
		t.Fatalf("journal 行数 = %d, 期望 2（建 + 请求取消）", lines)
	}

	// 列表也能看见它：排队不是一个只能靠 RunID 才能发现的死角。摘要只报状态与"有没有结果"，
	// 不带结果正文。
	page, err := newFileStore(t, dir).ListRuns(ctx, RunListOptions{ThreadID: latest.ThreadID})
	if err != nil {
		t.Fatalf("列排队记录: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].RunID != "never-picked" || page.Items[0].Status != StatusQueued {
		t.Fatalf("列表 = %+v", page.Items)
	}
	if page.Items[0].HasResult || page.Items[0].ErrorCode != "" || page.Items[0].RecordErrorCode != "" {
		t.Fatalf("列表项编造了结局: %+v", page.Items[0])
	}
	if !page.Items[0].CancelRequestedAt.Equal(requested.CancelRequestedAt) {
		t.Fatalf("列表里的取消请求时间 = %v, 磁盘上是 %v", page.Items[0].CancelRequestedAt, requested.CancelRequestedAt)
	}
}

// --- 夹具 ---------------------------------------------------------------------

// runRecoveryChild 起子进程做一种写，等它 ready、放行、读回结果，然后杀掉停在写之后的它。
func runRecoveryChild(t *testing.T, dir, runID, worker, action string) runRecoveryOutcome {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("定位测试二进制失败: %v", err)
	}
	childCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(childCtx, exe, "-test.run=^TestRunFileRecoveryProcessHelper$", "-test.v=false", "-test.timeout=60s")
	cmd.Env = append(os.Environ(),
		runRecoveryHelperEnv+"=1",
		runRecoveryDirEnv+"="+dir,
		runRecoveryRunEnv+"="+runID,
		runRecoveryWorkerEnv+"="+worker,
		runRecoveryActionEnv+"="+action,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("接子进程 stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("接子进程 stdout: %v", err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("等子进程 ready: %v\nstderr:\n%s", err, stderr.String())
	}
	if line != "ready\n" {
		t.Fatalf("子进程输出 = %q, want ready", line)
	}
	if _, err := fmt.Fprintln(stdin, "go"); err != nil {
		t.Fatalf("放行子进程: %v", err)
	}

	var outcome runRecoveryOutcome
	data, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("读子进程结果: %v\nstderr:\n%s", err, stderr.String())
	}
	if err := json.Unmarshal(data, &outcome); err != nil {
		t.Fatalf("解析子进程结果 %q: %v", data, err)
	}

	// 写完就停的那一方必须被杀掉：它留在原地，父进程才有"前任已死"这个前提。先杀再关
	// stdin——反过来会先放它自己退出，而 Windows 上 TerminateProcess 打在一个已退出的
	// 进程上返回 Access is denied。错误结局的写完就走，本来就不需要杀。
	if outcome.Err == "" && action != runRecoveryActionRestate {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("杀掉子进程: %v", err)
		}
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("子进程返回值不是退出码: %v\nstderr:\n%s", err, stderr.String())
		}
	}
	return outcome
}

func journalLines(t *testing.T, dir, runID string) int {
	t.Helper()
	return bytes.Count([]byte(readJournal(t, dir, runID)), []byte("\n"))
}

func mustGet(t *testing.T, s Store, runID string) RunRecord {
	t.Helper()
	record, err := s.Get(context.Background(), runID)
	if err != nil {
		t.Fatalf("读记录 %s: %v", runID, err)
	}
	return record
}
