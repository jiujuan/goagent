package runstore

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
	"sync"
	"testing"
	"time"
)

// This file exercises the cross-process half of the run store: two OS processes doing a
// compare-and-swap on the same run must not rely on a mutex in either one of them.
//
// Like the checkpoint package's cross-process test, it re-executes the test binary with a
// helper guard rather than shipping a second program, and it is not skipped on Windows —
// the lock file uses O_EXCL precisely so this works everywhere the store runs.

const runProcessHelperEnv = "GOAGENT_RUN_PROCESS_HELPER"

const (
	runProcessDirEnv    = "GOAGENT_RUN_PROCESS_DIR"
	runProcessRunIDEnv  = "GOAGENT_RUN_PROCESS_RUN"
	runProcessWorkerEnv = "GOAGENT_RUN_PROCESS_WORKER"
)

type runProcessOutcome struct {
	Worker string `json:"worker"`
	Err    string `json:"err"`
}

// TestRunFileProcessHelper is the child side: it reads the record, waits for the parent's
// go signal, then does its own compare-and-swap and reports the outcome as one JSON line.
func TestRunFileProcessHelper(t *testing.T) {
	if os.Getenv(runProcessHelperEnv) != "1" {
		return
	}
	dir := os.Getenv(runProcessDirEnv)
	runID := os.Getenv(runProcessRunIDEnv)
	worker := os.Getenv(runProcessWorkerEnv)

	store, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
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
	next.Status = StatusRunning
	next.WorkerID = worker
	next.WorkerEpoch = 1
	_, err = store.CompareAndSwap(ctx, runID, latest.Version, next)
	outcome := runProcessOutcome{Worker: worker}
	if err != nil {
		outcome.Err = err.Error()
	}
	if encodeErr := json.NewEncoder(os.Stdout).Encode(outcome); encodeErr != nil {
		t.Fatal(encodeErr)
	}
}

type runProcessChild struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *io.PipeReader
	stderr *bytes.Buffer
	done   chan struct{}
	wait   sync.Once
	err    error
}

// TestRunFileCASAcrossProcesses 让两个进程在同一个版本上各写一次：只有一方成功，落盘的
// journal 只有一条新事件，双方都不靠本进程 mutex 达成一致。
func TestRunFileCASAcrossProcesses(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)
	created := mustCreate(t, store, recordIn("cross-process", StatusQueued, testBase()))

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("定位测试二进制失败: %v", err)
	}
	childCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(childCtx, exe, "-test.run=^TestRunFileProcessHelper$", "-test.v=false")
	cmd.Env = append(os.Environ(),
		runProcessHelperEnv+"=1",
		runProcessDirEnv+"="+dir,
		runProcessRunIDEnv+"=cross-process",
		runProcessWorkerEnv+"=child-worker",
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("接子进程 stdin 失败: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("接子进程 stdout 失败: %v", err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程失败: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	// 子进程先读到 v1 并回报 ready（它读到的是同一条 journal 事实）。
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("等子进程 ready 失败: %v\nstderr:\n%s", err, stderr.String())
	}
	if got := line; got != "ready\n" {
		t.Fatalf("子进程输出 = %q, want ready", got)
	}

	// 同时放行：两边都在 version 1 上做 CAS，谁先拿到跨进程锁由系统决定。
	childOutcome := runProcessOutcome{}
	var childErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		outLine, readErr := reader.ReadString('\n')
		if readErr != nil {
			childErr = readErr
			return
		}
		if err := json.Unmarshal([]byte(outLine), &childOutcome); err != nil {
			childErr = fmt.Errorf("解析子进程结果 %q: %w", outLine, err)
		}
	}()
	if _, err := fmt.Fprintln(stdin, "go"); err != nil {
		t.Fatalf("放行子进程失败: %v", err)
	}
	_ = stdin.Close()

	parentNext := created
	parentNext.Status = StatusRunning
	parentNext.WorkerID = "parent-worker"
	parentNext.WorkerEpoch = 1
	_, parentErr := store.CompareAndSwap(ctx, "cross-process", created.Version, parentNext)
	<-done

	if childErr != nil {
		t.Fatalf("子进程回报失败: %v\nstderr:\n%s", childErr, stderr.String())
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("子进程退出异常: %v\nstderr:\n%s", err, stderr.String())
	}

	childFailed := childOutcome.Err != ""
	parentFailed := parentErr != nil
	if childFailed == parentFailed {
		t.Fatalf("同版本竞争必须恰好一个成功: child err=%q parent err=%v", childOutcome.Err, parentErr)
	}
	if parentFailed && !errors.Is(parentErr, ErrConflict) {
		t.Fatalf("父进程失败应报版本冲突: %v", parentErr)
	}

	// 只有失败的那一方没写下事件，所以赢家就是成功的一方。
	winner := "child-worker"
	if childFailed {
		winner = "parent-worker"
	}
	got, err := store.Get(ctx, "cross-process")
	if err != nil {
		t.Fatalf("竞争后 Get 失败: %v", err)
	}
	if got.Version != 2 || got.Status != StatusRunning || got.WorkerID != winner {
		t.Fatalf("竞争后的存储与赢家不符: got=%+v winner=%s childErr=%q parentErr=%v", got, winner, childOutcome.Err, parentErr)
	}
	if lines := bytes.Count([]byte(readJournal(t, dir, "cross-process")), []byte("\n")); lines != 2 {
		t.Fatalf("journal 行数 = %d, 期望 2", lines)
	}
}
