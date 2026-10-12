package runstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func newFileStore(t *testing.T, dir string) *File {
	t.Helper()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile(%q) 失败: %v", dir, err)
	}
	return f
}

func journalPath(t *testing.T, dir, runID string) string {
	t.Helper()
	return filepath.Join(dir, "runs", runID+".jsonl")
}

func readJournal(t *testing.T, dir, runID string) string {
	t.Helper()
	data, err := os.ReadFile(journalPath(t, dir, runID))
	if err != nil {
		t.Fatalf("读 journal 失败: %v", err)
	}
	return string(data)
}

func appendRawLines(t *testing.T, dir, runID string, lines ...string) {
	t.Helper()
	fh, err := os.OpenFile(journalPath(t, dir, runID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开 journal 失败: %v", err)
	}
	defer fh.Close()
	for _, line := range lines {
		if _, err := fh.WriteString(line); err != nil {
			t.Fatalf("写入 journal 失败: %v", err)
		}
	}
}

func mustEncodeEvent(t *testing.T, prev uint64, rec RunRecord) string {
	t.Helper()
	line, err := json.Marshal(journalEvent{
		Version:     runFormatVersion,
		Kind:        eventKindRecord,
		PrevVersion: prev,
		Record:      &rec,
	})
	if err != nil {
		t.Fatalf("编码事件失败: %v", err)
	}
	return string(line)
}

// TestRunFileLayout 证明记录落在独立的 runs 子目录、锁文件与 journal 同名不同后缀，
// 且不安全的 RunID 不会在目录之外创建任何东西。
func TestRunFileLayout(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	store := newFileStore(t, dir)
	ctx := context.Background()

	if _, err := store.Create(ctx, recordIn("first", StatusQueued, testBase())); err != nil {
		t.Fatalf("Create 失败: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "runs")); err != nil {
		t.Fatalf("runs 子目录没建立: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "first.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("journal 不该躺在根目录: %v", err)
	}

	for _, bad := range []string{"", "../escape", "a/b", `a\b`, "a:b", "a b", strings.Repeat("x", maxRunIDLen+1)} {
		if _, err := store.Create(ctx, recordIn(bad, StatusQueued, testBase())); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("不安全 RunID %q = %v, want ErrInvalidRecord", bad, err)
		}
		if _, err := store.Get(ctx, bad); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("Get 不安全 RunID %q = %v, want ErrInvalidRecord", bad, err)
		}
		if _, err := store.CompareAndSwap(ctx, bad, 1, recordIn(bad, StatusRunning, testBase())); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("CAS 不安全 RunID %q = %v, want ErrInvalidRecord", bad, err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "runs"))
	if err != nil {
		t.Fatalf("列目录失败: %v", err)
	}
	if len(entries) != 1 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("非法 ID 在 runs 目录留下了东西: %v", names)
	}
}

// TestRunFileReopen 是完成标准的第一半：进程内的第二个实例（等同重启后重开）能重新加载
// 身份、取消请求与结果。
func TestRunFileReopen(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)

	queued := mustCreate(t, store, func() RunRecord {
		r := recordIn("reopen2", StatusQueued, testBase())
		r.JobID = "job-2"
		r.ResumeOfRunID = "run-0"
		r.TurnID = "turn-2"
		return r
	}())

	running, err := cas(t, store, "reopen2", queued.Version, func(r *RunRecord) {
		r.Status = StatusRunning
		r.WorkerID = "w1"
		r.WorkerEpoch = 1
		r.HeartbeatAt = testBase().Add(time.Hour)
		r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: "thread-reopen2", CheckpointID: "cp-1"}
		r.CheckpointRevision = 11
	})
	if err != nil {
		t.Fatalf("queued→running 失败: %v", err)
	}
	cancelled, err := cas(t, store, "reopen2", running.Version, func(r *RunRecord) {
		r.Status = StatusCancelling
		r.CancelRequestedAt = testBase().Add(2 * time.Hour)
	})
	if err != nil {
		t.Fatalf("running→cancelling 失败: %v", err)
	}
	done, err := cas(t, store, "reopen2", cancelled.Version, func(r *RunRecord) {
		r.Status = StatusSucceeded
		r.FinishReason = FinishReasonCompleted
		r.Result = settledResult("重开也要能看到")
		r.CompletionRef = &checkpoint.Ref{ThreadID: "thread-reopen2", CheckpointID: "cp-done"}
		r.RecordError = &RunError{Code: ErrCodeDeadlineExceeded, Message: "收尾没写进去"}
	})
	if err != nil {
		t.Fatalf("cancelling→succeeded 失败: %v", err)
	}

	reopened := newFileStore(t, dir)
	got, err := reopened.Get(ctx, "reopen2")
	if err != nil {
		t.Fatalf("重开后 Get 失败: %v", err)
	}
	if got.RunID != "reopen2" || got.ThreadID != "thread-reopen2" || got.TurnID != "turn-2" ||
		got.JobID != "job-2" || got.ResumeOfRunID != "run-0" {
		t.Fatalf("重开后身份丢了: %+v", got)
	}
	if !got.CreatedAt.Equal(queued.CreatedAt) {
		t.Fatalf("重开后创建时间变了: %v vs %v", got.CreatedAt, queued.CreatedAt)
	}
	if !got.CancelRequestedAt.Equal(testBase().Add(2 * time.Hour)) {
		t.Fatalf("重开后取消请求丢了: %v", got.CancelRequestedAt)
	}
	if got.Result == nil || got.Result.Message.Parts[0].(core.Text).Text != "重开也要能看到" {
		t.Fatalf("重开后结果不对: %+v", got.Result)
	}
	if got.WorkerID != "w1" || got.WorkerEpoch != 1 || got.CheckpointRevision != 11 {
		t.Fatalf("重开后 worker/边界丢了: %+v", got)
	}
	if got.CompletionRef == nil || got.CompletionRef.CheckpointID != "cp-done" ||
		got.RecordError == nil || got.RecordError.Code != ErrCodeDeadlineExceeded {
		t.Fatalf("重开后收尾信息丢了: %+v", got)
	}
	if got.Version != done.Version || got.Version != 4 {
		t.Fatalf("重开后版本不对: got %d want %d", got.Version, done.Version)
	}
	if _, err := reopened.Get(ctx, "missing-here"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("未知 RunID = %v, want ErrNotFound", err)
	}
}

// TestRunFileTornTail 覆盖崩溃留下的尾部半条记录：读取只认上一条完整记录，下一次写入把
// 尾巴截掉并正常成功。
func TestRunFileTornTail(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)
	created := mustCreate(t, store, recordIn("tail", StatusQueued, testBase()))

	// 半条记录：上一条完整事件之后跟着没有换行的残片。
	appendRawLines(t, dir, "tail", `{"version":1,"kind":"rec`)

	got, err := store.Get(ctx, "tail")
	if err != nil {
		t.Fatalf("有尾巴时 Get 应仍可用: %v", err)
	}
	if got.Version != created.Version || got.Status != StatusQueued {
		t.Fatalf("半条记录被当成有效: %+v", got)
	}
	if strings.Count(readJournal(t, dir, "tail"), "\n") != 1 {
		t.Fatalf("只读不该截尾巴: %q", readJournal(t, dir, "tail"))
	}

	// 下一次写入先修好尾巴，再追加自己的事件。
	running, err := cas(t, store, "tail", created.Version, func(r *RunRecord) { r.Status = StatusRunning })
	if err != nil {
		t.Fatalf("带尾巴的 CAS 应能修复并成功: %v", err)
	}
	if running.Version != 2 {
		t.Fatalf("版本不对: %+v", running)
	}
	body := readJournal(t, dir, "tail")
	if !strings.HasSuffix(body, "\n") {
		t.Fatalf("尾巴没修干净: %q", body)
	}
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("journal 应有两条完整事件, got %d: %q", len(lines), body)
	}
	for _, line := range lines {
		if _, err := decodeEvent("tail", []byte(line)); err != nil {
			t.Fatalf("修复后仍有坏行: %v\n%q", err, line)
		}
	}
}

// TestRunFileCorruptJournal 覆盖中部损坏、未知版本、未知 kind、串错 RunID、断链：一律拒绝，
// 不静默跳过。
func TestRunFileCorruptJournal(t *testing.T) {
	cases := []struct {
		name   string
		lines  []string
		wantIn string
	}{
		{"中部坏行", []string{"not json\n", "\n"}, "corrupt line"},
		{"未知版本", []string{`{"version":2,"kind":"record"}` + "\n"}, "unsupported run journal version"},
		{"未知 kind", []string{`{"version":1,"kind":"heartbeat"}` + "\n"}, "unknown run journal kind"},
		{"缺记录", []string{`{"version":1,"kind":"record","prev_version":0}` + "\n"}, "has no record"},
		{"串错 RunID", []string{`{"version":1,"kind":"record","prev_version":0,"record":{"run_id":"other","thread_id":"t","status":"queued","version":1}}` + "\n"}, "carries run id"},
		{"断链", []string{`{"version":1,"kind":"record","prev_version":5,"record":{"run_id":"corrupt","thread_id":"thread-corrupt","status":"queued","version":6}}` + "\n"}, "replaces version"},
		{"记录本身不合法", []string{`{"version":1,"kind":"record","prev_version":0,"record":{"run_id":"corrupt","thread_id":"thread-corrupt","status":"failed","version":1}}` + "\n"}, "not a valid record"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			dir := t.TempDir()
			ctx := context.Background()
			store := newFileStore(t, dir)
			if _, err := store.Create(ctx, recordIn("corrupt", StatusQueued, testBase())); err != nil {
				t.Fatalf("Create 失败: %v", err)
			}
			appendRawLines(t, dir, "corrupt", c.lines...)
			if _, err := store.Get(ctx, "corrupt"); err == nil || !strings.Contains(err.Error(), c.wantIn) {
				t.Fatalf("Get 错误 = %v, 应包含 %q", err, c.wantIn)
			}
			// 写路径同样拒绝，不能"绕过"损坏继续追加。
			write := recordIn("corrupt", StatusRunning, testBase())
			if _, err := store.CompareAndSwap(ctx, "corrupt", 1, write); err == nil {
				t.Fatal("损坏 journal 上的写入应失败")
			} else if !strings.Contains(err.Error(), c.wantIn) {
				t.Fatalf("写入错误 = %v, 应包含 %q", err, c.wantIn)
			}
		})
	}
}

// TestRunFileDuplicateEvent 证明重复的同一条事件不递增版本。
func TestRunFileDuplicateEvent(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)
	created := mustCreate(t, store, recordIn("dupe", StatusQueued, testBase()))
	running, err := cas(t, store, "dupe", created.Version, func(r *RunRecord) { r.Status = StatusRunning })
	if err != nil {
		t.Fatalf("queued→running 失败: %v", err)
	}

	// 把刚落盘的那条事件再抄一遍，等同 append 成功但 Sync 报错后重投。
	appendRawLines(t, dir, "dupe", mustEncodeEvent(t, 1, running)+"\n")

	got, err := store.Get(ctx, "dupe")
	if err != nil {
		t.Fatalf("重复事件应被识别为同一条: %v", err)
	}
	if got.Version != running.Version || got.Status != StatusRunning {
		t.Fatalf("重复事件推进了版本: %d → %d", running.Version, got.Version)
	}
	// 认出来之后，下一次写入仍然接在真实版本之后。
	next, err := cas(t, store, "dupe", got.Version, func(r *RunRecord) {
		r.Status = StatusSucceeded
		r.FinishReason = FinishReasonCompleted
	})
	if err != nil {
		t.Fatalf("重复事件后应可继续写: %v", err)
	}
	if next.Version != running.Version+1 {
		t.Fatalf("版本接错了: %d", next.Version)
	}
}

// TestRunFileSyncFailure 覆盖完成标准第二半：写或 Sync 失败必须报错，不能对外说成功，
// 内存里也不能有一份"以为写好了"的状态。
func TestRunFileSyncFailure(t *testing.T) {
	syncErr := errors.New("boom: sync")
	writeErr := errors.New("boom: write")

	t.Run("Sync 失败", func(t *testing.T) {
		stubClock(t, testBase(), time.Hour)
		dir := t.TempDir()
		ctx := context.Background()
		store := newFileStore(t, dir)
		created := mustCreate(t, store, recordIn("syncfail", StatusQueued, testBase()))
		store.ops = fileOps{
			write: func(fh *os.File, b []byte) (int, error) { return fh.Write(b) },
			sync:  func(fh *os.File) error { return syncErr },
		}
		if _, err := cas(t, store, "syncfail", created.Version, func(r *RunRecord) { r.Status = StatusRunning }); !errors.Is(err, syncErr) {
			t.Fatalf("Sync 失败应原样报错: %v", err)
		}
		// 数据其实已经落进文件，所以调用方重试同一版本只能得到冲突——
		// 存储不假装这次写没发生过，也不把失败写报成成功。
		if _, err := cas(t, store, "syncfail", created.Version, func(r *RunRecord) { r.Status = StatusRunning }); !errors.Is(err, ErrConflict) {
			t.Fatalf("失败后的同版本重放应 ErrConflict: %v", err)
		}
		store.ops = defaultFileOps()
		got, err := store.Get(ctx, "syncfail")
		if err != nil {
			t.Fatalf("Sync 失败后应仍能读: %v", err)
		}
		if got.Version != 2 || got.Status != StatusRunning {
			t.Fatalf("落盘状态与报错不一致: %+v", got)
		}
	})

	t.Run("Write 失败", func(t *testing.T) {
		stubClock(t, testBase(), time.Hour)
		dir := t.TempDir()
		ctx := context.Background()
		store := newFileStore(t, dir)
		created := mustCreate(t, store, recordIn("writefail", StatusQueued, testBase()))
		store.ops = fileOps{
			write: func(fh *os.File, b []byte) (int, error) { return 0, writeErr },
			sync:  func(fh *os.File) error { return nil },
		}
		if _, err := cas(t, store, "writefail", created.Version, func(r *RunRecord) { r.Status = StatusRunning }); !errors.Is(err, writeErr) {
			t.Fatalf("写失败应原样报错: %v", err)
		}
		store.ops = defaultFileOps()
		got, err := store.Get(ctx, "writefail")
		if err != nil {
			t.Fatalf("写失败后应仍能读: %v", err)
		}
		if got.Version != created.Version || got.Status != StatusQueued {
			t.Fatalf("失败的写被发布了: %+v", got)
		}
		// 同一版本还能重写成功，说明失败没有留下半个事件。
		if _, err := cas(t, store, "writefail", created.Version, func(r *RunRecord) { r.Status = StatusRunning }); err != nil {
			t.Fatalf("失败后重投应成功: %v", err)
		}
	})

	t.Run("Create 写失败", func(t *testing.T) {
		stubClock(t, testBase(), time.Hour)
		dir := t.TempDir()
		ctx := context.Background()
		store := newFileStore(t, dir)
		store.ops = fileOps{
			write: func(fh *os.File, b []byte) (int, error) { return 0, writeErr },
			sync:  func(fh *os.File) error { return nil },
		}
		if _, err := store.Create(ctx, recordIn("createfail", StatusQueued, testBase())); !errors.Is(err, writeErr) {
			t.Fatalf("Create 写失败应报错: %v", err)
		}
		if _, err := store.Get(ctx, "createfail"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("失败的 Create 不该留下记录: %v", err)
		}
	})
}

// TestRunFileLockWaitIsCancellable 证明等待跨进程锁时可被 context 取消。
func TestRunFileLockWaitIsCancellable(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	store := newFileStore(t, dir)
	lockPath := filepath.Join(dir, "runs", "held.lock")
	if err := os.WriteFile(lockPath, []byte("other process"), 0o600); err != nil {
		t.Fatalf("造锁失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := store.Create(ctx, recordIn("held", StatusQueued, testBase()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等锁应可被 context 取消, got %v", err)
	}
	// 只留下锁文件，不该有 journal。
	if _, err := os.Stat(journalPath(t, dir, "held")); !os.IsNotExist(err) {
		t.Fatalf("没拿到锁却写了 journal: %v", err)
	}
	if _, err := store.Get(ctx, "held"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("已过期的 context 读也应失败: %v", err)
	}
}

// TestRunFileTwoInstancesSingleWinner 证明一致性不依赖同一进程的 mutex：两个 File 实例
// 在同一版本上并发写，只有一个成功。
func TestRunFileTwoInstancesSingleWinner(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()
	store := newFileStore(t, dir)
	created := mustCreate(t, store, recordIn("twofiles", StatusQueued, testBase()))

	a, b := newFileStore(t, dir), newFileStore(t, dir)
	start := make(chan struct{})
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i, s := range []*File{a, b} {
		wg.Add(1)
		go func(i int, s *File) {
			defer wg.Done()
			attempt := created
			attempt.Status = StatusRunning
			attempt.WorkerID = "w1"
			attempt.WorkerEpoch = 1
			<-start
			_, results[i] = s.CompareAndSwap(ctx, "twofiles", created.Version, attempt)
		}(i, s)
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatalf("失败方应只报版本冲突: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("两个实例同版本 %v, 期望恰好 1 个成功", results)
	}
	got, err := store.Get(ctx, "twofiles")
	if err != nil || got.Version != 2 {
		t.Fatalf("存储状态不对: %+v err=%v", got, err)
	}
	if lines := strings.Split(strings.TrimRight(readJournal(t, dir, "twofiles"), "\n"), "\n"); len(lines) != 2 {
		t.Fatalf("journal 行数 = %d, 期望 2: %q", len(lines), readJournal(t, dir, "twofiles"))
	}
}

// TestRunFileFormatFixture 用入库的 v1 journal 钉住磁盘格式：字段名或版本一旦改动，
// 这条用例就会失败。
func TestRunFileFormatFixture(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	ctx := context.Background()

	data, err := os.ReadFile(filepath.Join("testdata", "run_journal_v1.jsonl"))
	if err != nil {
		t.Fatalf("读 fixture 失败: %v", err)
	}
	// 仓库 core.autocrlf=true，签出可能是 CRLF，落盘前统一换回 LF。
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	target := filepath.Join(dir, "runs")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("建目录失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "fixture-run.jsonl"), []byte(normalized), 0o644); err != nil {
		t.Fatalf("写 fixture 失败: %v", err)
	}

	store := newFileStore(t, dir)
	got, err := store.Get(ctx, "fixture-run")
	if err != nil {
		t.Fatalf("读 fixture journal 失败: %v", err)
	}
	if got.RunID != "fixture-run" || got.ThreadID != "thread-fixture" || got.TurnID != "turn-fixture" ||
		got.JobID != "job-fixture" || got.ResumeOfRunID != "run-before" {
		t.Fatalf("fixture 身份读歪了: %+v", got)
	}
	if got.Status != StatusSucceeded || got.FinishReason != FinishReasonCompleted || got.Version != 4 {
		t.Fatalf("fixture 结局读歪了: %+v", got)
	}
	if !got.CreatedAt.Equal(time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("fixture 创建时间读歪了: %v", got.CreatedAt)
	}
	if !got.CancelRequestedAt.Equal(time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("fixture 取消时间读歪了: %v", got.CancelRequestedAt)
	}
	if got.Result == nil || got.Result.Message.Parts[0].(core.Text).Text != "fixture answer" {
		t.Fatalf("fixture 结果读歪了: %+v", got.Result)
	}
	if got.LatestCheckpointRef == nil || got.LatestCheckpointRef.CheckpointID != "cp-2" || got.CheckpointRevision != 2 {
		t.Fatalf("fixture 边界读歪了: %+v", got)
	}
	if got.CompletionRef == nil || got.CompletionRef.CheckpointID != "cp-3" {
		t.Fatalf("fixture 收尾引用读歪了: %+v", got.CompletionRef)
	}
	if got.WorkerID != "w1" || got.WorkerEpoch != 1 {
		t.Fatalf("fixture worker 读歪了: %+v", got)
	}

	// 从 fixture 状态继续走同内容重试：幂等，不追加事件。
	before := strings.Count(readJournal(t, dir, "fixture-run"), "\n")
	again, err := store.CompareAndSwap(ctx, "fixture-run", got.Version, got)
	if err != nil {
		t.Fatalf("fixture 上的同内容重试应幂等: %v", err)
	}
	if again.Version != got.Version {
		t.Fatalf("同内容重试推进了版本: %d → %d", got.Version, again.Version)
	}
	if after := strings.Count(readJournal(t, dir, "fixture-run"), "\n"); after != before {
		t.Fatalf("同内容重试追加了事件: %d → %d 行", before, after)
	}
}

// TestRunFileJournalShapeIsStable 把一行的实际字段顺序钉死，防止以后加字段时无声改格式。
func TestRunFileJournalShapeIsStable(t *testing.T) {
	stubClock(t, testBase(), time.Hour)
	dir := t.TempDir()
	store := newFileStore(t, dir)
	record := recordIn("shape", StatusQueued, testBase())
	created := mustCreate(t, store, record)

	line := strings.TrimSpace(readJournal(t, dir, "shape"))
	want := fmt.Sprintf(`{"version":1,"kind":"record","prev_version":0,"record":{"run_id":"shape","thread_id":"thread-shape","turn_id":"turn-shape","status":"queued","created_at":%q,"updated_at":%q,"version":1}}`,
		created.CreatedAt.Format(time.RFC3339Nano), created.UpdatedAt.Format(time.RFC3339Nano))
	if line != want {
		t.Fatalf("journal 行格式变了\n got: %s\nwant: %s", line, want)
	}
}
