package checkpoint

// SM-13 场景 D（ADR-0034 D7、D10）：分页投影在另一个进程写入时的表现。
//
// 写入方是 os.Executable 重新拉起的子进程：它打开同一个 File 目录，追加快照、损坏投影文件，
// 或者在 Sync 上失败一次。父进程这边要么正走在一次已经冻结的分页里，要么重开一个句柄去读。
// 两边不共享内存，所以"当前投影看不见新写入""新查询才看得见""损坏与失败都不许污染 head"
// 这三件事只能在磁盘上被验证。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

const snapshotQueryProcessHelperEnv = "GOAGENT_SNAPSHOT_QUERY_PROCESS_HELPER"

const (
	snapshotQueryRoleEnv   = "GOAGENT_SNAPSHOT_QUERY_ROLE"
	snapshotQueryDirEnv    = "GOAGENT_SNAPSHOT_QUERY_DIR"
	snapshotQueryThreadEnv = "GOAGENT_SNAPSHOT_QUERY_THREAD"
	snapshotQueryCountEnv  = "GOAGENT_SNAPSHOT_QUERY_COUNT"
	snapshotQueryPrefixEnv = "GOAGENT_SNAPSHOT_QUERY_PREFIX"
)

// 子进程写入方的三种故障点。
const (
	snapshotRoleAppend   = "append"
	snapshotRoleCorrupt  = "corrupt-sidecar"
	snapshotRoleFailSync = "fail-sync"
)

// 报告：写入方把"做了什么"回报给父进程，而不是让父进程猜。
const (
	snapshotReportAppended = "appended"
	snapshotReportCorrupt  = "corrupted"
	snapshotReportSync     = "sync-failure"
)

type snapshotQueryReport struct {
	Role        string `json:"role"`
	ThreadID    string `json:"thread_id,omitempty"`
	Count       int    `json:"count,omitempty"`
	HeadID      string `json:"head_id,omitempty"`
	HeadRevison uint64 `json:"head_revision,omitempty"`
	Error       string `json:"error,omitempty"`
}

// --- 场景 D①：分页走到一半，另一个进程接着写 ---------------------------------

// TestQueryProjectionAcrossProcessAppend 先取第一页（把投影冻住），再起一个子进程往同一个
// 线程追加三条快照，然后走剩下的页：这一次遍历必须正好是冻结时的七条，不重不漏；换一个新
// 查询才看得见追加的那三条。
func TestQueryProjectionAcrossProcessAppend(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	threadID := "sm13-append-midwalk"
	store := openSnapshotQueryStore(t, dir)
	seedSnapshotChain(t, store, threadID, "base", 7)

	first, err := store.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: 3})
	if err != nil {
		t.Fatalf("取第一页: %v", err)
	}
	if len(first.Items) != 3 || first.NextCursor == "" {
		t.Fatalf("第一页 = %+v", first)
	}

	// 追加方是另一个进程：它退出即代表写入已经落盘，父进程随后读取不存在竞态。
	runSnapshotQueryWriter(t, snapshotWriter{
		role: snapshotRoleAppend, dir: dir, threadID: threadID, count: 3, prefix: "late",
	})

	var walked []string
	page := first
	for page.NextCursor != "" {
		for _, item := range page.Items {
			walked = append(walked, item.Ref.CheckpointID)
		}
		next, err := store.QueryCheckpoints(ctx, ListOptions{
			ThreadID: threadID, Limit: 3, Cursor: page.NextCursor,
		})
		if err != nil {
			t.Fatalf("续页 %q: %v", page.NextCursor, err)
		}
		page = next
	}
	for _, item := range page.Items {
		walked = append(walked, item.Ref.CheckpointID)
	}

	want := []string{"base-1", "base-2", "base-3", "base-4", "base-5", "base-6", "base-7"}
	if strings.Join(walked, ",") != strings.Join(want, ",") {
		t.Fatalf("冻结投影的遍历 = %v, 期望 %v", walked, want)
	}

	// 新查询、新句柄：追加的三条必须可见，且排在原有七条之后。
	fresh := openSnapshotQueryStore(t, dir)
	all, err := fresh.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: MaxQueryLimit})
	if err != nil {
		t.Fatalf("新查询: %v", err)
	}
	if len(all.Items) != 10 || all.NextCursor != "" {
		t.Fatalf("新查询条数 = %d, cursor = %q", len(all.Items), all.NextCursor)
	}
	if all.Items[7].Ref.CheckpointID != "late-1" || all.Items[9].Ref.CheckpointID != "late-3" {
		t.Fatalf("新查询里的追加项位置不对: %+v", all.Items)
	}
	if all.Items[0].Revision != 1 || all.Items[9].Revision != 10 {
		t.Fatalf("revision 不连续: %+v %+v", all.Items[0], all.Items[9])
	}
}

// --- 场景 D②：另一个进程损坏投影文件 -----------------------------------------

// TestSidecarCorruptionFromAnotherProcess 先让一次查询把 "<thread>.meta.jsonl" 写出来，再让
// 子进程按四种方式把它写坏：版本号不对、线程不对、行数与消费位置不对、干脆不是 JSON。每一种
// 都只踩一道校验，所以四道校验都被单独证明是必要的；重开句柄必须都从 journal 重建出同一条
// 时间线，head 一条不多一条不少，journal 一个字节都没被碰。
func TestSidecarCorruptionFromAnotherProcess(t *testing.T) {
	for _, poison := range []string{"version", "thread", "sequence", "unparseable"} {
		t.Run(poison, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			threadID := "sm13-corrupt-" + poison
			store := openSnapshotQueryStore(t, dir)
			seedSnapshotChain(t, store, threadID, "keep", 5)

			before, err := store.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: MaxQueryLimit})
			if err != nil {
				t.Fatalf("建投影的首次查询: %v", err)
			}
			if len(before.Items) != 5 {
				t.Fatalf("初始摘要条数 = %d", len(before.Items))
			}
			sidecar := filepath.Join(dir, threadID+metadataSidecarSuffix)
			if _, err := os.Stat(sidecar); err != nil {
				t.Fatalf("查询后没写出投影文件 %s: %v", sidecar, err)
			}
			journal := filepath.Join(dir, threadID+".jsonl")
			journalBytes, err := os.ReadFile(journal)
			if err != nil {
				t.Fatalf("读 journal: %v", err)
			}

			runSnapshotQueryWriter(t, snapshotWriter{
				role: snapshotRoleCorrupt, dir: dir, threadID: threadID, prefix: poison,
			})

			// 损坏的投影不能改写成"另一种历史"：重开句柄只能从 journal 得到事实。
			reopened := openSnapshotQueryStore(t, dir)
			after, err := reopened.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: MaxQueryLimit})
			if err != nil {
				t.Fatalf("损坏后的查询: %v", err)
			}
			if len(after.Items) != 5 {
				t.Fatalf("%s 投毒后的摘要条数 = %d, 期望 5: %+v", poison, len(after.Items), after.Items)
			}
			for i := range after.Items {
				if after.Items[i].Ref.CheckpointID != before.Items[i].Ref.CheckpointID {
					t.Fatalf("%s 投毒改写了第 %d 条: %q → %q", poison, i, before.Items[i].Ref.CheckpointID, after.Items[i].Ref.CheckpointID)
				}
			}
			if after.Items[0].Ref.CheckpointID == "phantom-9" || after.Items[4].Ref.CheckpointID == "phantom-9" {
				t.Fatalf("%s 投毒让幻影进了投影: %+v", poison, after.Items)
			}

			metadata, found, err := reopened.ReadCheckpointMetadata(ctx, Ref{ThreadID: threadID, CheckpointID: "keep-5"})
			if err != nil || !found || metadata.Ref.CheckpointID != "keep-5" {
				t.Fatalf("按引用读单条摘要 = %+v ok=%v err=%v", metadata, found, err)
			}
			head, ok, err := reopened.ReadSessionHead(ctx, threadID)
			if err != nil || !ok {
				t.Fatalf("读 head: %v ok=%v", err, ok)
			}
			if head.Ref.CheckpointID != "keep-5" || head.Revision != 5 {
				t.Fatalf("%s 投毒污染了 head: %+v", poison, head)
			}
			latest, err := reopened.Latest(ctx, threadID)
			if err != nil || latest == nil || latest.ID != "keep-5" {
				t.Fatalf("Latest 与 head 不一致: %+v err=%v", latest, err)
			}

			// 损坏只该落在投影上：journal 字节一个都没变。
			current, err := os.ReadFile(journal)
			if err != nil {
				t.Fatalf("重读 journal: %v", err)
			}
			if !bytes.Equal(journalBytes, current) {
				t.Fatal("损坏投影时改动了 journal")
			}
		})
	}
}

// --- 场景 D③：另一个进程 Sync 失败 -------------------------------------------

// TestFailedSyncInAnotherProcessLeavesHeadClean 让子进程带着"第一次 Sync 必失败"的写入口提交
// 一条快照并报错退出，父进程重开句柄检查：head 要么是原有那条、要么就是新那条，不能是半个；
// 同一条重放不会重复入账，revision 严格连续。
func TestFailedSyncInAnotherProcessLeavesHeadClean(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	threadID := "sm13-failing-sync"
	store := openSnapshotQueryStore(t, dir)
	seedSnapshotChain(t, store, threadID, "solid", 4)

	report := runSnapshotQueryWriter(t, snapshotWriter{
		role: snapshotRoleFailSync, dir: dir, threadID: threadID, count: 1, prefix: "boom",
	})
	if report.Error == "" {
		t.Fatalf("Sync 失败却回报没有错误: %+v", report)
	}
	if !strings.Contains(report.Error, "injected sync failure") {
		t.Fatalf("错误不是注入的那条: %q", report.Error)
	}

	reopened := openSnapshotQueryStore(t, dir)
	after, err := reopened.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: MaxQueryLimit})
	if err != nil {
		t.Fatalf("失败之后重开查询: %v", err)
	}

	// 只允许两种形状：完全没进去，或者完整进去了。半途状态一律算污染。
	var boom int
	for i, item := range after.Items {
		if item.Revision != uint64(i+1) {
			t.Fatalf("revision 不连续: %+v", after.Items)
		}
		if item.Ref.CheckpointID == "boom-1" {
			boom++
		}
	}
	if boom > 1 {
		t.Fatalf("失败的那条写了 %d 遍", boom)
	}
	t.Logf("Sync 失败后 boom-1 落盘次数 = %d, 总条数 = %d", boom, len(after.Items))

	head, ok, err := reopened.ReadSessionHead(ctx, threadID)
	if err != nil || !ok {
		t.Fatalf("失败之后读 head: %v ok=%v", err, ok)
	}
	if boom == 0 && (head.Ref.CheckpointID != "solid-4" || head.Revision != 4) {
		t.Fatalf("没落盘却动了 head: %+v", head)
	}
	if boom == 1 && (head.Ref.CheckpointID != "boom-1" || head.Revision != 5) {
		t.Fatalf("落盘了 head 却没跟上: %+v", head)
	}

	// 健康句柄重放同一条：要么补齐、要么幂等，两种都不许多出一条，也不许改 head 之外的事实。
	replay := snapshotChainCheckpoint(threadID, "boom-1", "solid-4", 4)
	if _, err := reopened.Commit(ctx, CommitRequest{
		Checkpoint: replay, ExpectedRevision: uint64Ptr(head.Revision),
	}); err != nil && !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("重放提交: %v", err)
	}
	final, err := reopened.QueryCheckpoints(ctx, ListOptions{ThreadID: threadID, Limit: MaxQueryLimit})
	if err != nil {
		t.Fatalf("重放后查询: %v", err)
	}
	seen := map[string]int{}
	for _, item := range final.Items {
		seen[item.Ref.CheckpointID]++
	}
	if seen["boom-1"] != 1 {
		t.Fatalf("重放后 boom-1 出现 %d 次: %+v", seen["boom-1"], final.Items)
	}
	if seen["solid-1"] != 1 || seen["solid-4"] != 1 {
		t.Fatalf("重放动了原有历史: %+v", final.Items)
	}
	if len(final.Items) != 5 {
		t.Fatalf("重放后条数 = %d, 期望 5: %+v", len(final.Items), final.Items)
	}
}

// --- 子进程入口 ---------------------------------------------------------------

func TestSnapshotQueryProcessHelper(t *testing.T) {
	if os.Getenv(snapshotQueryProcessHelperEnv) != "1" {
		return
	}
	role := os.Getenv(snapshotQueryRoleEnv)
	dir := os.Getenv(snapshotQueryDirEnv)
	threadID := os.Getenv(snapshotQueryThreadEnv)
	count, err := strconv.Atoi(envOr(snapshotQueryCountEnv, "0"))
	if err != nil {
		t.Fatalf("解析追加条数: %v", err)
	}
	prefix := os.Getenv(snapshotQueryPrefixEnv)

	store, err := NewFile(dir)
	if err != nil {
		t.Fatalf("写入方打开存储: %v", err)
	}
	ctx := context.Background()

	switch role {
	case snapshotRoleAppend:
		head, ok, err := store.ReadSessionHead(ctx, threadID)
		if err != nil || !ok {
			t.Fatalf("写入方读 head: %v ok=%v", err, ok)
		}
		appended := appendChainFrom(t, store, threadID, prefix, head, count)
		snapshotPrintReport(snapshotReportAppended, snapshotQueryReport{
			Role: role, ThreadID: threadID, Count: appended,
		})
	case snapshotRoleCorrupt:
		// 只碰投影文件：journal 是事实，损坏测试不许改它。prefix 选投毒种类，每一种都只踩
		// 一道校验，这样四道校验各自都能被单独证明是必要的。
		path := filepath.Join(dir, threadID+metadataSidecarSuffix)
		journal, err := os.ReadFile(filepath.Join(dir, threadID+".jsonl"))
		if err != nil {
			t.Fatalf("读 journal: %v", err)
		}
		phantom := `{"ref":{"thread_id":"` + threadID + `","checkpoint_id":"phantom-9"},"revision":9,"step":9}`
		var garbage string
		switch prefix {
		case "version":
			garbage = `{"v":999,"thread":"` + threadID + `","offset":` + strconv.Itoa(len(journal)) + `,"items":1,"meta":` + phantom + `}`
		case "thread":
			garbage = `{"v":1,"thread":"` + threadID + `-other","offset":` + strconv.Itoa(len(journal)) + `,"items":1,"meta":` + phantom + `}`
		case "sequence":
			garbage = `{"v":1,"thread":"` + threadID + `","offset":` + strconv.Itoa(len(journal)) + `,"items":7,"meta":` + phantom + `}`
		case "unparseable":
			garbage = `not json at all`
		default:
			t.Fatalf("unknown sidecar poison %q", prefix)
		}
		if err := os.WriteFile(path, []byte(garbage+"\n"), 0o644); err != nil {
			t.Fatalf("写坏投影文件: %v", err)
		}
		snapshotPrintReport(snapshotReportCorrupt, snapshotQueryReport{Role: role, ThreadID: threadID, Count: len(journal)})
	case snapshotRoleFailSync:
		normal := defaultFileOps()
		syncs := 0
		store.ops = fileOps{
			write: normal.write,
			sync: func(fh *os.File) error {
				syncs++
				if syncs == 1 {
					return errors.New("injected sync failure")
				}
				return normal.sync(fh)
			},
		}
		head, ok, err := store.ReadSessionHead(ctx, threadID)
		if err != nil || !ok {
			t.Fatalf("失败注入方读 head: %v ok=%v", err, ok)
		}
		_, err = store.Commit(ctx, CommitRequest{
			Checkpoint:       snapshotChainCheckpoint(threadID, prefix+"-1", head.Ref.CheckpointID, head.Step+1),
			ExpectedRevision: uint64Ptr(head.Revision),
		})
		out := snapshotQueryReport{Role: role, ThreadID: threadID, Count: syncs}
		if err != nil {
			out.Error = err.Error()
		}
		snapshotPrintReport(snapshotReportSync, out)
	default:
		t.Fatalf("unknown snapshot query writer role %q", role)
	}
}

func snapshotPrintReport(tag string, report snapshotQueryReport) {
	data, err := json.Marshal(report)
	if err != nil {
		data = []byte(`{"error":"cannot encode report"}`)
	}
	fmt.Fprintf(os.Stdout, "%s %s\n", tag, data)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// --- 父进程侧 -----------------------------------------------------------------

type snapshotWriter struct {
	role     string
	dir      string
	threadID string
	count    int
	prefix   string
}

// runSnapshotQueryWriter 起一个写入方子进程并等它自己结束：子进程返回即代表它那些写已经
// 落盘，父进程后面的读取不需要再等时间。
func runSnapshotQueryWriter(t *testing.T, w snapshotWriter) snapshotQueryReport {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("定位测试二进制失败: %v", err)
	}
	env := []string{
		snapshotQueryProcessHelperEnv + "=1",
		snapshotQueryRoleEnv + "=" + w.role,
		snapshotQueryDirEnv + "=" + w.dir,
		snapshotQueryThreadEnv + "=" + w.threadID,
		snapshotQueryCountEnv + "=" + strconv.Itoa(w.count),
		snapshotQueryPrefixEnv + "=" + w.prefix,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSnapshotQueryProcessHelper$", "-test.v=false", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), env...)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	runErr := cmd.Run()

	// 失败诊断靠完整输出，不靠猜：子进程的 PASS/FAIL 文本全留着。
	diagnosis := func() string {
		return fmt.Sprintf("role=%s\nstdout:\n%s\nstderr:\n%s", w.role, stdout.String(), stderr.String())
	}
	if w.role == snapshotRoleFailSync {
		// 这条路径本来就要报错退出，Run 的错误不是断言失败。
		_ = runErr
	} else if runErr != nil {
		t.Fatalf("写入方 %s 退出异常: %v\n%s", w.role, runErr, diagnosis())
	}

	tag := map[string]string{
		snapshotRoleAppend:   snapshotReportAppended,
		snapshotRoleCorrupt:  snapshotReportCorrupt,
		snapshotRoleFailSync: snapshotReportSync,
	}[w.role]
	report := snapshotQueryReport{}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if !strings.HasPrefix(line, tag+" ") {
			continue
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, tag+" ")), &report); err != nil {
			t.Fatalf("解析 %s 回报 %q: %v\n%s", tag, line, err, diagnosis())
		}
	}
	if report.Role == "" {
		t.Fatalf("写入方 %s 没回报\n%s", w.role, diagnosis())
	}
	return report
}

func openSnapshotQueryStore(t *testing.T, dir string) *File {
	t.Helper()
	store, err := NewFile(dir)
	if err != nil {
		t.Fatalf("打开 File 存储: %v", err)
	}
	return store
}

func snapshotChainCheckpoint(threadID, id, parentID string, step int) *Checkpoint {
	return &Checkpoint{
		ID:       id,
		ThreadID: threadID,
		ParentID: parentID,
		Step:     step,
		State:    core.State{KV: map[string]any{"marker": id}},
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }

// seedSnapshotChain 从空线程开始写下 count 条 "<prefix>-1..<count>"，返回最后一条。
func seedSnapshotChain(t *testing.T, store *File, threadID, prefix string, count int) CheckpointMetadata {
	t.Helper()
	ctx := context.Background()
	zero := uint64(0)
	var head CheckpointMetadata
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		parent := ""
		expected := &zero
		if i > 1 {
			parent = head.Ref.CheckpointID
			expected = uint64Ptr(head.Revision)
		}
		receipt, err := store.Commit(ctx, CommitRequest{
			Checkpoint:       snapshotChainCheckpoint(threadID, id, parent, i-1),
			ExpectedRevision: expected,
		})
		if err != nil {
			t.Fatalf("写第 %d 条 %s: %v", i, id, err)
		}
		head = CheckpointMetadata{Ref: Ref{ThreadID: threadID, CheckpointID: id}, Revision: receipt.Revision, Step: i - 1}
	}
	return head
}

// appendChainFrom 由写入方（子进程）调用：从已知 head 往后接 count 条，返回接上去的条数。
func appendChainFrom(t *testing.T, store *File, threadID, prefix string, head CheckpointMetadata, count int) int {
	t.Helper()
	ctx := context.Background()
	current := head
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		receipt, err := store.Commit(ctx, CommitRequest{
			Checkpoint:       snapshotChainCheckpoint(threadID, id, current.Ref.CheckpointID, current.Step+1),
			ExpectedRevision: uint64Ptr(current.Revision),
		})
		if err != nil {
			t.Fatalf("追加第 %d 条 %s: %v", i, id, err)
		}
		current = CheckpointMetadata{
			Ref:      Ref{ThreadID: threadID, CheckpointID: id},
			Revision: receipt.Revision,
			Step:     current.Step + 1,
		}
	}
	return count
}
