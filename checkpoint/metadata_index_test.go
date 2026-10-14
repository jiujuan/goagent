package checkpoint

// TASK-SM-11（ADR-0034 D7）：File 的可重建摘要 sidecar。journal 是唯一事实来源，
// sidecar 只是投影；本文件钉住四件事：缺失或损坏能重建、写失败不污染已成功的提交、别的进程
// 提交的新记录能被读到、稳态查询不解码完整 State 也不还原 blob。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

const (
	metaProcessHelperEnv   = "GOAGENT_FILE_META_PROCESS_HELPER"
	metaProcessHelperName  = "TestFileMetadataProcessHelper"
	metaProcessDirEnv      = "GOAGENT_FILE_META_DIR"
	metaProcessThreadEnv   = "GOAGENT_FILE_META_THREAD"
	metaProcessFirstIDEnv  = "GOAGENT_FILE_META_FIRST"
	metaProcessSecondIDEnv = "GOAGENT_FILE_META_SECOND"
)

func metaClock() time.Time {
	return time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
}

func metaOpenFile(t testing.TB, dir string) *File {
	t.Helper()
	store, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	fixed := metaClock()
	store.now = func() time.Time { return fixed }
	store.projections.now = func() time.Time { return fixed }
	return store
}

// metaCheckpointID 提交一条带身份与终态的快照，返回它分配的 Revision。
func metaCheckpointID(t *testing.T, store *File, threadID, id, runID string) uint64 {
	t.Helper()
	cp := &Checkpoint{
		ID:        id,
		ThreadID:  threadID,
		Step:      1,
		ParentID:  "parent-" + id,
		ParentRef: &Ref{ThreadID: threadID, CheckpointID: "parent-" + id},
		RunID:     runID,
		TurnID:    "turn-" + id,
		State:     core.State{Messages: []core.Message{{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: "payload of " + id}}}}},
	}
	rec, err := store.Commit(context.Background(), CommitRequest{Checkpoint: cp})
	if err != nil {
		t.Fatalf("commit %s/%s: %v", threadID, id, err)
	}
	return rec.Revision
}

func metaSidecarPath(t *testing.T, dir, threadID string) string {
	t.Helper()
	return filepath.Join(dir, threadID+metadataSidecarSuffix)
}

func metaSidecarLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("读 sidecar: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func metaJournalPath(t *testing.T, dir, threadID string) string {
	t.Helper()
	return filepath.Join(dir, threadID+".jsonl")
}

func metaJournalSize(t *testing.T, dir, threadID string) int64 {
	t.Helper()
	info, err := os.Stat(metaJournalPath(t, dir, threadID))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat journal: %v", err)
	}
	return info.Size()
}

// metaStats 是计数器快照，测试只看增量。
type metaStats struct {
	scans                int
	tailLines            int
	stateDecodes         int
	rebuilds             int
	sidecarWriteFailures int
	refreshErrors        int
}

func metaReadStats(store *File) metaStats {
	store.mu.Lock()
	defer store.mu.Unlock()
	return metaStats{
		scans:                store.stats.scans,
		tailLines:            store.stats.tailLines,
		stateDecodes:         store.stats.stateDecodes,
		rebuilds:             store.stats.rebuilds,
		sidecarWriteFailures: store.stats.sidecarWriteFailures,
		refreshErrors:        store.stats.refreshErrors,
	}
}

func metaQueryAll(t *testing.T, store *File, opts ListOptions) []CheckpointMetadata {
	t.Helper()
	opts.Limit = MaxQueryLimit
	page, err := store.QueryCheckpoints(context.Background(), opts)
	if err != nil {
		t.Fatalf("QueryCheckpoints(%+v): %v", opts, err)
	}
	return page.Items
}

func TestFileMetadataRebuild(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)
	metaCheckpointID(t, store, "alpha", "a1", "run-a1")
	metaCheckpointID(t, store, "alpha", "a2", "run-a2")
	metaCheckpointID(t, store, "alpha", "a3", "run-a3")

	if got := len(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); got != 3 {
		t.Fatalf("基线条数 = %d, 期望 3", got)
	}

	// (a) sidecar 被删：从 journal 重建。
	before := metaReadStats(store)
	if err := os.Remove(metaSidecarPath(t, dir, "alpha")); err != nil {
		t.Fatalf("删 sidecar: %v", err)
	}
	items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if len(items) != 3 || items[0].Ref.CheckpointID != "a1" || items[2].Ref.CheckpointID != "a3" {
		t.Fatalf("删除后重建结果 = %v", metaQueryIDs(items))
	}
	after := metaReadStats(store)
	if after.rebuilds <= before.rebuilds {
		t.Errorf("缺失 sidecar 的重建没有被计数: %+v", after)
	}
	if after.scans > before.scans {
		t.Errorf("重建走了 scanLocked 全量解码: %+v", after)
	}

	// (b) sidecar 内容损坏：整份重建，且绝不因为索引坏掉而删 journal。
	corrupt := `{"v":1,"thread":"alpha","offset":999999,"items":7,"meta":{"revision":42}}` + "\n" + "garbage-not-json\n"
	if err := os.WriteFile(metaSidecarPath(t, dir, "alpha"), []byte(corrupt), 0o644); err != nil {
		t.Fatalf("写坏 sidecar: %v", err)
	}
	journalBefore, err := os.ReadFile(metaJournalPath(t, dir, "alpha"))
	if err != nil {
		t.Fatalf("读 journal: %v", err)
	}
	if items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"}); len(items) != 3 {
		t.Fatalf("损坏 sidecar 后重建结果 = %v, 期望 3 条", metaQueryIDs(items))
	}
	journalAfter, err := os.ReadFile(metaJournalPath(t, dir, "alpha"))
	if err != nil {
		t.Fatalf("重读 journal: %v", err)
	}
	if string(journalAfter) != string(journalBefore) {
		t.Errorf("查询改动了 journal")
	}

	// (b2) 一份"看起来自洽但条数不对"的投影：偏移合法、内容自造，只有序号能识破。
	forged := `{"v":1,"thread":"alpha","offset":1,"items":1,"meta":{"ref":{"thread_id":"alpha","checkpoint_id":"ghost"},"revision":99}}` + "\n"
	if err := os.WriteFile(metaSidecarPath(t, dir, "alpha"), []byte(forged), 0o644); err != nil {
		t.Fatalf("写伪造 sidecar: %v", err)
	}
	if items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"}); len(items) != 3 {
		t.Fatalf("伪造条数的 sidecar 后 = %v, 期望按 journal 重建出 3 条", metaQueryIDs(items))
	}
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); got[0] == "ghost" {
		t.Fatalf("投影里凭空多出的记录进了查询: %v", got)
	}

	// (b3) 投影整行被吞掉但偏移已经覆盖全 journal：只有序号能发现少了一条。
	good := metaSidecarLines(t, metaSidecarPath(t, dir, "alpha"))
	if len(good) < 4 {
		t.Fatalf("基线投影行数 = %d, 期望至少 4 行（3 条摘要 + 1 行推进）", len(good))
	}
	dropped := append([]string{good[0]}, good[2:len(good)]...)
	if err := os.WriteFile(metaSidecarPath(t, dir, "alpha"), []byte(strings.Join(dropped, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("写丢行的 sidecar: %v", err)
	}
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 3 {
		t.Fatalf("投影少一行后 = %v, 期望重建出 3 条", got)
	}

	// (c) sidecar 是从别的线程拷来的：线程标识不符要重建。
	other := t.TempDir()
	otherStore := metaOpenFile(t, other)
	metaCheckpointID(t, otherStore, "beta", "b1", "run-b1")
	betaSidecar := metaSidecarLines(t, metaSidecarPath(t, other, "beta"))
	if len(betaSidecar) == 0 {
		t.Fatalf("beta 没有写出 sidecar 行")
	}
	alphaPath := metaSidecarPath(t, dir, "alpha")
	alphaBefore, err := os.ReadFile(alphaPath)
	if err != nil {
		t.Fatalf("读 alpha 的 sidecar: %v", err)
	}
	if err := os.WriteFile(alphaPath, []byte(betaSidecar[0]+"\n"), 0o644); err != nil {
		t.Fatalf("覆盖 alpha 的 sidecar: %v", err)
	}
	if items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"}); len(items) != 3 {
		t.Fatalf("错线程 sidecar 后 = %v, 期望 3 条", metaQueryIDs(items))
	}
	// 重建必须与增量写入得出逐字相同的投影：beta 的行被替掉，且偏移、条数、内容都回到原样。
	rewritten, err := os.ReadFile(alphaPath)
	if err != nil {
		t.Fatalf("重读 alpha 的 sidecar: %v", err)
	}
	if !bytes.Equal(rewritten, alphaBefore) {
		t.Errorf("重建出来的投影与增量写入的不一致:\n重建 %s\n原样 %s", rewritten, alphaBefore)
	}

	// (c2) 别的线程的行，但偏移正好落在本线程的行边界上：只有线程标识能识破。
	forgedThread := fmt.Sprintf(
		`{"v":1,"thread":"beta","offset":%d,"items":1,"meta":{"ref":{"thread_id":"beta","checkpoint_id":"b1"},"revision":1}}`+"\n",
		metaJournalSize(t, dir, "alpha"))
	if err := os.WriteFile(alphaPath, []byte(forgedThread), 0o644); err != nil {
		t.Fatalf("写错线程的伪造行: %v", err)
	}
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 3 || got[0] != "a1" {
		t.Fatalf("错线程但偏移合法的投影被照用: %v", got)
	}

	// (c3) 偏移倒退：条数仍然自洽，只有单调性检查能发现这段记录会被再读一遍。
	current := metaSidecarLines(t, alphaPath)
	if len(current) < 4 {
		t.Fatalf("基线投影行数 = %d, 期望至少 4 行", len(current))
	}
	var first metadataSidecarLine
	if err := json.Unmarshal([]byte(current[0]), &first); err != nil {
		t.Fatalf("解析投影首行: %v", err)
	}
	var advance metadataSidecarLine
	if err := json.Unmarshal([]byte(current[len(current)-1]), &advance); err != nil {
		t.Fatalf("解析投影末行: %v", err)
	}
	if advance.Meta != nil || advance.Offset <= first.Offset {
		t.Fatalf("投影末行不像推进行: %+v", advance)
	}
	advance.Offset = first.Offset
	regressed, err := json.Marshal(advance)
	if err != nil {
		t.Fatalf("编码倒退偏移: %v", err)
	}
	lines := append(append([]string(nil), current[:len(current)-1]...), string(regressed))
	if err := os.WriteFile(alphaPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("写倒退偏移的投影: %v", err)
	}
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 3 {
		t.Fatalf("倒退偏移后 = %v, 期望重建出 3 条而不重复", got)
	}

	// (d) journal 变短（被截断或重写）：投影立即失效并按新内容重建。
	truncated := metaFirstLines(t, metaJournalPath(t, dir, "alpha"), 2)
	if err := os.WriteFile(metaJournalPath(t, dir, "alpha"), truncated, 0o644); err != nil {
		t.Fatalf("截短 journal: %v", err)
	}
	items = metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if len(items) != 2 {
		t.Fatalf("journal 截短后 = %v, 期望只剩 2 条", metaQueryIDs(items))
	}
	// 之后再提交一条，投影必须能接着往上长。
	metaCheckpointID(t, store, "alpha", "a9", "run-a9")
	items = metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if got := metaQueryIDs(items); len(got) != 3 || got[2] != "a9" {
		t.Fatalf("截短后继续提交 = %v, 期望 [a1 a2 a9]", got)
	}
	if _, ok, err := store.ReadSessionHead(context.Background(), "alpha"); err != nil || !ok {
		t.Fatalf("ReadSessionHead = ok %v err %v", ok, err)
	}
}

func metaQueryIDs(items []CheckpointMetadata) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.Ref.CheckpointID)
	}
	return ids
}

// metaFirstLines 取一个文件的前 n 行（含换行），用来模拟 journal 被截短。
func metaFirstLines(t *testing.T, path string, n int) []byte {
	t.Helper()
	fh, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开 %s: %v", path, err)
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	var builder strings.Builder
	count := 0
	for scanner.Scan() {
		builder.WriteString(scanner.Text())
		builder.WriteString("\n")
		count++
		if count >= n {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("扫描 %s: %v", path, err)
	}
	return []byte(builder.String())
}

func TestFileMetadataOtherProcessRefresh(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)
	metaCheckpointID(t, store, "cross", "p1", "run-p1")

	// 基线：父进程已有覆盖到当前 journal 的投影。
	if items := metaQueryAll(t, store, ListOptions{ThreadID: "cross"}); len(items) != 1 {
		t.Fatalf("基线 = %v", metaQueryIDs(items))
	}
	before := metaReadStats(store)
	if metaJournalSize(t, dir, "cross") == 0 {
		t.Fatalf("journal 为空")
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("取测试二进制: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^"+metaProcessHelperName+"$", "-test.v=false")
	cmd.Env = append(os.Environ(),
		metaProcessHelperEnv+"=1",
		metaProcessDirEnv+"="+dir,
		metaProcessThreadEnv+"=cross",
		metaProcessFirstIDEnv+"=c-child1",
		metaProcessSecondIDEnv+"=c-child2",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("接子进程 stdout: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	if !scanner.Scan() {
		_ = cmd.Wait()
		t.Fatalf("子进程没有报告进度: %v", scanner.Err())
	}
	line := strings.TrimSpace(scanner.Text())
	parts := strings.Split(line, "|")
	if len(parts) != 3 || parts[0] != "ready" {
		_ = cmd.Wait()
		t.Fatalf("子进程应答异常: %q", line)
	}
	childSidecarLines := 0
	if _, err := fmt.Sscanf(parts[1], "sidecar=%d", &childSidecarLines); err != nil {
		_ = cmd.Wait()
		t.Fatalf("解析子进程 sidecar 行数: %v", err)
	}
	var gate string
	if _, err := fmt.Sscanf(parts[2], "gate=%s", &gate); err != nil || gate != "open" {
		_ = cmd.Wait()
		t.Fatalf("子进程等待放行信号异常: %q", parts[2])
	}
	if childSidecarLines == 0 {
		t.Errorf("子进程打开时看不到父进程的投影（应当能读到并继续用）")
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("子进程退出异常: %v", err)
	}

	// 跨进程的提交必须被看到，而且不能靠重扫整份 journal。
	items := metaQueryAll(t, store, ListOptions{ThreadID: "cross"})
	got := metaQueryIDs(items)
	want := []string{"p1", "c-child1", "c-child2"}
	if len(got) != len(want) {
		t.Fatalf("跨进程后摘要 = %v, 期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("跨进程后摘要 = %v, 期望 %v", got, want)
		}
	}
	after := metaReadStats(store)
	if after.scans > before.scans {
		t.Errorf("跨进程补齐走了全量扫描: 之前 %+v 之后 %+v", before, after)
	}
	if items[2].RunID != "run-child2" || items[2].Revision != 3 {
		t.Errorf("子进程第二条摘要 = %+v", items[2])
	}

	// 投影整份丢掉时，也要能从 journal 读回来：删掉 sidecar 再查一次。
	if err := os.Remove(metaSidecarPath(t, dir, "cross")); err != nil {
		t.Fatalf("删 sidecar: %v", err)
	}
	rebuildBefore := metaReadStats(store)
	items = metaQueryAll(t, store, ListOptions{ThreadID: "cross"})
	if got := metaQueryIDs(items); len(got) != 3 {
		t.Fatalf("删掉投影后 = %v, 期望仍为 3 条", got)
	}
	rebuildAfter := metaReadStats(store)
	if rebuildAfter.tailLines <= rebuildBefore.tailLines {
		t.Errorf("删掉投影后没有读 journal 补齐: 之前 %+v 之后 %+v", rebuildBefore, rebuildAfter)
	}
	if rebuildAfter.scans > rebuildBefore.scans {
		t.Errorf("删掉投影后走了全量扫描: 之前 %+v 之后 %+v", rebuildBefore, rebuildAfter)
	}
	if rebuildAfter.rebuilds <= rebuildBefore.rebuilds {
		t.Errorf("丢投影没有被计成重建: 之前 %+v 之后 %+v", rebuildBefore, rebuildAfter)
	}
	if lines := metaSidecarLines(t, metaSidecarPath(t, dir, "cross")); len(lines) < 3 {
		t.Errorf("重建后 sidecar 没有写回: %d 行", len(lines))
	}

	// 反向：父进程再提交一条，另一个新实例（模拟重启后的进程）能立刻看到。
	metaCheckpointID(t, store, "cross", "p2", "run-p2")
	fresh := metaOpenFile(t, dir)
	if got := metaQueryIDs(metaQueryAll(t, fresh, ListOptions{ThreadID: "cross"})); len(got) != 4 {
		t.Fatalf("新实例看到的摘要 = %v, 期望 4 条", got)
	}
}

// TestFileMetadataProcessHelper 是上面用例拉起的子进程入口：先报告自己看到的 sidecar 行数，
// 再提交两条并退出。
func TestFileMetadataProcessHelper(t *testing.T) {
	if os.Getenv(metaProcessHelperEnv) != "1" {
		return
	}
	dir := os.Getenv(metaProcessDirEnv)
	threadID := os.Getenv(metaProcessThreadEnv)
	store := metaOpenFile(t, dir)
	items := metaQueryAll(t, store, ListOptions{ThreadID: threadID})
	sidecarLines := len(metaSidecarLines(t, metaSidecarPath(t, dir, threadID)))
	fmt.Printf("ready|sidecar=%d|gate=open\n", sidecarLines)
	if len(items) != 1 || items[0].Ref.CheckpointID != "p1" {
		t.Fatalf("子进程看不到父进程的提交: %v", metaQueryIDs(items))
	}
	metaCheckpointID(t, store, threadID, os.Getenv(metaProcessFirstIDEnv), "run-child1")
	metaCheckpointID(t, store, threadID, os.Getenv(metaProcessSecondIDEnv), "run-child2")
}

func TestFileMetadataIndexFailureAfterCommit(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)
	metaCheckpointID(t, store, "alpha", "a1", "run-a1")

	sidecar := metaSidecarPath(t, dir, "alpha")
	sidecarAfterFirst := metaSidecarLines(t, sidecar)
	if len(sidecarAfterFirst) < 2 || !strings.Contains(strings.Join(sidecarAfterFirst, "\n"), `"checkpoint_id":"a1"`) {
		t.Fatalf("第一次提交没把这条摘要写进投影: %v", sidecarAfterFirst)
	}
	linesAfterFirst := len(sidecarAfterFirst)

	// sidecar 写失败：Commit 仍然成功，journal 已同步，投影落后。
	// 注入点必须是 write 而不是 sync：sync 失败时字节已经进文件，行数就不会变。
	store.mu.Lock()
	store.metaOps.write = func(*os.File, []byte) (int, error) { return 0, errors.New("sidecar write unavailable") }
	store.mu.Unlock()

	revision, err := func() (uint64, error) {
		cp := &Checkpoint{ID: "a2", ThreadID: "alpha", Step: 1, RunID: "run-a2", ParentID: "parent-a2",
			ParentRef: &Ref{ThreadID: "alpha", CheckpointID: "parent-a2"}}
		rec, err := store.Commit(context.Background(), CommitRequest{Checkpoint: cp})
		if err != nil {
			return 0, err
		}
		return rec.Revision, nil
	}()
	if err != nil {
		t.Fatalf("sidecar 写失败让 Commit 失败了: %v", err)
	}
	if revision != 2 {
		t.Fatalf("Commit Revision = %d, 期望 2", revision)
	}
	if got := len(metaSidecarLines(t, sidecar)); got != linesAfterFirst {
		t.Errorf("sidecar 写失败却落了新行: %d 行（原来 %d）", got, linesAfterFirst)
	}
	if stats := metaReadStats(store); stats.sidecarWriteFailures == 0 {
		t.Errorf("投影写失败没有被记下, 无法确认失败真的发生在投影这一侧")
	}
	// 写入口还坏着的时候，查询照常工作：journal 是事实来源，投影只是加速。
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 2 || got[1] != "a2" {
		t.Fatalf("投影写失败期间查询 = %v, 期望 [a1 a2]", got)
	}
	if _, err := os.Stat(metaJournalPath(t, dir, "alpha")); err != nil {
		t.Fatalf("journal 状态异常: %v", err)
	}

	// 修好写入口后，下一次查询必须把落后补齐，两条都在，顺序正确。
	store.mu.Lock()
	store.metaOps = defaultFileOps()
	store.mu.Unlock()
	items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if got := metaQueryIDs(items); len(got) != 2 || got[0] != "a1" || got[1] != "a2" {
		t.Fatalf("补齐后的摘要 = %v, 期望 [a1 a2]", got)
	}
	if lines := metaSidecarLines(t, sidecar); len(lines) < linesAfterFirst+1 {
		t.Errorf("补齐后 sidecar 没有前进: %d 行", len(lines))
	}
	// 再提交一次，确认恢复正常。
	metaCheckpointID(t, store, "alpha", "a3", "run-a3")
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 3 || got[2] != "a3" {
		t.Fatalf("恢复后 = %v, 期望 [a1 a2 a3]", got)
	}
}

func TestFileMetadataNoBlobDecode(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)

	payload := strings.Repeat("大文件内容-", 4096) // 约几十 KB 的文件快照
	for _, id := range []string{"a1", "a2", "a3"} {
		cp := &Checkpoint{
			ID: id, ThreadID: "alpha", Step: 1, RunID: "run-" + id,
			ParentID: "parent-" + id, ParentRef: &Ref{ThreadID: "alpha", CheckpointID: "parent-" + id},
			State: core.State{Messages: []core.Message{{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: payload}}}}},
			FileSnapshot: map[string][]byte{
				"note.txt": []byte(payload),
			},
		}
		if _, err := store.Commit(context.Background(), CommitRequest{Checkpoint: cp}); err != nil {
			t.Fatalf("commit %s: %v", id, err)
		}
	}
	// 让全量读路径先把计数器跑一遍，证明计数挂在正确的地方。
	if _, err := store.History(context.Background(), "alpha"); err != nil {
		t.Fatalf("History: %v", err)
	}
	if after := metaReadStats(store); after.stateDecodes == 0 {
		t.Fatalf("全量 History 没有触发解码计数, 计数器无法证明任何事")
	}

	before := metaReadStats(store)
	items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	after := metaReadStats(store)
	if len(items) != 3 {
		t.Fatalf("摘要条数 = %d, 期望 3", len(items))
	}
	if after.scans > before.scans {
		t.Errorf("稳态摘要查询调用了全量扫描: 之前 %+v 之后 %+v", before, after)
	}
	if after.stateDecodes > before.stateDecodes {
		t.Errorf("稳态摘要查询解码了完整 State: 之前 %+v 之后 %+v", before, after)
	}
	encoded := queryMustJSON(t, items)
	if strings.Contains(encoded, payload) {
		t.Errorf("摘要里出现了文件快照正文")
	}

	// 更强的证据：把 blob 行从 journal 里删掉。全量读会因缺 blob 失败，摘要查询照常工作。
	journalPath := metaJournalPath(t, dir, "alpha")
	raw, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("读 journal: %v", err)
	}
	var kept []string
	removed := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, `"blob"`) {
			removed++
			continue
		}
		kept = append(kept, line)
	}
	if removed == 0 {
		t.Fatalf("journal 里没有 blob 行，测试无法证明不还原文件")
	}
	if err := os.WriteFile(journalPath, []byte(strings.Join(kept, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("重写 journal: %v", err)
	}
	reopened := metaOpenFile(t, dir)
	if _, err := reopened.History(context.Background(), "alpha"); err == nil {
		t.Errorf("删掉 blob 行后全量 History 仍成功, 说明这条证据无效")
	}
	if got := metaQueryIDs(metaQueryAll(t, reopened, ListOptions{ThreadID: "alpha"})); len(got) != 3 {
		t.Fatalf("删掉 blob 行后摘要 = %v, 期望仍为 3 条", got)
	}
	page, err := reopened.ListSessions(context.Background(), ListOptions{})
	if err != nil || len(page.Items) != 1 || page.Items[0].ThreadID != "alpha" {
		t.Fatalf("删掉 blob 行后的会话列表 = %+v, %v", page, err)
	}
	if _, ok, err := reopened.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "a2"}); err != nil || !ok {
		t.Fatalf("删掉 blob 行后按 Ref 回读 = ok %v err %v", ok, err)
	}
}

func TestFileMetadataEnumerationAndSteadyReads(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)
	metaCheckpointID(t, store, "zeta", "z1", "run-z1")
	metaCheckpointID(t, store, "alpha", "a1", "run-a1")

	// 只有 claim/inbox 行的线程不是会话。
	if _, err := store.Deliver(context.Background(), "ghost", InboxMessage{
		ID:      "msg-1",
		Message: core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: "hi"}}},
	}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	// 目录里的无关文件不能被当线程。
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o644); err != nil {
		t.Fatalf("写无关文件: %v", err)
	}
	// 一个不是本后端写出来的 .jsonl（线程名不合规则）也必须被跳过。
	if err := os.WriteFile(filepath.Join(dir, "not a thread.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("写外来文件: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "runs"), 0o755); err != nil {
		t.Fatalf("建子目录: %v", err)
	}

	page, err := store.ListSessions(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	var names []string
	for _, item := range page.Items {
		names = append(names, item.ThreadID)
	}
	if len(names) != 2 || names[0] != "alpha" || names[1] != "zeta" {
		t.Fatalf("会话枚举 = %v, 期望 [alpha zeta]（不含 ghost / notes.txt / .meta.jsonl）", names)
	}
	for _, item := range page.Items {
		if strings.HasSuffix(item.ThreadID, metadataSidecarSuffix) {
			t.Errorf("sidecar 被当成线程: %q", item.ThreadID)
		}
	}

	// 稳态：投影已覆盖时，读 sidecar 就够了，不再读 journal。
	if _, err := store.QueryCheckpoints(context.Background(), ListOptions{}); err != nil {
		t.Fatalf("预热查询: %v", err)
	}
	before := metaReadStats(store)
	if _, err := store.QueryCheckpoints(context.Background(), ListOptions{}); err != nil {
		t.Fatalf("稳态查询: %v", err)
	}
	after := metaReadStats(store)
	if after.scans > before.scans || after.stateDecodes > before.stateDecodes {
		t.Errorf("稳态查询做了全量工作: 之前 %+v 之后 %+v", before, after)
	}
	if after.tailLines > before.tailLines {
		t.Errorf("投影已覆盖时仍在读 journal 追加尾: 之前 %+v 之后 %+v", before, after)
	}

	// sidecar 末行被撕裂（写到一半崩溃）：忽略半条，按上一条继续。
	sidecar := metaSidecarPath(t, dir, "alpha")
	raw, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("读 sidecar: %v", err)
	}
	torn := append(append([]byte(nil), raw...), []byte(`{"v":1,"thread":"alpha","offset`)...)
	if err := os.WriteFile(sidecar, torn, 0o644); err != nil {
		t.Fatalf("追加半条 sidecar: %v", err)
	}
	items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if len(items) != 1 || items[0].Ref.CheckpointID != "a1" {
		t.Fatalf("撕裂尾之后的摘要 = %v", metaQueryIDs(items))
	}
	metaCheckpointID(t, store, "alpha", "a2", "run-a2")
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 2 || got[1] != "a2" {
		t.Fatalf("撕裂尾之后继续提交 = %v, 期望 [a1 a2]", got)
	}
}

func TestFileMetadataLegacyLinesAndLazyRevision(t *testing.T) {
	dir := t.TempDir()

	// 手写一份旧库：两行 legacy checkpoint（无 kind、无 created_at、只有 parent_id），
	// 中间夹一行 claim，末尾再放一行属于别的线程的 legacy 行（旧文件被清洗过时会留下）。
	legacyLines := []string{
		`{"id":"l1","thread_id":"old","step":1,"parent_id":"lp1","state":{"messages":[]}}`,
		`{"version":1,"kind":"claim","claim":{"thread_id":"old","revision":0,"pause_id":"p","owner":"o","fencing_token":1,"expires_at":"2026-10-14T09:00:00Z"}}`,
		`{"id":"l2","thread_id":"old","step":2,"parent_id":"l1","state":{"messages":[]}}`,
		`{"id":"l-x","thread_id":"elsewhere","step":1,"parent_id":"xp","state":{"messages":[]}}`,
	}
	journal := metaJournalPath(t, dir, "old")
	if err := os.WriteFile(journal, []byte(strings.Join(legacyLines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("写旧 journal: %v", err)
	}

	store := metaOpenFile(t, dir)
	items := metaQueryAll(t, store, ListOptions{ThreadID: "old"})
	if len(items) != 2 {
		t.Fatalf("旧库摘要 = %v, 期望 2 条", metaQueryIDs(items))
	}
	if items[0].Ref.CheckpointID != "l1" || items[1].Ref.CheckpointID != "l2" {
		t.Fatalf("旧库顺序 = %v, 期望 [l1 l2]", metaQueryIDs(items))
	}
	// 懒 Revision 与全量读一致：0 值记录按可见顺序补 1、2。
	all, err := store.History(context.Background(), "old")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(all) != 2 || all[0].Revision != items[0].Revision || all[1].Revision != items[1].Revision {
		t.Fatalf("全量读的 Revision = %d/%d, 摘要 = %d/%d", all[0].Revision, all[1].Revision, items[0].Revision, items[1].Revision)
	}
	for _, item := range items {
		if !item.CreatedAt.IsZero() {
			t.Errorf("%s 的 CreatedAt = %v, 旧记录应保持未知", item.Ref.CheckpointID, item.CreatedAt)
		}
		if !item.LegacyParentUnresolved {
			t.Errorf("%s 只有 parent_id, 应报 LegacyParentUnresolved", item.Ref.CheckpointID)
		}
		if item.ParentRef != nil {
			t.Errorf("%s 的 ParentRef = %+v, 旧记录不得猜测线程", item.Ref.CheckpointID, item.ParentRef)
		}
	}

	// 不带线程筛选时也不能把别的线程的行算进来：那一行属于 elsewhere，不属于 old。
	across := metaQueryIDs(metaQueryAll(t, store, ListOptions{}))
	if len(across) != 2 {
		t.Fatalf("跨线程查询 = %v, 期望 2 条（外来的旧行不能进来）", across)
	}
	if _, ok, err := store.ReadSessionHead(context.Background(), "elsewhere"); err != nil || ok {
		t.Errorf("别的线程的行被算进 elsewhere 的 head: ok %v err %v", ok, err)
	}

	// 旧库上第一次查询就把投影写下来，之后的新提交照常追加。
	if lines := metaSidecarLines(t, metaSidecarPath(t, dir, "old")); len(lines) < 2 {
		t.Errorf("旧库重建没有写出投影: %d 行", len(lines))
	}
	metaCheckpointID(t, store, "old", "l3", "run-l3")
	items = metaQueryAll(t, store, ListOptions{ThreadID: "old"})
	if got := metaQueryIDs(items); len(got) != 3 || got[2] != "l3" {
		t.Fatalf("旧库追加后 = %v, 期望 [l1 l2 l3]", got)
	}
	if items[2].Revision != 3 || items[2].CreatedAt.Compare(metaClock()) != 0 {
		t.Errorf("新记录摘要 = %+v", items[2])
	}
	// 时间筛选下，时间未知的旧记录被排除。
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "old", CreatedFrom: metaClock()})); len(got) != 1 || got[0] != "l3" {
		t.Fatalf("时间筛选 = %v, 期望 [l3]", got)
	}
}

func TestFileMetadataJournalTornTailNotConsumed(t *testing.T) {
	dir := t.TempDir()
	store := metaOpenFile(t, dir)
	metaCheckpointID(t, store, "alpha", "a1", "run-a1")
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 1 {
		t.Fatalf("基线 = %v, 期望 1 条", got)
	}

	// 另一个写入者正在追加一条提交：journal 末尾现在只有半行，没有换行。
	half := `{"version":1,"kind":"commit","checkpoint":{"id":"a2","thread_id":"alpha","step":2`
	journal := metaJournalPath(t, dir, "alpha")
	fh, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开 journal 追加: %v", err)
	}
	if _, err := fh.WriteString(half); err != nil {
		fh.Close()
		t.Fatalf("写半条记录: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("关闭 journal: %v", err)
	}

	items := metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})
	if got := metaQueryIDs(items); len(got) != 1 || got[0] != "a1" {
		t.Fatalf("半条未提交记录被读进来了: %v", got)
	}
	// 覆盖到的偏移停在半行之前，所以补全后能被读到。
	sidecar := metaSidecarLines(t, metaSidecarPath(t, dir, "alpha"))
	offsetOfLast := func() int64 {
		var parsed metadataSidecarLine
		if err := json.Unmarshal([]byte(sidecar[len(sidecar)-1]), &parsed); err != nil {
			t.Fatalf("解析 sidecar 末行: %v", err)
		}
		return parsed.Offset
	}
	covered := offsetOfLast()
	full, err := os.ReadFile(journal)
	if err != nil {
		t.Fatalf("读 journal: %v", err)
	}
	if covered >= int64(len(full)) {
		t.Errorf("投影声称覆盖了 %d 字节，但 journal 已有 %d 字节（半条也被算进去了）", covered, len(full))
	}

	fh2, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开 journal 补全: %v", err)
	}
	if _, err := fh2.WriteString("}}\n"); err != nil {
		fh2.Close()
		t.Fatalf("补全半条记录: %v", err)
	}
	if err := fh2.Close(); err != nil {
		t.Fatalf("关闭 journal: %v", err)
	}
	if got := metaQueryIDs(metaQueryAll(t, store, ListOptions{ThreadID: "alpha"})); len(got) != 2 || got[1] != "a2" {
		t.Fatalf("补全后 = %v, 期望 [a1 a2]", got)
	}
	if lines := metaSidecarLines(t, metaSidecarPath(t, dir, "alpha")); len(lines) < len(sidecar) {
		t.Errorf("补全后投影没有前进: 之前 %d 行, 现在 %d 行", len(sidecar), len(lines))
	}
}

// --- 基准 -------------------------------------------------------------------
//
// 卡面要求：记录 BenchmarkMetadataPage 与全量 History 在 1k/10k 条、含文件内容数据上的
// ns/op、B/op、allocs/op。这里只报实测数字，不承诺倍数关系。

const (
	benchThread      = "bench-thread"
	benchFilePayload = 4096
	benchTextPayload = 2048
)

// benchRecord 造一条带正文与文件快照的快照，两种后端用同一份数据。
func benchRecord(index int) *Checkpoint {
	text := strings.Repeat("x", benchTextPayload)
	return &Checkpoint{
		ID:        fmt.Sprintf("cp-%06d", index),
		ThreadID:  benchThread,
		Step:      index,
		ParentID:  fmt.Sprintf("cp-%06d", index-1),
		RunID:     fmt.Sprintf("run-%06d", index),
		TurnID:    fmt.Sprintf("turn-%06d", index),
		CreatedAt: metaClock().Add(time.Duration(index) * time.Second),
		State: core.State{
			Messages: []core.Message{{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: text}}}},
			KV:       map[string]any{"note": text},
		},
		FileSnapshot: map[string][]byte{"note.bin": []byte(strings.Repeat("f", benchFilePayload))},
	}
}

// benchSeedFileJournal 把 n 条提交记录直接写成 journal 行（一条共享 blob + n 条 commit），
// 这样建库不需要走一遍 O(n²) 的写路径，基准测的仍然是读。
func benchSeedFileJournal(t testing.TB, dir string, n int) {
	t.Helper()
	journal := filepath.Join(dir, benchThread+".jsonl")
	fh, err := os.OpenFile(journal, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("建基准 journal: %v", err)
	}
	defer fh.Close()
	writer := bufio.NewWriter(fh)

	payload := []byte(strings.Repeat("f", benchFilePayload))
	blobLine, err := json.Marshal(blobRecord{Blob: hashOf(payload), Data: payload})
	if err != nil {
		t.Fatalf("编码 blob: %v", err)
	}
	if _, err := writer.Write(append(blobLine, '\n')); err != nil {
		t.Fatalf("写 blob: %v", err)
	}
	for i := 1; i <= n; i++ {
		cp := benchRecord(i)
		rec := journalRecord{
			Version: durableFormatVersion,
			Kind:    recordCommit,
			Checkpoint: &checkpointRecord{
				Checkpoint: cp,
				Files:      map[string]string{"note.bin": hashOf(payload)},
			},
		}
		encoded, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("编码提交记录: %v", err)
		}
		if _, err := writer.Write(append(encoded, '\n')); err != nil {
			t.Fatalf("写提交记录: %v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("刷写基准 journal: %v", err)
	}
}

// benchSeed 建一个装了 size 条记录的后端。基准测的是读，所以 File 一侧直接铺 journal，
// 不去跑 O(n^2) 的写路径。
func benchSeed(b *testing.B, backend string, size int) (Query, Checkpointer) {
	b.Helper()
	fixed := metaClock()
	switch backend {
	case "memory":
		m := NewMemory()
		m.now = func() time.Time { return fixed }
		m.projections.now = func() time.Time { return fixed }
		for i := 1; i <= size; i++ {
			if _, err := m.Commit(context.Background(), CommitRequest{Checkpoint: benchRecord(i)}); err != nil {
				b.Fatalf("commit: %v", err)
			}
		}
		return m, m
	case "file":
		dir := b.TempDir()
		benchSeedFileJournal(b, dir, size)
		f := metaOpenFile(b, dir)
		return f, f
	}
	b.Fatalf("未知后端 %q", backend)
	return nil, nil
}

// BenchmarkMetadataPage 测稳态的一页：投影已在预热时冻结，一次调用只取 MaxQueryLimit 条摘要。
func BenchmarkMetadataPage(b *testing.B) {
	for _, backend := range []string{"memory", "file"} {
		for _, size := range []int{1000, 10000} {
			b.Run(fmt.Sprintf("%s/%d", backend, size), func(b *testing.B) {
				query, _ := benchSeed(b, backend, size)
				warm, err := query.QueryCheckpoints(context.Background(), ListOptions{Limit: MaxQueryLimit})
				if err != nil {
					b.Fatalf("预热首页: %v", err)
				}
				if len(warm.Items) != MaxQueryLimit || warm.NextCursor == "" {
					b.Fatalf("预热首页 = %d 条, cursor %q", len(warm.Items), warm.NextCursor)
				}
				// 回到同一冻结投影的开头，之后每次调用都只读这一页。
				payload, err := decodeCursor(warm.NextCursor)
				if err != nil {
					b.Fatalf("解析首页 cursor: %v", err)
				}
				payload.Offset = 0
				token := encodeCursor(payload)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					page, err := query.QueryCheckpoints(context.Background(), ListOptions{Limit: MaxQueryLimit, Cursor: token})
					if err != nil {
						b.Fatalf("查询: %v", err)
					}
					if len(page.Items) != MaxQueryLimit {
						b.Fatalf("首页条数 = %d, 期望 %d", len(page.Items), MaxQueryLimit)
					}
				}
			})
		}
	}
}

// BenchmarkMetadataFullWalk 走完 size 条摘要，含每次新建的冻结投影，是"列出全部会话摘要"
// 这一类调用真正付的代价。
func BenchmarkMetadataFullWalk(b *testing.B) {
	for _, backend := range []string{"memory", "file"} {
		for _, size := range []int{1000, 10000} {
			b.Run(fmt.Sprintf("%s/%d", backend, size), func(b *testing.B) {
				query, _ := benchSeed(b, backend, size)
				benchWalkAll(b, query, size)
			})
		}
	}
}

func benchWalkAll(b *testing.B, query Query, size int) {
	b.Helper()
	walk := func() int {
		total, cursor := 0, ""
		for page := 0; ; page++ {
			if page > 200 {
				b.Fatalf("分页没有收敛, 已累计 %d 条", total)
			}
			batch, err := query.QueryCheckpoints(context.Background(), ListOptions{Limit: MaxQueryLimit, Cursor: cursor})
			if err != nil {
				b.Fatalf("查询: %v", err)
			}
			total += len(batch.Items)
			if batch.NextCursor == "" {
				return total
			}
			cursor = batch.NextCursor
		}
	}
	if got := walk(); got != size {
		b.Fatalf("走读条数 = %d, 期望 %d", got, size)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := walk(); got != size {
			b.Fatalf("走读条数 = %d, 期望 %d", got, size)
		}
	}
}

func BenchmarkHistoryFull(b *testing.B) {
	for _, backend := range []string{"memory", "file"} {
		for _, size := range []int{1000, 10000} {
			b.Run(fmt.Sprintf("%s/%d", backend, size), func(b *testing.B) {
				_, store := benchSeed(b, backend, size)
				benchFullLoop(b, store, size)
			})
		}
	}
}

func benchFullLoop(b *testing.B, store Checkpointer, size int) {
	b.Helper()
	warm, err := store.History(context.Background(), benchThread)
	if err != nil {
		b.Fatalf("预热 History: %v", err)
	}
	if len(warm) != size {
		b.Fatalf("预热条数 = %d, 期望 %d", len(warm), size)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		all, err := store.History(context.Background(), benchThread)
		if err != nil {
			b.Fatalf("History: %v", err)
		}
		if len(all) != size {
			b.Fatalf("条数 = %d, 期望 %d", len(all), size)
		}
	}
}
