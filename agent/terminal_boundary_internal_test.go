package agent

// 结束边界由每次成功提交的回执写入。并行分支共用同一份 durable 状态，而回执返回的
// 顺序可以晚于存储提交的顺序：较早 revision 的回执如果最后到达，边界就会退回到已被
// 更晚提交覆盖的 revision，终态记录随即被自己冻结的 CAS 拒绝。
//
// lateReceiptStore 只暴露 Durable 能力（不含线程协调），这正是提交不被写锁串行的
// 那类存储；它把 revision 1 的回执压住，让测试能够把"迟到的回执"排在另一位分支
// 的边界写入之后。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

type lateReceiptStore struct {
	checkpoint.Durable

	mu      sync.Mutex
	ordered []checkpoint.CommitRequest

	// parked 在 revision 1 已落盘、回执尚未返回时关闭；release 关闭后回执才返回。
	parked  chan struct{}
	release chan struct{}
	once    sync.Once
}

func newLateReceiptStore() *lateReceiptStore {
	return &lateReceiptStore{
		Durable: checkpoint.NewMemory(),
		parked:  make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *lateReceiptStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	receipt, err := s.Durable.Commit(ctx, req)
	if err != nil {
		return receipt, err
	}
	s.mu.Lock()
	s.ordered = append(s.ordered, req)
	s.mu.Unlock()
	if receipt.Revision != 1 {
		return receipt, nil
	}
	s.once.Do(func() { close(s.parked) })
	select {
	case <-s.release:
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return receipt, ctx.Err()
	}
	return receipt, nil
}

func (s *lateReceiptStore) committedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.ordered))
	for _, req := range s.ordered {
		if req.Checkpoint != nil {
			ids = append(ids, req.Checkpoint.ID)
		}
	}
	return ids
}

func lateReceiptBranch(name string, ctx context.Context, threadID string, store *lateReceiptStore, shared *durableRunState) *RunContext {
	return &RunContext{
		Context:  ctx,
		RunID:    "run-1",
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{Messages: []core.Message{core.AssistantText(name)}},
		branch:   name,
		durable:  shared,
	}
}

// TestLateCommitReceiptKeepsHighestBoundary：两个分支共用一份 durable 状态，先提交
// 的分支最后返回回执。边界必须留在更高 revision 的那次提交上，ID 与 revision 成对
// 更新。
func TestLateCommitReceiptKeepsHighestBoundary(t *testing.T) {
	ctx := context.Background()
	const threadID = "late-receipt"
	store := newLateReceiptStore()
	shared := newDurableRunState()
	early := lateReceiptBranch("early", ctx, threadID, store, shared)
	late := lateReceiptBranch("late", ctx, threadID, store, shared)

	earlyDone := make(chan error, 1)
	go func() {
		earlyDone <- early.commitCheckpoint(&checkpoint.Checkpoint{
			ID:       "cp-early",
			ThreadID: threadID,
			State:    *early.State,
		}, false)
	}()

	select {
	case <-store.parked:
	case <-time.After(5 * time.Second):
		t.Fatal("第一次提交没有在存储落盘")
	}

	// 较新的提交在本 goroutine 走完：它在存储里拿到 revision 2，并写下边界。
	if err := late.commitCheckpoint(&checkpoint.Checkpoint{
		ID:       "cp-late",
		ThreadID: threadID,
		State:    *late.State,
	}, false); err != nil {
		t.Fatalf("较新提交失败: %v", err)
	}
	if id, revision, ok := late.committedBoundary(); !ok || id != "cp-late" || revision != 2 {
		t.Fatalf("较新提交后的边界 = %s/%d(%v)，期望 cp-late/2", id, revision, ok)
	}

	// 现在才放行第一次提交的回执——它的边界写入是最后发生的。
	close(store.release)
	if err := <-earlyDone; err != nil {
		t.Fatalf("迟到回执的提交失败: %v", err)
	}

	if id, revision, ok := early.committedBoundary(); !ok || id != "cp-late" || revision != 2 {
		t.Fatalf("迟到回执把边界退回成 %s/%d(%v)，期望留在 cp-late/2", id, revision, ok)
	}
	if ids := store.committedIDs(); len(ids) != 2 || ids[0] != "cp-early" || ids[1] != "cp-late" {
		t.Fatalf("提交顺序不符: %v", ids)
	}
}

// TestNoteCommittedBoundaryIsMonotonic：边界的推进规则——尚无边界时接受任何一对；
// 已有边界时只有更高的 revision 能成对替换，同 revision 与更低的 revision 都不动它。
func TestNoteCommittedBoundaryIsMonotonic(t *testing.T) {
	rc := &RunContext{
		Context:  context.Background(),
		ThreadID: "monotonic",
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	if _, _, ok := rc.committedBoundary(); ok {
		t.Fatal("新状态不应已有边界")
	}

	rc.noteCommittedBoundary("cp-high", 5)

	steps := []struct {
		id       string
		revision uint64
		wantID   string
		wantRev  uint64
		what     string
	}{
		{"cp-low", 3, "cp-high", 5, "更低的 revision"},
		{"cp-same", 5, "cp-high", 5, "相同的 revision"},
		{"cp-first", 1, "cp-high", 5, "最低的 revision"},
		{"cp-newer", 6, "cp-newer", 6, "更高的 revision"},
	}
	for _, s := range steps {
		rc.noteCommittedBoundary(s.id, s.revision)
		id, revision, ok := rc.committedBoundary()
		if !ok || id != s.wantID || revision != s.wantRev {
			t.Fatalf("%s 之后边界 = %s/%d(%v)，期望 %s/%d", s.what, id, revision, ok, s.wantID, s.wantRev)
		}
	}
}

// TestParallelRunRecordsCompletionWithLateReceipt：正常完成的 Parallel 运行里，
// revision 1 的回执晚于 revision 2 的边界写入返回。终态必须仍写在更高的 revision 上。
//
// 分支的 TurnDone 事件在它这一步的提交（含边界写入）之后发布，所以测试收到第一个
// TurnDone 就能确定：未压住的分支已经写完边界。此时才放行 revision 1 的回执。
func TestParallelRunRecordsCompletionWithLateReceipt(t *testing.T) {
	ctx := context.Background()
	const threadID = "parallel-late-receipt"
	store := newLateReceiptStore()
	root := Parallel("fan", lateReceiptTextAgent(t, "one"), lateReceiptTextAgent(t, "two"))
	root.store = store

	run := root.Stream(ctx, "root", OnThread(threadID))
	events, cancelEvents := run.Events(bus.Lossless)
	defer cancelEvents()

	type waitResult struct {
		text string
		err  error
	}
	waited := make(chan waitResult, 1)
	go func() {
		res, err := run.Wait()
		waited <- waitResult{text: res.Message.Text(), err: err}
	}()

	// 等另一条分支把它的边界写完。
	turns := 0
	for turns < 1 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("事件流在分支完成前结束")
			}
			if _, done := ev.(core.TurnDone); done {
				turns++
			}
		case <-time.After(10 * time.Second):
			t.Fatal("并行分支没有提交检查点")
		}
	}
	close(store.release)

	var res waitResult
	select {
	case res = <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("并行运行没有结束")
	}
	if res.err != nil {
		t.Fatalf("并行运行失败: %v", res.err)
	}
	if res.text != "one\n\ntwo" {
		t.Fatalf("并行结果 = %q，期望 %q", res.text, "one\n\ntwo")
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.Revision != 2 {
		t.Fatalf("head = %+v，期望 revision 2", head)
	}

	// 症状先行断言：终态必须记在运行结束时的 head 上，而不是被迟到回执拽回
	// revision 1 后被自己的 CAS 拒绝。
	if err := root.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("并行运行的终态记录失败: %v", err)
	}
	if id, revision, ok := run.rc.committedBoundary(); !ok || id != head.ID || revision != head.Revision {
		t.Fatalf("结束边界 = %s/%d(%v)，与运行结束时 head %s/%d 不一致", id, revision, ok, head.ID, head.Revision)
	}

	if head, err = store.Latest(ctx, threadID); err != nil {
		t.Fatal(err)
	}
	if head.Completion == nil || head.Completion.RunID != run.ID {
		t.Fatalf("终态没有写入: %+v", head.Completion)
	}
	if err := root.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("重复记录应确认同一条终态: %v", err)
	}
}

func lateReceiptTextAgent(t *testing.T, text string) *Agent {
	t.Helper()
	a, err := New(WithModel(mock.New("say-"+text, func(*llm.Request) *llm.Response { return mock.Text(text) })))
	if err != nil {
		t.Fatal(err)
	}
	return a
}
