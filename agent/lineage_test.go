package agent

// SM-02（ADR-0034 D2）：所有内置提交路径都要留下可定位的谱系与运行身份。
// 本文件用内部包测试，是为了直接检查运行内的提交序列（失败注入与并发先后），
// 这些是只看公开 API 无法证实的部分。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// --- 夹具 -------------------------------------------------------------------

// lineageStore 包装内层协调器：记录每次 Commit 的请求顺序，可按次序注入失败，
// 或让第 N 次提交卡在闸门上等外部放行。
type lineageStore struct {
	checkpoint.ThreadCoordinator

	mu       sync.Mutex
	commits  []checkpoint.CommitRequest
	attempts int
	failOn   map[int]error
	gateOn   int
	entered  chan struct{}
	release  chan struct{}
	gateOnce sync.Once
}

func newLineageStore(base checkpoint.ThreadCoordinator) *lineageStore {
	return &lineageStore{
		ThreadCoordinator: base,
		failOn:            map[int]error{},
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
}

func (s *lineageStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	s.attempts++
	n := s.attempts
	if s.failOn != nil && s.failOn[n] != nil {
		err := s.failOn[n]
		s.mu.Unlock()
		return checkpoint.CommitReceipt{}, err
	}
	s.commits = append(s.commits, req)
	gated := s.gateOn == n
	s.mu.Unlock()
	if gated {
		s.gateOnce.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return checkpoint.CommitReceipt{}, ctx.Err()
		}
	}
	return s.ThreadCoordinator.Commit(ctx, req)
}

// lineagePlainStore 只实现 Checkpointer，代表没有协调与提交能力的基础 store。
type lineagePlainStore struct {
	mu       sync.Mutex
	byThread map[string][]*checkpoint.Checkpoint
}

func newLineagePlainStore() *lineagePlainStore {
	return &lineagePlainStore{byThread: map[string][]*checkpoint.Checkpoint{}}
}

func (p *lineagePlainStore) Save(_ context.Context, cp *checkpoint.Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return errors.New("plain: checkpoint requires a thread")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	stored := *cp
	p.byThread[cp.ThreadID] = append(p.byThread[cp.ThreadID], &stored)
	return nil
}

func (p *lineagePlainStore) Load(_ context.Context, threadID, id string) (*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cp := range p.byThread[threadID] {
		if cp.ID == id {
			return cp, nil
		}
	}
	return nil, errors.New("plain: not found")
}

func (p *lineagePlainStore) Latest(_ context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hist := p.byThread[threadID]
	if len(hist) == 0 {
		return nil, nil
	}
	return hist[len(hist)-1], nil
}

func (p *lineagePlainStore) History(_ context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*checkpoint.Checkpoint(nil), p.byThread[threadID]...), nil
}

func lineageEchoTool(name string, calls *atomic.Int32, block <-chan struct{}) tool.Tool {
	return tool.New(name, "echo back", func(*tool.Context, struct{}) (string, error) {
		if block != nil {
			<-block
		}
		calls.Add(1)
		return "done", nil
	})
}

type lineageGate struct {
	BaseMiddleware
}

func (lineageGate) BeforeTool(lc *LoopContext, c *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(c) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

// --- 断言 -------------------------------------------------------------------

// assertChain 检查一段历史是同线程的完整线性链：首条没有父边，之后每条的
// ParentID 与 ParentRef 都指向前一条；所有记录共享同一 RunID 与 TurnID；
// 每条都有本次构造的创建时间，且不早于前一条。
func assertChain(t *testing.T, hist []*checkpoint.Checkpoint, runID string) string {
	t.Helper()
	if len(hist) == 0 {
		t.Fatal("历史为空")
	}
	var turnID string
	var previous time.Time
	for i, cp := range hist {
		if cp.RunID != runID {
			t.Fatalf("第 %d 条 RunID = %q, want %q", i, cp.RunID, runID)
		}
		if cp.TurnID == "" {
			t.Fatalf("第 %d 条没有 TurnID", i)
		}
		if turnID == "" {
			turnID = cp.TurnID
		} else if cp.TurnID != turnID {
			t.Fatalf("第 %d 条 TurnID = %q, want 同一次输入的 %q", i, cp.TurnID, turnID)
		}
		if cp.CreatedAt.IsZero() {
			t.Fatalf("第 %d 条没有创建时间", i)
		}
		if cp.CreatedAt.Before(previous) {
			t.Fatalf("第 %d 条创建时间早于前一条: %s < %s", i, cp.CreatedAt, previous)
		}
		previous = cp.CreatedAt
		if i == 0 {
			if cp.ParentID != "" || cp.ParentRef != nil {
				t.Fatalf("新线程首条不该有父边: %q %+v", cp.ParentID, cp.ParentRef)
			}
			continue
		}
		want := hist[i-1]
		if cp.ParentID != want.ID {
			t.Fatalf("第 %d 条 ParentID = %q, want 前一条 %q", i, cp.ParentID, want.ID)
		}
		if cp.ParentRef == nil || cp.ParentRef.CheckpointID != want.ID || cp.ParentRef.ThreadID != want.ThreadID {
			t.Fatalf("第 %d 条 ParentRef = %+v, want {%s, %s}", i, cp.ParentRef, want.ThreadID, want.ID)
		}
	}
	return turnID
}

// --- 普通、计划、并行、恢复路径 ---------------------------------------------

func TestLineageNormal(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var calls atomic.Int32
	a, err := New(
		WithCheckpointer(store),
		WithTools(lineageEchoTool("echo", &calls, nil)),
		WithModel(mock.New("normal", func(req *llm.Request) *llm.Response {
			switch toolResultRounds(req) {
			case 0:
				return mock.CallTool("call-1", "echo", `{}`)
			case 1:
				return mock.CallTool("call-2", "echo", `{}`)
			default:
				return mock.Text("done")
			}
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", OnThread("lineage-normal"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("工具执行次数 = %d, want 2", calls.Load())
	}
	hist, err := store.History(ctx, "lineage-normal")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Fatalf("两次工具往返加收尾只提交了 %d 条, want 3 条: %v", len(hist), commitIDs(hist))
	}
	assertChain(t, hist, run.ID)
}

// toolResultRounds 数请求里已带的工具结果批次，即模型已经进行过的工具往返轮数。
func toolResultRounds(req *llm.Request) int {
	rounds := 0
	for _, m := range req.Messages {
		if m.Role == core.RoleTool {
			rounds++
		}
	}
	return rounds
}

// TestLineagePlainStore：没有协调与提交能力的基础 store 也填谱系，只是不额外
// 承诺跨实例一致性。
func TestLineagePlainStore(t *testing.T) {
	ctx := context.Background()
	store := newLineagePlainStore()
	var calls atomic.Int32
	a, err := New(
		WithCheckpointer(store),
		WithTools(lineageEchoTool("echo", &calls, nil)),
		WithModel(mock.New("plain", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("done")
			}
			return mock.CallTool("call-1", "echo", `{}`)
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", OnThread("lineage-plain"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History(ctx, "lineage-plain")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) == 0 {
		t.Fatal("基础 store 没有任何提交")
	}
	assertChain(t, hist, run.ID)
}

func TestLineagePlan(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	worker, err := New(
		WithCheckpointer(store),
		WithModel(mock.New("worker", func(req *llm.Request) *llm.Response {
			return mock.Text(req.Messages[len(req.Messages)-1].Text())
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	root := NewPlan(
		"plan",
		Plan{Nodes: []Node{
			{ID: "first", Task: "first"},
			{ID: "second", Task: "second", DependsOn: []string{"first"}},
		}},
		WithWorker(worker),
		WithPlanCheckpointer(store),
	)
	run := root.Stream(ctx, "go", OnThread("lineage-plan"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History(ctx, "lineage-plan")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) < 3 {
		t.Fatalf("计划运行只提交了 %d 条: %v", len(hist), commitIDs(hist))
	}
	assertChain(t, hist, run.ID)
}

func TestLineageParallel(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	branch := func(name string) *Agent {
		var calls atomic.Int32
		a, err := New(
			WithName(name),
			WithCheckpointer(store),
			WithTools(lineageEchoTool("echo-"+name, &calls, nil)),
			WithModel(mock.New(name, func(req *llm.Request) *llm.Response {
				if _, ok := mock.LastToolResult(req); ok {
					return mock.Text(name + " done")
				}
				return mock.CallTool("call-1", "echo-"+name, `{}`)
			})),
		)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	root := Parallel("parallel", branch("a"), branch("b"))
	root.store = store
	run := root.Stream(ctx, "go", OnThread("lineage-parallel"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History(ctx, "lineage-parallel")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) < 3 {
		t.Fatalf("并行运行只提交了 %d 条: %v", len(hist), commitIDs(hist))
	}
	// 两个分支共享同一条提交序列：任何一条都不能与别人同父，否则就是各自从旧
	// head 构造出来的并行兄弟，历史不再是线性链。
	seenParents := map[string]string{}
	for _, cp := range hist {
		if cp.ParentID == "" {
			continue
		}
		if other, ok := seenParents[cp.ParentID]; ok {
			t.Fatalf("%s 与 %s 共用父记录 %s", cp.ID, other, cp.ParentID)
		}
		seenParents[cp.ParentID] = cp.ID
	}
	for _, cp := range hist {
		if cp.RunID != run.ID {
			t.Fatalf("%s 的 RunID = %q, want %q", cp.ID, cp.RunID, run.ID)
		}
		if cp.TurnID == "" {
			t.Fatalf("%s 没有 TurnID", cp.ID)
		}
	}
}

func TestLineageResume(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var calls atomic.Int32
	a, err := New(
		WithCheckpointer(store),
		WithTools(lineageEchoTool("danger", &calls, nil)),
		WithMiddleware(lineageGate{}),
		WithModel(mock.New("gated", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("resumed")
			}
			return mock.CallTool("call-1", "danger", `{}`)
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	first := a.Stream(ctx, "go", OnThread("lineage-resume"))
	var pending []core.ApprovalRequest
	for ev, err := range first.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
		}
	}
	if len(pending) != 1 {
		t.Fatalf("没有停在待审批的一批: %v", pending)
	}
	if calls.Load() != 0 {
		t.Fatal("暂停前工具就执行了")
	}

	paused, err := store.Latest(ctx, "lineage-resume")
	if err != nil {
		t.Fatal(err)
	}
	if paused.RunID != first.ID || paused.TurnID == "" {
		t.Fatalf("暂停记录身份不符: run=%q want %q turn=%q", paused.RunID, first.ID, paused.TurnID)
	}

	second, err := a.Resume(ctx, "lineage-resume", Allow(pending[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatal("恢复复用了旧 RunID")
	}
	hist, err := store.History(ctx, "lineage-resume")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) < 2 {
		t.Fatalf("恢复后历史过短: %v", commitIDs(hist))
	}
	// 暂停记录及其之前的提交属于第一次运行；恢复之后的提交属于第二次运行，
	// 但整个线程仍是一条链，且两轮共用同一个 TurnID（同一次用户输入）。
	turnID := paused.TurnID
	var firstRun, secondRun int
	for i, cp := range hist {
		if cp.TurnID != turnID {
			t.Fatalf("第 %d 条 TurnID = %q, want 沿用暂停记录的 %q", i, cp.TurnID, turnID)
		}
		if cp.CreatedAt.IsZero() {
			t.Fatalf("第 %d 条没有创建时间", i)
		}
		switch cp.RunID {
		case first.ID:
			firstRun++
		case second.ID:
			secondRun++
		default:
			t.Fatalf("第 %d 条 RunID = %q, 不属于两轮运行中的任何一轮", i, cp.RunID)
		}
		if i > 0 {
			prev := hist[i-1]
			if cp.ParentID != prev.ID {
				t.Fatalf("第 %d 条 ParentID = %q, want 前一条 %q", i, cp.ParentID, prev.ID)
			}
			if cp.ParentRef == nil || cp.ParentRef.CheckpointID != prev.ID || cp.ParentRef.ThreadID != prev.ThreadID {
				t.Fatalf("第 %d 条 ParentRef = %+v, want {%s, %s}", i, cp.ParentRef, prev.ThreadID, prev.ID)
			}
		}
	}
	if firstRun == 0 || secondRun == 0 {
		t.Fatalf("两轮运行各自都要留下提交: first=%d second=%d", firstRun, secondRun)
	}
}

// TestLineageAcrossRuns：同一线程上第二次输入续在第一次的最后一条成功提交之后，
// 换 RunID 也换新的 TurnID。协调器 store 与只有基础 Save 的 store 都验证，
// 因为继承 head 分别在 BeginRun 与 restore 解析。
func TestLineageAcrossRuns(t *testing.T) {
	ctx := context.Background()
	cases := map[string]checkpoint.Checkpointer{
		"durable": checkpoint.NewMemory(),
		"plain":   newLineagePlainStore(),
	}
	for name, store := range cases {
		t.Run(name, func(t *testing.T) {
			threadID := "lineage-across-" + name
			a, err := New(
				WithCheckpointer(store),
				WithModel(mock.New("text", func(*llm.Request) *llm.Response { return mock.Text("answer") })),
			)
			if err != nil {
				t.Fatal(err)
			}
			first := a.Stream(ctx, "one", OnThread(threadID))
			if _, err := first.Wait(); err != nil {
				t.Fatal(err)
			}
			second := a.Stream(ctx, "two", OnThread(threadID))
			if _, err := second.Wait(); err != nil {
				t.Fatal(err)
			}
			hist, err := store.History(ctx, threadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 2 {
				t.Fatalf("两次运行提交了 %d 条, want 2: %v", len(hist), commitIDs(hist))
			}
			if hist[0].ParentID != "" || hist[0].ParentRef != nil {
				t.Fatalf("首条不该有父边: %q %+v", hist[0].ParentID, hist[0].ParentRef)
			}
			if hist[1].ParentID != hist[0].ID {
				t.Fatalf("第二次运行的父记录 = %q, want 第一次的最后一条 %q", hist[1].ParentID, hist[0].ID)
			}
			if hist[1].ParentRef == nil || hist[1].ParentRef.CheckpointID != hist[0].ID || hist[1].ParentRef.ThreadID != threadID {
				t.Fatalf("第二次运行的 ParentRef = %+v", hist[1].ParentRef)
			}
			if hist[0].RunID != first.ID || hist[1].RunID != second.ID {
				t.Fatalf("两次运行的身份不符: %q %q, want %q %q", hist[0].RunID, hist[1].RunID, first.ID, second.ID)
			}
			if hist[0].TurnID == "" || hist[1].TurnID == "" {
				t.Fatalf("两次运行都要带 TurnID: %q %q", hist[0].TurnID, hist[1].TurnID)
			}
			if hist[0].TurnID == hist[1].TurnID {
				t.Fatalf("新的用户输入沿用了旧 TurnID: %q", hist[1].TurnID)
			}
			if hist[0].CreatedAt.IsZero() || hist[1].CreatedAt.IsZero() {
				t.Fatal("两次运行都要有创建时间")
			}
		})
	}
}

// --- 终态记录 ---------------------------------------------------------------

func TestLineageTerminal(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	base := &checkpoint.Checkpoint{
		ID:        "boundary-1",
		ThreadID:  "lineage-terminal",
		Step:      2,
		RunID:     "run-A",
		TurnID:    "turn-A",
		CreatedAt: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC),
		State:     core.State{Messages: []core.Message{core.AssistantText("answer")}},
	}
	if err := store.Save(ctx, base); err != nil {
		t.Fatal(err)
	}
	// 线程 head 之后推进到别的运行，终态记录仍必须钉在本次边界上。
	later := &checkpoint.Checkpoint{ID: "later-1", ThreadID: "lineage-terminal", ParentID: base.ID, Step: 3, RunID: "run-B", TurnID: "turn-B"}
	if err := store.Save(ctx, later); err != nil {
		t.Fatal(err)
	}

	record, err := buildTerminalRecord(ctx, store, "lineage-terminal", "completion-1", base.ID, base.Revision, &checkpoint.Completion{
		RunID: "run-A",
		Kind:  checkpoint.CompletionCompleted,
	}, "turn-A")
	if err != nil {
		t.Fatal(err)
	}
	if record.ParentID != base.ID || record.ParentRef == nil || record.ParentRef.CheckpointID != base.ID {
		t.Fatalf("终态记录没有指向本次边界: %q %+v", record.ParentID, record.ParentRef)
	}
	if record.RunID != "run-A" || record.TurnID != "turn-A" {
		t.Fatalf("终态记录缺少运行身份: run=%q turn=%q", record.RunID, record.TurnID)
	}
	if record.CreatedAt.IsZero() {
		t.Fatal("终态记录没有创建时间")
	}
	if record.ID != "completion-1" || record.Step != base.Step {
		t.Fatalf("终态记录基本字段不符: %+v", record)
	}

	// 旧边界没有 TurnID 时不伪造，记录保持未知。
	legacyBase := &checkpoint.Checkpoint{ID: "legacy-boundary", ThreadID: "lineage-terminal-legacy", Step: 1}
	if err := store.Save(ctx, legacyBase); err != nil {
		t.Fatal(err)
	}
	legacyRecord, err := buildTerminalRecord(ctx, store, legacyBase.ThreadID, "legacy-completion", legacyBase.ID, legacyBase.Revision, &checkpoint.Completion{
		RunID: "run-old",
		Kind:  checkpoint.CompletionCompleted,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if legacyRecord.TurnID != "" || legacyRecord.RunID != "run-old" {
		t.Fatalf("旧边界被补造了 TurnID: %+v", legacyRecord)
	}
}

// --- 失败注入与并发先后 -----------------------------------------------------

// TestLineageCommitFailureDoesNotAdvance：第二次提交失败后，第三次提交的父记录
// 仍是第一次成功提交，而不是失败的那一次。
func TestLineageCommitFailureDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-fail-advance"
	store := newLineageStore(checkpoint.NewMemory())
	store.failOn[2] = errors.New("injected commit failure")

	rc := &RunContext{
		Context:  ctx,
		RunID:    "run-fail",
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	rc.setLineageStart(nil, "turn-fail")

	for _, id := range []string{"fail-cp-1", "fail-cp-2", "fail-cp-3"} {
		err := rc.commitCheckpoint(&checkpoint.Checkpoint{ID: id, ThreadID: threadID, State: *rc.State}, false)
		if id == "fail-cp-2" {
			if err == nil {
				t.Fatal("注入的第二次提交失败没有传出来")
			}
			continue
		}
		if err != nil {
			t.Fatalf("提交 %s: %v", id, err)
		}
	}

	hist, err := store.History(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("历史条数 = %d (%v), want 2", len(hist), commitIDs(hist))
	}
	if hist[1].ID != "fail-cp-3" {
		t.Fatalf("历史顺序不符: %v", commitIDs(hist))
	}
	if hist[0].ParentID != "" || hist[0].ParentRef != nil {
		t.Fatalf("首条不该有父边: %q %+v", hist[0].ParentID, hist[0].ParentRef)
	}
	if hist[1].ParentID != "fail-cp-1" {
		t.Fatalf("失败提交推进了本地 head: 第三次提交的父记录 = %q, want fail-cp-1", hist[1].ParentID)
	}
	if hist[1].ParentRef == nil || hist[1].ParentRef.CheckpointID != "fail-cp-1" {
		t.Fatalf("第三次提交的 ParentRef = %+v", hist[1].ParentRef)
	}

	// 恢复同样按"最后一次成功提交"续链，不看失败的那一次。
	next := &RunContext{
		Context:  ctx,
		RunID:    "run-next",
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	next.setLineageStart(&checkpoint.Ref{ThreadID: threadID, CheckpointID: hist[1].ID}, "turn-fail")
	if err := next.commitCheckpoint(&checkpoint.Checkpoint{ID: "fail-cp-4", ThreadID: threadID, State: *next.State}, false); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx, threadID, "fail-cp-4")
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentID != "fail-cp-3" {
		t.Fatalf("续链父记录 = %q, want fail-cp-3", got.ParentID)
	}
}

// TestLineageConcurrentCommitsSerialize：第一次提交卡在 store 内部时，第二次提交
// 必须停在谱系互斥区之外，等到第一次成功推进 head 才能进 store，
// 因此它拿到的是前一条而不是同一个父。
func TestLineageConcurrentCommitsSerialize(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-barrier"
	store := newLineageStore(checkpoint.NewMemory())
	store.gateOn = 1

	rc := &RunContext{
		Context:  ctx,
		RunID:    "run-barrier",
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	rc.setLineageStart(nil, "turn-barrier")

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- rc.commitCheckpoint(&checkpoint.Checkpoint{ID: "bar-1", ThreadID: threadID, State: *rc.State}, false)
	}()
	<-store.entered

	secondDone := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		secondDone <- rc.commitCheckpoint(&checkpoint.Checkpoint{ID: "bar-2", ThreadID: threadID, State: *rc.State}, false)
	}()
	<-started

	// 此刻第一次提交仍卡在 store 内部，第二次只能在谱系互斥区外等。少了这把锁，
	// 它会在 head 还是空的时候组装父边，下面的父边断言就会拿到空父边。
	close(store.release)

	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}

	hist, err := store.History(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("历史条数 = %d, want 2", len(hist))
	}
	if hist[1].ParentID != "bar-1" || hist[1].ParentRef == nil || hist[1].ParentRef.CheckpointID != "bar-1" {
		t.Fatalf("并发提交各自认了旧 head: %+v %+v", hist[1].ParentRef, hist[1].ParentID)
	}
}

// TestLineageRetryKeepsIdentity：同一个提交对象重试复用身份与创建时间，
// 不会被 store 判成同 ID 异内容冲突。
func TestLineageRetryKeepsIdentity(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-retry"
	store := newLineageStore(checkpoint.NewMemory())
	rc := &RunContext{
		Context:  ctx,
		RunID:    "run-retry",
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	rc.setLineageStart(nil, "turn-retry")
	cp := &checkpoint.Checkpoint{ID: "retry-1", ThreadID: threadID, State: *rc.State}
	if err := rc.commitCheckpoint(cp, false); err != nil {
		t.Fatal(err)
	}
	before := *cp
	// 提交成功后 store 会写回 revision；重投同一份内容必须仍按幂等成功处理。
	if err := rc.commitCheckpoint(cp, false); err != nil {
		t.Fatalf("同内容重试被判失败: %v", err)
	}
	after, err := store.Load(ctx, threadID, "retry-1")
	if err != nil {
		t.Fatal(err)
	}
	if !after.CreatedAt.Equal(before.CreatedAt) || after.RunID != before.RunID || after.TurnID != before.TurnID {
		t.Fatalf("重试改变了身份: %+v vs %+v", after, before)
	}
	hist, err := store.History(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("重试追加了记录: %v", commitIDs(hist))
	}
}

// newLineageRunContext 造一个已经启动的运行环境，用于直接检验提交序列。
func newLineageRunContext(store checkpoint.Checkpointer, threadID, runID string) *RunContext {
	rc := &RunContext{
		Context:  context.Background(),
		RunID:    runID,
		ThreadID: threadID,
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	rc.setLineageStart(nil, "turn-"+runID)
	return rc
}

// TestLineageRetryRootAfterHeadAdvanced：重试一条父边本来就为空的根记录，不该被
// 当成"尚未组装谱系"而补上当前 head。
func TestLineageRetryRootAfterHeadAdvanced(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-retry-root"
	store := newLineageStore(checkpoint.NewMemory())
	rc := newLineageRunContext(store, threadID, "run-retry-root")

	created := make([]*checkpoint.Checkpoint, 0, 3)
	for _, id := range []string{"root-a", "step-b", "step-c"} {
		cp := &checkpoint.Checkpoint{ID: id, ThreadID: threadID, State: *rc.State}
		if err := rc.commitCheckpoint(cp, false); err != nil {
			t.Fatalf("提交 %s: %v", id, err)
		}
		created = append(created, cp)
	}

	if err := rc.commitCheckpoint(created[0], false); err != nil {
		t.Fatalf("重试已提交的根记录失败: %v", err)
	}
	root, err := store.Load(ctx, threadID, "root-a")
	if err != nil {
		t.Fatal(err)
	}
	if root.ParentID != "" || root.ParentRef != nil {
		t.Fatalf("重试改写了根记录的父边: %q %+v", root.ParentID, root.ParentRef)
	}

	next := &checkpoint.Checkpoint{ID: "step-d", ThreadID: threadID, State: *rc.State}
	if err := rc.commitCheckpoint(next, false); err != nil {
		t.Fatal(err)
	}
	if next.ParentID != "step-c" || next.ParentRef == nil || next.ParentRef.CheckpointID != "step-c" {
		t.Fatalf("重试把 head 带偏了: step-d 的父记录 = %q %+v, want step-c", next.ParentID, next.ParentRef)
	}
	hist, err := store.History(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 4 {
		t.Fatalf("历史条数 = %d, want 4: %v", len(hist), commitIDs(hist))
	}
}

// TestLineageRetryLoadedRootAfterHeadAdvanced proves retry identity is stable
// across Load/clone rather than tied to the address of the original Go object.
func TestLineageRetryLoadedRootAfterHeadAdvanced(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-retry-loaded-root"
	store := newLineageStore(checkpoint.NewMemory())
	rc := newLineageRunContext(store, threadID, "run-retry-loaded-root")

	for _, id := range []string{"root-a", "step-b", "step-c"} {
		cp := &checkpoint.Checkpoint{ID: id, ThreadID: threadID, State: *rc.State}
		if err := rc.commitCheckpoint(cp, false); err != nil {
			t.Fatalf("commit %s: %v", id, err)
		}
	}

	loadedRoot, err := store.Load(ctx, threadID, "root-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.commitCheckpoint(loadedRoot, false); err != nil {
		t.Fatalf("retry loaded root: %v", err)
	}
	if loadedRoot.ParentID != "" || loadedRoot.ParentRef != nil {
		t.Fatalf("retry rewrote loaded root parent: %q %+v", loadedRoot.ParentID, loadedRoot.ParentRef)
	}

	next := &checkpoint.Checkpoint{ID: "step-d", ThreadID: threadID, State: *rc.State}
	if err := rc.commitCheckpoint(next, false); err != nil {
		t.Fatal(err)
	}
	if next.ParentID != "step-c" {
		t.Fatalf("loaded retry moved head: step-d parent = %q, want step-c", next.ParentID)
	}
}

// TestLineageOlderRetryDoesNotMoveHeadBack：幂等确认一条旧提交不能把本地 head
// 拉回那条旧记录，否则之后的提交会插到历史中间去。
func TestLineageOlderRetryDoesNotMoveHeadBack(t *testing.T) {
	ctx := context.Background()
	threadID := "lineage-retry-older"
	store := newLineageStore(checkpoint.NewMemory())
	rc := newLineageRunContext(store, threadID, "run-retry-older")

	created := make([]*checkpoint.Checkpoint, 0, 3)
	for _, id := range []string{"old-a", "old-b", "old-c"} {
		cp := &checkpoint.Checkpoint{ID: id, ThreadID: threadID, State: *rc.State}
		if err := rc.commitCheckpoint(cp, false); err != nil {
			t.Fatalf("提交 %s: %v", id, err)
		}
		created = append(created, cp)
	}

	if err := rc.commitCheckpoint(created[1], false); err != nil {
		t.Fatalf("重试旧提交失败: %v", err)
	}
	next := &checkpoint.Checkpoint{ID: "old-d", ThreadID: threadID, State: *rc.State}
	if err := rc.commitCheckpoint(next, false); err != nil {
		t.Fatal(err)
	}
	if next.ParentID != "old-c" {
		t.Fatalf("本地 head 退回了旧提交: old-d 的父记录 = %q, want old-c", next.ParentID)
	}
	hist, err := store.History(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 4 {
		t.Fatalf("历史条数 = %d, want 4: %v", len(hist), commitIDs(hist))
	}
}

func commitIDs(hist []*checkpoint.Checkpoint) []string {
	out := make([]string, 0, len(hist))
	for _, cp := range hist {
		out = append(out, fmt.Sprintf("%s(parent=%q,run=%s,turn=%s)", cp.ID, cp.ParentID, cp.RunID, cp.TurnID))
	}
	return out
}
