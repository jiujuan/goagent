package agent_test

// SM-04（ADR-0034 D3）：Session 门面与原子 Fork。本文件只覆盖门面的边界与 Fork
// 的一次性写入；轻量摘要分页（Session.History）按计划留到 SM-11。

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// --- 夹具 -------------------------------------------------------------------

// sessionStore 让两次提交先进入 store 再一起放行，用来检验并发 Fork 只有一个成功。
type sessionStore struct {
	checkpoint.ThreadCoordinator
	entered chan struct{}
	release chan struct{}
}

func newSessionGate(base checkpoint.ThreadCoordinator) *sessionStore {
	s := &sessionStore{
		ThreadCoordinator: base,
		entered:           make(chan struct{}, 2),
		release:           make(chan struct{}),
	}
	return s
}

func (s *sessionStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return s.ThreadCoordinator.Commit(ctx, req)
}

// --- 打开与读取 -------------------------------------------------------------

func TestSessionOpenNoWrite(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"memory", "file"} {
		t.Run(name, func(t *testing.T) {
			dir := ""
			var base checkpoint.Checkpointer
			if name == "memory" {
				base = checkpoint.NewMemory()
			} else {
				dir = t.TempDir()
				f, err := checkpoint.NewFile(dir)
				if err != nil {
					t.Fatal(err)
				}
				base = f
			}
			a := terminalAgent(t, base, resultTextModel("m", "回答"))
			threadID := "session-open-" + name

			session, err := a.OpenSession(ctx, threadID)
			if err != nil {
				t.Fatalf("打开合法线程失败: %v", err)
			}
			if session.ThreadID() != threadID {
				t.Fatalf("绑定线程 = %q", session.ThreadID())
			}
			if count := historyCount(t, base, threadID); count != 0 {
				t.Fatalf("OpenSession 写了 %d 条记录", count)
			}
			head, err := session.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if head != nil {
				t.Fatalf("没有快照的线程返回了 head: %+v", head)
			}
			if name == "file" {
				if _, err := os.Stat(filepath.Join(dir, threadID+".jsonl")); !os.IsNotExist(err) {
					t.Fatalf("OpenSession 创建了日志文件: %v", err)
				}
			}

			// 非法线程标识：拒绝且不写。
			if _, err := a.OpenSession(ctx, "bad/id"); err == nil {
				t.Fatal("非法线程标识应被拒绝")
			}

			// 有快照之后再读，取到的是真实最新记录。
			if _, err := session.Send(ctx, "问题"); err != nil {
				t.Fatal(err)
			}
			after, err := session.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if after == nil || after.ThreadID != threadID {
				t.Fatalf("Send 之后 head 不符: %+v", after)
			}
			if count := historyCount(t, base, threadID); count != 1 {
				t.Fatalf("一次 Send 提交了 %d 条, want 1", count)
			}
		})
	}
}

// --- 发送绑定 ----------------------------------------------------------------

func TestSessionSendThreadBound(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	session, err := a.OpenSession(ctx, "session-bound")
	if err != nil {
		t.Fatal(err)
	}

	res, err := session.Send(ctx, "跑到别处", agent.OnThread("other-thread"))
	if err == nil {
		t.Fatal("冲突的 OnThread 应被拒绝")
	}
	if res.ThreadID != "session-bound" {
		t.Fatalf("拒绝后的结果丢了已知身份: %+v", res)
	}
	if res.RunID != "" || res.CheckpointRef != nil || res.Revision != 0 {
		t.Fatalf("没有运行却有运行身份或边界: %+v", res)
	}
	if count := historyCount(t, base, "other-thread"); count != 0 {
		t.Fatalf("被拒绝的发送在别的线程写了 %d 条", count)
	}
	if count := historyCount(t, base, "session-bound"); count != 0 {
		t.Fatalf("被拒绝的发送在绑定线程写了 %d 条", count)
	}

	// 指名同一线程和不指名都允许，落在绑定线程上。
	for _, opts := range [][]agent.RunOption{nil, {agent.OnThread("session-bound")}} {
		res, err := session.Send(ctx, "正常提问", opts...)
		if err != nil {
			t.Fatal(err)
		}
		if res.ThreadID != "session-bound" || res.CheckpointRef == nil || res.CheckpointRef.ThreadID != "session-bound" {
			t.Fatalf("Send 结果没有留在绑定线程: %+v", res)
		}
	}
}

// TestSessionSendReadsHeadAtStart：同一个 Session 连续两次发送，第二次的谱系续在
// 第一次的最后一条之后，证明 head 是发送时解析而不是打开时缓存。
func TestSessionSendReadsHeadAtStart(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultToolModel("m", 1), resultEchoTool())
	session, err := a.OpenSession(ctx, "session-head-at-start")
	if err != nil {
		t.Fatal(err)
	}
	first, err := session.Send(ctx, "带工具的问题", agent.OnThread("session-head-at-start"))
	if err != nil {
		t.Fatal(err)
	}
	// 别的运行在中间推进同一线程。
	other, err := a.RunDetailed(ctx, "插队", agent.OnThread("session-head-at-start"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.Send(ctx, "再来一次")
	if err != nil {
		t.Fatal(err)
	}
	if second.CheckpointRef == nil {
		t.Fatalf("第二次发送没有边界: %+v", second)
	}
	hist, err := base.History(ctx, "session-head-at-start")
	if err != nil {
		t.Fatal(err)
	}
	parent := findCheckpoint(t, hist, second.CheckpointRef.CheckpointID)
	if parent.ParentID != other.CheckpointRef.CheckpointID {
		t.Fatalf("第二次发送的父记录 = %q, want 中间那次运行的最后一条 %q", parent.ParentID, other.CheckpointRef.CheckpointID)
	}
	if first.CheckpointRef.CheckpointID == second.CheckpointRef.CheckpointID {
		t.Fatal("两次发送报同一个边界")
	}
	if first.TurnID == second.TurnID {
		t.Fatal("两次发送共用 TurnID")
	}
}

func findCheckpoint(t *testing.T, hist []*checkpoint.Checkpoint, id string) *checkpoint.Checkpoint {
	t.Helper()
	for _, cp := range hist {
		if cp.ID == id {
			return cp
		}
	}
	t.Fatalf("历史里找不到 %s", id)
	return nil
}

// --- Fork -------------------------------------------------------------------

func TestSessionForkLineage(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"memory", "file"} {
		t.Run(name, func(t *testing.T) {
			var base checkpoint.Checkpointer
			if name == "memory" {
				base = checkpoint.NewMemory()
			} else {
				f, err := checkpoint.NewFile(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				base = f
			}
			var modelCalls atomic.Int32
			a := terminalAgent(t, base, countingTextModel("m", "分支前回答", &modelCalls))
			sourceThread := "fork-source-" + name
			session, err := a.OpenSession(ctx, sourceThread)
			if err != nil {
				t.Fatal(err)
			}
			res, err := session.Send(ctx, "问题")
			if err != nil {
				t.Fatal(err)
			}

			callsBefore := modelCalls.Load()
			branch, err := session.ForkFrom(ctx, res.CheckpointRef.CheckpointID, "fork-branch-"+name)
			if err != nil {
				t.Fatal(err)
			}
			if modelCalls.Load() != callsBefore {
				t.Fatal("Fork 调用了模型")
			}
			if branch.ThreadID() != "fork-branch-"+name {
				t.Fatalf("分支线程 = %q", branch.ThreadID())
			}
			head, err := branch.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if head == nil {
				t.Fatal("分支没有首条记录")
			}
			if head.ParentID != res.CheckpointRef.CheckpointID {
				t.Fatalf("分支父记录 = %q, want %q", head.ParentID, res.CheckpointRef.CheckpointID)
			}
			if head.ParentRef == nil || head.ParentRef.CheckpointID != res.CheckpointRef.CheckpointID || head.ParentRef.ThreadID != sourceThread {
				t.Fatalf("分支的跨线程归属不符: %+v", head.ParentRef)
			}
			if head.RunID != "" || head.TurnID != "" {
				t.Fatalf("分支继承了来源的运行身份: run=%q turn=%q", head.RunID, head.TurnID)
			}
			if head.Completion != nil {
				t.Fatalf("分支继承了终态: %+v", head.Completion)
			}
			// 分支自己的运行开新的身份，谱系继续接在那条首记录之后。
			branchRes, err := branch.Send(ctx, "分支上的提问")
			if err != nil {
				t.Fatal(err)
			}
			if branchRes.RunID == res.RunID {
				t.Fatal("分支运行复用了旧 RunID")
			}
			if branchRes.TurnID == "" || branchRes.TurnID == res.TurnID {
				t.Fatalf("分支开新回合不符: %q vs %q", branchRes.TurnID, res.TurnID)
			}
			stored := findCheckpoint(t, historyOf(t, base, branch.ThreadID()), branchRes.CheckpointRef.CheckpointID)
			if stored.ParentID != head.ID {
				t.Fatalf("分支第二次提交的父记录 = %q, want 分支首条 %q", stored.ParentID, head.ID)
			}
		})
	}
}

func countingTextModel(name, text string, calls *atomic.Int32) llm.Model {
	return mock.New(name, func(*llm.Request) *llm.Response {
		calls.Add(1)
		return mock.Text(text)
	})
}

func historyOf(t *testing.T, store checkpoint.Checkpointer, threadID string) []*checkpoint.Checkpoint {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return hist
}

func TestSessionForkFiles(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	threadID := "fork-files"
	source := &checkpoint.Checkpoint{
		ID:           "fork-files-source",
		ThreadID:     threadID,
		Step:         1,
		State:        core.State{Messages: []core.Message{core.UserText("问题")}},
		FileSnapshot: map[string][]byte{"notes.md": []byte("内容")},
	}
	if err := base.Save(ctx, source); err != nil {
		t.Fatal(err)
	}
	empty := &checkpoint.Checkpoint{
		ID:           "fork-files-empty",
		ThreadID:     threadID,
		Step:         2,
		ParentID:     source.ID,
		State:        core.State{Messages: []core.Message{core.UserText("问题")}},
		FileSnapshot: map[string][]byte{},
	}
	if err := base.Save(ctx, empty); err != nil {
		t.Fatal(err)
	}
	external := &checkpoint.Checkpoint{
		ID:           "fork-files-external",
		ThreadID:     threadID,
		Step:         3,
		State:        core.State{Messages: []core.Message{core.UserText("问题")}},
		FileSnapshot: nil,
	}
	if err := base.Save(ctx, external); err != nil {
		t.Fatal(err)
	}

	session, err := a.OpenSession(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		id      string
		wantNil bool
		wantLen int
	}{
		{"快照带文件", source.ID, false, 1},
		{"显式空快照", empty.ID, false, 0},
		{"外部托管没有快照", external.ID, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			branchThread := "fork-files-branch-" + tc.id
			branch, err := session.ForkFrom(ctx, tc.id, branchThread)
			if err != nil {
				t.Fatal(err)
			}
			head, err := branch.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNil {
				if head.FileSnapshot != nil {
					t.Fatalf("外部托管的线程被造出了快照: %v", head.FileSnapshot)
				}
				return
			}
			if head.FileSnapshot == nil {
				t.Fatal("非 nil 的空文件快照在 Fork 后变成了 nil")
			}
			if len(head.FileSnapshot) != tc.wantLen {
				t.Fatalf("文件条目数 = %d, want %d", len(head.FileSnapshot), tc.wantLen)
			}
			if tc.wantLen == 1 && string(head.FileSnapshot["notes.md"]) != "内容" {
				t.Fatalf("文件内容没带过来: %v", head.FileSnapshot)
			}
		})
	}
}

func TestSessionForkTargetConflict(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	session, err := a.OpenSession(ctx, "fork-conflict-source")
	if err != nil {
		t.Fatal(err)
	}
	res, err := session.Send(ctx, "问题")
	if err != nil {
		t.Fatal(err)
	}

	taken := "fork-conflict-target"
	if _, err := a.RunDetailed(ctx, "目标线程已有历史", agent.OnThread(taken)); err != nil {
		t.Fatal(err)
	}
	before := historyOf(t, base, taken)

	branch, err := session.ForkFrom(ctx, res.CheckpointRef.CheckpointID, taken)
	if err == nil {
		t.Fatal("Fork 到非空线程应失败")
	}
	if branch != nil {
		t.Fatal("失败的 Fork 返回了门面")
	}
	if !errors.Is(err, checkpoint.ErrRevisionConflict) {
		t.Fatalf("失败原因 = %v, want ErrRevisionConflict", err)
	}
	after := historyOf(t, base, taken)
	if len(after) != len(before) {
		t.Fatalf("失败的 Fork 追加了记录: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if after[i].ID != before[i].ID {
			t.Fatalf("失败的 Fork 改写了目标线程的第 %d 条记录", i)
		}
	}
}

func TestSessionForkOwnership(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	session, err := a.OpenSession(ctx, "fork-owner-source")
	if err != nil {
		t.Fatal(err)
	}
	res, err := session.Send(ctx, "问题")
	if err != nil {
		t.Fatal(err)
	}

	occupied := "fork-owner-target"
	if _, err := a.RunDetailed(ctx, "先建首条", agent.OnThread(occupied)); err != nil {
		t.Fatal(err)
	}
	claimed, err := base.BeginRun(ctx, checkpoint.BeginRunRequest{
		ThreadID: occupied,
		Owner:    "另一个运行",
		Lease:    checkpoint.DefaultLease,
	})
	if err != nil {
		t.Fatal(err)
	}

	branch, err := session.ForkFrom(ctx, res.CheckpointRef.CheckpointID, "fork-owner-target-claimed")
	if err != nil {
		t.Fatalf("空闲线程的 Fork 失败: %v", err)
	}
	if branch == nil {
		t.Fatal("成功的 Fork 没有返回门面")
	}

	// 已有 owner 的线程上，Fork 不能绕过归属；目标非空也会一并拒绝。
	if _, err := session.ForkFrom(ctx, res.CheckpointRef.CheckpointID, occupied); err == nil {
		t.Fatal("被占用的线程允许 Fork")
	}

	// 空线程但被 claim 占住时，Fork 必须撞上归属检查。
	fresh := "fork-owner-empty-claimed"
	emptyClaim, err := base.BeginRun(ctx, checkpoint.BeginRunRequest{
		ThreadID: fresh,
		Owner:    "占住空线程",
		Lease:    checkpoint.DefaultLease,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.ForkFrom(ctx, res.CheckpointRef.CheckpointID, fresh); !errors.Is(err, checkpoint.ErrClaimRequired) {
		t.Fatalf("空线程被占用时 Fork 的错误 = %v, want ErrClaimRequired", err)
	}
	if count := historyCount(t, base, fresh); count != 0 {
		t.Fatalf("被拒绝的 Fork 写了 %d 条", count)
	}
	if err := base.ReleaseClaim(ctx, claimed.Claim); err != nil {
		t.Fatal(err)
	}
	if err := base.ReleaseClaim(ctx, emptyClaim.Claim); err != nil {
		t.Fatal(err)
	}
}

func TestSessionForkSourceValidation(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	session, err := a.OpenSession(ctx, "fork-src-mine")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Send(ctx, "问题"); err != nil {
		t.Fatal(err)
	}
	other, err := a.RunDetailed(ctx, "别人的历史", agent.OnThread("fork-src-other"))
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"别的线程上的记录": other.CheckpointRef.CheckpointID,
		"不存在的记录":   "no-such-checkpoint",
		"空 ID":     "",
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			branch, err := session.ForkFrom(ctx, id, "fork-src-branch")
			if err == nil {
				t.Fatal("来源校验没拦住")
			}
			if branch != nil {
				t.Fatal("失败的 Fork 返回了门面")
			}
			if count := historyCount(t, base, "fork-src-branch"); count != 0 {
				t.Fatalf("失败的 Fork 在新线程写了 %d 条", count)
			}
		})
	}
}

func TestSessionForkRequiresDurable(t *testing.T) {
	ctx := context.Background()
	base := newPlainCheckpointer()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	session, err := a.OpenSession(ctx, "fork-plain")
	if err != nil {
		t.Fatal(err)
	}
	res, err := session.Send(ctx, "问题")
	if err != nil {
		t.Fatalf("基础 store 上 Send 应可用: %v", err)
	}
	head, err := session.GetHead(ctx)
	if err != nil || head == nil {
		t.Fatalf("基础 store 上 GetHead 应可用: head=%+v err=%v", head, err)
	}
	if res.CheckpointRef != nil || res.Revision != 0 {
		t.Fatalf("没有回执的 store 不该报出边界: %+v rev=%d", res.CheckpointRef, res.Revision)
	}
	if res.RunID == "" || res.TurnID == "" || res.Result.Message.Text() != "回答" {
		t.Fatalf("基础 store 上结果身份不符: %+v", res)
	}
	branch, err := session.ForkFrom(ctx, head.ID, "fork-plain-branch")
	if !errors.Is(err, checkpoint.ErrDurableUnavailable) {
		t.Fatalf("基础 store 的 ForkFrom 错误 = %v, want ErrDurableUnavailable", err)
	}
	if branch != nil {
		t.Fatal("没有能力却返回了门面")
	}
	if count := historyCount(t, base, "fork-plain-branch"); count != 0 {
		t.Fatalf("不支持的 Fork 写了 %d 条", count)
	}
}

// TestSessionForkConcurrent：两个 Session 同时 Fork 到同一个目标线程，闸门保证两次
// 提交都进到 store 之后再放行，最终只有一个成功且目标线程只有一条记录。
func TestSessionForkConcurrent(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	a := terminalAgent(t, base, resultTextModel("m", "回答"))
	source, err := a.OpenSession(ctx, "fork-race-source")
	if err != nil {
		t.Fatal(err)
	}
	res, err := source.Send(ctx, "问题")
	if err != nil {
		t.Fatal(err)
	}

	gated := newSessionGate(base)
	racer := terminalAgent(t, gated, resultTextModel("m", "回答"))
	left, err := racer.OpenSession(ctx, "fork-race-source")
	if err != nil {
		t.Fatal(err)
	}
	right, err := racer.OpenSession(ctx, "fork-race-source")
	if err != nil {
		t.Fatal(err)
	}

	target := "fork-race-target"
	type outcome struct {
		branch *agent.Session
		err    error
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, session := range []*agent.Session{left, right} {
		wg.Add(1)
		go func(s *agent.Session) {
			defer wg.Done()
			branch, err := s.ForkFrom(ctx, res.CheckpointRef.CheckpointID, target)
			results <- outcome{branch: branch, err: err}
		}(session)
	}
	<-gated.entered
	<-gated.entered
	close(gated.release)
	wg.Wait()
	close(results)

	var succeeded int
	for got := range results {
		if got.err != nil {
			if got.branch != nil {
				t.Fatal("失败的 Fork 返回了门面")
			}
			if !errors.Is(got.err, checkpoint.ErrRevisionConflict) {
				t.Fatalf("失败方错误 = %v, want ErrRevisionConflict", got.err)
			}
			continue
		}
		succeeded++
		if got.branch == nil {
			t.Fatal("成功的 Fork 没有门面")
		}
	}
	if succeeded != 1 {
		t.Fatalf("并发 Fork 成功次数 = %d, want 1", succeeded)
	}
	hist := historyOf(t, base, target)
	if len(hist) != 1 {
		t.Fatalf("目标线程记录数 = %d, want 1", len(hist))
	}
	if hist[0].ParentRef == nil || hist[0].ParentRef.ThreadID != "fork-race-source" {
		t.Fatalf("目标首条的归属不符: %+v", hist[0].ParentRef)
	}
}
