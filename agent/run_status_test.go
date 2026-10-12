package agent_test

// BC-05（ADR-0033 D5，V2、V8 的查询部分）：RunStatus 的五种状态、错误出口、副本
// 边界，以及"查询不写"和"前台路径不追加记录"。

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// statusCountingStore 包装 Memory 的协调能力并统计每种调用的次数，用来证明查询只读。
type statusCountingStore struct {
	checkpoint.ThreadCoordinator

	mu        sync.Mutex
	counts    map[string]int
	latestErr error
}

func newStatusCountingStore() *statusCountingStore {
	return &statusCountingStore{
		ThreadCoordinator: checkpoint.NewMemory(),
		counts:            map[string]int{},
	}
}

func (s *statusCountingStore) tally(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[op]++
}

func (s *statusCountingStore) snapshotCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

func (s *statusCountingStore) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	s.tally("Save")
	return s.ThreadCoordinator.Save(ctx, cp)
}

func (s *statusCountingStore) Load(ctx context.Context, threadID, id string) (*checkpoint.Checkpoint, error) {
	s.tally("Load")
	return s.ThreadCoordinator.Load(ctx, threadID, id)
}

func (s *statusCountingStore) Latest(ctx context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	s.tally("Latest")
	if s.latestErr != nil {
		return nil, s.latestErr
	}
	return s.ThreadCoordinator.Latest(ctx, threadID)
}

func (s *statusCountingStore) History(ctx context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	s.tally("History")
	return s.ThreadCoordinator.History(ctx, threadID)
}

func (s *statusCountingStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.tally("Commit")
	return s.ThreadCoordinator.Commit(ctx, req)
}

func (s *statusCountingStore) BeginRun(ctx context.Context, req checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	s.tally("BeginRun")
	return s.ThreadCoordinator.BeginRun(ctx, req)
}

func (s *statusCountingStore) RenewClaim(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
	s.tally("RenewClaim")
	return s.ThreadCoordinator.RenewClaim(ctx, claim, lease)
}

func (s *statusCountingStore) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	s.tally("ReleaseClaim")
	return s.ThreadCoordinator.ReleaseClaim(ctx, claim)
}

// statusTerminalCount 数历史里带终态的记录条数。
func statusTerminalCount(t *testing.T, store checkpoint.Checkpointer, threadID string) int {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, cp := range hist {
		if cp.Completion != nil {
			n++
		}
	}
	return n
}

// statusPausedAgent 用审批闸门把运行停在暂停点。
func statusPausedAgent(t *testing.T, store checkpoint.Checkpointer, threadID string) (*agent.Agent, *agent.Run) {
	t.Helper()
	a := terminalAgentWith(t, store, gatedModel(), terminalGate{}, noopTool(), blockedTool())
	run := a.Stream(context.Background(), "go", agent.OnThread(threadID))
	if _, err := run.Wait(); err != nil {
		t.Fatalf("暂停是正常收尾: %v", err)
	}
	return a, run
}

// TestRunStatusStates：不存在、有步骤无终态、暂停、completed、failed 五种读法，
// 字段与 store 里的快照一致。暂停另走一遍真实恢复流程，不把 Paused 当恢复保证。
func TestRunStatusStates(t *testing.T) {
	ctx := context.Background()
	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()

			t.Run("不存在", func(t *testing.T) {
				a := recordAgent(t, b.store, "第一次")
				status, err := a.RunStatus(ctx, "status-missing-"+name)
				if err != nil {
					t.Fatal(err)
				}
				if status.Exists || status.Latest != nil || status.Completion != nil || status.Paused {
					t.Fatalf("未知线程读出了内容: %+v", status)
				}
				if status.ThreadID != "status-missing-"+name || status.Revision != 0 {
					t.Fatalf("基础字段不符: %+v", status)
				}
			})

			t.Run("有步骤无终态", func(t *testing.T) {
				threadID := "status-run-" + name
				a := recordAgent(t, b.store, "前台跑完")
				run := a.Stream(ctx, "go", agent.OnThread(threadID))
				if _, err := run.Wait(); err != nil {
					t.Fatal(err)
				}

				status, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				head := headCheckpoint(t, b.reopen(), threadID)
				if !status.Exists || status.Completion != nil || status.Paused {
					t.Fatalf("无终态的 head 读错: %+v", status)
				}
				if status.Revision != head.Revision || status.Step != head.Step || status.Latest.ID != head.ID {
					t.Fatalf("字段与 head 不一致: status=%+v head=%s/%d", status, head.ID, head.Revision)
				}
			})

			t.Run("暂停后恢复", func(t *testing.T) {
				threadID := "status-pause-" + name
				a, run := statusPausedAgent(t, b.store, threadID)
				if !run.Paused() {
					t.Fatal("夹具没有停在暂停点")
				}

				status, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				if !status.Exists || !status.Paused || status.Completion != nil {
					t.Fatalf("暂停读错: %+v", status)
				}
				if status.Latest.Pending == nil && status.Latest.Pause == nil {
					t.Fatal("Paused 为真但快照没有暂停元数据")
				}

				// Paused 只是元数据，不是"必然恢复成功"的承诺：批准后才继续。
				resumed, err := a.Resume(ctx, threadID, agent.Allow("b1"))
				if err != nil {
					t.Fatalf("Resume: %v", err)
				}
				res, err := resumed.Wait()
				if err != nil {
					t.Fatalf("恢复后的运行失败: %v", err)
				}
				if got := res.Message.Text(); got != "审批之后完成" {
					t.Fatalf("恢复结果 = %q", got)
				}
				after, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				if after.Paused {
					t.Fatalf("恢复后仍报暂停: %+v", after)
				}
				if after.Completion != nil {
					t.Fatalf("恢复路径不该有终态记录: %+v", after.Completion)
				}
			})

			t.Run("两种暂停元数据", func(t *testing.T) {
				// 真实中断会同时写下 Pause 与 Pending，这里直接各写一份只含其中一项的
				// 快照，确认两种元数据都读成暂停。
				shapes := []struct {
					what    string
					thread  string
					pending *checkpoint.PendingHITL
					pause   *checkpoint.Pause
				}{
					{"只有待办工具", "status-pending-" + name,
						&checkpoint.PendingHITL{Step: 1, Pending: []core.ToolCall{{ID: "b1", Name: "blocked", Args: json.RawMessage(`{}`)}}}, nil},
					{"只有暂停描述", "status-pauseonly-" + name, nil,
						&checkpoint.Pause{ID: "p1", Phase: "before_model", Reason: "审批", Recovery: checkpoint.RecoveryReplayModel}},
				}
				for _, shape := range shapes {
					if err := b.store.Save(ctx, &checkpoint.Checkpoint{
						ID:       core.NewID("cp"),
						ThreadID: shape.thread,
						Step:     1,
						State:    core.State{},
						Pending:  shape.pending,
						Pause:    shape.pause,
					}); err != nil {
						t.Fatalf("写入%s的快照: %v", shape.what, err)
					}
					a := recordAgent(t, b.store, "无关")
					status, err := a.RunStatus(ctx, shape.thread)
					if err != nil {
						t.Fatal(err)
					}
					if !status.Exists || !status.Paused || status.Completion != nil {
						t.Fatalf("%s 应读成暂停: %+v", shape.what, status)
					}
				}
			})

			t.Run("completed", func(t *testing.T) {
				threadID := "status-done-" + name
				a := recordAgent(t, b.store, "完成并记录")
				run := a.Stream(ctx, "go", agent.OnThread(threadID))
				if _, err := run.Wait(); err != nil {
					t.Fatal(err)
				}
				if err := a.RecordCompletion(ctx, run); err != nil {
					t.Fatal(err)
				}

				status, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				if !status.Exists || status.Paused {
					t.Fatalf("completed 读错: %+v", status)
				}
				if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionCompleted {
					t.Fatalf("终态不符: %+v", status.Completion)
				}
				if status.Completion.RunID != run.ID {
					t.Fatalf("终态归属错: %+v", status.Completion)
				}
				if status.Completion.Result == nil || status.Completion.Result.Message.Text() != "完成并记录" {
					t.Fatalf("终态结果不符: %+v", status.Completion.Result)
				}
			})

			t.Run("failed", func(t *testing.T) {
				threadID := "status-failed-" + name
				a := terminalAgent(t, b.store, errorModel{})
				run := a.Stream(ctx, "go", agent.OnThread(threadID))
				if _, err := run.Wait(); err == nil {
					t.Fatal("应报错")
				}
				if err := a.RecordCompletion(ctx, run); err != nil {
					t.Fatalf("失败运行应可记录: %v", err)
				}

				status, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionFailed {
					t.Fatalf("失败终态不符: %+v", status.Completion)
				}
				if status.Completion.Err == "" || status.Completion.Result != nil {
					t.Fatalf("失败终态字段不符: %+v", status.Completion)
				}
				if status.Paused {
					t.Fatal("失败运行不该报暂停")
				}
			})
		})
	}
}

// TestRunStatusErrorsAndCopies：非法线程 ID 在触达 store 之前被拒；store 读取错误原样
// 返回；出口副本被改写不污染历史；查询不写、不提交、不取 claim、不续租。
func TestRunStatusErrorsAndCopies(t *testing.T) {
	ctx := context.Background()

	t.Run("非法线程ID", func(t *testing.T) {
		store := newStatusCountingStore()
		a := recordAgent(t, store, "不应触发读取")
		before := store.snapshotCounts()

		for _, bad := range []string{"", "带中文的ID", "bad id", "a/b"} {
			if _, err := a.RunStatus(ctx, bad); err == nil {
				t.Fatalf("非法 ID %q 被接受", bad)
			} else if !strings.Contains(err.Error(), "core:") {
				t.Fatalf("非法 ID %q 的错误不是校验错误: %v", bad, err)
			}
		}
		after := store.snapshotCounts()
		for op, n := range after {
			if n != before[op] {
				t.Fatalf("非法 ID 触达了 store：%s 从 %d 增至 %d", op, before[op], n)
			}
		}
	})

	t.Run("store读取错误原样返回", func(t *testing.T) {
		injected := errors.New("存储不可用")
		store := newStatusCountingStore()
		store.latestErr = injected
		a := recordAgent(t, store, "无关")

		status, err := a.RunStatus(ctx, "status-store-err")
		if !errors.Is(err, injected) {
			t.Fatalf("错误 = %v，期望原样返回 %v", err, injected)
		}
		if status.Exists || status.Latest != nil {
			t.Fatalf("读取失败却填了状态: %+v", status)
		}
	})

	t.Run("副本不污染历史", func(t *testing.T) {
		for name, newBackends := range newTerminalBackends(t) {
			t.Run(name, func(t *testing.T) {
				b := newBackends()
				threadID := "status-copy-" + name
				store := b.store
				a := terminalAgent(t, store, terminalModel("原始终态", []byte("payload")))
				run := a.Stream(ctx, "go", agent.OnThread(threadID))
				if _, err := run.Wait(); err != nil {
					t.Fatal(err)
				}
				if err := a.RecordCompletion(ctx, run); err != nil {
					t.Fatal(err)
				}

				status, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				// 就地改写出口给出的副本：KV、终态结果里的可变字节。
				if status.Latest.State.KV == nil {
					status.Latest.State.KV = map[string]any{}
				}
				status.Latest.State.KV["injected"] = "from caller"
				status.Latest.Completion.Result.Message.Parts[0] = core.Text{Text: "被改写"}
				if data := status.Latest.Completion.Result.Message.Parts[1].(core.Image).Data; len(data) > 0 {
					data[0] = 'X'
				}

				reread, err := a.RunStatus(ctx, threadID)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := reread.Latest.State.KV["injected"]; ok {
					t.Fatal("对出口副本的 KV 改写进了历史")
				}
				if got := reread.Completion.Result.Message.Text(); got != "原始终态" {
					t.Fatalf("对出口副本的改写污染了终态结果: %q", got)
				}
				head := headCheckpoint(t, b.reopen(), threadID)
				if got := head.Completion.Result.Message.Text(); got != "原始终态" {
					t.Fatalf("重开 store 后终态被污染: %q", got)
				}
			})
		}
	})

	t.Run("查询不写不占claim", func(t *testing.T) {
		store := newStatusCountingStore()
		a := recordAgent(t, store, "查了三次")
		threadID := "status-readonly"
		run := a.Stream(ctx, "go", agent.OnThread(threadID))
		if _, err := run.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := a.RecordCompletion(ctx, run); err != nil {
			t.Fatal(err)
		}

		historyBefore := historyCount(t, store, threadID)
		headBefore := headCheckpoint(t, store, threadID)
		before := store.snapshotCounts()

		for i := 0; i < 3; i++ {
			if _, err := a.RunStatus(ctx, threadID); err != nil {
				t.Fatal(err)
			}
		}

		after := store.snapshotCounts()
		for _, op := range []string{"Save", "Commit", "BeginRun", "RenewClaim", "ReleaseClaim"} {
			if after[op] != before[op] {
				t.Fatalf("查询触发了 %s：%d -> %d", op, before[op], after[op])
			}
		}
		if after["Latest"] < before["Latest"]+3 {
			t.Fatalf("查询没有读 Latest：%d -> %d", before["Latest"], after["Latest"])
		}
		if got := historyCount(t, store, threadID); got != historyBefore {
			t.Fatalf("查询追加了历史：%d -> %d", historyBefore, got)
		}
		headAfter := headCheckpoint(t, store, threadID)
		if headAfter.ID != headBefore.ID || headAfter.Revision != headBefore.Revision {
			t.Fatalf("查询移动了 head：%s/%d -> %s/%d", headBefore.ID, headBefore.Revision, headAfter.ID, headAfter.Revision)
		}
	})
}

// TestRunStatusOldCompletionIsSnapshot：新运行已经开始但还没提交任何快照时，查询
// 仍显示上一次的终态——它只是当时的 head，不能推断"没有在跑"。
func TestRunStatusOldCompletionIsSnapshot(t *testing.T) {
	ctx := context.Background()
	const threadID = "status-snapshot"

	store := checkpoint.NewMemory()
	first := recordAgent(t, store, "上一轮的结果")
	runA := first.Stream(ctx, "go", agent.OnThread(threadID))
	if _, err := runA.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := first.RecordCompletion(ctx, runA); err != nil {
		t.Fatal(err)
	}

	// 第二轮：模型卡住不返回，因此这一轮还没有任何提交。
	entered := make(chan struct{})
	release := make(chan struct{})
	second, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(mock.New("slow", func(*llm.Request) *llm.Response {
			close(entered)
			<-release
			return mock.Text("新一轮的结果")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	runB := second.Stream(ctx, "again", agent.OnThread(threadID))
	runBDone := make(chan struct{})
	go func() {
		defer close(runBDone)
		_, _ = runB.Wait()
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("第二轮的模型没有开始")
	}

	status, err := second.RunStatus(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Completion == nil || status.Completion.RunID != runA.ID {
		t.Fatalf("新一轮进行中却读不到上一轮终态: %+v", status.Completion)
	}
	if status.Paused {
		t.Fatalf("带终态的 head 不该报暂停: %+v", status)
	}
	revisionSeen := status.Revision

	close(release)
	<-runBDone
	after, err := second.RunStatus(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision <= revisionSeen {
		t.Fatalf("第二轮提交后 revision 没前进：%d -> %d", revisionSeen, after.Revision)
	}
	// 新一轮的 head 没有终态记录：上一条断言只说明"当时读到的是那个快照"。
	if after.Completion != nil {
		t.Fatalf("新一轮自己的 head 不该带上轮的终态: %+v", after.Completion)
	}
}

// TestNonDetachedHistoryCountUnchanged：不调用 RecordCompletion 的前台路径，历史条数
// 与既有基线一致，且不出现任何终态记录。
func TestNonDetachedHistoryCountUnchanged(t *testing.T) {
	ctx := context.Background()

	perRun := func(t *testing.T, store checkpoint.Checkpointer, threadID string) int {
		t.Helper()
		a := recordAgent(t, store, "前台结果")
		run := a.Stream(ctx, "go", agent.OnThread(threadID))
		if _, err := run.Wait(); err != nil {
			t.Fatal(err)
		}
		return historyCount(t, store, threadID)
	}

	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()
			threadID := "status-baseline-" + name

			first := perRun(t, b.store, threadID)
			if got := statusTerminalCount(t, b.store, threadID); got != 0 {
				t.Fatalf("前台路径写了 %d 条终态记录", got)
			}
			second := perRun(t, b.store, threadID)
			if second-first != first {
				t.Fatalf("第二次前台运行的历史增量变了：%d -> %d（每轮 %d）", first, second, first)
			}
			if got := statusTerminalCount(t, b.store, threadID); got != 0 {
				t.Fatalf("第二次前台运行后出现 %d 条终态记录", got)
			}
		})
	}
}
