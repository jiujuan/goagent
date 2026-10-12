package agent_test

// BC-03（ADR-0033 D4）：Run 的结束凭据与 RecordCompletion。
// 本文件覆盖 V4（身份、边界、存储能力）与冻结顺序；并发与文件保留在
// terminal_cas_test.go、terminal_files_test.go。

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// --- 夹具 -------------------------------------------------------------------

// terminalStore 包装内层 Durable：记录每次 Commit 的请求，可注入失败与闸门。
type terminalStore struct {
	checkpoint.ThreadCoordinator
	mu      sync.Mutex
	commits []checkpoint.CommitRequest

	// beforeCommit 在委托内层之前调用；返回错误则这次提交没有发生。
	beforeCommit func(checkpoint.CommitRequest) error
	// afterCommit 可以推翻内层的成功，用来模拟"已落盘但回执丢失"。
	afterCommit func(checkpoint.CommitRequest, checkpoint.CommitReceipt, error) (checkpoint.CommitReceipt, error)
	// gate 命中的提交会卡在 entered/release 上。
	gate    func(checkpoint.CommitRequest) bool
	entered chan struct{}
	release chan struct{}
}

func (s *terminalStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	if s.beforeCommit != nil {
		if err := s.beforeCommit(req); err != nil {
			return checkpoint.CommitReceipt{}, err
		}
	}
	if s.gate != nil && s.gate(req) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return checkpoint.CommitReceipt{}, ctx.Err()
		}
	}
	s.mu.Lock()
	s.commits = append(s.commits, req)
	s.mu.Unlock()

	receipt, err := s.ThreadCoordinator.Commit(ctx, req)
	if s.afterCommit != nil {
		return s.afterCommit(req, receipt, err)
	}
	return receipt, err
}

// terminalCommits 返回携带终态记录的那几次 Commit 请求。
func (s *terminalStore) terminalCommits() []checkpoint.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []checkpoint.CommitRequest
	for _, req := range s.commits {
		if req.Checkpoint != nil && req.Checkpoint.Completion != nil {
			out = append(out, req)
		}
	}
	return out
}

// refuseBeginStore 让 prepare 阶段的 BeginRun 失败，用来造"本次没有提交"的运行。
type refuseBeginStore struct {
	checkpoint.ThreadCoordinator
}

func (s *refuseBeginStore) BeginRun(context.Context, checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	return checkpoint.BeginRunResult{}, errors.New("协调器不可用")
}

// plainCheckpointer 只实现 Checkpointer：没有原子的 revision 校验提交能力。
type plainCheckpointer struct {
	mu       sync.Mutex
	saved    int
	byThread map[string][]*checkpoint.Checkpoint
}

func newPlainCheckpointer() *plainCheckpointer {
	return &plainCheckpointer{byThread: map[string][]*checkpoint.Checkpoint{}}
}

func (p *plainCheckpointer) Save(_ context.Context, cp *checkpoint.Checkpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saved++
	if cp == nil {
		return errors.New("plain: nil checkpoint")
	}
	p.byThread[cp.ThreadID] = append(p.byThread[cp.ThreadID], cp)
	return nil
}

func (p *plainCheckpointer) Load(_ context.Context, threadID, id string) (*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cp := range p.byThread[threadID] {
		if cp.ID == id {
			return cp, nil
		}
	}
	return nil, errors.New("plain: not found")
}

func (p *plainCheckpointer) Latest(_ context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hist := p.byThread[threadID]
	if len(hist) == 0 {
		return nil, nil
	}
	return hist[len(hist)-1], nil
}

func (p *plainCheckpointer) History(_ context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*checkpoint.Checkpoint(nil), p.byThread[threadID]...), nil
}

func (p *plainCheckpointer) saveCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saved
}

// terminalModel 先调用一次工具（保证有中间提交边界），再给出最终回答。回答里带一段
// 可变字节（图片数据），用来验证"Wait 返回后被改写也影响不到冻结的终态"。
func terminalModel(finalText string, data []byte) llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return &llm.Response{
				Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
					core.Text{Text: finalText},
					core.Image{MIME: "image/png", Data: data},
				}},
				StopReason: llm.StopEnd,
			}
		}
		return mock.CallTool("t1", "noop", `{"value":"x"}`)
	})
}

func noopTool() tool.Tool {
	return tool.New("noop", "返回固定文本", func(_ *tool.Context, a struct {
		Value string `json:"value"`
	}) (string, error) {
		return a.Value, nil
	})
}

// emptyResultModel 返回一条不含任何部件的助手消息：结果为空但运行是正常完成。
func emptyResultModel() llm.Model {
	return mock.New("empty", func(*llm.Request) *llm.Response {
		return &llm.Response{Message: core.Message{Role: core.RoleAssistant}, StopReason: llm.StopEnd}
	})
}

// gatedModel 调用 blocked 工具，由 terminalGate 停在审批之前。
func gatedModel() llm.Model {
	return mock.New("g", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("审批之后完成")
		}
		return mock.CallTool("b1", "blocked", `{}`)
	})
}

func blockedTool() tool.Tool {
	return tool.New("blocked", "被审批闸门拦下的工具", func(_ *tool.Context, _ struct{}) (string, error) {
		return "执行了", nil
	})
}

type terminalGate struct{ agent.BaseMiddleware }

func (terminalGate) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if c.Name == "blocked" && !lc.IsApproved(c) {
		return core.Directive{Kind: core.Interrupt, Reason: "需要审批"}, nil
	}
	return core.Directive{}, nil
}

// errorModel 让运行失败。
type errorModel struct{}

func (errorModel) Name() string { return "err" }
func (errorModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, errors.New("模型故障"))
	}
}

func terminalAgent(t *testing.T, store checkpoint.Checkpointer, model llm.Model, tools ...tool.Tool) *agent.Agent {
	t.Helper()
	return terminalAgentWith(t, store, model, nil, tools...)
}

func terminalAgentWith(t *testing.T, store checkpoint.Checkpointer, model llm.Model, mw agent.Middleware, tools ...tool.Tool) *agent.Agent {
	t.Helper()
	opts := []agent.Option{agent.WithModel(model), agent.WithCheckpointer(store)}
	if mw != nil {
		opts = append(opts, agent.WithMiddleware(mw))
	}
	if len(tools) > 0 {
		opts = append(opts, agent.WithTools(tools...))
	}
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

type terminalBackends struct {
	store  checkpoint.Checkpointer
	reopen func() checkpoint.Checkpointer
}

func newTerminalBackends(t *testing.T) map[string]func() terminalBackends {
	t.Helper()
	return map[string]func() terminalBackends{
		"memory": func() terminalBackends {
			m := checkpoint.NewMemory()
			return terminalBackends{store: m, reopen: func() checkpoint.Checkpointer { return m }}
		},
		"file": func() terminalBackends {
			dir := t.TempDir()
			open := func() checkpoint.Checkpointer {
				f, err := checkpoint.NewFile(dir)
				if err != nil {
					t.Fatal(err)
				}
				return f
			}
			return terminalBackends{store: open(), reopen: open}
		},
	}
}

// headCheckpoint 读取线程最新快照。
func headCheckpoint(t *testing.T, store checkpoint.Checkpointer, threadID string) *checkpoint.Checkpoint {
	t.Helper()
	latest, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil {
		t.Fatalf("线程 %s 没有快照", threadID)
	}
	return latest
}

func headCompletion(t *testing.T, store checkpoint.Checkpointer, threadID string) *checkpoint.Completion {
	t.Helper()
	return headCheckpoint(t, store, threadID).Completion
}

func historyCount(t *testing.T, store checkpoint.Checkpointer, threadID string) int {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return len(hist)
}

// --- V4：身份与结果一致 -----------------------------------------------------

// TestRecordCompletionUsesRunIdentity：完成与失败的运行都用自己的 RunID 与冻结结果；
// 调用方改写 Wait 返回结果里的可变字节，不影响已经记录的终态。
func TestRecordCompletionUsesRunIdentity(t *testing.T) {
	ctx := context.Background()
	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()
			sent := []byte(`{"key":"before"}`)
			a := terminalAgent(t, b.store, terminalModel("最终回答", sent), noopTool())

			run := a.Stream(ctx, "干活", agent.OnThread("identity-"+name))
			res, err := run.Wait()
			if err != nil {
				t.Fatalf("运行失败: %v", err)
			}
			for _, part := range res.Message.Parts {
				if img, ok := part.(core.Image); ok {
					copy(img.Data, []byte(`{"key":"after!"}`))
				}
			}
			if err := a.RecordCompletion(ctx, run); err != nil {
				t.Fatalf("RecordCompletion: %v", err)
			}

			record := headCompletion(t, b.reopen(), "identity-"+name)
			if record == nil {
				t.Fatal("终态未写入")
			}
			if record.RunID != run.ID {
				t.Fatalf("终态 RunID = %s，期望 %s", record.RunID, run.ID)
			}
			if record.Kind != checkpoint.CompletionCompleted || record.Err != "" {
				t.Fatalf("终态种类/错误不符: %+v", record)
			}
			if record.Result == nil || record.Result.Message.Text() != "最终回答" {
				t.Fatalf("终态结果不符: %+v", record.Result)
			}
			for _, part := range record.Result.Message.Parts {
				if img, ok := part.(core.Image); ok && string(img.Data) != `{"key":"before"}` {
					t.Fatalf("调用方改写污染了冻结终态: %s", img.Data)
				}
			}
			// 终态记录指向本次运行的边界，且父边界的 step 被沿用。
			head := headCheckpoint(t, b.reopen(), "identity-"+name)
			if head.ParentID == "" {
				t.Fatal("终态记录没有父边界")
			}
			parent, err := b.reopen().Load(ctx, "identity-"+name, head.ParentID)
			if err != nil {
				t.Fatal(err)
			}
			if parent.Step != head.Step || parent.Completion != nil || parent.Pause != nil || parent.Pending != nil {
				t.Fatalf("父边界不符: step=%d completion=%+v pause=%+v", parent.Step, parent.Completion, parent.Pause)
			}

			// 失败运行：记录自己的错误文本，不带结果。
			failing := terminalAgent(t, b.reopen(), errorModel{})
			badRun := failing.Stream(ctx, "boom", agent.OnThread("identity-fail-"+name))
			if _, err := badRun.Wait(); err == nil {
				t.Fatal("失败运行应报错")
			}
			if err := failing.RecordCompletion(ctx, badRun); err != nil {
				t.Fatalf("失败运行记录: %v", err)
			}
			failed := headCompletion(t, b.reopen(), "identity-fail-"+name)
			if failed == nil || failed.Kind != checkpoint.CompletionFailed ||
				failed.RunID != badRun.ID || failed.Result != nil ||
				!strings.Contains(failed.Err, "模型故障") {
				t.Fatalf("失败终态不符: %+v", failed)
			}
		})
	}
}

// --- 暂停判定与冻结顺序 -----------------------------------------------------

// TestRunPausedOutcome：Paused 只表达 drive 冻结的实际结局，不能用"结果是否为空"代替。
func TestRunPausedOutcome(t *testing.T) {
	ctx := context.Background()

	t.Run("暂停", func(t *testing.T) {
		store := checkpoint.NewMemory()
		a := terminalAgentWith(t, store, gatedModel(), terminalGate{}, noopTool(), blockedTool())
		run := a.Stream(ctx, "go", agent.OnThread("pause-outcome"))
		if _, err := run.Wait(); err != nil {
			t.Fatalf("暂停是正常收尾: %v", err)
		}
		if !run.Paused() {
			t.Fatal("暂停运行应报告 Paused")
		}
		if err := a.RecordCompletion(ctx, run); !errors.Is(err, agent.ErrRunPaused) {
			t.Fatalf("暂停记录应被拒绝，实际: %v", err)
		}
	})

	t.Run("空结果正常完成", func(t *testing.T) {
		store := checkpoint.NewMemory()
		a := terminalAgent(t, store, emptyResultModel())
		run := a.Stream(ctx, "go", agent.OnThread("empty-outcome"))
		res, err := run.Wait()
		if err != nil {
			t.Fatalf("运行失败: %v", err)
		}
		if len(res.Message.Parts) != 0 {
			t.Fatalf("夹具本意是空结果，得到 %v", res.Message.Parts)
		}
		if run.Paused() {
			t.Fatal("正常完成不应报告 Paused")
		}
		if err := a.RecordCompletion(ctx, run); err != nil {
			t.Fatalf("空结果也应能记录终态: %v", err)
		}
		record := headCompletion(t, store, "empty-outcome")
		if record == nil || record.Kind != checkpoint.CompletionCompleted || record.Result == nil {
			t.Fatalf("空结果终态不符: %+v", record)
		}
	})

	t.Run("失败", func(t *testing.T) {
		store := checkpoint.NewMemory()
		a := terminalAgent(t, store, errorModel{})
		run := a.Stream(ctx, "go", agent.OnThread("fail-outcome"))
		if _, err := run.Wait(); err == nil {
			t.Fatal("应报错")
		}
		if run.Paused() {
			t.Fatal("失败运行不应报告 Paused")
		}
	})

	t.Run("未驱动", func(t *testing.T) {
		store := checkpoint.NewMemory()
		a := terminalAgent(t, store, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("done") }))
		run := a.Stream(ctx, "go", agent.OnThread("undriven"))
		if run.Paused() {
			t.Fatal("尚未驱动的运行不应报告 Paused")
		}
	})
}

// TestRunTerminalBoundaryBeforeEvents：终态事件可见时，结局与边界已经冻结完整——
// 冻结发生在发布终态事件之前，观察方拿不到"半个"终态。
func TestRunTerminalBoundaryBeforeEvents(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		slug       string
		model      llm.Model
		newStore   func() checkpoint.Checkpointer
		mw         agent.Middleware
		tools      []tool.Tool
		wantPaused bool
		wantErr    bool
		wantRecord error
	}{
		{
			name: "成功", slug: "order-ok", model: terminalModel("完成", []byte(`{}`)),
			newStore: func() checkpoint.Checkpointer { return checkpoint.NewMemory() },
			tools:    []tool.Tool{noopTool()}, wantRecord: nil,
		},
		{
			name: "暂停", slug: "order-pause", model: gatedModel(),
			newStore: func() checkpoint.Checkpointer { return checkpoint.NewMemory() },
			mw:       terminalGate{}, tools: []tool.Tool{noopTool(), blockedTool()}, wantPaused: true, wantRecord: agent.ErrRunPaused,
		},
		{
			// 模型故障：loop.fail 仍会写一个不含 Pause 的接缝提交，所以这条运行
			// 有自己的边界，终态按失败记录。
			name: "执行失败", slug: "order-fail", model: errorModel{},
			newStore: func() checkpoint.Checkpointer { return checkpoint.NewMemory() },
			wantErr:  true,
		},
		{
			name: "准备失败", slug: "order-prepare", model: terminalModel("完成", []byte(`{}`)),
			newStore: func() checkpoint.Checkpointer {
				return &refuseBeginStore{ThreadCoordinator: checkpoint.NewMemory()}
			},
			tools: []tool.Tool{noopTool()}, wantErr: true, wantRecord: agent.ErrNoCommittedBoundary,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.newStore()
			a := terminalAgentWith(t, store, tc.model, tc.mw, tc.tools...)
			run := a.Stream(ctx, "go", agent.OnThread(tc.slug))
			events, cancel := run.Events(bus.Lossless)
			defer cancel()

			type observed struct {
				pausedAtEvent bool
			}
			seen := make(chan observed, 1)
			go func() {
				for ev := range events {
					switch ev.(type) {
					case core.RunDone, core.RunFailed, core.Interrupted:
						seen <- observed{pausedAtEvent: run.Paused()}
						return
					}
				}
			}()

			_, err := run.Wait()
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr=%v", err, tc.wantErr)
			}

			var got observed
			select {
			case got = <-seen:
			case <-time.After(5 * time.Second):
				t.Fatal("没观察到终态事件")
			}
			if got.pausedAtEvent != run.Paused() {
				t.Fatalf("终态事件可见时 Paused = %v，运行稳定后 = %v", got.pausedAtEvent, run.Paused())
			}
			if got.pausedAtEvent != tc.wantPaused {
				t.Fatalf("终态事件可见时 Paused = %v，期望 %v", got.pausedAtEvent, tc.wantPaused)
			}

			recordErr := a.RecordCompletion(ctx, run)
			if tc.wantRecord == nil {
				if recordErr != nil {
					t.Fatalf("应可记录终态: %v", recordErr)
				}
				return
			}
			if !errors.Is(recordErr, tc.wantRecord) {
				t.Fatalf("记录错误 = %v，期望 %v", recordErr, tc.wantRecord)
			}
		})
	}
}

// --- V4：拒绝与边界 ---------------------------------------------------------

// TestRecordCompletionRejectsInvalidRun：nil、他人运行、未结束、暂停都拒绝且不写入历史。
func TestRecordCompletionRejectsInvalidRun(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	a := terminalAgent(t, store, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") }))
	other, err := agent.New(
		agent.WithModel(mock.New("o", func(*llm.Request) *llm.Response { return mock.Text("other") })),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	before := historyCount(t, store, "reject")

	if err := a.RecordCompletion(ctx, nil); !errors.Is(err, agent.ErrNoRun) {
		t.Fatalf("nil 运行应拒绝，实际: %v", err)
	}
	lazy := a.Stream(ctx, "go", agent.OnThread("reject"))
	if err := a.RecordCompletion(ctx, lazy); !errors.Is(err, agent.ErrRunNotFinished) {
		t.Fatalf("未结束的运行应拒绝，实际: %v", err)
	}
	foreign := other.Stream(ctx, "go", agent.OnThread("reject-other"))
	if _, err := foreign.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := a.RecordCompletion(ctx, foreign); !errors.Is(err, agent.ErrRunNotOwned) {
		t.Fatalf("他人运行应拒绝，实际: %v", err)
	}

	pausedAgent := terminalAgentWith(t, store, gatedModel(), terminalGate{}, noopTool(), blockedTool())
	paused := pausedAgent.Stream(ctx, "go", agent.OnThread("reject-paused"))
	if _, err := paused.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := pausedAgent.RecordCompletion(ctx, paused); !errors.Is(err, agent.ErrRunPaused) {
		t.Fatalf("暂停应拒绝，实际: %v", err)
	}

	if after := historyCount(t, store, "reject"); after != before {
		t.Fatalf("被拒绝的请求写入了历史: %d -> %d", before, after)
	}
	hist, err := store.History(ctx, "reject-paused")
	if err != nil {
		t.Fatal(err)
	}
	for _, cp := range hist {
		if cp.Completion != nil {
			t.Fatalf("暂停被记成终态: %+v", cp.Completion)
		}
	}
}

// TestRecordCompletionRequiresOwnBoundary：本次运行没有成功提交时不得伪造记录，
// 也不得改写前一次运行的终态。
func TestRecordCompletionRequiresOwnBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("首步提交失败", func(t *testing.T) {
		base := checkpoint.NewMemory()
		store := &terminalStore{
			ThreadCoordinator: base,
			beforeCommit: func(checkpoint.CommitRequest) error {
				return errors.New("存储不可用")
			},
		}
		a := terminalAgent(t, store, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") }))
		run := a.Stream(ctx, "go", agent.OnThread("first-commit-fails"))
		if _, err := run.Wait(); err == nil {
			t.Fatal("提交失败应让运行失败")
		}
		if err := a.RecordCompletion(ctx, run); !errors.Is(err, agent.ErrNoCommittedBoundary) {
			t.Fatalf("应报无边界，实际: %v", err)
		}
	})

	t.Run("仅继承旧 head", func(t *testing.T) {
		base := checkpoint.NewMemory()
		first := terminalAgent(t, base, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("第一次") }))
		run1 := first.Stream(ctx, "go", agent.OnThread("inherited"))
		if _, err := run1.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := first.RecordCompletion(ctx, run1); err != nil {
			t.Fatal(err)
		}
		before := historyCount(t, base, "inherited")

		store := &refuseBeginStore{ThreadCoordinator: base}
		second := terminalAgent(t, store, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("第二次") }))
		run2 := second.Stream(ctx, "go", agent.OnThread("inherited"))
		if _, err := run2.Wait(); err == nil {
			t.Fatal("prepare 失败应报错")
		}
		if err := second.RecordCompletion(ctx, run2); !errors.Is(err, agent.ErrNoCommittedBoundary) {
			t.Fatalf("继承的 head 不算本次边界，应拒绝: %v", err)
		}

		if after := historyCount(t, base, "inherited"); after != before {
			t.Fatalf("被拒绝的记录追加了历史: %d -> %d", before, after)
		}
		if head := headCompletion(t, base, "inherited"); head == nil || head.RunID != run1.ID {
			t.Fatalf("前次运行的终态被改写: %+v", head)
		}
	})
}

// TestRecordCompletionRequiresDurable：只有基础 Checkpointer 的存储返回
// ErrDurableUnavailable 且不回退成普通 Save；前台执行仍可用该存储。
func TestRecordCompletionRequiresDurable(t *testing.T) {
	ctx := context.Background()
	base := newPlainCheckpointer()
	a := terminalAgent(t, base, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") }))
	run := a.Stream(ctx, "go", agent.OnThread("plain-store"))
	if _, err := run.Wait(); err != nil {
		t.Fatalf("前台运行应正常完成: %v", err)
	}
	if base.saveCalls() == 0 {
		t.Fatal("前台路径没有使用基础存储")
	}
	savesAfterRun := base.saveCalls()

	if err := a.RecordCompletion(ctx, run); !errors.Is(err, checkpoint.ErrDurableUnavailable) {
		t.Fatalf("基础存储应报 ErrDurableUnavailable，实际: %v", err)
	}
	if base.saveCalls() != savesAfterRun {
		t.Fatalf("记录失败却回退了 Save: %d -> %d", savesAfterRun, base.saveCalls())
	}
}
