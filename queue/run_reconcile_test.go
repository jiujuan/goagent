package queue

// SM-12（ADR-0034 D5）：显式终态修复。执行已经完成、终态快照已经提交，而任务记录的收尾写入
// 没落地——这是重连客户端看得见 running 却永远等不到结局的那一种状态。ReconcileRun 只凭这条
// RunID 自己的已提交 Completion 修复它；没有证据就报 stale，绝不拿线程最新 head 上别的 Run
// 的结局来填，也不把"执行成功但终态快照没保存"猜成已保存。

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/runstore"
)

// --- 夹具 -------------------------------------------------------------------

// checkpointBackends 让每个修复用例都跑一遍内存和文件两种存储：修复读的是查询、摘要与 Load
// 三条路，两条后端都得给出同样的判定。
type checkpointFactory struct {
	name string
	open func(t *testing.T) checkpoint.Checkpointer
}

func checkpointBackends() []checkpointFactory {
	return []checkpointFactory{
		{"memory", func(t *testing.T) checkpoint.Checkpointer { return checkpoint.NewMemory() }},
		{"file", func(t *testing.T) checkpoint.Checkpointer {
			f, err := checkpoint.NewFile(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return f
		}},
	}
}

// runStores 同样让任务存储也走两遍：修复是一次 CAS，两种实现都要接得住。
type runStoreFactory struct {
	name string
	open func(t *testing.T) runstore.Store
}

func runStoreBackends() []runStoreFactory {
	return []runStoreFactory{
		{"memory", func(t *testing.T) runstore.Store { return runstore.NewMemory() }},
		{"file", func(t *testing.T) runstore.Store {
			f, err := runstore.NewFile(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return f
		}},
	}
}

// settleWithoutRecord 投递一次受管运行，让任务存储在收尾时不可用：执行完成、终态快照提交、
// 任务记录留在 running。这正是崩溃在两步之间留下的形状。
func settleWithoutRecord(t *testing.T, a *agent.Agent, runs runstore.Store, runID, threadID, input string) ManagedRunOutcome {
	t.Helper()
	w := managedWorker(t, a, &refuseTerminalStore{Store: runs}, "worker-repair")
	out, err := w.Execute(context.Background(), managedGet(t, runs, runID).RunID, input)
	if err == nil {
		t.Fatalf("任务存储收尾写入应当失败")
	}
	if out.RecordError == nil {
		t.Fatalf("执行结局没落到任务记录，却没报记录错误: %+v", out)
	}
	return out
}

func newManagedRecord(t *testing.T, runs runstore.Store, runID, threadID, turnID string) runstore.RunRecord {
	t.Helper()
	return managedCreate(t, runs, managedRecord(runID, threadID, turnID))
}

func resultText(t *testing.T, res *core.Result) string {
	t.Helper()
	if res == nil {
		t.Fatal("记录里没有结果")
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	return string(encoded)
}

// --- 有证据就修，没证据就说修不了 ---------------------------------------------------------

func TestReconcileExactRun(t *testing.T) {
	for _, cps := range checkpointBackends() {
		for _, rs := range runStoreBackends() {
			t.Run(cps.name+"/"+rs.name, func(t *testing.T) {
				ctx := context.Background()
				store := cps.open(t)
				runs := rs.open(t)
				var calls atomic.Int64
				a := managedAgent(t, store, "修复用的回答", &calls)

				newManagedRecord(t, runs, "run-fix", "thread-fix", "turn-fix")
				out := settleWithoutRecord(t, a, runs, "run-fix", "thread-fix", "问题")
				if out.CompletionRef == nil {
					t.Fatalf("终态快照本应提交: %+v", out)
				}
				stuck := managedGet(t, runs, "run-fix")
				if stuck.Status != runstore.StatusRunning {
					t.Fatalf("修复前记录应当仍是 running，实际 %s", stuck.Status)
				}

				r, err := NewRunReconciler(runs, store)
				if err != nil {
					t.Fatalf("NewRunReconciler: %v", err)
				}
				report, err := r.ReconcileRun(ctx, "run-fix")
				if err != nil {
					t.Fatalf("ReconcileRun: %v", err)
				}
				if !report.Repaired || report.Reason != ReconcileRepaired {
					t.Fatalf("修复判定 = %+v", report)
				}
				if report.Evidence == nil || report.Evidence.ThreadID != "thread-fix" {
					t.Fatalf("修复依据 = %+v", report.Evidence)
				}
				if report.Attempts < 1 {
					t.Errorf("修复应当报告 CAS 次数: %+v", report)
				}

				fixed := report.Record
				if fixed.Status != runstore.StatusSucceeded || fixed.FinishReason != runstore.FinishReasonCompleted {
					t.Fatalf("修复后的终态 = %s/%s", fixed.Status, fixed.FinishReason)
				}
				if fixed.Error != nil {
					t.Errorf("成功结局不该带错误: %+v", fixed.Error)
				}
				if !strings.Contains(resultText(t, fixed.Result), "修复用的回答") {
					t.Errorf("结果不是快照里冻住的那条: %s", resultText(t, fixed.Result))
				}
				if fixed.CompletionRef == nil || fixed.CompletionRef.CheckpointID != out.CompletionRef.CheckpointID {
					t.Errorf("修复后的 CompletionRef = %+v, 期望沿用同一条快照", fixed.CompletionRef)
				}
				// 收尾记录本来就没保存上，现在有已提交的终态快照为证，才把它清掉。
				if fixed.RecordError != nil {
					t.Errorf("修复后仍带着收尾错误: %+v", fixed.RecordError)
				}
				// 所有权与身份是原样保留的事实，不是修复新造出来的。
				if fixed.WorkerID != stuck.WorkerID || fixed.WorkerEpoch != stuck.WorkerEpoch {
					t.Errorf("修复改写了 worker 归属: %+v -> %+v", stuck, fixed)
				}
				if fixed.Version != stuck.Version+1 {
					t.Errorf("修复版本 = %d, 期望 %d", fixed.Version, stuck.Version+1)
				}

				// 再修一次是幂等确认：记录已经终态，不再写第二次。
				again, err := r.ReconcileRun(ctx, "run-fix")
				if err != nil {
					t.Fatalf("重复修复: %v", err)
				}
				if again.Repaired || again.Reason != ReconcileAlreadySettled || again.Record.Version != fixed.Version {
					t.Fatalf("重复修复的判定 = %+v", again)
				}
			})
		}
	}
}

func TestReconcileMissingEvidence(t *testing.T) {
	for _, cps := range checkpointBackends() {
		t.Run(cps.name, func(t *testing.T) {
			ctx := context.Background()
			store := cps.open(t)
			runs := runstore.NewMemory()
			var calls atomic.Int64
			a := managedAgent(t, store, "有快照无终态", &calls)

			// 运行执行过、快照也提交了，但从来没记过终态：这条记录缺的就是证据本身。
			newManagedRecord(t, runs, "run-noev", "thread-noev", "turn-noev")
			claimed := managedCAS(t, runs, managedGet(t, runs, "run-noev"), func(r *runstore.RunRecord) {
				r.Status = runstore.StatusRunning
				r.WorkerID = "worker-lost"
				r.WorkerEpoch = 1
			})
			if _, err := a.RunManaged(ctx, agent.ManagedRunSpec{
				RunID: "run-noev", ThreadID: "thread-noev", TurnID: "turn-noev",
			}, "问题"); err != nil {
				t.Fatalf("运行失败: %v", err)
			}

			r, err := NewRunReconciler(runs, store)
			if err != nil {
				t.Fatalf("NewRunReconciler: %v", err)
			}
			report, err := r.ReconcileRun(ctx, "run-noev")
			if err != nil {
				t.Fatalf("ReconcileRun: %v", err)
			}
			if report.Repaired || report.Reason != ReconcileStale || report.Evidence != nil {
				t.Fatalf("缺证据时应当报 stale: %+v", report)
			}
			if report.Record.Status != runstore.StatusRunning || report.Record.Version != claimed.Version {
				t.Fatalf("没有证据却改了记录: %+v", report.Record)
			}
			// 修不了不是一条错误：GetRun 与 ReconcileRun 都是纯读加显式写，这里没有任何写入。
			if got := managedGet(t, runs, "run-noev"); got.Version != claimed.Version {
				t.Fatalf("任务记录被写过: %d -> %d", claimed.Version, got.Version)
			}

			// 执行成功但终态快照没保存上：不能因为"看着像成功了"就把它说成已经持久化。
			newManagedRecord(t, runs, "run-recfail", "thread-recfail", "turn-recfail")
			claimed = managedCAS(t, runs, managedGet(t, runs, "run-recfail"), func(rec *runstore.RunRecord) {
				rec.Status = runstore.StatusRunning
				rec.WorkerID = "worker-lost"
				rec.WorkerEpoch = 1
				rec.LatestCheckpointRef = &checkpoint.Ref{ThreadID: "thread-recfail", CheckpointID: "cp-missing"}
				rec.CheckpointRevision = 3
				rec.RecordError = &runstore.RunError{Code: runstore.ErrCodeCompletionRecord, Message: "终态快照没保存上", Retryable: true}
			})
			report, err = r.ReconcileRun(ctx, "run-recfail")
			if err != nil {
				t.Fatalf("ReconcileRun 收尾失败样本: %v", err)
			}
			if report.Repaired || report.Reason != ReconcileStale {
				t.Fatalf("猜测式的修复: %+v", report)
			}
			kept := managedGet(t, runs, "run-recfail")
			if kept.Status != runstore.StatusRunning || kept.RecordError == nil || kept.RecordError.Code != runstore.ErrCodeCompletionRecord {
				t.Fatalf("收尾错误应当原样留着等人处理: %+v", kept)
			}
			if kept.Version != claimed.Version {
				t.Fatalf("没有证据却推进了版本: %d -> %d", claimed.Version, kept.Version)
			}

			// 未知 RunID 仍是 ErrNotFound，不会开一次空修。
			if _, err := r.ReconcileRun(ctx, "run-absent"); !errors.Is(err, runstore.ErrNotFound) {
				t.Errorf("未知 RunID: err = %v, 期望 runstore.ErrNotFound", err)
			}
		})
	}
}

// TestReconcileDoesNotBorrowAnotherRun 覆盖"B 已推进线程"：线程最新的那条终态属于 B，
// A 的修复只能取 A 自己的 Completion。
func TestReconcileDoesNotBorrowAnotherRun(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs := runstore.NewMemory()

	var aCalls, bCalls atomic.Int64
	agentA := managedAgent(t, store, "A 的结局", &aCalls)
	agentB := managedAgent(t, store, "B 的结局", &bCalls)

	// A 执行完成但收尾没落地。
	newManagedRecord(t, runs, "run-a", "thread-shared", "turn-a")
	settleWithoutRecord(t, agentA, runs, "run-a", "thread-shared", "A 的问题")

	// B 在同一条线程上跑完整的一轮，线程 head 从此属于 B。
	newManagedRecord(t, runs, "run-b", "thread-shared", "turn-b")
	w := managedWorker(t, agentB, runs, "worker-b")
	if _, err := w.Execute(ctx, "run-b", "B 的问题"); err != nil {
		t.Fatalf("B 的受管执行失败: %v", err)
	}
	head, found := func() (checkpoint.CheckpointMetadata, bool) {
		metadata, ok, err := store.ReadSessionHead(ctx, "thread-shared")
		if err != nil {
			t.Fatalf("读线程 head: %v", err)
		}
		return metadata, ok
	}()
	if !found {
		t.Fatal("线程应当已经有 head")
	}
	if head.RunID != "run-b" {
		t.Fatalf("B 应当已经推进线程 head: %+v", head)
	}

	r, err := NewRunReconciler(runs, store)
	if err != nil {
		t.Fatalf("NewRunReconciler: %v", err)
	}
	report, err := r.ReconcileRun(ctx, "run-a")
	if err != nil {
		t.Fatalf("ReconcileRun: %v", err)
	}
	if !report.Repaired {
		t.Fatalf("A 自己有已提交的终态: %+v", report)
	}
	repaired := report.Record
	if repaired.Status != runstore.StatusSucceeded {
		t.Fatalf("A 的终态 = %s", repaired.Status)
	}
	if text := resultText(t, repaired.Result); !strings.Contains(text, "A 的结局") || strings.Contains(text, "B 的结局") {
		t.Fatalf("A 借用了 B 的结局: %s", text)
	}
	if repaired.CompletionRef != nil && repaired.CompletionRef.ThreadID != "thread-shared" {
		t.Errorf("修复依据落在别的线程上: %+v", repaired.CompletionRef)
	}
	// B 的记录不受影响。
	if b := managedGet(t, runs, "run-b"); b.Status != runstore.StatusSucceeded {
		t.Errorf("B 的记录被牵动: %+v", b)
	}
	// 反过来：只有 B 的终态、自己没有证据的 C，依旧修不动。
	newManagedRecord(t, runs, "run-c", "thread-shared", "turn-c")
	managedCAS(t, runs, managedGet(t, runs, "run-c"), func(rec *runstore.RunRecord) {
		rec.Status = runstore.StatusRunning
		rec.WorkerID = "worker-c"
		rec.WorkerEpoch = 1
	})
	report, err = r.ReconcileRun(ctx, "run-c")
	if err != nil {
		t.Fatalf("ReconcileRun C: %v", err)
	}
	if report.Repaired || report.Reason != ReconcileStale {
		t.Fatalf("C 没有自己的证据，却得到了 %+v", report)
	}
}

// --- CAS 竞争 ---------------------------------------------------------------

// racingStore 拒绝第一次把记录带入终态的写入，模拟另一个写入方刚刚推进过版本。
type racingStore struct {
	runstore.Store

	mu       sync.Mutex
	refusals int
	arm      bool
}

func (s *racingStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	s.mu.Lock()
	shouldRefuse := s.arm && next.Status.Terminal()
	if shouldRefuse {
		s.arm = false
		s.refusals++
	}
	s.mu.Unlock()
	if shouldRefuse {
		// 先把一次真实的心跳写进去，再把这次 CAS 顶回去：修复看到的版本一定比它读到的新。
		latest, err := s.Store.Get(ctx, runID)
		if err != nil {
			return runstore.RunRecord{}, err
		}
		latest.HeartbeatAt = latest.UpdatedAt.Add(time.Minute)
		latest.WorkerLeaseUntil = latest.HeartbeatAt.Add(time.Minute)
		if _, err := s.Store.CompareAndSwap(ctx, runID, latest.Version, latest); err != nil {
			return runstore.RunRecord{}, err
		}
		return runstore.RunRecord{}, runstore.ErrConflict
	}
	return s.Store.CompareAndSwap(ctx, runID, expected, next)
}

func TestReconcileCASRace(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	base := runstore.NewMemory()
	var calls atomic.Int64
	a := managedAgent(t, store, "竞争下的回答", &calls)

	newManagedRecord(t, base, "run-race", "thread-race", "turn-race")
	settleWithoutRecord(t, a, base, "run-race", "thread-race", "问题")
	before := managedGet(t, base, "run-race")

	runs := &racingStore{Store: base, arm: true}
	r, err := NewRunReconciler(runs, store)
	if err != nil {
		t.Fatalf("NewRunReconciler: %v", err)
	}
	report, err := r.ReconcileRun(ctx, "run-race")
	if err != nil {
		t.Fatalf("一次冲突就把修复顶掉了: %v", err)
	}
	if !report.Repaired {
		t.Fatalf("竞争之后仍应凭证据修复: %+v", report)
	}
	if report.Attempts < 2 {
		t.Errorf("修复只走了一次 CAS, 没有重读重试: %+v", report)
	}
	runs.mu.Lock()
	refusals := runs.refusals
	runs.mu.Unlock()
	if refusals != 1 {
		t.Errorf("被顶回的 CAS 次数 = %d, 期望 1", refusals)
	}

	after := managedGet(t, base, "run-race")
	if after.Status != runstore.StatusSucceeded {
		t.Fatalf("竞争后的终态 = %s", after.Status)
	}
	if after.Version <= before.Version+1 {
		t.Fatalf("中间那次心跳没有被保留下来: %d -> %d", before.Version, after.Version)
	}
	if after.HeartbeatAt.IsZero() {
		t.Error("干扰写入的心跳被抹掉了")
	}
	if !strings.Contains(resultText(t, after.Result), "竞争下的回答") {
		t.Errorf("结果不是快照里冻住的那条: %s", resultText(t, after.Result))
	}
}

// --- 能力边界 ---------------------------------------------------------------

// plainCheckpointer 只有基础的四个方法，既不能列也不能读摘要。
type plainCheckpointer struct{ inner checkpoint.Checkpointer }

func (p plainCheckpointer) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	return p.inner.Save(ctx, cp)
}
func (p plainCheckpointer) Load(ctx context.Context, threadID, id string) (*checkpoint.Checkpoint, error) {
	return p.inner.Load(ctx, threadID, id)
}
func (p plainCheckpointer) Latest(ctx context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	return p.inner.Latest(ctx, threadID)
}
func (p plainCheckpointer) History(ctx context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	return p.inner.History(ctx, threadID)
}

// queryOnlyCheckpointer 能列摘要，却答不出"这一条快照是什么"，也不能按 Ref 定位。
type queryOnlyCheckpointer struct {
	plainCheckpointer
	query checkpoint.Query
}

func (q queryOnlyCheckpointer) QueryCheckpoints(ctx context.Context, opts checkpoint.ListOptions) (checkpoint.MetadataPage, error) {
	return q.query.QueryCheckpoints(ctx, opts)
}

func (q queryOnlyCheckpointer) ListSessions(ctx context.Context, opts checkpoint.ListOptions) (checkpoint.SessionPage, error) {
	return q.query.ListSessions(ctx, opts)
}

func TestReconcileRequiresCapabilities(t *testing.T) {
	base := checkpoint.NewMemory()
	runs := runstore.NewMemory()

	if _, err := NewRunReconciler(runs, plainCheckpointer{inner: base}); !errors.Is(err, checkpoint.ErrCapabilityUnavailable) {
		t.Errorf("只有基础接口的存储: err = %v, 期望 ErrCapabilityUnavailable", err)
	}
	if _, err := NewRunReconciler(runs, queryOnlyCheckpointer{
		plainCheckpointer: plainCheckpointer{inner: base},
		query:             base,
	}); !errors.Is(err, checkpoint.ErrCapabilityUnavailable) {
		t.Errorf("缺摘要读取的存储: err = %v, 期望 ErrCapabilityUnavailable", err)
	}
	readerOnly := readerOnlyCheckpointer{plainCheckpointer: plainCheckpointer{inner: base}, reader: base}
	if _, err := NewRunReconciler(runs, readerOnly); !errors.Is(err, checkpoint.ErrCapabilityUnavailable) {
		t.Errorf("缺列表查询的存储: err = %v, 期望 ErrCapabilityUnavailable", err)
	}
	if _, err := NewRunReconciler(nil, base); err == nil {
		t.Error("没有任务存储也建起了修复器")
	}
	if _, err := NewRunReconciler(runs, nil); err == nil {
		t.Error("没有快照存储也建起了修复器")
	}
	if _, err := NewRunReconciler(runs, base); err != nil {
		t.Errorf("两种能力齐备的存储应当可用: %v", err)
	}
}

// readerOnlyCheckpointer 能按 Ref 读摘要，但列不出这条 RunID 的终态在哪儿。
type readerOnlyCheckpointer struct {
	plainCheckpointer
	reader checkpoint.MetadataReader
}

func (r readerOnlyCheckpointer) ReadCheckpointMetadata(ctx context.Context, ref checkpoint.Ref) (checkpoint.CheckpointMetadata, bool, error) {
	return r.reader.ReadCheckpointMetadata(ctx, ref)
}

func (r readerOnlyCheckpointer) ReadSessionHead(ctx context.Context, threadID string) (checkpoint.CheckpointMetadata, bool, error) {
	return r.reader.ReadSessionHead(ctx, threadID)
}

// --- 证据与记录不一致 -------------------------------------------------------

func TestReconcileContradictoryEvidence(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs := runstore.NewMemory()
	var calls atomic.Int64
	a := managedAgent(t, store, "自相矛盾", &calls)

	newManagedRecord(t, runs, "run-contra", "thread-contra", "turn-contra")
	out := settleWithoutRecord(t, a, runs, "run-contra", "thread-contra", "问题")
	if out.CompletionRef == nil {
		t.Fatal("样本需要先有一条已提交的终态")
	}

	// 记录自己指了一条不属于它的 Completion：宁可报不一致，也不照着它去改结局。
	managedCAS(t, runs, managedGet(t, runs, "run-contra"), func(rec *runstore.RunRecord) {
		rec.CompletionRef = &checkpoint.Ref{ThreadID: "thread-contra", CheckpointID: "cp-that-is-not-mine"}
	})
	r, err := NewRunReconciler(runs, store)
	if err != nil {
		t.Fatalf("NewRunReconciler: %v", err)
	}
	report, err := r.ReconcileRun(ctx, "run-contra")
	if err != nil {
		t.Fatalf("不一致的证据不该变成存储错误: %v", err)
	}
	if report.Repaired || report.Reason != ReconcileContradictory {
		t.Fatalf("判定 = %+v", report)
	}
	if strings.TrimSpace(report.Detail) == "" {
		t.Error("不一致的判定没有说明哪里不一致")
	}
	if got := managedGet(t, runs, "run-contra"); got.Status != runstore.StatusRunning {
		t.Fatalf("不一致时不该改终态: %s", got.Status)
	}

	// 记录还停在 queued：worker 从没领过它，快照却不属于任何领取者，这种状态机不允许的写法
	// 原样报出来，不悄悄改成别的状态。
	// 终态快照落在别的线程上：这条 RunID 的记录不属于那条线程，结局也就不能算到这条记录头上。
	newManagedRecord(t, runs, "run-cross", "thread-home", "turn-cross")
	cross, err := a.StreamManaged(ctx, agent.ManagedRunSpec{RunID: "run-cross", ThreadID: "thread-other", TurnID: "turn-cross"}, "问题")
	if err != nil {
		t.Fatalf("在别的线程上启动运行: %v", err)
	}
	if _, err := cross.Wait(); err != nil {
		t.Fatalf("跨线程运行失败: %v", err)
	}
	if err := a.RecordCompletion(ctx, cross); err != nil {
		t.Fatalf("记跨线程终态: %v", err)
	}
	before := managedGet(t, runs, "run-cross")
	report, err = r.ReconcileRun(ctx, "run-cross")
	if err != nil {
		t.Fatalf("跨线程证据不该变成存储错误: %v", err)
	}
	if report.Repaired || report.Reason != ReconcileContradictory || !strings.Contains(report.Detail, "thread-other") {
		t.Fatalf("跨线程证据的判定 = %+v", report)
	}
	if got := managedGet(t, runs, "run-cross"); got.Status != before.Status || got.Version != before.Version {
		t.Fatalf("跨线程不一致时记录被改写了: %+v", got)
	}

	// 一条从没被领取的 queued 记录，却已经有了自己提交的终态快照：结局是冻结的，状态机却
	// 不允许 queued 直接变成 succeeded，这份矛盾要原样报出来。
	newManagedRecord(t, runs, "run-queued", "thread-queued", "turn-queued")
	run, err := a.StreamManaged(ctx, agent.ManagedRunSpec{RunID: "run-queued", ThreadID: "thread-queued", TurnID: "turn-queued"}, "问题")
	if err != nil {
		t.Fatalf("启动运行: %v", err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("运行失败: %v", err)
	}
	if err := a.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("记终态快照: %v", err)
	}
	report, err = r.ReconcileRun(ctx, "run-queued")
	if !errors.Is(err, runstore.ErrInvalidTransition) {
		t.Fatalf("queued 记录带终态: err = %v, 期望状态机拒绝", err)
	}
	if report.Repaired || report.Reason != ReconcileRefused {
		t.Fatalf("判定 = %+v", report)
	}
	if got := managedGet(t, runs, "run-queued"); got.Status != runstore.StatusQueued {
		t.Fatalf("状态机拒绝之后记录仍被改写: %s", got.Status)
	}
}

// TestReconcileStaleOnPlainBoundary 覆盖"记录指了一条本运行提交的普通快照"：那条快照确实
// 存在、确实属于这条 RunID，却没有终态记录，所以修复只能报 stale，不能把"跑过一步"读成"已经
// 结束"。
func TestReconcileStaleOnPlainBoundary(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs := runstore.NewMemory()
	var calls atomic.Int64
	a := managedAgent(t, store, "只跑了一步", &calls)

	newManagedRecord(t, runs, "run-blank", "thread-blank", "turn-blank")
	claimed := managedCAS(t, runs, managedGet(t, runs, "run-blank"), func(rec *runstore.RunRecord) {
		rec.Status = runstore.StatusRunning
		rec.WorkerID = "worker-blank"
		rec.WorkerEpoch = 1
	})
	run, err := a.StreamManaged(ctx, agent.ManagedRunSpec{RunID: "run-blank", ThreadID: "thread-blank", TurnID: "turn-blank"}, "问题")
	if err != nil {
		t.Fatalf("启动运行: %v", err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("运行失败: %v", err)
	}
	boundary, found := func() (checkpoint.CheckpointMetadata, bool) {
		metadata, ok, err := store.ReadSessionHead(ctx, "thread-blank")
		if err != nil {
			t.Fatalf("读 head: %v", err)
		}
		return metadata, ok
	}()
	if !found || boundary.CompletionKind != "" {
		t.Fatalf("样本需要一条没有终态记录的边界快照: %+v", boundary)
	}

	managedCAS(t, runs, managedGet(t, runs, "run-blank"), func(rec *runstore.RunRecord) {
		rec.CompletionRef = &checkpoint.Ref{ThreadID: boundary.Ref.ThreadID, CheckpointID: boundary.Ref.CheckpointID}
	})

	r, err := NewRunReconciler(runs, store)
	if err != nil {
		t.Fatalf("NewRunReconciler: %v", err)
	}
	report, err := r.ReconcileRun(ctx, "run-blank")
	if err != nil {
		t.Fatalf("ReconcileRun: %v", err)
	}
	if report.Repaired || report.Reason != ReconcileStale {
		t.Fatalf("普通边界不是终态证据: %+v", report)
	}
	got := managedGet(t, runs, "run-blank")
	if got.Status != runstore.StatusRunning || got.Version <= claimed.Version {
		t.Fatalf("没有终态记录却推进了结局: %+v", got)
	}
	if got.CompletionRef == nil || got.CompletionRef.CheckpointID != boundary.Ref.CheckpointID {
		t.Errorf("修复之外的事实被动过: %+v", got.CompletionRef)
	}
}

// TestReconcileClearsRecordingError 处理"上一轮把终态快照的失败记在了任务记录上，这一轮快照
// 其实已经提交"：修复写完结局的同时把那条收尾错误清掉，因为证据本身说明它已经不成立了。
func TestReconcileClearsRecordingError(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs := runstore.NewMemory()
	var calls atomic.Int64
	a := managedAgent(t, store, "补记的结局", &calls)

	newManagedRecord(t, runs, "run-clear", "thread-clear", "turn-clear")
	settleWithoutRecord(t, a, runs, "run-clear", "thread-clear", "问题")
	managedCAS(t, runs, managedGet(t, runs, "run-clear"), func(rec *runstore.RunRecord) {
		rec.RecordError = &runstore.RunError{Code: runstore.ErrCodeCompletionRecord, Message: "上一轮记终态快照失败", Retryable: true}
	})
	before := managedGet(t, runs, "run-clear")
	if before.Status != runstore.StatusRunning || before.RecordError == nil {
		t.Fatalf("样本应当是 running 且带着收尾错误: %+v", before)
	}

	r, err := NewRunReconciler(runs, store)
	if err != nil {
		t.Fatalf("NewRunReconciler: %v", err)
	}
	report, err := r.ReconcileRun(ctx, "run-clear")
	if err != nil {
		t.Fatalf("ReconcileRun: %v", err)
	}
	if !report.Repaired {
		t.Fatalf("有已提交的终态快照却没能修复: %+v", report)
	}
	fixed := managedGet(t, runs, "run-clear")
	if fixed.Status != runstore.StatusSucceeded {
		t.Fatalf("终态 = %s", fixed.Status)
	}
	if fixed.RecordError != nil {
		t.Fatalf("快照已经落库，收尾错误却还留在记录上: %+v", fixed.RecordError)
	}
	if fixed.CompletionRef == nil {
		t.Error("修复没有记下它是凭哪一条终态快照写的结局")
	}
	if fixed.WorkerID != before.WorkerID || fixed.WorkerEpoch != before.WorkerEpoch {
		t.Errorf("修复改写了 worker 归属: %+v -> %+v", before, fixed)
	}
}
