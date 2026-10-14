package queue

// SM-08（ADR-0034 D5）：受管任务提交。投递的是版本化 Type/Payload，不是闭包；任务记录先
// 落成 queued，投递失败也照样把 RunID 还给调用方。明确拒收才写 failed/enqueue_failed，
// 分不出来的投递结果保持 queued + dispatch_unknown。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// captureQueue 记下收到的 Job，可设定 Enqueue 的返回错误。
type captureQueue struct {
	mu   sync.Mutex
	jobs []Job
	err  error
	// onEnqueue 在返回前运行，用来模拟"投递其实被接手了但调用方拿不到结论"。
	onEnqueue func()
}

func (q *captureQueue) Enqueue(ctx context.Context, job Job) error {
	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	q.mu.Unlock()
	if q.onEnqueue != nil {
		q.onEnqueue()
	}
	return q.err
}

func (q *captureQueue) sent() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]Job(nil), q.jobs...)
}

// managedResolver 按 AgentKey 返回不同的 agent，并记下被查了几次。
type managedResolver struct {
	agents map[string]*agent.Agent
	misses map[string]int
	mu     sync.Mutex
}

func (r *managedResolver) resolve(_ context.Context, key string) (*agent.Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a, ok := r.agents[key]; ok {
		return a, nil
	}
	if r.misses == nil {
		r.misses = map[string]int{}
	}
	r.misses[key]++
	return nil, fmt.Errorf("没有配置 agent %q", key)
}

func managedHandlers(t *testing.T, store runstore.Store, r *managedResolver, opts ...WorkerOption) (Handler, error) {
	t.Helper()
	return ManagedRunHandler(store, "worker-A", r.resolve, opts...)
}

// --- 提交 -------------------------------------------------------------------

// TestManagedRunSubmitQueuesRecord：提交只建 queued 记录并发出 Type/Payload，RunID 与
// 载荷、记录、投递回执三处一致。
func TestManagedRunSubmitQueuesRecord(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	q := &captureQueue{}

	sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
		AgentKey: "support",
		RunID:    "run-submit-1",
		ThreadID: "thread-submit-1",
		TurnID:   "turn-submit-1",
		JobID:    "job-submit-1",
		Input:    "问题一",
	})
	if err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	if sub.RunID != "run-submit-1" || sub.JobID != "job-submit-1" {
		t.Fatalf("回执 = %+v", sub)
	}

	rec := managedGet(t, store, sub.RunID)
	if rec.Status != runstore.StatusQueued || rec.Version != 1 {
		t.Fatalf("记录 = %s v%d, 期望 queued v1", rec.Status, rec.Version)
	}
	if rec.RunID != "run-submit-1" || rec.ThreadID != "thread-submit-1" ||
		rec.TurnID != "turn-submit-1" || rec.JobID != "job-submit-1" {
		t.Fatalf("记录身份不全: %+v", rec)
	}
	if rec.Error != nil || rec.Result != nil || rec.WorkerID != "" {
		t.Fatalf("排队的记录不该带结局或所有权: %+v", rec)
	}

	jobs := q.sent()
	if len(jobs) != 1 {
		t.Fatalf("投递了 %d 个 Job", len(jobs))
	}
	if jobs[0].ID != sub.JobID || jobs[0].Type != ManagedRunJobType || jobs[0].Run != nil {
		t.Fatalf("Job 形状不对: ID=%q Type=%q Run!=nil:%v", jobs[0].ID, jobs[0].Type, jobs[0].Run != nil)
	}
	var payload ManagedRunPayload
	if err := json.Unmarshal(jobs[0].Payload, &payload); err != nil {
		t.Fatalf("载荷解不开: %v", err)
	}
	if payload.Version != ManagedRunPayloadVersion || payload.AgentKey != "support" ||
		payload.RunID != sub.RunID || payload.ThreadID != "thread-submit-1" ||
		payload.TurnID != "turn-submit-1" || payload.Input != "问题一" {
		t.Fatalf("载荷内容不对: %+v", payload)
	}
	if payload.Message != nil {
		t.Fatalf("纯文本输入不该带消息: %+v", payload.Message)
	}

	// 缺省 ID 由提交方生成，调用方只关心回执。
	auto, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{AgentKey: "support", Input: "再问一次"})
	if err != nil {
		t.Fatal(err)
	}
	if auto.RunID == "" || auto.JobID == "" {
		t.Fatalf("没生成 ID: %+v", auto)
	}
	autoRec := managedGet(t, store, auto.RunID)
	if autoRec.ThreadID == "" || autoRec.Status != runstore.StatusQueued {
		t.Fatalf("自动线程没落进记录: %+v", autoRec)
	}
}

// TestManagedRunSubmitValidation：坏请求既不建记录也不投递。
func TestManagedRunSubmitValidation(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	q := &captureQueue{}

	bad := map[string]ManagedRunRequest{
		"no agent key":  {AgentKey: "", Input: "x", ThreadID: "thread-v"},
		"no input":      {AgentKey: "support", RunID: "run-v-noinput", ThreadID: "thread-v"},
		"bad run id":    {AgentKey: "support", Input: "x", RunID: "run/bad", ThreadID: "thread-v"},
		"long run id":   {AgentKey: "support", Input: "x", RunID: "run-" + strings.Repeat("x", 80), ThreadID: "thread-v"},
		"bad thread id": {AgentKey: "support", Input: "x", ThreadID: "bad thread"},
	}
	for name, req := range bad {
		_, err := EnqueueManagedAgent(ctx, q, store, req)
		if err == nil {
			t.Fatalf("%s 应被拒绝", name)
		}
		if !errors.Is(err, ErrInvalidManagedSubmission) {
			t.Fatalf("%s 的错误 = %v, 期望 ErrInvalidManagedSubmission", name, err)
		}
	}
	if jobs := q.sent(); len(jobs) != 0 {
		t.Fatalf("坏请求投递了 %d 个 Job", len(jobs))
	}
	// 唯一带合法 RunID 的那一条：记录确实没被建出来。
	if _, err := store.Get(ctx, "run-v-noinput"); !errors.Is(err, runstore.ErrNotFound) {
		t.Fatalf("坏请求仍建了记录: %v", err)
	}
}

// TestManagedDispatchFailure 分三种投递结果：没调用队列、明确拒收、分不出结论。
func TestManagedDispatchFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("submission context gone", func(t *testing.T) {
		store := runstore.NewMemory()
		q := &captureQueue{}
		canceled, cancel := context.WithCancel(ctx)
		cancel()

		sub, err := EnqueueManagedAgent(canceled, q, store, ManagedRunRequest{
			AgentKey: "support", RunID: "run-df-none", ThreadID: "thread-df-none", Input: "x",
		})
		if err == nil {
			t.Fatal("提交 context 已取消应报错")
		}
		if sub.RunID != "run-df-none" {
			t.Fatalf("RunID 仍要还给调用方: %+v", sub)
		}
		if jobs := q.sent(); len(jobs) != 0 {
			t.Fatalf("已取消的提交仍投递了 %d 个 Job", len(jobs))
		}
		if _, err := store.Get(ctx, "run-df-none"); !errors.Is(err, runstore.ErrNotFound) {
			t.Fatalf("已取消的提交不该建记录: %v", err)
		}
	})

	t.Run("refused", func(t *testing.T) {
		store := runstore.NewMemory()
		q := &captureQueue{err: fmt.Errorf("broker 满了: %w", ErrDispatchRefused)}

		sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
			AgentKey: "support", RunID: "run-df-refused", ThreadID: "thread-df-refused", JobID: "job-df", Input: "x",
		})
		if err == nil {
			t.Fatal("明确拒收应报错")
		}
		if !errors.Is(err, ErrDispatchRefused) {
			t.Fatalf("错误丢了拒收判定: %v", err)
		}
		rec := managedGet(t, store, sub.RunID)
		if rec.Status != runstore.StatusFailed || rec.Error == nil || rec.Error.Code != runstore.ErrCodeEnqueueFailed {
			t.Fatalf("明确拒收应落成 failed/enqueue_failed: %+v", rec)
		}
		if rec.FinishReason != runstore.FinishReasonFailed {
			t.Fatalf("终态理由 = %q", rec.FinishReason)
		}
	})

	t.Run("outcome unknown", func(t *testing.T) {
		store := runstore.NewMemory()
		q := &captureQueue{err: errors.New("连接被重置")}

		sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
			AgentKey: "support", RunID: "run-df-unknown", ThreadID: "thread-df-unknown", JobID: "job-df", Input: "x",
		})
		if err == nil {
			t.Fatal("投递出错应报错")
		}
		if sub.RunID != "run-df-unknown" || sub.JobID != "job-df" {
			t.Fatalf("拿不到结论也要把身份还给调用方: %+v", sub)
		}
		rec := managedGet(t, store, sub.RunID)
		if rec.Status != runstore.StatusQueued {
			t.Fatalf("不确定的投递结果不能改状态: %s", rec.Status)
		}
		if rec.Error == nil || rec.Error.Code != runstore.ErrCodeDispatchUnknown || !rec.Error.Retryable {
			t.Fatalf("应登记 dispatch_unknown 且可重试: %+v", rec.Error)
		}

		// 用同一个回执重新投递：只重发同一条 Job，不新建记录。
		good := &captureQueue{}
		after, err := RetryDispatch(ctx, good, store, sub)
		if err != nil {
			t.Fatalf("重投失败: %v", err)
		}
		if after.RunID != sub.RunID {
			t.Fatalf("重投换了 RunID: %+v", after)
		}
		jobs := good.sent()
		if len(jobs) != 1 || jobs[0].ID != sub.JobID || string(jobs[0].Payload) == "" {
			t.Fatalf("重投的 Job 不对: %+v", jobs)
		}
		first := q.sent()
		if len(first) != 1 {
			t.Fatalf("第一次投递发出了 %d 个 Job", len(first))
		}
		if string(first[0].Payload) != string(jobs[0].Payload) || first[0].Type != jobs[0].Type {
			t.Fatal("重投必须逐字复用同一条载荷")
		}
		if rec := managedGet(t, store, sub.RunID); rec.Status != runstore.StatusQueued {
			t.Fatalf("重投把记录改成了 %s", rec.Status)
		}
	})

	t.Run("unknown outcome but already taken", func(t *testing.T) {
		// 队列返回错误，但 Job 其实被接手跑完了：不能把它登记成没投出去。
		store := runstore.NewMemory()
		cps := checkpoint.NewMemory()
		created := managedCreate(t, store, managedRecord("run-df-taken", "thread-df-taken", "turn-1"))
		var calls atomic.Int64
		w := managedWorker(t, managedAgent(t, cps, "其实跑完了", &calls), store, "worker-A")

		takeErr := errors.New("回执丢失")
		q := &captureQueue{err: takeErr}
		q.onEnqueue = func() {
			if _, err := w.Execute(ctx, created.RunID, "其实跑完了"); err != nil {
				t.Errorf("worker 执行失败: %v", err)
			}
		}
		sub, err := EnqueueManagedAgent(ctx, q, store, ManagedRunRequest{
			AgentKey: "support", RunID: created.RunID, ThreadID: created.ThreadID,
			TurnID: created.TurnID, JobID: created.JobID, Input: "其实跑完了",
		})
		if err == nil {
			t.Fatal("队列报错仍要返回错误")
		}
		if !errors.Is(err, takeErr) {
			t.Fatalf("返回的错误丢了投递失败: %v", err)
		}
		if errors.Is(err, runstore.ErrInvalidTransition) || errors.Is(err, runstore.ErrConflict) {
			t.Fatalf("已被接手的记录不该再被动一次，多出的写错误会盖住真实结局: %v", err)
		}
		if sub.RunID != created.RunID {
			t.Fatalf("回执 = %+v", sub)
		}
		rec := managedGet(t, store, sub.RunID)
		if rec.Status != runstore.StatusSucceeded {
			t.Fatalf("已被接手运行的记录被改成了 %s/%+v", rec.Status, rec.Error)
		}
		if rec.Error != nil {
			t.Fatalf("成功的记录不该带错误: %+v", rec.Error)
		}
	})

	t.Run("re-dispatch only a queued run", func(t *testing.T) {
		store := runstore.NewMemory()
		// 第一次投递拿不到结论，回执留在这里；期间 worker 已经把运行领走了。
		taken := &captureQueue{err: errors.New("回执丢失")}
		sub, err := EnqueueManagedAgent(ctx, taken, store, ManagedRunRequest{
			AgentKey: "support", RunID: "run-df-running", ThreadID: "thread-df-running",
			TurnID: "turn-1", JobID: "job-df-running", Input: "x",
		})
		if err == nil {
			t.Fatal("投递出错应报错")
		}
		queued := managedGet(t, store, sub.RunID)
		managedCAS(t, store, queued, func(r *runstore.RunRecord) {
			r.Status = runstore.StatusRunning
			r.WorkerID, r.WorkerEpoch = "worker-A", 1
		})

		good := &captureQueue{}
		rec, err := RetryDispatch(ctx, good, store, sub)
		if !errors.Is(err, ErrDispatchNotQueued) {
			t.Fatalf("重投已被领取的记录 = %v, 期望 ErrDispatchNotQueued", err)
		}
		if rec.Status != runstore.StatusRunning {
			t.Fatalf("应把当前记录带回来: %s", rec.Status)
		}
		if jobs := good.sent(); len(jobs) != 0 {
			t.Fatalf("已被领取的运行仍被重投了 %d 次", len(jobs))
		}
	})
}

// --- worker 侧的 Handler ----------------------------------------------------

// TestUnknownPayloadVersion：版本不认识或载荷解不开时，Handler 只能报错，不能领取运行。
func TestUnknownPayloadVersion(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	cps := checkpoint.NewMemory()
	var calls atomic.Int64
	resolver := &managedResolver{agents: map[string]*agent.Agent{
		"support": managedAgent(t, cps, "不该执行", &calls),
	}}
	handler, err := managedHandlers(t, store, resolver)
	if err != nil {
		t.Fatal(err)
	}

	created := managedCreate(t, store, managedRecord("run-ver", "thread-ver", "turn-1"))
	before := managedGet(t, store, created.RunID)

	futures := []ManagedRunPayload{
		{Version: ManagedRunPayloadVersion + 1, AgentKey: "support", RunID: created.RunID, ThreadID: created.ThreadID, Input: "x"},
		{Version: 0, AgentKey: "support", RunID: created.RunID, ThreadID: created.ThreadID, Input: "x"},
	}
	for _, p := range futures {
		data, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler(ctx, data); err == nil {
			t.Fatalf("载荷版本 %d 应被拒绝", p.Version)
		}
	}
	if err := handler(ctx, []byte("not json")); err == nil {
		t.Fatal("坏载荷应被拒绝")
	}
	// 缺 RunID / 缺 AgentKey 的载荷同样不能启动运行。
	for _, p := range []ManagedRunPayload{
		{Version: ManagedRunPayloadVersion, AgentKey: "support", ThreadID: created.ThreadID, Input: "x"},
		{Version: ManagedRunPayloadVersion, RunID: created.RunID, ThreadID: created.ThreadID, Input: "x"},
		{Version: ManagedRunPayloadVersion, AgentKey: "support", RunID: created.RunID, ThreadID: created.ThreadID},
	} {
		data, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler(ctx, data); err == nil {
			t.Fatalf("缺字段的载荷 %+v 应被拒绝", p)
		}
	}

	if calls.Load() != 0 {
		t.Fatalf("坏载荷仍调用了 %d 次模型", calls.Load())
	}
	if after := managedGet(t, store, created.RunID); after.Version != before.Version || after.Status != runstore.StatusQueued {
		t.Fatalf("坏载荷改动了记录: %s v%d", after.Status, after.Version)
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("坏载荷写出了 %d 条 checkpoint", count)
	}
}

// TestManagedRunHandlerExecutesRecord：Handler 按 AgentKey 取配置、领取并执行记录，
// 回执里的身份与 checkpoint、任务记录一致。
func TestManagedRunHandlerExecutesRecord(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	supportStore := checkpoint.NewMemory()
	otherStore := checkpoint.NewMemory()
	var supportCalls, otherCalls atomic.Int64
	resolver := &managedResolver{agents: map[string]*agent.Agent{
		"support": managedAgent(t, supportStore, "support 的回答", &supportCalls),
		"other":   managedAgent(t, otherStore, "other 的回答", &otherCalls),
	}}
	handler, err := managedHandlers(t, store, resolver)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(ManagedRunPayload{
		Version:  ManagedRunPayloadVersion,
		AgentKey: "support",
		RunID:    "run-handler-1",
		ThreadID: "thread-handler-1",
		TurnID:   "turn-handler-1",
		Input:    "问题",
	})
	if err != nil {
		t.Fatal(err)
	}
	managedCreate(t, store, managedRecord("run-handler-1", "thread-handler-1", "turn-handler-1"))

	if err := handler(ctx, payload); err != nil {
		t.Fatalf("Handler 执行失败: %v", err)
	}
	rec := managedGet(t, store, "run-handler-1")
	if rec.Status != runstore.StatusSucceeded || rec.Result == nil || rec.Result.Message.Text() != "support 的回答" {
		t.Fatalf("记录 = %s %+v", rec.Status, rec.Result)
	}
	if rec.WorkerID != "worker-A" || rec.WorkerEpoch != 1 {
		t.Fatalf("领取信息 = %q epoch %d", rec.WorkerID, rec.WorkerEpoch)
	}
	if supportCalls.Load() != 1 || otherCalls.Load() != 0 {
		t.Fatalf("模型调用 support=%d other=%d", supportCalls.Load(), otherCalls.Load())
	}
	if count := terminalCount(t, supportStore, "thread-handler-1"); count != 1 {
		t.Fatalf("终态 checkpoint %d 条", count)
	}

	// 未知 AgentKey：报错并保留排队状态，等下一次投递。
	missing, err := json.Marshal(ManagedRunPayload{
		Version: ManagedRunPayloadVersion, AgentKey: "nope", RunID: "run-handler-2",
		ThreadID: "thread-handler-2", Input: "问题",
	})
	if err != nil {
		t.Fatal(err)
	}
	managedCreate(t, store, managedRecord("run-handler-2", "thread-handler-2", "turn-2"))
	if err := handler(ctx, missing); err == nil {
		t.Fatal("取不到 agent 配置应报错")
	}
	if rec := managedGet(t, store, "run-handler-2"); rec.Status != runstore.StatusQueued || rec.WorkerID != "" {
		t.Fatalf("取不到配置却动了记录: %s worker=%q", rec.Status, rec.WorkerID)
	}
	if resolver.misses["nope"] != 1 {
		t.Fatalf("resolver 调用次数 = %d", resolver.misses["nope"])
	}
}

// TestManagedRunHandlerRunsMessage 证明载荷里的消息能原样进入运行，不只是纯文本。
func TestManagedRunHandlerRunsMessage(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	cps := checkpoint.NewMemory()
	var calls atomic.Int64
	resolver := &managedResolver{agents: map[string]*agent.Agent{
		"vision": managedAgent(t, cps, "看到了", &calls),
	}}
	handler, err := managedHandlers(t, store, resolver)
	if err != nil {
		t.Fatal(err)
	}

	message := core.Message{Role: core.RoleUser, Parts: []core.Part{
		core.Text{Text: "图里是什么"},
		core.Image{MIME: "image/png", Data: []byte("PNGLIKE")},
	}}
	payload, err := json.Marshal(ManagedRunPayload{
		Version: ManagedRunPayloadVersion, AgentKey: "vision", RunID: "run-handler-msg",
		ThreadID: "thread-handler-msg", TurnID: "turn-handler-msg", Message: &message,
	})
	if err != nil {
		t.Fatal(err)
	}
	managedCreate(t, store, managedRecord("run-handler-msg", "thread-handler-msg", "turn-handler-msg"))
	if err := handler(ctx, payload); err != nil {
		t.Fatalf("Handler 执行失败: %v", err)
	}

	rec := managedGet(t, store, "run-handler-msg")
	if rec.Status != runstore.StatusSucceeded {
		t.Fatalf("记录 = %s %+v", rec.Status, rec.Error)
	}
	boundary, err := cps.Load(ctx, rec.LatestCheckpointRef.ThreadID, rec.LatestCheckpointRef.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	var started core.Message
	for _, m := range boundary.State.Messages {
		if m.Role == core.RoleUser {
			started = m
		}
	}
	if len(started.Parts) != 2 {
		t.Fatalf("进入运行的用户消息不对: %+v", started)
	}
	if _, ok := started.Parts[1].(core.Image); !ok {
		t.Fatalf("图片段没传到运行里: %T", started.Parts[1])
	}
}

// TestDuplicateDeliveryNoSideEffects：至少一次投递重复送到同一条载荷时，第二次既不调模型
// 也不动记录，并且照样以成功返回让队列确认消息。
func TestDuplicateDeliveryNoSideEffects(t *testing.T) {
	ctx := context.Background()
	store := runstore.NewMemory()
	cps := checkpoint.NewMemory()
	var calls atomic.Int64
	resolver := &managedResolver{agents: map[string]*agent.Agent{
		"support": managedAgent(t, cps, "只跑一次", &calls),
	}}
	handler, err := managedHandlers(t, store, resolver)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(ManagedRunPayload{
		Version: ManagedRunPayloadVersion, AgentKey: "support", RunID: "run-dup",
		ThreadID: "thread-dup", TurnID: "turn-dup", Input: "问题",
	})
	if err != nil {
		t.Fatal(err)
	}
	managedCreate(t, store, managedRecord("run-dup", "thread-dup", "turn-dup"))

	if err := handler(ctx, payload); err != nil {
		t.Fatalf("第一次投递: %v", err)
	}
	settled := managedGet(t, store, "run-dup")
	commits := historyCount(t, cps, "thread-dup")
	if err := handler(ctx, payload); err != nil {
		t.Fatalf("重复投递应让队列能确认消息, got %v", err)
	}

	if calls.Load() != 1 {
		t.Fatalf("重复投递调用了 %d 次模型", calls.Load())
	}
	after := managedGet(t, store, "run-dup")
	if after.Version != settled.Version || after.Status != runstore.StatusSucceeded {
		t.Fatalf("重复投递改动了记录: v%d %s", after.Version, after.Status)
	}
	if count := historyCount(t, cps, "thread-dup"); count != commits {
		t.Fatalf("重复投递又写了 %d 条 checkpoint", count-commits)
	}
	if count := terminalCount(t, cps, "thread-dup"); count != 1 {
		t.Fatalf("重复投递写出了 %d 条终态 checkpoint", count)
	}
}

// TestManagedRunHandlerConstruction 检查 Handler 侧的依赖边界：缺 store、缺 worker id、
// 缺 resolver 都不能建出一个会静默丢消息的 Handler。
func TestManagedRunHandlerConstruction(t *testing.T) {
	store := runstore.NewMemory()
	resolver := &managedResolver{agents: map[string]*agent.Agent{}}

	if _, err := ManagedRunHandler(nil, "worker-A", resolver.resolve); err == nil {
		t.Fatal("没有 store 应构造失败")
	}
	if _, err := ManagedRunHandler(store, "", resolver.resolve); err == nil {
		t.Fatal("没有 worker id 应构造失败")
	}
	if _, err := ManagedRunHandler(store, "worker-A", nil); err == nil {
		t.Fatal("没有 resolver 应构造失败")
	}
	handler, err := ManagedRunHandler(store, "worker-A", resolver.resolve)
	if err != nil {
		t.Fatal(err)
	}
	if handler == nil {
		t.Fatal("Handler 为空")
	}
}

// historyCount 数一条线程上的全部快照。
func historyCount(t *testing.T, store checkpoint.Checkpointer, threadID string) int {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return len(hist)
}

// --- SM-13 场景 E：受管路径遇到只有四个方法的外部存储 -------------------------

// plainCapabilityStore 只转发 checkpoint.Checkpointer 的四个方法：外部接进来的最基础存储
// 就是这个形状，能力缺失必须在构造期或写的那一刻说清楚，不能悄悄降级。
type plainCapabilityStore struct {
	inner checkpoint.Checkpointer
}

func (s *plainCapabilityStore) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	return s.inner.Save(ctx, cp)
}

func (s *plainCapabilityStore) Load(ctx context.Context, threadID, checkpointID string) (*checkpoint.Checkpoint, error) {
	return s.inner.Load(ctx, threadID, checkpointID)
}

func (s *plainCapabilityStore) Latest(ctx context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	return s.inner.Latest(ctx, threadID)
}

func (s *plainCapabilityStore) History(ctx context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	return s.inner.History(ctx, threadID)
}

var _ checkpoint.Checkpointer = (*plainCapabilityStore)(nil)

// TestManagedRunReportsMissingCheckpointCapabilities 把受管运行压在只有四方法的存储上跑：
// 执行本身照常成功，任务记录写终态，但"补一条终态快照"这件事明确失败并单独记账；修复器在
// 构造时就拒绝，因为查证据的能力根本没有。重复投递依旧无害。
func TestManagedRunReportsMissingCheckpointCapabilities(t *testing.T) {
	ctx := context.Background()
	plain := &plainCapabilityStore{inner: checkpoint.NewMemory()}
	store := runstore.NewMemory()
	var calls atomic.Int64
	w := managedWorker(t, managedAgent(t, plain, "基础存储的答案", &calls), store, "worker-plain",
		WithCancelPollInterval(5*time.Millisecond))
	r := managedCreate(t, store, managedRecord("managed-plain", "thread-plain", "turn-plain"))

	out, err := w.Execute(ctx, r.RunID, "问题")
	if !out.Executed || calls.Load() != 1 {
		t.Fatalf("基础存储上执行没跑起来: executed=%v calls=%d", out.Executed, calls.Load())
	}
	if !errors.Is(out.CompletionError, checkpoint.ErrDurableUnavailable) {
		t.Fatalf("终态快照应报能力缺失: %v", out.CompletionError)
	}
	if out.RunError != nil {
		t.Fatalf("执行不该失败: %v", out.RunError)
	}
	if err == nil || !errors.Is(err, checkpoint.ErrDurableUnavailable) {
		t.Fatalf("返回错误应带上能力缺失: %v", err)
	}
	if out.CompletionRef != nil {
		t.Fatalf("没有能力却报出了终态引用: %+v", out.CompletionRef)
	}
	if out.Record.Status != runstore.StatusSucceeded || out.Record.Result == nil {
		t.Fatalf("任务记录没写下执行结果: %s %+v", out.Record.Status, out.Record.Result)
	}
	if out.Record.RecordError == nil || out.Record.RecordError.Code != runstore.ErrCodeCompletionRecord ||
		!out.Record.RecordError.Retryable {
		t.Fatalf("记账错误没单独报出: %+v", out.Record.RecordError)
	}

	// 修复器需要摘要能力：缺了就在构造期拒绝，不留"跑到一半才发现"的坑。
	if _, err := NewRunReconciler(store, plain); !errors.Is(err, checkpoint.ErrCapabilityUnavailable) {
		t.Fatalf("缺能力的修复器应被拒: %v", err)
	}

	// 记录已终结：同一份投递再来一次是无害的重复，不会第二次调模型。
	second, err := w.Execute(ctx, r.RunID, "问题")
	if err != nil || !second.Duplicate || second.Executed {
		t.Fatalf("重复投递应只报重复: duplicate=%v executed=%v err=%v", second.Duplicate, second.Executed, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("重复投递后模型调用 %d 次", calls.Load())
	}
}
