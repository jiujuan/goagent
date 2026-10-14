package agent

// SM-09（ADR-0034 D6）：checkpoint 认领租期由 agent 配置。普通运行的 BeginRun、恢复的
// Claim、续租循环与每次提交用的是同一个值；默认仍是 30 秒，checkpoint 包的默认值不受影响。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// leaseRecorder 包一层真实的线程协调器，记下每次递交的租期与提交用的 fencing token。
type leaseRecorder struct {
	checkpoint.ThreadCoordinator

	mu          sync.Mutex
	begins      []time.Duration
	claims      []time.Duration
	renews      []time.Duration
	commitLease []time.Duration
	tokens      []uint64

	renewErr         error
	failAfter        int
	renewed          int
	failed           int
	commitsAfterFail int
}

func (r *leaseRecorder) BeginRun(ctx context.Context, req checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	r.mu.Lock()
	r.begins = append(r.begins, req.Lease)
	r.mu.Unlock()
	return r.ThreadCoordinator.BeginRun(ctx, req)
}

func (r *leaseRecorder) Claim(ctx context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	r.mu.Lock()
	r.claims = append(r.claims, req.Lease)
	r.mu.Unlock()
	return r.ThreadCoordinator.Claim(ctx, req)
}

// RenewClaim 让前 failAfter 次续租照常成功，之后的都失败——模拟租期丢了、线程被接管。
func (r *leaseRecorder) RenewClaim(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
	r.mu.Lock()
	r.renews = append(r.renews, lease)
	fail := r.renewErr != nil && r.renewed >= r.failAfter
	r.renewed++
	if fail {
		r.failed++
	}
	r.mu.Unlock()
	if fail {
		return checkpoint.Claim{}, r.renewErr
	}
	return r.ThreadCoordinator.RenewClaim(ctx, claim, lease)
}

func (r *leaseRecorder) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	r.mu.Lock()
	r.commitLease = append(r.commitLease, req.Lease)
	if req.Claim != nil {
		r.tokens = append(r.tokens, req.Claim.FencingToken)
	}
	if r.failed > 0 {
		r.commitsAfterFail++
	}
	r.mu.Unlock()
	return r.ThreadCoordinator.Commit(ctx, req)
}

func (r *leaseRecorder) snapshot() (begins, claims, renews, commitLeases []time.Duration, tokens []uint64, commitsAfterFail int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.begins...),
		append([]time.Duration(nil), r.claims...),
		append([]time.Duration(nil), r.renews...),
		append([]time.Duration(nil), r.commitLease...),
		append([]uint64(nil), r.tokens...),
		r.commitsAfterFail
}

func (r *leaseRecorder) renewCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.renews)
}

// leaseGate 是内部测试用的审批闸门：第一次审批前拦下工具。
type leaseGate struct{ BaseMiddleware }

func (leaseGate) BeforeTool(lc *LoopContext, c *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(c) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "需要审批"}, nil
}

func leaseToolModel() llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("收尾回答")
		}
		return mock.CallTool("call-1", "echo", `{}`)
	})
}

// leaseBlockingTool 开始运行后通知测试，等到放行或 context 取消才返回。
func leaseBlockingTool(started chan struct{}, release <-chan struct{}) tool.Tool {
	var once sync.Once
	return tool.New("echo", "阻塞直到放行或取消", func(ctx *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return "released", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
}

func leaseInstantTool() tool.Tool {
	return tool.New("echo", "立即返回", func(*tool.Context, struct{}) (string, error) {
		return "done", nil
	})
}

func leaseTextModel(text string) llm.Model {
	return mock.New("m", func(*llm.Request) *llm.Response { return mock.Text(text) })
}

// TestClaimLeaseNormalAndResume：普通运行、恢复、续租与提交都取同一个配置值；
// 不配置时保持 30 秒，而不是 checkpoint 包被改写。
func TestClaimLeaseNormalAndResume(t *testing.T) {
	ctx := context.Background()
	const lease = 7 * time.Second

	rec := &leaseRecorder{ThreadCoordinator: checkpoint.NewMemory()}
	a, err := New(
		WithModel(leaseToolModel()),
		WithTools(leaseInstantTool()),
		WithMiddleware(leaseGate{}),
		WithCheckpointer(rec),
		WithClaimLease(lease),
	)
	if err != nil {
		t.Fatal(err)
	}

	threadID := "lease-normal-resume"
	run := a.Stream(ctx, "需要审批", OnThread(threadID))
	if _, err := run.Wait(); err != nil {
		t.Fatalf("首次运行应停在审批前: %v", err)
	}
	if !run.Paused() {
		t.Fatal("没有停在暂停")
	}

	resumed, err := a.Resume(ctx, threadID, Allow("call-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Wait(); err != nil {
		t.Fatalf("恢复的运行失败: %v", err)
	}

	begins, claims, renews, commitLeases, _, commitsAfterFail := rec.snapshot()
	if len(begins) != 1 || begins[0] != lease {
		t.Fatalf("BeginRun 租期 = %v, 期望一次 %v", begins, lease)
	}
	if len(claims) != 1 || claims[0] != lease {
		t.Fatalf("Claim 租期 = %v, 期望一次 %v", claims, lease)
	}
	if len(renews) == 0 {
		t.Fatal("没有观察到续租")
	}
	for i, got := range renews {
		if got != lease {
			t.Fatalf("第 %d 次续租租期 = %v, 期望 %v", i+1, got, lease)
		}
	}
	if len(commitLeases) == 0 {
		t.Fatal("没有观察到提交")
	}
	for i, got := range commitLeases {
		if got != lease {
			t.Fatalf("第 %d 次提交租期 = %v, 期望 %v", i+1, got, lease)
		}
	}
	if commitsAfterFail != 0 {
		t.Fatalf("没有失败的续租却记下了 %d 次提交", commitsAfterFail)
	}
	if checkpoint.DefaultLease != 30*time.Second {
		t.Fatalf("checkpoint 包默认租期被改动: %v", checkpoint.DefaultLease)
	}

	// 不配置时逐个环节仍是 30 秒。
	defaults := &leaseRecorder{ThreadCoordinator: checkpoint.NewMemory()}
	plain, err := New(
		WithModel(leaseToolModel()),
		WithTools(leaseInstantTool()),
		WithMiddleware(leaseGate{}),
		WithCheckpointer(defaults),
	)
	if err != nil {
		t.Fatal(err)
	}
	first := plain.Stream(ctx, "需要审批", OnThread("lease-default"))
	if _, err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	second, err := plain.Resume(ctx, "lease-default", Allow("call-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	dBegins, dClaims, dRenews, dCommits, _, _ := defaults.snapshot()
	for _, group := range [][]time.Duration{dBegins, dClaims, dRenews, dCommits} {
		if len(group) == 0 {
			t.Fatal("默认配置下某一处租期没被观察到")
		}
		for _, got := range group {
			if got != checkpoint.DefaultLease {
				t.Fatalf("默认租期观察值 = %v, 期望 %v", got, checkpoint.DefaultLease)
			}
		}
	}

	for name, buildErr := range map[string]error{
		"zero": func() error { _, e := New(WithModel(leaseTextModel("答")), WithClaimLease(0)); return e }(),
		"negative": func() error {
			_, e := New(WithModel(leaseTextModel("答")), WithClaimLease(-time.Second))
			return e
		}(),
	} {
		if buildErr == nil {
			t.Fatalf("%s 的 claim lease 应被拒绝", name)
		}
	}
}

// TestClaimLeaseWithoutCoordinator 覆盖没有线程协调能力的存储：运行不会有写保护 guard，
// 恢复的认领与每次提交仍然只能用配置的那一个租期。
func TestClaimLeaseWithoutCoordinator(t *testing.T) {
	ctx := context.Background()
	const lease = 9 * time.Second

	mem := checkpoint.NewMemory()
	only := &durableOnlyRecorder{Durable: mem}
	a, err := New(
		WithModel(leaseToolModel()),
		WithTools(leaseInstantTool()),
		WithMiddleware(leaseGate{}),
		WithCheckpointer(only),
		WithClaimLease(lease),
	)
	if err != nil {
		t.Fatal(err)
	}

	threadID := "lease-no-coordinator"
	run := a.Stream(ctx, "需要审批", OnThread(threadID))
	if _, err := run.Wait(); err != nil {
		t.Fatalf("首次运行应停在审批前: %v", err)
	}
	if !run.Paused() {
		t.Fatal("没有停在暂停")
	}
	resumed, err := a.Resume(ctx, threadID, Allow("call-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Wait(); err != nil {
		t.Fatalf("恢复的运行失败: %v", err)
	}

	claims, commitLeases := only.snapshot()
	if len(claims) != 1 || claims[0] != lease {
		t.Fatalf("Claim 租期 = %v, 期望一次 %v", claims, lease)
	}
	if len(commitLeases) == 0 {
		t.Fatal("没有观察到提交")
	}
	for i, got := range commitLeases {
		if got != lease {
			t.Fatalf("第 %d 次提交租期 = %v, 期望 %v（无 guard 的提交也必须取配置）", i+1, got, lease)
		}
	}
}

// durableOnlyRecorder 只暴露 Durable 那一面：没有 BeginRun/RenewClaim，所以运行不会建
// writeGuard，提交走的是直接带租期的那条路。
type durableOnlyRecorder struct {
	checkpoint.Durable

	mu          sync.Mutex
	claims      []time.Duration
	commitLease []time.Duration
}

func (r *durableOnlyRecorder) Claim(ctx context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	r.mu.Lock()
	r.claims = append(r.claims, req.Lease)
	r.mu.Unlock()
	return r.Durable.Claim(ctx, req)
}

func (r *durableOnlyRecorder) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	r.mu.Lock()
	r.commitLease = append(r.commitLease, req.Lease)
	r.mu.Unlock()
	return r.Durable.Commit(ctx, req)
}

func (r *durableOnlyRecorder) snapshot() (claims, commitLeases []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.claims...), append([]time.Duration(nil), r.commitLease...)
}

// TestLongCallRenewsClaim：一次跨多个租期的工具阻塞里，续租按配置间隔继续，提交始终带着
// 最新一次的 fencing token。用 20 毫秒的小租期跑真实续租循环，不睡真实 30 秒。
func TestLongCallRenewsClaim(t *testing.T) {
	ctx := context.Background()
	rec := &leaseRecorder{ThreadCoordinator: checkpoint.NewMemory()}
	started := make(chan struct{})
	release := make(chan struct{})

	a, err := New(
		WithModel(leaseToolModel()),
		WithTools(leaseBlockingTool(started, release)),
		WithCheckpointer(rec),
		WithClaimLease(20*time.Millisecond),
		WithMaxTurns(4),
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := a.Run(ctx, "慢工具", OnThread("lease-renews"))
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("工具没有开始")
	}

	// 等到至少三次续租发生再放行：租期 20 毫秒 ⇒ 续租间隔 10 毫秒。
	deadline := time.Now().Add(5 * time.Second)
	for rec.renewCount() < 4 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	close(release)

	if err := <-done; err != nil {
		t.Fatalf("续租正常的长调用应成功: %v", err)
	}
	_, _, renews, _, tokens, commitsAfterFail := rec.snapshot()
	if len(renews) < 4 {
		t.Fatalf("长调用期间只续租 %d 次", len(renews))
	}
	if len(tokens) == 0 {
		t.Fatal("提交没带 fencing token")
	}
	for _, token := range tokens {
		if token != tokens[0] {
			t.Fatalf("一次运行里 fencing token 变了: %d vs %d", tokens[0], token)
		}
	}
	if commitsAfterFail != 0 {
		t.Fatalf("没有续租失败却记下了 %d 次提交", commitsAfterFail)
	}
}

// TestRenewFailureCancels：续租一旦失败，执行被取消，之后的提交一律发不出去——旧 token
// 不能把已经丢掉的认领继续写进历史。
func TestRenewFailureCancels(t *testing.T) {
	ctx := context.Background()
	rec := &leaseRecorder{
		ThreadCoordinator: checkpoint.NewMemory(),
		failAfter:         1,
		renewErr:          errors.New("线程已被新主人接管"),
	}
	started := make(chan struct{})
	release := make(chan struct{})

	a, err := New(
		WithModel(leaseToolModel()),
		WithTools(leaseBlockingTool(started, release)),
		WithCheckpointer(rec),
		WithClaimLease(20*time.Millisecond),
		WithMaxTurns(4),
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := a.Run(ctx, "慢工具", OnThread("lease-renew-fail"))
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("工具没有开始")
	}

	// 第二次续租会失败：等到它发生，再放行工具，运行必须带着这个错误结束。
	deadline := time.Now().Add(5 * time.Second)
	for rec.renewCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if rec.renewCount() < 2 {
		close(release)
		<-done
		t.Fatal("没有观察到第二次续租")
	}
	close(release)

	runErr := <-done
	if runErr == nil {
		t.Fatal("续租失败后运行不能算成功")
	}
	if !errors.Is(runErr, rec.renewErr) {
		t.Fatalf("运行错误丢了续租失败: %v", runErr)
	}
	if _, _, _, _, _, commitsAfterFail := rec.snapshot(); commitsAfterFail != 0 {
		t.Fatalf("续租失败后仍有 %d 次提交发出去", commitsAfterFail)
	}
}
