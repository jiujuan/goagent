package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// expiringLeaseStore hands the write guard a claim whose lease has already run
// out, without cancelling the run. The next dispatch observes a fence error
// instead of context.Canceled, which is a real mid-batch failure that leaves
// later calls without a result.
type expiringLeaseStore struct {
	*checkpoint.Memory

	mu      sync.Mutex
	commits []checkpoint.CommitRequest
	armed   atomic.Bool
	expired chan struct{}
	once    sync.Once
}

func newExpiringLeaseStore() *expiringLeaseStore {
	return &expiringLeaseStore{Memory: checkpoint.NewMemory(), expired: make(chan struct{})}
}

func (s *expiringLeaseStore) RenewClaim(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
	renewed, err := s.Memory.RenewClaim(ctx, claim, lease)
	if err != nil {
		return renewed, err
	}
	if s.armed.Load() {
		renewed.ExpiresAt = time.Now().Add(-time.Minute)
		s.once.Do(func() { close(s.expired) })
	}
	return renewed, nil
}

func (s *expiringLeaseStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	s.commits = append(s.commits, req)
	s.mu.Unlock()
	return s.Memory.Commit(ctx, req)
}

// armExpiry makes the next renewal hand out an expired claim and returns a
// channel closed when that renewal happened.
func (s *expiringLeaseStore) armExpiry() <-chan struct{} {
	s.armed.Store(true)
	return s.expired
}

func (s *expiringLeaseStore) recordedCommits() []checkpoint.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.CommitRequest(nil), s.commits...)
}

// ctxAwareReadStore reports a cancelled context as a ledger error, so a backfill
// that reads with the run's context cannot see the pending record after a
// cancellation.
type ctxAwareReadStore struct {
	checkpoint.Durable
	reads atomic.Int32
}

func (s *ctxAwareReadStore) ToolExecution(ctx context.Context, threadID, invocationID string) (checkpoint.ToolExecution, bool, error) {
	s.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return checkpoint.ToolExecution{}, false, err
	}
	return s.Durable.ToolExecution(ctx, threadID, invocationID)
}

type cancelAfterTool struct {
	agent.BaseMiddleware
	cancel context.CancelFunc
	once   sync.Once
}

func (m *cancelAfterTool) AfterTool(*agent.LoopContext, *core.ToolResult) (core.Directive, error) {
	m.once.Do(m.cancel)
	return core.Directive{}, nil
}

func failureResultText(t *testing.T, p core.Part) string {
	t.Helper()
	tr, ok := p.(core.ToolResult)
	if !ok {
		t.Fatalf("part = %#v, want a tool result", p)
	}
	var b strings.Builder
	for _, part := range tr.Content {
		if text, ok := part.(core.Text); ok {
			b.WriteString(text.Text)
		}
	}
	return b.String()
}

// failurePairing asserts the failure checkpoint ends with one tool result per
// planned call, in plan order, and returns those results.
func failurePairing(t *testing.T, head *checkpoint.Checkpoint, calls []core.ToolCall) []core.ToolResult {
	t.Helper()
	if head == nil {
		t.Fatal("failure checkpoint is missing")
	}
	if head.InFlight != nil {
		t.Fatalf("failure checkpoint still carries an in-flight batch: %+v", head.InFlight)
	}
	if head.Pending != nil || head.Pause != nil {
		t.Fatalf("failure checkpoint carries work state: pending=%+v pause=%+v", head.Pending, head.Pause)
	}
	msgs := head.State.Messages
	if len(msgs) < 2 {
		t.Fatalf("failure history has %d messages, want the batch pair", len(msgs))
	}
	issued := msgs[len(msgs)-2]
	if issued.Role != core.RoleAssistant || len(issued.ToolCalls()) != len(calls) {
		t.Fatalf("message before the results = %+v, want the assistant batch", issued)
	}
	last := msgs[len(msgs)-1]
	if last.Role != core.RoleTool || len(last.Parts) != len(calls) {
		t.Fatalf("last message = %+v, want %d tool results", last, len(calls))
	}
	out := make([]core.ToolResult, len(calls))
	for i, p := range last.Parts {
		tr, ok := p.(core.ToolResult)
		if !ok || tr.CallID != calls[i].ID {
			t.Fatalf("result[%d] = %#v, want call %q in plan order", i, p, calls[i].ID)
		}
		out[i] = tr
	}
	return out
}

func TestFailedInFlightBatchKeepsPairing(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-pairing"
	store := newExpiringLeaseStore()
	rec := newRecordingTool("rec")
	rec.onCall = func(_ *tool.Context, got recordedToolCall) {
		if got.CallID != "a" {
			return
		}
		select {
		case <-store.armExpiry():
		case <-time.After(5 * time.Second):
			t.Errorf("the write lease was never renewed after arming")
		}
	}
	calls := recoveryCalls("a", "b")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithClaimLease(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(threadID)))
	if !errors.Is(runErr, checkpoint.ErrStaleFencingToken) {
		t.Fatalf("run error = %v, want a stale fencing token", runErr)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("handlers ran %d times, want only the dispatched call", got)
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	results := failurePairing(t, head, calls)
	if results[0].IsError || failureResultText(t, results[0]) != "ok" {
		t.Fatalf("first result = %+v, want the reported success", results[0])
	}
	if !results[1].IsError || !strings.Contains(failureResultText(t, results[1]), "not executed: the run stopped before this call started") {
		t.Fatalf("second result = %+v, want the not-executed wording", results[1])
	}

	commits := store.recordedCommits()
	if len(commits) < 2 {
		t.Fatalf("commits = %d, want the in-flight write and the failure checkpoint", len(commits))
	}
	first := commits[0]
	if first.Checkpoint == nil || first.Checkpoint.InFlight == nil || len(first.Checkpoint.InFlight.Calls) != 2 {
		t.Fatalf("first commit = %+v, want the two-call in-flight batch", first.Checkpoint)
	}
	if len(first.ToolExecutions) != 2 {
		t.Fatalf("in-flight commit carries %d executions, want 2", len(first.ToolExecutions))
	}
}

func TestFailedInFlightAfterCancel(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-cancel"
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	store := &ctxAwareReadStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("a", "b")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, runErr := phase1Drain(t, a.Stream(runCtx, "go", agent.OnThread(threadID)))
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", runErr)
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	results := failurePairing(t, head, calls)
	if got := failureResultText(t, results[1]); got != "not executed: the run stopped before this call started" {
		t.Fatalf("second result text = %q, want the backfill to read the pending record", got)
	}
	if got := store.reads.Load(); got != 1 {
		t.Fatalf("ledger reads = %d, want the one backfill read", got)
	}
}

func TestFailedInFlightBackfillWithoutDurable(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-plain"
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	store := &plainSaveStore{Checkpointer: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("a", "b")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, runErr := phase1Drain(t, a.Stream(runCtx, "go", agent.OnThread(threadID)))
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", runErr)
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.InFlight != nil {
		t.Fatalf("head = %+v, want no in-flight batch", head)
	}
	last := head.State.Messages[len(head.State.Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != 1 {
		t.Fatalf("last message = %+v, want the single reported result without backfill", last)
	}
	if tr, ok := last.Parts[0].(core.ToolResult); !ok || tr.CallID != "a" {
		t.Fatalf("last result = %#v, want call a", last.Parts[0])
	}
}

func TestFailedInFlightRecoveryBackfill(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-recovery"
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("a", "b")
	seedInFlightHead(t, store, threadID, 0, calls,
		inFlightSnapshot(0, rec), []core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecutionsFor(t, threadID, 0, calls)...)

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(runCtx, threadID)
	if err != nil {
		t.Fatalf("Resume = %v, want a run", err)
	}
	_, runErr := phase1Drain(t, run)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("resumed run error = %v, want context.Canceled", runErr)
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	results := failurePairing(t, head, calls)
	if results[0].IsError {
		t.Fatalf("first recovered result = %+v, want the executed result", results[0])
	}
	if got := failureResultText(t, results[1]); got != "not executed: the run stopped before this call started" {
		t.Fatalf("second recovered result text = %q, want the not-executed wording", got)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("handlers ran %d times, want only the dispatched call", got)
	}
}

// TestFailedInFlightBackfillLeavesPendingRetryable proves the backfill text is
// only a conversation record: the pending call's ledger entry stays pending, so
// a later run that reissues the same call executes it instead of replaying the
// "not executed" wording.
func TestFailedInFlightBackfillLeavesPendingRetryable(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-retryable"
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("a", "b")
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, runErr := phase1Drain(t, a.Stream(runCtx, "go", agent.OnThread(threadID))); !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", runErr)
	}

	// The reissued call has the same ID, arguments and step as the pending one,
	// so its invocation identity is the same ledger record. In-flight writes are
	// off here, which is what lets this run read that record directly.
	first := false
	reissue, err := agent.New(
		agent.WithModel(mock.New("reissue", func(*llm.Request) *llm.Response {
			if first {
				return mock.Text("done")
			}
			first = true
			return mock.CallTool("b", "rec", `{}`)
		})),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithInFlightCheckpoint(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reissue.Run(ctx, "retry", agent.OnThread(threadID)); err != nil {
		t.Fatalf("reissued run = %v, want success", err)
	}

	invocations := rec.invocations()
	if len(invocations) != 2 || invocations[0].CallID != "a" || invocations[1].CallID != "b" {
		t.Fatalf("handlers ran %+v, want a in the failed run and b in the reissued one", invocations)
	}
	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var toolMsg *core.Message
	for i := range head.State.Messages {
		if head.State.Messages[i].Role == core.RoleTool {
			toolMsg = &head.State.Messages[i]
		}
	}
	if toolMsg == nil || len(toolMsg.Parts) != 1 {
		t.Fatalf("last tool message = %+v, want the reissued call's result", toolMsg)
	}
	tr, ok := toolMsg.Parts[0].(core.ToolResult)
	if !ok || tr.CallID != "b" || tr.IsError || failureResultText(t, toolMsg.Parts[0]) != "ok" {
		t.Fatalf("reissued result = %#v, want a fresh successful execution", toolMsg.Parts[0])
	}
}

func TestFailedInCommitLeavesRecoverableHead(t *testing.T) {
	ctx := context.Background()
	const threadID = "failed-inflight-commit"
	commitErr := errors.New("checkpoint unavailable")
	store := &failCommitStore{Durable: checkpoint.NewMemory(), failAt: 2, err: commitErr}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("a", "b")
	a, err := agent.New(
		agent.WithModel(inFlightModel(calls...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithMiddleware(&cancelAfterTool{cancel: cancel}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, runErr := phase1Drain(t, a.Stream(runCtx, "go", agent.OnThread(threadID)))
	if !errors.Is(runErr, context.Canceled) || !errors.Is(runErr, commitErr) {
		t.Fatalf("run error = %v, want the cancellation and the failed checkpoint write", runErr)
	}
	if got := store.commitAttempts(); got != 2 {
		t.Fatalf("commit attempts = %d, want the in-flight write plus the failed failure checkpoint", got)
	}

	head, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.InFlight == nil || len(head.InFlight.Calls) != 2 {
		t.Fatalf("head after the failed failure checkpoint = %+v, want the in-flight batch", head)
	}

	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatalf("Resume of the recoverable head = %v", err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("resumed run = %v, want success", err)
	}
	invocations := rec.invocations()
	if len(invocations) != 2 || invocations[0].CallID != "a" || invocations[1].CallID != "b" {
		t.Fatalf("handlers ran %+v, want call a once and call b once", invocations)
	}

	final, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if final.InFlight != nil {
		t.Fatalf("final head still carries an in-flight batch: %+v", final.InFlight)
	}
	var toolMsg *core.Message
	for i := range final.State.Messages {
		if final.State.Messages[i].Role == core.RoleTool {
			toolMsg = &final.State.Messages[i]
		}
	}
	if toolMsg == nil || len(toolMsg.Parts) != 2 {
		t.Fatalf("final tool message = %+v, want two results", toolMsg)
	}
	unknown, ok := toolMsg.Parts[0].(core.ToolResult)
	if !ok || !unknown.IsError || !strings.Contains(failureResultText(t, toolMsg.Parts[0]), "was started before recovery") {
		t.Fatalf("first recovered result = %#v, want the unknown-outcome wording", toolMsg.Parts[0])
	}
	if got := failureResultText(t, toolMsg.Parts[1]); got != "ok" {
		t.Fatalf("second recovered result = %q, want the newly executed result", got)
	}
	if got := ledgerState(t, store, threadID, 0, calls[0]); got != checkpoint.ToolUnknown {
		t.Fatalf("ledger state of a = %q, want unknown", got)
	}
	if got := ledgerState(t, store, threadID, 0, calls[1]); got != checkpoint.ToolCompleted {
		t.Fatalf("ledger state of b = %q, want completed", got)
	}
}
