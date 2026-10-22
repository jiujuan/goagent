package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func failureCalls(ids ...string) []core.ToolCall {
	calls := make([]core.ToolCall, len(ids))
	for i, id := range ids {
		calls[i] = core.ToolCall{ID: id, Name: "rec", Args: json.RawMessage(`{}`)}
	}
	return calls
}

// ledgerSpy counts ClaimTool calls, which backfill must never make: settling a
// failed batch reads the ledger, it does not start work.
type ledgerSpy struct {
	checkpoint.Durable
	mu     sync.Mutex
	claims int
}

func (s *ledgerSpy) ClaimTool(ctx context.Context, req checkpoint.ToolClaimRequest) (checkpoint.ToolExecution, error) {
	s.mu.Lock()
	s.claims++
	s.mu.Unlock()
	return s.Durable.ClaimTool(ctx, req)
}

func (s *ledgerSpy) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claims
}

// failingLedger reports every ledger read as failed.
type failingLedger struct {
	checkpoint.Durable
	err error
}

func (s failingLedger) ToolExecution(context.Context, string, string) (checkpoint.ToolExecution, bool, error) {
	return checkpoint.ToolExecution{}, false, s.err
}

func failureRunContext(store checkpoint.Durable, threadID string) *RunContext {
	return &RunContext{Context: context.Background(), ThreadID: threadID, Store: store}
}

func failureResult(call core.ToolCall, text string) core.ToolResult {
	return core.ToolResult{
		CallID:  call.ID,
		Name:    call.Name,
		Content: []core.Part{core.Text{Text: text}},
	}
}

func failureCompletedExecution(threadID string, step int, call core.ToolCall, text string) checkpoint.ToolExecution {
	invocationID, idempotencyKey := toolInvocation(threadID, step, call)
	return checkpoint.ToolExecution{
		InvocationID:   invocationID,
		CallID:         call.ID,
		Tool:           call.Name,
		IdempotencyKey: idempotencyKey,
		Attempt:        1,
		State:          checkpoint.ToolCompleted,
		Result:         &core.Message{Role: core.RoleTool, Parts: []core.Part{failureResult(call, text)}},
	}
}

func seedFailureLedger(t *testing.T, store checkpoint.Durable, threadID string, executions ...checkpoint.ToolExecution) {
	t.Helper()
	if _, err := store.Commit(context.Background(), checkpoint.CommitRequest{
		Checkpoint:     &checkpoint.Checkpoint{ID: core.NewID("cp"), ThreadID: threadID},
		ToolExecutions: executions,
	}); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
}

func failureClaim(t *testing.T, store checkpoint.Durable, threadID string, step int, call core.ToolCall) checkpoint.ToolExecution {
	t.Helper()
	invocationID, idempotencyKey := toolInvocation(threadID, step, call)
	ex, err := store.ClaimTool(context.Background(), checkpoint.ToolClaimRequest{
		ThreadID:       threadID,
		InvocationID:   invocationID,
		CallID:         call.ID,
		Tool:           call.Name,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("claim tool: %v", err)
	}
	return ex
}

func failureText(t *testing.T, p core.Part) string {
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

func TestFailedInFlightBackfillUsesLedger(t *testing.T) {
	const threadID = "failed-backfill-claimed"
	step := 0
	calls := failureCalls("a", "b")
	mem := checkpoint.NewMemory()
	store := &ledgerSpy{Durable: mem}
	claimed := failureClaim(t, mem, threadID, step, calls[1])
	if claimed.State != checkpoint.ToolClaimed {
		t.Fatalf("seeded state = %q, want claimed", claimed.State)
	}
	reported := failureResult(calls[0], "ok")

	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls, []core.Part{reported})
	if len(out) != 2 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	missing, ok := out[1].(core.ToolResult)
	if !ok || !missing.IsError {
		t.Fatalf("missing result = %#v, want an error result", out[1])
	}
	text := failureText(t, out[1])
	if !strings.Contains(text, "was started before recovery") {
		t.Fatalf("missing result text = %q, want the unknown-outcome wording", text)
	}
	if !strings.Contains(text, claimed.IdempotencyKey) {
		t.Fatalf("missing result text = %q, want idempotency key %q", text, claimed.IdempotencyKey)
	}
	if got := store.claimCount(); got != 0 {
		t.Fatalf("backfill claimed %d tools, want 0", got)
	}
	invocationID, _ := toolInvocation(threadID, step, calls[1])
	ex, found, err := mem.ToolExecution(context.Background(), threadID, invocationID)
	if err != nil || !found || ex.State != checkpoint.ToolClaimed {
		t.Fatalf("ledger after backfill = (%+v, found=%v, err=%v), want claimed", ex, found, err)
	}
}

func TestFailedInFlightBackfillReplaysCompleted(t *testing.T) {
	const threadID = "failed-backfill-completed"
	step := 0
	calls := failureCalls("a", "b")
	mem := checkpoint.NewMemory()
	store := &ledgerSpy{Durable: mem}
	seedFailureLedger(t, mem, threadID, failureCompletedExecution(threadID, step, calls[1], "stored result"))

	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls,
		[]core.Part{failureResult(calls[0], "ok")})
	if len(out) != 2 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	replayed, ok := out[1].(core.ToolResult)
	if !ok || replayed.IsError {
		t.Fatalf("replayed result = %#v, want the stored success", out[1])
	}
	if got := failureText(t, out[1]); got != "stored result" {
		t.Fatalf("replayed text = %q, want the stored result", got)
	}
	if got := store.claimCount(); got != 0 {
		t.Fatalf("backfill claimed %d tools, want 0", got)
	}
	invocationID, _ := toolInvocation(threadID, step, calls[1])
	ex, found, err := mem.ToolExecution(context.Background(), threadID, invocationID)
	if err != nil || !found || ex.State != checkpoint.ToolCompleted {
		t.Fatalf("ledger after backfill = (%+v, found=%v, err=%v), want completed", ex, found, err)
	}
}

func TestFailedInFlightBackfillPendingIsNotExecuted(t *testing.T) {
	const threadID = "failed-backfill-pending"
	step := 0
	calls := failureCalls("a", "b")
	mem := checkpoint.NewMemory()
	store := &ledgerSpy{Durable: mem}
	seedFailureLedger(t, mem, threadID, pendingExecutions(threadID, step, calls[1:])...)

	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls,
		[]core.Part{failureResult(calls[0], "ok")})
	if len(out) != 2 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	missing, ok := out[1].(core.ToolResult)
	if !ok || !missing.IsError {
		t.Fatalf("missing result = %#v, want an error result", out[1])
	}
	if got := failureText(t, out[1]); got != "not executed: the run stopped before this call started" {
		t.Fatalf("missing result text = %q, want the not-executed wording", got)
	}
	if got := store.claimCount(); got != 0 {
		t.Fatalf("backfill claimed %d tools, want 0", got)
	}
	invocationID, _ := toolInvocation(threadID, step, calls[1])
	ex, found, err := mem.ToolExecution(context.Background(), threadID, invocationID)
	if err != nil || !found || ex.State != checkpoint.ToolPending {
		t.Fatalf("ledger after backfill = (%+v, found=%v, err=%v), want pending", ex, found, err)
	}
}

func TestFailedInFlightBackfillKeepsCallOrder(t *testing.T) {
	const threadID = "failed-backfill-order"
	step := 0
	calls := failureCalls("a", "b", "c")
	mem := checkpoint.NewMemory()
	store := &ledgerSpy{Durable: mem}
	executions := append([]checkpoint.ToolExecution{
		failureCompletedExecution(threadID, step, calls[1], "stored b"),
	}, pendingExecutions(threadID, step, calls[2:])...)
	seedFailureLedger(t, mem, threadID, executions...)

	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls,
		[]core.Part{failureResult(calls[0], "reported a")})
	if len(out) != 3 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	want := []string{"reported a", "stored b", "not executed: the run stopped before this call started"}
	for i, p := range out {
		tr, ok := p.(core.ToolResult)
		if !ok || tr.CallID != calls[i].ID {
			t.Fatalf("result[%d] = %#v, want call %q in plan order", i, p, calls[i].ID)
		}
		if got := failureText(t, p); got != want[i] {
			t.Fatalf("result[%d] text = %q, want %q", i, got, want[i])
		}
	}
}

func TestFailedInFlightBackfillOrdersAroundReportedResults(t *testing.T) {
	const threadID = "failed-backfill-late-report"
	step := 0
	calls := failureCalls("a", "b", "c")
	mem := checkpoint.NewMemory()
	store := &ledgerSpy{Durable: mem}
	executions := append([]checkpoint.ToolExecution{
		failureCompletedExecution(threadID, step, calls[0], "stored a"),
	}, pendingExecutions(threadID, step, calls[1:2])...)
	seedFailureLedger(t, mem, threadID, executions...)

	// Only the last call was reported, so a backfill that emits reported
	// results first would move c ahead of a and b.
	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls,
		[]core.Part{failureResult(calls[2], "reported c")})
	if len(out) != 3 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	want := []string{"stored a", "not executed: the run stopped before this call started", "reported c"}
	for i, p := range out {
		tr, ok := p.(core.ToolResult)
		if !ok || tr.CallID != calls[i].ID {
			t.Fatalf("result[%d] = %#v, want call %q in plan order", i, p, calls[i].ID)
		}
		if got := failureText(t, p); got != want[i] {
			t.Fatalf("result[%d] text = %q, want %q", i, got, want[i])
		}
	}
}

func TestFailedInFlightLedgerReadFailure(t *testing.T) {
	const threadID = "failed-backfill-broken-ledger"
	step := 0
	calls := failureCalls("a", "b", "c")
	ledgerErr := errors.New("ledger unavailable")
	store := &ledgerSpy{Durable: failingLedger{Durable: checkpoint.NewMemory(), err: ledgerErr}}

	out := (&AgentLoop{}).completeFailedBatch(failureRunContext(store, threadID), step, calls,
		[]core.Part{failureResult(calls[0], "ok")})
	if len(out) != 3 {
		t.Fatalf("results = %d, want one per call", len(out))
	}
	for i := 1; i < len(out); i++ {
		tr, ok := out[i].(core.ToolResult)
		if !ok || !tr.IsError {
			t.Fatalf("result[%d] = %#v, want an error result", i, out[i])
		}
		want := "not executed: the tool ledger could not be read: " + ledgerErr.Error()
		if got := failureText(t, out[i]); got != want {
			t.Fatalf("result[%d] text = %q, want %q", i, got, want)
		}
	}
	if got := store.claimCount(); got != 0 {
		t.Fatalf("backfill claimed %d tools, want 0", got)
	}
}
