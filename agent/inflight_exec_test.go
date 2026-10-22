package agent_test

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
	"github.com/jiujuan/goagent/tool"
)

func recoveryCalls(ids ...string) []core.ToolCall {
	calls := make([]core.ToolCall, len(ids))
	for i, id := range ids {
		calls[i] = core.ToolCall{ID: id, Name: "rec", Args: json.RawMessage(`{}`)}
	}
	return calls
}

func claimedExecution(t *testing.T, threadID string, step int, call core.ToolCall) checkpoint.ToolExecution {
	t.Helper()
	ex := pendingExecution(t, threadID, step, call)
	ex.Attempt = 1
	ex.State = checkpoint.ToolClaimed
	return ex
}

func pendingExecutionsFor(t *testing.T, threadID string, step int, calls []core.ToolCall) []checkpoint.ToolExecution {
	t.Helper()
	out := make([]checkpoint.ToolExecution, len(calls))
	for i, c := range calls {
		out[i] = pendingExecution(t, threadID, step, c)
	}
	return out
}

// ledgerState reads one invocation's recorded state; the call must exist.
func ledgerState(t *testing.T, durable checkpoint.Durable, threadID string, step int, call core.ToolCall) checkpoint.ToolExecutionState {
	t.Helper()
	invocationID, _ := phase2Invocation(threadID, step, call)
	ex, found, err := durable.ToolExecution(context.Background(), threadID, invocationID)
	if err != nil {
		t.Fatalf("read ledger %s: %v", invocationID, err)
	}
	if !found {
		t.Fatalf("ledger has no record for %s", invocationID)
	}
	return ex.State
}

// finalTextModel records every request and answers with one final text.
func finalTextModel(text string) (*mock.Model, func() []*llm.Request) {
	var mu sync.Mutex
	var reqs []*llm.Request
	model := mock.New("recovery", func(req *llm.Request) *llm.Response {
		mu.Lock()
		reqs = append(reqs, req)
		mu.Unlock()
		return mock.Text(text)
	})
	return model, func() []*llm.Request {
		mu.Lock()
		defer mu.Unlock()
		return append([]*llm.Request(nil), reqs...)
	}
}

// afterToolCounter counts AfterTool passes, which a replayed result must skip.
type afterToolCounter struct {
	agent.BaseMiddleware
	mu sync.Mutex
	n  int
}

func (m *afterToolCounter) AfterTool(*agent.LoopContext, *core.ToolResult) (core.Directive, error) {
	m.mu.Lock()
	m.n++
	m.mu.Unlock()
	return core.Directive{}, nil
}

func (m *afterToolCounter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

// recoveryHookSpy counts the execution-time gates a recovered batch must not
// re-enter.
type recoveryHookSpy struct {
	agent.BaseMiddleware
	mu     sync.Mutex
	policy int
	before int
}

func (m *recoveryHookSpy) CheckToolPolicy(*agent.LoopContext, []core.ToolCall) error {
	m.mu.Lock()
	m.policy++
	m.mu.Unlock()
	return nil
}

func (m *recoveryHookSpy) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	m.mu.Lock()
	m.before++
	m.mu.Unlock()
	return core.Directive{}, nil
}

func (m *recoveryHookSpy) counts() (policy, before int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policy, m.before
}

func newRecoveryAgent(t *testing.T, store checkpoint.Durable, model llm.Model, tools ...tool.Tool) *agent.Agent {
	t.Helper()
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(tools...),
		agent.WithCheckpointer(store),
		// Serial execution makes the handler-invocation order itself assertable;
		// the batch's result order is enforced by the loop either way.
		agent.WithToolExecution(agent.ToolSequential),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// recoveryCheckpoint finds the commit that settled the in-flight batch: it
// belongs to the batch's step and no longer carries an in-flight head.
func recoveryCheckpoint(t *testing.T, store checkpoint.Checkpointer, threadID string, step int) *checkpoint.Checkpoint {
	t.Helper()
	hist, err := store.History(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cp := range hist {
		if cp.Step == step && cp.InFlight == nil {
			return cp
		}
	}
	t.Fatalf("no checkpoint settled the in-flight batch at step %d: %+v", step, hist)
	return nil
}

func TestInFlightRecoveryExecutesPending(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-pending"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecutionsFor(t, threadID, 0, calls)...)

	model, requests := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var steps []int
	var recovered *core.ToolBatchRecovered
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.TurnStarted:
			steps = append(steps, e.Step)
		case core.ToolBatchRecovered:
			ev := e
			recovered = &ev
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	got := rec.invocations()
	if len(got) != 2 || got[0].CallID != "c1" || got[1].CallID != "c2" {
		t.Fatalf("handler invocations = %+v, want each pending call once in order", got)
	}
	if len(steps) != 1 || steps[0] != 1 {
		t.Fatalf("TurnStarted steps = %v, want only the next step (1)", steps)
	}
	if recovered == nil || recovered.Executed != 2 || recovered.Replayed != 0 || recovered.Unknown != 0 || recovered.Rejected != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want executed=2", recovered)
	}
	settled := recoveryCheckpoint(t, store, threadID, 0)
	last := settled.State.Messages[len(settled.State.Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != 2 {
		t.Fatalf("settled tool message = %+v, want two results", last)
	}
	for i, id := range []string{"c1", "c2"} {
		tr, ok := last.Parts[i].(core.ToolResult)
		if !ok || tr.CallID != id || tr.IsError {
			t.Fatalf("result %d = %+v, want a successful %s", i, last.Parts[i], id)
		}
	}
	latest, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.InFlight != nil || latest.Step != 1 {
		t.Fatalf("final head = step %d in-flight %v, want step 1 settled", latest.Step, latest.InFlight)
	}
	reqs := requests()
	if len(reqs) != 1 {
		t.Fatalf("model requests = %d, want the next step only", len(reqs))
	}
	reqLast := reqs[0].Messages[len(reqs[0].Messages)-1]
	if reqLast.Role != core.RoleTool || len(reqLast.Parts) != 2 {
		t.Fatalf("next request tail = %+v, want the recovered tool message", reqLast)
	}
}

func TestInFlightRecoveryReplaysCompleted(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-replay"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	execs := []checkpoint.ToolExecution{
		completedExecution(t, threadID, 0, calls[0]),
		completedExecution(t, threadID, 0, calls[1]),
	}
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)}, execs...)

	mw := &afterToolCounter{}
	model, _ := finalTextModel("done")
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(rec),
		agent.WithMiddleware(mw),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times, want a pure replay", got)
	}
	if got := mw.count(); got != 0 {
		t.Fatalf("AfterTool ran %d times, want replay to skip it", got)
	}
	settled := recoveryCheckpoint(t, store, threadID, 0)
	last := settled.State.Messages[len(settled.State.Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != 2 {
		t.Fatalf("settled tool message = %+v", last)
	}
	for i, c := range calls {
		tr, ok := last.Parts[i].(core.ToolResult)
		if !ok || tr.CallID != c.ID || tr.IsError {
			t.Fatalf("replayed result %d = %+v", i, last.Parts[i])
		}
		if text := tr.Content[0].(core.Text).Text; text != "stored result" {
			t.Fatalf("replayed result %d = %q, want the stored result", i, text)
		}
	}
}

func TestInFlightRecoveryReportsUnknown(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-unknown"
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		claimedExecution(t, threadID, 0, calls[0]))

	model, _ := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times, want none for a claimed call", got)
	}
	settled := recoveryCheckpoint(t, store, threadID, 0)
	last := settled.State.Messages[len(settled.State.Messages)-1]
	tr, ok := last.Parts[0].(core.ToolResult)
	if !ok || !tr.IsError || !strings.Contains(tr.Content[0].(core.Text).Text, "outcome was not durably recorded") {
		t.Fatalf("unknown result = %+v", last.Parts[0])
	}
	if state := ledgerState(t, store, threadID, 0, calls[0]); state != checkpoint.ToolUnknown {
		t.Fatalf("ledger state = %q, want unknown", state)
	}
	for _, req := range store.recordedCommits() {
		if req.Checkpoint.Step != 0 || req.Checkpoint.InFlight != nil {
			continue
		}
		if len(req.ToolExecutions) != 0 {
			t.Fatalf("recovery commit recorded %d tool executions for an unknown call", len(req.ToolExecutions))
		}
	}
}

func TestInFlightRecoveryMixedBatch(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-mixed"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	// The executed call is first but deliberately slow, so it finishes after the
	// replayed and unknown calls. The batch runs in parallel (the default) and
	// the results must still follow the calls' order, not completion order.
	rec.onCall = func(*tool.Context, recordedToolCall) { time.Sleep(30 * time.Millisecond) }
	calls := recoveryCalls("c1", "c2", "c3")
	execs := []checkpoint.ToolExecution{
		pendingExecution(t, threadID, 0, calls[0]),
		completedExecution(t, threadID, 0, calls[1]),
		claimedExecution(t, threadID, 0, calls[2]),
	}
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)}, execs...)

	model, _ := finalTextModel("done")
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var recovered *core.ToolBatchRecovered
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := ev.(core.ToolBatchRecovered); ok {
			ev := e
			recovered = &ev
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	got := rec.invocations()
	if len(got) != 1 || got[0].CallID != "c1" {
		t.Fatalf("handler invocations = %+v, want only the pending call", got)
	}
	if recovered == nil || recovered.Replayed != 1 || recovered.Executed != 1 || recovered.Unknown != 1 || recovered.Rejected != 0 {
		t.Fatalf("ToolBatchRecovered = %+v, want replayed=1 executed=1 unknown=1 rejected=0", recovered)
	}
	settled := recoveryCheckpoint(t, store, threadID, 0)
	last := settled.State.Messages[len(settled.State.Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != 3 {
		t.Fatalf("settled tool message = %+v, want three results", last)
	}
	for i, c := range calls {
		tr, ok := last.Parts[i].(core.ToolResult)
		if !ok || tr.CallID != c.ID {
			t.Fatalf("result %d = %+v, want %s in order", i, last.Parts[i], c.ID)
		}
	}
	if tr := last.Parts[0].(core.ToolResult); tr.IsError {
		t.Fatalf("pending call = %+v, want the handler result", tr)
	}
	if tr := last.Parts[1].(core.ToolResult); tr.IsError || tr.Content[0].(core.Text).Text != "stored result" {
		t.Fatalf("completed replay = %+v", tr)
	}
	if tr := last.Parts[2].(core.ToolResult); !tr.IsError {
		t.Fatalf("claimed call = %+v, want unknown error", tr)
	}
}

func TestInFlightRecoverySkipsExecutionChecks(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-no-gates"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	spy := &recoveryHookSpy{}
	model, _ := finalTextModel("done")
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(rec),
		agent.WithMiddleware(spy),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	policy, before := spy.counts()
	if policy != 0 || before != 0 {
		t.Fatalf("execution checks ran during recovery: CheckToolPolicy=%d BeforeTool=%d", policy, before)
	}
	if len(rec.invocations()) != 1 {
		t.Fatalf("handler ran %d times, want once", len(rec.invocations()))
	}
}

func TestInFlightRecoveryUsesSameStepNumbers(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-step"
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 3, calls, inFlightSnapshot(3, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 3, calls[0]))

	model, _ := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	wantInvocation, _ := phase2Invocation(threadID, 3, calls[0])
	got := rec.invocations()
	if len(got) != 1 || got[0].InvocationID != wantInvocation {
		t.Fatalf("handler invocation = %+v, want %s", got, wantInvocation)
	}
	var found bool
	for _, req := range store.recordedCommits() {
		for _, ex := range req.ToolExecutions {
			if ex.InvocationID == wantInvocation && ex.State == checkpoint.ToolCompleted {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("recovery commit never recorded completed invocation %s", wantInvocation)
	}
}

func TestInFlightRecoveryDirectiveEndsRun(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-stop"
	store := checkpoint.NewMemory()
	stopper := stopTool{}
	calls := []core.ToolCall{{ID: "c1", Name: "stopper", Args: json.RawMessage(`{}`)}}
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, stopper),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	model, requests := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, stopper)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var recovered bool
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ev.(core.ToolBatchRecovered); ok {
			recovered = true
			// The event is a report about a settled batch, so the head must
			// already be the committed checkpoint when observers see it.
			head, err := store.Latest(ctx, threadID)
			if err != nil {
				t.Fatal(err)
			}
			if head == nil || head.Step != 0 || head.InFlight != nil {
				t.Fatalf("head when ToolBatchRecovered was published = %+v, want the settled step-0 checkpoint", head)
			}
		}
	}
	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(requests()) != 0 {
		t.Fatalf("model was called %d times: a directive must end the run", len(requests()))
	}
	if !recovered {
		t.Fatal("ToolBatchRecovered was not published")
	}
	tc := res.Message.ToolCalls()
	if len(tc) != 1 || tc[0].Name != "stopper" {
		t.Fatalf("run result message = %+v, want the assistant message that issued the batch", res.Message)
	}
	settled := recoveryCheckpoint(t, store, threadID, 0)
	if last := settled.State.Messages[len(settled.State.Messages)-1]; last.Role != core.RoleTool || len(last.Parts) != 1 {
		t.Fatalf("settled history tail = %+v, want the tool result committed before the directive", last)
	}
}

func TestInFlightRecoveryCommitFailureKeepsHead(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-commit-fail"
	mem := checkpoint.NewMemory()
	commitErr := errors.New("recovery commit refused")
	store := &failCommitStore{Durable: mem, failAt: 2, err: commitErr}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	model, _ := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var recovered int
	for ev, err := range run.Iter() {
		if err != nil {
			// The expected failure; Wait below asserts its identity.
			break
		}
		if _, ok := ev.(core.ToolBatchRecovered); ok {
			recovered++
		}
	}
	if _, err := run.Wait(); !errors.Is(err, commitErr) {
		t.Fatalf("first recovery = %v, want the commit failure", err)
	}
	if recovered != 0 {
		t.Fatalf("ToolBatchRecovered published %d times for a failed commit, want 0", recovered)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("handlers ran %d times, want the side effect before the failed commit", got)
	}
	head, err := mem.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.InFlight == nil {
		t.Fatalf("head after a failed commit = %+v, want the in-flight checkpoint", head)
	}

	second, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatalf("second Resume: %v", err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("handlers ran %d times after retry, want no repeated side effect", got)
	}
	if state := ledgerState(t, mem, threadID, 0, calls[0]); state != checkpoint.ToolUnknown && state != checkpoint.ToolCompleted {
		t.Fatalf("ledger state = %q, want unknown or completed", state)
	}
}

func TestInFlightRecoveryNoPauseEvents(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-events"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	model, _ := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var interrupted int
	var turnsStarted, turnsDone []int
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.Interrupted:
			interrupted++
		case core.TurnStarted:
			turnsStarted = append(turnsStarted, e.Step)
		case core.TurnDone:
			turnsDone = append(turnsDone, e.Step)
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if interrupted != 0 {
		t.Fatalf("Interrupted events = %d, want none for a recovery", interrupted)
	}
	if len(turnsStarted) != 1 || turnsStarted[0] != 1 {
		t.Fatalf("TurnStarted steps = %v, want only 1", turnsStarted)
	}
	for _, step := range turnsDone {
		if step == 0 {
			t.Fatalf("TurnDone steps = %v, recovery must not close step 0", turnsDone)
		}
	}
}

func TestInFlightRecoveryKeepsPhaseSemantics(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-phases"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	model, requests := finalTextModel("done")
	a := newRecoveryAgent(t, store, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Steer(core.UserText("now")); err != nil {
		t.Fatal(err)
	}
	later, err := run.SteerWith(core.UserText("later"), agent.SteerOptions{Phase: core.InputNextTurn})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	reqs := requests()
	if len(reqs) == 0 {
		t.Fatal("the recovered run made no model request")
	}
	first := requestTexts(reqs[0])
	if !containsText(first, "now") {
		t.Fatalf("first request after recovery = %v, want the before_model message", first)
	}
	if containsText(first, "later") {
		t.Fatalf("first request after recovery = %v, must not carry next_turn input", first)
	}
	if out := later.Outcome(); out.Delivered && out.Step == 1 {
		t.Fatalf("next_turn input delivered at the recovered first step: %+v", out)
	}
}

func TestInFlightRecoveryFileStore(t *testing.T) {
	ctx := context.Background()
	const threadID = "recovery-file"
	dir := t.TempDir()
	seedStore, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1")
	seedInFlightHead(t, seedStore, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	model, _ := finalTextModel("done")
	a := newRecoveryAgent(t, reopened, model, rec)
	run, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatalf("Resume from a reopened directory: %v", err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("handlers ran %d times, want once", got)
	}
	head, err := reopened.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if head.InFlight != nil {
		t.Fatalf("head after recovery = %+v, want no in-flight batch", head)
	}
}

func requestTexts(req *llm.Request) []string {
	out := make([]string, 0, len(req.Messages))
	for _, m := range req.Messages {
		out = append(out, m.Text())
	}
	return out
}

func containsText(texts []string, want string) bool {
	for _, text := range texts {
		if strings.Contains(text, want) {
			return true
		}
	}
	return false
}
