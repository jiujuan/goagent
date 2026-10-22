package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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

// inFlightModel answers with a batch of tool calls on its first request and a
// final text once any tool result is visible.
func inFlightModel(calls ...core.ToolCall) *mock.Model {
	return mock.New("inflight", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		parts := make([]core.Part, len(calls))
		for i, c := range calls {
			parts[i] = c
		}
		return &llm.Response{
			Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
			StopReason: llm.StopToolUse,
		}
	})
}

type recordedToolCall struct {
	CallID       string
	InvocationID string
	Args         string
}

// recordingTool records every handler invocation together with the context
// identity the loop assigned it.
type recordingTool struct {
	name   string
	mu     sync.Mutex
	calls  []recordedToolCall
	onCall func(tc *tool.Context, rec recordedToolCall)
}

func newRecordingTool(name string) *recordingTool {
	return &recordingTool{name: name}
}

func (t *recordingTool) Name() string            { return t.name }
func (t *recordingTool) Description() string     { return "records every invocation" }
func (t *recordingTool) Schema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (t *recordingTool) Call(tc *tool.Context, args json.RawMessage) (*tool.Result, error) {
	rec := recordedToolCall{CallID: tc.CallID, InvocationID: tc.InvocationID, Args: string(args)}
	t.mu.Lock()
	t.calls = append(t.calls, rec)
	t.mu.Unlock()
	if t.onCall != nil {
		t.onCall(tc, rec)
	}
	return tool.TextResult("ok"), nil
}

func (t *recordingTool) invocations() []recordedToolCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]recordedToolCall(nil), t.calls...)
}

// failCommitStore injects one error into the Commit sequence and delegates the
// rest to a real durable store.
type failCommitStore struct {
	checkpoint.Durable
	mu       sync.Mutex
	attempts int
	failAt   int
	err      error
}

func (s *failCommitStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	s.attempts++
	n := s.attempts
	s.mu.Unlock()
	if n == s.failAt {
		return checkpoint.CommitReceipt{}, s.err
	}
	return s.Durable.Commit(ctx, req)
}

func (s *failCommitStore) commitAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// plainSaveStore exposes only Checkpointer, so the loop must not treat it as a
// durable store even though the embedded implementation supports more.
type plainSaveStore struct {
	checkpoint.Checkpointer
	mu    sync.Mutex
	saves []bool // cp.InFlight != nil per Save
}

func (s *plainSaveStore) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	s.mu.Lock()
	s.saves = append(s.saves, cp.InFlight != nil)
	s.mu.Unlock()
	return s.Checkpointer.Save(ctx, cp)
}

func (s *plainSaveStore) saveInFlightFlags() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.saves...)
}

// gatedCommitStore holds the first Commit until the test releases it, so the
// test can prove the ToolBatchInFlight event is published only after that
// commit completed.
type gatedCommitStore struct {
	checkpoint.Durable
	gateOnce      sync.Once
	commitStarted chan struct{}
	releaseCommit chan struct{}
}

func (s *gatedCommitStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	first := false
	s.gateOnce.Do(func() {
		first = true
		close(s.commitStarted)
	})
	if first {
		<-s.releaseCommit
	}
	return s.Durable.Commit(ctx, req)
}

type rewriteRecArgs struct{ agent.BaseMiddleware }

func (rewriteRecArgs) BeforeTool(_ *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if c.Name == "rec" {
		c.Args = json.RawMessage(`{"v":1}`)
	}
	return core.Directive{}, nil
}

type interruptBeforeTool struct{ agent.BaseMiddleware }

func (interruptBeforeTool) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

type stopBeforeTool struct{ agent.BaseMiddleware }

func (stopBeforeTool) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Stop}, nil
}

type cancelBeforeTool struct {
	agent.BaseMiddleware
	cancel context.CancelFunc
}

func (m cancelBeforeTool) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	m.cancel()
	return core.Directive{}, nil
}

func recCalls(n int) []core.ToolCall {
	calls := make([]core.ToolCall, n)
	for i := range calls {
		calls[i] = core.ToolCall{
			ID:   "c" + string(rune('1'+i)),
			Name: "rec",
			Args: json.RawMessage(`{}`),
		}
	}
	return calls
}

func firstCommitWith(t *testing.T, store *claimRecordingStore, match func(checkpoint.CommitRequest) bool) checkpoint.CommitRequest {
	t.Helper()
	for _, req := range store.recordedCommits() {
		if match(req) {
			return req
		}
	}
	t.Fatalf("no commit matched among %d commits", len(store.recordedCommits()))
	return checkpoint.CommitRequest{}
}

func hasEvent[T core.Event](events []core.Event) (T, bool) {
	for _, ev := range events {
		if v, ok := ev.(T); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

func TestInFlightCheckpointWrittenBeforeHandlers(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	var mu sync.Mutex
	var observed []*checkpoint.Checkpoint
	rec.onCall = func(_ *tool.Context, _ recordedToolCall) {
		head, err := store.Latest(context.Background(), "inflight-before")
		if err != nil {
			t.Errorf("Latest during handler: %v", err)
			return
		}
		mu.Lock()
		observed = append(observed, head)
		mu.Unlock()
	}

	a, err := agent.New(
		agent.WithModel(inFlightModel(recCalls(2)...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithToolExecution(agent.ToolSequential),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("inflight-before")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 2 {
		t.Fatalf("handlers observed %d heads, want 2", len(observed))
	}
	for i, head := range observed {
		if head == nil || head.InFlight == nil {
			t.Fatalf("handler %d saw head without an in-flight batch: %+v", i, head)
		}
		if head.InFlight.Step != 0 || len(head.InFlight.Calls) != 2 {
			t.Fatalf("handler %d in-flight = %+v, want step 0 with both calls", i, head.InFlight)
		}
		if head.Pending != nil || head.Pause != nil {
			t.Fatalf("handler %d head carries pause state: pending=%+v pause=%+v", i, head.Pending, head.Pause)
		}
		if head.Snapshot == nil || head.Snapshot.ToolDigest == "" {
			t.Fatalf("handler %d head has no step snapshot digest: %+v", i, head.Snapshot)
		}
		last := head.State.Messages[len(head.State.Messages)-1]
		if last.Role != core.RoleAssistant || len(last.ToolCalls()) != 2 {
			t.Fatalf("handler %d head does not end with the assistant batch: %+v", i, last)
		}
	}

	req := firstCommitWith(t, store, func(req checkpoint.CommitRequest) bool {
		return req.Checkpoint != nil && req.Checkpoint.InFlight != nil
	})
	if len(req.ToolExecutions) != 2 {
		t.Fatalf("in-flight commit carries %d executions, want 2", len(req.ToolExecutions))
	}
	for _, ex := range req.ToolExecutions {
		if ex.State != checkpoint.ToolPending {
			t.Fatalf("in-flight execution %s state = %q, want pending", ex.InvocationID, ex.State)
		}
	}
}

func TestInFlightCallsUseRewrittenArgs(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")

	a, err := agent.New(
		agent.WithModel(inFlightModel(core.ToolCall{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)})),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithMiddleware(rewriteRecArgs{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("inflight-rewritten")); err != nil {
		t.Fatal(err)
	}

	calls := rec.invocations()
	if len(calls) != 1 {
		t.Fatalf("handler invoked %d times, want 1", len(calls))
	}
	if calls[0].Args != `{"v":1}` {
		t.Fatalf("handler args = %s, want rewritten batch args", calls[0].Args)
	}

	req := firstCommitWith(t, store, func(req checkpoint.CommitRequest) bool {
		return req.Checkpoint != nil && req.Checkpoint.InFlight != nil
	})
	if got := string(req.Checkpoint.InFlight.Calls[0].Args); got != `{"v":1}` {
		t.Fatalf("in-flight args = %s, want rewritten args", got)
	}

	last := req.Checkpoint.State.Messages[len(req.Checkpoint.State.Messages)-1]
	tcs := last.ToolCalls()
	if len(tcs) != 1 || string(tcs[0].Args) != `{}` {
		t.Fatalf("assistant message args = %+v, want the model's original {}", tcs)
	}

	if len(req.ToolExecutions) != 1 {
		t.Fatalf("in-flight commit carries %d executions, want 1", len(req.ToolExecutions))
	}
	if req.ToolExecutions[0].InvocationID != calls[0].InvocationID {
		t.Fatalf("ledger invocation %q != handler invocation %q: rewritten args and ledger disagree",
			req.ToolExecutions[0].InvocationID, calls[0].InvocationID)
	}
}

func TestInFlightCommitFailureBlocksHandlers(t *testing.T) {
	t.Run("commit error", func(t *testing.T) {
		ctx := context.Background()
		commitErr := errors.New("commit unavailable")
		store := &failCommitStore{Durable: checkpoint.NewMemory(), failAt: 1, err: commitErr}
		rec := newRecordingTool("rec")

		a, err := agent.New(
			agent.WithModel(inFlightModel(recCalls(2)...)),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("inflight-commit-fail"))
		events, err := phase1Drain(t, run)
		if !errors.Is(err, commitErr) {
			t.Fatalf("run error = %v, want %v", err, commitErr)
		}
		if got := len(rec.invocations()); got != 0 {
			t.Fatalf("handlers ran %d times after a failed in-flight commit", got)
		}
		if _, ok := hasEvent[core.ToolStarted](events); ok {
			t.Fatal("ToolStarted published even though the in-flight commit failed")
		}
		if _, ok := hasEvent[core.ToolBatchInFlight](events); ok {
			t.Fatal("ToolBatchInFlight published even though the commit failed")
		}
		head, err := store.Latest(ctx, "inflight-commit-fail")
		if err != nil {
			t.Fatal(err)
		}
		if head == nil || head.InFlight != nil {
			t.Fatalf("head = %+v, want the failure checkpoint with no in-flight batch", head)
		}
		if got := store.commitAttempts(); got != 2 {
			t.Fatalf("commit attempts = %d, want the failed write plus the failure checkpoint", got)
		}
	})

	t.Run("cancelled before the write", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
		rec := newRecordingTool("rec")

		a, err := agent.New(
			agent.WithModel(inFlightModel(recCalls(2)...)),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
			agent.WithMiddleware(cancelBeforeTool{cancel: cancel}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("inflight-cancel"))
		events, err := phase1Drain(t, run)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context.Canceled", err)
		}
		if got := len(rec.invocations()); got != 0 {
			t.Fatalf("handlers ran %d times after cancellation", got)
		}
		if _, ok := hasEvent[core.ToolBatchInFlight](events); ok {
			t.Fatal("ToolBatchInFlight published for a cancelled run")
		}
		commits := store.recordedCommits()
		if len(commits) != 1 {
			t.Fatalf("commits = %d, want only the failure checkpoint", len(commits))
		}
		if commits[0].Checkpoint.InFlight != nil {
			t.Fatal("the failure checkpoint kept an in-flight batch")
		}
	})
}

func TestInFlightSkippedForPlainCheckpointer(t *testing.T) {
	ctx := context.Background()
	store := &plainSaveStore{Checkpointer: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")

	a, err := agent.New(
		agent.WithModel(inFlightModel(recCalls(2)...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("inflight-plain")); err != nil {
		t.Fatal(err)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handlers ran %d times, want 2", got)
	}
	flags := store.saveInFlightFlags()
	if len(flags) != 2 {
		t.Fatalf("saves = %d, want one per step", len(flags))
	}
	if slices.Contains(flags, true) {
		t.Fatalf("a plain checkpointer received an in-flight batch: %v", flags)
	}
	head, err := store.Latest(ctx, "inflight-plain")
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.InFlight != nil {
		t.Fatalf("head = %+v, want no in-flight batch", head)
	}
}

func TestInFlightDisabledByOption(t *testing.T) {
	ctx := context.Background()
	runWith := func(thread string, opts ...agent.Option) (int, []core.Event, *recordingTool) {
		store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
		rec := newRecordingTool("rec")
		base := []agent.Option{
			agent.WithModel(inFlightModel(recCalls(2)...)),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
		}
		a, err := agent.New(append(base, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread(thread))
		events, err := phase1Drain(t, run)
		if err != nil {
			t.Fatal(err)
		}
		return len(store.recordedCommits()), events, rec
	}

	defaultCommits, defaultEvents, defaultRec := runWith("inflight-option-on")
	disabledCommits, disabledEvents, disabledRec := runWith("inflight-option-off", agent.WithInFlightCheckpoint(false))

	if got := len(defaultRec.invocations()); got != 2 {
		t.Fatalf("default handlers ran %d times, want 2", got)
	}
	if got := len(disabledRec.invocations()); got != 2 {
		t.Fatalf("disabled handlers ran %d times, want 2", got)
	}
	if disabledCommits != defaultCommits-1 {
		t.Fatalf("disabled commits = %d, default commits = %d; want exactly one fewer", disabledCommits, defaultCommits)
	}
	if _, ok := hasEvent[core.ToolBatchInFlight](defaultEvents); !ok {
		t.Fatal("default run published no ToolBatchInFlight")
	}
	if _, ok := hasEvent[core.ToolBatchInFlight](disabledEvents); ok {
		t.Fatal("disabled run published ToolBatchInFlight")
	}
}

func TestInFlightNotWrittenForSubRun(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	var mu sync.Mutex
	var inFlightDuringSubRun []bool
	rec.onCall = func(_ *tool.Context, _ recordedToolCall) {
		head, err := store.Latest(context.Background(), "inflight-sub")
		if err != nil {
			t.Errorf("Latest during sub-run handler: %v", err)
			return
		}
		mu.Lock()
		inFlightDuringSubRun = append(inFlightDuringSubRun, head != nil && head.InFlight != nil)
		mu.Unlock()
	}

	// The delegate starts from the parent's history, which already ends with the
	// transfer tool result, so its own batch is scripted by call count rather than
	// by "any tool result visible".
	turns := 0
	expertCalls := recCalls(2)
	expert, err := agent.New(
		agent.WithName("expert"),
		agent.WithModel(mock.New("expert", func(*llm.Request) *llm.Response {
			turns++
			if turns > 1 {
				return mock.Text("done")
			}
			parts := make([]core.Part, len(expertCalls))
			for i, c := range expertCalls {
				parts[i] = c
			}
			return &llm.Response{
				Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
				StopReason: llm.StopToolUse,
			}
		})),
		agent.WithTools(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.New(
		agent.WithName("router"),
		agent.WithModel(mock.New("router", func(*llm.Request) *llm.Response {
			return mock.CallTool("t1", "transfer_to_agent", `{"agent":"expert"}`)
		})),
		agent.WithSubAgents(expert),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Run(ctx, "go", agent.OnThread("inflight-sub")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(inFlightDuringSubRun) != 2 {
		t.Fatalf("sub-run handlers observed %d heads, want 2", len(inFlightDuringSubRun))
	}
	if slices.Contains(inFlightDuringSubRun, true) {
		t.Fatalf("delegated run wrote an in-flight head on the parent thread: %v", inFlightDuringSubRun)
	}
}

func TestInFlightCommitKeepsClaim(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")

	a, err := agent.New(
		agent.WithModel(inFlightModel(recCalls(2)...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("inflight-claim")); err != nil {
		t.Fatal(err)
	}

	commits := store.recordedCommits()
	var releases []bool
	var sawInFlight bool
	for _, req := range commits {
		if req.Checkpoint.InFlight != nil {
			sawInFlight = true
		}
		releases = append(releases, req.ReleaseClaim)
	}
	if !sawInFlight {
		t.Fatalf("commits = %d, want an in-flight commit", len(commits))
	}
	if want := []bool{false, false, true}; !slices.Equal(releases, want) {
		t.Fatalf("ReleaseClaim sequence = %v, want %v", releases, want)
	}
}

func TestInFlightConfirmsThisStepAcks(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")

	a, err := agent.New(
		agent.WithModel(inFlightModel(recCalls(2)...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := a.Deliver(ctx, "inflight-ack", checkpoint.InboxMessage{
		ID:      "m1",
		Message: core.UserText("note"),
		Phase:   core.InputBeforeModel,
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != "m1" || receipt.Duplicate {
		t.Fatalf("delivery receipt = %+v, want a fresh m1", receipt)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("inflight-ack")); err != nil {
		t.Fatal(err)
	}

	req := firstCommitWith(t, store, func(req checkpoint.CommitRequest) bool {
		return req.Checkpoint != nil && req.Checkpoint.InFlight != nil
	})
	if !slices.Contains(req.AckMessageIDs, "m1") {
		t.Fatalf("in-flight commit acks = %v, want m1 confirmed with the batch", req.AckMessageIDs)
	}
	inbox, err := store.Inbox(ctx, "inflight-ack")
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 0 {
		t.Fatalf("inbox still holds %d messages after the run", len(inbox))
	}
}

func TestInFlightEventPublishedAfterCommit(t *testing.T) {
	ctx := context.Background()
	store := &gatedCommitStore{
		Durable:       checkpoint.NewMemory(),
		commitStarted: make(chan struct{}),
		releaseCommit: make(chan struct{}),
	}
	rec := newRecordingTool("rec")
	a, err := agent.New(
		agent.WithModel(inFlightModel(core.ToolCall{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)})),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithMiddleware(rewriteRecArgs{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("inflight-event"))
	events, cancel := a.Bus().Subscribe(bus.Topic(run.ID), bus.Lossy)
	defer cancel()
	inFlightCh := make(chan core.ToolBatchInFlight, 4)
	go func() {
		for ev := range events {
			if e, ok := ev.(core.ToolBatchInFlight); ok {
				inFlightCh <- e
			}
		}
	}()

	waitCh := make(chan error, 1)
	go func() {
		_, err := run.Wait()
		waitCh <- err
	}()

	select {
	case <-store.commitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight commit never started")
	}
	release := func() {
		select {
		case <-store.releaseCommit:
		default:
			close(store.releaseCommit)
		}
	}
	defer release()

	select {
	case ev := <-inFlightCh:
		t.Fatalf("ToolBatchInFlight published before the commit completed: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	release()

	select {
	case ev := <-inFlightCh:
		if ev.Step != 0 {
			t.Fatalf("event step = %d, want 0", ev.Step)
		}
		if len(ev.Calls) != 1 || string(ev.Calls[0].Args) != `{"v":1}` {
			t.Fatalf("event calls = %+v, want the rewritten batch", ev.Calls)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ToolBatchInFlight was never published after the commit")
	}
	if err := <-waitCh; err != nil {
		t.Fatal(err)
	}
}

func TestInFlightRejectedBeforeExecutionChecksPass(t *testing.T) {
	t.Run("interrupt", func(t *testing.T) {
		ctx := context.Background()
		store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
		rec := newRecordingTool("rec")
		a, err := agent.New(
			agent.WithModel(inFlightModel(recCalls(2)...)),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
			agent.WithMiddleware(interruptBeforeTool{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("inflight-gate-interrupt"))
		events, err := phase1Drain(t, run)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := hasEvent[core.ToolBatchInFlight](events); ok {
			t.Fatal("ToolBatchInFlight published before the gate interrupted")
		}
		if got := len(rec.invocations()); got != 0 {
			t.Fatalf("handlers ran %d times before the pause", got)
		}
		head, err := store.Latest(ctx, "inflight-gate-interrupt")
		if err != nil {
			t.Fatal(err)
		}
		if head == nil || head.InFlight != nil {
			t.Fatalf("head = %+v, want no in-flight batch", head)
		}
		if head.Pending == nil || head.Pause == nil {
			t.Fatalf("head = %+v, want pending and pause", head)
		}
		if got := len(store.recordedCommits()); got != 1 {
			t.Fatalf("commits = %d, want only the pause checkpoint", got)
		}
	})

	t.Run("stop", func(t *testing.T) {
		ctx := context.Background()
		store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
		rec := newRecordingTool("rec")
		a, err := agent.New(
			agent.WithModel(inFlightModel(recCalls(2)...)),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
			agent.WithMiddleware(stopBeforeTool{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("inflight-gate-stop"))
		events, err := phase1Drain(t, run)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := hasEvent[core.ToolBatchInFlight](events); ok {
			t.Fatal("ToolBatchInFlight published before the gate stopped the run")
		}
		if got := len(rec.invocations()); got != 0 {
			t.Fatalf("handlers ran %d times after the stop", got)
		}
		head, err := store.Latest(ctx, "inflight-gate-stop")
		if err != nil {
			t.Fatal(err)
		}
		if head == nil || head.InFlight != nil {
			t.Fatalf("head = %+v, want no in-flight batch", head)
		}
		if got := len(store.recordedCommits()); got != 1 {
			t.Fatalf("commits = %d, want only the stop checkpoint", got)
		}
	})
}

// An unadvertised call never runs, so a batch holding one has no side effect to
// recover. The write is skipped and the batch takes the ordinary per-call
// rejection path instead of failing the whole run on a storage invariant.
func TestInFlightSkipsUnadvertisedBatch(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	batch := []core.ToolCall{
		{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)},
		{ID: "c2", Name: "ghost", Args: json.RawMessage(`{}`)},
	}

	a, err := agent.New(
		agent.WithModel(inFlightModel(batch...)),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", agent.OnThread("inflight-unadvertised"))
	events, err := phase1Drain(t, run)
	if err != nil {
		t.Fatalf("run failed instead of rejecting the unadvertised call: %v", err)
	}
	if got := len(rec.invocations()); got != 1 {
		t.Fatalf("advertised handler ran %d times, want 1", got)
	}
	if _, ok := hasEvent[core.ToolBatchInFlight](events); ok {
		t.Fatal("ToolBatchInFlight published for a batch holding an unadvertised call")
	}
	hist, err := store.History(ctx, "inflight-unadvertised")
	if err != nil {
		t.Fatal(err)
	}
	for i, cp := range hist {
		if cp.InFlight != nil {
			t.Fatalf("checkpoint %d recorded an in-flight batch: %+v", i, cp.InFlight)
		}
	}
	var rejectedGhost bool
	for _, msg := range hist[len(hist)-1].State.Messages {
		if msg.Role != core.RoleTool {
			continue
		}
		for _, p := range msg.Parts {
			if r, ok := p.(core.ToolResult); ok && r.CallID == "c2" && r.IsError {
				rejectedGhost = true
			}
		}
	}
	if !rejectedGhost {
		t.Fatal("unadvertised call did not receive an error result")
	}
}
