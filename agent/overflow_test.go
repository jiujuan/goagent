package agent_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
)

const overflowThread = "overflow-thread"

func overflowError(limit, requested int) error {
	return llm.NewStatusError("test", 400,
		fmt.Sprintf("maximum context length is %d tokens, however you requested %d tokens", limit, requested))
}

func overflowHistory(n int) []core.Message {
	msgs := make([]core.Message, n)
	for i := range msgs {
		msgs[i] = core.UserText(fmt.Sprintf("message-%d", i))
	}
	return msgs
}

func seededStore(t *testing.T, msgs []core.Message) *checkpoint.Memory {
	t.Helper()
	store := checkpoint.NewMemory()
	cp := &checkpoint.Checkpoint{
		ID:       "seed",
		ThreadID: overflowThread,
		State:    core.State{Messages: msgs},
	}
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	return store
}

// dropOldest accepts a recovery when dropping the n oldest messages leaves at
// least one behind.
type dropOldest struct {
	agent.BaseMiddleware
	n int
}

func (d dropOldest) RecoverContext(_ *agent.LoopContext, history []core.Message, _ *llm.ContextLengthError) ([]core.Message, bool) {
	if len(history) <= d.n {
		return history, false
	}
	return history[d.n:], true
}

type recoverFunc struct {
	agent.BaseMiddleware
	fn func(*agent.LoopContext, []core.Message, *llm.ContextLengthError) ([]core.Message, bool)
}

func (r recoverFunc) RecoverContext(lc *agent.LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
	return r.fn(lc, history, overflow)
}

// partialThenErrorModel emits one partial before yielding the scripted error.
type partialThenErrorModel struct {
	mu       sync.Mutex
	calls    int
	overflow error
}

func (m *partialThenErrorModel) Name() string { return "partial" }

func (m *partialThenErrorModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.calls++
		m.mu.Unlock()
		if !yield(mock.Partial("half"), nil) {
			return
		}
		yield(nil, m.overflow)
	}
}

func (m *partialThenErrorModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// cancelThenOverflowModel cancels the run before reporting the overflow.
type cancelThenOverflowModel struct {
	mu       sync.Mutex
	calls    int
	cancel   context.CancelFunc
	overflow error
}

func (m *cancelThenOverflowModel) Name() string { return "cancel" }

func (m *cancelThenOverflowModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.calls++
		m.mu.Unlock()
		m.cancel()
		yield(nil, m.overflow)
	}
}

func (m *cancelThenOverflowModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// releaseRecorder records the ReleaseClaim flag of every durable commit.
type releaseRecorder struct {
	checkpoint.Durable

	mu       sync.Mutex
	releases []bool
}

func (r *releaseRecorder) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	r.mu.Lock()
	r.releases = append(r.releases, req.ReleaseClaim)
	r.mu.Unlock()
	return r.Durable.Commit(ctx, req)
}

func (r *releaseRecorder) releaseSequence() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.releases...)
}

// countingMW counts the prepare/request hooks so a test can prove a retry does
// not replay them. It can append a marker to the system prompt and observe the
// request AfterModel sees.
type countingMW struct {
	agent.BaseMiddleware

	mu       sync.Mutex
	before   int
	modify   int
	modelCtx int
	seen     [][]core.Message

	marker string
	after  func(*agent.LoopContext)
}

func (c *countingMW) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	c.mu.Lock()
	c.before++
	c.mu.Unlock()
	return core.Directive{}, nil
}

func (c *countingMW) ModifyRequest(_ *agent.LoopContext, req *llm.Request) error {
	c.mu.Lock()
	c.modify++
	marker := c.marker
	c.mu.Unlock()
	if marker != "" {
		req.System += " [" + marker + "]"
	}
	return nil
}

func (c *countingMW) ModelContext(_ *agent.LoopContext, ctx context.Context) context.Context {
	c.mu.Lock()
	c.modelCtx++
	c.mu.Unlock()
	return ctx
}

func (c *countingMW) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	if c.after != nil {
		c.after(lc)
	}
	return core.Directive{}, nil
}

func (c *countingMW) counts() (before, modify, modelCtx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.before, c.modify, c.modelCtx
}

func (c *countingMW) seenMessages() [][]core.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]core.Message, len(c.seen))
	for i := range c.seen {
		out[i] = append([]core.Message(nil), c.seen[i]...)
	}
	return out
}

func retriedEvents(events []core.Event) []core.ContextOverflowRetried {
	var out []core.ContextOverflowRetried
	for _, ev := range events {
		if e, ok := ev.(core.ContextOverflowRetried); ok {
			out = append(out, e)
		}
	}
	return out
}

func deltaCount(events []core.Event) int {
	n := 0
	for _, ev := range events {
		if _, ok := ev.(core.MessageDelta); ok {
			n++
		}
	}
	return n
}

func TestContextOverflowRecoversAndRetries(t *testing.T) {
	ctx := context.Background()
	store := seededStore(t, overflowHistory(6))
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 5}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if runErr != nil {
		t.Fatalf("run error: %v", runErr)
	}
	if got := m.calls(); got != 2 {
		t.Fatalf("model calls = %d, want 2", got)
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if len(reqs[1].Messages) >= len(reqs[0].Messages) {
		t.Fatalf("retry request has %d messages, first had %d", len(reqs[1].Messages), len(reqs[0].Messages))
	}

	got := retriedEvents(events)
	if len(got) != 1 {
		t.Fatalf("ContextOverflowRetried events = %d, want 1", len(got))
	}
	ev := got[0]
	if ev.Step != 0 || ev.Attempt != 1 {
		t.Fatalf("event step/attempt = %d/%d, want 0/1", ev.Step, ev.Attempt)
	}
	if ev.MessagesBefore != len(reqs[0].Messages) || ev.MessagesAfter != len(reqs[1].Messages) {
		t.Fatalf("event messages = %d→%d, requests = %d→%d",
			ev.MessagesBefore, ev.MessagesAfter, len(reqs[0].Messages), len(reqs[1].Messages))
	}
	if ev.Provider != "test" || ev.Limit != 100 || ev.Requested != 300 {
		t.Fatalf("event provider/limit/requested = %q/%d/%d, want test/100/300",
			ev.Provider, ev.Limit, ev.Requested)
	}
}

func TestContextOverflowDisableOption(t *testing.T) {
	ctx := context.Background()
	store := newPhase1FailOnSaveStore(nil)
	overflow := overflowError(100, 300)
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflow
		}
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}),
		agent.WithContextOverflowRetry(0),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 with recovery disabled", got)
	}
	var cle *llm.ContextLengthError
	if !errors.As(runErr, &cle) {
		t.Fatalf("run error = %v, want *llm.ContextLengthError", runErr)
	}
	if !errors.Is(runErr, overflow) {
		t.Fatalf("run error = %v, want the original overflow", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
	if got := store.SaveCount(); got != 1 {
		t.Fatalf("save attempts = %d, want only the terminal checkpoint", got)
	}
}

func TestContextOverflowDoesNotRetryAfterPartial(t *testing.T) {
	ctx := context.Background()
	m := &partialThenErrorModel{overflow: overflowError(100, 300)}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.callCount(); got != 1 {
		t.Fatalf("model calls = %d, want 1 after a partial was emitted", got)
	}
	if got := deltaCount(events); got != 1 {
		t.Fatalf("MessageDelta events = %d, want 1", got)
	}
	var cle *llm.ContextLengthError
	if !errors.As(runErr, &cle) {
		t.Fatalf("run error = %v, want *llm.ContextLengthError", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
}

func TestContextOverflowPersistsBeforeRetry(t *testing.T) {
	ctx := context.Background()
	store := seededStore(t, overflowHistory(6))
	var (
		mu      sync.Mutex
		latest  *checkpoint.Checkpoint
		latestE error
	)
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		cp, err := store.Latest(context.Background(), overflowThread)
		mu.Lock()
		latest, latestE = cp, err
		mu.Unlock()
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 5}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	mu.Lock()
	cp, loadErr := latest, latestE
	mu.Unlock()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if cp == nil {
		t.Fatal("no checkpoint was visible before the retried call")
	}
	if cp.Step != 0 {
		t.Fatalf("checkpoint step = %d, want 0", cp.Step)
	}
	if cp.Pause != nil {
		t.Fatalf("checkpoint carries a pause: %+v", cp.Pause)
	}
	if cp.ID == "seed" {
		t.Fatal("the retry reused the seeded checkpoint instead of committing a new one")
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if got, want := digest(cp.State.Messages), digest(reqs[1].Messages); got != want {
		t.Fatalf("checkpoint messages = %q, want the retried request %q", got, want)
	}
}

func TestContextOverflowMidStepKeepsClaim(t *testing.T) {
	ctx := context.Background()
	rec := &releaseRecorder{Durable: checkpoint.NewMemory()}
	seed := &checkpoint.Checkpoint{
		ID:       "seed",
		ThreadID: overflowThread,
		State:    core.State{Messages: overflowHistory(6)},
	}
	if err := rec.Save(ctx, seed); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 5}),
		agent.WithCheckpointer(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	got := rec.releaseSequence()
	if want := []bool{false, true}; !slices.Equal(got, want) {
		t.Fatalf("release sequence = %v, want %v", got, want)
	}
}

func TestContextOverflowSecondFailureReturnsSecondError(t *testing.T) {
	ctx := context.Background()
	store := seededStore(t, overflowHistory(6))
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		return nil, overflowError(100, 400-call*100)
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}),
		agent.WithContextOverflowRetry(2),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 3 {
		t.Fatalf("model calls = %d, want 3", got)
	}
	got := retriedEvents(events)
	if len(got) != 2 {
		t.Fatalf("ContextOverflowRetried events = %d, want 2", len(got))
	}
	if got[0].Attempt != 1 || got[1].Attempt != 2 {
		t.Fatalf("event attempts = %d,%d, want 1,2", got[0].Attempt, got[1].Attempt)
	}
	var cle *llm.ContextLengthError
	if !errors.As(runErr, &cle) {
		t.Fatalf("run error = %v, want *llm.ContextLengthError", runErr)
	}
	if cle.Requested != 100 {
		t.Fatalf("final requested = %d, want the third call's 100", cle.Requested)
	}
}

func TestContextOverflowNoRecovererFails(t *testing.T) {
	ctx := context.Background()
	overflow := overflowError(100, 300)
	m := &scriptModel{}
	m.pick = func(int, *llm.Request) (*llm.Response, error) {
		return nil, overflow
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithCheckpointer(seededStore(t, overflowHistory(2))),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 without a recoverer", got)
	}
	if !errors.Is(runErr, overflow) {
		t.Fatalf("run error = %v, want the overflow", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
}

func TestContextOverflowRequiresShrink(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(*agent.LoopContext, []core.Message, *llm.ContextLengthError) ([]core.Message, bool){
		"unchanged": func(_ *agent.LoopContext, history []core.Message, _ *llm.ContextLengthError) ([]core.Message, bool) {
			return history, true
		},
		"empty": func(*agent.LoopContext, []core.Message, *llm.ContextLengthError) ([]core.Message, bool) {
			return nil, true
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			overflow := overflowError(100, 300)
			m := &scriptModel{}
			m.pick = func(int, *llm.Request) (*llm.Response, error) {
				return nil, overflow
			}
			a, err := agent.New(
				agent.WithModel(m),
				agent.WithMiddleware(recoverFunc{fn: fn}),
				agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
			)
			if err != nil {
				t.Fatal(err)
			}
			events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
			if got := m.calls(); got != 1 {
				t.Fatalf("model calls = %d, want 1 for a %s result", got, name)
			}
			if !errors.Is(runErr, overflow) {
				t.Fatalf("run error = %v, want the overflow", runErr)
			}
			if got := retriedEvents(events); len(got) != 0 {
				t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
			}
		})
	}
}

func TestContextOverflowCheckpointFailureDoesNotRetry(t *testing.T) {
	ctx := context.Background()
	saveErr := errors.New("checkpoint unavailable")
	store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
	seed := &checkpoint.Checkpoint{
		ID:       "seed",
		ThreadID: overflowThread,
		State:    core.State{Messages: overflowHistory(6)},
	}
	if err := store.base.Save(ctx, seed); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	overflow := overflowError(100, 300)
	m := &scriptModel{}
	m.pick = func(int, *llm.Request) (*llm.Response, error) {
		return nil, overflow
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 5}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 when the mid-step checkpoint fails", got)
	}
	if !errors.Is(runErr, overflow) {
		t.Fatalf("run error = %v, want the overflow kept", runErr)
	}
	if !errors.Is(runErr, saveErr) {
		t.Fatalf("run error = %v, want the save error kept", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
	if got := store.SaveCount(); got != 2 {
		t.Fatalf("save attempts = %d, want the failed mid-step write plus the terminal write", got)
	}
}

func TestContextOverflowFreezeFailureDoesNotRetry(t *testing.T) {
	ctx := context.Background()
	overflow := overflowError(100, 300)
	m := &scriptModel{}
	m.pick = func(int, *llm.Request) (*llm.Response, error) {
		return nil, overflow
	}
	old := &viewShapeTool{name: "x", desc: "old", schema: `{"type":"object"}`}
	swap := recoverFunc{fn: func(lc *agent.LoopContext, history []core.Message, _ *llm.ContextLengthError) ([]core.Message, bool) {
		lc.AddTool(&viewShapeTool{name: "x", desc: "new", schema: `{"type":"object"}`})
		return history[1:], true
	}}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithTools(old),
		agent.WithMiddleware(swap),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 when the retry view cannot freeze", got)
	}
	if !errors.Is(runErr, agent.ErrToolViewInvalid) {
		t.Fatalf("run error = %v, want ErrToolViewInvalid", runErr)
	}
	if !errors.Is(runErr, overflow) {
		t.Fatalf("run error = %v, want the overflow kept", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
}

func TestContextOverflowCanceledContextDoesNotRecover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := &cancelThenOverflowModel{cancel: cancel, overflow: overflowError(100, 300)}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.callCount(); got != 1 {
		t.Fatalf("model calls = %d, want 1 when the context was canceled", got)
	}
	if runErr == nil {
		t.Fatal("run succeeded after its context was canceled")
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
}

func TestContextOverflowAfterModelSeesRetryRequest(t *testing.T) {
	ctx := context.Background()
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	mw := &countingMW{}
	mw.after = func(lc *agent.LoopContext) {
		mw.mu.Lock()
		mw.seen = append(mw.seen, append([]core.Message(nil), lc.Request.Messages...))
		mw.mu.Unlock()
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}, mw),
		agent.WithCheckpointer(seededStore(t, overflowHistory(4))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	seen := mw.seenMessages()
	if len(seen) != 1 {
		t.Fatalf("AfterModel observations = %d, want 1", len(seen))
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if got, want := digest(seen[0]), digest(reqs[1].Messages); got != want {
		t.Fatalf("AfterModel saw %q, want the retried request %q", got, want)
	}
	if len(seen[0]) >= len(reqs[0].Messages) {
		t.Fatalf("AfterModel saw %d messages, first request had %d", len(seen[0]), len(reqs[0].Messages))
	}
}

func TestContextOverflowModifyRequestRunsOnce(t *testing.T) {
	ctx := context.Background()
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	mw := &countingMW{}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}, mw),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	before, modify, modelCtx := mw.counts()
	if before != 1 || modify != 1 {
		t.Fatalf("BeforeModel/ModifyRequest = %d/%d, want 1/1", before, modify)
	}
	if modelCtx != 2 {
		t.Fatalf("ModelContext calls = %d, want 2", modelCtx)
	}
}

func TestContextOverflowDoesNotDuplicateSystemRewrite(t *testing.T) {
	ctx := context.Background()
	const marker = "unique-system-rewrite-7f3a"
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	mw := &countingMW{marker: marker}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}, mw),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	for i, req := range reqs {
		if got := strings.Count(req.System, marker); got != 1 {
			t.Fatalf("request %d system has %d markers, want 1: %q", i+1, got, req.System)
		}
	}
}

func TestContextOverflowRunBudgetWrapUp(t *testing.T) {
	ctx := context.Background()
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(
			dropOldest{n: 1},
			middleware.RunBudget(middleware.RunBudgetOptions{MaxTurns: 100}),
		),
		agent.WithMaxTurns(1),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread(overflowThread)); err != nil {
		t.Fatalf("run error: %v", err)
	}

	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if len(reqs[1].Tools) != 0 {
		t.Fatalf("retried request advertises %d tools, want none in wrap-up mode", len(reqs[1].Tools))
	}
	if got := strings.Count(reqs[1].System, middleware.WarnMarkerBudget); got != 1 {
		t.Fatalf("retried system has %d budget markers, want 1: %q", got, reqs[1].System)
	}
}

// perMessageCounter charges a fixed price per message so a test can predict
// exactly where the compaction cut lands.
func perMessageCounter(n int) middleware.TokenCounter {
	return func(msgs []core.Message) int { return len(msgs) * n }
}

func orphanToolResults(msgs []core.Message) []string {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, p := range m.Parts {
			if c, ok := p.(core.ToolCall); ok {
				calls[c.ID] = true
			}
		}
	}
	var orphans []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			if r, ok := p.(core.ToolResult); ok && !calls[r.CallID] {
				orphans = append(orphans, r.CallID)
			}
		}
	}
	return orphans
}

func TestContextOverflowCompactionRecovery(t *testing.T) {
	ctx := context.Background()
	big := strings.Repeat("x", 200)
	toolPair := func(id string) []core.Message {
		return []core.Message{
			{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: id, Name: "fetch", Args: []byte("{}")}}},
			{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: id, Name: "fetch", Content: []core.Part{core.Text{Text: big}}}}},
		}
	}
	seed := []core.Message{core.UserText("user-0 " + big)}
	seed = append(seed, toolPair("call-1")...)
	seed = append(seed, core.UserText("user-3 "+big))
	seed = append(seed, toolPair("call-2")...)
	seed = append(seed, core.UserText("user-6 "+big), core.UserText("user-7 "+big))
	if len(seed) != 8 {
		t.Fatalf("seed has %d messages, want 8", len(seed))
	}

	var summaryCalls int32
	sum := mock.New("sum", func(*llm.Request) *llm.Response {
		atomic.AddInt32(&summaryCalls, 1)
		return mock.Text("COMPACTED")
	})
	store := seededStore(t, seed)
	var (
		mu      sync.Mutex
		during  *checkpoint.Checkpoint
		duringE error
	)
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		cp, err := store.Latest(context.Background(), overflowThread)
		mu.Lock()
		during, duringE = cp, err
		mu.Unlock()
		return mock.Text("done"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(middleware.Compaction(middleware.CompactionOptions{
			Model: sum, MaxTokens: 1 << 30, KeepRecent: 2, Counter: perMessageCounter(10),
		})),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if runErr != nil {
		t.Fatalf("run error: %v", runErr)
	}
	if got := m.calls(); got != 2 {
		t.Fatalf("model calls = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&summaryCalls); got != 1 {
		t.Fatalf("summary calls = %d, want 1", got)
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if got := len(reqs[0].Messages); got != 9 {
		t.Fatalf("first request has %d messages, want 9", got)
	}
	if got, first := len(reqs[1].Messages), len(reqs[0].Messages); got >= first {
		t.Fatalf("retried request has %d messages, first had %d", got, first)
	}
	head := reqs[1].Messages[0]
	if head.Role != core.RoleUser || !strings.HasPrefix(head.Text(), "[earlier conversation summary]") {
		t.Fatalf("retried history starts with %q/%q, want a summary note", head.Role, head.Text())
	}
	if out := orphanToolResults(reqs[1].Messages); len(out) != 0 {
		t.Fatalf("retried request carries orphan tool results: %v", out)
	}

	var compactedAt, retriedAt = -1, -1
	var compacted core.HistoryCompacted
	for i, ev := range events {
		switch e := ev.(type) {
		case core.HistoryCompacted:
			if compactedAt < 0 {
				compactedAt, compacted = i, e
			}
		case core.ContextOverflowRetried:
			if retriedAt < 0 {
				retriedAt = i
			}
		}
	}
	if compactedAt < 0 || retriedAt < 0 {
		t.Fatalf("events missing HistoryCompacted (%d) or ContextOverflowRetried (%d)", compactedAt, retriedAt)
	}
	if compactedAt > retriedAt {
		t.Fatalf("HistoryCompacted at %d came after ContextOverflowRetried at %d", compactedAt, retriedAt)
	}
	if compacted.Dropped != 7 || compacted.Kept != 2 || compacted.EstTokens != 90 || compacted.Step != 0 {
		t.Fatalf("HistoryCompacted = %+v, want Dropped 7, Kept 2, EstTokens 90, Step 0", compacted)
	}

	mu.Lock()
	mid, midErr := during, duringE
	mu.Unlock()
	if midErr != nil || mid == nil {
		t.Fatalf("checkpoint visible to the retried call: %v (nil=%v)", midErr, mid == nil)
	}
	if mid.ID == "seed" {
		t.Fatal("the retry did not persist the recovered history before calling the model")
	}
	if !strings.HasPrefix(mid.State.Messages[0].Text(), "[earlier conversation summary]") {
		t.Fatalf("checkpoint at retry starts with %q, want the summary note", mid.State.Messages[0].Text())
	}
	if out := orphanToolResults(mid.State.Messages); len(out) != 0 {
		t.Fatalf("checkpoint at retry carries orphan tool results: %v", out)
	}

	cp, err := store.Latest(ctx, overflowThread)
	if err != nil || cp == nil {
		t.Fatalf("latest checkpoint: %v (nil=%v)", err, cp == nil)
	}
	if !strings.HasPrefix(cp.State.Messages[0].Text(), "[earlier conversation summary]") {
		t.Fatalf("durable history starts with %q, want the summary note", cp.State.Messages[0].Text())
	}
	if out := orphanToolResults(cp.State.Messages); len(out) != 0 {
		t.Fatalf("durable history carries orphan tool results: %v", out)
	}
}

func TestContextOverflowWindowRecovery(t *testing.T) {
	ctx := context.Background()
	big := strings.Repeat("x", 200)
	toolPair := func(id string) []core.Message {
		return []core.Message{
			{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: id, Name: "fetch", Args: []byte("{}")}}},
			{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: id, Name: "fetch", Content: []core.Part{core.Text{Text: big}}}}},
		}
	}
	seed := []core.Message{core.UserText("user-0 " + big)}
	seed = append(seed, toolPair("call-1")...)
	seed = append(seed, core.UserText("user-3 "+big), core.UserText("user-4 "+big), core.UserText("user-5 "+big))
	seed = append(seed, toolPair("call-2")...)
	if len(seed) != 8 {
		t.Fatalf("seed has %d messages, want 8", len(seed))
	}

	store := seededStore(t, seed)
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflowError(100, 300)
		}
		return mock.Text("done"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(middleware.Window(middleware.WindowOptions{
			Strategy: middleware.SlidingWindow(1 << 30),
			Counter:  perMessageCounter(10),
		})),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if runErr != nil {
		t.Fatalf("run error: %v", runErr)
	}
	if got := m.calls(); got != 2 {
		t.Fatalf("model calls = %d, want 2", got)
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("recorded requests = %d, want 2", len(reqs))
	}
	if got := len(reqs[0].Messages); got != 9 {
		t.Fatalf("first request has %d messages, want 9", got)
	}
	if got := len(reqs[1].Messages); got != 4 {
		t.Fatalf("retried request has %d messages, want 4", got)
	}
	if out := orphanToolResults(reqs[1].Messages); len(out) != 0 {
		t.Fatalf("retried request carries orphan tool results: %v", out)
	}

	var trimmedAt, retriedAt, trimmedCount = -1, -1, 0
	var trimmed core.WindowTrimmed
	for i, ev := range events {
		switch e := ev.(type) {
		case core.WindowTrimmed:
			trimmedCount++
			if trimmedAt < 0 {
				trimmedAt, trimmed = i, e
			}
		case core.ContextOverflowRetried:
			if retriedAt < 0 {
				retriedAt = i
			}
		}
	}
	if trimmedAt < 0 || retriedAt < 0 {
		t.Fatalf("events missing WindowTrimmed (%d) or ContextOverflowRetried (%d)", trimmedAt, retriedAt)
	}
	if trimmedCount != 1 {
		t.Fatalf("WindowTrimmed events = %d, want only the recovery event", trimmedCount)
	}
	if trimmedAt > retriedAt {
		t.Fatalf("WindowTrimmed at %d came after ContextOverflowRetried at %d", trimmedAt, retriedAt)
	}
	if trimmed.Strategy != "overflow_tail" || trimmed.Dropped != 5 || trimmed.Kept != 4 || trimmed.EstTokens != 90 || trimmed.Step != 0 {
		t.Fatalf("WindowTrimmed = %+v, want overflow_tail, Dropped 5, Kept 4, EstTokens 90, Step 0", trimmed)
	}

	cp, err := store.Latest(ctx, overflowThread)
	if err != nil || cp == nil {
		t.Fatalf("latest checkpoint: %v (nil=%v)", err, cp == nil)
	}
	want := append(append([]core.Message(nil), reqs[1].Messages...), core.AssistantText("done"))
	if got, wantDigest := digest(cp.State.Messages), digest(want); got != wantDigest {
		t.Fatalf("checkpoint messages = %q, want the recovered history plus the final reply %q", got, wantDigest)
	}
}

func TestContextOverflowOptionNegativeIsDisabled(t *testing.T) {
	ctx := context.Background()
	overflow := overflowError(100, 300)
	m := &scriptModel{}
	m.pick = func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, overflow
		}
		return mock.Text("ok"), nil
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithMiddleware(dropOldest{n: 1}),
		agent.WithContextOverflowRetry(-2),
		agent.WithCheckpointer(seededStore(t, overflowHistory(3))),
	)
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread(overflowThread)))
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want a negative option to behave like 0", got)
	}
	if !errors.Is(runErr, overflow) {
		t.Fatalf("run error = %v, want the overflow", runErr)
	}
	if got := retriedEvents(events); len(got) != 0 {
		t.Fatalf("ContextOverflowRetried events = %d, want none", len(got))
	}
}
