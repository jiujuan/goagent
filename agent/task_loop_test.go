package agent_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
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
)

// modelGate parks one model call until the test releases it.
type modelGate struct {
	started  chan struct{}
	released chan struct{}
	once     sync.Once
}

func newModelGate() *modelGate {
	return &modelGate{started: make(chan struct{}), released: make(chan struct{})}
}

func (g *modelGate) release() { g.once.Do(func() { close(g.released) }) }

// turnGatedModel answers call n with answers[n-1]. A nil gate at position n leaves
// that call unblocked; a non-nil gate closes its started channel and waits for
// released before answering. Each gate belongs to exactly one call.
type turnGatedModel struct {
	mu      sync.Mutex
	reqs    []*llm.Request
	gates   []*modelGate
	answers []string
	before  func(call int)
}

func (m *turnGatedModel) Name() string { return "gated" }

func (m *turnGatedModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		call := len(m.reqs)
		m.mu.Unlock()
		if m.before != nil {
			m.before(call)
		}
		if call <= len(m.gates) && m.gates[call-1] != nil {
			gate := m.gates[call-1]
			close(gate.started)
			<-gate.released
		}
		yield(mock.Text(m.answers[call-1]), nil)
	}
}

func (m *turnGatedModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reqs)
}

func (m *turnGatedModel) history() [][]core.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]core.Message, len(m.reqs))
	for i, r := range m.reqs {
		out[i] = r.Messages
	}
	return out
}

// releaseGates makes sure every gate is released even when a test fails before
// its steering goroutine does.
func releaseGates(t *testing.T, gates ...*modelGate) {
	t.Helper()
	t.Cleanup(func() {
		for _, g := range gates {
			if g != nil {
				g.release()
			}
		}
	})
}

// steerWhenStarted waits until the gated model call is in flight, steers a
// message into the run, then releases that call. The returned channel yields the
// ticket Steer returned.
func steerWhenStarted(t *testing.T, run *agent.Run, gate *modelGate, text string) <-chan *agent.SteerTicket {
	t.Helper()
	tickets := make(chan *agent.SteerTicket, 1)
	go func() {
		<-gate.started
		ticket, err := run.Steer(core.UserText(text))
		if err != nil {
			t.Errorf("Steer(%q): %v", text, err)
		}
		tickets <- ticket
		gate.release()
	}()
	return tickets
}

// awaitTicket bounds a ticket wait so a continuation that never happens fails
// the test instead of hanging it.
func awaitTicket(t *testing.T, ch <-chan *agent.SteerTicket) *agent.SteerTicket {
	t.Helper()
	select {
	case ticket := <-ch:
		return ticket
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the steering ticket")
		return nil
	}
}

func awaitErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the steering result")
		return nil
	}
}

// collectRun drives a run to its end through Iter, then returns the events it
// saw together with the frozen result.
func collectRun(t *testing.T, run *agent.Run) ([]core.Event, core.Result, error) {
	t.Helper()
	var events []core.Event
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatalf("run event: %v", err)
		}
		if ev != nil {
			events = append(events, ev)
		}
	}
	res, err := run.Wait()
	return events, res, err
}

func turnStartSteps(events []core.Event) []int {
	var steps []int
	for _, ev := range events {
		if e, ok := ev.(core.TurnStarted); ok {
			steps = append(steps, e.Step)
		}
	}
	return steps
}

func hasAssistantText(messages []core.Message, text string) bool {
	for _, m := range messages {
		if m.Role == core.RoleAssistant && m.Text() == text {
			return true
		}
	}
	return false
}

func turnDoneSteps(events []core.Event) []int {
	var steps []int
	for _, ev := range events {
		if e, ok := ev.(core.TurnDone); ok {
			steps = append(steps, e.Step)
		}
	}
	return steps
}

func countEvents[T any](events []core.Event) int {
	n := 0
	for _, ev := range events {
		if _, ok := ev.(T); ok {
			n++
		}
	}
	return n
}

func TestFinalSteeringStartsNextTurn(t *testing.T) {
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	a, err := agent.New(agent.WithModel(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(context.Background(), "start")
	tickets := steerWhenStarted(t, run, gate, "follow-up")
	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	ticket := awaitTicket(t, tickets)
	if ticket == nil {
		t.Fatal("Steer returned no ticket")
	}
	out := ticket.Outcome()
	if !out.Delivered || out.Step != 1 || out.Err != nil {
		t.Fatalf("ticket outcome = %+v, want delivered at step 1", out)
	}
	if m.calls() != 2 {
		t.Fatalf("model calls = %d, want 2", m.calls())
	}
	if got := digest(m.history()[1]); !strings.Contains(got, "follow-up") {
		t.Fatalf("second model request = %q, want the steered message", got)
	}
}

func TestFinalInboxStartsNextTurnOnce(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start")
	go func() {
		<-gate.started
		if _, err := store.Deliver(ctx, run.ThreadID, checkpoint.InboxMessage{
			ID:      "m1",
			Message: core.UserText("inbox-message"),
		}); err != nil {
			t.Errorf("Deliver: %v", err)
		}
		gate.release()
	}()

	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	if m.calls() != 2 {
		t.Fatalf("model calls = %d, want 2", m.calls())
	}
	if got := strings.Count(digest(m.history()[1]), "inbox-message"); got != 1 {
		t.Fatalf("inbox message appears %d times in the second request, want 1", got)
	}
	inbox, err := store.Inbox(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("inbox still holds %d messages after the run, want 0", len(inbox))
	}
}

func TestConsumedInboxDoesNotContinue(t *testing.T) {
	ctx := context.Background()
	const threadID = "thread-consumed-inbox"
	store := checkpoint.NewMemory()
	if _, err := store.Deliver(ctx, threadID, checkpoint.InboxMessage{
		ID:      "m1",
		Message: core.UserText("inbox-message"),
	}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	m := &scriptModel{pick: func(int, *llm.Request) (*llm.Response, error) {
		return mock.Text("done"), nil
	}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	events, res, err := collectRun(t, a.Stream(ctx, "start", agent.OnThread(threadID)))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	if steps := turnStartSteps(events); !reflect.DeepEqual(steps, []int{0}) {
		t.Fatalf("TurnStarted steps = %v, want [0]", steps)
	}
	if m.calls() != 1 {
		t.Fatalf("model calls = %d, want 1", m.calls())
	}
	if got := strings.Count(digest(m.history()[0]), "inbox-message"); got != 1 {
		t.Fatalf("inbox message appears %d times in the request, want 1", got)
	}
	inbox, err := store.Inbox(ctx, threadID)
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("inbox still holds %d messages, want 0", len(inbox))
	}
}

// countingInboxStore counts durable inbox reads. It embeds only Durable, so the
// wrapper does not accidentally implement ThreadCoordinator.
type countingInboxStore struct {
	checkpoint.Durable
	mu    sync.Mutex
	reads int
}

func (s *countingInboxStore) Inbox(ctx context.Context, threadID string) ([]checkpoint.InboxMessage, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.Durable.Inbox(ctx, threadID)
}

func (s *countingInboxStore) inboxReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func TestSettleTurnSkipsInboxWhenSteeringPending(t *testing.T) {
	store := &countingInboxStore{Durable: checkpoint.NewMemory()}
	gate := newModelGate()
	releaseGates(t, gate)
	var readsAtSecondCall atomic.Int32
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	m.before = func(call int) {
		if call == 2 {
			readsAtSecondCall.Store(int32(store.inboxReads()))
		}
	}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(context.Background(), "start")
	tickets := steerWhenStarted(t, run, gate, "follow-up")
	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	if got := readsAtSecondCall.Load(); got != 2 {
		t.Fatalf("inbox reads before the second model call = %d, want 2", got)
	}
	if out := awaitTicket(t, tickets).Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("ticket outcome = %+v, want delivered at step 1", out)
	}
}

// blockingInboxStore blocks the blockAt-th inbox read until released, and makes
// that blocked read return an empty inbox.
type blockingInboxStore struct {
	checkpoint.Durable
	blockAt  int
	started  chan struct{}
	released chan struct{}
	mu       sync.Mutex
	calls    int
}

func (s *blockingInboxStore) Inbox(ctx context.Context, threadID string) ([]checkpoint.InboxMessage, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == s.blockAt {
		close(s.started)
		<-s.released
		return nil, nil
	}
	return s.Durable.Inbox(ctx, threadID)
}

func TestSteerDuringInboxReadStartsNextTurn(t *testing.T) {
	ctx := context.Background()
	store := &blockingInboxStore{
		Durable:  checkpoint.NewMemory(),
		blockAt:  2,
		started:  make(chan struct{}),
		released: make(chan struct{}),
	}
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start")
	tickets := make(chan *agent.SteerTicket, 1)
	steerErrs := make(chan error, 1)
	finished := make(chan struct{})
	var releaseInbox sync.Once
	t.Cleanup(func() { releaseInbox.Do(func() { close(store.released) }) })
	go func() {
		<-gate.started
		gate.release()
		select {
		case <-store.started:
		case <-finished:
		}
		ticket, err := run.Steer(core.UserText("late"))
		tickets <- ticket
		steerErrs <- err
		releaseInbox.Do(func() { close(store.released) })
	}()

	res, err := run.Wait()
	close(finished)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	if err := awaitErr(t, steerErrs); err != nil {
		t.Fatalf("Steer: %v", err)
	}
	if out := awaitTicket(t, tickets).Outcome(); !out.Delivered || out.Step != 1 || out.Err != nil {
		t.Fatalf("ticket outcome = %+v, want delivered at step 1", out)
	}
	if got := digest(m.history()[1]); !strings.Contains(got, "late") {
		t.Fatalf("second model request = %q, want the steered message", got)
	}
}

func TestFinalTurnBudgetSettlesSteering(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first"}}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithCheckpointer(store),
		agent.WithMaxTurns(1),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start")
	tickets := make(chan *agent.SteerTicket, 1)
	go func() {
		<-gate.started
		ticket, err := run.Steer(core.UserText("late"))
		if err != nil {
			t.Errorf("Steer: %v", err)
		}
		tickets <- ticket
		if _, err := store.Deliver(ctx, run.ThreadID, checkpoint.InboxMessage{
			ID:      "m1",
			Message: core.UserText("inbox-message"),
		}); err != nil {
			t.Errorf("Deliver: %v", err)
		}
		gate.release()
	}()

	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "first" {
		t.Fatalf("result = %q, want first", got)
	}
	ticket := awaitTicket(t, tickets)
	if ticket == nil {
		t.Fatal("Steer returned no ticket")
	}
	if out := ticket.Outcome(); out.Delivered || !errors.Is(out.Err, agent.ErrMaxTurnsExceeded) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrMaxTurnsExceeded", out)
	}
	if late, err := run.Steer(core.UserText("after")); late != nil || !errors.Is(err, agent.ErrRunClosed) {
		t.Fatalf("Steer after the run ended = (%v, %v), want (nil, ErrRunClosed)", late, err)
	}
	inbox, err := store.Inbox(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 1 {
		t.Fatalf("inbox holds %d messages, want the undelivered one", len(inbox))
	}
	if m.calls() != 1 {
		t.Fatalf("model calls = %d, want 1", m.calls())
	}
}

// failingInboxStore fails the failAt-th inbox read.
type failingInboxStore struct {
	checkpoint.Durable
	failAt int
	err    error
	mu     sync.Mutex
	calls  int
}

func (s *failingInboxStore) Inbox(ctx context.Context, threadID string) ([]checkpoint.InboxMessage, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == s.failAt {
		return nil, s.err
	}
	return s.Durable.Inbox(ctx, threadID)
}

func TestInboxReadFailureFailsAfterFinal(t *testing.T) {
	ctx := context.Background()
	errInbox := errors.New("inbox unavailable")
	store := &failingInboxStore{Durable: checkpoint.NewMemory(), failAt: 2, err: errInbox}
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first"}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start")
	go func() {
		<-gate.started
		if _, err := store.Deliver(ctx, run.ThreadID, checkpoint.InboxMessage{
			ID:      "m1",
			Message: core.UserText("inbox-message"),
		}); err != nil {
			t.Errorf("Deliver: %v", err)
		}
		gate.release()
	}()

	_, waitErr := run.Wait()
	if !errors.Is(waitErr, errInbox) {
		t.Fatalf("Wait error = %v, want %v", waitErr, errInbox)
	}
	head, err := store.Latest(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if head == nil || len(head.State.Messages) == 0 {
		t.Fatal("no checkpoint was written for the failed run")
	}
	if got := head.State.Messages[len(head.State.Messages)-1].Text(); got != "first" {
		t.Fatalf("checkpointed final message = %q, want first", got)
	}
	inbox, err := store.Inbox(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 1 {
		t.Fatalf("inbox holds %d messages, want the unacknowledged one", len(inbox))
	}
	if late, err := run.Steer(core.UserText("after")); late != nil || !errors.Is(err, agent.ErrRunClosed) {
		t.Fatalf("Steer after the failed run = (%v, %v), want (nil, ErrRunClosed)", late, err)
	}
}

// claimRecordingStore records every commit request and release. It embeds only
// Durable, so no write guard is involved and the checkpoint's own release flag
// is what reaches the store.
type claimRecordingStore struct {
	checkpoint.Durable
	mu       sync.Mutex
	commits  []checkpoint.CommitRequest
	claims   []checkpoint.ClaimRequest
	releases int
}

func (s *claimRecordingStore) Claim(ctx context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	s.mu.Lock()
	s.claims = append(s.claims, req)
	s.mu.Unlock()
	return s.Durable.Claim(ctx, req)
}

func (s *claimRecordingStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	s.commits = append(s.commits, req)
	s.mu.Unlock()
	return s.Durable.Commit(ctx, req)
}

func (s *claimRecordingStore) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	s.mu.Lock()
	s.releases++
	s.mu.Unlock()
	return s.Durable.ReleaseClaim(ctx, claim)
}

func (s *claimRecordingStore) recordedCommits() []checkpoint.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.CommitRequest(nil), s.commits...)
}

func (s *claimRecordingStore) recordedClaims() []checkpoint.ClaimRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.ClaimRequest(nil), s.claims...)
}

func (s *claimRecordingStore) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releases
}

// interruptAfterFinalOnce pauses the first run after its first final answer,
// which leaves the replay-model pause a later Resume claims.
type interruptAfterFinalOnce struct {
	agent.BaseMiddleware
	once sync.Once
}

func (m *interruptAfterFinalOnce) AfterModel(*agent.LoopContext, *llm.Response) (core.Directive, error) {
	pause := false
	m.once.Do(func() { pause = true })
	if pause {
		return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
	}
	return core.Directive{}, nil
}

func TestContinuationKeepsClaim(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{
		gates:   []*modelGate{nil, gate, nil},
		answers: []string{"first", "first", "second"},
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithCheckpointer(store),
		agent.WithMiddleware(&interruptAfterFinalOnce{}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	paused := a.Stream(ctx, "start")
	if _, err := paused.Wait(); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if !paused.Paused() {
		t.Fatal("first run did not pause")
	}

	resumed, err := paused.Resume(ctx)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	tickets := steerWhenStarted(t, resumed, gate, "follow-up")
	res, err := resumed.Wait()
	if err != nil {
		t.Fatalf("resumed Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	if out := awaitTicket(t, tickets).Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("ticket outcome = %+v, want delivered at step 1", out)
	}

	var continuing, ending bool
	for _, req := range store.recordedCommits() {
		messages := req.Checkpoint.State.Messages
		if len(messages) == 0 {
			continue
		}
		last := messages[len(messages)-1].Text()
		switch {
		case req.Checkpoint.Step == 0 && hasAssistantText(messages, "first"):
			continuing = true
			if req.ReleaseClaim {
				t.Fatal("the checkpoint that started another turn released the claim")
			}
		case req.Checkpoint.Step == 1 && last == "second":
			ending = true
			if !req.ReleaseClaim {
				t.Fatal("the checkpoint that ended the run kept the claim")
			}
		}
	}
	if !continuing {
		t.Fatal("no continuing final checkpoint was committed")
	}
	if !ending {
		t.Fatal("no ending final checkpoint was committed")
	}
	// The ending checkpoint carries ReleaseClaim=true, so the claim is released
	// by the same durable commit that ends the run. The run must not need a
	// second, separate release call afterwards.
	if got := store.releaseCount(); got != 0 {
		t.Fatalf("separate ReleaseClaim calls = %d, want 0: the ending checkpoint released the claim", got)
	}
}

func TestContinuationEventOrder(t *testing.T) {
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	a, err := agent.New(agent.WithModel(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(context.Background(), "start")
	tickets := steerWhenStarted(t, run, gate, "follow-up")
	events, res, err := collectRun(t, run)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}
	if out := awaitTicket(t, tickets).Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("ticket outcome = %+v, want delivered at step 1", out)
	}
	if steps := turnStartSteps(events); !reflect.DeepEqual(steps, []int{0, 1}) {
		t.Fatalf("TurnStarted steps = %v, want [0 1]", steps)
	}
	if steps := turnDoneSteps(events); !reflect.DeepEqual(steps, []int{0, 1}) {
		t.Fatalf("TurnDone steps = %v, want [0 1]", steps)
	}
	if n := countEvents[core.RunDone](events); n != 1 {
		t.Fatalf("RunDone events = %d, want 1", n)
	}
	if len(events) == 0 {
		t.Fatal("no events were observed")
	}
	if _, ok := events[len(events)-1].(core.RunDone); !ok {
		t.Fatalf("last event = %T, want RunDone", events[len(events)-1])
	}
}

func TestContinuationRespectsTotalBudget(t *testing.T) {
	gates := []*modelGate{newModelGate(), newModelGate(), newModelGate()}
	releaseGates(t, gates...)
	m := &turnGatedModel{gates: gates, answers: []string{"one", "two", "three"}}
	a, err := agent.New(agent.WithModel(m), agent.WithMaxTurns(3))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(context.Background(), "start")
	var tickets []<-chan *agent.SteerTicket
	for i, gate := range gates {
		tickets = append(tickets, steerWhenStarted(t, run, gate, fmt.Sprintf("s%d", i+1)))
	}
	events, res, err := collectRun(t, run)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "three" {
		t.Fatalf("result = %q, want three", got)
	}
	if steps := turnStartSteps(events); !reflect.DeepEqual(steps, []int{0, 1, 2}) {
		t.Fatalf("TurnStarted steps = %v, want [0 1 2]", steps)
	}
	for i, ch := range tickets {
		out := awaitTicket(t, ch).Outcome()
		if i < 2 {
			if !out.Delivered || out.Step != i+1 || out.Err != nil {
				t.Fatalf("ticket %d outcome = %+v, want delivered at step %d", i+1, out, i+1)
			}
			continue
		}
		if out.Delivered || !errors.Is(out.Err, agent.ErrMaxTurnsExceeded) {
			t.Fatalf("ticket %d outcome = %+v, want undelivered ErrMaxTurnsExceeded", i+1, out)
		}
	}
	last := digest(m.history()[2])
	if !strings.Contains(last, "s1") || !strings.Contains(last, "s2") {
		t.Fatalf("third model request = %q, want both earlier additions", last)
	}
	if strings.Contains(last, "s3") {
		t.Fatalf("third model request = %q, does not expect the over-budget addition", last)
	}
	if m.calls() != 3 {
		t.Fatalf("model calls = %d, want 3", m.calls())
	}
}

// steerAfterFinalOnce injects one message as the first final answer is settling,
// standing in for a caller that only has a Session (and no Run handle).
type steerAfterFinalOnce struct {
	agent.BaseMiddleware
	once sync.Once
	err  error
}

func (m *steerAfterFinalOnce) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if len(resp.Message.ToolCalls()) == 0 {
		m.once.Do(func() {
			_, m.err = lc.Steer(core.UserText("more"))
		})
	}
	return core.Directive{}, nil
}

func TestSessionSendWaitsForContinuation(t *testing.T) {
	ctx := context.Background()
	m := &scriptModel{pick: func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return mock.Text("first"), nil
		}
		return mock.Text("second"), nil
	}}
	steer := &steerAfterFinalOnce{}
	a, err := agent.New(agent.WithModel(m), agent.WithMiddleware(steer))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	session, err := a.OpenSession(ctx, "thread-session-steer")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	res, err := session.Send(ctx, "start")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if steer.err != nil {
		t.Fatalf("Steer from middleware: %v", steer.err)
	}
	if got := res.Result.Message.Text(); got != "second" {
		t.Fatalf("Send result = %q, want the second final answer", got)
	}
	if m.calls() != 2 {
		t.Fatalf("model calls = %d, want 2", m.calls())
	}
}

// stopAfterModel ends every run right after its model reply.
type stopAfterModel struct{ agent.BaseMiddleware }

func (stopAfterModel) AfterModel(*agent.LoopContext, *llm.Response) (core.Directive, error) {
	return core.Directive{Kind: core.Stop, Reason: "test stop"}, nil
}

func TestPauseSettlesSteeringAsInterrupted(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	first := newModelGate()
	releaseGates(t, first)
	model := &scriptModel{pick: func(call int, req *llm.Request) (*llm.Response, error) {
		if call == 1 {
			close(first.started)
			<-first.released
			return mock.CallTool("c1", "blocked", `{}`), nil
		}
		return mock.Text("resumed done"), nil
	}}
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(blockedTool()),
		agent.WithMiddleware(terminalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("tl04_pause"))
	tickets := steerWhenStarted(t, run, first, "late")
	if _, err := run.Wait(); err != nil {
		t.Fatalf("paused Wait: %v", err)
	}
	if !run.Paused() {
		t.Fatal("the run did not pause")
	}
	if out := awaitTicket(t, tickets).Outcome(); out.Delivered || !errors.Is(out.Err, agent.ErrRunInterrupted) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunInterrupted", out)
	}
	if _, err := run.Steer(core.UserText("after pause")); !errors.Is(err, agent.ErrRunInterrupted) {
		t.Fatalf("Steer after pause = %v, want ErrRunInterrupted", err)
	}

	cp, err := store.Latest(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if cp == nil {
		t.Fatal("the pause wrote no checkpoint")
	}
	for _, m := range cp.State.Messages {
		if m.Text() == "late" {
			t.Fatalf("the paused checkpoint kept the undelivered message: %s", digest(cp.State.Messages))
		}
	}

	resumed, err := a.Resume(ctx, run.ThreadID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	resumedTicket, err := resumed.Steer(core.UserText("after resume"))
	if err != nil {
		t.Fatalf("Steer on the resumed run: %v", err)
	}
	res, err := resumed.Wait()
	if err != nil {
		t.Fatalf("resumed Wait: %v", err)
	}
	if got := res.Message.Text(); got != "resumed done" {
		t.Fatalf("resumed result = %q, want resumed done", got)
	}
	if out := resumedTicket.Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("resumed ticket outcome = %+v, want delivered at step 1", out)
	}
}

func TestStopSettlesSteeringAsStopped(t *testing.T) {
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first"}}
	a, err := agent.New(agent.WithModel(m), agent.WithMiddleware(stopAfterModel{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(context.Background(), "start")
	tickets := steerWhenStarted(t, run, gate, "late")
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if out := awaitTicket(t, tickets).Outcome(); out.Delivered || !errors.Is(out.Err, agent.ErrRunStopped) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunStopped", out)
	}
	if _, err := run.Steer(core.UserText("after stop")); !errors.Is(err, agent.ErrRunStopped) {
		t.Fatalf("Steer after stop = %v, want ErrRunStopped", err)
	}
}

func TestTransferSettlesParentSteering(t *testing.T) {
	ctx := context.Background()
	parentGate := newModelGate()
	targetGate := newModelGate()
	releaseGates(t, parentGate, targetGate)
	parentModel := &scriptModel{pick: func(int, *llm.Request) (*llm.Response, error) {
		close(parentGate.started)
		<-parentGate.released
		return mock.CallTool("t1", "transfer_to_agent", `{"agent":"target"}`), nil
	}}
	targetModel := &turnGatedModel{gates: []*modelGate{targetGate}, answers: []string{"delegated"}}
	target, err := agent.New(agent.WithName("target"), agent.WithModel(targetModel))
	if err != nil {
		t.Fatalf("New target: %v", err)
	}
	parent, err := agent.New(agent.WithModel(parentModel), agent.WithSubAgents(target))
	if err != nil {
		t.Fatalf("New parent: %v", err)
	}

	run := parent.Stream(ctx, "go")
	waited := make(chan struct {
		res core.Result
		err error
	}, 1)
	go func() {
		res, err := run.Wait()
		waited <- struct {
			res core.Result
			err error
		}{res, err}
	}()
	tickets := steerWhenStarted(t, run, parentGate, "parent late")
	ticket := awaitTicket(t, tickets)
	select {
	case <-targetGate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the transfer target never started")
	}
	select {
	case <-ticket.Done():
	default:
		t.Fatal("the parent ticket was not settled before the target started")
	}
	if out := ticket.Outcome(); out.Delivered || !errors.Is(out.Err, agent.ErrRunStopped) {
		t.Fatalf("parent ticket outcome = %+v, want undelivered ErrRunStopped", out)
	}
	if _, err := run.Steer(core.UserText("after transfer")); !errors.Is(err, agent.ErrRunStopped) {
		t.Fatalf("Steer after transfer = %v, want ErrRunStopped", err)
	}

	targetGate.release()
	select {
	case out := <-waited:
		if out.err != nil {
			t.Fatalf("Wait: %v", out.err)
		}
		if got := out.res.Message.Text(); got != "delegated" {
			t.Fatalf("result = %q, want delegated", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the transferred run never finished")
	}
}

func TestWorkflowRootClosesSteering(t *testing.T) {
	gate := newModelGate()
	releaseGates(t, gate)
	first, err := agent.New(agent.WithModel(mock.New("first", func(*llm.Request) *llm.Response {
		return mock.Text("alpha")
	})))
	if err != nil {
		t.Fatalf("New first: %v", err)
	}
	second, err := agent.New(agent.WithModel(mock.New("second", func(*llm.Request) *llm.Response {
		close(gate.started)
		<-gate.released
		return mock.Text("beta")
	})))
	if err != nil {
		t.Fatalf("New second: %v", err)
	}

	flow := agent.Sequential("flow", first, second)
	run := flow.Stream(context.Background(), "go")
	tickets := steerWhenStarted(t, run, gate, "late")
	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "beta" {
		t.Fatalf("workflow result = %q, want beta", got)
	}
	if out := awaitTicket(t, tickets).Outcome(); out.Delivered || !errors.Is(out.Err, agent.ErrRunClosed) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunClosed", out)
	}
	if _, err := run.Steer(core.UserText("after workflow")); !errors.Is(err, agent.ErrRunClosed) {
		t.Fatalf("Steer after workflow = %v, want ErrRunClosed", err)
	}
}
