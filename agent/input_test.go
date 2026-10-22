package agent_test

import (
	"context"
	"errors"
	"iter"
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

// quickTool is a tool that answers at once, so a batch can be exercised without
// parking a worker.
func quickTool(name string) tool.Tool {
	return tool.New(name, "answers immediately", func(_ *tool.Context, _ struct{}) (string, error) {
		return name + "-done", nil
	})
}

// blockedCallModel records every request, answers from a script, and parks one
// chosen call until the test releases it. Parking a call is how a test injects
// input after that call's PrepareTurn has already drained.
type blockedCallModel struct {
	mu      sync.Mutex
	reqs    []*llm.Request
	pick    func(call int, req *llm.Request) *llm.Response
	blockAt int
	entered chan struct{}
	release chan struct{}
}

func newBlockedCallModel(blockAt int, pick func(call int, req *llm.Request) *llm.Response) *blockedCallModel {
	return &blockedCallModel{
		blockAt: blockAt,
		pick:    pick,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (m *blockedCallModel) Name() string { return "blocked" }

func (m *blockedCallModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		call := len(m.reqs)
		m.mu.Unlock()
		if call == m.blockAt {
			close(m.entered)
			<-m.release
		}
		yield(m.pick(call, req), nil)
	}
}

func (m *blockedCallModel) history() [][]core.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]core.Message, len(m.reqs))
	for i, r := range m.reqs {
		out[i] = r.Messages
	}
	return out
}

// injectWhileParked steers each message once the parked call has started, then
// releases it. It returns one ticket per message, in order.
func injectWhileParked(t *testing.T, run *agent.Run, m *blockedCallModel, opts ...agent.SteerOptions) []*agent.SteerTicket {
	t.Helper()
	releaseOnce := sync.OnceFunc(func() { close(m.release) })
	t.Cleanup(releaseOnce)
	tickets := make(chan *agent.SteerTicket, len(opts))
	go func() {
		<-m.entered
		for _, o := range opts {
			msg := core.UserText("injected")
			tk, err := run.SteerWith(msg, o)
			if err != nil {
				t.Errorf("SteerWith(%q): %v", o.Phase, err)
			}
			tickets <- tk
		}
		releaseOnce()
	}()
	out := make([]*agent.SteerTicket, 0, len(opts))
	for i := 0; i < len(opts); i++ {
		select {
		case tk := <-tickets:
			out = append(out, tk)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out injecting into the parked call")
		}
	}
	return out
}

func hasText(msgs []core.Message, want string) bool {
	for _, m := range msgs {
		if m.Text() == want {
			return true
		}
	}
	return false
}

func deliveredSteps(t *testing.T, tk *agent.SteerTicket) (int, bool) {
	t.Helper()
	out, err := tk.Wait(context.Background())
	if err != nil {
		t.Fatalf("ticket Wait: %v", err)
	}
	return out.Step, out.Delivered
}

func deliveredEvents(events []core.Event) []core.InputDelivered {
	var out []core.InputDelivered
	for _, ev := range events {
		if e, ok := ev.(core.InputDelivered); ok {
			out = append(out, e)
		}
	}
	return out
}

// TestPhaseDeliveryAtFinalAnswerSettlesAll is the turn-boundary case: a final
// answer ends the turn, so every phase — next_turn included — is delivered by
// settleTurn, and none of it reaches the call that was already in flight.
func TestPhaseDeliveryAtFinalAnswerSettlesAll(t *testing.T) {
	gate := newModelGate()
	releaseGates(t, gate)
	m := &turnGatedModel{gates: []*modelGate{gate}, answers: []string{"first", "second"}}
	a, err := agent.New(agent.WithModel(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	run := a.Stream(context.Background(), "start")

	phases := []core.InputPhase{
		core.InputBeforeModel, core.InputAfterModel, core.InputAfterTools, core.InputNextTurn,
	}
	tickets := make(chan *agent.SteerTicket, len(phases))
	go func() {
		<-gate.started
		for _, p := range phases {
			tk, err := run.SteerWith(core.UserText("p:"+string(p)), agent.SteerOptions{Phase: p})
			if err != nil {
				t.Errorf("SteerWith(%q): %v", p, err)
			}
			tickets <- tk
		}
		gate.release()
	}()
	collected := make(chan []core.Event, 1)
	go func() {
		events, res, err := collectRun(t, run)
		if err != nil {
			t.Errorf("run: %v", err)
		}
		if got := res.Message.Text(); got != "second" {
			t.Errorf("result = %q, want second", got)
		}
		collected <- events
	}()
	events := <-collected

	reqs := m.history()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2", len(reqs))
	}
	for i, p := range phases {
		step, got := deliveredSteps(t, <-tickets)
		if !got || step != 1 {
			t.Fatalf("%s delivered at step %d (%v), want step 1", p, step, got)
		}
		if hasText(reqs[0], "p:"+string(p)) {
			t.Fatalf("phase %d reached the call already in flight", i)
		}
		if !hasText(reqs[1], "p:"+string(p)) {
			t.Fatalf("phase %d missing from the next turn", i)
		}
	}
	if len(deliveredEvents(events)) == 0 {
		t.Fatal("no InputDelivered event was published")
	}
}

// TestVolatilePhasesSurviveToolBatch is the mid-turn case: the step after a tool
// batch is not a new turn, so before_model, after_model and after_tools arrive
// while next_turn stays queued until the turn restarts.
func TestVolatilePhasesSurviveToolBatch(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	m := &scriptModel{pick: func(call int, _ *llm.Request) (*llm.Response, error) {
		switch call {
		case 1:
			return mock.CallTool("c1", "block", `{}`), nil
		case 2:
			return mock.Text("mid"), nil
		default:
			return mock.Text("final"), nil
		}
	}}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithTools(steeringBlockingTool("block", started, release)),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "start")
	phases := []core.InputPhase{
		core.InputBeforeModel, core.InputAfterModel, core.InputAfterTools, core.InputNextTurn,
	}
	tickets := make(chan *agent.SteerTicket, len(phases))
	go func() {
		// Injecting while the batch is parked puts the queue in front of step 1's
		// PrepareTurn, which is a mid-turn delivery point.
		<-started
		for _, p := range phases {
			tk, err := run.SteerWith(core.UserText("p:"+string(p)), agent.SteerOptions{Phase: p})
			if err != nil {
				t.Errorf("SteerWith(%q): %v", p, err)
			}
			tickets <- tk
		}
		releaseOnce.Do(func() { close(release) })
	}()
	_, res, err := collectRun(t, run)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := res.Message.Text(); got != "final" {
		t.Fatalf("result = %q, want final", got)
	}

	history := m.history()
	if len(history) != 3 {
		t.Fatalf("model calls = %d, want 3", len(history))
	}
	for _, p := range phases {
		text := "p:" + string(p)
		want := hasText(history[1], text)
		if p == core.InputNextTurn {
			if want {
				t.Fatalf("next_turn arrived mid-turn: %v", history[1])
			}
			if !hasText(history[2], text) {
				t.Fatalf("next_turn never arrived: %v", history[2])
			}
		} else if !want {
			t.Fatalf("%s was kept out of the step after the tool batch: %v", p, history[1])
		}
	}
	for _, p := range phases {
		tk := <-tickets
		step, got := deliveredSteps(t, tk)
		wantStep := 1
		if p == core.InputNextTurn {
			wantStep = 2
		}
		if !got || step != wantStep {
			t.Fatalf("%s delivered at step %d (%v), want step %d", p, step, got, wantStep)
		}
	}
}

// TestVolatileInputsPrecedeDurableInputs pins the delivery order and the event
// fields for both channels. Steering before the run starts is enough to be in
// the same PrepareTurn: driving begins lazily, on the first Iter or Wait.
func TestVolatileInputsPrecedeDurableInputs(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	if _, err := store.Deliver(ctx, "order-thread", checkpoint.InboxMessage{
		ID:      "inbox-1",
		Message: core.UserText("durable one"),
	}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	m := &scriptModel{pick: func(call int, _ *llm.Request) (*llm.Response, error) {
		return mock.Text("done"), nil
	}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "start", agent.OnThread("order-thread"))
	if _, err := run.Steer(core.UserText("volatile one")); err != nil {
		t.Fatalf("Steer: %v", err)
	}
	events, _, err := collectRun(t, run)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	history := m.history()
	if len(history) != 1 {
		t.Fatalf("model calls = %d, want 1", len(history))
	}
	var vi, di = -1, -1
	for i, msg := range history[0] {
		switch msg.Text() {
		case "volatile one":
			vi = i
		case "durable one":
			di = i
		}
	}
	if vi < 0 || di < 0 {
		t.Fatalf("inputs missing from the request: %v", history[0])
	}
	if vi > di {
		t.Fatalf("volatile input at %d came after durable at %d, want volatile first", vi, di)
	}

	var sawVolatile, sawDurable bool
	for _, e := range deliveredEvents(events) {
		switch e.Source {
		case core.InputVolatile:
			sawVolatile = true
			if len(e.MessageIDs) != 0 {
				t.Fatalf("volatile delivery event carries IDs %v, want none", e.MessageIDs)
			}
			if e.Count != 1 || e.Phase != core.InputBeforeModel || e.Preempted {
				t.Fatalf("volatile delivery event = %+v, want count 1, before_model, not preempted", e)
			}
		case core.InputDurable:
			sawDurable = true
			if e.Count != 1 || len(e.MessageIDs) != 1 || e.MessageIDs[0] != "inbox-1" {
				t.Fatalf("durable delivery event = %+v, want one ID inbox-1", e)
			}
			if e.Step != 0 {
				t.Fatalf("durable delivery step = %d, want 0", e.Step)
			}
		}
	}
	if !sawVolatile || !sawDurable {
		t.Fatalf("delivery events missing volatile=%v durable=%v", sawVolatile, sawDurable)
	}
}

// TestResumedRunFirstStepIsNotNewTurn is the resume case: a resumed run continues
// the turn it paused in, so a next_turn message queued before its first step must
// wait, while before_model input arrives at once.
func TestResumedRunFirstStepIsNotNewTurn(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	m := &turnGatedModel{answers: []string{"first", "first", "second"}}
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
	// Steering before the resumed run starts driving puts both messages in front of
	// its very first PrepareTurn, which is where the turn boundary matters.
	later, err := resumed.SteerWith(core.UserText("wait for the turn"), agent.SteerOptions{
		Phase: core.InputNextTurn,
	})
	if err != nil {
		t.Fatalf("SteerWith next_turn: %v", err)
	}
	now, err := resumed.SteerWith(core.UserText("arrive now"), agent.SteerOptions{})
	if err != nil {
		t.Fatalf("SteerWith before_model: %v", err)
	}

	res, err := resumed.Wait()
	if err != nil {
		t.Fatalf("resumed Wait: %v", err)
	}
	if got := res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want second", got)
	}

	reqs := m.history()
	if len(reqs) < 3 {
		t.Fatalf("model calls = %d, want at least 3", len(reqs))
	}
	if !hasText(reqs[1], "arrive now") {
		t.Fatalf("the resumed first step skipped before_model input: %v", reqs[1])
	}
	if hasText(reqs[1], "wait for the turn") {
		t.Fatal("the resumed run's first step delivered a next_turn message")
	}
	if !hasText(reqs[2], "wait for the turn") {
		t.Fatalf("next_turn message never delivered: %v", reqs[2])
	}
	step, delivered := deliveredSteps(t, now)
	if !delivered || step != 0 {
		t.Fatalf("before_model ticket = step %d delivered=%v, want step 0", step, delivered)
	}
	step, delivered = deliveredSteps(t, later)
	if !delivered || step != 1 {
		t.Fatalf("next_turn ticket = step %d delivered=%v, want step 1", step, delivered)
	}
}

// TestMaxTokensContinuationIsNotNewTurn covers the truncated-reply step: its tool
// calls were answered with errors instead of run, so the step that follows is
// still the same turn and next_turn input keeps waiting.
func TestMaxTokensContinuationIsNotNewTurn(t *testing.T) {
	cut := mock.CallTool("c1", "quick", `{}`)
	cut.StopReason = llm.StopMaxTokens
	m := newBlockedCallModel(1, func(call int, _ *llm.Request) *llm.Response {
		switch call {
		case 1:
			return cut
		case 2:
			return mock.Text("mid")
		default:
			return mock.Text("final")
		}
	})
	a, err := agent.New(agent.WithModel(m), agent.WithTools(quickTool("quick")))
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "start")
	driven := make(chan []core.Event, 1)
	go func() {
		events, res, err := collectRun(t, run)
		if err != nil {
			t.Errorf("run: %v", err)
		}
		if got := res.Message.Text(); got != "final" {
			t.Errorf("result = %q, want final", got)
		}
		driven <- events
	}()
	tickets := injectWhileParked(t, run, m, agent.SteerOptions{Phase: core.InputNextTurn})
	<-driven

	history := m.history()
	if len(history) != 3 {
		t.Fatalf("model calls = %d, want 3", len(history))
	}
	if hasText(history[1], "injected") {
		t.Fatal("the step after a truncated reply delivered a next_turn message")
	}
	if !hasText(history[2], "injected") {
		t.Fatalf("next_turn message never arrived after the turn restarted: %v", history[2])
	}
	step, delivered := deliveredSteps(t, tickets[0])
	if !delivered || step != 2 {
		t.Fatalf("next_turn ticket = step %d delivered=%v, want step 2", step, delivered)
	}
}

// TestInboxReadFailureStillFailsStep keeps the existing failure path: an inbox
// read error ends the run, and a message already taken from the volatile queue
// stays settled the way it was before phases existed.
func TestInboxReadFailureStillFailsStep(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("inbox unreadable")
	store := &failingInboxStore{Durable: checkpoint.NewMemory(), failAt: 1, err: readErr}
	m := &scriptModel{pick: func(call int, _ *llm.Request) (*llm.Response, error) {
		return mock.Text("done"), nil
	}}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start")
	if _, err := run.Steer(core.UserText("volatile")); err != nil {
		t.Fatalf("Steer: %v", err)
	}
	if _, err := run.Wait(); !errors.Is(err, readErr) {
		t.Fatalf("Wait error = %v, want the inbox error", err)
	}
}
