package agent_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

type asyncRun struct {
	events []core.Event
	res    core.Result
	err    error
}

// driveAsync drives a run on its own goroutine so the test can inject steering
// while a model call is parked.
func driveAsync(run *agent.Run) <-chan asyncRun {
	done := make(chan asyncRun, 1)
	go func() {
		var events []core.Event
		for ev, err := range run.Iter() {
			if ev != nil {
				events = append(events, ev)
			}
			if err != nil {
				break
			}
		}
		res, err := run.Wait()
		done <- asyncRun{events: events, res: res, err: err}
	}()
	return done
}

// preemptModel records every request and can park a call until it is released or
// its attempt context ends. The ctx-aware branch models a provider that stops
// streaming when canceled; ignoreCancel models one that ignores the signal.
type preemptModel struct {
	mu            sync.Mutex
	reqs          []*llm.Request
	gates         []*modelGate
	partial       func(call int) string
	answer        func(call int) (*llm.Response, error)
	ignoreCancel  bool
	errorOnCancel bool
	afterCancel   func(call int)
}

func (m *preemptModel) Name() string { return "preempt" }

func (m *preemptModel) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		call := len(m.reqs)
		m.mu.Unlock()
		if call <= len(m.gates) && m.gates[call-1] != nil {
			g := m.gates[call-1]
			if m.partial != nil {
				if text := m.partial(call); text != "" {
					if !yield(mock.Partial(text), nil) {
						return
					}
				}
			}
			close(g.started)
			if m.ignoreCancel {
				<-g.released
			} else {
				select {
				case <-g.released:
				case <-ctx.Done():
					if m.afterCancel != nil {
						m.afterCancel(call)
					}
					if m.errorOnCancel {
						yield(nil, ctx.Err())
					}
					return
				}
			}
		}
		resp, err := m.answer(call)
		if err != nil {
			yield(nil, err)
			return
		}
		yield(resp, nil)
	}
}

func (m *preemptModel) requests() []*llm.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*llm.Request(nil), m.reqs...)
}

func (m *preemptModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reqs)
}

func preemptedEvents(events []core.Event) []core.ModelPreempted {
	var out []core.ModelPreempted
	for _, ev := range events {
		if e, ok := ev.(core.ModelPreempted); ok {
			out = append(out, e)
		}
	}
	return out
}

func eventIndexes(events []core.Event, match func(core.Event) bool) []int {
	var out []int
	for i, ev := range events {
		if match(ev) {
			out = append(out, i)
		}
	}
	return out
}

func messageDoneCount(events []core.Event) int {
	n := 0
	for _, ev := range events {
		if _, ok := ev.(core.MessageDone); ok {
			n++
		}
	}
	return n
}

func countRequestsWith(requests []*llm.Request, text string) int {
	n := 0
	for _, req := range requests {
		if hasText(req.Messages, text) {
			n++
		}
	}
	return n
}

func TestPreemptCancelsAndRetriesSameStep(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	m := &preemptModel{
		gates: []*modelGate{gate},
		answer: func(call int) (*llm.Response, error) {
			if call == 1 {
				return mock.Text("first"), nil
			}
			return mock.Text("second"), nil
		},
	}
	counter := &countingMW{}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithInputPreemption(true),
		agent.WithMiddleware(counter),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true, Reason: "user"})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	res := <-done

	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	if got := res.res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want the retried answer", got)
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2 (one attempt plus one retry)", len(reqs))
	}
	if hasText(reqs[0].Messages, "stop") {
		t.Fatal("preempt message reached the attempt it was meant to cancel")
	}
	if !hasText(reqs[1].Messages, "stop") {
		t.Fatal("preempt message missing from the retried request")
	}
	out := ticket.Outcome()
	if !out.Delivered || out.Step != 0 {
		t.Fatalf("ticket = %+v, want delivered at step 0", out)
	}
	if steps := turnStartSteps(res.events); !slices.Equal(steps, []int{0}) {
		t.Fatalf("TurnStarted steps = %v, want [0] (the retry stays in the step)", steps)
	}

	evs := preemptedEvents(res.events)
	if len(evs) != 1 {
		t.Fatalf("ModelPreempted events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Step != 0 || ev.Count != 1 || ev.Reason != "user" || ev.Emitted {
		t.Fatalf("ModelPreempted = %+v, want step 0, count 1, reason user, nothing emitted", ev)
	}
	preIdx := eventIndexes(res.events, func(e core.Event) bool { _, ok := e.(core.ModelPreempted); return ok })
	deliveredIdx := eventIndexes(res.events, func(e core.Event) bool {
		d, ok := e.(core.InputDelivered)
		return ok && d.Preempted
	})
	if len(deliveredIdx) != 1 {
		t.Fatalf("preempted InputDelivered events = %d, want 1", len(deliveredIdx))
	}
	if preIdx[0] >= deliveredIdx[0] {
		t.Fatalf("ModelPreempted at %d must precede InputDelivered at %d", preIdx[0], deliveredIdx[0])
	}
	delivered := res.events[deliveredIdx[0]].(core.InputDelivered)
	if delivered.Step != 0 || delivered.Source != core.InputVolatile || delivered.Phase != core.InputBeforeModel || delivered.Count != 1 {
		t.Fatalf("InputDelivered = %+v, want step 0, volatile, before_model, count 1", delivered)
	}

	before, modify, modelCtx := counter.counts()
	if before != 1 || modify != 1 || modelCtx != 2 {
		t.Fatalf("middleware counts before/modify/modelCtx = %d/%d/%d, want 1/1/2 (no replay, one span per attempt)",
			before, modify, modelCtx)
	}
}

func TestPreemptEventFields(t *testing.T) {
	for _, tc := range []struct {
		name        string
		partial     string
		wantEmitted bool
	}{
		{name: "after a delta", partial: "half", wantEmitted: true},
		{name: "before any delta", wantEmitted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := newModelGate()
			defer gate.release()
			partial := tc.partial
			m := &preemptModel{
				gates: []*modelGate{gate},
				partial: func(call int) string {
					if call == 1 {
						return partial
					}
					return ""
				},
				answer: func(call int) (*llm.Response, error) {
					if call == 1 {
						return mock.Text("first"), nil
					}
					return mock.Text("done"), nil
				},
			}
			a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
			if err != nil {
				t.Fatal(err)
			}
			run := a.Stream(context.Background(), "start")
			done := driveAsync(run)
			<-gate.started
			if _, err := run.SteerWith(core.UserText("refresh"), agent.SteerOptions{Preempt: true, Reason: "refresh"}); err != nil {
				t.Fatalf("SteerWith(preempt) = %v", err)
			}
			res := <-done
			if res.err != nil {
				t.Fatalf("run error: %v", res.err)
			}
			evs := preemptedEvents(res.events)
			if len(evs) != 1 {
				t.Fatalf("ModelPreempted events = %d, want 1", len(evs))
			}
			ev := evs[0]
			if ev.Emitted != tc.wantEmitted {
				t.Fatalf("ModelPreempted.Emitted = %v, want %v", ev.Emitted, tc.wantEmitted)
			}
			if ev.Step != 0 || ev.Count != 1 || ev.Reason != "refresh" {
				t.Fatalf("ModelPreempted = %+v, want step 0, count 1, reason refresh", ev)
			}
			if got := deltaCount(res.events); (got > 0) != tc.wantEmitted {
				t.Fatalf("MessageDelta count = %d, emitted = %v", got, tc.wantEmitted)
			}
			if got := messageDoneCount(res.events); got != 1 {
				t.Fatalf("MessageDone events = %d, want only the retried attempt's", got)
			}
		})
	}
}

func TestPreemptSuccessWins(t *testing.T) {
	gate := newModelGate()
	m := &preemptModel{
		gates:        []*modelGate{gate},
		ignoreCancel: true,
		answer: func(call int) (*llm.Response, error) {
			if call == 1 {
				return mock.Text("first"), nil
			}
			return mock.Text("second"), nil
		},
	}
	a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	gate.release()
	res := <-done

	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	if got := res.res.Message.Text(); got != "second" {
		t.Fatalf("result = %q, want the next turn's answer", got)
	}
	if len(preemptedEvents(res.events)) != 0 {
		t.Fatal("a completed attempt must not be reported as preempted")
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2 (no in-step retry)", len(reqs))
	}
	if hasText(reqs[0].Messages, "stop") {
		t.Fatal("steering message reached the call that was already finishing")
	}
	if !hasText(reqs[1].Messages, "stop") {
		t.Fatal("steering message missing from the next turn")
	}
	if !hasAssistantText(reqs[1].Messages, "first") {
		t.Fatal("the completed attempt's answer was discarded")
	}
	out := ticket.Outcome()
	if !out.Delivered || out.Step != 1 {
		t.Fatalf("ticket = %+v, want delivered at step 1", out)
	}
	if steps := turnStartSteps(res.events); !slices.Equal(steps, []int{0, 1}) {
		t.Fatalf("TurnStarted steps = %v, want [0 1]", steps)
	}
}

func TestPreemptCancelledByRunWins(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	cancelSeen := make(chan struct{})
	proceed := make(chan struct{})
	m := &preemptModel{
		gates:       []*modelGate{gate},
		afterCancel: func(int) { close(cancelSeen); <-proceed },
		answer:      func(int) (*llm.Response, error) { return mock.Text("unused"), nil },
	}
	a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	<-cancelSeen
	run.Cancel()
	close(proceed)

	res := <-done
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", res.err)
	}
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 (run cancellation wins over the retry)", got)
	}
	if len(preemptedEvents(res.events)) != 0 {
		t.Fatal("a canceled run must not report a preemption retry")
	}
	if ticket.State() != core.InputStateRejected {
		t.Fatalf("ticket state = %q, want rejected", ticket.State())
	}
	if ticket.Outcome().Err == nil {
		t.Fatal("rejected ticket has no reason")
	}
}

func TestPreemptOnlyBeforeModelPhase(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	m := &preemptModel{
		gates: []*modelGate{gate},
		answer: func(call int) (*llm.Response, error) {
			switch call {
			case 1:
				return mock.Text("first"), nil
			case 2:
				return mock.Text("retry"), nil
			default:
				return mock.Text("next"), nil
			}
		},
	}
	a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	later, err := run.SteerWith(core.UserText("later-tools"), agent.SteerOptions{Phase: core.InputAfterTools})
	if err != nil {
		t.Fatalf("SteerWith(after_tools) = %v", err)
	}
	now, err := run.SteerWith(core.UserText("now"), agent.SteerOptions{Phase: core.InputBeforeModel, Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	next, err := run.SteerWith(core.UserText("next-turn"), agent.SteerOptions{Phase: core.InputNextTurn})
	if err != nil {
		t.Fatalf("SteerWith(next_turn) = %v", err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}

	reqs := m.requests()
	if len(reqs) != 3 {
		t.Fatalf("model calls = %d, want 3", len(reqs))
	}
	if !hasText(reqs[1].Messages, "now") {
		t.Fatal("before_model input missing from the retry")
	}
	for _, text := range []string{"later-tools", "next-turn"} {
		if hasText(reqs[1].Messages, text) {
			t.Fatalf("%q reached the retry, want it kept for a later delivery point", text)
		}
		if !hasText(reqs[2].Messages, text) {
			t.Fatalf("%q never reached a later delivery point", text)
		}
	}
	evs := preemptedEvents(res.events)
	if len(evs) != 1 || evs[0].Count != 1 {
		t.Fatalf("ModelPreempted = %+v, want one event carrying one message", evs)
	}
	if out := now.Outcome(); !out.Delivered || out.Step != 0 {
		t.Fatalf("preempt ticket = %+v, want delivered at step 0", out)
	}
	for name, tk := range map[string]*agent.SteerTicket{"after_tools": later, "next_turn": next} {
		if out := tk.Outcome(); !out.Delivered || out.Step != 1 {
			t.Fatalf("%s ticket = %+v, want delivered at step 1", name, out)
		}
	}
}

func TestPreemptDisabledReturnsError(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	m := &preemptModel{
		gates:  []*modelGate{gate},
		answer: func(int) (*llm.Response, error) { return mock.Text("done"), nil },
	}
	a, err := agent.New(agent.WithModel(m))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if !errors.Is(err, agent.ErrPreemptionDisabled) {
		t.Fatalf("SteerWith(preempt) = %v, want ErrPreemptionDisabled", err)
	}
	if ticket != nil {
		t.Fatal("a refused preemption returned a ticket")
	}
	gate.release()
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1", got)
	}
	if got := countRequestsWith(m.requests(), "stop"); got != 0 {
		t.Fatalf("refused message reached %d requests, want 0", got)
	}
}

func TestPreemptWrongPhaseReturnsError(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	m := &preemptModel{
		gates:  []*modelGate{gate},
		answer: func(int) (*llm.Response, error) { return mock.Text("done"), nil },
	}
	a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("later"), agent.SteerOptions{
		Phase:   core.InputNextTurn,
		Preempt: true,
	})
	if !errors.Is(err, agent.ErrPreemptionPhase) {
		t.Fatalf("SteerWith(next_turn, preempt) = %v, want ErrPreemptionPhase", err)
	}
	if ticket != nil {
		t.Fatal("a refused preemption returned a ticket")
	}
	gate.release()
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	if got := countRequestsWith(m.requests(), "later"); got != 0 {
		t.Fatalf("refused message reached %d requests, want 0", got)
	}
}

func TestPreemptWhileNoActiveCallQueues(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	m := &scriptModel{pick: func(call int, _ *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return mock.CallTool("c1", "block", `{}`), nil
		}
		return mock.Text("done"), nil
	}}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithTools(steeringBlockingTool("block", started, release)),
		agent.WithInputPreemption(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt during tools) = %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	reqs := m.requests()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2", len(reqs))
	}
	if hasText(reqs[0].Messages, "stop") {
		t.Fatal("steering message reached the call already in flight")
	}
	if !hasText(reqs[1].Messages, "stop") {
		t.Fatal("steering message missing from the next step")
	}
	if out := ticket.Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("ticket = %+v, want delivered at step 1", out)
	}
	if len(preemptedEvents(res.events)) != 0 {
		t.Fatal("no model call was active, so nothing should be preempted")
	}
}

func TestPreemptTwiceInOneStep(t *testing.T) {
	first := newModelGate()
	retry := newModelGate()
	defer first.release()
	defer retry.release()
	m := &preemptModel{
		gates: []*modelGate{first, retry},
		answer: func(call int) (*llm.Response, error) {
			switch call {
			case 1:
				return mock.Text("first"), nil
			case 2:
				return mock.Text("retry"), nil
			default:
				return mock.Text("next"), nil
			}
		},
	}
	a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-first.started
	now, err := run.SteerWith(core.UserText("now"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("first SteerWith(preempt) = %v", err)
	}
	<-retry.started
	later, err := run.SteerWith(core.UserText("later"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("second SteerWith(preempt) = %v", err)
	}
	retry.release()
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}

	evs := preemptedEvents(res.events)
	if len(evs) != 1 || evs[0].Count != 1 {
		t.Fatalf("ModelPreempted = %+v, want exactly one event carrying one message", evs)
	}
	reqs := m.requests()
	if len(reqs) != 3 {
		t.Fatalf("model calls = %d, want 3", len(reqs))
	}
	if !hasText(reqs[1].Messages, "now") {
		t.Fatal("first message missing from the retry")
	}
	if hasText(reqs[1].Messages, "later") {
		t.Fatal("second preempt request entered the retry instead of waiting")
	}
	if !hasText(reqs[2].Messages, "later") {
		t.Fatal("second message never reached a later delivery point")
	}
	if out := now.Outcome(); !out.Delivered || out.Step != 0 {
		t.Fatalf("first ticket = %+v, want delivered at step 0", out)
	}
	if out := later.Outcome(); !out.Delivered || out.Step != 1 {
		t.Fatalf("second ticket = %+v, want delivered at step 1", out)
	}
}

func TestStreamModelWithoutFinal(t *testing.T) {
	t.Run("no preemption", func(t *testing.T) {
		partial := mock.NewStream("partial-only", func(*llm.Request) []*llm.Response {
			return []*llm.Response{mock.Partial("half")}
		})
		a, err := agent.New(agent.WithModel(partial))
		if err != nil {
			t.Fatal(err)
		}
		_, runErr := a.Run(context.Background(), "start")
		if !errors.Is(runErr, agent.ErrModelNoFinal) {
			t.Fatalf("run error = %v, want ErrModelNoFinal", runErr)
		}
	})

	t.Run("preempted", func(t *testing.T) {
		gate := newModelGate()
		defer gate.release()
		m := &preemptModel{
			gates:   []*modelGate{gate},
			partial: func(call int) string { return "half" },
			answer: func(call int) (*llm.Response, error) {
				if call == 1 {
					return mock.Text("first"), nil
				}
				return mock.Text("done"), nil
			},
		}
		a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(context.Background(), "start")
		done := driveAsync(run)
		<-gate.started
		if _, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true}); err != nil {
			t.Fatalf("SteerWith(preempt) = %v", err)
		}
		res := <-done
		if res.err != nil {
			t.Fatalf("run error: %v", res.err)
		}
		if got := res.res.Message.Text(); got != "done" {
			t.Fatalf("result = %q, want done", got)
		}
	})
}

// errorCounter counts OnError passes so a test can prove cancellation is not
// reported as a provider failure.
type errorCounter struct {
	agent.BaseMiddleware
	mu sync.Mutex
	n  int
}

func (c *errorCounter) OnError(*agent.LoopContext, error) (core.Directive, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return core.Directive{}, nil
}

func (c *errorCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestPreemptDoesNotReportOnError(t *testing.T) {
	t.Run("preempted attempt", func(t *testing.T) {
		gate := newModelGate()
		defer gate.release()
		m := &preemptModel{
			gates:         []*modelGate{gate},
			errorOnCancel: true,
			answer: func(call int) (*llm.Response, error) {
				if call == 1 {
					return mock.Text("first"), nil
				}
				return mock.Text("done"), nil
			},
		}
		counter := &errorCounter{}
		a, err := agent.New(
			agent.WithModel(m),
			agent.WithInputPreemption(true),
			agent.WithMiddleware(counter),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(context.Background(), "start")
		done := driveAsync(run)
		<-gate.started
		if _, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true}); err != nil {
			t.Fatalf("SteerWith(preempt) = %v", err)
		}
		res := <-done
		if res.err != nil {
			t.Fatalf("run error: %v", res.err)
		}
		if got := counter.count(); got != 0 {
			t.Fatalf("OnError calls = %d, want 0 for a preempted attempt", got)
		}
	})

	t.Run("run cancelled", func(t *testing.T) {
		gate := newModelGate()
		defer gate.release()
		m := &preemptModel{
			gates:         []*modelGate{gate},
			errorOnCancel: true,
			answer:        func(int) (*llm.Response, error) { return mock.Text("unused"), nil },
		}
		counter := &errorCounter{}
		a, err := agent.New(agent.WithModel(m), agent.WithMiddleware(counter))
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(context.Background(), "start")
		done := driveAsync(run)
		<-gate.started
		run.Cancel()
		res := <-done
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("run error = %v, want context.Canceled", res.err)
		}
		if got := counter.count(); got != 0 {
			t.Fatalf("OnError calls = %d, want 0 for a canceled run", got)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		m := &scriptModel{pick: func(int, *llm.Request) (*llm.Response, error) {
			return nil, errProvider
		}}
		counter := &errorCounter{}
		a, err := agent.New(agent.WithModel(m), agent.WithMiddleware(counter))
		if err != nil {
			t.Fatal(err)
		}
		_, runErr := a.Run(context.Background(), "start")
		if !errors.Is(runErr, errProvider) {
			t.Fatalf("run error = %v, want the provider error", runErr)
		}
		if got := counter.count(); got != 1 {
			t.Fatalf("OnError calls = %d, want 1 for a real provider error", got)
		}
	})
}

// preemptCommitStore records every commit's ReleaseClaim flag and can fail one
// attempt by number.
type preemptCommitStore struct {
	checkpoint.Durable
	mu       sync.Mutex
	releases []bool
	attempts int
	failAt   int
	err      error
}

func (s *preemptCommitStore) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	s.mu.Lock()
	s.attempts++
	n := s.attempts
	s.releases = append(s.releases, req.ReleaseClaim)
	s.mu.Unlock()
	if n == s.failAt {
		return checkpoint.CommitReceipt{}, s.err
	}
	return s.Durable.Commit(ctx, req)
}

func (s *preemptCommitStore) releaseSequence() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.releases...)
}

func (s *preemptCommitStore) commitAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func TestPreemptCheckpointFailureFailsRun(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	commitErr := errors.New("commit: disk full")
	store := &preemptCommitStore{Durable: checkpoint.NewMemory(), failAt: 1, err: commitErr}
	m := &preemptModel{
		gates: []*modelGate{gate},
		answer: func(call int) (*llm.Response, error) {
			if call == 1 {
				return mock.Text("first"), nil
			}
			return mock.Text("second"), nil
		},
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithInputPreemption(true),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	res := <-done
	if !errors.Is(res.err, commitErr) {
		t.Fatalf("run error = %v, want the failed intermediate commit", res.err)
	}
	if got := m.calls(); got != 1 {
		t.Fatalf("model calls = %d, want 1 (no retry after a failed checkpoint)", got)
	}
	if ticket.State() != core.InputStateRejected {
		t.Fatalf("ticket state = %q, want rejected", ticket.State())
	}
	if !errors.Is(ticket.Outcome().Err, agent.ErrRunClosed) {
		t.Fatalf("ticket reason = %v, want ErrRunClosed", ticket.Outcome().Err)
	}
	if got := store.commitAttempts(); got < 2 {
		t.Fatalf("commit attempts = %d, want the failed intermediate commit plus the failure checkpoint", got)
	}
	releases := store.releaseSequence()
	if len(releases) < 2 || releases[0] || !releases[1] {
		t.Fatalf("release sequence = %v, want the intermediate commit to keep the claim and the failure commit to release it", releases)
	}
}

func TestPreemptAndOverflowRetryCountedSeparately(t *testing.T) {
	gate := newModelGate()
	defer gate.release()
	m := &preemptModel{
		gates: []*modelGate{gate},
		answer: func(call int) (*llm.Response, error) {
			switch call {
			case 1:
				return mock.Text("first"), nil
			case 2:
				return nil, overflowError(100, 300)
			default:
				return mock.Text("done"), nil
			}
		},
	}
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithInputPreemption(true),
		agent.WithMiddleware(dropOldest{n: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "start")
	done := driveAsync(run)
	<-gate.started
	ticket, err := run.SteerWith(core.UserText("stop"), agent.SteerOptions{Preempt: true})
	if err != nil {
		t.Fatalf("SteerWith(preempt) = %v", err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("run error: %v", res.err)
	}
	if got := res.res.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	if got := m.calls(); got != 3 {
		t.Fatalf("model calls = %d, want preempt retry + overflow retry + final", got)
	}
	if evs := preemptedEvents(res.events); len(evs) != 1 {
		t.Fatalf("ModelPreempted events = %d, want 1", len(evs))
	}
	if evs := retriedEvents(res.events); len(evs) != 1 {
		t.Fatalf("ContextOverflowRetried events = %d, want 1", len(evs))
	}
	reqs := m.requests()
	if !hasText(reqs[1].Messages, "stop") {
		t.Fatal("preempt message missing from the first retry")
	}
	if !hasText(reqs[2].Messages, "stop") {
		t.Fatal("preempt message lost by overflow recovery")
	}
	if hasText(reqs[2].Messages, "start") {
		t.Fatal("overflow recovery did not shorten the history")
	}
	if out := ticket.Outcome(); !out.Delivered || out.Step != 0 {
		t.Fatalf("ticket = %+v, want delivered at step 0", out)
	}
	preIdx := eventIndexes(res.events, func(e core.Event) bool { _, ok := e.(core.ModelPreempted); return ok })
	overflowIdx := eventIndexes(res.events, func(e core.Event) bool { _, ok := e.(core.ContextOverflowRetried); return ok })
	if preIdx[0] >= overflowIdx[0] {
		t.Fatalf("ModelPreempted at %d must precede ContextOverflowRetried at %d", preIdx[0], overflowIdx[0])
	}
}

// settleRaceModel answers immediately or finishes while a preemption request is
// racing it, depending on scheduling.
type settleRaceModel struct {
	mu    sync.Mutex
	reqs  []*llm.Request
	calls int
	gate  <-chan struct{}
}

func (m *settleRaceModel) Name() string { return "settle-race" }

func (m *settleRaceModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		m.calls++
		call := m.calls
		m.mu.Unlock()
		if call == 1 {
			<-m.gate
			yield(mock.Text("first"), nil)
			return
		}
		yield(mock.Text("retry"), nil)
	}
}

func (m *settleRaceModel) requests() []*llm.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*llm.Request(nil), m.reqs...)
}

func TestPreemptSettleRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("iteration-%d", i), func(t *testing.T) {
			gate := make(chan struct{})
			m := &settleRaceModel{gate: gate}
			a, err := agent.New(agent.WithModel(m), agent.WithInputPreemption(true))
			if err != nil {
				t.Fatal(err)
			}
			run := a.Stream(context.Background(), "start")
			done := driveAsync(run)
			close(gate)
			ticket, steerErr := run.SteerWith(core.UserText("msg"), agent.SteerOptions{Preempt: true})
			res := <-done
			if res.err != nil {
				t.Fatalf("run error: %v", res.err)
			}
			reqs := m.requests()
			contains := countRequestsWith(reqs, "msg")
			if len(preemptedEvents(res.events)) > 1 {
				t.Fatalf("ModelPreempted events = %d, want at most 1", len(preemptedEvents(res.events)))
			}
			switch {
			case steerErr != nil:
				if !errors.Is(steerErr, agent.ErrRunClosed) {
					t.Fatalf("SteerWith = %v, want ErrRunClosed", steerErr)
				}
				if contains != 0 || len(reqs) != 1 || res.res.Message.Text() != "first" {
					t.Fatalf("closed run consumed the message: requests=%d contains=%d result=%q",
						len(reqs), contains, res.res.Message.Text())
				}
			case ticket.State() == core.InputStateDelivered:
				if contains != 1 {
					t.Fatalf("delivered message reached %d requests, want 1", contains)
				}
				switch {
				case len(reqs) == 1 && res.res.Message.Text() == "first":
					// The request raced ahead of the steer: the message entered
					// the first attempt's PrepareTurn instead of preempting it.
				case len(reqs) == 2 && res.res.Message.Text() == "retry":
					// The message arrived after the first call parked and the
					// step retried with it.
				default:
					t.Fatalf("delivered message: requests=%d result=%q", len(reqs), res.res.Message.Text())
				}
			case ticket.State() == core.InputStateRejected:
				if contains != 0 || len(reqs) != 1 || res.res.Message.Text() != "first" {
					t.Fatalf("rejected message was consumed: requests=%d contains=%d result=%q",
						len(reqs), contains, res.res.Message.Text())
				}
			default:
				t.Fatalf("ticket stayed %q after the run settled", ticket.State())
			}
			if evs := preemptedEvents(res.events); len(evs) == 1 {
				if len(reqs) != 2 || !hasText(reqs[1].Messages, "msg") {
					t.Fatal("a preemption retry did not carry the message")
				}
			}
		})
	}
}
