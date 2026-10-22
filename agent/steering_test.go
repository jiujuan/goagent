package agent_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// steeringBlockingTool signals started and then parks until release closes, so
// a test can inject a message while the loop is inside a step's tool phase.
func steeringBlockingTool(name string, started chan struct{}, release <-chan struct{}) tool.Tool {
	var once sync.Once
	return tool.New(name, "blocks until the test releases it", func(_ *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(started) })
		<-release
		return name + "-done", nil
	})
}

func TestSteerTicketDeliveredAtStep(t *testing.T) {
	ctx := context.Background()
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
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "start")
	tickets := make(chan *agent.SteerTicket, 1)
	steerErrs := make(chan error, 1)
	go func() {
		<-started
		ticket, err := run.Steer(core.UserText("from steering"))
		tickets <- ticket
		steerErrs <- err
		releaseOnce.Do(func() { close(release) })
	}()

	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	ticket := <-tickets
	if err := <-steerErrs; err != nil {
		t.Fatalf("Steer: %v", err)
	}
	if got := res.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	out := ticket.Outcome()
	if !out.Delivered || out.Step != 1 || out.Err != nil {
		t.Fatalf("outcome = %+v, want delivered at step 1", out)
	}
	hist := m.history()
	if len(hist) != 2 {
		t.Fatalf("model calls = %d, want 2", len(hist))
	}
	if got := digest(hist[1]); !strings.Contains(got, "from steering") {
		t.Fatalf("second model request = %q, want the steered message", got)
	}
}

func TestSteerAfterRunClosed(t *testing.T) {
	a, err := agent.New(agent.WithModel(mock.New("final", func(*llm.Request) *llm.Response {
		return mock.Text("done")
	})))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "hi")
	if _, err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	ticket, err := run.Steer(core.UserText("too late"))
	if ticket != nil {
		t.Fatalf("Steer after the run ended returned ticket %+v, want nil", ticket)
	}
	if !errors.Is(err, agent.ErrRunClosed) {
		t.Fatalf("Steer after the run ended = %v, want ErrRunClosed", err)
	}
}

func TestSteerTicketSettledOnRunFailure(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	m := mock.New("loop", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "block", `{}`)
	})
	a, err := agent.New(
		agent.WithModel(m),
		agent.WithTools(steeringBlockingTool("block", started, release)),
		agent.WithMaxTurns(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "start")
	tickets := make(chan *agent.SteerTicket, 1)
	steerErrs := make(chan error, 1)
	go func() {
		<-started
		ticket, err := run.Steer(core.UserText("never delivered"))
		tickets <- ticket
		steerErrs <- err
		releaseOnce.Do(func() { close(release) })
	}()

	_, waitErr := run.Wait()
	if !errors.Is(waitErr, agent.ErrMaxTurnsExceeded) {
		t.Fatalf("Wait error = %v, want ErrMaxTurnsExceeded", waitErr)
	}
	ticket := <-tickets
	if err := <-steerErrs; err != nil {
		t.Fatalf("Steer: %v", err)
	}
	out := ticket.Outcome()
	if out.Delivered || !errors.Is(out.Err, agent.ErrRunClosed) {
		t.Fatalf("outcome = %+v, want undelivered ErrRunClosed", out)
	}
}

func TestCancelSettlesSteeringTicket(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	model := mock.New("blocked", func(*llm.Request) *llm.Response {
		startedOnce.Do(func() { close(started) })
		<-release
		return mock.Text("late answer")
	})
	a, err := agent.New(agent.WithModel(model))
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "hi")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = run.Wait()
	}()
	<-started
	ticket, err := run.Steer(core.UserText("pending"))
	if err != nil {
		t.Fatalf("Steer: %v", err)
	}

	run.Cancel()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := ticket.Wait(waitCtx)
	if err != nil {
		t.Fatalf("ticket did not settle after Cancel: %v", err)
	}
	if out.Delivered || !errors.Is(out.Err, agent.ErrRunStopped) {
		t.Fatalf("outcome = %+v, want undelivered ErrRunStopped", out)
	}

	releaseOnce.Do(func() { close(release) })
	<-done
}

// TestSteerKeepsLegacyDeliveryTiming guards the existing Steer contract while
// delivery phases exist: a plain Steer is still delivered at the next
// PrepareTurn, and an empty SteerOptions behaves the same way.
func TestSteerKeepsLegacyDeliveryTiming(t *testing.T) {
	ctx := context.Background()
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
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "start")
	type settled struct {
		name   string
		ticket *agent.SteerTicket
		err    error
	}
	steered := make(chan settled, 2)
	go func() {
		// Injecting while the batch is parked means step 0's PrepareTurn has
		// already drained, so the delivery step is fixed at 1.
		<-started
		plain, err := run.Steer(core.UserText("plain steer"))
		steered <- settled{name: "Steer", ticket: plain, err: err}
		withOpts, err := run.SteerWith(core.UserText("with options"), agent.SteerOptions{})
		steered <- settled{name: "SteerWith", ticket: withOpts, err: err}
		releaseOnce.Do(func() { close(release) })
	}()

	res, err := run.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := res.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}

	for i := 0; i < 2; i++ {
		s := <-steered
		if s.err != nil {
			t.Fatalf("%s: %v", s.name, s.err)
		}
		out := s.ticket.Outcome()
		if !out.Delivered || out.Step != 1 || out.Err != nil {
			t.Fatalf("%s outcome = %+v, want delivered at step 1", s.name, out)
		}
		if out.State != core.InputDelivered {
			t.Fatalf("%s outcome state = %q, want %q", s.name, out.State, core.InputDelivered)
		}
		if got := s.ticket.State(); got != core.InputDelivered {
			t.Fatalf("%s ticket state = %q, want %q", s.name, got, core.InputDelivered)
		}
	}

	history := m.history()
	if len(history) != 2 {
		t.Fatalf("model called %d times, want 2", len(history))
	}
	for _, want := range []string{"plain steer", "with options"} {
		if !slices.ContainsFunc(history[1], func(msg core.Message) bool { return msg.Text() == want }) {
			t.Fatalf("second request %v is missing the steered message %q", history[1], want)
		}
	}
}
