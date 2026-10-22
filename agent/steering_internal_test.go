package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestSteeringQueuePushDrainSettlesTickets(t *testing.T) {
	q := &steeringQueue{}
	first, err := q.push(core.UserText("one"))
	if err != nil {
		t.Fatalf("push first: %v", err)
	}
	second, err := q.push(core.UserText("two"))
	if err != nil {
		t.Fatalf("push second: %v", err)
	}

	entries := q.drain()
	if len(entries) != 2 {
		t.Fatalf("drained %d entries, want 2", len(entries))
	}
	if got := entries[0].msg.Text(); got != "one" {
		t.Fatalf("first message = %q, want one", got)
	}
	if got := entries[1].msg.Text(); got != "two" {
		t.Fatalf("second message = %q, want two", got)
	}
	if entries[0].ticket != first || entries[1].ticket != second {
		t.Fatal("drain returned entries out of ticket order")
	}
	select {
	case <-first.Done():
		t.Fatal("ticket settled before the loop delivered it")
	default:
	}
	if got := first.Outcome(); got.Delivered || got.Step != 0 || got.Err != nil {
		t.Fatalf("unsettled outcome = %+v, want zero value", got)
	}
	if again := q.drain(); again != nil {
		t.Fatalf("second drain = %v, want nil", again)
	}

	settleDelivered(entries, 3)
	if got := first.Outcome(); !got.Delivered || got.Step != 3 || got.Err != nil {
		t.Fatalf("first outcome = %+v, want delivered at step 3", got)
	}
	if got := second.Outcome(); !got.Delivered || got.Step != 3 || got.Err != nil {
		t.Fatalf("second outcome = %+v, want delivered at step 3", got)
	}
}

func TestSteeringQueueCloseSettlesRemainingOnce(t *testing.T) {
	q := &steeringQueue{}
	first, err := q.push(core.UserText("one"))
	if err != nil {
		t.Fatalf("push first: %v", err)
	}
	second, err := q.push(core.UserText("two"))
	if err != nil {
		t.Fatalf("push second: %v", err)
	}

	closeErr := errors.New("closed for good")
	q.close(closeErr)
	for i, ticket := range []*SteerTicket{first, second} {
		out := ticket.Outcome()
		if out.Delivered || !errors.Is(out.Err, closeErr) {
			t.Fatalf("ticket %d outcome = %+v, want undelivered with the close reason", i, out)
		}
		select {
		case <-ticket.Done():
		default:
			t.Fatalf("ticket %d was not settled by close", i)
		}
	}
	if entries := q.drain(); entries != nil {
		t.Fatalf("closed queue still holds %d entries", len(entries))
	}
	if _, err := q.push(core.UserText("late")); !errors.Is(err, closeErr) {
		t.Fatalf("push after close = %v, want the close reason", err)
	}

	q.close(errors.New("second reason"))
	if _, err := q.push(core.UserText("later")); !errors.Is(err, closeErr) {
		t.Fatalf("push after repeated close = %v, want the first close reason", err)
	}
}

func TestSteeringQueueDrainOrCloseIsAtomic(t *testing.T) {
	q := &steeringQueue{}
	if entries, ok := q.drainOrClose(ErrRunClosed); ok || entries != nil {
		t.Fatalf("drainOrClose on empty queue = (%v, %v), want closed with no entries", entries, ok)
	}
	if _, err := q.push(core.UserText("late")); !errors.Is(err, ErrRunClosed) {
		t.Fatalf("push after drainOrClose closed the queue = %v, want ErrRunClosed", err)
	}

	open := &steeringQueue{}
	ticket, err := open.push(core.UserText("pending"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	entries, ok := open.drainOrClose(ErrRunClosed)
	if !ok || len(entries) != 1 {
		t.Fatalf("drainOrClose on non-empty queue = (%d entries, %v), want one entry and true", len(entries), ok)
	}
	if entries[0].ticket != ticket {
		t.Fatal("drainOrClose returned the wrong ticket")
	}
	if _, err := open.push(core.UserText("later")); err != nil {
		t.Fatalf("drainOrClose closed a non-empty queue: %v", err)
	}
}

func TestSteerTicketWaitHonorsContext(t *testing.T) {
	q := &steeringQueue{}
	ticket, err := q.push(core.UserText("pending"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ticket.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait with a canceled context = %v, want context.Canceled", err)
	}

	waited := make(chan SteerOutcome, 1)
	go func() {
		out, err := ticket.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait after settlement: %v", err)
		}
		waited <- out
	}()
	q.close(ErrRunClosed)
	out := <-waited
	if out.Delivered || !errors.Is(out.Err, ErrRunClosed) {
		t.Fatalf("settled outcome = %+v, want undelivered ErrRunClosed", out)
	}
}

func TestDriveStartErrClosesSteeringQueue(t *testing.T) {
	a, err := New(WithModel(mock.New("stub", nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	run := a.newRunHandle(context.Background(), "thread", &core.State{})
	run.startErr = errors.New("invalid run input")
	ticket, err := run.Steer(core.UserText("late"))
	if err != nil {
		t.Fatalf("Steer before start failure = %v, want the ticket", err)
	}
	if _, err := run.Wait(); err == nil {
		t.Fatal("Wait succeeded despite startErr")
	}
	out := ticket.Outcome()
	if out.Delivered || !errors.Is(out.Err, ErrRunClosed) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunClosed", out)
	}
}

// TestRunHandleWiresPreemptionOption covers the plumbing a queue-level test
// cannot see: WithInputPreemption has to reach the queue the run handle owns,
// so a preempt request is refused or accepted according to how the agent was
// built. The handle is not driven, so nothing depends on model timing.
func TestRunHandleWiresPreemptionOption(t *testing.T) {
	ctx := context.Background()

	off, err := New(WithModel(mock.New("stub", nil)))
	if err != nil {
		t.Fatalf("New off: %v", err)
	}
	offRun := off.newRunHandle(ctx, "thread-off", &core.State{})
	ticket, err := offRun.SteerWith(core.UserText("stop"), SteerOptions{Preempt: true})
	if ticket != nil {
		t.Fatal("a disabled agent returned a preempt ticket")
	}
	if !errors.Is(err, ErrPreemptionDisabled) {
		t.Fatalf("disabled preempt error = %v, want ErrPreemptionDisabled", err)
	}
	if got := len(offRun.rc.steering.entries); got != 0 {
		t.Fatalf("disabled queue holds %d entries, want 0", got)
	}
	if _, err := offRun.SteerWith(core.UserText("plain"), SteerOptions{}); err != nil {
		t.Fatalf("disabled agent refused an ordinary message: %v", err)
	}

	on, err := New(WithModel(mock.New("stub", nil)), WithInputPreemption(true))
	if err != nil {
		t.Fatalf("New on: %v", err)
	}
	onRun := on.newRunHandle(ctx, "thread-on", &core.State{})
	if _, err := onRun.SteerWith(core.UserText("late"), SteerOptions{
		Phase: core.InputNextTurn, Preempt: true,
	}); !errors.Is(err, ErrPreemptionPhase) {
		t.Fatalf("wrong-phase preempt error = %v, want ErrPreemptionPhase", err)
	}
	if _, err := onRun.SteerWith(core.UserText("stop"), SteerOptions{Preempt: true, Reason: "user asked"}); err != nil {
		t.Fatalf("enabled agent refused a preempt request: %v", err)
	}

	entries := onRun.rc.steering.takeReady(true)
	if len(entries) != 1 {
		t.Fatalf("enabled queue holds %d entries, want only the accepted preempt request", len(entries))
	}
	if entries[0].phase != core.InputBeforeModel {
		t.Fatalf("accepted preempt entry phase = %q, want before_model", entries[0].phase)
	}
}

type steeringStaticRunnable struct{ out runOutcome }

func (r steeringStaticRunnable) run(*RunContext) runOutcome { return r.out }

func TestSteeringQueuePushWithNormalizesPhase(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   core.InputPhase
		want core.InputPhase
	}{
		{name: "empty", in: "", want: core.InputBeforeModel},
		{name: "before model", in: core.InputBeforeModel, want: core.InputBeforeModel},
		{name: "after model", in: core.InputAfterModel, want: core.InputAfterModel},
		{name: "after tools", in: core.InputAfterTools, want: core.InputAfterTools},
		{name: "next turn", in: core.InputNextTurn, want: core.InputNextTurn},
	} {
		q := &steeringQueue{}
		if _, err := q.pushWith(core.UserText(tc.name), SteerOptions{Phase: tc.in}); err != nil {
			t.Fatalf("%s: pushWith: %v", tc.name, err)
		}
		entries := q.drain()
		if len(entries) != 1 {
			t.Fatalf("%s: drained %d entries, want 1", tc.name, len(entries))
		}
		if entries[0].phase != tc.want {
			t.Fatalf("%s: entry phase = %q, want %q", tc.name, entries[0].phase, tc.want)
		}
	}

	q := &steeringQueue{}
	ticket, err := q.pushWith(core.UserText("bad"), SteerOptions{Phase: "later"})
	if ticket != nil {
		t.Fatal("pushWith returned a ticket for an unknown phase")
	}
	if !errors.Is(err, core.ErrUnknownInputPhase) {
		t.Fatalf("pushWith unknown phase error = %v, want ErrUnknownInputPhase", err)
	}
	if len(q.entries) != 0 {
		t.Fatalf("queue holds %d entries after a refused push, want 0", len(q.entries))
	}
}

func TestSteeringQueueTakeReadyKeepsNextTurn(t *testing.T) {
	q := &steeringQueue{}
	for _, phase := range []core.InputPhase{
		core.InputNextTurn, core.InputBeforeModel, core.InputNextTurn, core.InputAfterTools,
	} {
		if _, err := q.pushWith(core.UserText(string(phase)), SteerOptions{Phase: phase}); err != nil {
			t.Fatalf("pushWith %q: %v", phase, err)
		}
	}

	taken := q.takeReady(false)
	if len(taken) != 2 {
		t.Fatalf("takeReady(false) took %d entries, want 2", len(taken))
	}
	if taken[0].phase != core.InputBeforeModel || taken[1].phase != core.InputAfterTools {
		t.Fatalf("takeReady(false) took %q then %q, want queue order", taken[0].phase, taken[1].phase)
	}
	kept := q.takeReady(false)
	if len(kept) != 0 {
		t.Fatalf("takeReady(false) took %d entries on the second pass, want 0", len(kept))
	}
	rest := q.takeReady(true)
	if len(rest) != 2 {
		t.Fatalf("takeReady(true) took %d entries, want the two kept next_turn messages", len(rest))
	}
	for i, e := range rest {
		if e.phase != core.InputNextTurn {
			t.Fatalf("kept entry %d phase = %q, want next_turn", i, e.phase)
		}
	}
	if e := rest[0].msg.Text(); e != string(core.InputNextTurn) {
		t.Fatalf("first kept entry = %q, want the first queued next_turn message", e)
	}
}

func TestSteeringQueueTakeReadyNewTurnTakesAll(t *testing.T) {
	q := &steeringQueue{}
	order := []core.InputPhase{
		core.InputNextTurn, core.InputAfterModel, core.InputBeforeModel, core.InputAfterTools,
	}
	for _, phase := range order {
		if _, err := q.pushWith(core.UserText(string(phase)), SteerOptions{Phase: phase}); err != nil {
			t.Fatalf("pushWith %q: %v", phase, err)
		}
	}
	entries := q.takeReady(true)
	if len(entries) != len(order) {
		t.Fatalf("takeReady(true) took %d entries, want %d", len(entries), len(order))
	}
	for i, e := range entries {
		if e.phase != order[i] {
			t.Fatalf("entry %d phase = %q, want %q", i, e.phase, order[i])
		}
	}
	if left := q.drain(); left != nil {
		t.Fatalf("queue still holds %d entries, want empty", len(left))
	}
}

func TestSteeringQueueTakePhase(t *testing.T) {
	q := &steeringQueue{}
	for _, phase := range []core.InputPhase{
		core.InputAfterTools, core.InputBeforeModel, core.InputNextTurn, core.InputBeforeModel,
	} {
		if _, err := q.pushWith(core.UserText(string(phase)), SteerOptions{Phase: phase}); err != nil {
			t.Fatalf("pushWith %q: %v", phase, err)
		}
	}

	taken := q.takePhase(core.InputBeforeModel)
	if len(taken) != 2 {
		t.Fatalf("takePhase took %d entries, want the two before_model messages", len(taken))
	}
	for _, e := range taken {
		if e.phase != core.InputBeforeModel {
			t.Fatalf("taken entry phase = %q, want before_model", e.phase)
		}
	}
	if again := q.takePhase(core.InputBeforeModel); again != nil {
		t.Fatalf("second takePhase took %d entries, want none", len(again))
	}

	rest := q.drain()
	if len(rest) != 2 {
		t.Fatalf("queue holds %d entries after takePhase, want the two untouched messages", len(rest))
	}
	if rest[0].phase != core.InputAfterTools || rest[1].phase != core.InputNextTurn {
		t.Fatalf("remaining order is %q then %q, want the original order", rest[0].phase, rest[1].phase)
	}
}

func TestSteerTicketStateLifecycle(t *testing.T) {
	q := &steeringQueue{}
	delivered, err := q.push(core.UserText("one"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	refused, err := q.push(core.UserText("two"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	if got := delivered.State(); got != core.InputStateQueued {
		t.Fatalf("unsettled ticket state = %q, want %q", got, core.InputStateQueued)
	}
	if got := delivered.Outcome(); got.Delivered || got.Step != 0 || got.Err != nil || got.State != "" {
		t.Fatalf("unsettled outcome = %+v, want the zero value", got)
	}

	settleDelivered([]steerEntry{{ticket: delivered}}, 4)
	if got := delivered.State(); got != core.InputStateDelivered {
		t.Fatalf("delivered ticket state = %q, want %q", got, core.InputStateDelivered)
	}
	out := delivered.Outcome()
	if !out.Delivered || out.Step != 4 || out.Err != nil || out.State != core.InputStateDelivered {
		t.Fatalf("delivered outcome = %+v, want delivered at step 4", out)
	}

	settleUndelivered([]steerEntry{{ticket: refused}}, ErrRunClosed)
	if got := refused.State(); got != core.InputStateRejected {
		t.Fatalf("refused ticket state = %q, want %q", got, core.InputStateRejected)
	}
	if out := refused.Outcome(); out.Delivered || !errors.Is(out.Err, ErrRunClosed) || out.State != core.InputStateRejected {
		t.Fatalf("refused outcome = %+v, want rejected with ErrRunClosed", out)
	}

	settleDelivered([]steerEntry{{ticket: refused}}, 9)
	if out := refused.Outcome(); out.Delivered || out.Step != 0 || !errors.Is(out.Err, ErrRunClosed) {
		t.Fatalf("re-settled outcome = %+v, want the first settlement kept", out)
	}
}

func TestSteeringQueuePreemptValidation(t *testing.T) {
	disabled := &steeringQueue{}
	ticket, err := disabled.pushWith(core.UserText("stop"), SteerOptions{Preempt: true})
	if ticket != nil {
		t.Fatal("a disabled queue returned a ticket for a preempt request")
	}
	if !errors.Is(err, ErrPreemptionDisabled) {
		t.Fatalf("disabled preempt error = %v, want ErrPreemptionDisabled", err)
	}
	if len(disabled.entries) != 0 {
		t.Fatalf("disabled queue holds %d entries, want 0", len(disabled.entries))
	}

	enabled := &steeringQueue{preemptionEnabled: true}
	ticket, err = enabled.pushWith(core.UserText("late"), SteerOptions{Phase: core.InputNextTurn, Preempt: true})
	if ticket != nil {
		t.Fatal("a wrong-phase preempt request returned a ticket")
	}
	if !errors.Is(err, ErrPreemptionPhase) {
		t.Fatalf("wrong-phase preempt error = %v, want ErrPreemptionPhase", err)
	}
	if len(enabled.entries) != 0 {
		t.Fatalf("wrong-phase queue holds %d entries, want 0", len(enabled.entries))
	}

	if _, err := enabled.pushWith(core.UserText("stop"), SteerOptions{Preempt: true}); err != nil {
		t.Fatalf("enabled preempt push: %v", err)
	}
	entries := enabled.takeReady(true)
	if len(entries) != 1 || entries[0].phase != core.InputBeforeModel {
		t.Fatalf("enabled preempt stored %+v, want one before_model entry", entries)
	}
}

func TestSteeringQueueClosedRejectsPushWith(t *testing.T) {
	q := &steeringQueue{}
	q.close(ErrRunClosed)
	// The close reason wins over argument validation: a caller of a closed run
	// learns the run is over, not that the phase spelled wrong.
	ticket, err := q.pushWith(core.UserText("late"), SteerOptions{Phase: "later"})
	if ticket != nil {
		t.Fatal("a closed queue returned a ticket")
	}
	if !errors.Is(err, ErrRunClosed) {
		t.Fatalf("closed queue pushWith error = %v, want ErrRunClosed", err)
	}
}

func TestRunChildClosesChildQueue(t *testing.T) {
	child := &RunContext{Context: context.Background()}
	ticket, err := child.Steer(core.UserText("pending"))
	if err != nil {
		t.Fatalf("Steer: %v", err)
	}
	out := runChild(child, steeringStaticRunnable{out: runOutcome{Result: core.Result{Message: core.AssistantText("ok")}}})
	if got := out.Result.Message.Text(); got != "ok" {
		t.Fatalf("runChild result = %q, want ok", got)
	}
	settled := ticket.Outcome()
	if settled.Delivered || !errors.Is(settled.Err, ErrRunClosed) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunClosed", settled)
	}
	if _, err := child.Steer(core.UserText("after")); !errors.Is(err, ErrRunClosed) {
		t.Fatalf("Steer after runChild = %v, want ErrRunClosed", err)
	}
}
