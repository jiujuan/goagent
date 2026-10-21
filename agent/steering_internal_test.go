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

type steeringStaticRunnable struct{ out runOutcome }

func (r steeringStaticRunnable) run(*RunContext) runOutcome { return r.out }

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
