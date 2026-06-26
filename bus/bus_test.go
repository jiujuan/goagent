package bus_test

import (
	"fmt"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/event"
)

func TestFanOutOrdered(t *testing.T) {
	b := bus.New()
	ch1, c1 := b.Subscribe("run", bus.Lossless)
	defer c1()
	ch2, c2 := b.Subscribe("run", bus.Lossless)
	defer c2()

	// Default buffer (64) > 3, so synchronous Publish never blocks.
	for i := 0; i < 3; i++ {
		b.Publish("run", event.TurnStarted{Step: i})
	}
	for i := 0; i < 3; i++ {
		if got := (<-ch1).(event.TurnStarted).Step; got != i {
			t.Fatalf("ch1 out of order: got %d want %d", got, i)
		}
		if got := (<-ch2).(event.TurnStarted).Step; got != i {
			t.Fatalf("ch2 out of order: got %d want %d", got, i)
		}
	}
}

func TestLossyDropsWhenFull(t *testing.T) {
	b := bus.New(bus.WithBufferSize(2))
	ch, cancel := b.Subscribe("run", bus.Lossy)
	defer cancel()

	// Nobody reads; publishing 10 must not block, and at most 2 are buffered.
	for i := 0; i < 10; i++ {
		b.Publish("run", event.TurnStarted{Step: i})
	}
	if n := len(ch); n != 2 {
		t.Fatalf("lossy buffered %d, want 2 (cap)", n)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := bus.New()
	_, cancel := b.Subscribe("run", bus.Lossless)
	if got := b.Subscribers("run"); got != 1 {
		t.Fatalf("Subscribers = %d, want 1", got)
	}
	cancel()
	cancel() // idempotent
	if got := b.Subscribers("run"); got != 0 {
		t.Fatalf("Subscribers after cancel = %d, want 0", got)
	}
}

func TestAdaptEndsOnRunDone(t *testing.T) {
	b := bus.New()
	ch, cancel := b.Subscribe("run", bus.Lossless)
	defer cancel()

	go func() {
		b.Publish("run", event.RunStarted{RunID: "r1"})
		b.Publish("run", event.MessageDone{Message: core.AssistantText("hi")})
		b.Publish("run", event.RunDone{Result: event.Result{Message: core.AssistantText("hi")}})
	}()

	var kinds []string
	for ev, err := range bus.Adapt(ch) {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		kinds = append(kinds, fmt.Sprintf("%T", ev))
	}
	want := []string{"event.RunStarted", "event.MessageDone", "event.RunDone"}
	if len(kinds) != len(want) {
		t.Fatalf("got %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("event %d = %s, want %s", i, kinds[i], want[i])
		}
	}
}

func TestAdaptPropagatesError(t *testing.T) {
	b := bus.New()
	ch, cancel := b.Subscribe("run", bus.Lossless)
	defer cancel()

	go func() {
		b.Publish("run", event.RunStarted{RunID: "r1"})
		b.Publish("run", event.RunFailed{Err: fmt.Errorf("boom")})
	}()

	var sawErr error
	for _, err := range bus.Adapt(ch) {
		if err != nil {
			sawErr = err
		}
	}
	if sawErr == nil || sawErr.Error() != "boom" {
		t.Fatalf("expected boom error, got %v", sawErr)
	}
}
