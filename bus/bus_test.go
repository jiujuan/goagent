package bus_test

import (
	"fmt"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

func TestFanOutOrdered(t *testing.T) {
	b := bus.New()
	ch1, c1 := b.Subscribe("run", bus.Lossless)
	defer c1()
	ch2, c2 := b.Subscribe("run", bus.Lossless)
	defer c2()
	for i := 0; i < 3; i++ {
		b.Publish("run", core.TurnStarted{Step: i})
	}
	for i := 0; i < 3; i++ {
		if (<-ch1).(core.TurnStarted).Step != i || (<-ch2).(core.TurnStarted).Step != i {
			t.Fatalf("fan-out out of order at %d", i)
		}
	}
}

func TestLossyDropsWhenFull(t *testing.T) {
	b := bus.New(bus.WithBufferSize(2))
	ch, cancel := b.Subscribe("run", bus.Lossy)
	defer cancel()
	for i := 0; i < 10; i++ {
		b.Publish("run", core.TurnStarted{Step: i})
	}
	if n := len(ch); n != 2 {
		t.Fatalf("lossy buffered %d, want 2", n)
	}
}

func TestAdaptEndsOnTerminal(t *testing.T) {
	b := bus.New()
	ch, cancel := b.Subscribe("run", bus.Lossless)
	defer cancel()
	go func() {
		b.Publish("run", core.RunStarted{RunID: "r"})
		b.Publish("run", core.MessageDone{Message: core.AssistantText("hi")})
		b.Publish("run", core.RunDone{Result: core.Result{Message: core.AssistantText("hi")}})
	}()
	var kinds []string
	for ev, err := range bus.Adapt(ch) {
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, fmt.Sprintf("%T", ev))
	}
	if len(kinds) != 3 || kinds[2] != "core.RunDone" {
		t.Fatalf("got %v", kinds)
	}
}
