package queue_test

import (
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/queue"
)

func TestMemBusPubSub(t *testing.T) {
	b := queue.NewMemBus()
	ch, cancel := b.Subscribe("job-1")
	defer cancel()

	b.Publish("job-1", core.RunStarted{RunID: "r1"})
	b.Publish("job-1", core.RunDone{Result: core.Result{Message: core.AssistantText("ok")}})
	b.Publish("other", core.RunStarted{RunID: "nope"}) // different key, not delivered

	if _, ok := (<-ch).(core.RunStarted); !ok {
		t.Fatal("expected RunStarted")
	}
	done, ok := (<-ch).(core.RunDone)
	if !ok || done.Result.Message.Text() != "ok" {
		t.Fatalf("expected RunDone(ok), got %#v", done)
	}
}

func TestBridgeForwardsEvents(t *testing.T) {
	b := queue.NewMemBus()
	out, cancel := b.Subscribe("k")
	defer cancel()

	src := make(chan core.Event, 4)
	src <- core.PlanNodeStarted{NodeID: "n1"}
	src <- core.PlanNodeDone{NodeID: "n1", Status: "done"}
	close(src)

	go queue.Bridge(b, "k", src)

	select {
	case ev := <-out:
		if s, ok := ev.(core.PlanNodeStarted); !ok || s.NodeID != "n1" {
			t.Fatalf("bridged event = %#v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not forward")
	}
}

func TestNewBusInProcess(t *testing.T) {
	b, err := queue.NewBus() // no WithRedis -> MemBus
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := b.(*queue.MemBus); !ok {
		t.Fatalf("NewBus() = %T, want *MemBus", b)
	}
}
