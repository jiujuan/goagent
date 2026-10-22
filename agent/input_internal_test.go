package agent

import (
	"slices"
	"testing"
	"time"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm/mock"
)

// inputQueueFor builds a RunContext plus queue against a real memory store, so
// the filtering tests read the same durable channel the loop reads.
func inputQueueFor(t *testing.T, threadID string, msgs ...checkpoint.InboxMessage) (*RunContext, inputQueue) {
	t.Helper()
	store := checkpoint.NewMemory()
	for _, msg := range msgs {
		if _, err := store.Deliver(t.Context(), threadID, msg); err != nil {
			t.Fatalf("Deliver %s: %v", msg.ID, err)
		}
	}
	a, err := New(WithModel(mock.New("stub", nil)), WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rc := a.newRunHandle(t.Context(), threadID, &core.State{}).rc
	return rc, rc.inputs()
}

func TestInputQueueTakeVolatileUsesPhase(t *testing.T) {
	rc, q := inputQueueFor(t, "thread-volatile")
	for _, phase := range []core.InputPhase{core.InputNextTurn, core.InputBeforeModel} {
		if _, err := rc.SteerWith(core.UserText(string(phase)), SteerOptions{Phase: phase}); err != nil {
			t.Fatalf("SteerWith %q: %v", phase, err)
		}
	}
	entries := q.takeVolatile(false)
	if len(entries) != 1 || entries[0].phase != core.InputBeforeModel {
		t.Fatalf("takeVolatile(false) = %+v, want only the before_model entry", entries)
	}
	settleDelivered(entries, 0)
	if got := len(rc.steering.takeReady(true)); got != 1 {
		t.Fatalf("next_turn entry count = %d, want it still queued", got)
	}
}

func TestInputQueueTakeVolatilePhase(t *testing.T) {
	rc, q := inputQueueFor(t, "thread-phase")
	for _, phase := range []core.InputPhase{core.InputAfterTools, core.InputBeforeModel} {
		if _, err := rc.SteerWith(core.UserText(string(phase)), SteerOptions{Phase: phase}); err != nil {
			t.Fatalf("SteerWith %q: %v", phase, err)
		}
	}
	entries := q.takeVolatilePhase(core.InputBeforeModel)
	if len(entries) != 1 || entries[0].phase != core.InputBeforeModel {
		t.Fatalf("takeVolatilePhase = %+v, want the before_model entry only", entries)
	}
	if left := rc.steering.drain(); len(left) != 1 || left[0].phase != core.InputAfterTools {
		t.Fatalf("remaining entries = %+v, want the after_tools entry kept", left)
	}
}

func TestInputQueueTakeDurableSkipsAckedIDs(t *testing.T) {
	rc, q := inputQueueFor(t, "thread-durable",
		checkpoint.InboxMessage{ID: "m1", Message: core.UserText("early")},
		checkpoint.InboxMessage{ID: "m2", Message: core.UserText("later")},
	)

	got, err := q.takeDurable(rc, false)
	if err != nil {
		t.Fatalf("takeDurable: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("takeDurable = %+v, want both inbox messages", got)
	}

	// Registering an acknowledgement hides the message from the next read, even
	// before the commit that removes it from the inbox lands.
	rc.addInboxAcks([]string{"m1", "m2"})
	got, err = q.takeDurable(rc, false)
	if err != nil {
		t.Fatalf("takeDurable after ack: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("takeDurable after ack = %+v, want nothing re-delivered", got)
	}
}

func TestInputQueueTakeDurableKeepsInsertionOrder(t *testing.T) {
	_, q := inputQueueFor(t, "thread-order",
		checkpoint.InboxMessage{ID: "a", Message: core.UserText("a")},
		checkpoint.InboxMessage{ID: "b", Message: core.UserText("b")},
		checkpoint.InboxMessage{ID: "c", Message: core.UserText("c")},
	)
	got, err := q.takeDurable(q.rc, true)
	if err != nil {
		t.Fatalf("takeDurable: %v", err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].ID != want {
			t.Fatalf("inbox order %d = %q, want %q", i, got[i].ID, want)
		}
	}
}

func TestInputQueueWithoutDurableStore(t *testing.T) {
	a, err := New(WithModel(mock.New("stub", nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	run := a.newRunHandle(t.Context(), "thread-nostore", &core.State{})
	q := run.rc.inputs()
	got, err := q.takeDurable(run.rc, true)
	if err != nil {
		t.Fatalf("takeDurable without a durable store: %v", err)
	}
	if got != nil {
		t.Fatalf("takeDurable = %+v, want nil", got)
	}
}

// TestInputQueueTakeDurableKeepsNextTurnMidTurn mirrors the volatile queue rule
// on the durable channel: a mid-turn read leaves a next_turn record in the
// inbox, and the same record is taken once the turn restarts.
func TestInputQueueTakeDurableKeepsNextTurnMidTurn(t *testing.T) {
	rc, q := inputQueueFor(t, "thread-next-turn",
		checkpoint.InboxMessage{ID: "later", Message: core.UserText("later"), Phase: core.InputNextTurn},
		checkpoint.InboxMessage{ID: "now", Message: core.UserText("now"), Phase: core.InputBeforeModel},
	)

	got, err := q.takeDurable(rc, false)
	if err != nil {
		t.Fatalf("takeDurable mid-turn: %v", err)
	}
	if len(got) != 1 || got[0].ID != "now" {
		t.Fatalf("mid-turn takeDurable = %+v, want only the before_model record", got)
	}
	// A read only hides a record once the loop has registered its acknowledgement,
	// which is what the delivery point does when it appends the message.
	rc.addInboxAcks([]string{"now"})
	got, err = q.takeDurable(rc, true)
	if err != nil {
		t.Fatalf("takeDurable at a new turn: %v", err)
	}
	if len(got) != 1 || got[0].ID != "later" {
		t.Fatalf("new-turn takeDurable = %+v, want the next_turn record", got)
	}
}

func TestPublishDeliveredDurableGroupsByPhase(t *testing.T) {
	rc, _ := inputQueueFor(t, "thread-group")
	events, cancel := rc.Bus.Subscribe(rc.Topic, bus.Lossy)
	defer cancel()

	rc.publishDeliveredDurable(2, []checkpoint.InboxMessage{
		{ID: "a", Phase: core.InputNextTurn},
		{ID: "b", Phase: core.InputBeforeModel},
		{ID: "c", Phase: core.InputNextTurn},
	}, false)

	var got []core.InputDelivered
	for len(got) < 2 {
		select {
		case ev := <-events:
			if d, ok := ev.(core.InputDelivered); ok {
				got = append(got, d)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("delivery events = %+v, want one per phase", got)
		}
	}
	want := []struct {
		phase core.InputPhase
		ids   []string
	}{
		{core.InputNextTurn, []string{"a", "c"}},
		{core.InputBeforeModel, []string{"b"}},
	}
	for i, w := range want {
		e := got[i]
		if e.Source != core.InputDurable || e.Phase != w.phase || e.Step != 2 || e.Preempted {
			t.Fatalf("delivery event %d = %+v, want durable %s at step 2", i, e, w.phase)
		}
		if e.Count != len(w.ids) || !slices.Equal(e.MessageIDs, w.ids) {
			t.Fatalf("delivery event %d = %+v, want IDs %v", i, e, w.ids)
		}
	}
	if len(events) != 0 {
		t.Fatalf("extra delivery events published: %+v", events)
	}
}
