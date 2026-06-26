package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

func drainTo(t *testing.T, ch <-chan *core.Event, want func(*core.Event) bool) *core.Event {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev := <-ch:
			if want(ev) {
				return ev
			}
		case <-timeout:
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestWorkerRunsJobAndPublishes(t *testing.T) {
	q := NewMemQueue(4)
	bus := NewMemBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewWorker(Config{Consumer: q, Bus: bus}).Run(ctx)

	ch, unsub := bus.Subscribe("k")
	defer unsub()

	err := q.Enqueue(ctx, Job{
		ID:  "job1",
		Key: "k",
		Run: func(_ context.Context, emit func(*core.Event)) (*core.Event, error) {
			emit(&core.Event{Progress: &core.Progress{Status: "running", Percent: 50}})
			return &core.Event{
				Message:  &core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Text{Text: "done"}}},
				Progress: &core.Progress{Status: "succeeded", Percent: 100},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	prog := drainTo(t, ch, func(ev *core.Event) bool { return ev.Partial })
	if prog.Progress.JobID != "job1" {
		t.Errorf("progress JobID not stamped: %q", prog.Progress.JobID)
	}
	if prog.Progress.Percent != 50 {
		t.Errorf("progress percent = %d", prog.Progress.Percent)
	}

	final := drainTo(t, ch, func(ev *core.Event) bool { return !ev.Partial && ev.Message != nil })
	if final.Progress.JobID != "job1" || final.Progress.Status != "succeeded" {
		t.Errorf("final progress wrong: %+v", final.Progress)
	}
}

func TestWorkerPublishesFailure(t *testing.T) {
	q := NewMemQueue(4)
	bus := NewMemBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewWorker(Config{Consumer: q, Bus: bus}).Run(ctx)

	ch, unsub := bus.Subscribe("k")
	defer unsub()

	_ = q.Enqueue(ctx, Job{
		ID:  "job2",
		Key: "k",
		Run: func(_ context.Context, _ func(*core.Event)) (*core.Event, error) {
			return nil, errors.New("boom")
		},
	})

	failed := drainTo(t, ch, func(ev *core.Event) bool {
		return ev.Progress != nil && ev.Progress.Status == "failed"
	})
	if failed.Progress.Err != "boom" || failed.Progress.JobID != "job2" {
		t.Errorf("failure event wrong: %+v", failed.Progress)
	}
}

func TestMemBusUnsubscribe(t *testing.T) {
	bus := NewMemBus()
	ch, unsub := bus.Subscribe("k")
	unsub()
	// Publishing after unsubscribe must not panic; channel is closed/drained.
	bus.Publish("k", &core.Event{})
	if _, ok := <-ch; ok {
		t.Error("expected closed channel after unsubscribe")
	}
}

// TestWorkerResolvesRegistryHandler exercises the cross-process job shape: a Job
// with Type+Payload (no Run) is run via the worker's Registry — the same path
// the Redis backend uses, validated here over an in-process MemQueue.
func TestWorkerResolvesRegistryHandler(t *testing.T) {
	q := NewMemQueue(4)
	bus := NewMemBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := Registry{
		"echo": func(_ context.Context, payload []byte, emit func(*core.Event)) (*core.Event, error) {
			emit(&core.Event{Progress: &core.Progress{Status: "running"}})
			return &core.Event{
				Message:  &core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Text{Text: string(payload)}}},
				Progress: &core.Progress{Status: "succeeded"},
			}, nil
		},
	}
	go NewWorker(Config{Consumer: q, Bus: bus, Registry: reg}).Run(ctx)

	ch, unsub := bus.Subscribe("k")
	defer unsub()

	if err := q.Enqueue(ctx, Job{ID: "j1", Key: "k", Type: "echo", Payload: []byte("hi")}); err != nil {
		t.Fatal(err)
	}

	final := drainTo(t, ch, func(ev *core.Event) bool { return !ev.Partial && ev.Message != nil })
	if got := final.Message.Parts[0].(core.Text).Text; got != "hi" {
		t.Errorf("handler payload not delivered: %q", got)
	}
	if final.Progress.JobID != "j1" {
		t.Errorf("final JobID not stamped: %q", final.Progress.JobID)
	}
}

// TestWorkerUnknownTypeFails verifies a Job.Type with no registered handler
// surfaces a failed event rather than hanging or panicking.
func TestWorkerUnknownTypeFails(t *testing.T) {
	q := NewMemQueue(4)
	bus := NewMemBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewWorker(Config{Consumer: q, Bus: bus}).Run(ctx) // nil Registry

	ch, unsub := bus.Subscribe("k")
	defer unsub()

	_ = q.Enqueue(ctx, Job{ID: "j2", Key: "k", Type: "nope"})

	failed := drainTo(t, ch, func(ev *core.Event) bool {
		return ev.Progress != nil && ev.Progress.Status == "failed"
	})
	if failed.Progress.JobID != "j2" {
		t.Errorf("failed JobID = %q", failed.Progress.JobID)
	}
}
