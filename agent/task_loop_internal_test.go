package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// TestNestedLoopDoesNotConsumeSteering pins the workflow rule: a loop that is
// not a top-level run (Sequential/Loop members share their parent's queue) must
// not settle remaining input after a final answer or close the queue. The message
// stays queued until the owner closes it.
func TestNestedLoopDoesNotConsumeSteering(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(released) }) })
	var startOnce sync.Once
	model := mock.New("nested", func(*llm.Request) *llm.Response {
		startOnce.Do(func() { close(started) })
		<-released
		return mock.Text("done")
	})
	l := newLoop(config{model: model})
	rc := &RunContext{
		Context:  context.Background(),
		RunID:    "run-nested",
		ThreadID: "thread-nested",
		Bus:      bus.New(),
		Topic:    bus.Topic("nested"),
		State:    &core.State{},
	}

	outcomes := make(chan runOutcome, 1)
	go func() { outcomes <- l.run(rc) }()
	<-started
	ticket, err := rc.Steer(core.UserText("held"))
	if err != nil {
		t.Fatalf("Steer while the nested loop runs: %v", err)
	}
	releaseOnce.Do(func() { close(released) })

	out := <-outcomes
	if out.Err != nil {
		t.Fatalf("nested run: %v", out.Err)
	}
	if got := out.Result.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	select {
	case <-ticket.Done():
		t.Fatal("the nested loop settled a message queued after its step began")
	default:
	}
	if _, err := rc.Steer(core.UserText("still open")); err != nil {
		t.Fatalf("the nested loop closed the shared queue: %v", err)
	}

	rc.steering.close(ErrRunClosed)
	if got := ticket.Outcome(); got.Delivered || !errors.Is(got.Err, ErrRunClosed) {
		t.Fatalf("ticket outcome = %+v, want undelivered ErrRunClosed", got)
	}
}
