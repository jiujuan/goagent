package runtime_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/event"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runtime"
	"github.com/jiujuan/goagent/tool"
)

// driveAndCollect subscribes first (so no events are lost), drives the loop in a
// goroutine, and collects the event stream via the pull adapter until terminal.
func driveAndCollect(t *testing.T, spec runtime.AgentSpec, seed []core.Message) ([]event.Event, error) {
	t.Helper()
	b := bus.New()
	ch, cancel := b.Subscribe("t1", bus.Lossless)
	defer cancel()

	rc := &runtime.RunContext{
		Context:  context.Background(),
		RunID:    "r1",
		ThreadID: "t1",
		Bus:      b,
		Topic:    "t1",
		State:    &core.State{Messages: seed},
	}
	go runtime.Drive(rc, spec)

	var evs []event.Event
	var runErr error
	for ev, err := range bus.Adapt(ch) {
		if err != nil {
			runErr = err
		}
		evs = append(evs, ev)
	}
	return evs, runErr
}

func kinds(evs []event.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = fmt.Sprintf("%T", e)
	}
	return out
}

func TestLoopToolThenReply(t *testing.T) {
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "get_x", "{}")
	})
	getX := tool.New("get_x", "get x", func(_ *tool.Context, _ struct{}) (string, error) {
		return "X", nil
	})
	spec := runtime.AgentSpec{Name: "a", Model: model, Tools: []tool.Tool{getX}}

	evs, err := driveAndCollect(t, spec, []core.Message{core.UserText("hi")})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	want := []string{
		"event.RunStarted",
		"event.TurnStarted", // step 0
		"event.MessageDone", // assistant tool call
		"event.ToolStarted", // get_x
		"event.ToolDone",    // -> X
		"event.TurnDone",    // step 0
		"event.TurnStarted", // step 1
		"event.MessageDone", // "done"
		"event.TurnDone",    // step 1
		"event.RunDone",
	}
	if got := kinds(evs); !equal(got, want) {
		t.Fatalf("event sequence\n got: %v\nwant: %v", got, want)
	}

	last := evs[len(evs)-1].(event.RunDone)
	if last.Result.Message.Text() != "done" {
		t.Fatalf("final result = %q, want %q", last.Result.Message.Text(), "done")
	}
}

func TestLoopBeforeToolInterruptPauses(t *testing.T) {
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "danger", "{}")
	})
	danger := tool.New("danger", "dangerous", func(_ *tool.Context, _ struct{}) (string, error) {
		t.Fatal("tool must not run when BeforeTool returns Interrupt")
		return "", nil
	})
	var log []string
	gate := recorder{name: "gate", log: &log, beforeTool: core.Directive{Kind: core.Interrupt, Reason: "needs approval"}}
	spec := runtime.AgentSpec{Name: "a", Model: model, Tools: []tool.Tool{danger}, Middleware: []runtime.Middleware{gate}}

	evs, err := driveAndCollect(t, spec, []core.Message{core.UserText("delete it")})
	if err != nil {
		t.Fatalf("interrupt should not be an error: %v", err)
	}

	var interrupted *event.Interrupted
	for i := range evs {
		if iv, ok := evs[i].(event.Interrupted); ok {
			interrupted = &iv
		}
		if _, ok := evs[i].(event.ToolDone); ok {
			t.Fatalf("tool ran despite interrupt: %v", kinds(evs))
		}
	}
	if interrupted == nil {
		t.Fatalf("expected Interrupted event, got %v", kinds(evs))
	}
	if len(interrupted.Pending) != 1 || interrupted.Pending[0].Tool != "danger" {
		t.Fatalf("pending = %+v, want one 'danger' call", interrupted.Pending)
	}
}

func TestLoopBeforeModelStopEndsRun(t *testing.T) {
	model := mock.New("m", func(*llm.Request) *llm.Response {
		t.Fatal("model must not be called when BeforeModel returns Stop")
		return nil
	})
	var log []string
	stopper := recorder{name: "s", log: &log, beforeModel: core.Directive{Kind: core.Stop}}
	spec := runtime.AgentSpec{Name: "a", Model: model, Middleware: []runtime.Middleware{stopper}}

	evs, err := driveAndCollect(t, spec, nil)
	if err != nil {
		t.Fatalf("stop should not be an error: %v", err)
	}
	if _, ok := evs[len(evs)-1].(event.RunDone); !ok {
		t.Fatalf("expected RunDone last, got %v", kinds(evs))
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
