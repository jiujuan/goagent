package runtime_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runtime"
	"github.com/jiujuan/goagent/tool"
)

func weatherSpec() agent.Spec {
	weather := tool.New("get_weather", "weather",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return in.City + ": sunny", nil
		})
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done: " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"Beijing"}`)
	})
	return agent.Spec{Name: "a", Model: model, Tools: []tool.Tool{weather}}
}

func TestRunIterAndWait(t *testing.T) {
	rt := runtime.New(runtime.Config{})
	ag := rt.Compile(weatherSpec())
	run, err := ag.Start(context.Background(), runtime.RunRequest{
		ThreadID: "t1", Message: core.UserText("weather?"),
	})
	if err != nil {
		t.Fatal(err)
	}

	var sawTool, sawDone bool
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch ev.(type) {
		case core.ToolDone:
			sawTool = true
		case core.RunDone:
			sawDone = true
		}
	}
	if !sawTool || !sawDone {
		t.Fatalf("missing events: tool=%v done=%v", sawTool, sawDone)
	}

	res, err := run.Wait()
	if err != nil {
		t.Fatalf("wait err: %v", err)
	}
	if res.Message.Text() != "Done: Beijing: sunny" {
		t.Fatalf("result = %q", res.Message.Text())
	}
}

func TestWaitOnlyDrivesRun(t *testing.T) {
	rt := runtime.New(runtime.Config{})
	ag := rt.Compile(weatherSpec())
	run, _ := ag.Start(context.Background(), runtime.RunRequest{ThreadID: "t1", Message: core.UserText("weather?")})
	res, err := run.Wait() // no Iter consumer
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "Done: Beijing: sunny" {
		t.Fatalf("result = %q", res.Message.Text())
	}
}

func TestResumeContinuesThread(t *testing.T) {
	ctx := context.Background()
	rt := runtime.New(runtime.Config{})
	ag := rt.Compile(weatherSpec())

	run1, _ := ag.Start(ctx, runtime.RunRequest{ThreadID: "t1", Message: core.UserText("weather?")})
	if _, err := run1.Wait(); err != nil {
		t.Fatal(err)
	}
	// Thread now has checkpoints; resume should restore and re-run cleanly.
	run2, err := ag.Resume(ctx, "t1")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := run2.Wait(); err != nil {
		t.Fatalf("resumed run err: %v", err)
	}
}

// gate is a middleware that interrupts before any tool call.
type gate struct{ agent.BaseMiddleware }

func (gate) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func TestHITLInterruptPausesAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	rt := runtime.New(runtime.Config{Checkpointer: store})
	spec := weatherSpec()
	spec.Middleware = []agent.Middleware{gate{}}
	ag := rt.Compile(spec)

	run, _ := ag.Start(ctx, runtime.RunRequest{ThreadID: "t1", Message: core.UserText("weather?")})

	var interrupted bool
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch ev.(type) {
		case core.Interrupted:
			interrupted = true
		case core.ToolDone:
			t.Fatal("tool ran despite interrupt")
		}
	}
	if !interrupted {
		t.Fatal("expected Interrupted event")
	}
	// The pause must have been checkpointed with a Pending snapshot.
	cp, err := store.Latest(ctx, "t1")
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint saved: %v", err)
	}
	if cp.Pending == nil || len(cp.Pending.Pending) != 1 || cp.Pending.Pending[0].Name != "get_weather" {
		t.Fatalf("expected Pending get_weather, got %+v", cp.Pending)
	}
}
