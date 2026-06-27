package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func weatherTool() tool.Tool {
	return tool.New("get_weather", "weather",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return in.City + ": sunny", nil
		})
}

func weatherModel() llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done: " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"Beijing"}`)
	})
}

func TestRunReturnsAnswer(t *testing.T) {
	a, err := agent.New(agent.WithModel(weatherModel()), agent.WithTools(weatherTool()))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := a.Run(context.Background(), "weather?")
	if err != nil {
		t.Fatal(err)
	}
	if answer != "Done: Beijing: sunny" {
		t.Fatalf("answer = %q", answer)
	}
}

func TestNewRequiresModel(t *testing.T) {
	if _, err := agent.New(agent.WithInstruction("x")); err == nil {
		t.Fatal("expected error when WithModel is missing")
	}
}

func TestStreamEvents(t *testing.T) {
	a, _ := agent.New(agent.WithModel(weatherModel()), agent.WithTools(weatherTool()))
	var sawTool, sawDone bool
	for ev, err := range a.Stream(context.Background(), "weather?").Iter() {
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
}

func TestThreadAccumulatesAndResumes(t *testing.T) {
	ctx := context.Background()
	a, _ := agent.New(agent.WithModel(weatherModel()), agent.WithTools(weatherTool()))
	if _, err := a.Run(ctx, "weather?", agent.OnThread("t1")); err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, "t1")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("resumed run err: %v", err)
	}
}

// gate interrupts before any tool call.
type gate struct{ agent.BaseMiddleware }

func (gate) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func TestHITLPauseRejectResume(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	a, _ := agent.New(
		agent.WithModel(weatherModel()),
		agent.WithTools(weatherTool()),
		agent.WithMiddleware(gate{}),
		agent.WithCheckpointer(store),
	)

	run := a.Stream(ctx, "weather?", agent.OnThread("t1"))
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
		t.Fatal("expected Interrupted")
	}

	// The pause was checkpointed with the pending call.
	cp, _ := store.Latest(ctx, "t1")
	if cp == nil || cp.Pending == nil || len(cp.Pending.Pending) != 1 {
		t.Fatalf("expected Pending checkpoint, got %+v", cp)
	}

	// Reject the call; resume. The model receives the rejection and replies.
	run.Decide(agent.Reject("c1", "too risky"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatalf("continued run err: %v", err)
	}
	if res.Message.Text() == "" {
		t.Fatal("expected a reply after resume")
	}
}

func TestHITLApproveResumeRunsTool(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	a, _ := agent.New(
		agent.WithModel(weatherModel()),
		agent.WithTools(weatherTool()),
		agent.WithMiddleware(gate{}),
		agent.WithCheckpointer(store),
	)
	run := a.Stream(ctx, "weather?", agent.OnThread("t1"))
	for range run.Iter() {
	}
	run.Decide(agent.Allow("c1"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	// Approved tool ran, so the model answered from its result.
	if res.Message.Text() != "Done: Beijing: sunny" {
		t.Fatalf("answer = %q", res.Message.Text())
	}
}
