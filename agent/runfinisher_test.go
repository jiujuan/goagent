package agent_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// finisher records the run-end callback and the State it saw.
type finisher struct {
	agent.BaseMiddleware
	name   string
	calls  int
	res    core.Result
	runErr error
	msgs   int
}

func (f *finisher) FinishRun(rc *agent.RunContext, res core.Result, err error) {
	f.calls++
	f.res = res
	f.runErr = err
	if rc.State != nil {
		f.msgs = len(rc.State.Messages)
	}
}

func (f *finisher) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	// Prove the hook is a run-end hook, not a step hook.
	return core.Directive{}, nil
}

func answeringModel(text string) llm.Model {
	return mock.New("m", func(*llm.Request) *llm.Response { return mock.Text(text) })
}

// failingModel yields a model-level error, which surfaces as a failed run.
type failing struct{}

func (failing) Name() string { return "failing" }

func (failing) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, errors.New("provider exploded"))
	}
}

// The hook fires exactly once for a completed run, with the final State visible.
func TestRunFinisherOnCompletion(t *testing.T) {
	f := &finisher{name: "a"}
	a, err := agent.New(agent.WithModel(answeringModel("done")), agent.WithMiddleware(f))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := a.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	if answer != "done" {
		t.Fatalf("answer = %q", answer)
	}
	if f.calls != 1 {
		t.Fatalf("FinishRun calls = %d, want 1", f.calls)
	}
	if f.runErr != nil {
		t.Fatalf("runErr = %v, want nil", f.runErr)
	}
	if f.res.Message.Text() != "done" {
		t.Fatalf("result = %q, want the final answer", f.res.Message.Text())
	}
	// user message + assistant reply
	if f.msgs != 2 {
		t.Fatalf("finisher saw %d messages, want the full transcript", f.msgs)
	}
}

// A failed run still gets its end hook, with the error.
func TestRunFinisherOnFailure(t *testing.T) {
	f := &finisher{}
	a, _ := agent.New(agent.WithModel(failing{}), agent.WithMiddleware(f))
	if _, err := a.Run(context.Background(), "hi"); err == nil {
		t.Fatal("run should have failed")
	}
	if f.calls != 1 {
		t.Fatalf("FinishRun calls = %d, want 1", f.calls)
	}
	if f.runErr == nil {
		t.Fatal("finisher should see the run error")
	}
}

// A run paused for human-in-the-loop is not finished: the hook waits until the
// resumed run settles.
func TestRunFinisherNotOnInterrupt(t *testing.T) {
	f := &finisher{}
	a, err := agent.New(
		agent.WithModel(weatherModel()),
		agent.WithTools(weatherTool()),
		agent.WithMiddleware(gate{}, f),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "weather?")
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if f.calls != 0 {
		t.Fatalf("FinishRun calls = %d, want 0 while the run is only paused", f.calls)
	}
}

// Hooks run in registration order.
func TestRunFinisherOrder(t *testing.T) {
	var log []string
	first := &orderRecorder{log: &log, name: "first"}
	second := &orderRecorder{log: &log, name: "second"}
	a, _ := agent.New(agent.WithModel(answeringModel("ok")), agent.WithMiddleware(first, second))
	if _, err := a.Run(context.Background(), "hi"); err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 || log[0] != "first" || log[1] != "second" {
		t.Fatalf("hook order = %v, want registration order", log)
	}
}

type orderRecorder struct {
	agent.BaseMiddleware
	log  *[]string
	name string
}

func (o *orderRecorder) FinishRun(*agent.RunContext, core.Result, error) {
	*o.log = append(*o.log, o.name)
}
