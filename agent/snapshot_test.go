package agent_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

// signalAgent emits one committed response and signals only after yield returns.
// Through Runner, that means the event has been committed before waiters resume.
type signalAgent struct {
	name string
	done chan struct{}
	once sync.Once
}

func (a *signalAgent) Name() string             { return a.name }
func (a *signalAgent) Description() string      { return "emits and signals after commit" }
func (a *signalAgent) SubAgents() []agent.Agent { return nil }
func (a *signalAgent) Run(ictx agent.InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		msg := core.AssistantText("first-result")
		yield(&core.Event{
			ID:           core.NewID("evt"),
			InvocationID: ictx.InvocationID,
			Author:       a.name,
			Message:      &msg,
		}, nil)
		a.once.Do(func() { close(a.done) })
	}
}

type waitingAgent struct {
	done  <-chan struct{}
	inner agent.Agent
}

func (a *waitingAgent) Name() string             { return a.inner.Name() }
func (a *waitingAgent) Description() string      { return a.inner.Description() }
func (a *waitingAgent) SubAgents() []agent.Agent { return a.inner.SubAgents() }
func (a *waitingAgent) Run(ictx agent.InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		select {
		case <-a.done:
		case <-ictx.Done():
			yield(nil, ictx.Err())
			return
		}
		core.Pipe(a.inner.Run(ictx), yield)
	}
}

func TestParallelChildrenShareOneBaselineSnapshot(t *testing.T) {
	done := make(chan struct{})
	first := &signalAgent{name: "first", done: done}

	var got []core.Message
	second := agent.New(agent.Config{
		Name:            "second",
		DisableTransfer: true,
		Model: mock.New("capture", func(req *llm.Request) *llm.Response {
			got = core.CloneMessages(req.Messages)
			return mock.Text("second-result")
		}),
	})
	root := agent.Parallel("parallel", first, &waitingAgent{done: done, inner: second})
	driveAgent(t, root)

	if len(got) != 1 || got[0].Role != core.RoleUser || got[0].Text() != "go" {
		t.Fatalf("second branch history = %v, want only the baseline user message", messageTexts(got))
	}
}

func TestSequentialStageRefreshesSnapshot(t *testing.T) {
	done := make(chan struct{})
	first := &signalAgent{name: "first", done: done}

	var got []core.Message
	second := agent.New(agent.Config{
		Name:            "second",
		DisableTransfer: true,
		Model: mock.New("capture", func(req *llm.Request) *llm.Response {
			got = core.CloneMessages(req.Messages)
			return mock.Text("second-result")
		}),
	})
	driveAgent(t, agent.Sequential("sequential", first, second))

	if want := []string{"go", "first-result"}; !sameStrings(messageTexts(got), want) {
		t.Fatalf("second stage history = %v, want %v", messageTexts(got), want)
	}
}

func driveAgent(t *testing.T, root agent.Agent) {
	t.Helper()
	r := runner.New(runner.Config{Root: root})
	for _, err := range r.Run(context.Background(), "user", "session", core.UserText("go")) {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func messageTexts(messages []core.Message) []string {
	out := make([]string, len(messages))
	for i, message := range messages {
		out[i] = message.Text()
	}
	return out
}

func sameStrings(a, b []string) bool {
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

var _ agent.Agent = (*signalAgent)(nil)
var _ agent.Agent = (*waitingAgent)(nil)
