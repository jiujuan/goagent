package runner_test

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

type gatedModel struct {
	calls         atomic.Int32
	firstStarted  chan struct{}
	secondStarted chan struct{}
	releaseFirst  chan struct{}
}

func (m *gatedModel) Name() string { return "gated" }

func (m *gatedModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		call := m.calls.Add(1)
		if call == 1 {
			close(m.firstStarted)
			<-m.releaseFirst
		} else if call == 2 {
			close(m.secondStarted)
		}
		msg := core.AssistantText("reply")
		yield(&llm.Response{Message: msg, StopReason: llm.StopEnd}, nil)
	}
}

func TestRunnerSerializesInvocationsForOneSession(t *testing.T) {
	model := &gatedModel{
		firstStarted:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		releaseFirst:  make(chan struct{}),
	}
	store := session.InMemory()
	r := runner.New(runner.Config{
		Root:  agent.New(agent.Config{Name: "agent", Model: model, DisableTransfer: true}),
		Store: store,
	})

	var wg sync.WaitGroup
	drive := func(text string) {
		defer wg.Done()
		for _, err := range r.Run(context.Background(), "user", "same", core.UserText(text)) {
			if err != nil {
				t.Errorf("run %q: %v", text, err)
			}
		}
	}

	wg.Add(1)
	go drive("first")
	<-model.firstStarted
	wg.Add(1)
	go drive("second")

	select {
	case <-model.secondStarted:
		t.Fatal("second invocation entered the model before the first released the session")
	case <-time.After(50 * time.Millisecond):
	}

	close(model.releaseFirst)
	select {
	case <-model.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second invocation did not start after the first completed")
	}
	wg.Wait()

	sess, err := store.GetOrCreate(context.Background(), "goagent", "user", "same")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]core.Role, 0, 4)
	for _, message := range sess.Messages() {
		got = append(got, message.Role)
	}
	want := []core.Role{core.RoleUser, core.RoleAssistant, core.RoleUser, core.RoleAssistant}
	if len(got) != len(want) {
		t.Fatalf("roles = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("roles = %v, want %v", got, want)
		}
	}
}

func TestRunnerAllowsDifferentSessionsToRunConcurrently(t *testing.T) {
	model := &gatedModel{
		firstStarted:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		releaseFirst:  make(chan struct{}),
	}
	r := runner.New(runner.Config{
		Root: agent.New(agent.Config{Name: "agent", Model: model, DisableTransfer: true}),
	})

	var wg sync.WaitGroup
	drive := func(sessionID string) {
		defer wg.Done()
		for _, err := range r.Run(context.Background(), "user", sessionID, core.UserText("go")) {
			if err != nil {
				t.Errorf("run %q: %v", sessionID, err)
			}
		}
	}

	wg.Add(1)
	go drive("first-session")
	<-model.firstStarted
	wg.Add(1)
	go drive("second-session")

	select {
	case <-model.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("a different session was blocked by the first session's invocation")
	}
	close(model.releaseFirst)
	wg.Wait()
}

var _ llm.Model = (*gatedModel)(nil)
