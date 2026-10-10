package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// phase1FailOnSaveStore fails selected Save attempts while delegating durable
// reads and successful writes to a real store. It lets the tests exercise the
// loop's terminal behavior at each checkpoint boundary.
type phase1FailOnSaveStore struct {
	base checkpoint.Checkpointer

	mu    sync.Mutex
	saves int
	fail  map[int]error
}

func (s *phase1FailOnSaveStore) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	s.mu.Lock()
	s.saves++
	err := s.fail[s.saves]
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.base.Save(ctx, cp)
}

func (s *phase1FailOnSaveStore) Load(ctx context.Context, threadID, checkpointID string) (*checkpoint.Checkpoint, error) {
	return s.base.Load(ctx, threadID, checkpointID)
}

func (s *phase1FailOnSaveStore) Latest(ctx context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	return s.base.Latest(ctx, threadID)
}

func (s *phase1FailOnSaveStore) History(ctx context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	return s.base.History(ctx, threadID)
}

func (s *phase1FailOnSaveStore) SaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func newPhase1FailOnSaveStore(fail map[int]error) *phase1FailOnSaveStore {
	return &phase1FailOnSaveStore{base: checkpoint.NewMemory(), fail: fail}
}

func phase1AssertFailedTerminal(t *testing.T, events []core.Event, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("run error = %v, want %v", err, want)
	}
	if !phase1HasTerminal(events, core.RunFailed{}) {
		t.Fatalf("events = %T, want RunFailed", events)
	}
	if phase1HasTerminal(events, core.RunDone{}) {
		t.Fatalf("events included RunDone after save failure: %T", events)
	}
	if _, paused := phase1Interrupted(events); paused {
		t.Fatalf("events included Interrupted after save failure: %T", events)
	}
}

func TestCheckpointFailureChangesTerminal(t *testing.T) {
	ctx := context.Background()

	t.Run("model completion", func(t *testing.T) {
		saveErr := errors.New("checkpoint unavailable")
		store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
		a, err := agent.New(
			agent.WithModel(mock.New("done", func(*llm.Request) *llm.Response { return mock.Text("done") })),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("save_model")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := store.SaveCount(); got != 1 {
			t.Fatalf("save attempts = %d, want 1", got)
		}
	})

	t.Run("truncated tool call", func(t *testing.T) {
		saveErr := errors.New("checkpoint unavailable")
		store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
		a, err := agent.New(
			agent.WithModel(mock.New("truncated", func(*llm.Request) *llm.Response {
				return &llm.Response{
					Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
						core.ToolCall{ID: "cut", Name: "tool", Args: json.RawMessage(`{}`)},
					}},
					StopReason: llm.StopMaxTokens,
				}
			})),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("save_truncated")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
	})

	t.Run("completed tool batch", func(t *testing.T) {
		saveErr := errors.New("checkpoint unavailable")
		store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "one", Name: "one", Args: json.RawMessage(`{}`)})),
			agent.WithTools(phase1Tool("one", &ran)),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("save_batch")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := ran.Load(); got != 1 {
			t.Fatalf("tool runs = %d, want 1", got)
		}
	})

	t.Run("tool pause", func(t *testing.T) {
		saveErr := errors.New("checkpoint unavailable")
		store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "blocked", Name: "blocked", Args: json.RawMessage(`{}`)})),
			agent.WithTools(phase1Tool("blocked", &ran)),
			agent.WithMiddleware(phase1ApprovalGate{}),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("save_pause")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran after failed pause checkpoint = %d, want 0", got)
		}
	})

	t.Run("resumed tool batch", func(t *testing.T) {
		saveErr := errors.New("checkpoint unavailable")
		store := newPhase1FailOnSaveStore(map[int]error{2: saveErr})
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "resume", Name: "resume", Args: json.RawMessage(`{}`)})),
			agent.WithTools(phase1Tool("resume", &ran)),
			agent.WithMiddleware(phase1ApprovalGate{}),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		run := a.Stream(ctx, "go", agent.OnThread("save_resume"))
		events, runErr := phase1Drain(t, run)
		if runErr != nil {
			t.Fatal(runErr)
		}
		paused, ok := phase1Interrupted(events)
		if !ok || len(paused.Pending) != 1 {
			t.Fatalf("initial events did not pause one call: %#v", events)
		}
		run.Decide(agent.Allow(paused.Pending[0].CallID))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events, runErr = phase1Drain(t, cont)
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := ran.Load(); got != 1 {
			t.Fatalf("resumed tool runs = %d, want 1", got)
		}
	})
}

func TestFailurePreservesBothErrors(t *testing.T) {
	ctx := context.Background()
	modelErr := errors.New("provider unavailable")
	saveErr := errors.New("checkpoint unavailable")
	store := newPhase1FailOnSaveStore(map[int]error{1: saveErr})
	model := &scriptModel{pick: func(int, *llm.Request) (*llm.Response, error) {
		return nil, modelErr
	}}
	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("both_errors")))
	if !errors.Is(runErr, modelErr) || !errors.Is(runErr, saveErr) {
		t.Fatalf("run error = %v, want model and checkpoint errors", runErr)
	}
	if !phase1HasTerminal(events, core.RunFailed{}) || phase1HasTerminal(events, core.RunDone{}) {
		t.Fatalf("terminal events = %#v, want only RunFailed", events)
	}
	if got := store.SaveCount(); got != 1 {
		t.Fatalf("save attempts = %d, want 1 without recursive failure checkpoint", got)
	}
}

type phase1AfterToolFailure struct {
	agent.BaseMiddleware
	err error
}

func (m phase1AfterToolFailure) AfterTool(*agent.LoopContext, *core.ToolResult) (core.Directive, error) {
	return core.Directive{}, m.err
}

func phase1AssertWholeToolBatch(t *testing.T, store checkpoint.Checkpointer, thread string, names ...string) {
	t.Helper()
	cp, err := store.Latest(context.Background(), thread)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || len(cp.State.Messages) == 0 {
		t.Fatalf("failure checkpoint = %#v, want tool results", cp)
	}
	last := cp.State.Messages[len(cp.State.Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != len(names) {
		t.Fatalf("last checkpoint message = %#v, want %d tool results", last, len(names))
	}
	for i, want := range names {
		result, ok := last.Parts[i].(core.ToolResult)
		if !ok || result.Name != want {
			t.Fatalf("tool result[%d] = %#v, want %q", i, last.Parts[i], want)
		}
	}
}

func TestAfterToolErrorSurvivesBatch(t *testing.T) {
	ctx := context.Background()
	afterErr := errors.New("after tool rejected the result")
	calls := []core.ToolCall{
		{ID: "a", Name: "a", Args: json.RawMessage(`{}`)},
		{ID: "b", Name: "b", Args: json.RawMessage(`{}`)},
	}

	for _, mode := range []struct {
		name string
		mode agent.ToolExecMode
	}{
		{name: "sequential", mode: agent.ToolSequential},
		{name: "parallel", mode: agent.ToolParallel},
	} {
		t.Run(mode.name, func(t *testing.T) {
			store := checkpoint.NewMemory()
			var ran atomic.Int32
			a, err := agent.New(
				agent.WithModel(phase1Calls(calls...)),
				agent.WithTools(phase1Tool("a", &ran), phase1Tool("b", &ran)),
				agent.WithToolExecution(mode.mode),
				agent.WithMiddleware(phase1AfterToolFailure{err: afterErr}),
				agent.WithCheckpointer(store),
			)
			if err != nil {
				t.Fatal(err)
			}

			events, runErr := phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("after_"+mode.name)))
			phase1AssertFailedTerminal(t, events, runErr, afterErr)
			if got := ran.Load(); got != 2 {
				t.Fatalf("tool runs = %d, want complete batch of 2", got)
			}
			phase1AssertWholeToolBatch(t, store, "after_"+mode.name, "a", "b")
		})
	}

	t.Run("resumed", func(t *testing.T) {
		store := checkpoint.NewMemory()
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(calls...)),
			agent.WithTools(phase1Tool("a", &ran), phase1Tool("b", &ran)),
			agent.WithMiddleware(phase1ApprovalGate{}, phase1AfterToolFailure{err: afterErr}),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		run := a.Stream(ctx, "go", agent.OnThread("after_resumed"))
		events, runErr := phase1Drain(t, run)
		if runErr != nil {
			t.Fatal(runErr)
		}
		paused, ok := phase1Interrupted(events)
		if !ok || len(paused.Pending) != len(calls) {
			t.Fatalf("initial events did not pause complete batch: %#v", events)
		}
		for _, call := range paused.Pending {
			run.Decide(agent.Allow(call.CallID))
		}
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}

		events, runErr = phase1Drain(t, cont)
		phase1AssertFailedTerminal(t, events, runErr, afterErr)
		if got := ran.Load(); got != 2 {
			t.Fatalf("resumed tool runs = %d, want complete batch of 2", got)
		}
		phase1AssertWholeToolBatch(t, store, "after_resumed", "a", "b")
	})
}
