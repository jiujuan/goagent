package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// injector is a middleware that supplies a tool at request-build time. Before
// the run-scoped table existed, the model could be told about a tool the loop
// could not resolve, and the call came back "unknown tool".
type injector struct {
	agent.BaseMiddleware
	make func(lc *agent.LoopContext) tool.Tool
}

func (m *injector) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if m.make == nil {
		return nil
	}
	if t := m.make(lc); t != nil {
		lc.AddTool(t)
	}
	return nil
}

func lookupTool(body string) tool.Tool {
	return tool.New("lookup", "look something up", func(_ *tool.Context, in struct {
		Q string `json:"q" desc:"query"`
	}) (string, error) {
		return body + ":" + in.Q, nil
	})
}

// TestInjectedToolResolvesForTheRun: a middleware-injected tool is both
// advertised and callable, with its arguments validated like any other tool.
func TestInjectedToolResolvesForTheRun(t *testing.T) {
	var requests []*llm.Request
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		requests = append(requests, req)
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "lookup", `{"q":"beijing"}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool {
			return lookupTool("injected")
		}}),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go")
	var done core.ToolResult
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if td, ok := ev.(core.ToolDone); ok {
			done = td.Result
		}
	}
	res, err := run.Wait()
	if err != nil {
		t.Fatal(err)
	}

	if done.IsError {
		t.Fatalf("injected tool failed: %+v", done)
	}
	if res.Message.Text() != "saw:injected:beijing" {
		t.Fatalf("result = %q", res.Message.Text())
	}
	// Advertised exactly once, on the step that injected it.
	if n := countNamed(requests[0].Tools, "lookup"); n != 1 {
		t.Fatalf("advertised %d times in step 0, want 1", n)
	}
}

// TestInjectedToolAdvertisedInLaterSteps: injecting once, on the first step, is
// enough for the whole run. The step that injects is covered by AddTool appending
// to the request it just built; every later step is covered by the loop merging
// the run's table when it rebuilds the advertisement. The tool is therefore called
// on two consecutive steps, so advertisement and resolution must both persist.
func TestInjectedToolAdvertisedInLaterSteps(t *testing.T) {
	var requests []*llm.Request
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		requests = append(requests, req)
		switch len(requests) {
		case 1, 2:
			return mock.CallTool("c"+string(rune('0'+len(requests))), "lookup", `{"q":"x"}`)
		default:
			tr, ok := mock.LastToolResult(req)
			if !ok {
				t.Errorf("no tool result in the third request")
			}
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(&injector{make: func(lc *agent.LoopContext) tool.Tool {
			if lc.Step != 0 {
				return nil // inject once
			}
			return lookupTool("injected")
		}}),
	)
	res, err := a.Stream(context.Background(), "go").Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "saw:injected:x" {
		t.Fatalf("result = %q, want the once-injected tool callable on later steps", res.Message.Text())
	}
	if len(requests) != 3 {
		t.Fatalf("model requests = %d, want 3", len(requests))
	}
	for i, req := range requests {
		if n := countNamed(req.Tools, "lookup"); n != 1 {
			t.Fatalf("step %d advertised lookup %d times, want exactly 1", i, n)
		}
	}
}

// TestInjectionDoesNotMutateTheAgent: a run's injection must not become part of
// the agent, so a later run of the same agent cannot resolve the name.
func TestInjectionDoesNotMutateTheAgent(t *testing.T) {
	var on atomic.Bool
	on.Store(true)
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			if tr.IsError {
				return mock.Text("not advertised any more")
			}
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "lookup", `{"q":"x"}`)
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool {
			if !on.Load() {
				return nil
			}
			return lookupTool("injected")
		}}),
	)
	ctx := context.Background()
	res, err := a.Stream(ctx, "go").Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "saw:injected:x" {
		t.Fatalf("first run = %q", res.Message.Text())
	}

	on.Store(false)
	res, err = a.Stream(ctx, "again").Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "not advertised any more" {
		t.Fatalf("second run = %q, want the injected name to be unresolvable", res.Message.Text())
	}
}

// TestInjectedToolOverridesAgentTool: resolving the run's table first is what
// lets a middleware swap an implementation — a mock, a cache, a policy wrapper —
// without rebuilding the agent.
func TestInjectedToolOverridesAgentTool(t *testing.T) {
	var requests []*llm.Request
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		requests = append(requests, req)
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "lookup", `{"q":"x"}`)
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithTools(lookupTool("static")),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool { return lookupTool("replaced") }}),
	)
	res, err := a.Stream(context.Background(), "go").Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "saw:replaced:x" {
		t.Fatalf("result = %q, want the injected implementation to win", res.Message.Text())
	}
	if n := countNamed(requests[0].Tools, "lookup"); n != 1 {
		t.Fatalf("overridden tool advertised %d times, want 1", n)
	}
}

// TestInjectionDoesNotLeakAcrossRuns: two concurrent runs of one agent each see
// only their own injected tool, and the agent's static table is left alone.
func TestInjectionDoesNotLeakAcrossRuns(t *testing.T) {
	var mu sync.Mutex
	ids := map[string]bool{}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "perrun", `{}`)
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(&injector{make: func(lc *agent.LoopContext) tool.Tool {
			return tool.New("perrun", "reports its own run", func(_ *tool.Context, _ struct{}) (string, error) {
				mu.Lock()
				ids[lc.RunID] = true
				mu.Unlock()
				return "run:" + lc.RunID, nil
			})
		}}),
	)

	ctx := context.Background()
	const n = 4
	var wg sync.WaitGroup
	saw := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := a.Stream(ctx, "go").Wait()
			if err != nil {
				t.Errorf("run %d: %v", i, err)
				return
			}
			saw[i] = res.Message.Text()
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != n {
		t.Fatalf("%d distinct runs injected, want %d", len(ids), n)
	}
	for i, got := range saw {
		want := "saw:run:"
		if !strings.HasPrefix(got, want) || strings.TrimPrefix(got, want) == "" {
			t.Fatalf("run %d answered %q, want its own run id", i, got)
		}
		// Each run resolved the name against the tool it injected, not a peer's.
		if !ids[strings.TrimPrefix(got, want)] {
			t.Fatalf("run %d used a tool injected by another run: %q", i, got)
		}
	}
}

// TestInjectedSequentialToolDowngradesBatch: a tool injected for a run carries the
// same execution contract as a static one — declaring the SequentialTool
// capability serializes the whole batch it appears in, even when the agent was
// built with the default parallel mode.
func TestInjectedSequentialToolDowngradesBatch(t *testing.T) {
	var mu sync.Mutex
	active, peak := 0, 0
	track := func() {
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		mu.Unlock()
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
	}
	greedy := tool.New("greedy", "parallel-safe", func(_ *tool.Context, _ struct{}) (string, error) {
		track()
		return "g", nil
	})
	serial := tool.AsSequential(tool.New("serial", "must not overlap", func(_ *tool.Context, _ struct{}) (string, error) {
		track()
		return "s", nil
	}))

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "greedy", Args: json.RawMessage(`{}`)},
				core.ToolCall{ID: "c2", Name: "serial", Args: json.RawMessage(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithTools(greedy),
		agent.WithMiddleware(&injector{make: func(lc *agent.LoopContext) tool.Tool {
			if lc.Step != 0 {
				return nil
			}
			return serial
		}}),
	)
	if _, err := a.Stream(context.Background(), "go").Wait(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak != 1 {
		t.Fatalf("peak concurrent tools = %d, want 1: the injected tool's capability was not honored", peak)
	}
}

func countNamed(list []llm.ToolSchema, name string) int {
	var n int
	for _, s := range list {
		if s.Name == name {
			n++
		}
	}
	return n
}
