package agent_test

import (
	"context"
	"errors"
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

// ctxKey is the value key the ToolContexter test middleware stamps contexts with.
type ctxKey string

// tagger implements agent.ToolContexter: it stamps a value onto the call context
// and records what it was handed, which is how the fold order is observed.
type tagger struct {
	agent.BaseMiddleware

	mark       string
	sawParent  string
	sawTool    string
	deadlineAt time.Time

	mu sync.Mutex
}

// cancelTag records the order in which its cancel runs.
type cancelTag struct {
	agent.BaseMiddleware

	name string
	mu   *sync.Mutex
	log  *[]string
}

func (c cancelTag) ToolContext(_ *agent.LoopContext, ctx context.Context, _ *core.ToolCall) (context.Context, context.CancelFunc) {
	return ctx, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		*c.log = append(*c.log, c.name)
	}
}

func (t *tagger) ToolContext(lc *agent.LoopContext, ctx context.Context, call *core.ToolCall) (context.Context, context.CancelFunc) {
	t.mu.Lock()
	if v, ok := ctx.Value(ctxKey("tag")).(string); ok {
		t.sawParent = v
	}
	t.sawTool = call.Name
	t.mu.Unlock()
	if !t.deadlineAt.IsZero() {
		dctx, cancel := context.WithDeadline(ctx, t.deadlineAt)
		return dctx, cancel
	}
	// Nothing to release on this path, which also exercises a nil cancel.
	return context.WithValue(ctx, ctxKey("tag"), t.mark), nil
}

func (t *tagger) seen() (string, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sawParent, t.sawTool
}

// plain is a middleware that does not implement ToolContexter, used to prove the
// fold skips it.
type plain struct{ agent.BaseMiddleware }

// TestStackToolContextFoldsInOrder: Stack.ToolContext threads each implementer's
// result to the next in registration order and skips middleware that does not
// implement the capability. With no implementer the input context comes back.
func TestStackToolContextFoldsInOrder(t *testing.T) {
	lc := &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background()}}
	call := &core.ToolCall{Name: "lookup"}

	first := &tagger{mark: "first"}
	skip := &plain{}
	second := &tagger{mark: "second"}
	stack := agent.NewStack(first, skip, second)

	base := context.WithValue(context.Background(), ctxKey("tag"), "root")
	got, cancel := stack.ToolContext(lc, base, call)

	if v := got.Value(ctxKey("tag")); v != "second" {
		t.Fatalf("fold left tag = %v, want the last implementer's value", v)
	}
	if parent, toolName := second.seen(); parent != "first" || toolName != "lookup" {
		t.Fatalf("second saw parent=%q tool=%q, want first/lookup", parent, toolName)
	}
	if parent, _ := first.seen(); parent != "root" {
		t.Fatalf("first saw parent=%q, want root", parent)
	}

	// Nothing was attached that needs releasing on the value path, so releasing
	// the fold is a no-op rather than a nil-call panic.
	cancel()

	none := agent.NewStack(&plain{})
	gotNone, noneCancel := none.ToolContext(lc, base, call)
	if gotNone.Value(ctxKey("tag")) != "root" {
		t.Fatal("fold with no implementer changed the context")
	}
	noneCancel()

	// Releases run in reverse registration order, so a middleware whose derived
	// context wraps another's is untangled before the inner one. The skip in the
	// middle contributes nothing.
	var order []string
	mu := &sync.Mutex{}
	_, release := agent.NewStack(
		cancelTag{name: "outer", mu: mu, log: &order},
		&plain{},
		cancelTag{name: "inner", mu: mu, log: &order},
	).ToolContext(lc, base, call)
	release()
	if len(order) != 2 || order[0] != "inner" || order[1] != "outer" {
		t.Fatalf("release order = %v, want inner then outer", order)
	}
}

// TestToolTimeoutCancelsCooperativeTool: a tool that watches its context is
// released at the bound, and its own error is what reaches the model.
func TestToolTimeoutCancelsCooperativeTool(t *testing.T) {
	var cancelled atomic.Bool

	waiter := tool.New("wait", "blocks until its context ends",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			<-tctx.Done()
			cancelled.Store(true)
			return "", tctx.Err()
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		return mock.CallTool("c1", "wait", "{}")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(waiter),
		agent.WithToolTimeout(30*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	// The outer cap is a tripwire: if the per-call bound ever stops reaching the
	// tool, this run ends on the run-level cancellation instead of hanging the
	// package's whole test binary.
	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	answer, err := a.Run(runCtx, "go")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("run: %v (answer %q)", err, answer)
	}

	if !cancelled.Load() {
		t.Fatal("the tool was never cancelled; WithToolTimeout did not reach it")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("run took %s, want the 30ms bound to release it", elapsed)
	}
	if !strings.Contains(answer, "context deadline exceeded") {
		t.Fatalf("answer = %q, want the tool's own cancellation error", answer)
	}
	// The tool's report beats the loop's: a handler that watched the cancellation
	// knows what it managed to do, and abandonGrace exists to let it say so.
	if strings.Contains(answer, "timed out") {
		t.Fatalf("answer = %q, want the handler's error, not the synthesized timeout", answer)
	}
}

// TestToolTimeoutZeroLeavesRunUntouched: with no bound configured the call
// context carries no deadline at all, so existing tools see exactly what they
// saw before this option existed.
func TestToolTimeoutZeroLeavesRunUntouched(t *testing.T) {
	var hadDeadline atomic.Bool

	probe := tool.New("probe", "reports whether its context is bounded",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			_, ok := tctx.Deadline()
			hadDeadline.Store(ok)
			return "polled", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		return mock.CallTool("c1", "probe", "{}")
	})

	a, err := agent.New(agent.WithModel(model), agent.WithTools(probe))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := a.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	if hadDeadline.Load() {
		t.Fatal("a deadline reached the tool although no timeout is configured")
	}
	if answer != "saw:polled" {
		t.Fatalf("answer = %q, want saw:polled", answer)
	}
}

// TestToolCallCtxTakesEarlierDeadline: the agent default and a middleware bound
// coexist by taking the earlier of the two — the seam can only tighten.
func TestToolCallCtxTakesEarlierDeadline(t *testing.T) {
	cases := []struct {
		name     string
		agentSet time.Duration
		mwSet    time.Duration
		wantMax  time.Duration
	}{
		{"middleware tightens the agent default", 5 * time.Second, 200 * time.Millisecond, time.Second},
		{"agent default tightens the middleware", 200 * time.Millisecond, 5 * time.Second, time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var remaining atomic.Value // time.Duration
			probe := tool.New("probe", "reports its own deadline",
				func(tctx *tool.Context, _ struct{}) (string, error) {
					d, ok := tctx.Deadline()
					if !ok {
						return "", errors.New("no deadline on the call context")
					}
					remaining.Store(time.Until(d))
					return "ok", nil
				})
			model := mock.New("m", func(req *llm.Request) *llm.Response {
				if tr, ok := mock.LastToolResult(req); ok {
					return mock.Text(toolResultText(tr))
				}
				return mock.CallTool("c1", "probe", "{}")
			})

			mw := &tagger{deadlineAt: time.Now().Add(tc.mwSet), mark: "bounded"}
			a, err := agent.New(
				agent.WithModel(model),
				agent.WithTools(probe),
				agent.WithToolTimeout(tc.agentSet),
				agent.WithMiddleware(mw),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Run(context.Background(), "go"); err != nil {
				t.Fatal(err)
			}

			got, ok := remaining.Load().(time.Duration)
			if !ok {
				t.Fatal("the tool never reported a deadline")
			}
			if got > tc.wantMax {
				t.Fatalf("effective deadline = %s, want the earlier bound (<=%s)", got, tc.wantMax)
			}
			if _, toolName := mw.seen(); toolName != "probe" {
				t.Fatalf("middleware saw tool %q, want probe", toolName)
			}
		})
	}
}

// TestToolUpdateSurvivesDerivedDeadline: Context.Update resolves the tool.Updater
// capability off the context the tool is handed, so wrapping that context in a
// deadline (or in whatever a middleware returns) must not drop partial results.
func TestToolUpdateSurvivesDerivedDeadline(t *testing.T) {
	reporter := tool.New("render", "long job",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			tctx.Update(core.Text{Text: "30%"})
			return "finished", nil
		})

	// A middleware that replaces the context (without a deadline) exercises the
	// same forwarding as the timeout path.
	shaper := &tagger{mark: "shaped"}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text(toolResultText(tr))
		}
		return mock.CallTool("c1", "render", "{}")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(reporter),
		agent.WithToolTimeout(5*time.Second),
		agent.WithMiddleware(shaper),
	)
	if err != nil {
		t.Fatal(err)
	}

	var updates []string
	run := a.Stream(context.Background(), "go")
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if u, ok := ev.(core.ToolUpdate); ok {
			if u.CallID != "c1" {
				t.Fatalf("ToolUpdate CallID = %q, want c1", u.CallID)
			}
			updates = append(updates, u.Partial.(core.Text).Text)
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0] != "30%" {
		t.Fatalf("ToolUpdate events = %v, want one 30%% partial", updates)
	}
}

// TestToolTimeoutReachesInjectedTool: a tool injected for the run by middleware
// is bounded like any other, and resolves ahead of the agent's own table.
func TestToolTimeoutReachesInjectedTool(t *testing.T) {
	var cancelled atomic.Bool

	waiter := tool.New("injected", "blocks until its context ends",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			<-tctx.Done()
			cancelled.Store(true)
			return "", tctx.Err()
		})

	injector := &inject{t: waiter}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		return mock.CallTool("c1", "injected", "{}")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithToolTimeout(30*time.Millisecond),
		agent.WithMiddleware(injector),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := a.Run(runCtx, "go"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !cancelled.Load() {
		t.Fatal("the injected tool was never cancelled")
	}
}

// inject adds a tool to every run from ModifyRequest, where the loop picks up
// the advertisement for that step and later ones.
type inject struct {
	agent.BaseMiddleware
	t tool.Tool
}

func (i *inject) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	lc.AddTool(i.t)
	return nil
}
