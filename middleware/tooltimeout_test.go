package middleware_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// resultText renders a tool result the way the model receives it.
func resultText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

// callOnceModel scripts the smallest useful loop: call the named tool, then
// repeat back whatever it answered.
func callOnceModel(toolName string) *mock.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + resultText(tr))
		}
		return mock.CallTool("c1", toolName, `{}`)
	})
}

// sleepingTool never looks at its context, which is the case the loop's abandoned
// wait exists for.
func sleepingTool(name string, d time.Duration) tool.Tool {
	return tool.New(name, "sleeps without watching its context",
		func(_ *tool.Context, _ struct{}) (string, error) {
			time.Sleep(d)
			return "late:" + name, nil
		})
}

// deadlineProbe reports whether the context of its own call carried a deadline.
func deadlineProbe(name string, bounded *atomic.Bool) tool.Tool {
	return tool.New(name, "reports whether its context is bounded",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			_, ok := tctx.Deadline()
			bounded.Store(ok)
			return "ok", nil
		})
}

// TestToolTimeoutTightensAgentDefault: with the agent granting 5s to every call,
// the middleware can still hold one tool to far less — the earlier deadline wins.
func TestToolTimeoutTightensAgentDefault(t *testing.T) {
	a, err := agent.New(
		agent.WithModel(callOnceModel("stuck")),
		agent.WithTools(sleepingTool("stuck", 20*time.Second)),
		agent.WithToolTimeout(5*time.Second),
		agent.WithMiddleware(middleware.ToolTimeout(middleware.ToolTimeoutOptions{
			PerTool: map[string]time.Duration{"stuck": 40 * time.Millisecond},
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	answer, err := a.Run(ctx, "go")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("run took %s; the per-tool bound did not beat the 5s agent default", elapsed)
	}
	if !strings.Contains(answer, "timed out") {
		t.Fatalf("answer = %q, want the timeout wording", answer)
	}
}

// TestToolTimeoutBoundsWithoutAgentDefault: the middleware alone is enough — an
// agent that never configured WithToolTimeout still gets bounded calls.
func TestToolTimeoutBoundsWithoutAgentDefault(t *testing.T) {
	a, err := agent.New(
		agent.WithModel(callOnceModel("stuck")),
		agent.WithTools(sleepingTool("stuck", 20*time.Second)),
		agent.WithMiddleware(middleware.ToolTimeout(middleware.ToolTimeoutOptions{
			Default: 40 * time.Millisecond,
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	answer, err := a.Run(ctx, "go")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(answer, `tool "stuck" timed out after`) {
		t.Fatalf("answer = %q, want the timeout wording", answer)
	}
}

// TestToolTimeoutUnboundedCallKeepsContext: a tool the middleware does not bound
// must see the run context exactly as it would without this middleware — no
// deadline, and the loop still waits for it as long as it takes.
func TestToolTimeoutUnboundedCallKeepsContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    middleware.ToolTimeoutOptions
		tool    string
		bounded bool
	}{
		{
			name:    "exempt tool",
			opts:    middleware.ToolTimeoutOptions{Default: 5 * time.Second, Exempt: []string{"poll"}},
			tool:    "poll",
			bounded: false,
		},
		{
			name: "per tool zero lifts the default",
			opts: middleware.ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"poll": 0}},
			tool: "poll",
		},
		{
			name:    "an unlisted tool keeps the default",
			opts:    middleware.ToolTimeoutOptions{Default: 5 * time.Second},
			tool:    "other",
			bounded: true,
		},
		{
			name: "a listed tool gets its own bound",
			opts: middleware.ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"poll": time.Minute}},
			tool: "poll",

			bounded: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bounded atomic.Bool
			a, err := agent.New(
				agent.WithModel(callOnceModel(tc.tool)),
				agent.WithTools(deadlineProbe(tc.tool, &bounded)),
				agent.WithMiddleware(middleware.ToolTimeout(tc.opts)),
			)
			if err != nil {
				t.Fatal(err)
			}
			answer, err := a.Run(context.Background(), "go")
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if answer != "saw:ok" {
				t.Fatalf("answer = %q, want the tool's own answer", answer)
			}
			if bounded.Load() != tc.bounded {
				t.Fatalf("call context carried a deadline = %v, want %v", bounded.Load(), tc.bounded)
			}
		})
	}
}
