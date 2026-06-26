package middleware_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

// runAgent drives one agent run to completion and returns the final text reply.
func runAgent(t *testing.T, model llm.Model, tools []tool.Tool, mw middleware.Middleware) string {
	t.Helper()
	ag := agent.New(agent.Config{
		Name:       "a",
		Model:      model,
		Tools:      tools,
		Middleware: []middleware.Middleware{mw},
	})
	r := runner.New(runner.Config{Root: ag})
	var final string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("开始")) {
		if err != nil {
			t.Fatalf("run error: %v", err)
		}
		if ev.IsFinalResponse() {
			final = ev.Message.Text()
		}
	}
	return final
}

// TestHITLApproveExecutes: an approved tool call runs normally and the model
// finishes on its result.
func TestHITLApproveExecutes(t *testing.T) {
	var ran atomic.Bool
	danger := tool.New("danger", "dangerous op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			ran.Store(true)
			return "done", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("已完成")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})

	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Approver: func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
			return middleware.Approve(), nil
		},
	})

	final := runAgent(t, model, []tool.Tool{danger}, mw)
	if !ran.Load() {
		t.Fatal("approved tool did not execute")
	}
	if !strings.Contains(final, "已完成") {
		t.Fatalf("unexpected final reply: %q", final)
	}
}

// TestHITLDenyReroutes: when the only tool call is denied, the tool never runs
// and the model is re-invoked with the denial so it can change course.
func TestHITLDenyReroutes(t *testing.T) {
	var ran atomic.Bool
	danger := tool.New("danger", "dangerous op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			ran.Store(true)
			return "done", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok && tr.IsError {
			// Saw the human denial fed back as a tool error: take another path.
			return mock.Text("改用安全方案")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})

	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Approver: func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
			return middleware.Deny("not allowed"), nil
		},
	})

	final := runAgent(t, model, []tool.Tool{danger}, mw)
	if ran.Load() {
		t.Fatal("denied tool must not execute")
	}
	if !strings.Contains(final, "安全方案") {
		t.Fatalf("model did not reroute after denial: %q", final)
	}
}

// TestHITLEditArgs: the tool executes with the human-corrected arguments.
func TestHITLEditArgs(t *testing.T) {
	var mu sync.Mutex
	var gotPath string
	write := tool.New("write", "write a file",
		func(_ *tool.Context, in struct {
			Path string `json:"path"`
		}) (string, error) {
			mu.Lock()
			gotPath = in.Path
			mu.Unlock()
			return "ok", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("完成")
		}
		return mock.CallTool("c1", "write", `{"path":"prod.db"}`)
	})

	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Approver: func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
			return middleware.ApproveWithArgs([]byte(`{"path":"sandbox.txt"}`)), nil
		},
	})

	runAgent(t, model, []tool.Tool{write}, mw)
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "sandbox.txt" {
		t.Fatalf("tool ran with un-edited args: path=%q", gotPath)
	}
}

// TestHITLMixed: in a turn with two calls, the approved one runs, the denied one
// does not, and the denial is injected before the next model call.
func TestHITLMixed(t *testing.T) {
	var safeRan, dangerRan atomic.Bool
	safe := tool.New("safe", "safe op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			safeRan.Store(true)
			return "ok", nil
		})
	danger := tool.New("danger", "dangerous op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			dangerRan.Store(true)
			return "done", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		for _, msg := range req.Messages {
			if msg.Role == core.RoleUser && strings.Contains(msg.Text(), "已被人工拒绝") {
				return mock.Text("收到拒绝反馈")
			}
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "safe", Args: []byte(`{}`)},
				core.ToolCall{ID: "c2", Name: "danger", Args: []byte(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})

	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Approver: func(_ context.Context, call core.ToolCall) (middleware.Decision, error) {
			if call.Name == "danger" {
				return middleware.Deny("blocked"), nil
			}
			return middleware.Approve(), nil
		},
	})

	final := runAgent(t, model, []tool.Tool{safe, danger}, mw)
	if !safeRan.Load() {
		t.Fatal("approved 'safe' tool did not run")
	}
	if dangerRan.Load() {
		t.Fatal("denied 'danger' tool must not run")
	}
	if !strings.Contains(final, "收到拒绝反馈") {
		t.Fatalf("denial note was not injected before the next call: %q", final)
	}
}

// TestHITLCancel: a cancelled context fails the call before any tool runs.
func TestHITLCancel(t *testing.T) {
	var ran atomic.Bool
	danger := tool.New("danger", "dangerous op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			ran.Store(true)
			return "done", nil
		})

	model := mock.New("m", func(_ *llm.Request) *llm.Response {
		return mock.CallTool("c1", "danger", `{}`)
	})
	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Approver: func(ctx context.Context, _ core.ToolCall) (middleware.Decision, error) {
			<-ctx.Done() // a well-behaved approver respects cancellation
			return middleware.Decision{}, ctx.Err()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	decorated := mw(model)
	req := &llm.Request{Messages: []core.Message{core.UserText("hi")}}
	var gotErr error
	for _, err := range decorated.Generate(ctx, req) {
		if err != nil {
			gotErr = err
		}
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", gotErr)
	}
	if ran.Load() {
		t.Fatal("tool ran despite cancellation")
	}
	_ = danger
}

// TestHITLGateBypass: calls that fail the Gate skip approval entirely and run
// untouched (zero-overhead path; Approver is never consulted).
func TestHITLGateBypass(t *testing.T) {
	var ran atomic.Bool
	danger := tool.New("danger", "dangerous op",
		func(_ *tool.Context, _ struct{}) (string, error) {
			ran.Store(true)
			return "done", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("完成")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})

	mw := middleware.HumanInTheLoop(middleware.HITLOptions{
		Gate: middleware.RequireApprovalFor("other"), // does not match "danger"
		Approver: func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
			t.Fatal("approver must not be called for un-gated calls")
			return middleware.Decision{}, nil
		},
	})

	final := runAgent(t, model, []tool.Tool{danger}, mw)
	if !ran.Load() {
		t.Fatal("un-gated tool should have run")
	}
	if !strings.Contains(final, "完成") {
		t.Fatalf("unexpected final reply: %q", final)
	}
}
