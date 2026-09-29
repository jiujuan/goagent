package agent_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// gateOnce interrupts every tool call, so a run always pauses before running
// one. It also rewrites whatever result a tool returns and records the calls
// AfterTool saw, which is what makes the resume path observable: an approved
// call that skipped the loop's tool machinery would leave after empty and the
// history un-rewritten.
type gateOnce struct {
	agent.BaseMiddleware

	mu    sync.Mutex
	after []string
	steps []int
}

func (*gateOnce) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func (m *gateOnce) AfterTool(lc *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	m.mu.Lock()
	m.after = append(m.after, tr.CallID)
	m.steps = append(m.steps, lc.Step)
	m.mu.Unlock()
	if len(tr.Content) > 0 {
		if t, ok := tr.Content[0].(core.Text); ok {
			tr.Content = []core.Part{core.Text{Text: "rewritten:" + t.Text}}
		}
	}
	return core.Directive{}, nil
}

func (m *gateOnce) seen() ([]string, []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.after...), append([]int(nil), m.steps...)
}

// pauseOn gates and returns a run paused at its first tool batch, plus the
// pending calls it left behind.
func pauseOn(t *testing.T, a *agent.Agent, ctx context.Context, input string) (*agent.Run, []core.ApprovalRequest) {
	t.Helper()
	var pending []core.ApprovalRequest
	run := a.Stream(ctx, input, agent.OnThread("t1"))
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
		}
	}
	if len(pending) == 0 {
		t.Fatal("expected the run to pause for approval")
	}
	return run, pending
}

// TestResumeApprovedCallGoesThroughAfterTool pins the fix: a tool approved on
// resume must run through the same machinery as a tool executed inside a step.
func TestResumeApprovedCallGoesThroughAfterTool(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	mw := &gateOnce{}

	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		return "boom", nil
	})
	var modelCalls int
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		modelCalls++
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "danger", "{}")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(mw),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run, pending := pauseOn(t, a, ctx, "go")
	if got, _ := mw.seen(); len(got) != 0 {
		t.Fatal("AfterTool ran before any approval")
	}
	if modelCalls != 1 {
		t.Fatalf("model called %d times before pause", modelCalls)
	}

	var started, done []string
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for ev, err := range cont.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolStarted:
			started = append(started, e.Call.ID)
		case core.ToolDone:
			done = append(done, e.Result.CallID)
		}
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// The approved call is observed like any other call.
	if len(started) != 1 || started[0] != "c1" || len(done) != 1 || done[0] != "c1" {
		t.Fatalf("resume did not publish the tool pair: started=%v done=%v", started, done)
	}
	// And it went through the hook: exactly one AfterTool, for the paused step.
	after, steps := mw.seen()
	if len(after) != 1 || after[0] != "c1" {
		t.Fatalf("AfterTool did not run for the approved call: %v", after)
	}
	if len(steps) != 1 || steps[0] != 0 {
		t.Fatalf("AfterTool saw step %v, want the step the run paused at (0)", steps)
	}
	// The rewrite the hook applied is what the model answered from, so the
	// resumed batch really entered the conversation.
	if res.Message.Text() != "saw:rewritten:boom" {
		t.Fatalf("result = %q, want the rewritten tool result to reach the model", res.Message.Text())
	}
}

// TestResumeDeniedCallPublishesWithoutAfterTool: a denied call never reaches the
// handler, so no hook runs for it, but the decision must still be visible on the
// event stream and reported to the model.
func TestResumeDeniedCallPublishesWithoutAfterTool(t *testing.T) {
	ctx := context.Background()
	mw := &gateOnce{}
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		t.Error("denied tool must not run")
		return "", nil
	})
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "danger", "{}")
	})
	a, _ := agent.New(
		agent.WithModel(model), agent.WithTools(danger),
		agent.WithMiddleware(mw), agent.WithCheckpointer(checkpoint.NewMemory()),
	)

	run, pending := pauseOn(t, a, ctx, "go")
	run.Decide(agent.Reject(pending[0].CallID, "too risky"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got core.ToolResult
	var n int
	for ev, err := range cont.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if td, ok := ev.(core.ToolDone); ok {
			got, n = td.Result, n+1
		}
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ToolDone events = %d, want 1", n)
	}
	if !got.IsError || got.Content[0].(core.Text).Text != "rejected: too risky" {
		t.Fatalf("denied result = %+v", got)
	}
	if after, _ := mw.seen(); len(after) != 0 {
		t.Fatalf("AfterTool ran for a call that never executed: %v", after)
	}
	if res.Message.Text() != "saw:rejected: too risky" {
		t.Fatalf("result = %q", res.Message.Text())
	}
}

// stopTool reports a control directive alongside its result, the way a tool
// asking to end the run does.
type stopTool struct{}

func (stopTool) Name() string        { return "stopper" }
func (stopTool) Description() string { return "requests stop" }

func (stopTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (stopTool) Call(*tool.Context, json.RawMessage) (*tool.Result, error) {
	return &tool.Result{
		Content: []core.Part{core.Text{Text: "halting"}},
		Control: &core.Directive{Kind: core.Stop, Reason: "policy"},
	}, nil
}

// TestResumeApprovedCallDirectiveEndsRun: an approved call that asks for Stop
// must end the resumed run with that directive instead of having it dropped on
// the floor, exactly as it would mid-step.
func TestResumeApprovedCallDirectiveEndsRun(t *testing.T) {
	ctx := context.Background()
	mw := &gateOnce{}
	var calls int
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		calls++
		return mock.CallTool("c1", "stopper", `{}`)
	})
	a, _ := agent.New(
		agent.WithModel(model), agent.WithTools(stopTool{}),
		agent.WithMiddleware(mw), agent.WithCheckpointer(checkpoint.NewMemory()),
	)

	run, pending := pauseOn(t, a, ctx, "go")
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// The Stop directive lands instead of being discarded.
	if calls != 1 {
		t.Fatalf("model called %d times: the resumed directive did not end the run", calls)
	}
	tc := res.Message.ToolCalls()
	if len(tc) != 1 || tc[0].Name != "stopper" {
		t.Fatalf("RunDone carried %+v, want the assistant message that issued the call", res.Message)
	}
	if after, _ := mw.seen(); len(after) != 1 {
		t.Fatalf("AfterTool calls = %v, want the approved call observed", after)
	}
}

// TestResumeKeepsOriginalCallOrder: approved and denied calls interleave in the
// batch, but the tool message the model reads must list them in the order the
// model called them.
func TestResumeKeepsOriginalCallOrder(t *testing.T) {
	ctx := context.Background()
	var requests []*llm.Request
	capture := mock.New("m", func(req *llm.Request) *llm.Response {
		requests = append(requests, req)
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)},
				core.ToolCall{ID: "c2", Name: "danger", Args: json.RawMessage(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		return "ran", nil
	})
	mw := &gateOnce{}
	a, _ := agent.New(
		agent.WithModel(capture), agent.WithTools(danger),
		agent.WithMiddleware(mw), agent.WithCheckpointer(checkpoint.NewMemory()),
	)

	run, pending := pauseOn(t, a, ctx, "go")
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want both calls", len(pending))
	}
	// Approve only the second call.
	run.Decide(agent.Allow(pending[1].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}

	if len(requests) != 2 {
		t.Fatalf("model requests = %d, want 2", len(requests))
	}
	last := requests[1].Messages[len(requests[1].Messages)-1]
	if last.Role != core.RoleTool || len(last.Parts) != 2 {
		t.Fatalf("resumed tool message = %+v", last)
	}
	first := last.Parts[0].(core.ToolResult)
	second := last.Parts[1].(core.ToolResult)
	if first.CallID != "c1" || !first.IsError {
		t.Fatalf("c1 should be the undecided rejection, got %+v", first)
	}
	if second.CallID != "c2" || second.IsError || second.Content[0].(core.Text).Text != "rewritten:ran" {
		t.Fatalf("c2 should be the approved call through AfterTool, got %+v", second)
	}
}
