package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// --- the four rejections callOne can make, and the one it must not invent ------

// okTool answers anything and is only ever registered, never called, so an
// unrelated name can be rejected as unknown.
type okTool struct{ name string }

func (o okTool) Name() string        { return o.name }
func (o okTool) Description() string { return "answers at once" }
func (o okTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{},"required":[]}`)
}
func (o okTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	return tool.TextResult("ran:" + o.name), nil
}

// failingPreparer rejects the arguments before the handler, the way a tool that
// cannot repair what it was given does.
type failingPreparer struct{ okTool }

func (f failingPreparer) PrepareArguments(json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("cannot resolve the alias 'query'")
}

// strictTool needs a "q" argument, so an empty one fails schema validation.
type strictTool struct{ okTool }

func (s strictTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)
}

// brokenTool runs and reports its own failure through the result, which is the
// shape that must NOT be counted as a rejection.
type brokenTool struct{ okTool }

func (b brokenTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	return tool.ErrorResult("upstream said no"), nil
}

// goerTool runs and returns a Go error, the fourth rejection.
type goerTool struct{ okTool }

func (g goerTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	return nil, errors.New("handler exploded")
}

// echoScript asks for one tool call, then reports back what the call answered.
func echoScript(name, args string, answer *string) *mock.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			*answer = toolResultText(tr)
			return mock.Text("done")
		}
		return mock.CallTool("c1", name, args)
	})
}

// TestRejectClassesFromCallOne: each of the four ways the loop rejects a call is
// reported with its own class, and the text a guard is given is the text the model
// was given.
func TestRejectClassesFromCallOne(t *testing.T) {
	cases := []struct {
		class agent.RejectClass
		name  string
		tools []tool.Tool
		call  string
		args  string
	}{
		{agent.RejectUnknownTool, "unknown_tool", []tool.Tool{okTool{"keep"}}, "absent", `{}`},
		{agent.RejectPrepareFailed, "prepare_failed", []tool.Tool{failingPreparer{okTool{"p"}}}, "p", `{}`},
		{agent.RejectSchemaInvalid, "schema_invalid", []tool.Tool{strictTool{okTool{"s"}}}, "s", `{}`},
		{agent.RejectHandlerError, "handler_error", []tool.Tool{goerTool{okTool{"g"}}}, "g", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &rejectSpy{}
			var answer string
			a, err := agent.New(
				agent.WithModel(echoScript(tc.call, tc.args, &answer)),
				agent.WithTools(tc.tools...),
				agent.WithMiddleware(spy),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Run(context.Background(), "go"); err != nil {
				t.Fatal(err)
			}

			got := spy.seen()
			if len(got) != 1 {
				t.Fatalf("rejections = %d (%v), want exactly 1", len(got), got)
			}
			r := got[0]
			if r.Class != tc.class {
				t.Fatalf("class = %v, want %v", r.Class, tc.class)
			}
			if r.Call.Name != tc.call || r.Call.ID != "c1" {
				t.Fatalf("call = %+v, want the rejected call c1/%s", r.Call, tc.call)
			}
			if r.Detail != answer {
				t.Fatalf("Detail = %q, want the model's own text %q", r.Detail, answer)
			}
			if r.Class.String() != tc.name {
				t.Fatalf("class name = %q, want %q", r.Class.String(), tc.name)
			}
		})
	}
}

// TestHandlerIsErrorIsNotARejection: a tool that ran and reported failure answered
// the call. Counting that as a rejection would make a remote 500 look like a model
// that cannot use the tool.
func TestHandlerIsErrorIsNotARejection(t *testing.T) {
	spy := &rejectSpy{}
	var answer string
	a, err := agent.New(
		agent.WithModel(echoScript("broken", `{}`, &answer)),
		agent.WithTools(brokenTool{okTool{"broken"}}),
		agent.WithMiddleware(spy),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(answer, "upstream said no") {
		t.Fatalf("answer = %q, want the tool's own report", answer)
	}
	if got := spy.seen(); len(got) != 0 {
		t.Fatalf("rejections = %v, want none for a result the tool reported itself", got)
	}
}

// TestRejectionsDispatchedAfterBatchSerially: a parallel batch that rejects two calls
// delivers both hooks serially on the loop's goroutine, so a guard can write
// State.KV with no locking of its own.
func TestRejectionsDispatchedAfterBatchSerially(t *testing.T) {
	spy := &rejectSpy{}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "absent_one", Args: []byte(`{}`)},
				core.ToolCall{ID: "c2", Name: "absent_two", Args: []byte(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(okTool{"keep"}),
		agent.WithMiddleware(spy),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}

	got := spy.seen()
	if len(got) != 2 {
		t.Fatalf("rejections = %d (%v), want 2", len(got), got)
	}
	if spy.overlapped {
		t.Fatal("OnToolReject ran concurrently for two calls in one batch")
	}
	// Both calls are rejected, and both KV writes landed — the unguarded writes are
	// the point of dispatching after the batch joined.
	for _, id := range []string{"c1", "c2"} {
		if spy.kv[id] != agent.RejectUnknownTool.String() {
			t.Fatalf("State.KV[reject_%s] = %q, want %q", id, spy.kv[id], agent.RejectUnknownTool.String())
		}
	}
	for _, r := range got {
		if r.Class != agent.RejectUnknownTool {
			t.Fatalf("class = %v, want unknown_tool for both", r.Class)
		}
	}
}

// TestRejectionsCheckpointedSameStep: the hook runs before the step's snapshot is
// written, so what a guard records is durable from this step on rather than waiting
// for the next one.
func TestRejectionsCheckpointedSameStep(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	spy := &rejectSpy{}
	var answer string
	a, err := agent.New(
		agent.WithModel(echoScript("absent", `{}`, &answer)),
		agent.WithTools(okTool{"keep"}),
		agent.WithMiddleware(spy),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("t1")); err != nil {
		t.Fatal(err)
	}

	history, err := store.History(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	// Walk oldest-first to the first snapshot that carries the rejection in history:
	// that same snapshot must already carry the guard's KV write.
	var seen bool
	for i, cp := range history {
		if !hasToolResult(cp.State.Messages, "unknown tool: absent") {
			continue
		}
		if cp.State.KV["reject_c1"] == nil {
			t.Fatalf("checkpoint %d has the rejection in history but not in KV: %v", i, cp.State.KV)
		}
		if cp.Step != 0 {
			t.Fatalf("rejection first appears at step %d, want step 0", cp.Step)
		}
		seen = true
		break
	}
	if !seen {
		t.Fatal("no checkpoint carried the rejected call's result")
	}
}

// TestResumedBatchDispatchesRejections: the batch an approval leaves behind is
// counted too, or a model could spend its rejections past the gate.
func TestResumedBatchDispatchesRejections(t *testing.T) {
	ctx := context.Background()
	spy := &rejectSpy{}
	gate := &gateOnce{}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "strict", `{}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(strictTool{okTool{"strict"}}),
		agent.WithMiddleware(gate, spy),
		agent.WithCheckpointer(checkpoint.NewMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	run, pending := pauseOn(t, a, ctx, "go")
	if len(spy.seen()) != 0 {
		t.Fatalf("rejections before approval = %v, want none: nothing ran", spy.seen())
	}
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}

	got := spy.seen()
	if len(got) != 1 {
		t.Fatalf("rejections after resume = %d (%v), want 1", len(got), got)
	}
	if got[0].Class != agent.RejectSchemaInvalid {
		t.Fatalf("class = %v, want schema_invalid", got[0].Class)
	}
	if !strings.Contains(got[0].Detail, `"q"`) {
		t.Fatalf("Detail = %q, want the validation text naming the missing field", got[0].Detail)
	}
	if spy.kv["c1"] != agent.RejectSchemaInvalid.String() {
		t.Fatalf("State.KV[reject_c1] = %q, want %q", spy.kv["c1"], agent.RejectSchemaInvalid.String())
	}
}

// --- helpers ----------------------------------------------------------------

// rejectSpy records what the loop reported. Its counters and maps are touched with
// no locking on purpose: ToolRejecter is documented to run serially on the loop's
// goroutine, so a concurrent dispatch is both a failed assertion here and a data
// race under go test -race.
type rejectSpy struct {
	agent.BaseMiddleware

	inHook     int
	overlapped bool
	got        []agent.ToolRejection
	kv         map[string]string
}

func (s *rejectSpy) OnToolReject(lc *agent.LoopContext, r agent.ToolRejection) {
	s.inHook++
	if s.inHook > 1 {
		s.overlapped = true
	}
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: "reject_" + r.Call.ID, Value: r.Class.String()})
	time.Sleep(5 * time.Millisecond) // wide enough that an interleaved dispatch would show
	s.inHook--
	s.got = append(s.got, r)
	s.kv = map[string]string{}
	for k, v := range lc.State.KV {
		if strings.HasPrefix(k, "reject_") {
			s.kv[strings.TrimPrefix(k, "reject_")] = asString(v)
		}
	}
}

func (s *rejectSpy) seen() []agent.ToolRejection {
	return append([]agent.ToolRejection(nil), s.got...)
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// hasToolResult reports whether any tool-role message part carries the given text.
// The parts a tool batch appends are core.ToolResult values, so those are what has
// to be unwrapped here.
func hasToolResult(msgs []core.Message, want string) bool {
	for _, m := range msgs {
		if m.Role != core.RoleTool {
			continue
		}
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && strings.Contains(toolResultText(tr), want) {
				return true
			}
			if t, ok := p.(core.Text); ok && strings.Contains(t.Text, want) {
				return true
			}
		}
	}
	return false
}
