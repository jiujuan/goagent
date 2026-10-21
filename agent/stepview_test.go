package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func toolNames(schemas []llm.ToolSchema) []string {
	out := make([]string, len(schemas))
	for i, s := range schemas {
		out[i] = s.Name
	}
	return out
}

// requestMutator is the smallest ModifyRequest hook: it edits the outgoing
// request so a test can pin what the loop accepts or refuses to freeze.
type requestMutator struct {
	agent.BaseMiddleware
	fn func(*agent.LoopContext, *llm.Request)
}

func (m *requestMutator) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	m.fn(lc, req)
	return nil
}

// viewLifecycleSpy records what lc.View() returned in every phase that can see
// a frozen step view.
type viewLifecycleSpy struct {
	agent.BaseMiddleware

	mu               sync.Mutex
	beforeModelNil   bool
	modifyRequestNil bool
	modelContextNil  bool
	afterModel       *agent.StepView
	beforeTool       *agent.StepView
	afterTool        *agent.StepView
}

func (s *viewLifecycleSpy) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	s.mu.Lock()
	s.beforeModelNil = lc.View() == nil
	s.mu.Unlock()
	return core.Directive{}, nil
}

func (s *viewLifecycleSpy) ModifyRequest(lc *agent.LoopContext, _ *llm.Request) error {
	s.mu.Lock()
	s.modifyRequestNil = lc.View() == nil
	s.mu.Unlock()
	return nil
}

func (s *viewLifecycleSpy) ModelContext(lc *agent.LoopContext, ctx context.Context) context.Context {
	s.mu.Lock()
	s.modelContextNil = lc.View() == nil
	s.mu.Unlock()
	return ctx
}

func (s *viewLifecycleSpy) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	s.mu.Lock()
	if s.afterModel == nil {
		s.afterModel = lc.View()
	}
	s.mu.Unlock()
	return core.Directive{}, nil
}

func (s *viewLifecycleSpy) BeforeTool(lc *agent.LoopContext, _ *core.ToolCall) (core.Directive, error) {
	s.mu.Lock()
	s.beforeTool = lc.View()
	s.mu.Unlock()
	return core.Directive{}, nil
}

func (s *viewLifecycleSpy) AfterTool(lc *agent.LoopContext, _ *core.ToolResult) (core.Directive, error) {
	s.mu.Lock()
	s.afterTool = lc.View()
	s.mu.Unlock()
	return core.Directive{}, nil
}

// TestStepViewLifecycle pins when a frozen view exists and that it mirrors the
// request the model was handed.
func TestStepViewLifecycle(t *testing.T) {
	spy := &viewLifecycleSpy{}
	var requests []*llm.Request
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		requests = append(requests, req)
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "ping", `{}`)
	})
	ping := tool.New("ping", "answers pings", func(*tool.Context, struct{}) (string, error) {
		return "pong", nil
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(ping),
		agent.WithMiddleware(spy),
		agent.WithModelOptions(llm.WithModel("view-model"), llm.WithTemperature(0.2), llm.WithStop("STOP")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if !spy.beforeModelNil {
		t.Fatal("View() must be nil in BeforeModel: the request is not built yet")
	}
	if !spy.modifyRequestNil {
		t.Fatal("View() must be nil in ModifyRequest: the request is still being edited")
	}
	if spy.modelContextNil {
		t.Fatal("View() must be frozen before the provider context is derived")
	}
	for name, v := range map[string]*agent.StepView{
		"AfterModel": spy.afterModel,
		"BeforeTool": spy.beforeTool,
		"AfterTool":  spy.afterTool,
	} {
		if v == nil {
			t.Fatalf("View() is nil in %s", name)
		}
	}
	v := spy.afterModel
	req := requests[0]
	if v.Step() != 0 || v.Model() != "view-model" {
		t.Fatalf("view step/model = %d/%q, want 0/view-model", v.Step(), v.Model())
	}
	if v.System() != req.System {
		t.Fatalf("view system = %q, want %q", v.System(), req.System)
	}
	if !reflect.DeepEqual(v.Options(), req.Options) {
		t.Fatalf("view options = %+v, want %+v", v.Options(), req.Options)
	}
	wantNames := toolNames(req.Tools)
	if got := v.Tools().Names(); !slices.Equal(got, wantNames) {
		t.Fatalf("view tool names = %v, want %v", got, wantNames)
	}
	if got := v.Digest().Advertised; !slices.Equal(got, wantNames) {
		t.Fatalf("view advertised = %v, want %v", got, wantNames)
	}
	if len(v.Messages()) != len(req.Messages) {
		t.Fatalf("view messages = %d, want %d", len(v.Messages()), len(req.Messages))
	}
	if !v.Tools().Advertised("ping") || v.Tools().Sequential("ping") {
		t.Fatalf("view tool table = advertised:%v sequential:%v", v.Tools().Advertised("ping"), v.Tools().Sequential("ping"))
	}
	if _, ok := v.Tools().Resolve("ping"); !ok {
		t.Fatal("view cannot resolve an advertised tool")
	}
}

// TestInjectedToolReplacesSchemaAndImplementation: a same-name injection must
// replace what the model sees, not merely what executes.
func TestInjectedToolReplacesSchemaAndImplementation(t *testing.T) {
	static := tool.New("lookup", "static lookup", func(_ *tool.Context, in struct {
		Q string `json:"q"`
	}) (string, error) {
		return "static:" + in.Q, nil
	})
	injected := tool.New("lookup", "injected lookup", func(_ *tool.Context, in struct {
		Term string `json:"term"`
	}) (string, error) {
		return "injected:" + in.Term, nil
	})

	var first []llm.ToolSchema
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		first = append([]llm.ToolSchema(nil), req.Tools...)
		return mock.CallTool("c1", "lookup", `{"term":"x"}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(static),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool { return injected }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	if res != "saw:injected:x" {
		t.Fatalf("result = %q, want the injected implementation to run", res)
	}
	if n := countNamed(first, "lookup"); n != 1 {
		t.Fatalf("lookup advertised %d times, want exactly 1", n)
	}
	if first[0].Description != "injected lookup" {
		t.Fatalf("advertised description = %q, want the injected one", first[0].Description)
	}
	if !json.Valid(first[0].Parameters) || !containsJSONField(first[0].Parameters, "term") {
		t.Fatalf("advertised parameters = %s, want the injected schema", first[0].Parameters)
	}
}

func containsJSONField(raw json.RawMessage, field string) bool {
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		return false
	}
	props, _ := v["properties"].(map[string]any)
	_, ok := props[field]
	return ok
}

func failingRun(t *testing.T, mw agent.Middleware, opts ...agent.Option) (int, error) {
	t.Helper()
	var calls int
	model := mock.New("m", func(*llm.Request) *llm.Response {
		calls++
		return mock.Text("model must not be called")
	})
	base := []agent.Option{agent.WithModel(model), agent.WithMiddleware(mw)}
	a, err := agent.New(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Run(context.Background(), "go")
	return calls, err
}

// TestAdvertisedToolWithoutImplementationFailsBeforeModel: a middleware can
// advertise anything it likes, but the loop refuses before spending a model call.
func TestAdvertisedToolWithoutImplementationFailsBeforeModel(t *testing.T) {
	mw := &requestMutator{fn: func(_ *agent.LoopContext, req *llm.Request) {
		req.Tools = append(req.Tools, llm.ToolSchema{
			Name:        "ghost",
			Description: "never registered",
			Parameters:  json.RawMessage(`{"type":"object","properties":{}}`),
		})
	}}
	calls, err := failingRun(t, mw)
	if !errors.Is(err, agent.ErrToolViewInvalid) {
		t.Fatalf("error = %v, want ErrToolViewInvalid", err)
	}
	if calls != 0 {
		t.Fatalf("model calls = %d, want 0", calls)
	}
}

// TestToolSchemaRewriteFailsBeforeModel: changing only the advertisement would
// leave the model and the handler disagreeing, so it is refused.
func TestToolSchemaRewriteFailsBeforeModel(t *testing.T) {
	lookup := tool.New("lookup", "look up a thing", func(_ *tool.Context, in struct {
		Q string `json:"q"`
	}) (string, error) {
		return in.Q, nil
	})
	cases := map[string]func(*llm.ToolSchema){
		"description": func(s *llm.ToolSchema) { s.Description = "rewritten" },
		"parameters": func(s *llm.ToolSchema) {
			s.Parameters = json.RawMessage(`{"type":"object","properties":{"term":{"type":"string"}}}`)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			mw := &requestMutator{fn: func(_ *agent.LoopContext, req *llm.Request) {
				mutate(&req.Tools[0])
			}}
			calls, err := failingRun(t, mw, agent.WithTools(lookup))
			if !errors.Is(err, agent.ErrToolViewInvalid) {
				t.Fatalf("error = %v, want ErrToolViewInvalid", err)
			}
			if calls != 0 {
				t.Fatalf("model calls = %d, want 0", calls)
			}
		})
	}
}

// TestSchemaParameterKeyOrderAccepted: the comparison is JSON-semantic, so a
// rewrite that only reorders keys is not a mismatch.
func TestSchemaParameterKeyOrderAccepted(t *testing.T) {
	custom := &viewShapeTool{
		name:   "shape",
		desc:   "shape test",
		schema: `{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`,
	}
	mw := &requestMutator{fn: func(_ *agent.LoopContext, req *llm.Request) {
		req.Tools[0].Parameters = json.RawMessage(`{"required":["q"],"properties":{"q":{"type":"string"}},"type":"object"}`)
	}}
	a, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		agent.WithTools(custom),
		agent.WithMiddleware(mw),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatalf("key-order-only rewrite was refused: %v", err)
	}
}

// viewShapeTool is a hand-written tool whose schema bytes a test controls.
type viewShapeTool struct {
	name   string
	desc   string
	schema string
}

func (t *viewShapeTool) Name() string            { return t.name }
func (t *viewShapeTool) Description() string     { return t.desc }
func (t *viewShapeTool) Schema() json.RawMessage { return json.RawMessage(t.schema) }
func (t *viewShapeTool) Call(*tool.Context, json.RawMessage) (*tool.Result, error) {
	return tool.TextResult("ok"), nil
}

// TestDuplicateAdvertisementFailsBeforeModel: one name, one slot.
func TestDuplicateAdvertisementFailsBeforeModel(t *testing.T) {
	lookup := tool.New("lookup", "look up a thing", func(*tool.Context, struct{}) (string, error) {
		return "ok", nil
	})
	mw := &requestMutator{fn: func(_ *agent.LoopContext, req *llm.Request) {
		req.Tools = append(req.Tools, req.Tools[0])
	}}
	calls, err := failingRun(t, mw, agent.WithTools(lookup))
	if !errors.Is(err, agent.ErrToolViewInvalid) {
		t.Fatalf("error = %v, want ErrToolViewInvalid", err)
	}
	if calls != 0 {
		t.Fatalf("model calls = %d, want 0", calls)
	}
}

// phaseToolInjector injects one tool from a chosen middleware phase, once, at
// one step. The AfterModel/AfterTool variants also record whether the current
// step's request and view changed after AddTool.
type phaseToolInjector struct {
	agent.BaseMiddleware

	phase string
	tool  tool.Tool
	step  int

	injected bool

	beforeAddTools int
	afterAddTools  int
	viewAdvertised bool
}

func (m *phaseToolInjector) inject(lc *agent.LoopContext) {
	if m.injected || lc.Step != m.step {
		return
	}
	m.injected = true
	lc.AddTool(m.tool)
}

func (m *phaseToolInjector) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	if m.phase == "before_model" {
		m.inject(lc)
	}
	return core.Directive{}, nil
}

func (m *phaseToolInjector) ModifyRequest(lc *agent.LoopContext, _ *llm.Request) error {
	if m.phase == "modify_request" {
		m.inject(lc)
	}
	return nil
}

func (m *phaseToolInjector) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	if m.phase != "after_model" || m.injected {
		return core.Directive{}, nil
	}
	m.beforeAddTools = len(lc.Request.Tools)
	m.inject(lc)
	m.afterAddTools = len(lc.Request.Tools)
	m.viewAdvertised = lc.View().Tools().Advertised(m.tool.Name())
	return core.Directive{}, nil
}

func (m *phaseToolInjector) AfterTool(lc *agent.LoopContext, _ *core.ToolResult) (core.Directive, error) {
	if m.phase != "after_tool" || m.injected {
		return core.Directive{}, nil
	}
	m.beforeAddTools = len(lc.Request.Tools)
	m.inject(lc)
	m.afterAddTools = len(lc.Request.Tools)
	m.viewAdvertised = lc.View().Tools().Advertised(m.tool.Name())
	return core.Directive{}, nil
}

// TestAddToolPhaseVisibility pins which step an injection reaches: everything
// before the freeze reaches this step; anything after it waits for the next.
func TestAddToolPhaseVisibility(t *testing.T) {
	late := func(ran *atomic.Int32) tool.Tool {
		return tool.New("late", "injected late", func(*tool.Context, struct{}) (string, error) {
			ran.Add(1)
			return "late", nil
		})
	}

	t.Run("before_model", func(t *testing.T) {
		var ran atomic.Int32
		mw := &phaseToolInjector{phase: "before_model", tool: late(&ran)}
		var requests []*llm.Request
		model := mock.New("m", func(req *llm.Request) *llm.Response {
			requests = append(requests, req)
			if tr, ok := mock.LastToolResult(req); ok {
				return mock.Text("saw:" + toolResultText(tr))
			}
			return mock.CallTool("c1", "late", `{}`)
		})
		a, err := agent.New(agent.WithModel(model), agent.WithMiddleware(mw))
		if err != nil {
			t.Fatal(err)
		}
		res, err := a.Run(context.Background(), "go")
		if err != nil {
			t.Fatal(err)
		}
		if n := countNamed(requests[0].Tools, "late"); n != 1 {
			t.Fatalf("step 0 advertised late %d times, want 1", n)
		}
		if ran.Load() != 1 || res != "saw:late" {
			t.Fatalf("ran=%d result=%q, want the tool callable in the same step", ran.Load(), res)
		}
	})

	t.Run("modify_request", func(t *testing.T) {
		var ran atomic.Int32
		mw := &phaseToolInjector{phase: "modify_request", tool: late(&ran)}
		var requests []*llm.Request
		model := mock.New("m", func(req *llm.Request) *llm.Response {
			requests = append(requests, req)
			if tr, ok := mock.LastToolResult(req); ok {
				return mock.Text("saw:" + toolResultText(tr))
			}
			return mock.CallTool("c1", "late", `{}`)
		})
		a, err := agent.New(agent.WithModel(model), agent.WithMiddleware(mw))
		if err != nil {
			t.Fatal(err)
		}
		res, err := a.Run(context.Background(), "go")
		if err != nil {
			t.Fatal(err)
		}
		if n := countNamed(requests[0].Tools, "late"); n != 1 {
			t.Fatalf("step 0 advertised late %d times, want 1", n)
		}
		if ran.Load() != 1 || res != "saw:late" {
			t.Fatalf("ran=%d result=%q, want the tool callable in the same step", ran.Load(), res)
		}
	})

	for _, phase := range []string{"after_model", "after_tool"} {
		t.Run(phase, func(t *testing.T) {
			var lateRan, pingRan atomic.Int32
			ping := tool.New("ping", "answers pings", func(*tool.Context, struct{}) (string, error) {
				pingRan.Add(1)
				return "pong", nil
			})
			mw := &phaseToolInjector{phase: phase, tool: late(&lateRan)}
			var requests []*llm.Request
			model := mock.New("m", func(req *llm.Request) *llm.Response {
				requests = append(requests, req)
				switch len(requests) {
				case 1:
					return mock.CallTool("c1", "ping", `{}`)
				case 2:
					return mock.CallTool("c2", "late", `{}`)
				default:
					return mock.Text("done")
				}
			})
			a, err := agent.New(agent.WithModel(model), agent.WithTools(ping), agent.WithMiddleware(mw))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Run(context.Background(), "go"); err != nil {
				t.Fatal(err)
			}
			if len(requests) != 3 {
				t.Fatalf("model requests = %d, want 3", len(requests))
			}
			if n := countNamed(requests[0].Tools, "late"); n != 0 {
				t.Fatalf("step 0 advertised late %d times, want 0: injection happened after the freeze", n)
			}
			if n := countNamed(requests[1].Tools, "late"); n != 1 {
				t.Fatalf("step 1 advertised late %d times, want 1", n)
			}
			if pingRan.Load() != 1 || lateRan.Load() != 1 {
				t.Fatalf("ping ran %d, late ran %d, want 1 each", pingRan.Load(), lateRan.Load())
			}
			if mw.beforeAddTools != mw.afterAddTools {
				t.Fatalf("AddTool changed the current request: %d -> %d tools", mw.beforeAddTools, mw.afterAddTools)
			}
			if mw.viewAdvertised {
				t.Fatal("AddTool changed the frozen view of the current step")
			}
		})
	}
}

// TestInjectedSequentialToolReplacingStaticDowngradesBatch: the sequential flag
// follows the winning implementation.
func TestInjectedSequentialToolReplacingStaticDowngradesBatch(t *testing.T) {
	o := &overlap{}
	static := slowTool(t, "both", o)
	injected := tool.AsSequential(slowTool(t, "both", o))
	a, err := agent.New(
		agent.WithModel(twoCallModel(callTwo("both", "both"))),
		agent.WithTools(static),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool { return injected }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if p := o.peak.Load(); p != 1 {
		t.Fatalf("peak concurrency = %d, want 1: the injected sequential implementation must serialize", p)
	}
}

// TestInjectedParallelToolReplacingStaticSequentialAllowsParallel: the static
// tool's flag must not outlive a non-sequential replacement.
func TestInjectedParallelToolReplacingStaticSequentialAllowsParallel(t *testing.T) {
	o := &overlap{}
	static := tool.AsSequential(slowTool(t, "both", o))
	injected := slowTool(t, "both", o)
	a, err := agent.New(
		agent.WithModel(twoCallModel(callTwo("both", "both"))),
		agent.WithTools(static),
		agent.WithMiddleware(&injector{make: func(*agent.LoopContext) tool.Tool { return injected }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if p := o.peak.Load(); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2: the replacement is not sequential", p)
	}
}

// TestNotAdvertisedRejectionClass: a registered tool that this step did not
// advertise is refused without executing, and the model is told why.
func TestNotAdvertisedRejectionClass(t *testing.T) {
	var ran atomic.Int32
	keep := tool.New("keep", "registered but hidden", func(*tool.Context, struct{}) (string, error) {
		ran.Add(1)
		return "ran", nil
	})
	clear := &requestMutator{fn: func(_ *agent.LoopContext, req *llm.Request) { req.Tools = nil }}
	var answer string
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			answer = toolResultText(tr)
			return mock.Text("done")
		}
		return mock.CallTool("c1", "keep", `{}`)
	})
	spy := &rejectSpy{}
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(keep),
		agent.WithMiddleware(clear, spy),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	got := spy.seen()
	if len(got) != 1 {
		t.Fatalf("rejections = %d (%v), want 1", len(got), got)
	}
	if got[0].Class != agent.RejectNotAdvertised {
		t.Fatalf("class = %v, want RejectNotAdvertised", got[0].Class)
	}
	if got[0].Class.String() != "not_advertised" {
		t.Fatalf("class name = %q, want not_advertised", got[0].Class.String())
	}
	if ran.Load() != 0 {
		t.Fatal("a tool that was not advertised must not execute")
	}
	if !strings.Contains(strings.ToLower(answer), "not advertised") {
		t.Fatalf("model-visible text = %q, want it to say the tool was not advertised", answer)
	}
}

// TestUnknownToolRejectionClassUnchanged: a name that was never registered is
// still an unknown tool, not a not-advertised one.
func TestUnknownToolRejectionClassUnchanged(t *testing.T) {
	var ran atomic.Int32
	keep := tool.New("keep", "registered", func(*tool.Context, struct{}) (string, error) {
		ran.Add(1)
		return "ran", nil
	})
	spy := &rejectSpy{}
	var answer string
	a, err := agent.New(
		agent.WithModel(echoScript("absent", `{}`, &answer)),
		agent.WithTools(keep),
		agent.WithMiddleware(spy),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	got := spy.seen()
	if len(got) != 1 || got[0].Class != agent.RejectUnknownTool {
		t.Fatalf("rejections = %+v, want one unknown-tool rejection", got)
	}
	if got[0].Class.String() != "unknown_tool" {
		t.Fatalf("class name = %q, want unknown_tool", got[0].Class.String())
	}
	if ran.Load() != 0 {
		t.Fatal("an unrelated registered tool must not run")
	}
	if !strings.Contains(strings.ToLower(answer), "unknown tool") {
		t.Fatalf("model-visible text = %q, want the unknown-tool wording", answer)
	}
}

// resumeViewSpy records the view a resumed batch executes under.
type resumeViewSpy struct {
	agent.BaseMiddleware

	mu      sync.Mutex
	view    *agent.StepView
	request *llm.Request
	seen    bool
}

func (s *resumeViewSpy) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if !lc.IsApproved(c) {
		return core.Directive{}, nil
	}
	s.mu.Lock()
	s.view = lc.View()
	s.request = lc.Request
	s.seen = true
	s.mu.Unlock()
	return core.Directive{}, nil
}

// TestResumeBatchHasFallbackView: a legacy pending checkpoint has no snapshot,
// so the resumed batch gets a view built from its own call names.
func TestResumeBatchHasFallbackView(t *testing.T) {
	ctx := context.Background()
	var ran atomic.Int32
	danger := tool.New("danger", "dangerous op", func(*tool.Context, struct{}) (string, error) {
		ran.Add(1)
		return "boom", nil
	})
	spy := &resumeViewSpy{}
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(&gateOnce{}, spy),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, pending := pauseOn(t, a, ctx, "go")
	// Pauses persist a snapshot since SV-02; strip it to reproduce the legacy
	// record this fallback covers.
	paused, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	legacy := *paused
	legacy.ID = "legacy-fallback"
	legacy.Snapshot = nil
	if err := store.Save(ctx, &legacy); err != nil {
		t.Fatal(err)
	}
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if !spy.seen || spy.view == nil {
		t.Fatal("the resumed batch ran without a step view")
	}
	if spy.request != nil {
		t.Fatalf("resumed view request = %+v, want nil", spy.request)
	}
	if got := spy.view.Tools().Names(); !slices.Equal(got, []string{"danger"}) {
		t.Fatalf("resumed view names = %v, want the batch's call names", got)
	}
	if got := spy.view.Digest(); got.ToolDigest != "" || got.SettingsDigest != "" || len(got.Advertised) != 0 {
		t.Fatalf("resumed fallback digest = %+v, want empty", got)
	}
	if !spy.view.Tools().Advertised("danger") {
		t.Fatal("the batch's own call name must be advertised in the fallback view")
	}
	if _, ok := spy.view.Tools().Resolve("danger"); !ok {
		t.Fatal("the fallback view cannot resolve the batch's tool")
	}
	if ran.Load() != 1 {
		t.Fatalf("tool ran %d times, want 1", ran.Load())
	}
}

// viewCopySpy mutates every slice the view hands out, then re-reads it.
type viewCopySpy struct {
	agent.BaseMiddleware

	storedSchemas    []llm.ToolSchema
	storedNames      []string
	storedAdvertised []string
	storedMsg0       core.Message
	storedStop       string

	afterSchemas    []llm.ToolSchema
	afterNames      []string
	afterAdvertised []string
	afterMsg0       core.Message
	afterStop       string
}

func (s *viewCopySpy) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	v := lc.View()
	schemas := v.Tools().Schemas()
	s.storedSchemas = append([]llm.ToolSchema(nil), schemas...)
	schemas[0].Name = "hacked"
	names := v.Tools().Names()
	s.storedNames = append([]string(nil), names...)
	names[0] = "hacked"
	dig := v.Digest()
	s.storedAdvertised = append([]string(nil), dig.Advertised...)
	dig.Advertised[0] = "hacked"
	msgs := v.Messages()
	s.storedMsg0 = msgs[0]
	msgs[0] = core.AssistantText("hacked")
	opts := v.Options()
	s.storedStop = opts.Stop[0]
	opts.Stop[0] = "hacked"

	s.afterSchemas = v.Tools().Schemas()
	s.afterNames = v.Tools().Names()
	s.afterAdvertised = v.Digest().Advertised
	s.afterMsg0 = v.Messages()[0]
	s.afterStop = v.Options().Stop[0]
	return core.Directive{}, nil
}

// TestStepViewReturnsCopies: accessors hand out copies, so an observer cannot
// rewrite the frozen step under the loop.
func TestStepViewReturnsCopies(t *testing.T) {
	spy := &viewCopySpy{}
	keep := tool.New("keep", "registered", func(*tool.Context, struct{}) (string, error) {
		return "ok", nil
	})
	a, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		agent.WithTools(keep),
		agent.WithMiddleware(spy),
		agent.WithModelOptions(llm.WithStop("END")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spy.afterSchemas, spy.storedSchemas) {
		t.Fatalf("Schemas() returned shared storage: %v -> %v", spy.storedSchemas, spy.afterSchemas)
	}
	if !slices.Equal(spy.afterNames, spy.storedNames) {
		t.Fatalf("Names() returned shared storage: %v -> %v", spy.storedNames, spy.afterNames)
	}
	if !slices.Equal(spy.afterAdvertised, spy.storedAdvertised) {
		t.Fatalf("Digest().Advertised returned shared storage: %v -> %v", spy.storedAdvertised, spy.afterAdvertised)
	}
	if !reflect.DeepEqual(spy.afterMsg0, spy.storedMsg0) {
		t.Fatal("Messages() returned shared storage")
	}
	if spy.afterStop != spy.storedStop {
		t.Fatalf("Options().Stop returned shared storage: %q -> %q", spy.storedStop, spy.afterStop)
	}
}

// digestSpy captures the digest of the first frozen view it sees.
type digestSpy struct {
	agent.BaseMiddleware

	mu     sync.Mutex
	digest agent.StepDigest
	got    bool
}

func (s *digestSpy) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	s.mu.Lock()
	if !s.got {
		s.digest = lc.View().Digest()
		s.got = true
	}
	s.mu.Unlock()
	return core.Directive{}, nil
}

func stabilityTool(name string) tool.Tool {
	return tool.New(name, name+" tool", func(*tool.Context, struct{}) (string, error) {
		return name, nil
	})
}

// mapToolInjector injects a set of tools from a map, so insertion order is the
// agent's own business; the step's advertisement must not depend on it.
type mapToolInjector struct {
	agent.BaseMiddleware
	tools map[string]tool.Tool
}

func (m *mapToolInjector) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	for _, t := range m.tools {
		lc.AddTool(t)
	}
	return core.Directive{}, nil
}

func runForDigest(t *testing.T, order []string, opts ...agent.Option) agent.StepDigest {
	t.Helper()
	spy := &digestSpy{}
	tools := make(map[string]tool.Tool, len(order))
	for _, name := range order {
		tools[name] = stabilityTool(name)
	}
	base := []agent.Option{
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		agent.WithMiddleware(&mapToolInjector{tools: tools}),
	}
	base = append(base, agent.WithMiddleware(spy))
	base = append(base, opts...)
	a, err := agent.New(base...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if !spy.got {
		t.Fatal("no frozen view observed")
	}
	return spy.digest
}

// TestStepViewDigestStability pins the digest inputs: request order and content
// for tools, everything but Metadata for settings.
func TestStepViewDigestStability(t *testing.T) {
	first := runForDigest(t, []string{"zzz", "aaa"})
	second := runForDigest(t, []string{"aaa", "zzz"})
	if first.ToolDigest != second.ToolDigest {
		t.Fatalf("tool digest depends on injection order: %q vs %q", first.ToolDigest, second.ToolDigest)
	}
	if !slices.Equal(first.Advertised, second.Advertised) || !slices.Equal(first.Advertised, []string{"aaa", "zzz"}) {
		t.Fatalf("advertised = %v vs %v, want sorted aaa, zzz", first.Advertised, second.Advertised)
	}

	base := runForDigest(t, []string{"aaa"}, agent.WithModelOptions(llm.WithTemperature(0.2), llm.WithMetadata("k", 1)))
	sameMeta := runForDigest(t, []string{"aaa"}, agent.WithModelOptions(llm.WithTemperature(0.2), llm.WithMetadata("k", "other")))
	if base.SettingsDigest != sameMeta.SettingsDigest {
		t.Fatal("settings digest must ignore Metadata")
	}
	hotter := runForDigest(t, []string{"aaa"}, agent.WithModelOptions(llm.WithTemperature(0.3), llm.WithMetadata("k", 1)))
	if base.SettingsDigest == hotter.SettingsDigest {
		t.Fatal("settings digest must change with Temperature")
	}
	otherSystem := runForDigest(t, []string{"aaa"}, agent.WithInstruction("a different system prompt"))
	if base.SettingsDigest == otherSystem.SettingsDigest {
		t.Fatal("settings digest must change with System")
	}
}
