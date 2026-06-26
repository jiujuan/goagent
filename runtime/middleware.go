package runtime

import (
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Middleware hooks into the AgentLoop's phases. Unlike v1's model decorator,
// these hooks see the whole loop — tool execution included — so HITL,
// permissions, retry, compaction, RAG, steering and summarization all express
// as middleware (ADR 0023).
//
// "Before" hooks and OnError run in registration order; "After" hooks run in
// reverse (onion outbound). Each returning hook yields a core.Directive; the
// loop folds a phase's directives by precedence (core.Resolve). Embed
// BaseMiddleware to get no-op defaults and override only the hooks you need.
//
// Hooks must be safe to call: a returned error fails the run; AfterTool may run
// concurrently across a parallel tool batch, so its implementation must be
// goroutine-safe.
type Middleware interface {
	// BeforeModel runs before each model call (gate, steering injection).
	BeforeModel(*LoopContext) (core.Directive, error)
	// ModifyRequest may rewrite the request for just this call (compaction, RAG).
	ModifyRequest(*LoopContext, *llm.Request) error
	// AfterModel runs after the model responds.
	AfterModel(*LoopContext, *llm.Response) (core.Directive, error)
	// BeforeTool gates a tool call; return Interrupt for HITL, Stop to abort.
	BeforeTool(*LoopContext, *core.ToolCall) (core.Directive, error)
	// AfterTool runs after a tool result is produced.
	AfterTool(*LoopContext, *core.ToolResult) (core.Directive, error)
	// OnError runs when a model call errors (retry decisions live here).
	OnError(*LoopContext, error) (core.Directive, error)
}

// BaseMiddleware provides no-op defaults (every hook returns Continue, nil).
// Embed it so a concrete middleware overrides only the hooks it cares about.
type BaseMiddleware struct{}

func (BaseMiddleware) BeforeModel(*LoopContext) (core.Directive, error) {
	return core.Directive{}, nil
}
func (BaseMiddleware) ModifyRequest(*LoopContext, *llm.Request) error { return nil }
func (BaseMiddleware) AfterModel(*LoopContext, *llm.Response) (core.Directive, error) {
	return core.Directive{}, nil
}
func (BaseMiddleware) BeforeTool(*LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{}, nil
}
func (BaseMiddleware) AfterTool(*LoopContext, *core.ToolResult) (core.Directive, error) {
	return core.Directive{}, nil
}
func (BaseMiddleware) OnError(*LoopContext, error) (core.Directive, error) {
	return core.Directive{}, nil
}

// Stack runs a list of middleware as one, folding per-phase directives by
// precedence and applying onion ordering (before = forward, after = reverse).
type Stack struct{ mws []Middleware }

// NewStack composes middleware. The first listed is the outermost.
func NewStack(mws ...Middleware) *Stack { return &Stack{mws: mws} }

func (s *Stack) BeforeModel(lc *LoopContext) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for _, m := range s.mws {
		d, err := m.BeforeModel(lc)
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

func (s *Stack) ModifyRequest(lc *LoopContext, req *llm.Request) error {
	for _, m := range s.mws {
		if err := m.ModifyRequest(lc, req); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stack) AfterModel(lc *LoopContext, resp *llm.Response) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for i := len(s.mws) - 1; i >= 0; i-- {
		d, err := s.mws[i].AfterModel(lc, resp)
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

func (s *Stack) BeforeTool(lc *LoopContext, call *core.ToolCall) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for _, m := range s.mws {
		d, err := m.BeforeTool(lc, call)
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

func (s *Stack) AfterTool(lc *LoopContext, res *core.ToolResult) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for i := len(s.mws) - 1; i >= 0; i-- {
		d, err := s.mws[i].AfterTool(lc, res)
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

func (s *Stack) OnError(lc *LoopContext, e error) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for _, m := range s.mws {
		d, err := m.OnError(lc, e)
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

var _ Middleware = BaseMiddleware{}
