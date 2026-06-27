package middleware

import (
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Logger is the minimal sink Tracing writes to (satisfied by *log.Logger).
type Logger interface {
	Printf(format string, args ...any)
}

// Tracing logs each model reply, tool completion, and model error. It is a pure
// observer — every hook returns Continue. Pass nil to use the standard logger.
func Tracing(logger Logger) agent.Middleware {
	if logger == nil {
		logger = log.Default()
	}
	return &tracing{log: logger}
}

type tracing struct {
	agent.BaseMiddleware
	log Logger
}

func (t *tracing) AfterModel(lc *agent.LoopContext, r *llm.Response) (core.Directive, error) {
	t.log.Printf("turn %d: model replied (%d tool call(s))", lc.Step, len(r.Message.ToolCalls()))
	return core.Directive{}, nil
}

func (t *tracing) AfterTool(_ *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	t.log.Printf("tool %s done (isError=%v)", tr.Name, tr.IsError)
	return core.Directive{}, nil
}

func (t *tracing) OnError(_ *agent.LoopContext, err error) (core.Directive, error) {
	t.log.Printf("model error: %v", err)
	return core.Directive{}, nil
}
