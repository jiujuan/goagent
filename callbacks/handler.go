// Package callbacks defines a single observability interface that components
// fire lifecycle hooks into. It is opt-in: a nil Handler means zero overhead.
package callbacks

import (
	"context"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Handler receives lifecycle notifications from the runner, agents, the model,
// and tools. Embed NoopHandler to implement only the hooks you care about.
type Handler interface {
	OnEvent(ctx context.Context, e *core.Event)

	OnModelStart(ctx context.Context, req *llm.Request)
	OnModelEnd(ctx context.Context, resp *llm.Response)
	OnModelError(ctx context.Context, err error)

	OnToolStart(ctx context.Context, name string, args []byte)
	OnToolEnd(ctx context.Context, name string, result string)
	OnToolError(ctx context.Context, name string, err error)
}

// NoopHandler implements Handler with empty methods, for embedding.
type NoopHandler struct{}

func (NoopHandler) OnEvent(context.Context, *core.Event)        {}
func (NoopHandler) OnModelStart(context.Context, *llm.Request)  {}
func (NoopHandler) OnModelEnd(context.Context, *llm.Response)   {}
func (NoopHandler) OnModelError(context.Context, error)         {}
func (NoopHandler) OnToolStart(context.Context, string, []byte) {}
func (NoopHandler) OnToolEnd(context.Context, string, string)   {}
func (NoopHandler) OnToolError(context.Context, string, error)  {}

var _ Handler = NoopHandler{}
