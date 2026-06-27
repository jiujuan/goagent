package prompt

import (
	"context"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// Peer is the minimal view of a sub-agent a Section needs for delegation-aware
// prompts. It carries only name and description, decoupling prompt from the
// agent package.
type Peer struct {
	Name        string
	Description string
}

// Context is the per-invocation input handed to every Section. It is the DTO
// that decouples prompt from agent: the agent populates it from the run, so
// Sections never import agent. It embeds context.Context so Sections can honour
// cancellation and pass it to any lookups.
type Context struct {
	context.Context

	State       *core.State
	UserContent core.Message
	AgentName   string
	AgentDesc   string
	Tools       []tool.Tool
	SubAgents   []Peer
}
