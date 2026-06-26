// Package runtime is v2's execution engine: the explicit AgentLoop phase
// machine and the loop-scoped Middleware that wraps it. It replaces v1's hidden
// `for range maxSteps` inside LLMAgent.Run (which buried control flow and could
// only be decorated at the llm.Model boundary) with an inspectable state
// machine whose every phase publishes observational events to a Bus and can be
// intercepted by middleware. See ADR 0023.
//
// Dual-track note: this package is additive and does not touch v1. The v2
// Middleware interface lives here (not in the v1 `middleware` package, whose
// Middleware is `func(llm.Model) llm.Model`). AgentSpec/LoopPolicy live here for
// now; ADR 0023 step 7 may extract them into a `spec` package.
package runtime

import (
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// AgentSpec declaratively describes one agent. It is pure data — no execution
// logic — so it is easy to test, serialize and compose. The loop logic lives in
// AgentLoop; the spec just configures it.
type AgentSpec struct {
	Name        string
	Description string
	Model       llm.Model
	Instruction string
	Tools       []tool.Tool
	Middleware  []Middleware
	Loop        LoopPolicy
}

// LoopPolicy bounds and shapes the controllable loop.
type LoopPolicy struct {
	// MaxSteps caps the model<->tool iterations (default 16).
	MaxSteps int
	// ToolExecution selects concurrent (default) or one-at-a-time tool running.
	ToolExecution ToolExecMode
}

// ToolExecMode selects how a batch of tool calls in one step is executed.
type ToolExecMode int

const (
	// Parallel runs a step's tool calls concurrently (default).
	Parallel ToolExecMode = iota
	// Sequential runs them one at a time, in the model's call order.
	Sequential
)
