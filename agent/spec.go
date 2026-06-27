// Package agent is v2's execution model: the declarative Spec, the loop-scoped
// Middleware, and the explicit AgentLoop phase machine. These types are
// mutually recursive (a Spec holds []Middleware; Middleware hooks take a
// *LoopContext; the loop consumes a Spec), so they live together in one package.
//
// The package depends only on abstractions — bus.Bus, checkpoint.Checkpointer,
// llm, tool, core — and NOT on the runtime engine or concrete middleware, which
// import agent. That keeps the dependency graph acyclic: runtime → agent,
// middleware → agent.
package agent

import (
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// Spec declaratively describes one LLM agent. It is pure data — no execution
// logic — so it is easy to test, serialize and compose. The loop logic lives in
// AgentLoop; the Spec configures it.
type Spec struct {
	Name        string
	Description string
	Model       llm.Model
	Instruction string
	Tools       []tool.Tool
	Middleware  []Middleware
	SubAgents   []Spec
	Loop        LoopPolicy
	// ModelOptions are applied to every model request.
	ModelOptions []llm.Option
}

// compileTools indexes the spec's tools by name and builds their advertised
// schemas for the model request.
func (s Spec) compileTools() (map[string]tool.Tool, []llm.ToolSchema) {
	return tool.ByName(s.Tools), tool.Schemas(s.Tools)
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
