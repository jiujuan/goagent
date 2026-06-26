package plan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// StepContext is handed to an Executor on invocation. It embeds the request
// context (so it honors cancellation and per-step timeouts) and exposes the
// session state, through which steps read their upstreams' outputs and publish
// their own. The convention: a completed step's output lives at the state key
// StepResultKey(step.ID).
type StepContext struct {
	context.Context
	State session.State
	Step  *Step

	// ictx is the enclosing invocation, used by AgentExecutor to run a sub-agent.
	ictx agent.InvocationContext
}

// StepResultKey is the session-state key under which a completed step's textual
// output is published, for downstream steps to read.
func StepResultKey(id string) string { return "step:" + id + ":result" }

// Executor is how a single step does its work. The three built-ins bridge the
// rest of the framework: a tool, a sub-agent, or a plain Go function.
type Executor interface {
	Execute(sc *StepContext) (*StepResult, error)
}

// --- ToolExecutor -----------------------------------------------------------

// ToolExecutor runs one tool with fixed arguments. The tool's textual result
// becomes the step's Output; a tool result flagged IsError becomes a step error.
type ToolExecutor struct {
	Tool tool.Tool
	Args json.RawMessage
}

func (e ToolExecutor) Execute(sc *StepContext) (*StepResult, error) {
	if e.Tool == nil {
		return nil, fmt.Errorf("step %q: nil tool", sc.Step.ID)
	}
	tctx := &tool.Context{
		Context: sc.Context,
		State:   sc.State,
		Actions: &core.Actions{},
		CallID:  "plan:" + sc.Step.ID,
	}
	res, err := e.Tool.Call(tctx, e.Args)
	if err != nil {
		return nil, err
	}
	out := partsText(res.Content)
	if res.IsError {
		return nil, fmt.Errorf("%s", out)
	}
	return &StepResult{StepID: sc.Step.ID, Title: sc.Step.Name, Output: out}, nil
}

// --- AgentExecutor ----------------------------------------------------------

// AgentExecutor runs a sub-agent to completion and takes its final assistant
// text as the step's Output. This is what lets a plan step be an arbitrarily
// complex sub-computation — including another PlanAgent (nested plans).
type AgentExecutor struct {
	Agent agent.Agent
}

func (e AgentExecutor) Execute(sc *StepContext) (*StepResult, error) {
	if e.Agent == nil {
		return nil, fmt.Errorf("step %q: nil agent", sc.Step.ID)
	}
	ictx := sc.ictx.ForSubAgent(e.Agent, "")
	var final string
	for ev, err := range e.Agent.Run(ictx) {
		if err != nil {
			return nil, err
		}
		if ev.IsFinalResponse() {
			final = ev.Message.Text()
		}
	}
	return &StepResult{StepID: sc.Step.ID, Title: sc.Step.Name, Output: final}, nil
}

// --- FuncExecutor -----------------------------------------------------------

// FuncExecutor adapts a plain Go function into an Executor — handy for native
// steps and for tests that need no model.
type FuncExecutor func(sc *StepContext) (*StepResult, error)

func (f FuncExecutor) Execute(sc *StepContext) (*StepResult, error) { return f(sc) }

// partsText concatenates the Text parts of a tool result.
func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}
