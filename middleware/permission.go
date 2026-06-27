package middleware

import (
	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// Decision is a permission verdict for a tool call.
type Decision int

const (
	// AllowTool runs the tool normally.
	AllowTool Decision = iota
	// AskTool pauses for human approval (HITL): the run interrupts and the
	// caller resumes with Run.Decide + Run.Resume.
	AskTool
	// DenyTool refuses the tool and ends the run with a reason.
	DenyTool
)

// Rule decides what to do with a tool call. Rules are evaluated in order; the
// first non-AllowTool verdict wins.
type Rule func(call *core.ToolCall) Decision

// RequireApprovalFor asks for human approval before the named tools run.
func RequireApprovalFor(names ...string) Rule {
	set := toSet(names)
	return func(c *core.ToolCall) Decision {
		if set[c.Name] {
			return AskTool
		}
		return AllowTool
	}
}

// DenyFor refuses the named tools outright.
func DenyFor(names ...string) Rule {
	set := toSet(names)
	return func(c *core.ToolCall) Decision {
		if set[c.Name] {
			return DenyTool
		}
		return AllowTool
	}
}

// Permission gates tool calls with the given rules.
func Permission(rules ...Rule) agent.Middleware {
	return &permission{rules: rules}
}

type permission struct {
	agent.BaseMiddleware
	rules []Rule
}

func (p *permission) BeforeTool(_ *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	for _, r := range p.rules {
		switch r(c) {
		case AskTool:
			return core.Directive{Kind: core.Interrupt, Reason: "tool " + c.Name + " requires approval"}, nil
		case DenyTool:
			return core.Directive{Kind: core.Stop, Reason: "tool " + c.Name + " denied by policy"}, nil
		}
	}
	return core.Directive{}, nil
}

func toSet(names []string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}
