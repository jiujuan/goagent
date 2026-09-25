package workspace

import (
	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/skills"
)

// Gate returns the middleware that enforces the loaded skills' allowed-tools
// declarations: a tool call no active skill claimed pauses for human approval
// instead of running silently. See skills.Gate for the four-way decision.
//
// It returns nil when Config.SkillGate was off or no skill loaded, and a caller
// mounting it must skip nil rather than passing it to agent.WithMiddleware —
// that is why this hands back a plain nil interface instead of a disabled
// middleware.
//
// Gate is orthogonal to the workspace's own containment: os.Root confines the
// file tools' paths, the sandbox confines which programs start, and this
// confines which tool calls the model may make without asking.
func (w *Workspace) Gate() agent.Middleware {
	if !w.gateRequested || w.skills == nil {
		return nil
	}
	return skills.Gate(w.skills)
}
