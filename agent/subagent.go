package agent

import (
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/tool"
)

// AsTool wraps a child agent as a tool that runs it in an ISOLATED context — the
// deep-agents "quarantine" model. This is the key difference from transfer:
//
//   - transfer hands over the whole conversation; the delegate continues the
//     same State and its turns appear inline.
//   - AsTool isolates: the child gets ONLY the task string as input (the
//     parent's history is not passed), and ONLY the child's final text returns
//     to the parent as the tool result. The child's intermediate reasoning and
//     tool calls never enter the parent's context.
//
// The virtual filesystem is shared (a collaboration surface): large artifacts
// can be written by the child and read by the parent without bloating context.
func AsTool(child *Agent, name, description string) tool.Tool {
	return &subAgentTool{child: child, name: name, desc: description}
}

type subAgentTool struct {
	child *Agent
	name  string
	desc  string
}

func (s *subAgentTool) Name() string        { return s.name }
func (s *subAgentTool) Description() string { return s.desc }
func (s *subAgentTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"task":{"type":"string","description":"the task for the sub-agent to perform"}},"required":["task"]}`)
}

func (s *subAgentTool) Call(tctx *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		Task string `json:"task"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.Task == "" {
		return tool.ErrorResult(s.name + " requires a 'task' string"), nil
	}

	// Run the child as a fresh, isolated run (child.Run starts a new ephemeral
	// thread → fresh State, messages = [task]), sharing the parent's Files so
	// the filesystem remains a shared collaboration surface.
	var opts []RunOption
	if tctx.State != nil && tctx.State.Files != nil {
		opts = append(opts, WithRunFiles(tctx.State.Files))
	}
	answer, err := s.child.Run(tctx, in.Task, opts...)
	if err != nil {
		return tool.ErrorResult(fmt.Sprintf("sub-agent %s failed: %v", s.name, err)), nil
	}
	// Only the final text crosses back — the quarantine boundary.
	return tool.TextResult(answer), nil
}
