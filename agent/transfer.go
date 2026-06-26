package agent

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/tool"
)

// transferToolName is the reserved name of the auto-injected delegation tool.
const transferToolName = "transfer_to_agent"

// maxTransferDepth bounds agent-to-agent delegation chaining within one run to
// prevent runaway ping-pong (e.g. parent<->child loops the model might induce).
const maxTransferDepth = 8

// transferTarget is one advertised delegation destination.
type transferTarget struct {
	name string
	hint string // direction + description, shown to the model
}

// transferTool is the synthetic tool an agent advertises when delegation is
// enabled and it has at least one allowed target (sub-agent, parent, or peer).
// Calling it sets Actions.TransferToAgent; the turn engine resolves the target
// (validating it is in the allowed set) and hands over control.
type transferTool struct {
	schema  json.RawMessage
	targets []transferTarget
}

func newTransferTool(targets []transferTarget) *transferTool {
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.name
	}
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"agent_name": map[string]any{
				"type":        "string",
				"enum":        names,
				"description": "要转交给的目标 agent 名称。",
			},
		},
		"required": []string{"agent_name"},
	}
	b, _ := json.Marshal(schema)
	return &transferTool{schema: b, targets: targets}
}

func (t *transferTool) Name() string { return transferToolName }

func (t *transferTool) Description() string {
	var b strings.Builder
	b.WriteString("当另一个 agent 更适合处理当前请求时，把对话转交给它。可选目标：")
	for i, tg := range t.targets {
		if i > 0 {
			b.WriteString("；")
		}
		b.WriteString(tg.name)
		if tg.hint != "" {
			b.WriteString(" ")
			b.WriteString(tg.hint)
		}
	}
	return b.String()
}

func (t *transferTool) Schema() json.RawMessage { return t.schema }

func (t *transferTool) Call(ctx *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		AgentName string `json:"agent_name"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return tool.ErrorResult("invalid transfer arguments: " + err.Error()), nil
	}
	if in.AgentName == "" {
		return tool.ErrorResult("agent_name is required"), nil
	}
	ctx.Actions.TransferToAgent = in.AgentName
	return tool.TextResult("转交给 " + in.AgentName), nil
}

var _ tool.Tool = (*transferTool)(nil)

// transferableTargets computes, per the direction rules, the agents this agent
// may delegate to, and returns them indexed by name. Rules:
//   - sub-agents: always (when delegation is enabled);
//   - parent: unless DisallowTransferToParent;
//   - peers (siblings): unless DisallowTransferToPeers, and only if the parent
//     is itself delegation-capable (otherwise a peer handoff would strand).
func (a *LLMAgent) transferableTargets(ictx InvocationContext) map[string]Agent {
	if a.cfg.DisableTransfer {
		return nil
	}
	out := map[string]Agent{}

	for _, sub := range a.cfg.SubAgents {
		out[sub.Name()] = sub
	}

	if ictx.Root != nil {
		if parent := parentOf(ictx.Root, a); parent != nil {
			if !a.cfg.DisallowTransferToParent {
				out[parent.Name()] = parent
			}
			if !a.cfg.DisallowTransferToPeers && transferCapable(parent) {
				for _, sib := range parent.SubAgents() {
					if sib.Name() != a.cfg.Name {
						out[sib.Name()] = sib
					}
				}
			}
		}
	}
	delete(out, a.cfg.Name) // never transfer to self
	return out
}

// orderedTargets renders the target map as advertised entries (sub, parent,
// peer) with a direction hint for the model.
func (a *LLMAgent) orderedTargets(ictx InvocationContext, targets map[string]Agent) []transferTarget {
	parent := Agent(nil)
	if ictx.Root != nil {
		parent = parentOf(ictx.Root, a)
	}
	subNames := map[string]bool{}
	for _, s := range a.cfg.SubAgents {
		subNames[s.Name()] = true
	}

	entries := make([]transferTarget, 0, len(targets))
	for name, ag := range targets {
		var dir string
		switch {
		case subNames[name]:
			dir = "(下级)"
		case parent != nil && parent.Name() == name:
			dir = "(上级)"
		default:
			dir = "(同级)"
		}
		entries = append(entries, transferTarget{name: name, hint: dir + ag.Description()})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries
}

// transferCapable reports whether an agent can itself delegate (used to gate
// peer transfer).
func transferCapable(a Agent) bool {
	la, ok := a.(*LLMAgent)
	return ok && !la.cfg.DisableTransfer
}
