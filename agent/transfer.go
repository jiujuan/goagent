package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// This file implements LLM-driven delegation. When an agent is configured with
// sub-agents (WithSubAgents), a synthetic transfer_to_agent tool is advertised;
// when the model calls it, the tool returns a Transfer control directive, and
// the llmRunner wrapping the loop hands control to the named agent (sharing the
// conversation State so the delegate continues the same thread).

// maxTransferDepth bounds delegation chains so agents cannot ping-pong forever.
const maxTransferDepth = 8

// transferToolName is the synthetic tool's name.
const transferToolName = "transfer_to_agent"

// llmRunner wraps an AgentLoop and performs delegation. It keeps a back-ref to
// the Agent so it can resolve transfer targets lazily at run time (targets like
// parent/peers are wired after New). The loop itself never delegates; it just
// surfaces a Transfer outcome, which this runner acts on.
type llmRunner struct {
	agent *Agent
	loop  *AgentLoop
}

func (r *llmRunner) run(rc *RunContext) runOutcome {
	out := r.loop.run(rc)
	if out.Control.Kind != core.Transfer {
		return out
	}
	target := r.agent.resolveTransfer(out.Control.Target)
	if target == nil || rc.transferDepth >= maxTransferDepth {
		// Unknown/disallowed target or depth exceeded: end with what we have.
		return runOutcome{Result: out.Result}
	}
	rc.publish(core.MessageDone{Message: core.AssistantText(
		fmt.Sprintf("→ 转交给 %s", target.cfg.name))})
	// Delegate on the same State (the delegate continues the conversation),
	// one level deeper.
	return target.runnable.run(rc.deeper())
}

var _ Runnable = (*llmRunner)(nil)

// transferTargets returns the agents this agent may delegate to, keyed by name:
// its sub-agents (down), its parent (up), and its peers (siblings), minus any
// disabled by options.
func (a *Agent) transferTargets() map[string]*Agent {
	if a.cfg.noTransfer {
		return nil
	}
	out := map[string]*Agent{}
	for _, s := range a.cfg.subAgents {
		out[s.cfg.name] = s
	}
	if a.parent != nil && !a.cfg.noTransferParent {
		out[a.parent.cfg.name] = a.parent
		if !a.cfg.noTransferPeers {
			for _, peer := range a.parent.cfg.subAgents {
				if peer != a {
					out[peer.cfg.name] = peer
				}
			}
		}
	}
	return out
}

// resolveTransfer looks up a transfer target by name.
func (a *Agent) resolveTransfer(name string) *Agent {
	return a.transferTargets()[name]
}

// transferToolFor builds the synthetic transfer tool advertising the given
// targets, or nil if there are none.
func transferToolFor(targets map[string]*Agent) tool.Tool {
	if len(targets) == 0 {
		return nil
	}
	names := make([]string, 0, len(targets))
	var b strings.Builder
	b.WriteString("Delegate the conversation to another agent. Available agents:\n")
	for name, ag := range targets {
		names = append(names, name)
		fmt.Fprintf(&b, "- %s: %s\n", name, ag.cfg.description)
	}
	enum, _ := json.Marshal(names)
	schema := json.RawMessage(fmt.Sprintf(
		`{"type":"object","properties":{"agent":{"type":"string","enum":%s,"description":"target agent name"}},"required":["agent"]}`,
		enum))
	return &transferTool{desc: b.String(), schema: schema}
}

type transferTool struct {
	desc   string
	schema json.RawMessage
}

func (transferTool) Name() string              { return transferToolName }
func (t transferTool) Description() string     { return t.desc }
func (t transferTool) Schema() json.RawMessage { return t.schema }

func (transferTool) Call(_ *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		Agent string `json:"agent"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.Agent == "" {
		return tool.ErrorResult("transfer_to_agent requires an 'agent' name"), nil
	}
	return &tool.Result{
		Content: []core.Part{core.Text{Text: "transferring to " + in.Agent}},
		Control: &core.Directive{Kind: core.Transfer, Target: in.Agent},
	}, nil
}
