package agent_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

// alwaysTransfer returns a model that always delegates to target.
func alwaysTransfer(target string) *mock.Model {
	return mock.New("t", func(*llm.Request) *llm.Response {
		return mock.CallTool("x", "transfer_to_agent", `{"agent_name":"`+target+`"}`)
	})
}

func runCollect(t *testing.T, root agent.Agent, text string) (authors []string, final string) {
	t.Helper()
	r := runner.New(runner.Config{Root: root})
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText(text)) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.Message != nil {
			authors = append(authors, ev.Author)
			if ev.IsFinalResponse() {
				final = ev.Message.Text()
			}
		}
	}
	return authors, final
}

// TestTransferToPeer: coordinator -> billing -> (peer) tech -> answer.
func TestTransferToPeer(t *testing.T) {
	tech := agent.New(agent.Config{
		Name: "tech", Description: "技术支持",
		Model: mock.New("tech", func(*llm.Request) *llm.Response { return mock.Text("tech-answer") }),
	})
	billing := agent.New(agent.Config{
		Name: "billing", Description: "账单", Model: alwaysTransfer("tech"),
	})
	coordinator := agent.New(agent.Config{
		Name: "coordinator", Description: "路由", SubAgents: []agent.Agent{billing, tech},
		Model: alwaysTransfer("billing"),
	})

	authors, final := runCollect(t, coordinator, "我的网络坏了")
	if final != "tech-answer" {
		t.Fatalf("final = %q", final)
	}
	if !slices.Contains(authors, "billing") || !slices.Contains(authors, "tech") {
		t.Fatalf("expected billing and tech to run; authors = %v", authors)
	}
}

// TestTransferToParent: parent delegates to child, child returns to parent,
// parent then answers.
func TestTransferToParent(t *testing.T) {
	child := agent.New(agent.Config{
		Name: "child", Description: "子代理", Model: alwaysTransfer("root"),
	})
	root := agent.New(agent.Config{
		Name: "root", Description: "父代理", SubAgents: []agent.Agent{child},
		Model: mock.New("root", func(req *llm.Request) *llm.Response {
			// After control has bubbled back (tool results exist in history), answer.
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("root-final")
			}
			return mock.CallTool("x", "transfer_to_agent", `{"agent_name":"child"}`)
		}),
	})

	authors, final := runCollect(t, root, "你好")
	if final != "root-final" {
		t.Fatalf("final = %q", final)
	}
	if !slices.Contains(authors, "child") {
		t.Fatalf("child should have run before root answered; authors = %v", authors)
	}
}

// TestDisallowPeerTransfer: with DisallowTransferToPeers, the peer is not in the
// advertised target enum (only the parent is).
func TestDisallowPeerTransfer(t *testing.T) {
	var enum []string
	tech := agent.New(agent.Config{
		Name: "tech", Description: "技术",
		Model: mock.New("tech", func(*llm.Request) *llm.Response { return mock.Text("x") }),
	})
	billing := agent.New(agent.Config{
		Name: "billing", Description: "账单", DisallowTransferToPeers: true,
		Model: mock.New("billing", func(req *llm.Request) *llm.Response {
			enum = transferEnum(req)
			return mock.Text("billing-done")
		}),
	})
	coordinator := agent.New(agent.Config{
		Name: "coordinator", Description: "路由", SubAgents: []agent.Agent{billing, tech},
		Model: alwaysTransfer("billing"),
	})

	runCollect(t, coordinator, "账单问题")
	if slices.Contains(enum, "tech") {
		t.Fatalf("peer 'tech' must not be advertised when peers disallowed; enum = %v", enum)
	}
	if !slices.Contains(enum, "coordinator") {
		t.Fatalf("parent 'coordinator' should still be advertised; enum = %v", enum)
	}
}

// TestTransferDepthGuard: two agents that always delegate to each other must
// terminate (bounded by maxTransferDepth) rather than loop forever.
func TestTransferDepthGuard(t *testing.T) {
	b := agent.New(agent.Config{Name: "b", Model: alwaysTransfer("a")})
	a := agent.New(agent.Config{Name: "a", SubAgents: []agent.Agent{b}, Model: alwaysTransfer("b")})

	authors, _ := runCollect(t, a, "ping")
	// Each hop emits a couple of events; the guard caps the chain, so the run
	// must finish with a bounded number of events.
	if len(authors) > 60 {
		t.Fatalf("delegation did not terminate cleanly: %d events", len(authors))
	}
	if len(authors) == 0 {
		t.Fatal("expected some events")
	}
}

// transferEnum extracts the agent_name enum from the advertised transfer tool.
func transferEnum(req *llm.Request) []string {
	for _, ts := range req.Tools {
		if ts.Name != "transfer_to_agent" {
			continue
		}
		var schema struct {
			Properties struct {
				AgentName struct {
					Enum []string `json:"enum"`
				} `json:"agent_name"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(ts.Parameters, &schema); err == nil {
			return schema.Properties.AgentName.Enum
		}
	}
	return nil
}
