package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

func TestTransferToSubAgent(t *testing.T) {
	// The specialist produces the final answer.
	specialist := agent.New(agent.Config{
		Name:        "specialist",
		Description: "处理天气问题的专家",
		Model: mock.New("spec", func(*llm.Request) *llm.Response {
			return mock.Text("specialist handled the weather request")
		}),
	})

	// The router sees the transfer tool (auto-injected because it has a
	// sub-agent) and delegates on its first turn.
	router := agent.New(agent.Config{
		Name:        "router",
		Description: "把请求路由到合适的专家",
		SubAgents:   []agent.Agent{specialist},
		Model: mock.New("router", func(req *llm.Request) *llm.Response {
			// Sanity: the transfer tool must be advertised.
			var hasTransfer bool
			for _, ts := range req.Tools {
				if ts.Name == "transfer_to_agent" {
					hasTransfer = true
				}
			}
			if !hasTransfer {
				return mock.Text("ERROR: no transfer tool advertised")
			}
			return mock.CallTool("t1", "transfer_to_agent", `{"agent_name":"specialist"}`)
		}),
	})

	r := runner.New(runner.Config{Root: router})

	var authors []string
	var finalText string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("北京天气？")) {
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if ev.Message != nil {
			authors = append(authors, ev.Author)
			if ev.IsFinalResponse() {
				finalText = ev.Message.Text()
			}
		}
	}

	if finalText != "specialist handled the weather request" {
		t.Fatalf("final text = %q", finalText)
	}
	// The specialist must have authored at least one event.
	if !strings.Contains(strings.Join(authors, ","), "specialist") {
		t.Fatalf("specialist never ran; authors = %v", authors)
	}
}

func TestNoTransferToolWithoutSubAgents(t *testing.T) {
	var advertised bool
	a := agent.New(agent.Config{
		Name: "solo",
		Model: mock.New("solo", func(req *llm.Request) *llm.Response {
			for _, ts := range req.Tools {
				if ts.Name == "transfer_to_agent" {
					advertised = true
				}
			}
			return mock.Text("ok")
		}),
	})
	r := runner.New(runner.Config{Root: a})
	for _, err := range r.Run(context.Background(), "u", "s", core.UserText("hi")) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if advertised {
		t.Fatal("transfer tool should not be advertised without sub-agents")
	}
}
