package eval

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// TestGateRefinesUntilPass is the online closed loop end to end: a worker gives a
// poor answer first, the Gate judges it below threshold and steers a critique,
// and on the next round the worker (seeing the critique) produces a good answer
// the Gate accepts via Escalate — breaking the agent.Loop.
func TestGateRefinesUntilPass(t *testing.T) {
	judge := scriptedJudge("完整") // scores 5 when the answer contains 完整, else 2

	// Worker: returns a good answer once it sees the steered 评审意见, else a draft.
	workerModel := mock.New("worker", func(req *llm.Request) *llm.Response {
		for _, m := range req.Messages {
			if strings.Contains(m.Text(), "评审意见") {
				return mock.Text("这是完整且准确的最终答案")
			}
		}
		return mock.Text("草稿")
	})

	worker, err := agent.New(
		agent.WithModel(workerModel),
		agent.WithInstruction("写一个答案；若上文有评审意见就据此改进。"),
		agent.WithMiddleware(Gate(Rubric(judge, "答案是否高质量", WithThreshold(0.7)), 0.7)),
	)
	if err != nil {
		t.Fatal(err)
	}

	ans, err := agent.Loop("refine", 3, worker).Run(context.Background(), "写点东西")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(ans, "完整") {
		t.Fatalf("expected the refined (good) answer, got %q", ans)
	}
}

// TestGateStopsAtMaxRounds: a worker that never improves should not loop forever;
// the agent.Loop bound caps the rounds and returns the last (rejected) answer.
func TestGateStopsAtMaxRounds(t *testing.T) {
	judge := scriptedJudge("完整")
	stubborn := mock.New("stubborn", func(*llm.Request) *llm.Response {
		return mock.Text("永远的草稿")
	})
	worker, _ := agent.New(
		agent.WithModel(stubborn),
		agent.WithMiddleware(Gate(Rubric(judge, "答案是否高质量"), 0.7)),
	)
	ans, err := agent.Loop("refine", 2, worker).Run(context.Background(), "写点东西")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(ans, "草稿") {
		t.Fatalf("expected last rejected answer, got %q", ans)
	}
}

// TestToolGuardMarksBadResult verifies ToolGuard flips a non-conforming tool
// result to IsError (and that the rewrite reaches both the event stream and the
// model history, thanks to the exectools ordering fix).
func TestToolGuardMarksBadResult(t *testing.T) {
	badTool := tool.New("lookup", "look something up",
		func(_ *tool.Context, _ struct{}) (string, error) {
			return `{"bad":1}`, nil // missing required "city"
		})
	schema := json.RawMessage(`{"type":"object","required":["city"],"properties":{"city":{"type":"string"}}}`)

	model := mock.New("guarded", func(req *llm.Request) *llm.Response {
		for _, m := range req.Messages {
			if m.Role == core.RoleTool && toolErrored(m) {
				return mock.Text("工具返回错误，已知悉")
			}
		}
		return mock.CallTool("c1", "lookup", `{}`)
	})

	worker, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(badTool),
		agent.WithMiddleware(ToolGuard(JSONSchema(schema))),
	)
	if err != nil {
		t.Fatal(err)
	}

	traj, _, err := Record(worker.Stream(context.Background(), "查一下"))
	if err != nil {
		t.Fatal(err)
	}
	if len(traj.Tools) != 1 {
		t.Fatalf("tools = %d, want 1", len(traj.Tools))
	}
	if !traj.Tools[0].Result.IsError {
		t.Fatal("ToolGuard should have flipped the result to IsError")
	}
	if !strings.Contains(partsText(traj.Tools[0].Result.Content), "[评估未通过]") {
		t.Fatalf("expected critique appended, got %q", partsText(traj.Tools[0].Result.Content))
	}
}

// toolErrored reports whether a tool-role message carries an errored result.
func toolErrored(m core.Message) bool {
	for _, p := range m.Parts {
		if tr, ok := p.(core.ToolResult); ok && tr.IsError {
			return true
		}
	}
	return false
}
