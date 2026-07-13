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

// weatherAgent builds an agent whose mock model calls a tool once, then answers.
func weatherAgent(t *testing.T) *agent.Agent {
	t.Helper()
	weather := tool.New("get_weather", "look up weather",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return "晴 25C", nil
		})
	model := mock.New("rec", func(req *llm.Request) *llm.Response {
		for _, m := range req.Messages {
			if m.Role == core.RoleTool {
				return mock.Text("北京天气晴，25 摄氏度。")
			}
		}
		return mock.CallTool("c1", "get_weather", `{"city":"北京"}`)
	})
	a, err := agent.New(agent.WithModel(model), agent.WithTools(weather))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRecordTrajectory(t *testing.T) {
	a := weatherAgent(t)
	traj, res, err := Record(a.Stream(context.Background(), "北京天气?"))
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if traj.Steps != 2 {
		t.Fatalf("Steps = %d, want 2 (tool turn + answer turn)", traj.Steps)
	}
	if len(traj.Tools) != 1 {
		t.Fatalf("Tools = %d, want 1", len(traj.Tools))
	}
	ep := traj.Tools[0]
	if ep.Call.Name != "get_weather" || ep.Result.Name != "get_weather" || ep.Result.IsError {
		t.Fatalf("unexpected tool episode: %+v", ep)
	}
	if traj.Usage.OutputTokens == 0 {
		t.Fatal("expected non-zero output tokens accumulated from MessageDone")
	}
	if !strings.Contains(res.Message.Text(), "晴") {
		t.Fatalf("final answer missing content: %q", res.Message.Text())
	}

	// The recorded trajectory feeds trajectory/tool scorers directly.
	if mustScore(t, MaxSteps(3), Sample{Traj: traj}).Passed != true {
		t.Fatal("MaxSteps(3) should pass for a 2-step run")
	}
	if mustScore(t, NoToolError{}, Sample{Tool: &traj.Tools[0]}).Passed != true {
		t.Fatal("NoToolError should pass for a successful tool")
	}
}

func TestHarnessReport(t *testing.T) {
	model := mock.New("h", func(req *llm.Request) *llm.Response {
		q := req.Messages[len(req.Messages)-1].Text()
		if strings.Contains(q, "退货") {
			return mock.Text("请在订单页申请退款")
		}
		return mock.Text("未发货前可在订单详情修改地址")
	})
	a, err := agent.New(agent.WithModel(model))
	if err != nil {
		t.Fatal(err)
	}

	ds := Dataset{
		{Name: "退货", Input: "我想退货", Reference: "申请退款"},
		{Name: "改址", Input: "怎么改地址", Reference: "修改地址"},
	}
	h := &Harness{Agent: a, Scorers: []Scorer{
		Named("退款", Contains("退款")),
		Named("地址", Contains("地址")),
	}}

	rep, err := h.Run(context.Background(), ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Cases) != 2 {
		t.Fatalf("cases = %d, want 2", len(rep.Cases))
	}
	// case1: 退款✓ 地址✗ ; case2: 退款✗ 地址✓ → 2/4 passed.
	if rep.PassRate < 0.49 || rep.PassRate > 0.51 {
		t.Fatalf("PassRate = %g, want 0.5", rep.PassRate)
	}
	if _, ok := rep.Mean["退款"]; !ok {
		t.Fatalf("Mean missing '退款': %v", rep.Mean)
	}

	// JSON artifact must be valid JSON; Print must not panic.
	if !json.Valid(rep.JSON()) {
		t.Fatal("Report.JSON() is not valid JSON")
	}
	rep.Print()
}

func TestHarnessMisuse(t *testing.T) {
	if _, err := (&Harness{}).Run(context.Background(), nil); err == nil {
		t.Fatal("expected error with no Agent")
	}
	a, _ := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("x") })))
	if _, err := (&Harness{Agent: a}).Run(context.Background(), nil); err == nil {
		t.Fatal("expected error with no Scorers")
	}
}
