// Command eval-trajectory 演示「评估 Agent 执行结果」:不仅看最终答案对不对,还看
// 智能体「怎么做到的」——用对工具没?步数合理吗?有没有绕圈?
//
// 流程:跑一个会用工具的智能体 → eval.Record 从事件流重建 Trajectory → 用组合评分器打分:
//   - TrajectoryJudge:裁判看整条轨迹(思考/调用/观察/回答),评工具使用是否得当。
//   - MaxSteps       :步数预算(确定性,零成本)。
//   - Reference      :最终答案对照 gold。
//
// 默认离线(mock 智能体 + mock 裁判);设 EVAL_LIVE=1 且提供 AGNES_API_KEY 时换真实模型。
//
//	go run ./examples/eval-trajectory
//	EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-trajectory
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/eval"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/tool"
)

const (
	question = "北京今天天气怎么样?"
	gold     = "北京今天晴,气温 25 摄氏度"
)

func main() {
	ctx := context.Background()
	live := os.Getenv("EVAL_LIVE") != "" && os.Getenv("AGNES_API_KEY") != ""
	if live {
		fmt.Println("(联网模式:真实模型 + 真实裁判)")
	} else {
		fmt.Println("(离线模式:mock 智能体 + mock 裁判;设 EVAL_LIVE=1 切真实模型)")
	}

	// 被测智能体:先查天气工具,再据观察作答。
	weather := tool.New("get_weather", "查询某城市当前天气",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"城市名"`
		}) (string, error) {
			return "晴,25℃", nil
		})
	sut, _ := agent.New(
		agent.WithModel(pickAgent(live)),
		agent.WithInstruction("先用 get_weather 查到天气,再用一句话回答用户。"),
		agent.WithTools(weather),
		agent.WithMaxTurns(6),
	)

	// 跑一次并录制轨迹。
	traj, res, err := eval.Record(sut.Stream(ctx, question))
	if err != nil {
		fmt.Println("运行失败:", err)
		return
	}
	traj.Input = question // Record 不从事件流取原始问题,这里补上供裁判参考

	fmt.Printf("\n最终答案: %s\n", res.Message.Text())
	fmt.Printf("轨迹概况: 步数=%d, 工具调用=%d, tokens=%d\n",
		traj.Steps, len(traj.Tools), traj.Usage.InputTokens+traj.Usage.OutputTokens)

	// 组合评分:轨迹质量 50% + 步数预算 20% + 终答正确 30%。
	judge := pickJudge(live)
	scorer := eval.Weighted(
		eval.Weight(eval.TrajectoryJudge(judge, "是否调用了合适的工具、步数合理、无冗余往返"), 0.5),
		eval.Weight(eval.MaxSteps(4), 0.2),
		eval.Weight(eval.Reference(judge), 0.3),
	)
	sc, err := scorer.Score(ctx, eval.Sample{
		Input:     question,
		Output:    res.Message.Text(),
		Reference: gold,
		Traj:      traj,
	})
	if err != nil {
		fmt.Println("打分失败:", err)
		return
	}

	fmt.Printf("\n综合得分: %.2f  通过: %v\n", sc.Value, sc.Passed)
	for _, sub := range sc.Sub {
		mark := "✗"
		if sub.Passed {
			mark = "✓"
		}
		fmt.Printf("  - %-16s %.2f %s  %s\n", sub.Name, sub.Value, mark, sub.Reason)
	}
}

// --- 模型 -------------------------------------------------------------------

func pickAgent(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线智能体:先调工具,见到工具结果后作答。
	return mock.New("sut", func(req *llm.Request) *llm.Response {
		for _, m := range req.Messages {
			if m.Role == core.RoleTool {
				return mock.Text("北京今天晴,气温 25 摄氏度。")
			}
		}
		return mock.CallTool("c1", "get_weather", `{"city":"北京"}`)
	})
}

func pickJudge(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线裁判:轨迹里用了 get_weather 即算合理(5 分);终答含「晴」且含「25」即对(5 分)。
	return mock.New("judge", func(req *llm.Request) *llm.Response {
		u := req.Messages[len(req.Messages)-1].Text()
		if strings.Contains(u, "执行轨迹") { // TrajectoryJudge 的 prompt
			if strings.Contains(u, "get_weather") {
				return mock.Text(`{"score":5,"reason":"正确调用天气工具,一步到位"}`)
			}
			return mock.Text(`{"score":2,"reason":"未使用合适的工具"}`)
		}
		// Reference 的 prompt
		if strings.Contains(u, "晴") && strings.Contains(u, "25") {
			return mock.Text(`{"score":5,"reason":"与参考答案一致"}`)
		}
		return mock.Text(`{"score":2,"reason":"与参考答案不符"}`)
	})
}

func liveModel() llm.Model {
	return openaicompat.Agnes(
		envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
		envOr("AGNES_MODEL", "gemini-2.5-flash"),
		os.Getenv("AGNES_API_KEY"))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
