// Command eval-quickstart 是评估系统最小上手示例:给「一个答案」打分。
//
// 核心抽象只有一个 —— Scorer 把一个 Sample 映射成 [0,1] 的 Score。这里用 Weighted 把一个
// 零成本的规则评分器(Contains)和一个 LLM 裁判(Rubric)组合起来:既要命中关键词,又要
// 裁判认可专业度。Score.Sub 给出每个分项的明细。
//
// 默认离线(mock 裁判);设 EVAL_LIVE=1 且提供 AGNES_API_KEY 时换真实裁判。
//
//	go run ./examples/eval-quickstart
//	EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-quickstart
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jiujuan/goagent/eval"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/llm/openaicompat"
)

func main() {
	ctx := context.Background()
	judge := pickJudge(os.Getenv("EVAL_LIVE") != "" && os.Getenv("AGNES_API_KEY") != "")

	// 组合评分器:30% 看是否提到「退款」,70% 看裁判给的专业度。
	scorer := eval.Weighted(
		eval.Weight(eval.Contains("退款"), 0.3),
		eval.Weight(eval.Rubric(judge, "回答是否专业、给出可执行步骤", eval.WithThreshold(0.7)), 0.7),
	)

	samples := []eval.Sample{
		{Input: "我想退货怎么办?", Output: "您可在订单页点击「申请退款」,7 天内原路退回到付款账户。"},
		{Input: "我想退货怎么办?", Output: "不知道,你问问别人吧。"},
	}

	for i, s := range samples {
		sc, err := scorer.Score(ctx, s)
		if err != nil {
			fmt.Println("打分失败:", err)
			continue
		}
		fmt.Printf("\n样本 %d\n  问题: %s\n  答案: %s\n", i+1, s.Input, s.Output)
		fmt.Printf("  总分: %.2f  通过: %v\n", sc.Value, sc.Passed)
		for _, sub := range sc.Sub {
			mark := "✗"
			if sub.Passed {
				mark = "✓"
			}
			fmt.Printf("    - %-8s %.2f %s  %s\n", sub.Name, sub.Value, mark, sub.Reason)
		}
	}
}

func pickJudge(live bool) llm.Model {
	if live {
		fmt.Println("(联网模式:真实裁判)")
		return openaicompat.Agnes(
			envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
			envOr("AGNES_MODEL", "gemini-2.5-flash"),
			os.Getenv("AGNES_API_KEY"))
	}
	fmt.Println("(离线模式:mock 裁判;设 EVAL_LIVE=1 切真实裁判)")
	// 离线裁判:答案给出「申请」步骤即视为专业(5 分),否则 2 分。
	return mock.New("judge", func(req *llm.Request) *llm.Response {
		ans := req.Messages[len(req.Messages)-1].Text()
		if strings.Contains(ans, "申请") {
			return mock.Text(`{"score":5,"reason":"给出了可执行的申请步骤"}`)
		}
		return mock.Text(`{"score":2,"reason":"未给出任何可执行步骤"}`)
	})
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
