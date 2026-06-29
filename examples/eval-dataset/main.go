// Command eval-dataset 演示「离线评估闭环」——闭环 B:在数据集上批量评分 + CI 门禁。
//
// 把一组测试用例(Dataset)逐个跑过被测智能体,用一组 Scorer 打分,聚合成 Report:
// 每例一行、每个评分器一列,末尾给出均值与整体通过率。通过率低于阈值即判定质量门禁
// 不通过(在 CI 里 os.Exit(1) 拦截合并)。失败用例回流进 Dataset,评估集越跑越强。
//
// 默认离线运行(mock FAQ 智能体 + mock 裁判,确定性输出)。设 EVAL_LIVE=1 且提供
// AGNES_API_KEY 时,被测智能体与裁判改用真实模型。
//
//	go run ./examples/eval-dataset
//	EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-dataset
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/eval"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/llm/openaicompat"
)

// 质量门禁:通过率达不到它就让 CI 失败。
const passGate = 0.8

func main() {
	ctx := context.Background()
	live := os.Getenv("EVAL_LIVE") != "" && os.Getenv("AGNES_API_KEY") != ""
	if live {
		fmt.Println("(联网模式:被测智能体与裁判均为真实模型)")
	} else {
		fmt.Println("(离线模式:mock FAQ 智能体 + mock 裁判;设 EVAL_LIVE=1 切真实模型)")
	}

	// 被测系统:一个电商客服 FAQ 智能体。
	sut, _ := agent.New(
		agent.WithModel(pickAgent(live)),
		agent.WithInstruction("你是电商客服,用一句话具体、可执行地回答用户问题,不要说套话。"),
	)

	// 评估集:每个用例带一个参考答案(gold)。第 2 例故意被智能体答得很水,用来制造回归。
	ds := eval.Dataset{
		{Name: "退货", Input: "我想退货怎么办?", Reference: "在订单页申请退款,7 天内原路退回"},
		{Name: "改地址", Input: "怎么修改收货地址?", Reference: "未发货前在订单详情页修改收货地址"},
		{Name: "发票", Input: "怎么开发票?", Reference: "订单完成后在订单页申请电子发票"},
	}

	judge := pickJudge(live)
	h := &eval.Harness{
		Agent: sut,
		Scorers: []eval.Scorer{
			eval.Rubric(judge, "回答是否具体、可执行(避免「请联系客服」之类套话)", eval.WithThreshold(0.7)),
			eval.Named("无套话", eval.Not(eval.Contains("联系客服"))),
		},
	}

	rep, err := h.Run(ctx, ds)
	if err != nil {
		fmt.Println("评估失败:", err)
		os.Exit(2)
	}

	fmt.Println()
	rep.Print()

	// CI 门禁:通过率不达标则失败退出。
	fmt.Println()
	if rep.PassRate < passGate {
		fmt.Printf("❌ 质量门禁未通过:通过率 %.0f%% < 阈值 %.0f%%(CI 将拦截合并)\n", rep.PassRate*100, passGate*100)
		os.Exit(1)
	}
	fmt.Printf("✅ 质量门禁通过:通过率 %.0f%% ≥ 阈值 %.0f%%\n", rep.PassRate*100, passGate*100)
}

// --- 模型 -------------------------------------------------------------------

func pickAgent(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线 FAQ:退货/发票答得具体,改地址故意答套话(制造一个回归)。
	return mock.New("faq", func(req *llm.Request) *llm.Response {
		q := req.Messages[len(req.Messages)-1].Text()
		switch {
		case strings.Contains(q, "退货"):
			return mock.Text("在订单页点击申请退款,7 天内原路退回到您的付款账户。")
		case strings.Contains(q, "发票"):
			return mock.Text("订单完成后,在订单详情页点击「申请发票」即可开具电子发票。")
		default: // 改地址:套话
			return mock.Text("这个问题请您联系客服处理,谢谢。")
		}
	})
}

func pickJudge(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线裁判:含套话「联系客服」判 2 分,否则 5 分。
	return mock.New("judge", func(req *llm.Request) *llm.Response {
		text := req.Messages[len(req.Messages)-1].Text()
		// 取「被评回答」段落判断(避免标准/问题里的字干扰)。
		ans := text
		if i := strings.Index(text, "被评回答"); i >= 0 {
			ans = text[i:]
		}
		if strings.Contains(ans, "联系客服") {
			return mock.Text(`{"score":2,"reason":"回答是套话,未给出可执行步骤"}`)
		}
		return mock.Text(`{"score":5,"reason":"具体、可执行"}`)
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
