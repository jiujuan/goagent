// Command eval-judge 演示两种 LLM-as-Judge 用法:
//
//   - Rubric  :按一条标准给单个答案打「绝对分」(1..5 归一到 0..1)。
//   - Pairwise:让裁判在两个答案间二选一,做「相对比较」(A/B、版本回归)。Pairwise 会
//     正反两序各问一次,抵消裁判的位置偏置——谁更好就该两序都赢。
//
// 默认离线(mock 裁判);设 EVAL_LIVE=1 且提供 AGNES_API_KEY 时换真实裁判。
//
//	go run ./examples/eval-judge
//	EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-judge
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

const question = "为什么 Go 适合写网络服务?"

const (
	weakAns   = "因为它快。"
	strongAns = "Go 有轻量的 goroutine 和 channel,天然适合高并发 I/O;标准库自带成熟的 net/http,部署是单一静态二进制。"
)

func main() {
	ctx := context.Background()
	judge := pickJudge(os.Getenv("EVAL_LIVE") != "" && os.Getenv("AGNES_API_KEY") != "")

	// 1) Rubric:分别给两个答案打绝对分。
	section("Rubric · 绝对打分")
	rubric := eval.Rubric(judge, "回答是否给出了有依据的具体理由", eval.WithThreshold(0.7))
	for _, a := range []string{strongAns, weakAns} {
		sc, _ := rubric.Score(ctx, eval.Sample{Input: question, Output: a})
		fmt.Printf("  [%.2f %s] %s\n  └─ %s\n", sc.Value, pass(sc.Passed), a, sc.Reason)
	}

	// 2) Pairwise:把两个答案放一起,让裁判选更好的(正反两序去偏置)。
	section("Pairwise · 相对比较(A=强答案, B=弱答案)")
	pair := eval.Pairwise(judge)
	sc, err := pair.Compare(ctx,
		eval.Sample{Input: question, Output: strongAns},
		eval.Sample{Input: question, Output: weakAns})
	if err != nil {
		fmt.Println("比较失败:", err)
		return
	}
	fmt.Printf("  A 胜率: %.2f  (A 更好: %v)\n  └─ %s\n", sc.Value, sc.Passed, sc.Reason)
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
	// 一个裁判同时应付两种 prompt:含「回答 A」的是 Pairwise,否则是 Rubric。
	// 判定都以是否点出「goroutine」这一具体机制为准。
	return mock.New("judge", func(req *llm.Request) *llm.Response {
		u := req.Messages[len(req.Messages)-1].Text()
		if ai, bi := strings.Index(u, "回答 A："), strings.Index(u, "回答 B："); ai >= 0 && bi >= 0 {
			a, b := u[ai:bi], u[bi:]
			winner := "tie"
			switch {
			case strings.Contains(a, "goroutine") && !strings.Contains(b, "goroutine"):
				winner = "A"
			case strings.Contains(b, "goroutine") && !strings.Contains(a, "goroutine"):
				winner = "B"
			}
			return mock.Text(`{"winner":"` + winner + `","reason":"点出具体机制者更优"}`)
		}
		if strings.Contains(u, "goroutine") {
			return mock.Text(`{"score":5,"reason":"给出了 goroutine/channel 等具体依据"}`)
		}
		return mock.Text(`{"score":2,"reason":"只有结论,没有依据"}`)
	})
}

func pass(b bool) string {
	if b {
		return "✓"
	}
	return "✗"
}

func section(t string) { fmt.Printf("\n===== %s =====\n", t) }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
