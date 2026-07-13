// Command eval-reflect 演示「在线评估闭环」——闭环 A:让答案在运行时自动变好。
//
// 一个 worker 智能体先写出回答;一个 LLM 裁判(eval.Gate)给它打分:达标就用
// Escalate 跳出循环、把这版答案作为最终结果;不达标就把裁判的批评 Steer 回去,由外层
// agent.Loop 再跑一轮——worker 看到批评后改进。如此往复,直到达标或到轮数上限。
//
// 这正是 examples/refine 的精炼循环形态,只是「评审」换成了可复用的 eval.Scorer。
//
// 默认离线运行(mock 脚本化复现「初稿粗糙 → 被打回 → 改进达标」)。设 EVAL_LIVE=1 且
// 提供 AGNES_API_KEY 时,worker 与裁判都改用真实模型做真正的自我修正。
//
//	go run ./examples/eval-reflect
//	EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-reflect
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
)

const task = "用一句话向初学者介绍 Go 的并发模型。"

// 裁判标准:刻意不含关键词「goroutine」,以免污染离线 mock 裁判的判定。
const criterion = "这句介绍是否具体、点出了关键并发机制,而非泛泛而谈"

func main() {
	ctx := context.Background()
	live := os.Getenv("EVAL_LIVE") != "" && os.Getenv("AGNES_API_KEY") != ""
	if live {
		fmt.Println("(联网模式:worker 与裁判均为真实模型)")
	} else {
		fmt.Println("(离线模式:mock 脚本化复现自纠过程;设 EVAL_LIVE=1 切真实模型)")
	}

	judge := pickJudge(live)

	// 1) 基线:不加评估闭环,只跑一次 —— 容易停在「粗糙初稿」。
	section("基线 · 无评估闭环(单次运行)")
	base, _ := agent.New(
		agent.WithModel(pickWorker(live)),
		agent.WithInstruction("用一句话回答用户问题。"),
	)
	baseAns, _ := base.Run(ctx, task)
	fmt.Println("最终答案:", baseAns)

	// 2) 闭环:Gate 评分 + agent.Loop 精炼 —— 不达标自动带反馈重写。
	section("闭环 A · 在线自纠(Gate + agent.Loop,最多 3 轮)")
	worker, err := agent.New(
		agent.WithModel(pickWorker(live)),
		agent.WithInstruction("用一句话回答用户问题。若上文有【评审意见】,务必据此改进,补上具体的并发机制。"),
		agent.WithMiddleware(
			eval.Gate(eval.Rubric(judge, criterion, eval.WithThreshold(0.8)), 0.8),
		),
	)
	if err != nil {
		fmt.Println("构建失败:", err)
		return
	}
	refine := agent.Loop("refine", 3, worker)
	finalAns := runAndTrace(ctx, refine)
	fmt.Println("\n✅ 达标答案:", finalAns)
}

// runAndTrace 跑一次精炼循环,把每一轮的 worker 草稿打印出来,直观看到「越改越好」。
func runAndTrace(ctx context.Context, a *agent.Agent) string {
	run := a.Stream(ctx, task)
	round := 0
	var final string
	for ev, err := range run.Iter() {
		if err != nil {
			continue
		}
		if m, ok := ev.(core.MessageDone); ok {
			text := strings.TrimSpace(m.Message.Text())
			if text == "" || len(m.Message.ToolCalls()) > 0 {
				continue
			}
			round++
			fmt.Printf("第 %d 轮草稿:%s\n", round, text)
			final = text
		}
	}
	res, _ := run.Wait()
	if t := strings.TrimSpace(res.Message.Text()); t != "" {
		final = t
	}
	return final
}

// --- 模型 -------------------------------------------------------------------

func pickWorker(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线 worker:见过「评审意见」就给出具体答案,否则给粗糙初稿。
	return mock.New("worker", func(req *llm.Request) *llm.Response {
		for _, m := range req.Messages {
			if strings.Contains(m.Text(), "评审意见") {
				return mock.Text("Go 用 goroutine 做轻量并发、用 channel 在它们之间安全传递数据。")
			}
		}
		return mock.Text("Go 支持并发编程。")
	})
}

func pickJudge(live bool) llm.Model {
	if live {
		return liveModel()
	}
	// 离线裁判:答案点出 goroutine 才算具体(5 分),否则泛泛(2 分)。
	return mock.New("judge", func(req *llm.Request) *llm.Response {
		ans := req.Messages[len(req.Messages)-1].Text()
		if strings.Contains(ans, "goroutine") {
			return mock.Text(`{"score":5,"reason":"点出了 goroutine 与 channel,具体"}`)
		}
		return mock.Text(`{"score":2,"reason":"过于笼统,未提及任何并发机制"}`)
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

func section(title string) { fmt.Printf("\n===== %s =====\n", title) }
