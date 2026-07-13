// Command refine answers "how do I run N rounds to get the best answer?".
//
// Two different knobs are easy to confuse:
//
//   - WithMaxTurns(n): a SAFETY CAP on one agent run's internal model<->tool
//     loop (e.g. a tool-using agent won't loop forever). NOT "best of N".
//   - agent.Loop(name, n, subs...): a REFINEMENT loop — run sub-agents up to n
//     rounds, stopping early when a critic is satisfied (it calls exit_loop).
//     THIS is "run 3 rounds to get the best answer".
//
// Here a drafter writes/improves a one-line intro; a critic either approves
// (exit_loop) or gives one improvement note; the loop repeats up to 3 rounds.
// The drafter stores its latest draft under "draft" (WithOutputKey), and a tiny
// finalizer emits it via the {{draft}} placeholder.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/refine
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
)

func main() {
	model, ok := buildModel()
	if !ok {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL / AGNES_BASE_URL)再运行。")
		return
	}
	ctx := context.Background()

	// drafter:有评审意见就改进,否则初稿;每轮把最新稿写进 state["draft"]。
	// WithMaxTurns(2):单次运行至多 2 个 model 轮次的安全上限(本例用不到工具,主要做示范)。
	drafter, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是写手。若上文有评审建议就据此改进简介,否则为主题写一句精彩简介。直接输出简介本身,不要解释。"),
		agent.WithOutputKey("draft"),
		agent.WithMaxTurns(2),
	)

	// critic:满意就 exit_loop 结束循环,否则给一句改进建议。
	critic, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是严格评审。若最新简介已足够精彩,就调用 exit_loop 工具结束;否则只用一句话给出一个改进建议。"),
		agent.WithTools(agent.ExitLoopTool()),
	)

	// finalizer:原样输出最佳稿(读 state["draft"])。
	finalizer, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("原样输出下面的最终稿,一个字都不要改:\n{{draft}}"),
	)

	// Loop 跑最多 3 轮(drafter→critic),critic 调用 exit_loop 则提前停;之后 finalizer 取最佳稿。
	pipe := agent.Sequential("refine-pipeline",
		agent.Loop("refine", 3, drafter, critic),
		finalizer,
	)

	round := 0
	for ev, err := range pipe.Stream(ctx, "主题:Go 的并发").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			if e.Result.Name == "exit_loop" {
				fmt.Println("   ✅ 评审通过,提前结束循环")
			}
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				round++
				fmt.Printf("· 产出 %d:%s\n", round, t)
			}
		}
	}
}

func buildModel() (llm.Model, bool) {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		return nil, false
	}
	base := envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	model := envOr("AGNES_MODEL", "gemini-2.5-flash")
	return openaicompat.Agnes(base, model, key), true
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
