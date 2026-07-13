// Command planning compares two ways to handle a multi-step task, so you can
// see what write_todos is and is NOT.
//
//  1. write_todos (SOFT planning): the model records/updates a todo list and
//     decides each next step itself. The plan is advisory; nothing is executed,
//     parallelized, or dependency-managed by the framework — the LLM drives.
//
//  2. workflow composition (HARD execution): you (or a planner) structure the
//     work as Sequential/Parallel/Loop agents. The RUNTIME executes them
//     deterministically — real parallelism, checkpoint-resume, Permission
//     approval. The structure is fixed up front.
//
// Rule of thumb: write_todos keeps an open-ended agent focused; workflow
// composition gives guarantees (order, parallelism, resume). They compose — a
// planner can emit a plan, then a workflow executes it.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/planning
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

	section("方案一:write_todos —— 软计划(LLM 自己列计划、自己推进,框架不执行)")
	demoSoft(ctx, model)

	section("方案二:workflow 组合 —— 硬执行(运行时确定性地跑,真并行 + 可恢复)")
	demoHard(ctx, model)
}

// --- 方案一:write_todos 软计划 ---------------------------------------------

func demoSoft(ctx context.Context, model llm.Model) {
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("处理多步任务时,先用 write_todos 列出计划,每完成一步就用 write_todos 更新状态,最后给出结论。"),
		agent.WithTools(agent.WriteTodosTool()),
	)
	if err != nil {
		log.Fatal(err)
	}
	// 流式打印 write_todos 的每次更新 —— 能看到模型自己在维护计划。
	for ev, err := range a.Stream(ctx, "为『给团队介绍 Go 泛型』准备一个 3 步小计划并执行,给出最终讲稿要点。").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			if e.Result.Name == "write_todos" {
				fmt.Println("📝 计划更新:")
				fmt.Println(indent(e.Result.Content[0].(core.Text).Text))
			}
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}

// --- 方案二:workflow 组合硬执行 --------------------------------------------

func demoHard(ctx context.Context, model llm.Model) {
	stage := func(instruction string) *agent.Agent {
		a, _ := agent.New(agent.WithModel(model), agent.WithInstruction(instruction))
		return a
	}
	// 把同一任务结构成确定性流水线:规划 → 并发起草两部分 → 汇总。
	plan := stage("把『给团队介绍 Go 泛型』拆成 2 个并行子任务,每个一句话。")
	partA := stage("就『泛型语法』给 2 条要点。")
	partB := stage("就『泛型使用场景』给 2 条要点。")
	merge := stage("把上文要点合并成一份讲稿大纲。")

	pipe := agent.NewPipeline("talk").
		Then(plan).
		ThenParallel("draft", partA, partB). // ← 运行时真并发执行
		Then(merge).
		Build()

	out, err := pipe.Run(ctx, "主题:Go 泛型")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", out)
	fmt.Println("   (这一版的顺序/并发/可恢复由运行时保证;若中途崩溃,可用同 thread 的 Resume 续跑)")
}

// --- helpers ----------------------------------------------------------------

func indent(s string) string {
	out := ""
	for _, line := range splitLines(s) {
		out += "   " + line + "\n"
	}
	return out
}

func splitLines(s string) []string {
	var lines []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			lines = append(lines, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
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

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }
