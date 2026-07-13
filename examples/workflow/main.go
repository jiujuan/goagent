// Command workflow is a tutorial for goagent's deterministic multi-agent
// orchestration: Sequential, Parallel, Loop, and the Pipeline builder. Workflow
// agents are *Agent values too, so they share the same New/Run/Stream API as a
// plain LLM agent — you just compose smaller agents into bigger ones.
//
// Driven by a real Agnes chat model (OpenAI-compatible).
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash     # 你的 Agnes 聊天模型 id(可选)
//	go run ./examples/workflow
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
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

	section("1. Sequential —— 顺序流水线(上一阶段输出流向下一阶段)")
	demoSequential(ctx, model)

	section("2. Parallel —— 并发 fan-out(分支隔离 + 结果合并)")
	demoParallel(ctx, model)

	section("3. Loop —— 循环精修(到 exit_loop 跳出)")
	demoLoop(ctx, model)

	section("4. Pipeline —— builder 声明多阶段(含并发/循环复合阶段)")
	demoPipeline(ctx, model)
}

// --- 1. Sequential ----------------------------------------------------------

// 每个阶段都是一个用 agent.New 造的小 agent;agent.Sequential 把它们串成一个更大的
// agent。同一条 State 贯穿,所以后一阶段能看到前一阶段写进对话的内容。
func demoSequential(ctx context.Context, model llm.Model) {
	planner := stage(model, "你是选题策划。把用户主题拆成 3 个要点,列表输出,不要展开。")
	writer := stage(model, "你是作者。根据上文要点,写一段 100 字以内的简介。")
	polisher := stage(model, "你是润色编辑。把上文改得更简洁有力,直接输出最终文字。")

	flow := agent.Sequential("write-pipeline", planner, writer, polisher)
	out, err := flow.Run(ctx, "主题:向量数据库")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", out)
}

// --- 2. Parallel ------------------------------------------------------------

// 并发跑多个 agent,每个在自己的 State 分支上(互不干扰),最后把各自终答合并。
func demoParallel(ctx context.Context, model llm.Model) {
	pros := stage(model, "只列出『使用 Go 写后端』的 3 个优点,简短。")
	cons := stage(model, "只列出『使用 Go 写后端』的 3 个缺点,简短。")

	flow := agent.Parallel("pros-cons", pros, cons)
	out, err := flow.Run(ctx, "评估 Go 做后端")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖\n" + out)
}

// --- 3. Loop ----------------------------------------------------------------

// Loop 反复跑子 agent,直到某个 agent 调用 exit_loop(agent.ExitLoopTool 返回的
// 工具,其结果带 Escalate 控制信号,loopRunner 读到就停),或到达 maxIter。
func demoLoop(ctx context.Context, model llm.Model) {
	critic := stage(model,
		"你是严格的评审。若上文草稿已足够好,就调用 exit_loop 工具结束;否则用一句话给出改进建议。",
		agent.WithTools(agent.ExitLoopTool()),
	)
	reviser := stage(model, "你是修改者。根据最近的评审建议改写草稿,直接输出新草稿。")

	flow := agent.Loop("refine", 4, critic, reviser)
	out, err := flow.Run(ctx, "草稿:Go 是一种语言。请精修到一句话的精彩简介。")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", out)
}

// --- 4. Pipeline ------------------------------------------------------------

// Pipeline 是 Sequential 的 fluent 糖:每个 stage 可以是普通 agent,也可以是
// Parallel/Loop 复合阶段。Build() 折叠成一个可运行的 Sequential agent。
func demoPipeline(ctx context.Context, model llm.Model) {
	planner := stage(model, "把主题拆成 2 个调研角度,列表输出。")
	angleA := stage(model, "就第一个角度给一句要点。")
	angleB := stage(model, "就第二个角度给一句要点。")
	writer := stage(model, "综合上文,写 80 字以内的总结。")
	critic := stage(model, "若总结够好就调用 exit_loop;否则给一句改进建议。", agent.WithTools(agent.ExitLoopTool()))
	reviser := stage(model, "按评审意见改写总结,直接输出。")

	pipe := agent.NewPipeline("research-report").
		Then(planner).
		ThenParallel("gather", angleA, angleB).
		Then(writer).
		ThenLoop("review", 3, critic, reviser).
		Build()

	out, err := pipe.Run(ctx, "主题:Go 的并发模型")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", out)
}

// --- helpers ----------------------------------------------------------------

// stage builds a small single-purpose agent for use as a workflow stage.
func stage(model llm.Model, instruction string, opts ...agent.Option) *agent.Agent {
	a, err := agent.New(append([]agent.Option{
		agent.WithModel(model),
		agent.WithInstruction(instruction),
	}, opts...)...)
	if err != nil {
		log.Fatal(err)
	}
	return a
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
