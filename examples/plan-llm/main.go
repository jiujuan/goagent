// Command plan-llm is a tutorial for LLM-generated planning (NewLLMPlan): give
// the agent a single high-level task and a planner; the planner DECOMPOSES it
// into a DAG (nodes + dependencies) automatically, and the executor runs it —
// with real parallelism, resume, etc. Add WithReplanner for a fully autonomous
// loop: plan → execute → replan → execute.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/plan-llm
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
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

	// planner:把任务拆成 DAG(系统提示由框架补充 JSON schema,这里给点风格约束)。
	planner, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你擅长把复杂任务拆成最少的、能并行的步骤。"),
	)
	// worker:执行每个被拆出的节点。
	worker, _ := agent.New(agent.WithModel(model))

	pa := agent.NewLLMPlan("auto", planner,
		agent.WithWorker(worker),
		agent.WithConcurrency(4),
		agent.WithReplanner(planner), // 执行后允许再补步骤(可选)
		agent.WithMaxReplanRounds(1),
	)

	fmt.Println("任务:为「Go 适不适合写 AI Agent 框架」写一段有依据的结论")
	fmt.Println()
	for ev, err := range pa.Stream(ctx, "为「Go 适不适合写 AI Agent 框架」写一段有依据的结论").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.PlanNodeStarted:
			fmt.Printf("▶️  %s\n", label(e.NodeID))
		case core.PlanNodeDone:
			fmt.Printf("✅ %s %s\n", label(e.NodeID), e.Status)
		case core.RunDone:
			fmt.Println("\n📄 结果:", e.Result.Message.Text())
		}
	}
}

func label(id string) string {
	switch id {
	case "__planner__":
		return "规划(LLM 拆解任务)"
	case "__replan__":
		return "重规划(LLM 检查是否补步骤)"
	default:
		return "节点 " + id
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
