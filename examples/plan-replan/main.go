// Command plan-replan is a tutorial for dynamic replanning (Step 3): a plan that
// REWRITES itself and executes the rewritten todos. When the current DAG goes
// quiescent, a replanner agent inspects the results so far and may add new nodes
// (with dependencies); the executor merges and runs them, repeating until the
// replanner says done (bounded by WithMaxReplanRounds).
//
// Here the initial plan gathers two angles; the replanner notices there is no
// synthesis step yet and adds one depending on both. So "decompose → execute →
// decompose more → execute".
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/plan-replan
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

	worker, _ := agent.New(agent.WithModel(model))

	// 重规划器:看到已完成结果后,决定是否追加步骤。系统提示约束它只输出 JSON。
	replanner, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction(
			"你是规划修订器。看完已完成步骤的结果后,判断是否还缺一个『综合总结』步骤。\n"+
				"- 若结果里还没有综合总结,就只输出 JSON 追加一个节点:\n"+
				`  {"nodes":[{"id":"synthesis","task":"综合 {{angle_a}} 与 {{angle_b}} 写一段结论","depends_on":["angle_a","angle_b"]}],"done":false}`+"\n"+
				`- 若已经有综合结论了,就只输出 {"done":true}。只输出 JSON。`),
	)

	// 初始计划:两个并行角度,故意不含总结步骤(留给重规划器补)。
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "angle_a", Task: "就主题「{{input}}」给出技术角度的一句要点。"},
		{ID: "angle_b", Task: "就主题「{{input}}」给出业务角度的一句要点。"},
	}}

	pa := agent.NewPlan("replan-dag", plan,
		agent.WithWorker(worker),
		agent.WithReplanner(replanner),
		agent.WithMaxReplanRounds(2),
	)

	out, err := pa.Run(ctx, "为什么用 Go 写 AI Agent 框架")
	// 用 Stream 也可以看到 __replan__ 节点事件;这里直接拿结果。
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("📄 最终结果:", out)

	// 若想看每个节点(含 __replan__)的事件,用流式:
	fmt.Println("\n--- 流式再跑一遍(看节点/重规划事件)---")
	for ev, e := range pa.Stream(ctx, "为什么用 Go 写 AI Agent 框架", agent.OnThread("s2")).Iter() {
		if e != nil {
			log.Fatal(e)
		}
		switch v := ev.(type) {
		case core.PlanNodeStarted:
			fmt.Printf("▶️  %s\n", v.NodeID)
		case core.PlanNodeDone:
			fmt.Printf("✅ %s %s\n", v.NodeID, v.Status)
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
