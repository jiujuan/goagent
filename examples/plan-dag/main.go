// Command plan-dag is a tutorial for the DAG plan executor (agent.NewPlan): a
// complex task decomposed into nodes with dependencies, run with real
// parallelism, a final human-approval gate, and crash-safe resume.
//
// The graph:
//
//	research ──┬─> risks ──┐
//	           └─> bench ──┴─> report (final approval)
//
// research runs first; risks and bench run CONCURRENTLY; report runs after both
// and pauses for approval. Each node references upstream outputs via {{id}}.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/plan-dag
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

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "research", Task: "用 3 句话调研主题「{{input}}」的现状。"},
		{ID: "risks", Task: "基于这份调研列出 2 个风险:\n{{research}}", DependsOn: []string{"research"}},
		{ID: "bench", Task: "基于这份调研给 2 条选型/性能建议:\n{{research}}", DependsOn: []string{"research"}},
		{ID: "report", Task: "综合风险与建议写一段 80 字结论。\n风险:{{risks}}\n建议:{{bench}}",
			DependsOn: []string{"risks", "bench"}},
	}}

	pa := agent.NewPlan("research-dag", plan,
		agent.WithWorker(worker),
		agent.WithConcurrency(4),  // risks 与 bench 真并发
		agent.WithFinalApproval(), // 全部完成后,出报告前人工批准
	)

	const thread = "job-1"
	run := pa.Stream(ctx, "向量数据库选型", agent.OnThread(thread))

	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.PlanNodeStarted:
			fmt.Printf("▶️  节点 %-9s 开始\n", e.NodeID)
		case core.PlanNodeDone:
			fmt.Printf("✅ 节点 %-9s %s\n", e.NodeID, e.Status)
		case core.Interrupted:
			pending = e.Pending
		}
	}

	if len(pending) == 0 {
		return
	}
	fmt.Println("\n⏸️  全部节点完成,等待对最终报告的批准... (演示:批准)")
	for _, p := range pending {
		run.Decide(agent.Allow(p.CallID))
	}
	cont, err := run.Resume(ctx) // 也可在新进程里 pa.Resume(ctx, thread) 续跑
	if err != nil {
		log.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n📄 最终报告:", res.Message.Text())
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
