// Command plan-approval is a tutorial for per-node approval in the DAG executor
// (Step 2): a node marked Approve pauses for a human decision BEFORE it runs,
// while independent branches keep executing. Approve/reject via Run.Decide +
// Run.Resume; a rejected node cascades (its dependents are skipped).
//
// The graph:
//
//	analyze ──┬─> draft ───────────┐
//	          └─> sensitive(审批) ──┴─> summary
//
// After analyze: draft runs AND sensitive waits for approval (concurrently).
// draft finishes; the plan pauses for sensitive; you approve; sensitive runs;
// summary runs.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/plan-approval
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
		{ID: "analyze", Task: "用一句话分析主题「{{input}}」。"},
		{ID: "draft", Task: "据此写一句正面介绍:{{analyze}}", DependsOn: []string{"analyze"}},
		{ID: "sensitive", Task: "据此写一句对外公关口径:{{analyze}}", DependsOn: []string{"analyze"},
			Approve: true}, // ← 对外口径需人工批准
		{ID: "summary", Task: "合并:{{draft}} / {{sensitive}}", DependsOn: []string{"draft", "sensitive"}},
	}}

	pa := agent.NewPlan("approval-dag", plan, agent.WithWorker(worker), agent.WithConcurrency(4))

	const thread = "job-1"
	run := pa.Stream(ctx, "公司新产品发布", agent.OnThread(thread))

	// 恢复直至完成:可能多次暂停(每"波"待批节点一次)。
	var result string
	for {
		var pending []core.ApprovalRequest
		done := false
		for ev, err := range run.Iter() {
			if err != nil {
				log.Fatal(err)
			}
			switch e := ev.(type) {
			case core.PlanNodeStarted:
				fmt.Printf("▶️  %s 开始\n", e.NodeID)
			case core.PlanNodeDone:
				fmt.Printf("✅ %s %s\n", e.NodeID, e.Status)
			case core.Interrupted:
				pending = e.Pending
			case core.RunDone:
				result = e.Result.Message.Text()
				done = true
			}
		}
		if done {
			break
		}
		for _, p := range pending {
			fmt.Printf("⏸️  审批节点 %q —— 将执行:%s\n   → 批准\n", p.CallID, string(p.Args))
			run.Decide(agent.Allow(p.CallID)) // 改 agent.Reject(p.CallID, "理由") 则该节点及其下游被跳过
		}
		var err error
		if run, err = run.Resume(ctx); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("\n📄 结果:", result)
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
