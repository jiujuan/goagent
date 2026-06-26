// Command plan-dag demonstrates goagent's execution-plan system: a dependency
// graph of steps, not a flat list. Where examples/plan-execute runs a fixed
// Pipeline(planner→executor→solver), here the plan is a DAG and the scheduler
// resolves dependencies and runs independent steps concurrently.
//
//	┌──────────────┐
//	│ get_weather  │──┐
//	└──────────────┘  │   两步无依赖 → 并行执行
//	┌──────────────┐  │
//	│ attractions  │──┤
//	└──────────────┘  │
//	                  ▼
//	         ┌──────────────────┐
//	         │ estimate_budget  │   依赖天气+景点 → 等二者完成
//	         └──────────────────┘
//	                  ▼
//	         ┌──────────────────┐
//	         │  compose_report  │   汇总，依赖预算
//	         └──────────────────┘
//
// 任务：「给 10 人团队规划杭州 2 天团建。」每个步骤由一个工具执行
// （plan.ToolExecutor），步骤间通过 session state 传递结果。
//
//	go run ./examples/plan-dag
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/plan"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// --- 能力工具：每个步骤跑一个 ------------------------------------------
	weather := tool.New("get_weather", "查询某城市未来若干天天气",
		func(ctx *tool.Context, in struct {
			City string `json:"city"`
			Days int    `json:"days"`
		}) (string, error) {
			return fmt.Sprintf("%s 未来 %d 天：周六 晴 26℃，周日 多云 24℃，适宜户外。", in.City, in.Days), nil
		})

	attractions := tool.New("pick_attractions", "挑选适合团建的景点",
		func(ctx *tool.Context, in struct {
			City  string `json:"city"`
			Count int    `json:"count"`
		}) (string, error) {
			pool := []string{"西湖游船", "灵隐寺", "西溪湿地", "宋城千古情"}
			if in.Count < len(pool) {
				pool = pool[:in.Count]
			}
			return fmt.Sprintf("%s 精选 %d 处：%v", in.City, in.Count, pool), nil
		})

	// estimate_budget 依赖前两步：从 state 读取它们的结果再算预算。
	budget := tool.New("estimate_budget", "估算团建预算（读取天气与景点结果）",
		func(ctx *tool.Context, in struct {
			People int `json:"people"`
			Days   int `json:"days"`
		}) (string, error) {
			w, _ := ctx.State.Get(plan.StepResultKey("weather"))
			a, _ := ctx.State.Get(plan.StepResultKey("attractions"))
			total := in.People*in.Days*100 + in.People*(in.Days-1)*250 + in.People*350
			return fmt.Sprintf("基于【%v】【%v】，%d 人 %d 天合计约 ¥%d。",
				w, a, in.People, in.Days, total), nil
		})

	report := tool.New("compose_report", "汇总各步骤结果，生成最终方案",
		func(ctx *tool.Context, _ struct{}) (string, error) {
			b, _ := ctx.State.Get(plan.StepResultKey("budget"))
			return fmt.Sprintf("📋 杭州 2 天团建方案已就绪。预算结论：%v 建议周六西湖+灵隐，周日宋城。", b), nil
		})

	args := func(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

	// --- 声明执行计划（DAG）-------------------------------------------------
	p := &plan.Plan{
		ID:   "hz-teambuilding",
		Goal: "给 10 人团队规划杭州 2 天团建",
		Steps: []*plan.Step{
			{ID: "weather", Name: "查询天气",
				Exec: plan.ToolExecutor{Tool: weather, Args: args(map[string]any{"city": "杭州", "days": 2})}},
			{ID: "attractions", Name: "挑选景点",
				Exec: plan.ToolExecutor{Tool: attractions, Args: args(map[string]any{"city": "杭州", "count": 4})}},
			{ID: "budget", Name: "估算预算", DependsOn: []string{"weather", "attractions"},
				Exec: plan.ToolExecutor{Tool: budget, Args: args(map[string]any{"people": 10, "days": 2})}},
			{ID: "report", Name: "生成方案", DependsOn: []string{"budget"},
				Exec: plan.ToolExecutor{Tool: report, Args: args(struct{}{})}},
		},
	}

	planAgent := plan.New(plan.Config{
		Name:        "team-build-planner",
		Description: "用 DAG 计划编排杭州团建规划",
		Plan:        p,
		MaxConc:     4,
		// Backend 选择并发底座：默认 BackendGoroutines（goroutine+信号量，errgroup 式），
		// 或 plan.BackendQueue 复用 queue.Worker 池；两者 OnError/Retry 行为一致。
		Backend: plan.BackendGoroutines,
	})

	r := runner.New(runner.Config{AppName: "plan-dag", Root: planAgent, Store: session.InMemory()})
	fmt.Println("=== 执行计划（DAG）开始 ===")
	for ev, err := range r.Run(context.Background(), "user", "s1", core.UserText("开始规划")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev == nil || ev.Partial {
			continue
		}
		if ev.Progress != nil && ev.Progress.Kind == "plan_step" {
			fmt.Printf("  · 步骤 %-12s → %s\n", ev.Progress.JobID, ev.Progress.Status)
		} else if ev.Message != nil && ev.Author == planAgent.Name() {
			fmt.Println(ev.Message.Text())
		}
	}
	fmt.Println("=== 完成 ===")
}
