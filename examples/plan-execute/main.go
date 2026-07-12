// Command plan-execute demonstrates the Plan-and-Execute pattern on goagent.
//
// Unlike ReAct (one agent that interleaves reasoning and acting), Plan-and-
// Execute SEPARATES the two into distinct stages, wired with the deterministic
// workflow agents — here a Pipeline of three stages:
//
//	Pipeline「plan-and-execute」
//	├─ planner    一次性把任务拆成有序步骤计划       → set_plan 工具写入 state["plan"]
//	├─ executor   逐步执行计划：每步挑对应工具跑一次  → 结果写入 state["exec.results"]
//	└─ solver     读取全部步骤结果，汇总成最终方案    → compose_report 写入 state["final"]
//
// 数据如何在阶段间流动（正是框架的核心机制）：
//   - planner 通过 set_plan 工具把结构化计划写进 session state，其工具结果也进入
//     已提交的消息历史；
//   - executor 在 Run 开始时从 session 重建历史，于是能读到计划，再在自己的
//     模型↔工具内循环里逐步把每一步执行掉，结果累积进 state；
//   - solver 用一个工具读取 state 里积累的步骤结果，合成最终交付物。
//
// 任务：「给 10 人规模的团队，规划一次杭州 2 天团建。」
//
// Everything runs on the mock provider — no API key required:
//
//	go run ./examples/plan-execute
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// Step is one planned action: a human description plus the tool (and its raw
// JSON args) that the executor will invoke to carry it out.
type Step struct {
	N    int             `json:"n"`
	Desc string          `json:"desc"`
	Tool string          `json:"tool"`
	Args json.RawMessage `json:"args"`
}

// StepResult is what executing a step produced; results accrete in state and the
// solver consumes them.
type StepResult struct {
	N      int    `json:"n"`
	Title  string `json:"title"`
	Output string `json:"output"`
}

// capabilityTools are the tool names that count as "executing a plan step".
var capabilityTools = map[string]bool{
	"get_weather": true, "pick_attractions": true, "estimate_budget": true,
}

func main() {
	// === Stage 1: PLANNER =====================================================
	// set_plan persists the model's structured plan into session state and echoes
	// it back as the tool result (so later stages can also read it from history).
	setPlan := tool.New("set_plan", "登记一份有序的执行计划",
		func(ctx *tool.Context, in struct {
			Steps []Step `json:"steps"`
		}) (string, error) {
			ctx.State.Set("plan", in.Steps)
			out, _ := json.Marshal(in.Steps)
			return string(out), nil // 计划 JSON 进入历史，executor 据此执行
		})

	// The plan a real planner LLM would emit, here deterministic. Each step names
	// the capability tool and its arguments.
	plan := []Step{
		{N: 1, Desc: "查询杭州未来两天天气", Tool: "get_weather", Args: json.RawMessage(`{"city":"杭州","days":2}`)},
		{N: 2, Desc: "挑选 4 个适合团建的景点", Tool: "pick_attractions", Args: json.RawMessage(`{"city":"杭州","count":4}`)},
		{N: 3, Desc: "估算 10 人 2 天的预算", Tool: "estimate_budget", Args: json.RawMessage(`{"people":10,"days":2}`)},
	}
	planArgs, _ := json.Marshal(struct {
		Steps []Step `json:"steps"`
	}{plan})

	planner := agent.New(agent.Config{
		Name: "planner", Description: "把任务拆解成有序步骤计划",
		Tools:           []tool.Tool{setPlan},
		DisableTransfer: true,
		Instruction:     "你是规划者。把用户任务拆成一份有序、可执行的步骤计划，并登记下来。",
		Model: mock.New("planner", func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == "set_plan" {
				return mock.Text(fmt.Sprintf("✅ 已制定 %d 步执行计划。", len(plan)))
			}
			return mock.CallTool("p1", "set_plan", string(planArgs))
		}),
	})

	// === Stage 2: EXECUTOR ====================================================
	// Capability tools — the real work. Each reads its typed args, does its job,
	// and appends a StepResult into state["exec.results"].
	weather := tool.New("get_weather", "查询某城市未来若干天天气",
		func(ctx *tool.Context, in struct {
			City string `json:"city"`
			Days int    `json:"days"`
		}) (string, error) {
			out := fmt.Sprintf("%s 未来 %d 天：周六 晴 26℃，周日 多云 24℃，适宜户外。", in.City, in.Days)
			recordStep(ctx.State, "天气", out)
			return out, nil
		})
	attractions := tool.New("pick_attractions", "挑选适合的景点",
		func(ctx *tool.Context, in struct {
			City  string `json:"city"`
			Count int    `json:"count"`
		}) (string, error) {
			pool := []string{"西湖游船", "灵隐寺", "西溪湿地", "宋城千古情", "九溪烟树", "良渚古城"}
			if in.Count > len(pool) {
				in.Count = len(pool)
			}
			out := fmt.Sprintf("%s 精选 %d 处：%v", in.City, in.Count, pool[:in.Count])
			recordStep(ctx.State, "景点", out)
			return out, nil
		})
	budget := tool.New("estimate_budget", "估算团建预算",
		func(ctx *tool.Context, in struct {
			People int `json:"people"`
			Days   int `json:"days"`
		}) (string, error) {
			nights := in.Days - 1
			meals := in.People * in.Days * 100
			lodging := in.People * nights * 250
			transport := in.People * 200
			tickets := in.People * 150
			total := meals + lodging + transport + tickets
			out := fmt.Sprintf("%d 人 %d 天：餐饮¥%d + 住宿¥%d + 交通¥%d + 门票¥%d = 合计 ¥%d",
				in.People, in.Days, meals, lodging, transport, tickets, total)
			recordStep(ctx.State, "预算", out)
			return out, nil
		})

	executor := agent.New(agent.Config{
		Name: "executor", Description: "按计划逐步执行",
		Tools:           []tool.Tool{weather, attractions, budget},
		DisableTransfer: true,
		MaxSteps:        2*len(plan) + 2, // 每步 = 一次模型 + 一次工具，留足余量
		Instruction:     "你是执行者。按既定计划，逐步挑选对应工具执行每一步，直到全部完成。",
		Model: mock.New("executor", func(req *llm.Request) *llm.Response {
			steps := planFromHistory(req)
			done := countExecuted(req)
			if done >= len(steps) {
				return mock.Text(fmt.Sprintf("✅ 计划 %d 步已全部执行完毕。", len(steps)))
			}
			next := steps[done] // 取下一未执行步骤，调用其指定工具
			return mock.CallTool(fmt.Sprintf("e%d", next.N), next.Tool, string(next.Args))
		}),
	})

	// === Stage 3: SOLVER ======================================================
	// compose_report reads every accumulated step result from state and synthesizes
	// the final deliverable, writing it to state["final"].
	composeReport := tool.New("compose_report", "汇总各步骤结果，生成最终方案",
		func(ctx *tool.Context, _ struct{}) (string, error) {
			rs := resultsOf(ctx.State)
			if len(rs) == 0 {
				return "", fmt.Errorf("没有可汇总的执行结果")
			}
			report := "📋 杭州 2 天团建方案\n"
			for _, r := range rs {
				report += fmt.Sprintf("   • 【%s】%s\n", r.Title, r.Output)
			}
			report += "   建议：周六上午西湖+灵隐，下午西溪；周日宋城+自由活动。雨具备用。"
			ctx.State.Set("final", report)
			return report, nil
		})

	solver := agent.New(agent.Config{
		Name: "solver", Description: "综合步骤结果产出最终方案",
		Tools:           []tool.Tool{composeReport},
		DisableTransfer: true,
		Instruction:     "你是汇总者。读取已执行各步骤的结果，合成一份完整、可落地的方案。",
		Model: mock.New("solver", func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == "compose_report" {
				return mock.Text("已生成最终方案，见上。")
			}
			return mock.CallTool("s1", "compose_report", `{}`)
		}),
	})

	// === 组装：Pipeline builder 自上而下声明三阶段 ============================
	pipeline := agent.Pipeline("plan-and-execute").
		Describe("规划 → 执行 → 汇总").
		Then(planner).
		Then(executor).
		Then(solver).
		Build()

	store := session.InMemory()
	r := runner.New(runner.Config{AppName: "plan-exec", Root: pipeline, Store: store})
	ctx := context.Background()

	banner("goagent Plan-and-Execute 示例：先规划再执行",
		"Pipeline · planner→executor→solver · 经 state/history 串联")

	task := "给 10 人团队规划一次杭州 2 天团建。"
	fmt.Printf("\n👤 用户：%s\n", task)

	for ev, err := range r.Run(ctx, "u", "s1", core.UserText(task)) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}

	// 收尾：展示数据在各阶段落进 state 的痕迹。
	fmt.Println("\n── 共享 state 痕迹 ──")
	s, err := store.GetOrCreate(ctx, "plan-exec", "u", "s1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   plan          = %d 步\n", len(planFromState(s.State())))
	fmt.Printf("   exec.results  = %d 条\n", len(resultsOf(s.State())))
	if v, ok := s.State().Get("final"); ok {
		fmt.Printf("   final         = 已生成（%d 字）\n", len([]rune(fmt.Sprint(v))))
	}
}

// --- 阶段间的数据读写助手 ---------------------------------------------------

// recordStep appends a StepResult into shared state, auto-numbering it.
func recordStep(st session.State, title, output string) {
	rs := resultsOf(st)
	rs = append(rs, StepResult{N: len(rs) + 1, Title: title, Output: output})
	st.Set("exec.results", rs)
}

func resultsOf(st session.StateReader) []StepResult {
	if v, ok := st.Get("exec.results"); ok {
		if rs, ok := v.([]StepResult); ok {
			// Appending may reuse a slice's backing array. Copy on read so recordStep
			// always builds a replacement value instead of modifying State-owned data.
			return append([]StepResult(nil), rs...)
		}
	}
	return nil
}

func planFromState(st session.StateReader) []Step {
	if v, ok := st.Get("plan"); ok {
		if p, ok := v.([]Step); ok {
			return append([]Step(nil), p...)
		}
	}
	return nil
}

// planFromHistory recovers the plan the planner registered, by finding the
// set_plan tool result in the committed message history and decoding its JSON.
func planFromHistory(req *llm.Request) []Step {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		for _, p := range req.Messages[i].Parts {
			if tr, ok := p.(core.ToolResult); ok && tr.Name == "set_plan" {
				var steps []Step
				if json.Unmarshal([]byte(partsText(tr.Content)), &steps) == nil {
					return steps
				}
			}
		}
	}
	return nil
}

// countExecuted counts how many capability-tool results already exist in history
// — i.e. how many plan steps have run.
func countExecuted(req *llm.Request) int {
	n := 0
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && capabilityTools[tr.Name] {
				n++
			}
		}
	}
	return n
}

// --- 流式打印 ---------------------------------------------------------------

func printEvent(ev *core.Event) {
	if ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				fmt.Printf("   %s %-9s →调用 %s(%s)\n", stageIcon(ev.Author), ev.Author, c.Name, string(c.Args))
			}
			return
		}
		fmt.Printf("   %s %-9s %s\n", stageIcon(ev.Author), ev.Author, ev.Message.Text())
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("      ↳ %s\n", partsText(tr.Content))
			}
		}
	}
}

func stageIcon(author string) string {
	switch author {
	case "planner":
		return "🗺️"
	case "executor":
		return "⚙️"
	case "solver":
		return "🧩"
	default:
		return "•"
	}
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}
