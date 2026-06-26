// Command workflow demonstrates goagent's DETERMINISTIC orchestration agents —
// Sequential, Parallel and Loop — composed into one pipeline, with no reliance
// on the LLM to decide control flow. It models a small "marketing copy"
// production line:
//
//	Sequential「pipeline」
//	├─ Parallel「research」   三个调研 agent 并发跑，各自把结论写入 session state
//	│   ├─ market_research      OutputKey: research.market
//	│   ├─ competitor_research  OutputKey: research.competitor
//	│   └─ audience_research    OutputKey: research.audience
//	├─ writer                 读取前序调研（消息历史）产出初稿
//	└─ Loop「refine」(≤3 轮)   critic 评审 / reviser 修订，达标时 Escalate 跳出
//
// It showcases:
//   - Parallel 并发分支 + OutputKey 写入共享 state；
//   - Sequential 阶段间通过已提交的消息历史传递数据；
//   - Loop 反复迭代直到某个 sub-agent 触发 Escalate（而非靠固定轮数）。
//
// Everything runs on the mock provider — no API key required.
package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// --- 阶段 1：并发调研。每个 agent 把结论写入 state（OutputKey），其最终回复
	//     也作为消息进入会话历史，供下游 writer 读取。 ---
	market := researcher("market_research", "research.market",
		"市场规模约 120 亿元，年增长 18%，下沉市场尚未饱和。")
	competitor := researcher("competitor_research", "research.competitor",
		"头部 3 家竞品合计占 40% 份额，普遍主打高价高配，缺少性价比款。")
	audience := researcher("audience_research", "research.audience",
		"核心用户为 25–35 岁一二线城市白领，看重续航与拍照。")
	research := agent.Parallel("research", market, competitor, audience)

	// --- 阶段 2：撰稿。读取历史里的三段调研，产出初稿。 ---
	writer := agent.New(agent.Config{
		Name: "writer", Description: "综合调研产出推广初稿",
		DisableTransfer: true,
		Model: mock.New("writer", func(req *llm.Request) *llm.Response {
			facts := priorFindings(req)
			return mock.Text("初稿：GoPhone 面向年轻白领，主打超长续航与影像；" +
				"综合调研要点 —— " + strings.Join(facts, "；") + "。")
		}),
	})

	// --- 阶段 3：评审/修订循环，达标时通过 approve 工具触发 Escalate 跳出 Loop。 ---
	approve := tool.New("approve", "批准草稿可发布，结束修订循环",
		func(ctx *tool.Context, _ struct{}) (string, error) {
			ctx.Actions.Escalate = true // 通知外层 Loop 停止迭代
			return "approved", nil
		})

	round := 0
	critic := agent.New(agent.Config{
		Name: "critic", Description: "评审草稿质量",
		DisableTransfer: true,
		Tools:           []tool.Tool{approve},
		Model: mock.New("critic", func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == "approve" {
				return mock.Text("✓ 已批准，可以发布。")
			}
			round++
			if round >= 2 { // 第二轮起认为已达标
				return mock.CallTool("a", "approve", `{}`)
			}
			return mock.Text("✗ 开头缺少钩子，且未标注数据来源，请修订。")
		}),
	})

	rev := 0
	reviser := agent.New(agent.Config{
		Name: "reviser", Description: "按评审意见修订草稿",
		DisableTransfer: true,
		Model: mock.New("reviser", func(*llm.Request) *llm.Response {
			rev++
			return mock.Text(fmt.Sprintf(
				"修订稿 v%d：开头加入钩子「每天省一杯咖啡钱，续航多撑一天」，并补注数据来源。", rev))
		}),
	})
	refine := agent.Loop("refine", 3, critic, reviser)

	// --- 组装并运行流水线 ---
	pipeline := agent.Sequential("pipeline", research, writer, refine)

	store := session.InMemory()
	r := runner.New(runner.Config{AppName: "report", Root: pipeline, Store: store})
	ctx := context.Background()

	banner("goagent 工作流示例：营销文案生产流水线",
		"Sequential · Parallel · Loop · OutputKey→state · Escalate")

	for ev, err := range r.Run(ctx, "u", "s1", core.UserText("为 GoPhone 写一段市场推广短文")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}

	// --- 收尾：展示并发调研经 OutputKey 写入的共享 state ---
	fmt.Println("\n── 共享 state（research.* 由并发分支写入）──")
	s, err := store.GetOrCreate(ctx, "report", "u", "s1")
	if err != nil {
		log.Fatal(err)
	}
	for _, k := range sortedResearchKeys(s.State()) {
		v, _ := s.State().Get(k)
		fmt.Printf("   %-22s = %v\n", k, v)
	}
}

// printEvent renders one streamed event with a stage icon keyed by author.
func printEvent(ev *core.Event) {
	if ev.Message == nil {
		return // OutputKey 等纯 state 事件无消息体
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("\n👤 %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				if c.Name == "approve" {
					fmt.Printf("   ✅ %s 批准草稿 → 触发 Escalate，退出 refine 循环\n", ev.Author)
				}
			}
			return
		}
		fmt.Printf("   %s %-18s %s\n", icon(ev.Author), ev.Author, ev.Message.Text())
	}
}

func icon(author string) string {
	switch {
	case strings.HasSuffix(author, "research"):
		return "🔎"
	case author == "writer":
		return "✍️"
	case author == "critic":
		return "🧐"
	case author == "reviser":
		return "🛠️"
	default:
		return "•"
	}
}

// researcher builds a leaf agent that returns a fixed finding and also records
// it into session state under outputKey.
func researcher(name, outputKey, finding string) *agent.LLMAgent {
	return agent.New(agent.Config{
		Name: name, Description: "调研：" + name,
		OutputKey:       outputKey,
		DisableTransfer: true,
		Model: mock.New(name, func(*llm.Request) *llm.Response {
			return mock.Text(finding)
		}),
	})
}

// priorFindings extracts the research agents' replies already committed to the
// conversation, so the writer can compose from them.
func priorFindings(req *llm.Request) []string {
	var out []string
	for _, m := range req.Messages {
		if m.Role == core.RoleAssistant && m.Text() != "" && len(m.ToolCalls()) == 0 {
			out = append(out, m.Text())
		}
	}
	return out
}

func sortedResearchKeys(state session.State) []string {
	var keys []string
	for _, k := range []string{"research.market", "research.competitor", "research.audience"} {
		if _, ok := state.Get(k); ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}
