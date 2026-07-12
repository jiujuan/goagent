// Command builder is the second, richer pipeline example. Where
// examples/pipeline shows a hand-wired Sequential ETL flow, this one uses the
// fluent agent.Pipeline builder to compose a multi-stage research-report
// pipeline that mixes a plain stage, a concurrent fan-out stage, and a
// review/revise loop — read top-to-bottom, no nested constructors:
//
//	agent.Pipeline("research-report").
//	    Then(planner).                              // ① 规划提纲
//	    ThenParallel("gather", web, papers, news).  // ② 三源并发检索（带工具）
//	    Then(writer).                               // ③ 综合成稿
//	    ThenLoop("review", 3, critic, reviser).     // ④ 评审-修订循环（Escalate 跳出）
//	    Build()
//
// The fan-out stage uses isolated state/event branches and a deterministic
// merge before writer runs. Each leaf uses the mock provider; no key is needed.
package main

import (
	"context"
	"fmt"
	"log"
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
	// ① 规划：产出提纲，并经 OutputKey 落到 state。
	planner := agent.New(agent.Config{
		Name: "planner", Description: "规划报告提纲",
		OutputKey:       "report.plan",
		DisableTransfer: true,
		Model: mock.New("planner", func(*llm.Request) *llm.Response {
			return mock.Text("提纲：①市场现状 ②增长趋势 ③主要风险")
		}),
	})

	// ② 检索：三个数据源 agent，各带一个抓取工具，并发执行。
	web := source("web", "report.web", "web_search", "检索公开网页",
		"网页源：折叠屏季度出货同比 +45%")
	papers := source("papers", "report.papers", "paper_scan", "检索学术论文",
		"论文源：柔性 OLED 良率突破 70%")
	news := source("news", "report.news", "news_scan", "检索新闻",
		"新闻源：两家厂商发布新一代折叠机")

	// ③ 撰稿：综合提纲与三源检索（消息历史）成稿。
	writer := agent.New(agent.Config{
		Name: "writer", Description: "综合资料产出初稿",
		DisableTransfer: true,
		Model: mock.New("writer", func(req *llm.Request) *llm.Response {
			return mock.Text("初稿：依据提纲综合各源资料 —— " +
				strings.Join(priorFindings(req), "；") + "。")
		}),
	})

	// ④ 评审循环：critic 不达标则反馈，reviser 修订；达标时 approve 触发 Escalate。
	approve := tool.New("approve", "批准草稿可发布，结束评审循环",
		func(ctx *tool.Context, _ struct{}) (string, error) {
			ctx.Actions.Escalate = true
			return "approved", nil
		})
	round := 0
	critic := agent.New(agent.Config{
		Name: "critic", Description: "评审草稿",
		DisableTransfer: true,
		Tools:           []tool.Tool{approve},
		Model: mock.New("critic", func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == "approve" {
				return mock.Text("✓ 已批准。")
			}
			round++
			if round >= 2 {
				return mock.CallTool("a", "approve", `{}`)
			}
			return mock.Text("✗ 论据偏薄，请补充数据来源。")
		}),
	})
	rev := 0
	reviser := agent.New(agent.Config{
		Name: "reviser", Description: "按意见修订",
		DisableTransfer: true,
		Model: mock.New("reviser", func(*llm.Request) *llm.Response {
			rev++
			return mock.Text(fmt.Sprintf("修订稿 v%d：为每条论据补注来源与时间。", rev))
		}),
	})

	// 用 builder 把四个阶段（含并发与循环复合阶段）串成一条管道。
	report := agent.Pipeline("research-report").
		Describe("研究报告生成流水线").
		Then(planner).
		ThenParallel("gather", web, papers, news).
		Then(writer).
		ThenLoop("review", 3, critic, reviser).
		Build()

	store := session.InMemory()
	r := runner.New(runner.Config{AppName: "report", Root: report, Store: store})
	ctx := context.Background()

	banner("goagent Pipeline builder 示例：研究报告生成",
		"agent.Pipeline().Then().ThenParallel().Then().ThenLoop().Build()")

	for ev, err := range r.Run(ctx, "u", "s1", core.UserText("写一份折叠屏市场研究报告")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}

	fmt.Println("\n── OutputKey 写入的共享 state ──")
	s, err := store.GetOrCreate(ctx, "report", "u", "s1")
	if err != nil {
		log.Fatal(err)
	}
	for _, k := range []string{"report.plan", "report.web", "report.papers", "report.news"} {
		if v, ok := s.State().Get(k); ok {
			fmt.Printf("   %-14s = %s\n", k, v)
		}
	}
}

// source builds a data-source agent: it calls its fetch tool, relays the
// finding, and records it under outKey via OutputKey.
func source(name, outKey, toolName, toolDesc, finding string) *agent.LLMAgent {
	fetch := tool.New(toolName, toolDesc, func(_ *tool.Context, _ struct{}) (string, error) {
		return finding, nil
	})
	return agent.New(agent.Config{
		Name: name, Description: toolDesc,
		OutputKey:       outKey,
		DisableTransfer: true,
		Tools:           []tool.Tool{fetch},
		Model: mock.New(name, func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == toolName {
				return mock.Text(partsText(res.Content))
			}
			return mock.CallTool("c", toolName, `{}`)
		}),
	})
}

func printEvent(ev *core.Event) {
	if ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("\n👤 %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				if c.Name == "approve" {
					fmt.Printf("   ✅ %s 批准 → Escalate，退出 review 循环\n", ev.Author)
				}
			}
			return
		}
		fmt.Printf("   %s %-9s %s\n", icon(ev.Author), ev.Author, ev.Message.Text())
	}
}

func icon(author string) string {
	switch author {
	case "planner":
		return "🗺️"
	case "web", "papers", "news":
		return "🔎"
	case "writer":
		return "✍️"
	case "critic":
		return "🧐"
	case "reviser":
		return "🛠️"
	default:
		return "•"
	}
}

// priorFindings returns the plain-text assistant messages already in history
// (planner outline + the three sources' findings), for the writer to compose.
func priorFindings(req *llm.Request) []string {
	var out []string
	for _, m := range req.Messages {
		if m.Role == core.RoleAssistant && m.Text() != "" && len(m.ToolCalls()) == 0 {
			out = append(out, m.Text())
		}
	}
	return out
}

func partsText(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}
