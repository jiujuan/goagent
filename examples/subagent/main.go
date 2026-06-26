// Command subagent is a deeper demonstration of LLM-driven delegation than the
// multiagent example: it builds a THREE-level agent tree and exercises every
// direction the transfer mechanism allows — down to a child, sideways to a
// peer, and (capability-wise) back up to a parent — while the specialists call
// real tools and read/write session state. Everything runs on the mock
// provider, so no API key is required.
//
// The tree:
//
//	concierge (总台 · root)
//	├── tech_lead (技术主管)
//	│   ├── network_expert  ── ping_host 工具；判断其实是硬件问题时「同级」转交
//	│   └── hardware_expert ── check_device 工具
//	└── billing (账单)      ── lookup_invoice 工具（读写 session state）
//
// Four scenes walk distinct delegation paths:
//
//  1. 三级下钻 + 工具      concierge → tech_lead → network_expert(ping_host)
//  2. 另一条下钻路径        concierge → tech_lead → hardware_expert(check_device)
//  3. 路由到账单 + state    concierge → billing(lookup_invoice 写入并复用缓存)
//  4. 同级转交（peer）      …→ network_expert 探测后转交 hardware_expert
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// --- 叶子专家：各自携带工具 ---

	pingHost := tool.New("ping_host", "测试到目标主机的网络连通性",
		func(_ *tool.Context, in struct {
			Host string `json:"host" desc:"目标主机或 IP"`
		}) (string, error) {
			return fmt.Sprintf("PING %s：0%% 丢包，平均 13ms（本地链路正常）", in.Host), nil
		})

	checkDevice := tool.New("check_device", "读取本机硬件传感器读数",
		func(_ *tool.Context, _ struct{}) (string, error) {
			return "CPU 86°C（偏高），风扇 4200rpm（异常）", nil
		})

	// lookup_invoice 演示工具读写 session.State：首次查询写入缓存，再次命中缓存。
	lookupInvoice := tool.New("lookup_invoice", "查询指定月份的账单金额",
		func(ctx *tool.Context, in struct {
			Month string `json:"month" desc:"账单月份，如 2026-06"`
		}) (string, error) {
			key := "invoice:" + in.Month
			if v, ok := ctx.State.Get(key); ok {
				return fmt.Sprintf("账单 %s：¥%v（来自缓存）", in.Month, v), nil
			}
			amount := 199.00
			ctx.State.Set(key, amount)
			return fmt.Sprintf("账单 %s：¥%.2f（已记录）", in.Month, amount), nil
		})

	networkExpert := agent.New(agent.Config{
		Name: "network_expert", Description: "排查网络连通性问题",
		Tools: []tool.Tool{pingHost},
		Model: mock.New("network", func(req *llm.Request) *llm.Response {
			if !answered(req) {
				return mock.CallTool("p", "ping_host", `{"host":"8.8.8.8"}`)
			}
			// ping 正常但用户还在抱怨过热 → 其实是硬件问题，转交同级专家。
			if has(lastUser(req), "烫", "热", "死机", "蓝屏") {
				return transfer("hardware_expert")
			}
			return mock.Text("链路检测正常。请重启光猫并稍候 1 分钟，多数断连可自行恢复。")
		}),
	})

	hardwareExpert := agent.New(agent.Config{
		Name: "hardware_expert", Description: "诊断硬件与过热故障",
		Tools: []tool.Tool{checkDevice},
		Model: mock.New("hardware", func(req *llm.Request) *llm.Response {
			if !answered(req) {
				return mock.CallTool("c", "check_device", `{}`)
			}
			return mock.Text("检测到 CPU 过热、风扇异常，这会引发蓝屏死机。建议清理风道并更换硅脂。")
		}),
	})

	// --- 中层协调者：把技术问题分派给合适的叶子专家 ---

	techLead := agent.New(agent.Config{
		Name: "tech_lead", Description: "技术问题的分派主管",
		SubAgents: []agent.Agent{networkExpert, hardwareExpert},
		Model: mock.New("tech_lead", func(req *llm.Request) *llm.Response {
			q := lastUser(req)
			switch {
			case has(q, "网", "连不上", "断网", "wifi", "WiFi", "慢", "宽带"):
				return transfer("network_expert")
			case has(q, "烫", "热", "死机", "蓝屏", "风扇", "硬件"):
				return transfer("hardware_expert")
			default:
				// 非技术问题：交回上级总台（演示「向上」委派能力）。
				return transfer("concierge")
			}
		}),
	})

	// --- 账单专家：携带状态工具 ---

	billing := agent.New(agent.Config{
		Name: "billing", Description: "处理账单与发票查询",
		Tools: []tool.Tool{lookupInvoice},
		Model: mock.New("billing", func(req *llm.Request) *llm.Response {
			if !answered(req) {
				return mock.CallTool("i", "lookup_invoice", `{"month":"2026-06"}`)
			}
			return mock.Text("已为您查询完毕，如对账单有疑问可申请人工复核。")
		}),
	})

	// --- 根：总台，按问题类别路由到中层或账单专家 ---

	concierge := agent.New(agent.Config{
		Name: "concierge", Description: "智能客服总台",
		SubAgents: []agent.Agent{techLead, billing},
		Model: mock.New("concierge", func(req *llm.Request) *llm.Response {
			q := lastUser(req)
			switch {
			case has(q, "网", "连不上", "断网", "慢", "宽带", "烫", "热", "死机", "蓝屏", "风扇", "硬件", "电脑"):
				return transfer("tech_lead")
			case has(q, "账单", "发票", "扣费", "退款", "收费", "付款"):
				return transfer("billing")
			default:
				return mock.Text("您好，我是智能客服总台，请问有什么可以帮您？")
			}
		}),
	})

	r := runner.New(runner.Config{AppName: "support", Root: concierge})
	ctx := context.Background()
	const sid = "vip-1"

	banner("goagent 子 agent 示例：三级技术支持委派",
		"hierarchy · transfer(down/peer/parent) · tools · session state")

	scene("1. 三级下钻 + 工具：网络连不上")
	consume(ctx, r, sid, "我家宽带突然连不上了")

	scene("2. 另一条下钻路径：硬件过热")
	consume(ctx, r, sid, "电脑总是蓝屏死机")

	scene("3. 路由到账单 + 读写 state（首查写缓存）")
	consume(ctx, r, sid, "帮我查下这个月的账单")

	scene("4. 同级（peer）转交：表面网络问题，实为硬件")
	consume(ctx, r, sid, "网速特别慢，而且机器烫得厉害")
}

// --- 流式消费与打印 ---

func consume(ctx context.Context, r *runner.Runner, sessionID, user string) {
	fmt.Printf("👤 %s\n", user)
	for ev, err := range r.Run(ctx, "user", sessionID, core.UserText(user)) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message == nil {
			continue
		}
		switch ev.Message.Role {
		case core.RoleAssistant:
			if calls := ev.Message.ToolCalls(); len(calls) > 0 {
				for _, c := range calls {
					if c.Name == "transfer_to_agent" {
						fmt.Printf("   🧭 %s 委派 → %s\n", ev.Author, targetName(c.Args))
					} else {
						fmt.Printf("   🔧 %s 调用工具 %s%s\n", ev.Author, c.Name, argsHint(c.Args))
					}
				}
				continue
			}
			fmt.Printf("   🤖 %s：%s\n", ev.Author, ev.Message.Text())
		case core.RoleTool:
			for _, res := range toolResults(ev.Message) {
				if res.Name == "transfer_to_agent" {
					continue // 委派已在上方打印
				}
				fmt.Printf("   ↳ %s ⇒ %s\n", res.Name, partsText(res.Content))
			}
		}
	}
}

// --- 小工具 ---

// answered reports whether the expert's OWN tool has already run, i.e. the
// model is being re-invoked with that tool's result in history. A transfer
// hand-off also leaves a tool result behind, so we explicitly ignore it —
// otherwise a freshly delegated-to expert would skip straight to its answer.
func answered(req *llm.Request) bool {
	res, ok := mock.LastToolResult(req)
	return ok && res.Name != "transfer_to_agent"
}

func transfer(name string) *llm.Response {
	return mock.CallTool("t", "transfer_to_agent", `{"agent_name":"`+name+`"}`)
}

func targetName(args []byte) string {
	var v struct {
		AgentName string `json:"agent_name"`
	}
	_ = json.Unmarshal(args, &v)
	if v.AgentName == "" {
		return string(args)
	}
	return v.AgentName
}

func argsHint(args []byte) string {
	s := strings.TrimSpace(string(args))
	if s == "" || s == "{}" {
		return ""
	}
	return " " + s
}

func toolResults(m *core.Message) []core.ToolResult {
	var out []core.ToolResult
	for _, p := range m.Parts {
		if r, ok := p.(core.ToolResult); ok {
			out = append(out, r)
		}
	}
	return out
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

func lastUser(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}

func has(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}

func scene(title string) { fmt.Printf("\n── %s ──\n", title) }
