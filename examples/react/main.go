// Command react demonstrates the ReAct pattern (Reason + Act) on goagent.
//
// ReAct is NOT a special agent type in goagent — it is exactly what a plain
// LLMAgent already does: a single agent that, in one turn, repeatedly
//
//	Thought  →  the model reasons about what it still needs
//	Action   →  the model calls a tool
//	Observation → the tool result comes back into history
//
// and loops until the model can answer without another tool call. That loop is
// the `for range a.maxSteps` in agent.LLMAgent.Run — you don't implement it.
//
// This example wires ONE agent with three tools and poses a compound question
// that cannot be answered in a single tool call:
//
//	"3 个人坐高铁从北京往返上海，一共多少钱？再帮我换算成美元。"
//
// The scripted mock model drives a genuine multi-step chain — each step's
// arguments are formed from the PREVIOUS step's observation, which is the
// essence of ReAct:
//
//	① search_kb  查到单程票价 ──► ② calculate 553×6 ──► ③ convert 人民币→美元 ──► ④ 作答
//
// Everything runs on the mock provider — no API key required:
//
//	go run ./examples/react
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

// --- 工具：ReAct 里的 "Act" 能力 -------------------------------------------

type kbArgs struct {
	Query string `json:"query" desc:"要在知识库中检索的问题"`
}

// searchKB is a tiny deterministic knowledge base. Each entry is phrased so the
// salient number trails a '=' sign, which the model parses to form the next
// action's arguments (see numberAfter).
var kb = map[string]string{
	"高铁票价": "高铁二等座 北京⇄上海 单程票价 = 553 元/人",
	"汇率":   "今日参考汇率 CNY→USD：1 美元 = 7.10 元",
}

func searchKB(_ *tool.Context, in kbArgs) (string, error) {
	for k, v := range kb {
		if strings.Contains(in.Query, k) || strings.Contains(k, in.Query) {
			return v, nil
		}
	}
	return "知识库未命中：" + in.Query, nil
}

type calcArgs struct {
	A  float64 `json:"a" desc:"左操作数"`
	B  float64 `json:"b" desc:"右操作数"`
	Op string  `json:"op" desc:"运算：add/sub/mul/div"`
}

func calculate(_ *tool.Context, in calcArgs) (string, error) {
	var r float64
	sym := map[string]string{"add": "+", "sub": "-", "mul": "×", "div": "÷"}[in.Op]
	switch in.Op {
	case "add":
		r = in.A + in.B
	case "sub":
		r = in.A - in.B
	case "mul":
		r = in.A * in.B
	case "div":
		if in.B == 0 {
			return "", fmt.Errorf("除数不能为 0")
		}
		r = in.A / in.B
	default:
		return "", fmt.Errorf("未知运算符：%q", in.Op)
	}
	return fmt.Sprintf("%g %s %g = %g", in.A, sym, in.B, r), nil
}

type convArgs struct {
	Amount float64 `json:"amount" desc:"金额"`
	Rate   float64 `json:"rate" desc:"1 美元兑人民币"`
}

func convert(_ *tool.Context, in convArgs) (string, error) {
	if in.Rate == 0 {
		return "", fmt.Errorf("汇率缺失")
	}
	return fmt.Sprintf("%.2f 元 ÷ %.2f = %.2f 美元", in.Amount, in.Rate, in.Amount/in.Rate), nil
}

func main() {
	tools := []tool.Tool{
		tool.New("search_kb", "在知识库中检索事实（票价、汇率等）", searchKB),
		tool.New("calculate", "做一次四则运算", calculate),
		tool.New("convert", "把人民币金额按汇率换算成美元", convert),
	}

	// 一个 ReAct 智能体：模型在一个 turn 内反复 思考→调用工具→读观察→再思考。
	// 这里用脚本化 mock 复刻真实模型会走的推理链：每一步的参数都来自上一步的观察。
	react := agent.New(agent.Config{
		Name:        "react-assistant",
		Description: "ReAct 推理-行动智能体",
		Tools:       tools,
		MaxSteps:    10, // 兜底：限制 Thought/Action 循环最多 10 步，防止失控
		Instruction: "你是一个会使用工具的助手。遇到需要外部信息或计算的问题时，" +
			"先思考缺什么，再调用合适的工具，拿到结果后继续推理，直到能给出最终答案。",
		Model: mock.New("react-mock", reactBrain),
	})

	r := runner.New(runner.Config{Root: react})
	ctx := context.Background()

	banner("goagent ReAct 示例：推理-行动循环 (Reason + Act)",
		"单个 LLMAgent · Thought→Action→Observation 反复迭代直到能作答")

	question := "3 个人坐高铁从北京往返上海一共多少钱？再帮我换算成美元。"
	fmt.Printf("\n👤 用户：%s\n\n", question)

	step := 0
	for ev, err := range r.Run(ctx, "u", "s1", core.UserText(question)) {
		if err != nil {
			log.Fatal(err)
		}
		step = printReAct(ev, step)
	}
}

// reactBrain is the scripted "reasoning". It inspects history to decide the next
// action — the same decision a real model makes, here made deterministic. The
// key ReAct trait: each action's arguments are derived from the latest
// observation (numberAfter / lastResultOf), not hard-coded.
func reactBrain(req *llm.Request) *llm.Response {
	last, ok := mock.LastToolResult(req)
	if !ok {
		// 第 1 步 —— Thought：要算车票，得先知道单程票价。Action：查知识库。
		return mock.CallTool("c1", "search_kb", `{"query":"高铁票价"}`)
	}

	switch last.Name {
	case "search_kb":
		text := partsText(last.Content)
		if strings.Contains(text, "汇率") {
			// 已查到汇率 —— Action：把人民币总额换算成美元。
			total := lastNumberOfCalc(req)
			rate := numberAfter(text, "=")
			return mock.CallTool("c3", "convert",
				fmt.Sprintf(`{"amount":%g,"rate":%g}`, total, rate))
		}
		// 拿到单程票价 553 —— Thought：往返×2、3 人×3 → ×6。Action：计算。
		price := numberAfter(text, "=")
		return mock.CallTool("c2", "calculate",
			fmt.Sprintf(`{"a":%g,"b":6,"op":"mul"}`, price))

	case "calculate":
		// 得到总价 —— Thought：还需要汇率才能换算。Action：再查知识库。
		return mock.CallTool("c2b", "search_kb", `{"query":"汇率"}`)

	case "convert":
		// 拿到美元金额 —— 信息齐了，给出最终答案（无工具调用 → 结束循环）。
		cny := lastNumberOfCalc(req)
		usd := numberAfter(partsText(last.Content), "=")
		return mock.Text(fmt.Sprintf(
			"3 人北京⇄上海高铁往返合计 %.0f 元（每人单程 553 元 × 往返 2 × 3 人），"+
				"按 7.10 汇率约合 %.2f 美元。", cny, usd))
	}
	return mock.Text("我没能完成推理，请换个问法。")
}

// --- ReAct 轨迹打印 --------------------------------------------------------

func printReAct(ev *core.Event, step int) int {
	if ev.Message == nil {
		return step
	}
	switch ev.Message.Role {
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				step++
				fmt.Printf("┌─ 第 %d 步\n", step)
				fmt.Printf("│  🧠 Thought/Act  调用 %s(%s)\n", c.Name, string(c.Args))
			}
			return step
		}
		fmt.Printf("\n✅ 最终回答：%s\n", ev.Message.Text())
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("│  👁 Observation  %s\n└─\n", partsText(tr.Content))
			}
		}
	}
	return step
}

// --- 解析观察值的小工具（模型据此形成下一步动作的参数）-----------------------

// numberAfter returns the first number appearing after the last sep in s.
func numberAfter(s, sep string) float64 {
	if i := strings.LastIndex(s, sep); i >= 0 {
		s = s[i+len(sep):]
	}
	return firstNumber(s)
}

// firstNumber scans s and parses the first float64 it finds (handles decimals).
func firstNumber(s string) float64 {
	var b strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r == '.' && b.Len() > 0) {
			b.WriteRune(r)
		} else if b.Len() > 0 {
			break
		}
	}
	f, _ := strconv.ParseFloat(b.String(), 64)
	return f
}

// lastNumberOfCalc finds the most recent calculate result in history and returns
// the number after its '=' — i.e. the running total carried across steps.
func lastNumberOfCalc(req *llm.Request) float64 {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		for _, p := range req.Messages[i].Parts {
			if tr, ok := p.(core.ToolResult); ok && tr.Name == "calculate" {
				return numberAfter(partsText(tr.Content), "=")
			}
		}
	}
	return 0
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
