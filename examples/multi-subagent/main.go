// Command multi-subagent 演示「一个主 agent(编排者)调用多个子 agent」的标准做法,
// 重点说明子 agent 的【上下文隔离 / 独立 context】。
//
// ┌──────────────────────────────────────────────────────────────────────┐
// │  核心概念:agent.AsTool(child, name, desc)                              │
// │                                                                        │
// │  把一个子 agent 包成「工具」挂到主 agent 上。当主 agent 调用这个工具时:  │
// │                                                                        │
// │    1. 子 agent 以【全新、隔离】的 context 启动 —— 它的输入【只有】这次   │
// │       工具调用传入的 task 字符串,看不到主 agent 的对话历史。           │
// │    2. 子 agent 内部可以多步思考、调用自己的工具(下面每个子 agent 都有   │
// │       自己的工具),这些中间过程【不会】污染主 agent 的 context。        │
// │    3. 子 agent 只把【最终一段文本】作为工具结果回传给主 agent。         │
// │                                                                        │
// │  这就是 deep-agents 的「quarantine(隔离/检疫)」模型:                  │
// │  主 agent 的 context 始终干净,只装「任务 → 结论」,不装子 agent 的过程。│
// └──────────────────────────────────────────────────────────────────────┘
//
// 对比另外两个例子:
//   - examples/multiagent : transfer_to_agent(转交)—— 把【整段对话】交给子 agent,
//                           是「交接」,共享同一个 State,不隔离。
//   - examples/subagent   : AsTool 的最小版,只挂两个无工具的子 agent。
//   - 本例(multi-subagent): AsTool 的进阶版 —— 多个【各自带工具】的子 agent,
//                           并用两段实验【直观证明】每个子 agent 的 context 是独立的。
//
// 运行(需要一个 OpenAI 兼容的聊天模型,这里用 Agnes):
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash      # 可选
//	export AGNES_BASE_URL=https://...        # 可选
//	go run ./examples/multi-subagent
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	model, ok := buildModel()
	if !ok {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL / AGNES_BASE_URL)再运行。")
		return
	}
	ctx := context.Background()

	// ──────────────────────────────────────────────────────────────────
	// 第 1 步:构建三个【专才子 agent】,每个都有自己的工具和系统提示。
	//
	// 关键点:子 agent 是完整的 *agent.Agent —— 它有自己的模型、指令、工具,
	// 每次被调用都会跑一个独立的 Run(全新 State / 全新消息历史)。
	// 它们彼此之间、以及与主 agent 之间,context 完全不共享。
	// ──────────────────────────────────────────────────────────────────

	// 子 agent A:调研员。配了一个「知识库查询」工具(假数据)。
	// 它收到主题后,会【自己】决定调用 kb_lookup 工具,再总结成要点。
	researcher, err := agent.New(
		agent.WithName("researcher"),
		agent.WithModel(model),
		agent.WithInstruction(
			"你是调研员。先用 kb_lookup 工具查询主题资料,再把结果整理成 3 条一句话要点。"+
				"只输出要点,不要解释你的步骤。"),
		agent.WithTools(knowledgeBaseTool()),
	)
	must(err)

	// 子 agent B:计算器。配了一个「算术」工具。
	// 它会把自然语言里的数字关系转成对 calc 工具的调用,再报告结果。
	calculator, err := agent.New(
		agent.WithName("calculator"),
		agent.WithModel(model),
		agent.WithInstruction(
			"你是计算器助手。遇到需要算的地方就调用 calc 工具,最后用一句话报告数值结论。"),
		agent.WithTools(arithmeticTool()),
	)
	must(err)

	// 子 agent C:撰稿人。没有工具,纯文本加工。
	writer, err := agent.New(
		agent.WithName("writer"),
		agent.WithModel(model),
		agent.WithInstruction(
			"你是撰稿人。把给你的素材改写成一段通顺、80 字以内的中文简介。"),
	)
	must(err)

	// ──────────────────────────────────────────────────────────────────
	// 第 2 步:构建【主 agent / 编排者】,把三个子 agent 用 AsTool 包成工具挂上。
	//
	// 主 agent 不直接干活,而是像项目经理一样:决定先调研、再计算、最后成文,
	// 依次调用对应的子 agent 工具。每次调用,子 agent 都在隔离 context 里完成,
	// 只把结论回传。主 agent 的 context 里只会出现:
	//     research 的结论 / calc 的结论 / write 的结论 / 自己的最终答复
	// 而【看不到】子 agent 内部对 kb_lookup、calc 的调用。
	// ──────────────────────────────────────────────────────────────────
	orchestrator, err := agent.New(
		agent.WithName("orchestrator"),
		agent.WithModel(model),
		agent.WithInstruction(
			"你是主笔编排者。完成用户任务的流程:\n"+
				"1) 调用 research 工具调研主题;\n"+
				"2) 如果任务里有需要计算的数字,调用 calc 工具算出来;\n"+
				"3) 调用 write 工具,把调研要点和计算结果合成一段简介;\n"+
				"4) 输出 write 返回的成品。"),
		agent.WithTools(
			// AsTool 的三个参数:子 agent、工具名(给模型看的)、工具描述(给模型判断何时用)。
			agent.AsTool(researcher, "research", "把一个调研任务交给隔离的调研子 agent,返回 3 条要点"),
			agent.AsTool(calculator, "calc", "把一个计算任务交给隔离的计算子 agent,返回数值结论"),
			agent.AsTool(writer, "write", "把素材交给隔离的撰稿子 agent,返回润色后的简介"),
		),
	)
	must(err)

	// ──────────────────────────────────────────────────────────────────
	// 实验一:跑一个真实编排任务,观察主 agent 如何依次调度多个子 agent。
	//
	// 我们流式订阅主 agent 的事件:
	//   - core.ToolStarted:主 agent【开始】调用某个子 agent 工具。
	//   - core.ToolDone   :某个子 agent 【返回最终文本】(隔离边界 —— 我们只
	//                       看得到结论,看不到它内部用了 kb_lookup / calc)。
	//   - core.MessageDone:主 agent 自己产出文本(最终答复)。
	// ──────────────────────────────────────────────────────────────────
	section("实验一:主 agent 编排 research → calc → write 三个隔离子 agent")
	task := "为「一打鸡蛋的总价」写一句科普简介:一打是 12 个,每个 2.5 元。" +
		"请先调研「一打(dozen)」这个计量单位,再算出总价,最后合成简介。"
	fmt.Println("👤 用户任务:", task)
	fmt.Println()

	for ev, err := range orchestrator.Stream(ctx, task).Iter() {
		must(err)
		switch e := ev.(type) {
		case core.ToolStarted:
			fmt.Printf("   → 主 agent 派活给子 agent [%s]\n", e.Call.Name)
		case core.ToolDone:
			// 这里拿到的,只是子 agent 的最终文本 —— 这就是「独立 context」的外在表现:
			// 子 agent 的中间步骤被关在它自己的 context 里,没有回流到主 agent。
			fmt.Printf("   ← 子 agent [%s] 交回结论:%s\n", e.Result.Name, oneLine(text(e.Result.Content)))
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("\n🤖 主 agent 最终成品:", t)
			}
		}
	}

	// ──────────────────────────────────────────────────────────────────
	// 实验二:用一个计数器【直接证明】子 agent 的 context 是独立的(无记忆)。
	//
	// 我们直接(不经过主 agent)对同一个 calculator 子 agent 连续提两次问。
	// 如果它们【共享 context】,第二次它应该「记得」第一次说过的话;
	// 但因为 AsTool / Run 每次都开全新 State,第二次它是一张白纸 ——
	// 这正是隔离 context 的本质。tracker 计数器还顺带说明:每一次子 agent 调用
	// 都是一次完整、独立的内部运行。
	// ──────────────────────────────────────────────────────────────────
	section("实验二:直接连问同一个子 agent 两次 —— 证明两次 context 互不相通")

	probe, calls := memoryProbeAgent(model) // calls 记录该子 agent 内部工具被调了几次

	fmt.Println("第 1 次提问:记住数字 42。")
	ans1, err := probe.Run(ctx, "请把数字 42 记在心里,然后只回复『已记住』。")
	must(err)
	fmt.Println("   子 agent 答:", oneLine(ans1))

	fmt.Println("第 2 次提问(全新 context):我刚才让你记的数字是多少?")
	ans2, err := probe.Run(ctx, "我上一条消息让你记的数字是几?如果你不知道,就直说『没有上下文』。")
	must(err)
	fmt.Println("   子 agent 答:", oneLine(ans2))

	fmt.Printf("\n说明:两次 Run 的 context 完全独立,第二次看不到第一次的『42』。\n")
	fmt.Printf("      (该子 agent 内部工具累计被调用 %d 次,每次 Run 都是独立的内部流程)\n", calls.Load())
}

// ========================================================================
// 子 agent 用到的自定义工具(用 tool.New 由强类型函数自动生成 JSON Schema)。
// ========================================================================

// knowledgeBaseTool 是调研员子 agent 的「知识库」工具(此处用假数据模拟外部检索)。
func knowledgeBaseTool() tool.Tool {
	return tool.New("kb_lookup", "在内部知识库里按关键词查询资料",
		func(_ *tool.Context, in struct {
			Query string `json:"query" desc:"要查询的关键词或主题"`
		}) (string, error) {
			// 真实项目里这里会查数据库 / 向量库 / API。演示用静态数据。
			kb := map[string]string{
				"dozen": "dozen(一打)是英美计量单位,固定等于 12 个,常用于鸡蛋、面包等;源自古法语 dozaine。",
				"一打":    "「一打」即 dozen,固定为 12 个;半打为 6 个;一罗(gross)为 12 打即 144 个。",
			}
			for k, v := range kb {
				if strings.Contains(in.Query, k) {
					return v, nil
				}
			}
			return "未在知识库中找到「" + in.Query + "」的资料。", nil
		})
}

// arithmeticTool 是计算器子 agent 的「算术」工具,支持四则运算。
func arithmeticTool() tool.Tool {
	return tool.New("calc", "对两个数做一次四则运算",
		func(_ *tool.Context, in struct {
			A  float64 `json:"a" desc:"第一个操作数"`
			Op string  `json:"op" desc:"运算符,取值之一:+ - * /"`
			B  float64 `json:"b" desc:"第二个操作数"`
		}) (string, error) {
			var r float64
			switch in.Op {
			case "+":
				r = in.A + in.B
			case "-":
				r = in.A - in.B
			case "*":
				r = in.A * in.B
			case "/":
				if in.B == 0 {
					return "", fmt.Errorf("除数不能为 0")
				}
				r = in.A / in.B
			default:
				return "", fmt.Errorf("不支持的运算符 %q,只支持 + - * /", in.Op)
			}
			return fmt.Sprintf("%g %s %g = %g", in.A, in.Op, in.B, r), nil
		})
}

// memoryProbeAgent 造一个带「记事本」工具的子 agent,并返回一个原子计数器,
// 用来统计它内部工具的总调用次数 —— 借此说明每次 Run 都是独立的内部运行。
func memoryProbeAgent(model llm.Model) (*agent.Agent, *atomic.Int64) {
	var calls atomic.Int64
	notepad := tool.New("note", "把一段内容写进当前这次运行的记事本",
		func(_ *tool.Context, in struct {
			Content string `json:"content" desc:"要记下的内容"`
		}) (string, error) {
			calls.Add(1) // 每次被调用 +1;跨 Run 不清零,但每个 Run 的 State 是新的
			return "已记下:" + in.Content, nil
		})
	a, err := agent.New(
		agent.WithName("memory_probe"),
		agent.WithModel(model),
		agent.WithInstruction("你是个简短的助手。需要记东西时可用 note 工具。回答尽量短。"),
		agent.WithTools(notepad),
	)
	must(err)
	return a, &calls
}

// ========================================================================
// 模型构建 + 小工具函数(与 examples 里其它例子保持一致)。
// ========================================================================

func buildModel() (llm.Model, bool) {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		return nil, false
	}
	base := envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	model := envOr("AGNES_MODEL", "gemini-2.5-flash")
	return openaicompat.Agnes(base, model, key), true
}

// text 从工具结果的 Part 列表里取出第一段文本。
func text(parts []core.Part) string {
	if len(parts) > 0 {
		if t, ok := parts[0].(core.Text); ok {
			return t.Text
		}
	}
	return ""
}

// oneLine 把多行文本压成一行,便于在事件流里单行展示。
func oneLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " …"
	}
	return s
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }
