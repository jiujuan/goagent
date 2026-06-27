// Command agent-tutorial is a hands-on tour of goagent's public agent API,
// driven by a real Agnes LLM (OpenAI-compatible chat endpoint).
//
// It walks, section by section, through everything you need to build agents:
//
//  1. New + Run            最简一问一答
//  2. WithTools            让模型调用工具
//  3. Stream + Iter        流式消费事件(逐 token)
//  4. WithMiddleware       中间件:加逻辑(观测)/ 改逻辑(改写请求·改控制流)
//  5. HITL                 危险工具人工审批:暂停 → Decide → Resume
//  6. OnThread             多轮会话:同一线程累积上下文
//  7. Steer                运行中插话(steering)
//
// Run it:
//
//	export AGNES_API_KEY=sk-...                       # 必填
//	export AGNES_MODEL=gemini-2.5-flash               # 你的 Agnes 聊天模型 id(可选)
//	export AGNES_BASE_URL=https://apihub.agnes-ai.com/v1   # 可选,默认即此
//	go run ./examples/agent-tutorial
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出(不报错)。
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
	"github.com/jiujuan/goagent/tool"
)

func main() {
	model, ok := buildModel()
	if !ok {
		fmt.Println("未检测到 AGNES_API_KEY。请先设置环境变量:")
		fmt.Println("  export AGNES_API_KEY=sk-...")
		fmt.Println("  export AGNES_MODEL=<你的 Agnes 聊天模型 id>")
		fmt.Println("  go run ./examples/agent-tutorial")
		return
	}
	ctx := context.Background()

	section("1. New + Run —— 最简一问一答")
	demoRun(ctx, model)

	section("2. WithTools —— 让模型调用工具")
	demoTools(ctx, model)

	section("3. Stream + Iter —— 流式消费事件")
	demoStream(ctx, model)

	section("4. WithMiddleware —— 加逻辑 / 改逻辑")
	demoMiddleware(ctx, model)

	section("5. HITL —— 危险工具人工审批")
	demoHITL(ctx, model)

	section("6. OnThread —— 多轮会话累积上下文")
	demoThread(ctx, model)

	section("7. Steer —— 运行中插话")
	demoSteer(ctx, model)
}

// --- 1. New + Run -----------------------------------------------------------

// 最简形态:agent.New(...选项) 造一个 agent,a.Run(ctx, 问题) 阻塞返回答案文本。
func demoRun(ctx context.Context, model llm.Model) {
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个简洁的助手,一句话作答。"),
	)
	if err != nil {
		log.Fatal(err)
	}
	answer, err := a.Run(ctx, "用一句话解释什么是 AI Agent。")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", answer)
}

// --- 2. WithTools -----------------------------------------------------------

// 用 tool.New 定义类型化工具(JSON Schema 由参数结构体反射生成)。模型会自行决定
// 调用它;agent loop 自动:模型→调工具→把结果回喂模型→出最终答案。
func demoTools(ctx context.Context, model llm.Model) {
	weather := tool.New("get_weather", "查询某城市当前天气",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"城市名,如 北京"`
		}) (string, error) {
			// 真实场景这里会调天气 API;教程里返回假数据。
			return in.City + ":晴,25°C,东南风 2 级", nil
		})

	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("需要实时天气时调用 get_weather 工具,再用中文作答。"),
		agent.WithTools(weather),
	)
	answer, err := a.Run(ctx, "北京今天天气怎么样?适合穿什么?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", answer)
}

// --- 3. Stream + Iter -------------------------------------------------------

// a.Stream 返回 *Run 句柄;run.Iter() 是一个 iter.Seq2[core.Event, error],
// 用原生 for-range 消费。开 llm.WithStream(true) 后能收到逐 token 的 MessageDelta。
func demoStream(ctx context.Context, model llm.Model) {
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个讲故事的人。"),
		agent.WithModelOptions(llm.WithStream(true)), // 打开 SSE 流式
	)
	fmt.Print("🤖 ")
	for ev, err := range a.Stream(ctx, "用三句话讲个关于程序员的小故事。").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.MessageDelta: // 增量:逐 token 打印
			fmt.Print(e.Delta.Text())
		case core.RunDone:
			fmt.Println()
		}
	}
}

// --- 4. WithMiddleware ------------------------------------------------------

// 中间件钩在 loop 的每个相位上,分两种力量:
//
//	加逻辑(观测,不改流程):见 logMW —— 在 AfterModel/AfterTool 打日志。
//	改逻辑:① 改写请求(ModifyRequest 直接改 *llm.Request,如注入背景/压缩历史);
//	       ② 改控制流(任意钩子返回非 Continue 的 Directive,如 Stop/Interrupt)。
//
// 这里 logMW 演示"加逻辑",budgetMW 演示"改控制流"(超步数即 Stop)。
func demoMiddleware(ctx context.Context, model llm.Model) {
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个助手。"),
		agent.WithMiddleware(logMW{}, budgetMW{maxSteps: 3}),
	)
	answer, err := a.Run(ctx, "简短介绍一下 Go 语言的并发模型。")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", answer)
}

// logMW:加逻辑 —— 只观测,所有钩子都返回 Continue(零值 Directive)。
type logMW struct{ agent.BaseMiddleware }

func (logMW) AfterModel(lc *agent.LoopContext, r *llm.Response) (core.Directive, error) {
	fmt.Printf("   [log] step %d 模型已响应 (tokens=%v)\n", lc.Step, r.Usage)
	return core.Directive{}, nil
}
func (logMW) AfterTool(_ *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	fmt.Printf("   [log] 工具 %s 完成 (err=%v)\n", tr.Name, tr.IsError)
	return core.Directive{}, nil
}

// budgetMW:改逻辑(改控制流)—— 超过 maxSteps 就返回 Stop 结束运行。
type budgetMW struct {
	agent.BaseMiddleware
	maxSteps int
}

func (m budgetMW) AfterModel(lc *agent.LoopContext, _ *llm.Response) (core.Directive, error) {
	if lc.Step+1 >= m.maxSteps {
		fmt.Printf("   [budget] 到达步数上限 %d,停止。\n", m.maxSteps)
		return core.Directive{Kind: core.Stop}, nil
	}
	return core.Directive{}, nil
}

// --- 5. HITL ----------------------------------------------------------------

// 危险工具人工审批闭环:guardMW 在 BeforeTool 对受保护工具返回 Interrupt → loop
// 落 Pending 快照并发 Interrupted 事件、暂停 → 我们 run.Decide(Allow/Reject) →
// run.Resume(ctx) 续跑(批准则执行工具,拒绝则把原因回喂模型,模型可改道)。
func demoHITL(ctx context.Context, model llm.Model) {
	deleteFile := tool.New("delete_file", "删除服务器上的一个文件",
		func(_ *tool.Context, in struct {
			Path string `json:"path" desc:"要删除的文件绝对路径"`
		}) (string, error) {
			return "已删除 " + in.Path, nil // 教程不真的删
		})

	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("用户要求删除文件时,调用 delete_file 工具。"),
		agent.WithTools(deleteFile),
		agent.WithMiddleware(guardMW{protect: "delete_file"}),
	)

	run := a.Stream(ctx, "请删除 /var/data/old.log", agent.OnThread("hitl-demo"))

	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
			for _, p := range pending {
				fmt.Printf("   ⏸️  待审批:%s(args=%s)\n", p.Tool, string(p.Args))
			}
		}
	}
	if len(pending) == 0 {
		fmt.Println("   (模型这次没有调用受保护工具)")
		return
	}

	// 人类决策:对每个待审批调用做决定,CallID 取自事件(不要写死)。这里演示"拒绝"
	// 并把理由回喂模型;改成 run.Decide(agent.Allow(p.CallID)) 即批准执行该工具。
	for _, p := range pending {
		run.Decide(agent.Reject(p.CallID, "生产日志禁止删除,请改为归档"))
	}
	cont, err := run.Resume(ctx)
	if err != nil {
		log.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", res.Message.Text())
}

// guardMW:对受保护工具返回 Interrupt,触发 HITL 暂停。
type guardMW struct {
	agent.BaseMiddleware
	protect string
}

func (g guardMW) BeforeTool(_ *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if c.Name == g.protect {
		return core.Directive{Kind: core.Interrupt, Reason: "需要人工批准"}, nil
	}
	return core.Directive{}, nil
}

// --- 6. OnThread ------------------------------------------------------------

// 同一个 OnThread(id) 下,状态与 checkpoint 跨多次调用累积:第二问能引用第一问。
func demoThread(ctx context.Context, model llm.Model) {
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个助手,记住对话上下文。"),
	)
	const thread = "chat-1"

	a1, _ := a.Run(ctx, "我叫小明,在学 Go。", agent.OnThread(thread))
	fmt.Println("🤖", a1)
	a2, _ := a.Run(ctx, "我叫什么?在学什么?", agent.OnThread(thread))
	fmt.Println("🤖", a2) // 应能答出"小明 / Go"
}

// --- 7. Steer ---------------------------------------------------------------

// run.Steer 从另一处把消息注入正在跑的运行,下一次模型调用前生效。多用于"边跑边
// 纠偏"。这里在订阅事件的同时插一句风格要求。
func demoSteer(ctx context.Context, model llm.Model) {
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个助手。"),
	)
	run := a.Stream(ctx, "写一段关于大海的描写。")
	run.Steer(core.UserText("请改用更简短、口语化的风格。")) // 插话
	res, err := run.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", res.Message.Text())
}

// --- helpers ----------------------------------------------------------------

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

func section(title string) {
	fmt.Printf("\n========== %s ==========\n", title)
}
