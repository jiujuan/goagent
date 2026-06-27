// Command middleware-hooks makes "loop-hook form" concrete: phaseTracer
// implements ALL SIX agent.Middleware hooks, each printing when the loop calls
// it. Run it (offline, mock model) to literally see the loop's phases fire in
// order across turns — that is what "middleware as loop hooks" means.
//
//	go run ./examples/middleware-hooks
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// phaseTracer implements the full agent.Middleware interface and prints at each
// hook, so you can see exactly where in the loop each phase fires and what a
// real middleware (RAG, RateLimit, Permission, ...) would do there.
type phaseTracer struct{}

func (phaseTracer) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	fmt.Printf("[turn %d] ① BeforeModel    — 模型调用前(RateLimit 限速 / 排空 steering)\n", lc.Step)
	return core.Directive{}, nil
}
func (phaseTracer) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	fmt.Printf("[turn %d] ② ModifyRequest  — 改写本次请求(RAG 注入背景 / Compaction 压缩历史);现有 %d 条消息\n", lc.Step, len(req.Messages))
	return nil
}
func (phaseTracer) AfterModel(lc *agent.LoopContext, r *llm.Response) (core.Directive, error) {
	fmt.Printf("[turn %d] ③ AfterModel     — 模型已响应(%d 个工具调用);返回 Stop 可在此结束\n", lc.Step, len(r.Message.ToolCalls()))
	return core.Directive{}, nil
}
func (phaseTracer) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	fmt.Printf("[turn %d] ④ BeforeTool     — 执行工具 %q 前(Permission/HITL 门禁:返回 Interrupt 即暂停)\n", lc.Step, c.Name)
	return core.Directive{}, nil
}
func (phaseTracer) AfterTool(lc *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	fmt.Printf("[turn %d] ⑤ AfterTool      — 工具 %q 完成(isError=%v)\n", lc.Step, tr.Name, tr.IsError)
	return core.Directive{}, nil
}
func (phaseTracer) OnError(lc *agent.LoopContext, err error) (core.Directive, error) {
	fmt.Printf("[turn %d] ⑥ OnError        — 模型出错:%v\n", lc.Step, err)
	return core.Directive{}, nil
}

func main() {
	weather := tool.New("get_weather", "查询天气",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return in.City + ":晴 25°C", nil
		})

	// 第一轮调用工具,第二轮据结果作答 —— 这样能看到带工具的完整相位序列。
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("北京" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"北京"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是天气助手。"),
		agent.WithTools(weather),
		agent.WithMiddleware(phaseTracer{}), // ← 把"钩子"挂到 loop 上
	)
	if err != nil {
		log.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "北京天气?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n最终答案:", answer)
}
