// Command step-view 演示每步冻结的执行视图：中间件在 ModifyRequest 里用同名工具
// 替换静态工具，模型在该步看到的就是注入后的 description 与 parameters，执行也走
// 注入实现；AfterModel 通过 View() 打印本步实际的模型名、广告工具、设置与两个摘要。
//
// 全程 mock 模型，无网络、无随机输出，可重复执行：
//
//	go run ./examples/step-view
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
	"github.com/jiujuan/goagent/tool"
)

// stepViewDemo 在请求定稿时注入同名工具，并在模型响应后读取冻结视图。
type stepViewDemo struct {
	agent.BaseMiddleware
	replacement tool.Tool
}

func (d stepViewDemo) ModifyRequest(lc *agent.LoopContext, _ *llm.Request) error {
	// AddTool 会同时替换本步请求里的同名广告和执行表；视图随后冻结，
	// 因此本步的 schema、后续调用解析和摘要都以注入实现为准。
	lc.AddTool(d.replacement)
	return nil
}

func (d stepViewDemo) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	view := lc.View()
	digest := view.Digest()
	fmt.Printf("\n[步骤 %d] 视图：模型 %q，广告工具 %v\n", lc.Step, view.Model(), view.Tools().Names())
	for _, s := range view.Tools().Schemas() {
		fmt.Printf("  广告 schema：%s — %s\n                参数 %s\n", s.Name, s.Description, s.Parameters)
	}
	fmt.Printf("  设置：system=%q temperature=%.1f\n", view.System(), view.Options().Temperature)
	fmt.Printf("  摘要：tool=%s settings=%s\n", shortDigest(digest.ToolDigest), shortDigest(digest.SettingsDigest))
	fmt.Printf("  本步模型发起的工具调用：%d 个\n", len(resp.Message.ToolCalls()))
	return core.Directive{}, nil
}

func (d stepViewDemo) AfterTool(lc *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	fmt.Printf("[步骤 %d] 工具 %s 的实际返回：%s\n", lc.Step, tr.Name, toolResultText(*tr))
	return core.Directive{}, nil
}

func main() {
	staticLookup := tool.New("lookup", "在静态目录里查一个键",
		func(_ *tool.Context, in struct {
			Key string `json:"key"`
		}) (string, error) {
			return "static:" + in.Key, nil
		})
	injectedLookup := tool.New("lookup", "在注入目录里查一个键（同名运行期实现）",
		func(_ *tool.Context, in struct {
			Key    string `json:"key"`
			Source string `json:"source,omitempty"`
		}) (string, error) {
			return "injected:" + in.Key, nil
		})

	fmt.Printf("静态 lookup：%s\n", staticLookup.Description())
	fmt.Printf("注入 lookup：%s\n", injectedLookup.Description())

	model := mock.New("mock-step-view", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("收尾：" + toolResultText(tr))
		}
		return mock.CallTool("call-1", "lookup", `{"key":"alpha"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个查询助手。"),
		agent.WithModelOptions(llm.WithModel("demo-model"), llm.WithTemperature(0.2)),
		agent.WithTools(staticLookup),
		agent.WithMiddleware(stepViewDemo{replacement: injectedLookup}),
	)
	if err != nil {
		log.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "查一下 alpha")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n最终答案:", answer)
}

func toolResultText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if text, ok := p.(core.Text); ok {
			b.WriteString(text.Text)
		}
	}
	return b.String()
}

func shortDigest(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}
