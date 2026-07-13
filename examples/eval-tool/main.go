// Command eval-tool 演示「评估工具执行结果」——而且全程不需要 LLM:工具评估靠确定性
// 规则评分器即可,零成本、可复现,适合放进单元测试 / CI 冒烟。
//
// 这里评估三种工具结果:正常、报错、输出结构不合约。组合 All(NoToolError, JSONSchema):
//
//   - NoToolError :结果未被标记为错误。
//
//   - JSONSchema  :输出 JSON 符合约定结构(必填字段齐、类型对)。
//
//     go run ./examples/eval-tool
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/eval"
	"github.com/jiujuan/goagent/tool"
)

// 约定的工具输出结构:{"city": string, "temp": number}。
var outputSchema = json.RawMessage(`{
	"type":"object",
	"required":["city","temp"],
	"properties":{
		"city":{"type":"string"},
		"temp":{"type":"number"}
	}
}`)

func main() {
	ctx := context.Background()

	// 三个工具,分别模拟:正常 / 报错 / 输出结构不合约。
	good := tool.New("weather_ok", "返回合约内的天气",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return fmt.Sprintf(`{"city":%q,"temp":25.5}`, in.City), nil
		})
	broken := tool.New("weather_err", "总是报错",
		func(_ *tool.Context, _ struct {
			City string `json:"city"`
		}) (string, error) {
			return "", fmt.Errorf("上游天气服务超时")
		})
	malformed := tool.New("weather_bad", "输出缺字段",
		func(_ *tool.Context, _ struct {
			City string `json:"city"`
		}) (string, error) {
			return `{"city":"北京"}`, nil // 缺 temp
		})

	// 工具结果评估器:既要不报错,又要结构合约。
	scorer := eval.All(
		eval.NoToolError{},
		eval.JSONSchema(outputSchema),
	)

	for _, tl := range []tool.Tool{good, broken, malformed} {
		ep := callTool(ctx, tl, `{"city":"北京"}`)
		sc, _ := scorer.Score(ctx, eval.Sample{
			Output: partsText(ep.Result.Content), // JSONSchema 读 Output
			Tool:   &ep,                          // NoToolError 读 Tool
		})
		fmt.Printf("\n工具 %-12s 通过: %v\n", tl.Name(), sc.Passed)
		fmt.Printf("  输出: %s (is_error=%v)\n", partsText(ep.Result.Content), ep.Result.IsError)
		for _, sub := range sc.Sub {
			mark := "✗"
			if sub.Passed {
				mark = "✓"
			}
			fmt.Printf("  - %-12s %s  %s\n", sub.Name, mark, sub.Reason)
		}
	}
}

// callTool 调用一个工具并把结果包装成 eval.ToolEpisode。
func callTool(ctx context.Context, tl tool.Tool, args string) eval.ToolEpisode {
	res, _ := tl.Call(&tool.Context{Context: ctx, CallID: "c1"}, json.RawMessage(args))
	return eval.ToolEpisode{
		Call: core.ToolCall{ID: "c1", Name: tl.Name(), Args: json.RawMessage(args)},
		Result: core.ToolResult{
			CallID:  "c1",
			Name:    tl.Name(),
			Content: res.Content,
			IsError: res.IsError,
		},
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
