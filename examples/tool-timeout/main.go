// Command tool-timeout 是 agent.WithToolTimeout 与 middleware.ToolTimeout 的离线演示。
//
// 工具循环对被调用的工具原本没有任何时间约束：唯一的取消信号是整个 run 被取消。
// 这里演示三层能力——上限设在哪（agent 默认 / 按工具）、到点后循环怎么做
// （取消 context 并停止等待它）、以及被限住的工具还能不能上报进度。
//
//	go run ./examples/tool-timeout
//
// 无 API key 依赖：全程使用 llm/mock。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// polite 盯着自己的 context，被取消时立刻回报它做到了哪一步。
func polite() tool.Tool {
	return tool.New("polite", "waits, but watches its context",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			select {
			case <-time.After(1200 * time.Millisecond):
				return "跑满 1.2s 后自己完成", nil
			case <-tctx.Done():
				return "", fmt.Errorf("stopped early: %w", tctx.Err())
			}
		})
}

// hang 完全不看 context。Go 无法中止一个已经在跑的函数，所以它只能自己跑完——
// 上限能做的，是让循环不再等它。
func hang() tool.Tool {
	return tool.New("hang", "blocks without watching its context",
		func(_ *tool.Context, _ struct{}) (string, error) {
			time.Sleep(2 * time.Second)
			return "两秒后才醒", nil
		})
}

// progress 一边跑一边上报中间结果，用来演示进度上报不会被上限吃掉。
func progress() tool.Tool {
	return tool.New("progress", "long job that reports partials",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			tctx.Update(core.Text{Text: "已连接"})
			for i := 1; i <= 3; i++ {
				select {
				case <-time.After(200 * time.Millisecond):
					tctx.Update(core.Text{Text: fmt.Sprintf("已完成 %d/3", i)})
				case <-tctx.Done():
					return "", tctx.Err()
				}
			}
			return "all done", nil
		})
}

func main() {
	fmt.Println("== 1. 不设上限：循环只能一直等（工具自己要跑多久就等多久） ==")
	scenario("polite 工具，无上限", 0, nil, "polite")
	scenario("hang 工具，无上限", 0, nil, "hang")

	fmt.Println("\n== 2. agent 默认上限：到点取消 context ==")
	scenario("polite 工具，上限 400ms", 400*time.Millisecond, nil, "polite")

	fmt.Println("\n== 3. 到点之后循环不再等它：hang 工具还在后台跑完它的 2s ==")
	answer := scenario("hang 工具，上限 400ms", 400*time.Millisecond, nil, "hang")
	fmt.Printf("     模型看到的完整一句：\n       %s\n", answer)

	fmt.Println("\n== 4. 按工具设上限（middleware.ToolTimeout）：取更早的那个界限 ==")
	// 不设 agent 默认，只装中间件，同样限得住。
	scenario("hang 工具，只装中间件（Default 300ms）", 0, &middleware.ToolTimeoutOptions{
		Default: 300 * time.Millisecond,
	}, "hang")
	// agent 默认 5s，中间件的 Default 3s 更紧，hang 再被单独压到 300ms：生效的是最早那个。
	scenario("hang 工具，agent 默认 5s + 按工具 300ms", 5*time.Second, &middleware.ToolTimeoutOptions{
		Default: 3 * time.Second,
		PerTool: map[string]time.Duration{"hang": 300 * time.Millisecond},
	}, "hang")
	// Exempt 的那一个工具回到“无上限”，另两个仍受 Default 约束。
	scenario("polite 工具，Default 300ms 但 exempt polite", 0, &middleware.ToolTimeoutOptions{
		Default: 300 * time.Millisecond,
		Exempt:  []string{"polite"},
	}, "polite")

	fmt.Println("\n== 5. 被限住的工具照样能上报进度 ==")
	partials()

	fmt.Println(`
要点：
  - 上限是 per call，不是 per batch；整轮挂钟归 RunBudget.MaxDuration。
  - 到点后循环改口替工具回答，那条迟到结果、它请求的 Control 与 State 全部作废。
  - 派生 context 只能收紧、不能放宽：要给某个工具更多时间，就别设 agent 默认，
    改用 middleware.ToolTimeout 逐个设。`)
}

// scenario 跑一种配置：模型先调用一个工具，再把工具答复原样回述，于是打印出来的
// 最终答案就是模型看到的那句话。
func scenario(title string, bound time.Duration, mwOpts *middleware.ToolTimeoutOptions, toolName string) string {
	opts := []agent.Option{
		agent.WithModel(echoModel(toolName)),
		agent.WithTools(polite(), hang(), progress()),
		agent.WithMaxTurns(3),
	}
	if bound > 0 {
		opts = append(opts, agent.WithToolTimeout(bound))
	}
	if mwOpts != nil {
		opts = append(opts, agent.WithMiddleware(middleware.ToolTimeout(*mwOpts)))
	}

	a, err := agent.New(opts...)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	answer, err := a.Run(ctx, "do it")
	if err != nil {
		answer = "error: " + err.Error()
	}
	fmt.Printf("  %-44s -> %-46s %.1fs\n", title, clip(firstLine(answer), 46), time.Since(start).Seconds())
	return answer
}

// echoModel 第一轮回一个工具调用，第二轮把工具结果回述一遍。
func echoModel(toolName string) *mock.Model {
	return mock.New("echo", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("模型收到: " + firstLine(resultText(tr)))
		}
		return mock.CallTool("c1", toolName, `{}`)
	})
}

// partials 演示进度上报：设了上限之后，core.ToolUpdate 仍然可达。
func partials() {
	a, err := agent.New(
		agent.WithModel(echoModel("progress")),
		agent.WithTools(polite(), hang(), progress()),
		agent.WithToolTimeout(500*time.Millisecond),
		agent.WithMaxTurns(3),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := a.Stream(ctx, "long job")
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolUpdate:
			fmt.Printf("  进度上报 ToolUpdate: %s\n", firstLine(resultTextOf(e.Partial)))
		case core.ToolDone:
			fmt.Printf("  最终结果 ToolDone:    %s\n", firstLine(resultText(e.Result)))
		}
	}
	if _, err := run.Wait(); err != nil {
		log.Fatal(err)
	}
}

func resultText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func resultTextOf(p core.Part) string {
	if t, ok := p.(core.Text); ok {
		return t.Text
	}
	return fmt.Sprintf("%T", p)
}

// clip 只为了排版：演示输出对齐用，截掉的只是显示。
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
