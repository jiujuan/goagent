// Command tool-timeout-author 从"写工具的人"这个视角演示上限该怎么配合。
//
// agent.WithToolTimeout 与 middleware.ToolTimeout 能做的只有两件事：到点取消工具
// 收到的 context，以及到点之后循环不再等这条调用。Go 不能中止一个已经在跑的函数，
// 所以副作用何时停、停在哪，只有工具自己知道。这个示例把两种写法放在一起对照：
//
//	fetch_rows   —— 每做一步副作用之前先看一次 context，被中断时回报"我做到哪了"
//	legacy_batch —— 完全不看 context，循环放弃等待之后它还在继续写
//
//	go run ./examples/tool-timeout-author
//
// 无 API key 依赖：全程使用 llm/mock。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

const bound = 300 * time.Millisecond

func main() {
	fmt.Printf("每次调用的上限：%v\n", bound)

	watched()
	unwatched()

	fmt.Println(`
三条写法约定：
  1. 每做一步副作用之前先看一次 context。副作用切得越小，被中断时造成的损失越小。
  2. 被中断时把"做到哪了"回报出去，而不是只交出 context.Canceled。模型拿到这句话
     才知道该重试、续做还是改道；循环替你写的那句超时提示，只在工具什么都不说时
     才会出现。
  3. 做不到 1（第三方库内部阻塞、拿不到可中断的循环）就把副作用设计成幂等：同一条
     调用重复执行的结果一样。因为循环放弃等待之后，它是真的还在跑。`)
}

// watched 演示会收尾的工具：被打断时它写下的行数就是最终行数。
func watched() {
	var rows atomic.Int32

	fetcher := tool.New("fetch_rows", "逐行写入，随时可被中断",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			for i := 1; i <= 10; i++ {
				select {
				case <-time.After(80 * time.Millisecond):
					rows.Add(1) // 真实副作用发生在这里
					tctx.Update(core.Text{Text: fmt.Sprintf("已写入 %d 行", i)})
				case <-tctx.Done():
					// 约定 2：说清自己停在哪，而不是只交出错误对象。
					return "", fmt.Errorf("写完 %d 行后中断（第 %d 行未落盘）：%w",
						rows.Load(), i, tctx.Err())
				}
			}
			return "10 行全部写入", nil
		})

	fmt.Println("\n== A. 会收尾的工具：每步之前看 context ==")
	answer := run("fetch_rows", fetcher)
	fmt.Printf("  模型收到 -> %s\n", tail(answer))
	fmt.Printf("  它写下的行数：%d；再等 1s 之后 %d（没变，说明副作用确实止住了）\n",
		rows.Load(), settle(&rows))
}

// unwatched 演示不看 context 的工具：循环已经不等了，它还在往后写。
func unwatched() {
	var rows atomic.Int32

	legacy := tool.New("legacy_batch", "一批写 6 行，中途不看 context",
		func(_ *tool.Context, _ struct{}) (string, error) {
			for i := 1; i <= 6; i++ {
				time.Sleep(150 * time.Millisecond) // 合计 900ms，远超上限
				rows.Add(1)
			}
			return "6 行写完", nil
		})

	fmt.Println("\n== B. 不看 context 的工具：循环只能替它回答 ==")
	answer := run("legacy_batch", legacy)
	fmt.Printf("  模型收到 -> %s\n", tail(answer))
	atRelease := rows.Load()
	fmt.Printf("  run 结束时它已写了 %d 行；再等 1s 之后 %d 行。多出来的部分发生在模型已经\n"+
		"  被告知“结果未知”之后——这就是约定 3 存在的原因。\n", atRelease, settle(&rows))
}

// run 跑一轮最小对话：模型调用一次指定工具，然后把工具的答复原样回述。
func run(toolName string, t tool.Tool) string {
	a, err := agent.New(
		agent.WithModel(echoModel(toolName)),
		agent.WithTools(t),
		agent.WithToolTimeout(bound),
		agent.WithMaxTurns(3),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	answer, err := a.Run(ctx, "do it")
	if err != nil {
		return "error: " + err.Error()
	}
	return answer
}

// echoModel 第一轮回一个工具调用，第二轮把工具结果回述一遍。
func echoModel(toolName string) *mock.Model {
	return mock.New("echo", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("模型收到: " + resultText(tr))
		}
		return mock.CallTool("c1", toolName, `{}`)
	})
}

// settle 等那条已被放弃的调用彻底跑完，再读一次计数。
func settle(rows *atomic.Int32) int {
	time.Sleep(1 * time.Second)
	return int(rows.Load())
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

// tail 去掉 "模型收到: " 这一层前缀，只留工具真正说的那句话，用于打印。
func tail(answer string) string {
	line := answer
	if i := strings.Index(line, ": "); i >= 0 {
		line = line[i+2:]
	}
	return firstLine(line)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
