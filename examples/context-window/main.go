// Command context-window 是 middleware.Window 的离线对照演示。
//
// 同一段脚本跑五遍，每遍让模型连问十次工具（其中一次返回约 4000 字的巨大输出），
// 并在第 5 步注入一条人写的决定，然后收束。五遍分别是：不装窗口、RecentN、
// SlidingWindow、ImportanceWeighted、Compaction+Window。每遍打印：模型最后一次的
// 实际输入、期间产生的裁剪/压缩事件、以及那条决定是否还在输入里。
//
// 全程 llm/mock，无网络、无密钥、无随机输出，可重复执行：
//
//	go run ./examples/context-window
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// 脚本参数：模型调用次数、注入决定的步号、以及三条预算数字。
const (
	toolTurns   = 10  // 前 10 次模型调用都要调工具
	steerAtStep = 5   // 第 5 步注入这条决定
	windowKeep  = 4   // RecentN 的单元数
	windowTok   = 900 // SlidingWindow / ImportanceWeighted 的 token 预算
)

const decision = "决定：后续统一采用方案 B，先按预算分配，不再讨论方案 A。"

func main() {
	passes := []struct {
		title string
		build func(inj agent.Middleware) []agent.Middleware
	}{
		{"1. 不装窗口（基线）", func(inj agent.Middleware) []agent.Middleware {
			return []agent.Middleware{inj}
		}},
		{"2. RecentN(4)：按单元条数保留末尾", func(inj agent.Middleware) []agent.Middleware {
			return []agent.Middleware{inj, middleware.Window(middleware.WindowOptions{
				Strategy: middleware.RecentN(windowKeep),
			})}
		}},
		{"3. SlidingWindow(900)：按 token 保留连续末尾", func(inj agent.Middleware) []agent.Middleware {
			return []agent.Middleware{inj, middleware.Window(middleware.WindowOptions{
				Strategy: middleware.SlidingWindow(windowTok),
			})}
		}},
		{"4. ImportanceWeighted(900)：按分数保留不连续的高价值单元", func(inj agent.Middleware) []agent.Middleware {
			return []agent.Middleware{inj, middleware.Window(middleware.WindowOptions{
				Strategy: middleware.ImportanceWeighted(middleware.ImportanceOptions{BudgetTokens: windowTok}),
			})}
		}},
		{"5. Compaction(persist) + Window(request)：摘要保语义，窗口兜最后一道上限", func(inj agent.Middleware) []agent.Middleware {
			return []agent.Middleware{inj,
				middleware.Compaction(middleware.CompactionOptions{
					Model: mock.New("summarizer", func(*llm.Request) *llm.Response { return mock.Text("此前对话的摘要。") }),
					// 估算掉到 800 token 以下就不再摘要，所以这一遍的摘要次数比裁剪次数少得多。
					MaxTokens:  800,
					KeepRecent: 3,
					Persist:    true,
				}),
				middleware.Window(middleware.WindowOptions{
					Strategy: middleware.SlidingWindow(windowTok),
				})}
		}},
	}

	for _, p := range passes {
		runPass(p.title, p.build)
	}
	fmt.Println("读法见文件头注释；三条策略的计量差别见 middleware.WindowStrategy 的注释。")
}

// runPass 跑一遍并把打印全部放在主流程里：模型回调运行在另一个 goroutine，
// 在回调里打印会与这里的事件消费输出交错。
func runPass(title string, build func(agent.Middleware) []agent.Middleware) {
	var lastSent []core.Message
	calls := 0
	conv := mock.New("model", func(req *llm.Request) *llm.Response {
		calls++
		lastSent = req.Messages
		n := calls
		if n <= toolTurns {
			name := "fetch"
			if n == 3 {
				name = "dump" // 那一条约 4000 字的输出
			}
			return mock.CallTool(fmt.Sprintf("call-%d", n), name, "{}")
		}
		return mock.Text("收尾完成。")
	})

	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithTools(fetchTool(), dumpTool()),
		agent.WithMiddleware(build(&steerer{step: steerAtStep})...),
		agent.WithMaxTurns(toolTurns+4),
	)
	if err != nil {
		panic(err)
	}

	run := a.Stream(context.Background(), "怎么给这次迁移定预算？")
	var trimmed []core.WindowTrimmed
	var compacted []core.HistoryCompacted
	for ev, err := range run.Iter() {
		if err != nil {
			panic(err)
		}
		switch e := ev.(type) {
		case core.WindowTrimmed:
			trimmed = append(trimmed, e)
		case core.HistoryCompacted:
			compacted = append(compacted, e)
		}
	}
	if _, err := run.Wait(); err != nil {
		panic(err)
	}

	fmt.Printf("\n== %s\n", title)
	fmt.Printf("   模型最后一次输入：%d 条消息、%d 字符，其中巨大工具输出 %d 条\n",
		len(lastSent), countChars(lastSent), countBig(lastSent))
	fmt.Printf("   那条决定是否在输入里：%s\n", yesNo(hasDecision(lastSent)))
	if len(trimmed) == 0 {
		fmt.Println("   WindowTrimmed：无")
	} else {
		fmt.Printf("   WindowTrimmed：%d 条，列前 3 条\n", len(trimmed))
		for i, e := range trimmed {
			if i >= 3 {
				break
			}
			fmt.Printf("     step %d 策略 %s 丢弃 %d 保留 %d 裁剪前估算 %d token\n",
				e.Step, e.Strategy, e.Dropped, e.Kept, e.EstTokens)
		}
	}
	for _, e := range compacted {
		fmt.Printf("   HistoryCompacted：step %d 摘要掉 %d 条、留下 %d 条（摘要前的估算 %d token）\n",
			e.Step, e.Dropped, e.Kept, e.EstTokens)
	}
}

// steerer 在指定 step 注入一条人为决定，位置由循环的阶段顺序决定，因此可重复。
type steerer struct {
	agent.BaseMiddleware
	step int
	done bool
}

func (s *steerer) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	if !s.done && lc.Step == s.step {
		s.done = true
		lc.Steer(core.UserText(decision))
	}
	return core.Directive{}, nil
}

func fetchTool() tool.Tool {
	return tool.New("fetch", "取一小段数据", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("y", 400), nil
	})
}

func dumpTool() tool.Tool {
	return tool.New("dump", "取一大段数据", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("z", 4000), nil
	})
}

// hasDecision 检查那条决定是否在给定消息集里。
func hasDecision(msgs []core.Message) bool {
	for _, m := range msgs {
		if strings.Contains(m.Text(), decision) {
			return true
		}
	}
	return false
}

// countBig 数有几条 4000 字级别的工具结果被发出去。
func countBig(msgs []core.Message) int {
	n := 0
	for _, m := range msgs {
		for _, p := range m.Parts {
			tr, ok := p.(core.ToolResult)
			if !ok {
				continue
			}
			for _, c := range tr.Content {
				if t, ok := c.(core.Text); ok && strings.Contains(t.Text, strings.Repeat("z", 500)) {
					n++
				}
			}
		}
	}
	return n
}

// countChars 统计发给模型的文本总量（含工具结果内容），用来把"规模"读成一个数。
func countChars(msgs []core.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Text())
		for _, p := range m.Parts {
			tr, ok := p.(core.ToolResult)
			if !ok {
				continue
			}
			for _, c := range tr.Content {
				if t, ok := c.(core.Text); ok {
					n += len(t.Text)
				}
			}
		}
	}
	return n
}

func yesNo(b bool) string {
	if b {
		return "在"
	}
	return "不在（已被裁掉）"
}
