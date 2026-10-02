// Command arg-guard 是 middleware.ArgGuard 的离线对照演示。
//
// 同一段"每次参数都不太对"的脚本跑两遍：一遍只装 LoopGuard，看它为什么不动作；
// 一遍装 ArgGuard，看它按工具名计数、给一次带 required 清单的告警、到阈值升级，
// 以及人工放行之后干预预算怎么把run收尾。全程 llm/mock，无网络、无密钥。
//
//	go run ./examples/arg-guard
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// 演示用的三个数：告警阈值、升级阈值、一次 run 能花掉的干预次数。
const (
	warnAt   = 3
	escalate = 5
	budget   = 2
)

// maxTurns 是两遍共用的步数上限。它也是"没有东西拦停"那一遍最终的出口：用满步数，
// 抛 ErrMaxTurnsExceeded，既没有结果也多花了 token。
const maxTurns = 8

const threadID = "arg-guard-demo"

// badArgs 是模型轮着发的三种坏参数：缺 q、缺 limit、q 的类型不对。
var badArgs = []string{
	`{"limit":5}`,
	`{"q":"cats"}`,
	`{"q":7,"limit":5}`,
}

type lookupTool struct{ runs *int }

func (l lookupTool) Name() string        { return "lookup" }
func (l lookupTool) Description() string { return "按关键词查资料" }
func (l lookupTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"},"limit":{"type":"integer"}},"required":["q","limit"]}`)
}

func (l lookupTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	*l.runs++
	return tool.TextResult("查到了"), nil
}

func main() {
	fmt.Println("工具 lookup 的 schema 有两个必填字段：q（string）、limit（integer）。")
	fmt.Println("模型每一步都发一个坏参数调用，错法和取值都在变；它的 handler 一次都不该被调到。")

	pass("A. 只装 LoopGuard：三种错法轮着来，它两条判据都不满足", middleware.LoopGuard(middleware.LoopGuardOptions{}))
	pass("B. 只装 ArgGuard：同一段脚本，按工具名数到阈值", middleware.ArgGuard(middleware.ArgGuardOptions{
		WarnThreshold:     warnAt,
		EscalateThreshold: escalate,
		MaxInterventions:  budget,
	}))

	fmt.Println(`
五条读法：
  1. ArgGuard 数的是"这个工具被拒了几次"，不看参数长什么样。模型反复修正参数这件事本身
     就是它要拦的对象，而按调用签名或按错误首行计数，会看着它一次次归零（看 A 那段）。
  2. 告警里那份 required 清单取自这一步真正广告给模型的工具表，取不到就整句省掉。缺必填
     字段是最常见的一种错法，把字段名直接念出来比"参数不合法"有用得多。
  3. 升级不在这个中间件里做：它只在 State.KV 上留一个标记，下一步的 BeforeTool 才把它变成
     Interrupt 或 Stop。所以被拦下的永远是下一条调用，本条的结果照常写进历史。
  4. 预算管的是"人工放行能不能把这条路走完"。每一段放行都花一次干预，花完之后再犯就直接
     终止，不然"中断—放行—再中断"本身会变成新的循环。阈值定在哪由你判断模型还有没有救：
     告警越早越吵，升级越早越容易在人还没看清时就打断。
  5. 行末的 step 是每一段 run 自己的序号：放行回来的那一批仍标着它暂停时的步数，之后的
     步数从 0 重新数。要看的其实是 Count——"这个工具累计被拒了几次"，它跨段、跨进程都
     接着涨。`)
}

// pass 跑一遍：装一个中间件，让模型连发坏参数，把它看得见每一步打印出来。遇有人工
// 决定就放行并继续，所以第二遍能走完整个阶梯。
func pass(title string, mw agent.Middleware) {
	fmt.Printf("\n=== %s ===\n", title)

	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs, step := 0, 0

	// 告警文案由模型的响应函数取出，交给主流程打印：循环跑在另一个 goroutine 里，
	// 谁先落笔并不固定，而这份输出要能一遍遍对得上。告警只发一次，但它留在历史里，
	// 所以只报第一次看见的那次。
	var injected string
	markerSeen := false

	model := mock.New("stubborn", func(req *llm.Request) *llm.Response {
		if !markerSeen {
			if txt := firstMarker(req.Messages, middleware.WarnMarkerArg); txt != "" {
				injected, markerSeen = txt, true
			}
		}
		args := withAttempt(badArgs[step%len(badArgs)], step)
		step++
		return mock.CallTool(fmt.Sprintf("c%d", step), "lookup", args)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(lookupTool{runs: &runs}),
		agent.WithMiddleware(mw),
		agent.WithCheckpointer(store),
		agent.WithMaxTurns(maxTurns),
	)
	if err != nil {
		log.Fatal(err)
	}

	refusals, detected := 0, 0
	run := a.Stream(ctx, "帮我查一下", agent.OnThread(threadID))
	for wave := 1; ; wave++ {
		terminal := "未结束"
		var pending []core.ApprovalRequest
		// 每段先把这一波的行攒起来，末了连同告警一起打印：告警的正文要到下一次模型
		// 请求才被取到，而它在时间上紧跟着第 warnAt 次被拒，所以要插回那个位置。
		var lines []string
		warnRow := -1
		for ev, err := range run.Iter() {
			if err != nil {
				terminal = "失败：" + err.Error()
				continue
			}
			switch e := ev.(type) {
			case core.ArgRejected:
				detected++
				lines = append(lines, fmt.Sprintf("  [拒绝 ] 第 %d 次调用 lookup（step %d，%s）", e.Count, e.Step, e.Class))
				if e.Count == warnAt {
					warnRow = len(lines) - 1
				}
			case core.StuckDetected:
				detected++
				lines = append(lines, fmt.Sprintf("  [卡死 ] step %d，rule=%s：%s", e.Step, e.Rule, e.Reason))
			case core.ToolDone:
				if e.Result.IsError {
					refusals++ // 每条被拒的调用都会给模型回一条错误结果
				}
			case core.Interrupted:
				pending = e.Pending
				terminal = fmt.Sprintf("停在人工决定：%d 条待批（%s）", len(e.Pending), e.Pending[0].Tool)
			case core.RunDone:
				if txt := strings.TrimSpace(e.Result.Message.Text()); txt != "" {
					terminal = "正常结束：" + txt
				} else {
					terminal = "被直接终止：这一步的调用还没执行就 Stop 了，所以没有答案"
				}
			case core.RunFailed:
				terminal = "失败：" + e.Err.Error()
			}
		}
		if injected != "" {
			lines = insertAfter(lines, warnRow, fmt.Sprintf(
				"  [注入 ] 下一次的模型请求里带着这条告警：\n            %s", wrap(injected, 74)))
			injected = ""
		}
		for _, l := range lines {
			fmt.Println(l)
		}
		fmt.Printf("  第 %d 段：%s\n", wave, terminal)
		if len(pending) == 0 {
			break
		}
		run.Decide(agent.Allow(pending[0].CallID))
		next, err := run.Resume(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  [人工 ] 放行了 %s，让它接着试\n", pending[0].Tool)
		run = next
	}

	cp, err := store.Latest(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  合计：调用被拒 %d 次，防护记录 %d 条，lookup 的 handler 实际跑了 %d 次\n", refusals, detected, runs)
	fmt.Printf("  State.KV 里防护写下的键：%s\n", guardKeys(cp.State.KV))
}

// insertAfter 把告警正文插到第 warnAt 次被拒那一行之后：那一步的 OnToolReject 既发
// 出这条事件，也写下这条告警，所以两者本来就属于同一刻。
func insertAfter(lines []string, at int, block string) []string {
	if at < 0 || at >= len(lines) {
		return append(lines, block)
	}
	out := make([]string, 0, len(lines)+1)
	out = append(out, lines[:at+1]...)
	out = append(out, block)
	return append(out, lines[at+1:]...)
}

// withAttempt 往参数对象里加一个每次都变大的字段，只为让每次调用的签名互不相同。
// 工具的 schema 没有禁止多余属性，所以这个字段不影响校验结果。
func withAttempt(args string, n int) string {
	return strings.TrimSuffix(args, "}") + fmt.Sprintf(`,"attempt":%d}`, n)
}

// guardKeys 列出 ArgGuard 写进 State.KV 的那几个键，按键名排序，好让这段输出每次跑
// 都对得上。中间件的状态全在这里，而不是在它自己的字段里。
func guardKeys(kv map[string]any) string {
	var names []string
	for k, v := range kv {
		if strings.HasPrefix(k, "_argguard.") {
			names = append(names, fmt.Sprintf("%s=%v", k, v))
		}
	}
	if len(names) == 0 {
		return "（无）"
	}
	sort.Strings(names)
	return strings.Join(names, "; ")
}

// firstMarker 取出历史里第一条带着某个标记的用户消息正文，取不到返回空串。
func firstMarker(msgs []core.Message, marker string) string {
	for _, m := range msgs {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), marker) {
			return m.Text()
		}
	}
	return ""
}

// wrap 只为排版：把一条长文案折行到指定宽度。
func wrap(s string, width int) string {
	words := strings.Fields(s)
	var b strings.Builder
	line := 0
	for _, w := range words {
		switch {
		case line > 0 && line+1+len(w) > width:
			b.WriteString("\n            ")
			line = 0
		case line > 0:
			b.WriteString(" ")
			line++
		}
		b.WriteString(w)
		line += len(w)
	}
	return b.String()
}
