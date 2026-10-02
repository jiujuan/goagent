// Command arg-guard-tool-author 从"写这个工具的人"这个视角演示参数拒绝计数。
//
// middleware.ArgGuard 数的是"循环没执行的那次调用"，所以一个工具被记几次，很大程度
// 取决于写工具的人把哪些错法接在了循环外面。三段对照用的是同一段模型脚本，前三步一字
// 不改：
//
//	1 裸 schema            —— 别名、字符串数字全交给循环拒掉，四次记录挤垮一个工具
//	2 加 PrepareArguments  —— 能接的接住，接不住的那次自己说一句缺什么，只剩两次记录
//	3 handler 自己报服务错  —— 默认不计；把它放进 Count，500 期间字段齐全的调用也会被
//	                          按"参数不合格"的口吻训斥
//
// 阈值这里统一取 WarnThreshold=3、EscalateThreshold=99：只演到告警这一级，升级与人工
// 决定的完整阶梯看 examples/arg-guard。
//
//	go run ./examples/arg-guard-tool-author
//
// 无 API key 依赖：全程使用 llm/mock。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

const (
	warnAt     = 3
	noEscalate = 99 // 本示例只到告警那一级
	maxTurns   = 10
	threadID   = "arg-guard-author"
)

// --- 三个工具 -----------------------------------------------------------------

// bookRoom 只声明 schema，别的一概不管：date(string) 与 room(integer) 都必填。
type bookRoom struct{ runs *int }

func (b bookRoom) Name() string        { return "book_room" }
func (b bookRoom) Description() string { return "按日期和房间号订房" }
func (b bookRoom) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"date":{"type":"string"},"room":{"type":"integer"}},"required":["date","room"]}`)
}

func (b bookRoom) Call(_ *tool.Context, raw json.RawMessage) (*tool.Result, error) {
	var in struct {
		Date string `json:"date"`
		Room int    `json:"room"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	*b.runs++
	return tool.TextResult(fmt.Sprintf("已订 %s 的 %d 号房", in.Date, in.Room)), nil
}

// repairedRoom 是同一个工具多写一层 PrepareArguments：看得懂的错就修，修不了的自己
// 说清缺什么。名字、schema、handler 都照抄 bookRoom，只有这一层是新加的。
type repairedRoom struct{ bookRoom }

func (r repairedRoom) PrepareArguments(raw json.RawMessage) (json.RawMessage, error) {
	var in map[string]any
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err // 连 JSON 都不是，交给循环按原样回一句
	}
	if _, ok := in["date"]; !ok {
		if v, ok := in["checkin"]; ok { // 旧字段名：模型常从别的工具串过来
			in["date"] = v
			delete(in, "checkin")
		}
	}
	if s, ok := in["room"].(string); ok { // "7" 与 7 是同一个房间号
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
			in["room"] = n
		}
	}
	if _, ok := in["room"]; !ok {
		// 这句话会原样进到模型下一步的历史里，所以给它一条出路而不是一句"校验失败"。
		return nil, errors.New(`room 是必填的房间号（整数）；不知道有哪些房，先调 list_rooms`)
	}
	out, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// remoteStatus 的参数每次都合格，失败在下游服务上：handler 返回 Go 错误。
type remoteStatus struct{ runs *int }

func (s remoteStatus) Name() string        { return "remote_status" }
func (s remoteStatus) Description() string { return "查远方服务的状态" }
func (s remoteStatus) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"where":{"type":"string"}},"required":["where"]}`)
}

func (s remoteStatus) Call(_ *tool.Context, raw json.RawMessage) (*tool.Result, error) {
	var in struct {
		Where string `json:"where"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	*s.runs++
	return nil, fmt.Errorf("upstream 503：%s 的状态服务暂时不可用", in.Where)
}

// --- 脚本 ---------------------------------------------------------------------

// step 是模型的一次调用：工具名 + 它给的参数。
type step struct{ toolName, args string }

// groping 是前三段共用的脚本：别名、字符串数字、真缺字段、拼错名字、最后写对。
var groping = []step{
	{"book_room", `{"checkin":"2026-10-05","room":7}`},
	{"book_room", `{"date":"2026-10-05","room":"7"}`},
	{"book_room", `{"date":"2026-10-05"}`},
	{"book_roomd", `{"date":"2026-10-05","room":7}`}, // 名字拼错
	{"book_room", `{"date":"2026-10-05","room":7}`},  // 写对了
}

// outage 的参数每一步都合法，失败全在服务那头。
var outage = []step{
	{"remote_status", `{"where":"paris"}`},
	{"remote_status", `{"where":"paris"}`},
	{"remote_status", `{"where":"berlin"}`},
}

func main() {
	fmt.Println("工具 book_room 的 schema：date(string) 与 room(integer) 都必填；ArgGuard 取 WarnThreshold=3。")
	fmt.Println("1、2 两段跑的是同一段脚本，差别只在工具有没有 PrepareArguments。")

	pass("1. 裸 schema：四种错法全交给循环拒",
		func(runs *int) []tool.Tool { return []tool.Tool{bookRoom{runs}} },
		middleware.ArgGuardOptions{},
		groping)

	pass("2. 加 PrepareArguments：把看得懂的错接在循环外面",
		func(runs *int) []tool.Tool { return []tool.Tool{repairedRoom{bookRoom{runs}}} },
		middleware.ArgGuardOptions{},
		groping)

	pass("3a. 服务 500，Count 用默认范围",
		func(runs *int) []tool.Tool { return []tool.Tool{remoteStatus{runs}} },
		middleware.ArgGuardOptions{},
		outage)

	pass("3b. 同一段脚本，把 handler_error 也计入 Count",
		func(runs *int) []tool.Tool { return []tool.Tool{remoteStatus{runs}} },
		middleware.ArgGuardOptions{
			Count: []agent.RejectClass{agent.RejectSchemaInvalid, agent.RejectHandlerError},
		},
		outage)

	fmt.Println(`
五条写法约定：
  1. 别名、字符串数字、大小写这类"看得懂的错"在 PrepareArguments 里接住。接住了就根本不
     是一次拒绝，账本上不留痕；留给循环拒，每拒一次记一次，阈值是它替你数的。
  2. 接不住时自己写清楚缺什么、下一步能干什么。PrepareArguments 返出的错会被原样回给模型
     （前缀 invalid arguments:），也是告警里 "last reason" 引的那一句；交给循环代答只能说
     required argument "room" is missing，模型读完常常只是换个写法再猜。
  3. required 写全，告警才有可执行的下一步：那半句 "Its required arguments are: ..." 是
     从这一步真正广告给模型的工具表里读出来的，schema 里没有 required 就整段省掉。
  4. 名字拼错的记录挂在拼错的那个名字上，不会并到正主名下。所以账本里冒出陌生的键名，说
     明模型在猜名字——该改的是工具名、描述与工具表，不是阈值。
  5. handler 返回 Go 错误默认不计（Count 只含 unknown_tool / prepare_failed /
     schema_invalid）。把 agent.RejectHandlerError 加进去之前想清楚：服务挂的那几分钟里，
     字段齐全的调用也会被按"参数不合格"的口吻训斥，模型除了重试什么都做不了。要盯服务健
     康去看 obs/otel 的 agent.tool.calls 指标，别借这本账。`)
}

// pass 跑一段：装配一个 agent，让模型按脚本依次调用，把事件流与账本打出来。
func pass(title string, mk func(runs *int) []tool.Tool, opts middleware.ArgGuardOptions, steps []step) {
	fmt.Printf("\n=== %s ===\n", title)

	ctx := context.Background()
	store := checkpoint.NewMemory()
	runs := 0

	opts.WarnThreshold = warnAt
	opts.EscalateThreshold = noEscalate

	// 脚本按"第几次模型调用"分支：历史里从第一步起就带着工具结果，按工具结果分支会恒为真。
	called := 0
	model := mock.New("groping", func(*llm.Request) *llm.Response {
		if called >= len(steps) {
			return mock.Text("我就按现在拿到的信息答复")
		}
		s := steps[called]
		called++
		return mock.CallTool(fmt.Sprintf("c%d", called), s.toolName, s.args)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(mk(&runs)...),
		agent.WithMiddleware(middleware.ArgGuard(opts)),
		agent.WithCheckpointer(store),
		agent.WithMaxTurns(maxTurns),
	)
	if err != nil {
		log.Fatal(err)
	}

	terminal := "未结束"
	var refusals int
	run := a.Stream(ctx, "订 10 月 5 号的房", agent.OnThread(threadID))
	for ev, err := range run.Iter() {
		if err != nil {
			terminal = "失败：" + err.Error()
			continue
		}
		switch e := ev.(type) {
		case core.ArgRejected:
			refusals++
			fmt.Printf("  [拒绝 ] %s 被记第 %d 次（class=%s，step=%d）\n", e.Tool, e.Count, e.Class, e.Step)
		case core.Interrupted:
			terminal = fmt.Sprintf("停在人工决定：%s（本示例不该出现）", e.Pending[0].Tool)
		case core.RunDone:
			terminal = "模型给出答复：" + e.Result.Message.Text()
		case core.RunFailed:
			terminal = "失败：" + e.Err.Error()
		}
	}
	fmt.Printf("  [结局 ] %s；handler 实际跑了 %d 次，被拒 %d 次\n", terminal, runs, refusals)

	cp, err := store.Latest(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	if warn := firstMarker(cp.State.Messages, middleware.WarnMarkerArg); warn != "" {
		fmt.Printf("  [告警 ] 模型在下一步的请求里读到：\n            %s\n", wrap(warn, 76))
	} else {
		fmt.Println("  [告警 ] 没有发出（计数没到阈值）")
	}
	for _, line := range errorTexts(cp.State.Messages) {
		fmt.Printf("  [回写 ] 模型看到的这句：%s\n", line)
	}
	fmt.Printf("  [账本 ] %s\n", ledger(cp.State.KV))
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

// errorTexts 列出历史里每一条报错的工具结果文本，按出现顺序去重：这就是模型逐步读到
// 的那些话。被循环拒掉的调用也在这里，所以 PrepareArguments 写的那句话看得见。
func errorTexts(msgs []core.Message) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			tr, ok := p.(core.ToolResult)
			if !ok || !tr.IsError {
				continue
			}
			var b []string
			for _, c := range tr.Content {
				if t, ok := c.(core.Text); ok {
					b = append(b, t.Text)
				}
			}
			line := tr.Name + ": " + strings.Join(b, " ")
			if !seen[line] {
				seen[line] = true
				out = append(out, line)
			}
		}
	}
	return out
}

// ledger 打印 ArgGuard 写进 State.KV 的键，按键名排序：map 的迭代顺序是随机的，而这份
// 输出要能一遍遍对得上。
func ledger(kv map[string]any) string {
	var names []string
	for k, v := range kv {
		if strings.HasPrefix(k, "_argguard.") {
			names = append(names, fmt.Sprintf("%s=%v", k, v))
		}
	}
	if len(names) == 0 {
		return "（空：一次都没记）"
	}
	sort.Strings(names)
	return strings.Join(names, "; ")
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
