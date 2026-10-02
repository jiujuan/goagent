// Command arg-guard-ops 从"看着这条 run 的人"这个视角演示参数拒绝计数。
//
// middleware.ArgGuard 的整本账都在事件流和 State.KV 里，不在它的字段里，所以运维侧要读
// 的就是这两处。三段各读一样东西：
//
//	1 事件流   —— core.ArgRejected 编成一行 JSON 日志，四个字段就是仪表盘要聚合的
//	2 账本形状 —— 同一个键，从内存读回 int、从 JSONL 读回 float64；写错形状会静默归零
//	3 重启与放行 —— 进程换了一个计数照旧，告警不会重发，预算在升级那一刻扣、否决不计
//
//	go run ./examples/arg-guard-ops
//
// 无 API key 依赖：全程使用 llm/mock；落盘只写本程序自建的临时目录，跑完删掉。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

const (
	warnAt     = 2 // 记到第 2 次发告警
	escalateAt = 3 // 记到第 3 次写标记，下一步拦下来
	budget     = 3 // 一段 run 允许几次升级
	maxTurns   = 8
	threadOps  = "arg-guard-ops"
)

// bad 是模型的三连坏参数，错法各不相同：缺 q、q 类型不对、缺 limit。
var bad = []string{`{"limit":5}`, `{"q":7,"limit":5}`, `{"q":"cats"}`}

type lookupTool struct{ runs *int }

func (l lookupTool) Name() string        { return "lookup" }
func (l lookupTool) Description() string { return "按关键词查资料" }
func (l lookupTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"},"limit":{"type":"integer"}},"required":["q","limit"]}`)
}

func (l lookupTool) Call(_ *tool.Context, raw json.RawMessage) (*tool.Result, error) {
	var in struct {
		Q string `json:"q"`
	}
	_ = json.Unmarshal(raw, &in)
	*l.runs++
	return tool.TextResult("查到了：" + in.Q), nil
}

func main() {
	logLines()
	ledgerShapes()
	restartAndDecisions()

	fmt.Println(`
五条读法约定：
  1. 聚合按 (thread, tool) 看 count 的增长速度，不要按 step 看：count 是该工具跨步累计，
     step 只是这一段 run 自己的序号，resume 之后从 0 重数。单条事件说明不了什么，一条越来
     越密的序列才是"模型在猜 schema"。
  2. 读 State.KV 里的数字要按"可能是 float64"写。同一次落盘，内存 checkpointer 原样交出
     int，JSONL 读回来一律是 float64；直接 v.(int) 在后者上恒失败、读成 0，于是仪表盘安静
     地显示"一切正常"。
  3. 中间件往 KV 里只能写纯 map（键值都可 JSON 化）。带私有字段的 struct marshal 出来是
     {}，计数会静默归零——这正是 ArgGuard 把账本逐字段摊平成 map 的原因。
  4. 待批的那条未必是坏参数那条：标记写在被拒那一步，被拦下的永远是下一条调用，所以人看
     到的可能是个参数完全正确的调用。而预算在写标记那一刻就扣掉了，跟你随后放行还是否决无
     关；否决只是让这条调用不执行，它不给该工具计数——那是人的决定，不是模型不会调用。
  5. actions 非空表示防护留了话没说完：标记只在被兑现的那一次清掉，所以这段 run 结束后，
     线程下一次醒来的第一条该工具调用会被拦下来。看到它就别再去翻计数了，那是一次已经决定
     要拦的动作。`)
}

// --- 1. 事件流：一行一条 JSON 日志 --------------------------------------------

func logLines() {
	fmt.Println("\n=== 1. 把 ArgRejected 落成日志行 ===")

	store := checkpoint.NewMemory()
	var runs int
	evs, _ := collect(newAgent(store, guard(99), []tool.Tool{lookupTool{&runs}}, stubborn(bad)))

	var last []byte
	for _, ev := range evs {
		rejected, ok := ev.(core.ArgRejected)
		if !ok {
			continue
		}
		line := mustMarshal(rejected)
		last = line
		fmt.Printf("  %s\n", line)
	}

	// 另一头的进程解回来：类型串 arg_rejected，四个字段都在。
	fmt.Printf("  解回来：%#v\n", mustUnmarshal(last))
	fmt.Println("  注意第一行没有 step 字段：eventWire 的 step 带 omitempty，step 0 会被省掉，")
	fmt.Println("  消费侧要按缺省即 0 处理，别把缺字段当成坏消息。")
	fmt.Printf("  handler 实际跑了 %d 次；%s\n", runs, ledger(store))
}

// --- 2. 账本的两种形状，和一种静默归零 ----------------------------------------

func ledgerShapes() {
	fmt.Println("\n=== 2. 同一个键，内存里是 int、盘上是 float64 ===")

	dir, err := os.MkdirTemp("", "arg-guard-ops-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fileStore, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}

	// 同一份脚本、同一个防护配置，分别落进内存与 JSONL。
	memStore := checkpoint.NewMemory()
	refuseThree(memStore)
	refuseThree(fileStore)

	fmt.Printf("  内存读回   ：%s\n", readout(memStore))
	fmt.Printf("  JSONL 读回 ：%s\n", readout(fileStore))

	// 形状写错的代价：带私有字段的 struct 进 KV，marshal 出来是空对象。
	type myLedger struct {
		count  int
		warned bool
	}
	structJSON, _ := json.Marshal(map[string]any{"_demo.tools": map[string]any{"lookup": myLedger{count: 3, warned: true}}})
	mapJSON, _ := json.Marshal(map[string]any{"_demo.tools": map[string]any{"lookup": map[string]any{"count": 3, "warned": true}}})
	fmt.Printf("  struct 进 KV：%s\n", structJSON)
	fmt.Printf("  纯 map 进 KV：%s\n", mapJSON)
}

// readout 把 lookup 的计数按两种写法各读一遍：只认 int 的断言，和同时认 float64 的读法。
func readout(store checkpoint.Checkpointer) string {
	cp := latest(store)
	tools, _ := cp.State.KV["_argguard.tools"].(map[string]any)
	rec, _ := tools["lookup"].(map[string]any)
	raw := rec["count"]

	n, isInt := raw.(int)
	if isInt {
		return fmt.Sprintf("类型 %T；按 int 断言读到 %d；容错读到 %d", raw, n, tolerant(raw))
	}
	return fmt.Sprintf("类型 %T；按 int 断言失败（读成 0）；容错读到 %d", raw, tolerant(raw))
}

// tolerant 是中间件内部读数字的写法：既认活运行里的 int，也认 JSONL 解出来的 float64。
func tolerant(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// --- 3. 换进程续账，以及干预预算那本账 ----------------------------------------

func restartAndDecisions() {
	fmt.Println("\n=== 3. 进程换了计数照旧；预算随升级走，否决不计 ===")

	ctx := context.Background()
	dir, err := os.MkdirTemp("", "arg-guard-ops-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store1, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	// 四步：三次坏参数（第 2 次告警、第 3 次写标记）之后，第 4 步发一条参数完全正确的调用
	// ——它仍会被上一条账拦下来，run 停在人工决定。
	steps := append(append([]string{}, bad...), `{"q":"cats","limit":5}`)
	wave1, run1 := collect(newAgent(store1, guard(escalateAt), []tool.Tool{lookupTool{new(int)}}, stubborn(steps)))
	fmt.Printf("  第 1 段：%s\n", summarize(wave1))
	fmt.Printf("  账本：%s\n", ledger(store1))

	// 人工否决那一条：循环把人的理由回给模型。这条调用既没执行，也就不给该工具计数，
	// 更不花干预预算——预算记的是防护的升级，不是人的决定。
	if p := pendingOf(wave1); p != nil {
		run1.Decide(agent.Reject(p.CallID, "q 一直没给对，这一轮别再调 lookup，直接答复"))
		resumed, err := run1.Resume(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  否决之后接着跑完：%s\n", summarize(drain(resumed)))
		fmt.Printf("  账本：%s\n", ledger(store1))
	} else {
		fmt.Println("  第 1 段没有停在人工决定，本示例不该出现这种情况")
	}

	// "重启"：新 store、新 agent、新中间件实例，线程还是同一个。脚本只再发一次坏参数，
	// 然后交出文本答复。防护若忘了账本，这里会从 1 重新数、并把告警再发一遍。
	store2, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	wave2, _ := collect(newAgent(store2, guard(escalateAt), []tool.Tool{lookupTool{new(int)}}, stubborn([]string{`{"limit":5}`})))
	fmt.Printf("  第 2 段（新进程）：%s\n", summarize(wave2))
	fmt.Printf("  历史里带 %s 的消息：第 1 段末 %d 条，第 2 段末 %d 条（没有再发）\n",
		middleware.WarnMarkerArg, warnings(store1), warnings(store2))
	fmt.Printf("  账本：%s\n", ledger(store2))
	fmt.Printf("  干预预算共花掉 %d 次（第 1 段 %d 次、第 2 段 %d 次）\n",
		interventions(store2), escalated(wave1), escalated(wave2))
}

// pendingOf 取出这段 run 停在人工决定时的第一条待批调用。
func pendingOf(evs []core.Event) *core.ApprovalRequest {
	for _, ev := range evs {
		if i, ok := ev.(core.Interrupted); ok && len(i.Pending) > 0 {
			return &i.Pending[0]
		}
	}
	return nil
}

// --- 装配与驱动 ---------------------------------------------------------------

// guard 装配一个 ArgGuard：本示例固定 WarnThreshold=2，升级阈值由调用处给（99 表示不升级）。
func guard(escalateAt int) agent.Middleware {
	return middleware.ArgGuard(middleware.ArgGuardOptions{
		WarnThreshold:     warnAt,
		EscalateThreshold: escalateAt,
		MaxInterventions:  budget,
	})
}

// stubborn 让模型按 args 依次发坏参数调用；发完就交出一句文本答复。
func stubborn(args []string) func(*llm.Request) *llm.Response {
	called := 0
	return func(*llm.Request) *llm.Response {
		if called >= len(args) {
			return mock.Text("我就按现在拿到的信息答复")
		}
		called++
		return mock.CallTool(fmt.Sprintf("c%d", called), "lookup", args[called-1])
	}
}

func newAgent(store checkpoint.Checkpointer, mw agent.Middleware, tools []tool.Tool, resp func(*llm.Request) *llm.Response) *agent.Agent {
	a, err := agent.New(
		agent.WithModel(mock.New("stubborn", resp)),
		agent.WithTools(tools...),
		agent.WithMiddleware(mw),
		agent.WithCheckpointer(store),
		agent.WithMaxTurns(maxTurns),
	)
	if err != nil {
		log.Fatal(err)
	}
	return a
}

// drain 把一次 run 的事件按到达顺序收下来。
func drain(run *agent.Run) []core.Event {
	var out []core.Event
	for ev, err := range run.Iter() {
		if err != nil {
			out = append(out, core.RunFailed{Err: err})
			continue
		}
		out = append(out, ev)
	}
	return out
}

// collect 起一次 run，同时把 Run 交回来：停在人工决定的那条 run 要拿它 Decide/Resume。
// 打印全部留到调用处：模型响应函数跑在另一个 goroutine 里，边跑边打会与这里的行交错。
func collect(a *agent.Agent) ([]core.Event, *agent.Run) {
	run := a.Stream(context.Background(), "帮我查一下", agent.OnThread(threadOps))
	return drain(run), run
}

// refuseThree 只为把三连坏参数的账落进给定 store，不看输出。
func refuseThree(store checkpoint.Checkpointer) {
	_, _ = collect(newAgent(store, guard(99), []tool.Tool{lookupTool{new(int)}}, stubborn(bad)))
}

func mustMarshal(ev core.Event) []byte {
	b, err := core.MarshalEvent(ev)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func mustUnmarshal(b []byte) core.Event {
	ev, err := core.UnmarshalEvent(b)
	if err != nil {
		log.Fatal(err)
	}
	return ev
}

func latest(store checkpoint.Checkpointer) *checkpoint.Checkpoint {
	cp, err := store.Latest(context.Background(), threadOps)
	if err != nil {
		log.Fatal(err)
	}
	if cp == nil {
		log.Fatal("no checkpoint for thread " + threadOps)
	}
	return cp
}

// summarize 把一段 run 的终端情况说成一行。
func summarize(evs []core.Event) string {
	var refused int
	for _, ev := range evs {
		if _, ok := ev.(core.ArgRejected); ok {
			refused++
		}
	}
	for _, ev := range evs {
		switch e := ev.(type) {
		case core.Interrupted:
			return fmt.Sprintf("停在人工决定，待批 %s；被拒 %d 次", e.Pending[0].Tool, refused)
		case core.RunFailed:
			return fmt.Sprintf("失败：%v；被拒 %d 次", e.Err, refused)
		case core.RunDone:
			return fmt.Sprintf("模型给出答复：%s；被拒 %d 次", e.Result.Message.Text(), refused)
		}
	}
	return fmt.Sprintf("未收尾；被拒 %d 次", refused)
}

// escalated 数这一段里写下降级标记的次数——只有到升级阈值那一步才动 interventions，而
// 事件里能看到的信号就是该工具的计数越过了阈值。
func escalated(evs []core.Event) int {
	var n int
	for _, ev := range evs {
		if r, ok := ev.(core.ArgRejected); ok && r.Count >= escalateAt {
			n++
		}
	}
	return n
}

// warnings 数历史里带 [arg-guard] 标记的用户消息，即告警实际发过几次。
func warnings(store checkpoint.Checkpointer) int {
	var n int
	for _, m := range latest(store).State.Messages {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), middleware.WarnMarkerArg) {
			n++
		}
	}
	return n
}

// ledger 打印 ArgGuard 写进 State.KV 的键，按键名排序，好让这份输出每次跑都对得上。
func ledger(store checkpoint.Checkpointer) string {
	var names []string
	for k, v := range latest(store).State.KV {
		if strings.HasPrefix(k, "_argguard.") {
			names = append(names, fmt.Sprintf("%s=%v", k, v))
		}
	}
	if len(names) == 0 {
		return "（空）"
	}
	sort.Strings(names)
	return strings.Join(names, "; ")
}

// interventions 从账本里读干预预算的花费。
func interventions(store checkpoint.Checkpointer) int {
	return tolerant(latest(store).State.KV["_argguard.interventions"])
}
