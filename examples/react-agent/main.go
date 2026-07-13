// Command react-agent 演示 ReAct(Reason + Act)风格的智能体:模型每一步先「思考」
// 写出推理,再「行动」调用一个工具,拿到「观察」结果后继续,直到信息足够给出「回答」。
// goagent 的 AgentLoop 天然就是一个 ReAct 循环(模型↔工具反复往返),本例把这一过程
// 可视化,并给出两种用法:
//
//   - single:单次运行。一个跨境订单结算问题,需要顺序链式调用 5 个工具
//     (查订单→查商品→查汇率→算货款→查运费→算总额),每一步都依赖上一步的观察。
//   - multi :多次运行。在同一个会话线程(OnThread)上连问三轮,后一轮能引用前一轮的
//     结果(跨次记忆),每一轮内部又各自跑一段 ReAct。
//
// 默认离线运行(mock 模型脚本化地复现 ReAct,确定性输出,无需网络/Key)。设置
// REACT_LIVE=1 且提供 AGNES_API_KEY 时,改用真实模型做真正的 ReAct 推理。
//
//	go run ./examples/react-agent          # 依次跑 single + multi(离线 mock)
//	go run ./examples/react-agent single   # 只跑单次 ReAct
//	go run ./examples/react-agent multi    # 只跑多次 ReAct(同线程多轮)
//	REACT_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/react-agent
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/tool"
)

// reactInstruction 是给真实模型的 ReAct 系统提示;mock 模型不依赖它(它脚本化驱动)。
const reactInstruction = `你是跨境电商订单结算助手。请用 ReAct 方式解决问题:
每一步先用一句话写出你的推理(思考),再调用恰好一个工具(行动),拿到结果(观察)后
再决定下一步,直到信息足够,最后用中文给出结论。不要臆造任何数字,一切以工具结果为准。`

func main() {
	cmd := "all"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	ctx := context.Background()
	switch cmd {
	case "single":
		demoSingle(ctx)
	case "multi":
		demoMulti(ctx)
	case "all":
		demoSingle(ctx)
		demoMulti(ctx)
	default:
		fmt.Println("用法: go run ./examples/react-agent [single|multi]")
	}
}

// --- demo 1:单次运行的 ReAct ------------------------------------------------
//
// 一个问题需要 6 步链式工具调用才能回答,每一步都依赖前一步的观察结果。这正是 ReAct
// 的价值:把一个复杂问题拆成「思考→行动→观察」的小步骤,逐步逼近答案。
func demoSingle(ctx context.Context) {
	section("demo 1 · 单次运行 ReAct(链式调用多个工具)")

	a, err := agent.New(
		agent.WithModel(pickModel(singleModel())),
		agent.WithInstruction(reactInstruction),
		agent.WithTools(buildTools()...),
		agent.WithMaxTurns(12), // 安全上限:6 步推理 + 余量
	)
	if err != nil {
		log.Fatal(err)
	}

	question := "订单 A1001 运到上海,算上运费,客户一共要付多少人民币?"
	fmt.Println("👤 用户:", question)
	fmt.Println("—— ReAct 轨迹 ——")
	if err := runReAct(a.Stream(ctx, question)); err != nil {
		log.Fatal(err)
	}
}

// --- demo 2:多次运行的 ReAct(同线程多轮)----------------------------------
//
// 同一个 OnThread(id) 下,状态与对话历史跨多次 Run 累积:第二、三轮能引用前面轮次
// 已经查到的订单、商品、汇率,无需重查。每一轮内部仍是一段独立的 ReAct。
func demoMulti(ctx context.Context) {
	section("demo 2 · 多次运行 ReAct(同线程多轮 · 跨轮记忆)")

	a, err := agent.New(
		agent.WithModel(pickModel(multiModel())),
		agent.WithInstruction(reactInstruction),
		agent.WithTools(buildTools()...),
		agent.WithMaxTurns(12),
	)
	if err != nil {
		log.Fatal(err)
	}

	const thread = "settle-A1001"
	questions := []string{
		"订单 A1001 的货款,折合人民币大约是多少?",     // 第 1 轮:完整查一遍
		"再加上运到上海的运费,一共要付多少?",           // 第 2 轮:复用上轮的订单/商品
		"如果数量改成 5 件,货款会变成多少?(单价、汇率不变)", // 第 3 轮:复用单价/汇率
	}
	for i, q := range questions {
		fmt.Printf("\n👤 用户(第 %d 轮): %s\n", i+1, q)
		fmt.Println("—— ReAct 轨迹 ——")
		if err := runReAct(a.Stream(ctx, q, agent.OnThread(thread))); err != nil {
			log.Fatal(err)
		}
	}
}

// runReAct 订阅一次运行的事件流,把它渲染成「思考 / 行动 / 观察 / 回答」四段式轨迹。
// 关键点:一条带工具调用的助手消息里,Text() 是这一步的「思考」,ToolCalls() 是「行动」;
// 不带工具调用的助手消息就是最终「回答」。
func runReAct(run *agent.Run) error {
	step := 0
	for ev, err := range run.Iter() {
		if err != nil {
			return err
		}
		switch e := ev.(type) {
		case core.MessageDone:
			if len(e.Message.ToolCalls()) > 0 {
				step++
				if t := e.Message.Text(); t != "" {
					fmt.Printf("  💭 思考 %d: %s\n", step, t)
				}
			} else if t := e.Message.Text(); t != "" {
				fmt.Printf("  ✅ 回答: %s\n", t)
			}
		case core.ToolStarted:
			fmt.Printf("  🔧 行动 %d: %s %s\n", step, e.Call.Name, string(e.Call.Args))
		case core.ToolDone:
			fmt.Printf("  👀 观察 %d: %s\n", step, trText(e.Result))
		}
	}
	return nil
}

// --- 模型选择 ----------------------------------------------------------------

// pickModel:默认用传入的离线 mock;REACT_LIVE=1 且有 Key 时换真实模型。
func pickModel(offline llm.Model) llm.Model {
	if os.Getenv("REACT_LIVE") != "" {
		if key := os.Getenv("AGNES_API_KEY"); key != "" {
			fmt.Println("(联网模式:真实模型做 ReAct 推理)")
			return openaicompat.Agnes(
				envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
				envOr("AGNES_MODEL", "gemini-2.5-flash"), key)
		}
		fmt.Println("(已设 REACT_LIVE 但缺 AGNES_API_KEY,回退离线 mock)")
	}
	return offline
}

// --- mock 模型:脚本化地复现 ReAct ------------------------------------------
//
// mock 不会真的「思考」,而是按观察到的工具结果决定下一步,从而稳定复现一条 ReAct 链。
// 它读取观察结果(JSON)并解析出字段喂给下一个工具,正是真实模型 ReAct 时做的事。

// singleModel 驱动 demo 1 的 6 步链:查订单→查商品→查汇率→算货款→查运费→算总额→回答。
func singleModel() llm.Model {
	return mock.New("react-single", func(req *llm.Request) *llm.Response {
		t := turnObs(req) // 只看「当前这一问」之后产生的观察
		orderTxt, hasOrder := obsLast(t, "lookup_order")
		prodTxt, hasProd := obsLast(t, "lookup_product")
		rateTxt, hasRate := obsLast(t, "exchange_rate")
		shipTxt, hasShip := obsLast(t, "shipping_fee")
		nCalc := obsCount(t, "calculate")

		switch {
		case !hasOrder:
			return think("先查订单 A1001 的客户、商品和数量。",
				"s1", "lookup_order", jsonArg("order_id", "A1001"))
		case !hasProd:
			o := asOrder(orderTxt)
			return think(fmt.Sprintf("订单是 %s ×%d 件,查它的美元单价和重量。", o.ProductID, o.Quantity),
				"s2", "lookup_product", jsonArg("product_id", o.ProductID))
		case !hasRate:
			return think("单价是美元,要折人民币,先取 USD→CNY 汇率。",
				"s3", "exchange_rate", `{"from":"USD","to":"CNY"}`)
		case nCalc == 0:
			o, p, r := asOrder(orderTxt), asProduct(prodTxt), asRate(rateTxt)
			expr := fmt.Sprintf("%d*%g*%g", o.Quantity, p.UnitPriceUSD, r.Rate)
			return think(fmt.Sprintf("货款 = 数量 × 单价 × 汇率 = %s。", expr),
				"s4", "calculate", jsonArg("expression", expr))
		case !hasShip:
			o, p := asOrder(orderTxt), asProduct(prodTxt)
			w := totalWeight(o.Quantity, p.WeightKg)
			return think(fmt.Sprintf("总重 %gkg,查运到上海的运费。", w),
				"s5", "shipping_fee", fmt.Sprintf(`{"weight_kg":%g,"city":"上海"}`, w))
		case nCalc == 1:
			goods, _ := obsLast(t, "calculate")
			return think("把货款和运费相加,得到应付总额。",
				"s6", "calculate", jsonArg("expression", goods+"+"+shipTxt))
		default:
			total, _ := obsLast(t, "calculate")
			o := asOrder(orderTxt)
			return mock.Text(fmt.Sprintf("订单 A1001(客户 %s)运至上海,应付总额约 ¥%s(含货款与运费)。", o.Customer, total))
		}
	})
}

// multiModel 驱动 demo 2:按当前这一问的关键词分流到三轮各自的 ReAct 脚本。
func multiModel() llm.Model {
	return mock.New("react-multi", func(req *llm.Request) *llm.Response {
		q := lastUserText(req)
		switch {
		case strings.Contains(q, "运费"):
			return multiTurnShipping(req)
		case strings.Contains(q, "数量改成"):
			return multiTurnRequote(req)
		default:
			return multiTurnGoods(req)
		}
	})
}

// 第 1 轮:完整查一遍货款(查订单→查商品→查汇率→算货款→回答)。
func multiTurnGoods(req *llm.Request) *llm.Response {
	t := turnObs(req)
	orderTxt, hasOrder := obsLast(t, "lookup_order")
	prodTxt, hasProd := obsLast(t, "lookup_product")
	rateTxt, hasRate := obsLast(t, "exchange_rate")
	nCalc := obsCount(t, "calculate")

	switch {
	case !hasOrder:
		return think("先查订单 A1001 的客户、商品和数量。",
			"g1", "lookup_order", jsonArg("order_id", "A1001"))
	case !hasProd:
		o := asOrder(orderTxt)
		return think(fmt.Sprintf("订单是 %s ×%d 件,查单价和重量。", o.ProductID, o.Quantity),
			"g2", "lookup_product", jsonArg("product_id", o.ProductID))
	case !hasRate:
		return think("单价是美元,取 USD→CNY 汇率。",
			"g3", "exchange_rate", `{"from":"USD","to":"CNY"}`)
	case nCalc == 0:
		o, p, r := asOrder(orderTxt), asProduct(prodTxt), asRate(rateTxt)
		expr := fmt.Sprintf("%d*%g*%g", o.Quantity, p.UnitPriceUSD, r.Rate)
		return think(fmt.Sprintf("货款 = %s。", expr),
			"g4", "calculate", jsonArg("expression", expr))
	default:
		goods, _ := obsLast(t, "calculate")
		o := asOrder(orderTxt)
		return mock.Text(fmt.Sprintf("订单 A1001(客户 %s)的货款约为 ¥%s。", o.Customer, goods))
	}
}

// 第 2 轮:复用历史里的订单/商品算总重,查运费,再加上「上一轮的货款」得总额。
func multiTurnShipping(req *llm.Request) *llm.Response {
	t, h := turnObs(req), allObs(req)
	shipTxt, hasShip := obsLast(t, "shipping_fee")
	nCalc := obsCount(t, "calculate")

	switch {
	case !hasShip:
		o := asOrder(histText(h, "lookup_order")) // 来自第 1 轮的观察
		p := asProduct(histText(h, "lookup_product"))
		w := totalWeight(o.Quantity, p.WeightKg)
		return think(fmt.Sprintf("沿用上一轮查到的订单,总重 %gkg,查上海运费。", w),
			"h1", "shipping_fee", fmt.Sprintf(`{"weight_kg":%g,"city":"上海"}`, w))
	case nCalc == 0:
		goods, _ := obsLast(h, "calculate") // 此刻历史里最近的 calculate 即上一轮的货款
		return think("货款(上一轮已算)+ 运费 = 应付总额。",
			"h2", "calculate", jsonArg("expression", goods+"+"+shipTxt))
	default:
		total, _ := obsLast(t, "calculate")
		return mock.Text(fmt.Sprintf("含运费后,订单 A1001 应付总额约 ¥%s。", total))
	}
}

// 第 3 轮:复用历史里的单价与汇率,按新数量 5 件重算货款。
func multiTurnRequote(req *llm.Request) *llm.Response {
	t, h := turnObs(req), allObs(req)
	if obsCount(t, "calculate") == 0 {
		p := asProduct(histText(h, "lookup_product"))
		r := asRate(histText(h, "exchange_rate"))
		expr := fmt.Sprintf("5*%g*%g", p.UnitPriceUSD, r.Rate)
		return think(fmt.Sprintf("数量改为 5 件,货款 = 5 × 单价 × 汇率 = %s。", expr),
			"r1", "calculate", jsonArg("expression", expr))
	}
	goods, _ := obsLast(t, "calculate")
	return mock.Text(fmt.Sprintf("数量改成 5 件后,货款约为 ¥%s(单价、汇率不变)。", goods))
}

// think 构造一条「先思考、再行动」的助手消息:Text 是思考,ToolCall 是行动。
func think(thought, id, name, args string) *llm.Response {
	return &llm.Response{
		Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
			core.Text{Text: thought},
			core.ToolCall{ID: id, Name: name, Args: []byte(args)},
		}},
		StopReason: llm.StopToolUse,
	}
}

// --- 工具与数据 --------------------------------------------------------------

type orderInfo struct {
	Customer  string `json:"customer"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type productInfo struct {
	Name         string  `json:"name"`
	UnitPriceUSD float64 `json:"unit_price_usd"`
	WeightKg     float64 `json:"weight_kg"`
}

type rateInfo struct {
	Rate float64 `json:"rate"`
}

// 内置的小型「数据库」,让离线 demo 完全自洽。
var (
	orders = map[string]orderInfo{
		"A1001": {Customer: "张伟", ProductID: "P-200", Quantity: 3},
		"A1002": {Customer: "李娜", ProductID: "P-310", Quantity: 1},
	}
	products = map[string]productInfo{
		"P-200": {Name: "无线降噪耳机", UnitPriceUSD: 59.9, WeightKg: 0.3},
		"P-310": {Name: "机械键盘", UnitPriceUSD: 89.0, WeightKg: 1.1},
	}
	rates = map[string]float64{
		"USD->CNY": 7.18,
		"EUR->CNY": 7.76,
	}
	cityBase = map[string]float64{ // 运费 = 基础价 + 12 元/kg
		"上海": 8, "北京": 10, "广州": 9,
	}
)

func buildTools() []tool.Tool {
	lookupOrder := tool.New("lookup_order", "按订单号查询订单(客户、商品编号、数量)",
		func(_ *tool.Context, in struct {
			OrderID string `json:"order_id" desc:"订单号,如 A1001"`
		}) (orderInfo, error) {
			o, ok := orders[in.OrderID]
			if !ok {
				return orderInfo{}, fmt.Errorf("订单 %q 不存在", in.OrderID)
			}
			return o, nil
		})

	lookupProduct := tool.New("lookup_product", "按商品编号查询商品(名称、美元单价、单件重量kg)",
		func(_ *tool.Context, in struct {
			ProductID string `json:"product_id" desc:"商品编号,如 P-200"`
		}) (productInfo, error) {
			p, ok := products[in.ProductID]
			if !ok {
				return productInfo{}, fmt.Errorf("商品 %q 不存在", in.ProductID)
			}
			return p, nil
		})

	exchangeRate := tool.New("exchange_rate", "查询两种货币之间的汇率",
		func(_ *tool.Context, in struct {
			From string `json:"from" desc:"源货币,如 USD"`
			To   string `json:"to" desc:"目标货币,如 CNY"`
		}) (rateInfo, error) {
			r, ok := rates[in.From+"->"+in.To]
			if !ok {
				return rateInfo{}, fmt.Errorf("暂无 %s->%s 的汇率", in.From, in.To)
			}
			return rateInfo{Rate: r}, nil
		})

	shippingFee := tool.New("shipping_fee", "按总重量和目的城市计算运费(人民币)",
		func(_ *tool.Context, in struct {
			WeightKg float64 `json:"weight_kg" desc:"总重量,单位千克"`
			City     string  `json:"city" desc:"目的城市,如 上海"`
		}) (string, error) {
			base, ok := cityBase[in.City]
			if !ok {
				return "", fmt.Errorf("暂不支持配送到 %q", in.City)
			}
			return strconv.FormatFloat(base+12.0*in.WeightKg, 'f', 2, 64), nil
		})

	calculate := tool.New("calculate", "计算一个算术表达式,支持 + - * / 和括号,返回保留两位小数的结果",
		func(_ *tool.Context, in struct {
			Expression string `json:"expression" desc:"算术表达式,如 3*59.9*7.18"`
		}) (string, error) {
			v, err := evalExpr(in.Expression)
			if err != nil {
				return "", err
			}
			return strconv.FormatFloat(v, 'f', 2, 64), nil
		})

	return []tool.Tool{lookupOrder, lookupProduct, exchangeRate, shippingFee, calculate}
}

// --- 观察解析辅助 ------------------------------------------------------------

// observation 是一次工具观察:工具名 + 结果文本。
type observation struct{ name, text string }

// turnObs 收集「最近一条用户消息之后」产生的工具观察 —— 即当前这一问的 ReAct 步骤,
// 天然把多轮里各轮的步骤隔开。
func turnObs(req *llm.Request) []observation {
	start := 0
	for i, m := range req.Messages {
		if m.Role == core.RoleUser {
			start = i + 1
		}
	}
	return collectObs(req.Messages[start:])
}

// allObs 收集整段历史里的工具观察,供后一轮引用前一轮的结果。
func allObs(req *llm.Request) []observation { return collectObs(req.Messages) }

func collectObs(msgs []core.Message) []observation {
	var out []observation
	for _, m := range msgs {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				out = append(out, observation{tr.Name, trText(tr)})
			}
		}
	}
	return out
}

func obsLast(os []observation, name string) (string, bool) {
	for i := len(os) - 1; i >= 0; i-- {
		if os[i].name == name {
			return os[i].text, true
		}
	}
	return "", false
}

func obsCount(os []observation, name string) int {
	n := 0
	for _, o := range os {
		if o.name == name {
			n++
		}
	}
	return n
}

func histText(os []observation, name string) string {
	t, _ := obsLast(os, name)
	return t
}

func asOrder(s string) orderInfo     { var o orderInfo; _ = json.Unmarshal([]byte(s), &o); return o }
func asProduct(s string) productInfo { var p productInfo; _ = json.Unmarshal([]byte(s), &p); return p }
func asRate(s string) rateInfo       { var r rateInfo; _ = json.Unmarshal([]byte(s), &r); return r }

// totalWeight 计算订单总重并保留两位小数,避免浮点尾差(如 3×0.3=0.8999…)。
func totalWeight(qty int, unitKg float64) float64 {
	return math.Round(float64(qty)*unitKg*100) / 100
}

// --- 小工具函数 --------------------------------------------------------------

// trText 取工具结果的首个文本片段。
func trText(tr core.ToolResult) string {
	if len(tr.Content) > 0 {
		if t, ok := tr.Content[0].(core.Text); ok {
			return t.Text
		}
	}
	return ""
}

// lastUserText 返回最近一条用户消息的文本。
func lastUserText(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}

// jsonArg 构造单字段的工具参数 JSON,确保正确转义。
func jsonArg(key, val string) string {
	b, _ := json.Marshal(map[string]string{key: val})
	return string(b)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }

// --- 算术表达式求值(递归下降)----------------------------------------------
//
// 支持 + - * /、括号、一元正负与小数。calculate 工具用它,既服务离线脚本,也能处理
// 真实模型给出的任意表达式。

func evalExpr(s string) (float64, error) {
	p := &exprParser{src: s}
	v, err := p.expr()
	if err != nil {
		return 0, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return 0, fmt.Errorf("表达式无法解析: %q", s)
	}
	return v, nil
}

type exprParser struct {
	src string
	pos int
}

func (p *exprParser) skipSpace() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *exprParser) expr() (float64, error) { // 加减
	v, err := p.term()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) {
			break
		}
		op := p.src[p.pos]
		if op != '+' && op != '-' {
			break
		}
		p.pos++
		r, err := p.term()
		if err != nil {
			return 0, err
		}
		if op == '+' {
			v += r
		} else {
			v -= r
		}
	}
	return v, nil
}

func (p *exprParser) term() (float64, error) { // 乘除
	v, err := p.factor()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) {
			break
		}
		op := p.src[p.pos]
		if op != '*' && op != '/' {
			break
		}
		p.pos++
		r, err := p.factor()
		if err != nil {
			return 0, err
		}
		if op == '*' {
			v *= r
		} else {
			if r == 0 {
				return 0, fmt.Errorf("除以零")
			}
			v /= r
		}
	}
	return v, nil
}

func (p *exprParser) factor() (float64, error) { // 括号 / 一元符号 / 数字
	p.skipSpace()
	if p.pos < len(p.src) && p.src[p.pos] == '(' {
		p.pos++
		v, err := p.expr()
		if err != nil {
			return 0, err
		}
		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return 0, fmt.Errorf("缺少右括号")
		}
		p.pos++
		return v, nil
	}
	if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
		neg := p.src[p.pos] == '-'
		p.pos++
		v, err := p.factor()
		if err != nil {
			return 0, err
		}
		if neg {
			return -v, nil
		}
		return v, nil
	}
	return p.number()
}

func (p *exprParser) number() (float64, error) {
	p.skipSpace()
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if (c >= '0' && c <= '9') || c == '.' {
			p.pos++
		} else {
			break
		}
	}
	if start == p.pos {
		return 0, fmt.Errorf("位置 %d 处期望数字", p.pos)
	}
	return strconv.ParseFloat(p.src[start:p.pos], 64)
}
