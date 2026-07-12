// Command pipeline is a data-processing (ETL-style) counterpart to the workflow
// example. Where workflow shows orchestration control flow (Parallel/Loop), this
// shows DATA flowing through a linear Sequential pipeline: each stage pulls the
// previous stage's structured output from shared session state, transforms it,
// and writes its own — Extract → Classify → Score → Report.
//
//	Sequential「feedback-pipeline」
//	├─ ingest    clean      数据源 → 清洗去重           → state[pipe.ingested]
//	├─ classify  classify   按关键词打类别（bug/feature/praise）→ state[pipe.classified]
//	├─ score     score      计算优先级（含加权）       → state[pipe.scored]
//	└─ report    report     聚合统计 + Top 列表          → state[pipe.report]
//
// Each stage is an LLMAgent whose model simply invokes that stage's tool and
// relays the result; the real work — and the data threading — happens in the
// tools via ctx.State. Runs on the mock provider, no API key required.
package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// Feedback is the record that flows through the pipeline, accreting fields as it
// passes each stage.
type Feedback struct {
	ID       int
	Text     string
	Category string // bug | feature | praise
	Priority int    // 1..5
}

// rawFeedback simulates an external data source the first stage extracts from.
// It deliberately contains a blank line and a duplicate to exercise cleaning.
var rawFeedback = []string{
	"应用一打开就闪退，太严重了",
	"希望能增加深色模式",
	"  ",
	"界面很好看，用着很顺手",
	"列表滑动经常卡顿，偶尔报错",
	"应用一打开就闪退，太严重了", // 重复
	"能不能支持导出 PDF",
}

func main() {
	stages := []*agent.LLMAgent{
		stage("ingest", clean),
		stage("classify", classify),
		stage("score", score),
		stage("report", report),
	}
	pipeline := agent.Sequential("feedback-pipeline",
		stages[0], stages[1], stages[2], stages[3])

	store := session.InMemory()
	r := runner.New(runner.Config{AppName: "etl", Root: pipeline, Store: store})
	ctx := context.Background()

	banner("goagent 数据流水线示例：用户反馈分析 (ETL)",
		"Sequential · 各阶段经 session state 逐级传递结构化数据")

	for ev, err := range r.Run(ctx, "u", "s1", core.UserText("分析本批用户反馈")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}

	// 收尾：展示数据在管道里逐级落到 state 的痕迹。
	fmt.Println("\n── 管道各阶段在 state 中的产物 ──")
	s, err := store.GetOrCreate(ctx, "etl", "u", "s1")
	if err != nil {
		log.Fatal(err)
	}
	for _, k := range []string{"pipe.ingested", "pipe.classified", "pipe.scored"} {
		fmt.Printf("   %-18s = %d 条记录\n", k, len(items(s.State(), k)))
	}
}

// --- 阶段工具（真正的数据变换都在这里，经 ctx.State 串联）---

// clean: Extract——从数据源拉取，去空白与重复，落到 pipe.ingested。
func clean(ctx *tool.Context, _ struct{}) (string, error) {
	var out []Feedback
	seen := map[string]bool{}
	for _, raw := range rawFeedback {
		t := strings.TrimSpace(raw)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, Feedback{ID: len(out) + 1, Text: t})
	}
	ctx.State.Set("pipe.ingested", out)
	return fmt.Sprintf("清洗完成：原始 %d 条 → 有效 %d 条（去空 / 去重）", len(rawFeedback), len(out)), nil
}

// classify: 读 pipe.ingested，按关键词打类别，落到 pipe.classified。
func classify(ctx *tool.Context, _ struct{}) (string, error) {
	in := items(ctx.State, "pipe.ingested")
	if len(in) == 0 {
		return "", fmt.Errorf("上游无数据：pipe.ingested 为空")
	}
	counts := map[string]int{}
	for i := range in {
		in[i].Category = categorize(in[i].Text)
		counts[in[i].Category]++
	}
	ctx.State.Set("pipe.classified", in)
	return fmt.Sprintf("分类完成：bug=%d feature=%d praise=%d",
		counts["bug"], counts["feature"], counts["praise"]), nil
}

// score: 读 pipe.classified，按类别基线 + 严重词加权算优先级，落到 pipe.scored。
func score(ctx *tool.Context, _ struct{}) (string, error) {
	in := items(ctx.State, "pipe.classified")
	if len(in) == 0 {
		return "", fmt.Errorf("上游无数据：pipe.classified 为空")
	}
	base := map[string]int{"bug": 4, "feature": 2, "praise": 1}
	max := 0
	for i := range in {
		p := base[in[i].Category]
		if hasAny(in[i].Text, "严重", "经常", "急") {
			p++ // 严重词加权
		}
		if p > 5 {
			p = 5
		}
		in[i].Priority = p
		if p > max {
			max = p
		}
	}
	ctx.State.Set("pipe.scored", in)
	return fmt.Sprintf("打分完成：%d 条已评级，最高优先级 P%d", len(in), max), nil
}

// report: 读 pipe.scored，聚合统计并列出高优先级 Top3，落到 pipe.report。
func report(ctx *tool.Context, _ struct{}) (string, error) {
	in := items(ctx.State, "pipe.scored")
	if len(in) == 0 {
		return "", fmt.Errorf("上游无数据：pipe.scored 为空")
	}
	sorted := append([]Feedback(nil), in...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Priority > sorted[j].Priority })

	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 条；按优先级 Top3：", len(sorted))
	for i, f := range sorted {
		if i == 3 {
			break
		}
		fmt.Fprintf(&b, "\n        P%d [%s] %s", f.Priority, f.Category, f.Text)
	}
	out := b.String()
	ctx.State.Set("pipe.report", out)
	return out, nil
}

// --- 分类规则 ---

func categorize(text string) string {
	switch {
	case hasAny(text, "闪退", "崩溃", "卡", "报错", "bug", "错误"):
		return "bug"
	case hasAny(text, "希望", "建议", "增加", "能不能", "支持"):
		return "feature"
	default:
		return "praise"
	}
}

// --- 阶段装配与流式打印 ---

// stage builds an LLMAgent that calls one stage tool and relays its summary as
// the stage's reply. The answered/relay pattern keeps every stage uniform.
func stage(name string, fn tool.Func[struct{}, string]) *agent.LLMAgent {
	t := tool.New(name, "执行 "+name+" 阶段", fn)
	return agent.New(agent.Config{
		Name: name, Description: name + " 阶段",
		Tools:           []tool.Tool{t},
		DisableTransfer: true,
		Model: mock.New(name, func(req *llm.Request) *llm.Response {
			// 只认本阶段同名工具的结果——上一阶段的工具结果也在历史里，
			// 若不按名字区分，本阶段会误以为已执行而跳过自己的工具。
			if res, ok := mock.LastToolResult(req); ok && res.Name == name {
				return mock.Text(partsText(res.Content)) // 中继工具产出的摘要
			}
			return mock.CallTool("c", name, `{}`)
		}),
	})
}

func printEvent(ev *core.Event) {
	if ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("\n👤 %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			return // 工具调用本身不打印，只展示其结果
		}
		fmt.Printf("   ⛓ %-9s ⇒ %s\n", ev.Author, ev.Message.Text())
	}
}

// --- 小工具 ---

func items(st session.StateReader, key string) []Feedback {
	if v, ok := st.Get(key); ok {
		if fb, ok := v.([]Feedback); ok {
			// State owns the stored slice after Set. Return a copy so downstream
			// stages can enrich records without mutating an earlier stage's value or
			// racing with a concurrent snapshot reader.
			return append([]Feedback(nil), fb...)
		}
	}
	return nil
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

func hasAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}
