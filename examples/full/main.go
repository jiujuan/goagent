// Command full is the capstone example: it wires together every goagent
// capability into one runnable program (no API key required — all models and
// embeddings are mocks).
//
// It builds a small customer-support system and walks through six scenes:
//
//  1. 多 agent 路由 + RAG + 流式 + 重试   coordinator 委派给 kb_expert，
//     RAG 注入知识库，模型流式作答，Retry 中间件吞掉一次瞬时故障。
//  2. 方向规则委派                       coordinator 委派给 billing_expert。
//  3. steering 运行中插话                外部排入的指令在下次模型调用前生效。
//  4. 上下文压缩                         超阈值时旧历史被结构化摘要替换。
//  5. 限流                               RateLimit 强制最小调用间隔。
//  6. JSONL 持久化与恢复                 用全新 Store 从磁盘重放恢复整段会话。
package main

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "goagent-full-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// --- 知识库 (memory + mock embedder) ---
	kb := memory.InMemory(embmock.New())
	_ = kb.Add(ctx,
		memory.Doc("GoPhone 的电池续航约为 48 小时，并支持 30 分钟快充。"),
		memory.Doc("GoPhone 支持 7 天无理由退货。"),
		memory.Doc("GoPhone 的屏幕为 6.7 英寸 OLED。"),
	)

	// kb_expert：RAG 自动注入 + 流式 + 首次调用故障由 Retry 兜住。
	kbModel := &flakyStreamModel{failFirst: true}
	kbExpert := agent.New(agent.Config{
		Name: "kb_expert", Description: "回答产品知识问题",
		Model: kbModel,
		Middleware: []middleware.Middleware{
			memory.NewRAG(kb, &memory.RAGOptions{K: 2}),
			middleware.Retry(&middleware.RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}),
		},
	})

	billingExpert := agent.New(agent.Config{
		Name: "billing_expert", Description: "处理退款与账单",
		Model: mock.New("billing", func(*llm.Request) *llm.Response {
			return mock.Text("已为您登记退款申请，预计 3 个工作日内到账。")
		}),
	})

	coordinator := agent.New(agent.Config{
		Name: "coordinator", Description: "客服路由",
		SubAgents: []agent.Agent{kbExpert, billingExpert},
		Model: mock.New("coordinator", func(req *llm.Request) *llm.Response {
			q := lastUserText(req.Messages)
			switch {
			case containsAny(q, "续航", "电池", "屏幕", "退货"):
				return transfer("kb_expert")
			case containsAny(q, "退款", "账单", "发票"):
				return transfer("billing_expert")
			default:
				return mock.Text("请问您是产品咨询还是账单问题？")
			}
		}),
	})

	store, err := session.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	r := runner.New(runner.Config{AppName: "support", Root: coordinator, Store: store})

	banner("goagent 综合示例：智能客服系统",
		"多 agent + 方向规则委派 · RAG · 流式 · retry/ratelimit/steering/compaction · JSONL 持久化")

	// === 场景 1 ===
	scene("1. 多 agent 路由 + RAG + 流式 + 重试")
	consume(ctx, r, "chat-1", "GoPhone 的电池续航怎么样？")
	fmt.Printf("   ⚙ kb_expert 模型共尝试 %d 次（首次故障被 Retry 兜住）\n", kbModel.attempts)

	// === 场景 2 ===
	scene("2. 方向规则委派（路由到账单专家）")
	consume(ctx, r, "chat-1", "我要申请退款")

	// === 场景 3 ===
	scene("3. steering 运行中插话")
	demoSteering(ctx)

	// === 场景 4 ===
	scene("4. 上下文压缩（超阈值自动摘要旧历史）")
	demoCompaction(ctx)

	// === 场景 5 ===
	scene("5. 限流（强制最小调用间隔）")
	demoRateLimit(ctx)

	// === 场景 6 ===
	scene("6. JSONL 持久化与恢复（全新 Store 从磁盘重放）")
	demoRecover(ctx, dir)
}

// --- 场景 3：steering ---

func demoSteering(ctx context.Context) {
	steer := middleware.NewSteering()
	assistant := agent.New(agent.Config{
		Name: "assistant",
		Model: mock.New("a", func(req *llm.Request) *llm.Response {
			for _, m := range req.Messages {
				if strings.Contains(m.Text(), "一句话") {
					return mock.Text("GoPhone 是一款续航强、屏幕好的旗舰手机。")
				}
			}
			return mock.Text("（这里本会是一段很长的介绍……）")
		}),
		Middleware: []middleware.Middleware{steer.Middleware()},
	})
	r := runner.New(runner.Config{Root: assistant})

	// 模拟运行前/中从外部排入一条指导消息。
	steer.SteerText("请用一句话回答")
	fmt.Println("   （已从外部排入 steering：\"请用一句话回答\"）")
	consume(ctx, r, "s", "详细介绍一下 GoPhone")
}

// --- 场景 4：compaction ---

func demoCompaction(ctx context.Context) {
	summarizer := mock.New("sum", func(*llm.Request) *llm.Response {
		return mock.Text("【摘要】用户在咨询 GoPhone 的多项参数；已逐条回答。")
	})
	var got *llm.Request
	base := mock.New("capture", func(req *llm.Request) *llm.Response {
		got = req
		return mock.Text("ok")
	})
	model := middleware.Chain(base,
		middleware.Compaction(summarizer, &middleware.CompactionOptions{MaxTokens: 40, KeepRecentTokens: 16}))

	var msgs []core.Message
	for i := range 20 {
		role := core.RoleUser
		if i%2 == 1 {
			role = core.RoleAssistant
		}
		msgs = append(msgs, core.Message{Role: role, Parts: []core.Part{core.Text{Text: "GoPhone 的某项参数问答内容"}}})
	}
	for _, err := range model.Generate(ctx, &llm.Request{Messages: msgs}) {
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("   历史 %d 条 → 压缩后 %d 条；首条为摘要：%q\n",
		len(msgs), len(got.Messages), truncate(got.Messages[0].Text(), 30))
}

// --- 场景 5：ratelimit ---

func demoRateLimit(ctx context.Context) {
	base := mock.New("rl", func(*llm.Request) *llm.Response { return mock.Text("ok") })
	model := middleware.Chain(base, middleware.RateLimit(&middleware.RateLimitOptions{RPS: 50})) // 20ms 间隔

	start := time.Now()
	const n = 4
	for range n {
		for _, err := range model.Generate(ctx, &llm.Request{}) {
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	fmt.Printf("   %d 次调用耗时 %v（RPS=50 → 至少 ~%v）\n",
		n, time.Since(start).Round(time.Millisecond), time.Duration(n-1)*20*time.Millisecond)
}

// --- 场景 6：persistence ---

func demoRecover(ctx context.Context, dir string) {
	fresh, err := session.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	s, err := fresh.GetOrCreate(ctx, "support", "user", "chat-1")
	if err != nil {
		log.Fatal(err)
	}
	msgs := s.Messages()
	fmt.Printf("   从磁盘恢复会话 chat-1：%d 条消息\n", len(msgs))
	if len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		fmt.Printf("   最后一条 [%s]：%s\n", last.Role, truncate(last.Text(), 40))
	}
}

// --- 流式消费与打印 ---

func consume(ctx context.Context, r *runner.Runner, sessionID, user string) {
	fmt.Printf("👤 %s\n", user)
	streaming := false
	prev := ""
	for ev, err := range r.Run(ctx, "user", sessionID, core.UserText(user)) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message == nil {
			continue
		}
		switch ev.Message.Role {
		case core.RoleAssistant:
			if ev.Partial {
				if !streaming {
					fmt.Printf("🤖 %s：", ev.Author)
					streaming, prev = true, ""
				}
				txt := ev.Message.Text()
				fmt.Print(strings.TrimPrefix(txt, prev))
				prev = txt
				continue
			}
			if calls := ev.Message.ToolCalls(); len(calls) > 0 {
				for _, c := range calls {
					if c.Name == "transfer_to_agent" {
						fmt.Printf("🧭 %s 委派 → %s\n", ev.Author, string(c.Args))
					}
				}
				continue
			}
			if streaming {
				fmt.Println()
				streaming = false
				continue
			}
			fmt.Printf("🤖 %s：%s\n", ev.Author, ev.Message.Text())
		}
	}
}

// --- mock 模型与小工具 ---

// flakyStreamModel streams its answer (derived from the RAG-injected system
// prompt) but fails the very first call to demonstrate Retry.
type flakyStreamModel struct {
	failFirst bool
	attempts  int
}

func (m *flakyStreamModel) Name() string { return "kb-model" }

func (m *flakyStreamModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.attempts++
		if m.failFirst && m.attempts == 1 {
			yield(nil, errors.New("503 服务临时不可用"))
			return
		}
		ans := "抱歉，我没有查到相关资料。"
		if strings.Contains(req.System, "48 小时") {
			ans = "GoPhone 的电池续航约为 48 小时，支持 30 分钟快充。"
		}
		runes := []rune(ans)
		acc := ""
		const chunks = 3
		for i := range chunks {
			seg := runes[i*len(runes)/chunks : (i+1)*len(runes)/chunks]
			acc += string(seg)
			if !yield(&llm.Response{Message: core.AssistantText(acc), Partial: true}, nil) {
				return
			}
		}
		yield(&llm.Response{Message: core.AssistantText(ans), StopReason: llm.StopEnd}, nil)
	}
}

func transfer(name string) *llm.Response {
	return mock.CallTool("t", "transfer_to_agent", `{"agent_name":"`+name+`"}`)
}

func lastUserText(msgs []core.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleUser {
			return msgs[i].Text()
		}
	}
	return ""
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func banner(title, sub string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println("════════════════════════════════════════════════════════")
}

func scene(title string) {
	fmt.Printf("\n── %s ──\n", title)
}
