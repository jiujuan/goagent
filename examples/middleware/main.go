// Command middleware is a tutorial for the middleware package: reusable loop
// capabilities you attach with agent.WithMiddleware, plus the RetryModel model
// decorator. Each is independent; stack the ones you need.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/middleware
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	raw, ok := buildModel()
	if !ok {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL / AGNES_BASE_URL)再运行。")
		return
	}
	ctx := context.Background()

	// RetryModel 是模型装饰器(不是 loop 中间件):重试裸模型调用。包在最里层。
	model := middleware.RetryModel(raw, middleware.RetryOptions{MaxAttempts: 3})

	// 一个知识库(用内置的关键词检索器;生产换成向量库)。
	kb := middleware.NewInMemory(
		"goagent 的并发工具执行默认是 Parallel,可用 WithToolExecution(ToolSequential) 改串行。",
		"goagent 的 HITL 通过 BeforeTool 返回 Interrupt 暂停,Run.Decide + Resume 继续。",
		"goagent 的 workflow 有 Sequential / Parallel / Loop 与 Pipeline builder。",
	)

	// 受保护工具:删除操作需要人工批准(Permission → HITL)。
	deleteFile := tool.New("delete_file", "删除文件",
		func(_ *tool.Context, in struct {
			Path string `json:"path"`
		}) (string, error) {
			return "已删除 " + in.Path, nil
		})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是 goagent 答疑助手。涉及删除时调用 delete_file。"),
		agent.WithTools(deleteFile),
		agent.WithMiddleware(
			middleware.Tracing(nil),                                    // 加逻辑:日志
			middleware.RateLimit(middleware.RateLimitOptions{RPS: 5}),  // 改逻辑:限速
			middleware.RAG(middleware.RAGOptions{Retriever: kb, K: 2}), // 改逻辑:注入背景
			middleware.Compaction(middleware.CompactionOptions{ // 改逻辑:超长压缩
				Model: raw, MaxTokens: 6000, KeepRecent: 8,
			}),
			middleware.Permission(middleware.RequireApprovalFor("delete_file")), // 改逻辑:工具门禁
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	section("1. RAG + Tracing —— 普通问答(注入知识库背景)")
	answer, err := a.Run(ctx, "goagent 的工具执行是并发还是串行?", agent.OnThread("mw"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", answer)

	section("2. Permission —— 危险工具需人工批准(HITL)")
	run := a.Stream(ctx, "请删除 /tmp/old.log", agent.OnThread("mw2"))
	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
		}
	}
	if len(pending) > 0 {
		fmt.Printf("   ⏸️  %d 个调用待批,演示批准...\n", len(pending))
		for _, p := range pending {
			run.Decide(agent.Allow(p.CallID))
		}
		cont, err := run.Resume(ctx)
		if err != nil {
			log.Fatal(err)
		}
		res, _ := cont.Wait()
		fmt.Println("🤖", res.Message.Text())
	}
}

func buildModel() (llm.Model, bool) {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		return nil, false
	}
	base := envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	model := envOr("AGNES_MODEL", "gemini-2.5-flash")
	return openaicompat.Agnes(base, model, key), true
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }
