// Command websearch is a tutorial for the tool/web package (web.go + search.go):
// the web_search and web_fetch tools an agent can call. It is organized as a
// series of self-contained lessons.
//
// Lessons 1–4 are fully DETERMINISTIC and need no network: web_search runs
// against an injected stub Backend, and web_fetch reads a local httptest server.
// Lesson 5 makes REAL DuckDuckGo requests and is opt-in — run with WEB_LIVE=1:
//
//	go run ./examples/websearch              # offline lessons 1–4
//	WEB_LIVE=1 go run ./examples/websearch   # also runs the live lesson 5
//
// Everything an agent needs is in tool/web; this file just exercises it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/tool/web"
)

func main() {
	ctx := context.Background()

	// A local page that web_fetch will read in lessons 2 and 3 — note the
	// <script>/<style> the tool is expected to strip away.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, articleHTML)
	}))
	defer srv.Close()

	banner("tool/web 教程：web_search 与 web_fetch")

	lesson1Search(ctx)
	lesson2Fetch(ctx, srv.URL)
	lesson3Agent(ctx, srv.URL)
	lesson4Options(ctx, srv.URL)
	lesson5Live(ctx)
}

// --- Lesson 1: 直接调用 web_search（注入离线 Backend，结果可控） ---

func lesson1Search(ctx context.Context) {
	scene("1. web_search —— 直接调用，注入自定义 Backend")

	// web.WithBackend 用任意实现了 web.Backend 接口的对象替换默认的 DuckDuckGo
	// 后端。这里用一个静态 stub，既离线可跑，也演示了「接生产搜索 API」的扩展点。
	backend := stubBackend{results: []web.SearchResult{
		{Title: "Go 1.23 发布要点", URL: "https://example.com/go123", Snippet: "range-over-func 迭代器、工具链改进。"},
		{Title: "Go 官方博客", URL: "https://go.dev/blog", Snippet: "版本发布与设计说明。"},
		{Title: "Go 标准库文档", URL: "https://pkg.go.dev/std", Snippet: "标准库 API 参考。"},
	}}

	searchTool := web.Search(web.WithBackend(backend))
	out, isErr := call(ctx, searchTool, `{"query":"Go 1.23 新特性","limit":2}`)
	fmt.Printf("   调用 web_search(query=\"Go 1.23 新特性\", limit=2) → isError=%v\n", isErr)
	indent(out)
}

// --- Lesson 2: 直接调用 web_fetch（抓取本地页面并剥离 HTML） ---

func lesson2Fetch(ctx context.Context, url string) {
	scene("2. web_fetch —— 抓取网页并转纯文本")

	fetchTool := web.Fetch()
	out, isErr := call(ctx, fetchTool, fmt.Sprintf(`{"url":%q}`, url))
	fmt.Printf("   调用 web_fetch(url=%s) → isError=%v\n", url, isErr)
	indent(out)
	if strings.Contains(out, "track(") || strings.Contains(out, "display:none") {
		log.Fatal("脚本/样式未被剥离")
	}
}

// --- Lesson 3: 把 web.Tools() 挂到 agent，走 search → fetch → 作答闭环 ---

func lesson3Agent(ctx context.Context, pageURL string) {
	scene("3. 挂到 agent：mock LLM 自主 search → fetch → 作答")

	// 搜索结果指向我们的本地文章页，于是 fetch 能接力打开它。
	backend := stubBackend{results: []web.SearchResult{
		{Title: "Go 1.23 发布要点", URL: pageURL, Snippet: "range-over-func 迭代器。"},
	}}

	// mock 模型按「历史里已有哪个工具结果」推进流程；真实模型会读搜索结果自行决定。
	model := mock.New("researcher", func(req *llm.Request) *llm.Response {
		res, ok := mock.LastToolResult(req)
		switch {
		case !ok:
			return mock.CallTool("s", "web_search", `{"query":"Go 1.23 新特性","limit":3}`)
		case res.Name == "web_search":
			return mock.CallTool("f", "web_fetch", fmt.Sprintf(`{"url":%q}`, pageURL))
		default: // web_fetch 已返回
			return mock.Text("综合检索与原文：Go 1.23 支持 range-over-func 迭代器，并改进了工具链与标准库。")
		}
	})

	researcher := agent.New(agent.Config{
		Name:        "researcher",
		Instruction: "需要最新信息时先 web_search，再用 web_fetch 打开链接阅读后作答。",
		Model:       model,
		Tools:       web.Tools(web.WithBackend(backend)), // web_search + web_fetch
	})

	r := runner.New(runner.Config{Root: researcher})
	consume(ctx, r, "Go 1.23 有哪些新特性？")
}

// --- Lesson 4: options 一览 ---

func lesson4Options(ctx context.Context, pageURL string) {
	scene("4. options：定制超时 / UA / 字节上限 / 结果数 / 后端")

	client := &http.Client{Timeout: 5 * time.Second}
	tools := web.Tools(
		web.WithHTTPClient(client),               // 自定义 HTTP 客户端（超时/代理/transport）
		web.WithUserAgent("my-research-bot/1.0"), // 自定义 User-Agent
		web.WithMaxBytes(64<<10),                 // web_fetch 下载上限 64 KiB
		web.WithMaxResults(3),                    // web_search 默认结果数
		web.WithBackend(stubBackend{results: []web.SearchResult{{Title: "doc", URL: pageURL}}}),
	)
	fmt.Printf("   web.Tools(...) 返回 %d 个工具：", len(tools))
	for i, t := range tools {
		if i > 0 {
			fmt.Print("、")
		}
		fmt.Print(t.Name())
	}
	fmt.Println()
	out, _ := call(ctx, tools[0], `{"query":"x"}`) // tools[0] = web_search
	indent(out)
}

// --- Lesson 5: 真实联网（opt-in：WEB_LIVE=1） ---

func lesson5Live(ctx context.Context) {
	scene("5. 真实联网（DuckDuckGo） —— 需 WEB_LIVE=1")

	if os.Getenv("WEB_LIVE") != "1" {
		fmt.Println("   已跳过。用 `WEB_LIVE=1 go run ./examples/websearch` 启用真实搜索与抓取。")
		return
	}

	// 默认 web.Tools() 即用零配置的 DuckDuckGo 后端，无需 API key。
	searchTool, fetchTool := web.Search(), web.Fetch()

	fmt.Println("   web_search(\"golang range over func\")：")
	out, isErr := call(ctx, searchTool, `{"query":"golang range over func","limit":3}`)
	if isErr {
		fmt.Println("   搜索失败：", out)
		return
	}
	indent(out)

	// 取第一条结果的 URL 抓取正文。
	if url := firstURL(out); url != "" {
		fmt.Printf("\n   web_fetch(%s)：\n", url)
		body, _ := call(ctx, fetchTool, fmt.Sprintf(`{"url":%q}`, url))
		indent(truncate(body, 400))
	}
}

// --- 复用：把 web.Backend 接口实现成一个静态 stub ---

type stubBackend struct{ results []web.SearchResult }

func (s stubBackend) Search(_ context.Context, _ string, limit int) ([]web.SearchResult, error) {
	r := s.results
	if len(r) > limit {
		r = r[:limit]
	}
	return r, nil
}

// --- 小工具 ---

// call invokes a tool with JSON args, returning its rendered text and IsError.
func call(ctx context.Context, tl tool.Tool, args string) (string, bool) {
	res, err := tl.Call(&tool.Context{Context: ctx}, json.RawMessage(args))
	if err != nil {
		log.Fatal(err)
	}
	var b strings.Builder
	for _, p := range res.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String(), res.IsError
}

// consume runs one agent turn and prints tool calls, tool results, and the reply.
func consume(ctx context.Context, r *runner.Runner, user string) {
	fmt.Printf("   👤 %s\n", user)
	for ev, err := range r.Run(ctx, "u", "s", core.UserText(user)) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message == nil {
			continue
		}
		switch ev.Message.Role {
		case core.RoleAssistant:
			if calls := ev.Message.ToolCalls(); len(calls) > 0 {
				for _, c := range calls {
					fmt.Printf("   🔧 调用 %s %s\n", c.Name, string(c.Args))
				}
				continue
			}
			fmt.Printf("   🤖 %s\n", ev.Message.Text())
		case core.RoleTool:
			for _, p := range ev.Message.Parts {
				if tr, ok := p.(core.ToolResult); ok {
					fmt.Printf("   ↳ %s ⇒ %s\n", tr.Name, truncate(oneline(partsText(tr.Content)), 80))
				}
			}
		}
	}
}

func firstURL(searchOutput string) string {
	for _, line := range strings.Split(searchOutput, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			return line
		}
	}
	return ""
}

func partsText(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

// oneline collapses all runs of whitespace (incl. newlines) to single spaces.
func oneline(s string) string { return strings.Join(strings.Fields(s), " ") }

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func indent(s string) {
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		fmt.Println("      " + line)
	}
}

func banner(title string) {
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  " + title)
	fmt.Println("════════════════════════════════════════════════════════")
}

func scene(title string) { fmt.Printf("\n── %s ──\n", title) }

const articleHTML = `<!doctype html><html><head>
<title>Go 1.23 发布</title>
<style>.nav{display:none}</style>
<script>track('pageview')</script>
</head><body>
<nav class="nav">导航栏</nav>
<h1>Go 1.23 发布要点</h1>
<p>Go 1.23 正式支持 <b>range-over-func</b> 迭代器。</p>
<p>此外改进了工具链与标准库。</p>
</body></html>`
