// Command websearch is a tutorial for the web tools: web_search and web_fetch.
// They mount on an agent so the model can look things up. web.Search uses a
// pluggable Backend (default: dependency-free DuckDuckGo HTML scraping;
// WithBackend swaps in Bing/Brave/Tavily/…); web.Fetch downloads a page and
// strips it to plain text.
//
// This demo runs OFFLINE by default (a fake search backend + mock model), so it
// is deterministic. Set WEB_LIVE=1 to use the real DuckDuckGo backend instead
// (network required).
//
//	go run ./examples/websearch
//	WEB_LIVE=1 go run ./examples/websearch
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool/web"
)

// fakeBackend is an offline, deterministic search backend.
type fakeBackend struct{}

func (fakeBackend) Search(_ context.Context, query string, limit int) ([]web.SearchResult, error) {
	return []web.SearchResult{
		{Title: "Go 语言官网", URL: "https://go.dev", Snippet: "Go 是 Google 设计的开源编程语言,擅长并发。"},
		{Title: "Go 并发模型", URL: "https://go.dev/blog/pipelines", Snippet: "goroutine + channel 的 CSP 风格并发。"},
	}, nil
}

func main() {
	ctx := context.Background()

	// Default: offline fake backend; WEB_LIVE=1 uses the real DuckDuckGo backend.
	var searchOpts []web.Option
	if os.Getenv("WEB_LIVE") == "" {
		searchOpts = append(searchOpts, web.WithBackend(fakeBackend{}))
		fmt.Println("(离线模式:使用假搜索后端;设 WEB_LIVE=1 走真实 DuckDuckGo)")
	} else {
		fmt.Println("(联网模式:真实 DuckDuckGo 搜索)")
	}

	// mock model: turn 0 searches, turn 1 answers from results.
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("根据搜索结果:" + firstLine(tr.Content[0].(core.Text).Text))
		}
		return mock.CallTool("c1", "web_search", `{"query":"Go 并发"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("需要最新信息时先 web_search,再用 web_fetch 打开链接阅读后作答。"),
		agent.WithTools(web.Search(searchOpts...), web.Fetch()),
	)
	if err != nil {
		log.Fatal(err)
	}

	for ev, err := range a.Stream(ctx, "Go 的并发是怎么做的?").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			fmt.Printf("🔧 %s 返回:%s\n", e.Result.Name, firstLine(e.Result.Content[0].(core.Text).Text))
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}

func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' {
			return s[:i] + " …"
		}
	}
	return s
}
