package web

import (
	"context"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/jiujuan/goagent/tool"
)

// SearchResult is a single web_search hit.
type SearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet,omitempty"`
}

// Backend performs a web search. Implement it to plug in a production search
// provider (Bing, Brave, Tavily, SerpAPI, …) via WithBackend; the package's
// default is a dependency-free DuckDuckGo HTML backend.
type Backend interface {
	Search(ctx context.Context, query string, limit int) ([]SearchResult, error)
}

type searchArgs struct {
	Query string `json:"query" desc:"搜索关键词或问题"`
	Limit int    `json:"limit,omitempty" desc:"返回结果条数上限（默认 5，最多 10）"`
}

func searchTool(cfg *config) tool.Tool {
	return tool.New("web_search",
		"在公开互联网上搜索，返回若干条「标题 + URL + 摘要」。需要查找最新信息或核实事实时调用；随后可用 web_fetch 打开某条 URL 阅读全文。",
		func(ctx *tool.Context, in searchArgs) (string, error) {
			q := strings.TrimSpace(in.Query)
			if q == "" {
				return "", fmt.Errorf("query 不能为空")
			}
			limit := in.Limit
			if limit <= 0 {
				limit = cfg.maxResults
			}
			if limit > 10 {
				limit = 10
			}

			results, err := cfg.backend.Search(ctx, q, limit)
			if err != nil {
				return "", err
			}
			if len(results) == 0 {
				return "（未找到相关结果）", nil
			}

			var b strings.Builder
			for i, r := range results {
				fmt.Fprintf(&b, "%d. %s\n   %s\n", i+1, r.Title, r.URL)
				if r.Snippet != "" {
					fmt.Fprintf(&b, "   %s\n", r.Snippet)
				}
			}
			return strings.TrimRight(b.String(), "\n"), nil
		})
}

// --- DuckDuckGo backend -----------------------------------------------------

// duckDuckGo is the default Backend: it queries DuckDuckGo's HTML endpoint and
// scrapes the result list. It needs no API key, but HTML scraping is inherently
// brittle — inject a real search API with WithBackend for production use.
type duckDuckGo struct {
	client    *http.Client
	userAgent string
}

func (d *duckDuckGo) Search(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	endpoint := "https://html.duckduckgo.com/html/?q=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("搜索后端返回 HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	results := parseDDGResults(string(body))
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

var (
	reResultA = regexp.MustCompile(`(?s)<a[^>]*class="[^"]*result__a[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>`)
	reSnippet = regexp.MustCompile(`(?s)<a[^>]*class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</a>`)
)

// parseDDGResults extracts the result list from a DuckDuckGo HTML page. Split
// out from the HTTP call so it can be unit-tested without network access.
func parseDDGResults(htmlBody string) []SearchResult {
	titles := reResultA.FindAllStringSubmatch(htmlBody, -1)
	snippets := reSnippet.FindAllStringSubmatch(htmlBody, -1)

	out := make([]SearchResult, 0, len(titles))
	for i, m := range titles {
		r := SearchResult{
			URL:   decodeDDGURL(m[1]),
			Title: cleanText(m[2]),
		}
		if i < len(snippets) {
			r.Snippet = cleanText(snippets[i][1])
		}
		if r.Title == "" || r.URL == "" {
			continue
		}
		out = append(out, r)
	}
	return out
}

// decodeDDGURL unwraps DuckDuckGo's redirect links (…/l/?uddg=<encoded>) to the
// real destination URL.
func decodeDDGURL(href string) string {
	href = html.UnescapeString(href)
	if strings.HasPrefix(href, "//") {
		href = "https:" + href
	}
	u, err := url.Parse(href)
	if err != nil {
		return href
	}
	if dest := u.Query().Get("uddg"); dest != "" {
		return dest
	}
	return href
}

// cleanText strips inline tags, decodes entities, and collapses whitespace.
func cleanText(s string) string {
	s = reTag.ReplaceAllString(s, "")
	s = html.UnescapeString(s)
	return strings.TrimSpace(reInlineSpace.ReplaceAllString(s, " "))
}
