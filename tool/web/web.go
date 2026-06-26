// Package web provides ready-to-use tools that let an agent search the public
// web and read page content. Both are constructed with functional options and
// return tool.Tool values you drop straight into agent.Config.Tools:
//
//	ag := agent.New(agent.Config{
//	    Name:        "researcher",
//	    Instruction: "需要最新信息时先 web_search，再用 web_fetch 打开链接阅读。",
//	    Model:       model,
//	    Tools:       web.Tools(), // web_search + web_fetch
//	})
//
// web_search defaults to a dependency-free DuckDuckGo HTML backend; swap in a
// production search API with WithBackend. Neither tool needs an API key, and the
// package pulls in only the standard library.
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
	"time"

	"github.com/jiujuan/goagent/tool"
)

const (
	defaultUA       = "goagent-web/0.1 (+https://github.com/jiujuan/goagent)"
	defaultMaxBytes = 512 << 10 // 512 KiB raw-download cap per fetch
	defaultResults  = 5
	maxOutputRunes  = 12000 // cap returned text to protect the model's context
)

type config struct {
	client     *http.Client
	userAgent  string
	maxBytes   int64
	backend    Backend
	maxResults int
}

// Option configures the web tools.
type Option func(*config)

// WithHTTPClient sets the HTTP client used by both tools (and the default search
// backend). Use it to control timeouts, proxies, or transport.
func WithHTTPClient(c *http.Client) Option { return func(cfg *config) { cfg.client = c } }

// WithUserAgent overrides the User-Agent header sent with each request.
func WithUserAgent(ua string) Option { return func(cfg *config) { cfg.userAgent = ua } }

// WithMaxBytes caps how many bytes web_fetch downloads from a page.
func WithMaxBytes(n int64) Option { return func(cfg *config) { cfg.maxBytes = n } }

// WithMaxResults sets the default number of web_search results.
func WithMaxResults(n int) Option { return func(cfg *config) { cfg.maxResults = n } }

// WithBackend plugs in a custom search backend (e.g. a production search API),
// replacing the default DuckDuckGo HTML backend.
func WithBackend(b Backend) Option { return func(cfg *config) { cfg.backend = b } }

func newConfig(opts ...Option) *config {
	cfg := &config{
		client:     &http.Client{Timeout: 15 * time.Second},
		userAgent:  defaultUA,
		maxBytes:   defaultMaxBytes,
		maxResults: defaultResults,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.client == nil {
		cfg.client = &http.Client{Timeout: 15 * time.Second}
	}
	if cfg.maxBytes <= 0 {
		cfg.maxBytes = defaultMaxBytes
	}
	if cfg.maxResults <= 0 {
		cfg.maxResults = defaultResults
	}
	if cfg.backend == nil {
		cfg.backend = &duckDuckGo{client: cfg.client, userAgent: cfg.userAgent}
	}
	return cfg
}

// Tools returns both web_search and web_fetch sharing one configuration.
func Tools(opts ...Option) []tool.Tool {
	cfg := newConfig(opts...)
	return []tool.Tool{searchTool(cfg), fetchTool(cfg)}
}

// Search returns just the web_search tool.
func Search(opts ...Option) tool.Tool { return searchTool(newConfig(opts...)) }

// Fetch returns just the web_fetch tool.
func Fetch(opts ...Option) tool.Tool { return fetchTool(newConfig(opts...)) }

// --- web_fetch --------------------------------------------------------------

type fetchArgs struct {
	URL string `json:"url" desc:"要抓取的网页地址（http/https）"`
}

func fetchTool(cfg *config) tool.Tool {
	return tool.New("web_fetch",
		"抓取指定 URL 的网页并返回纯文本正文（已剥离 HTML 标签、脚本与样式）。需要阅读某个网页的具体内容时调用。",
		func(ctx *tool.Context, in fetchArgs) (string, error) {
			return cfg.fetch(ctx, strings.TrimSpace(in.URL))
		})
}

func (cfg *config) fetch(ctx context.Context, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("URL 无法解析: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("仅支持 http/https，收到 %q", u.Scheme)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", cfg.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8")

	resp, err := cfg.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.maxBytes))
	if err != nil {
		return "", err
	}

	ct := resp.Header.Get("Content-Type")
	text := string(body)
	switch {
	case strings.Contains(ct, "html") || looksLikeHTML(text):
		text = htmlToText(text)
	case ct == "" || strings.Contains(ct, "text") || strings.Contains(ct, "json"):
		text = strings.TrimSpace(text)
	default:
		return "", fmt.Errorf("不支持的内容类型: %s", ct)
	}
	if text == "" {
		return "（页面无可提取的文本内容）", nil
	}
	return truncateRunes(text, maxOutputRunes), nil
}

// --- HTML → text ------------------------------------------------------------

var (
	reScriptStyle = regexp.MustCompile(`(?is)<(script|style|noscript|template|svg)[^>]*>.*?</(script|style|noscript|template|svg)>`)
	reComment     = regexp.MustCompile(`(?s)<!--.*?-->`)
	reBlockBreak  = regexp.MustCompile(`(?i)</(p|div|li|tr|h[1-6]|section|article|header|footer|ul|ol|table|blockquote)>|<br\s*/?>`)
	reTag         = regexp.MustCompile(`(?s)<[^>]+>`)
	reInlineSpace = regexp.MustCompile(`[ \t\f\v]+`)
	reNewlines    = regexp.MustCompile(`\n{3,}`)
)

func looksLikeHTML(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "<!doctype html") || strings.Contains(s, "<html") || strings.Contains(s, "<body")
}

// htmlToText strips scripts/styles/tags, decodes entities, and normalizes
// whitespace into a compact, model-friendly plain-text rendering.
func htmlToText(s string) string {
	s = reScriptStyle.ReplaceAllString(s, " ")
	s = reComment.ReplaceAllString(s, " ")
	s = reBlockBreak.ReplaceAllString(s, "\n")
	s = reTag.ReplaceAllString(s, "")
	s = html.UnescapeString(s)

	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(reInlineSpace.ReplaceAllString(line, " "))
		if line != "" {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return strings.TrimSpace(reNewlines.ReplaceAllString(b.String(), "\n\n"))
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "\n…[内容已截断]"
}
