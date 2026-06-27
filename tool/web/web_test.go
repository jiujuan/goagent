package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// callTool invokes a tool with JSON args and returns its rendered text plus the
// IsError flag (funcTool reports handler errors as IsError results, not Go errs).
func callTool(t *testing.T, tl tool.Tool, args string) (string, bool) {
	t.Helper()
	res, err := tl.Call(&tool.Context{Context: context.Background()}, json.RawMessage(args))
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	var b strings.Builder
	for _, p := range res.Content {
		if txt, ok := p.(core.Text); ok {
			b.WriteString(txt.Text)
		}
	}
	return b.String(), res.IsError
}

func TestFetchExtractsTextAndStripsNoise(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, `<!doctype html><html><head>
			<style>.x{color:red}</style>
			<script>alert('secret')</script>
		</head><body>
			<h1>首页标题</h1>
			<p>第一段&amp;要点</p>
			<p>第二段内容</p>
		</body></html>`)
	}))
	defer srv.Close()

	got, isErr := callTool(t, Fetch(), `{"url":"`+srv.URL+`"}`)
	if isErr {
		t.Fatalf("unexpected error result: %s", got)
	}
	if strings.Contains(got, "alert(") || strings.Contains(got, "color:red") {
		t.Fatalf("script/style not stripped:\n%s", got)
	}
	for _, want := range []string{"首页标题", "第一段&要点", "第二段内容"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestFetchRejectsNonHTTPScheme(t *testing.T) {
	got, isErr := callTool(t, Fetch(), `{"url":"file:///etc/passwd"}`)
	if !isErr {
		t.Fatalf("expected error result for file:// scheme, got: %s", got)
	}
	if !strings.Contains(got, "http") {
		t.Fatalf("error should mention scheme restriction, got: %s", got)
	}
}

func TestFetchReportsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	got, isErr := callTool(t, Fetch(), `{"url":"`+srv.URL+`"}`)
	if !isErr || !strings.Contains(got, "404") {
		t.Fatalf("expected 404 error result, got isErr=%v: %s", isErr, got)
	}
}

// stubBackend records the limit it was asked for and returns canned results.
type stubBackend struct{ gotLimit int }

func (s *stubBackend) Search(_ context.Context, _ string, limit int) ([]SearchResult, error) {
	s.gotLimit = limit
	return []SearchResult{
		{Title: "标题一", URL: "https://a.example", Snippet: "摘要一"},
		{Title: "标题二", URL: "https://b.example"},
	}, nil
}

func TestSearchFormatsResultsAndRespectsLimit(t *testing.T) {
	be := &stubBackend{}
	st := Search(WithBackend(be), WithMaxResults(5))

	got, isErr := callTool(t, st, `{"query":"折叠屏"}`)
	if isErr {
		t.Fatalf("unexpected error: %s", got)
	}
	if be.gotLimit != 5 {
		t.Fatalf("backend limit = %d, want default 5", be.gotLimit)
	}
	for _, want := range []string{"标题一", "https://a.example", "摘要一", "标题二", "https://b.example"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestSearchEmptyQueryIsError(t *testing.T) {
	got, isErr := callTool(t, Search(WithBackend(&stubBackend{})), `{"query":"   "}`)
	if !isErr {
		t.Fatalf("expected error for empty query, got: %s", got)
	}
}

func TestParseDDGResults(t *testing.T) {
	sample := `
	<div class="result">
	  <a rel="nofollow" class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage&amp;rut=abc">Example &amp; Title</a>
	  <a class="result__snippet" href="x">A short <b>snippet</b> here</a>
	</div>
	<div class="result">
	  <a rel="nofollow" class="result__a" href="https://direct.example/two">Second</a>
	</div>`

	res := parseDDGResults(sample)
	if len(res) != 2 {
		t.Fatalf("got %d results, want 2: %+v", len(res), res)
	}
	if res[0].URL != "https://example.com/page" {
		t.Fatalf("uddg not decoded: %q", res[0].URL)
	}
	if res[0].Title != "Example & Title" {
		t.Fatalf("title = %q", res[0].Title)
	}
	if !strings.Contains(res[0].Snippet, "snippet here") {
		t.Fatalf("snippet = %q", res[0].Snippet)
	}
	if res[1].URL != "https://direct.example/two" {
		t.Fatalf("direct url = %q", res[1].URL)
	}
}
