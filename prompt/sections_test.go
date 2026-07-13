package prompt

import (
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

func TestEnvironmentDeterministicClock(t *testing.T) {
	fixed := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	out, err := Environment(WithNow(func() time.Time { return fixed })).Render(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Date: 2026-06-25") {
		t.Fatalf("expected injected date, got: %q", out)
	}
}

func TestToolGuidance(t *testing.T) {
	weather := tool.New("get_weather", "查询天气",
		func(_ *tool.Context, _ struct{}) (string, error) { return "", nil })

	out, err := ToolGuidance().Render(Context{Tools: []tool.Tool{weather}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "get_weather") || !strings.Contains(out, "查询天气") {
		t.Fatalf("tool not listed: %q", out)
	}

	// No tools -> omitted.
	empty, err := ToolGuidance().Render(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if empty != "" {
		t.Fatalf("expected empty, got: %q", empty)
	}
}

func TestSessionState(t *testing.T) {
	ctx := Context{State: &core.State{KV: map[string]any{"plan": "ship it"}}}

	out, err := SessionState("plan", "missing").Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "plan: ship it") {
		t.Fatalf("present key not rendered: %q", out)
	}
	if strings.Contains(out, "missing") {
		t.Fatalf("absent key should be skipped: %q", out)
	}

	// All keys absent -> omitted.
	none, err := SessionState("nope").Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if none != "" {
		t.Fatalf("expected empty, got: %q", none)
	}
}

func TestIdentity(t *testing.T) {
	out, err := Identity("you are helpful").Render(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if out != "you are helpful" {
		t.Fatalf("identity verbatim failed: %q", out)
	}
}
