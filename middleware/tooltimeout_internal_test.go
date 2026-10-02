package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

type probeKey struct{}

func TestToolTimeoutBoundSelection(t *testing.T) {
	cases := []struct {
		name string
		opts ToolTimeoutOptions
		tool string
		want time.Duration
		ok   bool
	}{
		{
			name: "zero options bound nothing",
			opts: ToolTimeoutOptions{},
			tool: "any",
		},
		{
			name: "default bounds an unlisted tool",
			opts: ToolTimeoutOptions{Default: 5 * time.Second},
			tool: "any",
			want: 5 * time.Second, ok: true,
		},
		{
			name: "per tool wins over default",
			opts: ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"slow": time.Minute}},
			tool: "slow",
			want: time.Minute, ok: true,
		},
		{
			name: "per tool zero unbounds against default",
			opts: ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"slow": 0}},
			tool: "slow",
		},
		{
			name: "listing one tool leaves the others on default",
			opts: ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"slow": time.Minute}},
			tool: "other",
			want: 5 * time.Second, ok: true,
		},
		{
			name: "exempt beats a per tool entry",
			opts: ToolTimeoutOptions{Default: 5 * time.Second, PerTool: map[string]time.Duration{"poll": time.Second}, Exempt: []string{"poll"}},
			tool: "poll",
		},
		{
			name: "negative default is no bound",
			opts: ToolTimeoutOptions{Default: -time.Second},
			tool: "any",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mw := ToolTimeout(tc.opts).(*toolTimeout)
			got, ok := mw.bound(tc.tool)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("bound(%q) = %v, %v; want %v, %v", tc.tool, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestToolTimeoutCancelReleasesTimer: the cancel handed back to the loop is what
// releases the derived context. Returning it is the whole point of the
// ToolContexter signature — a timer that is never cancelled stays attached to its
// parent for the rest of the run, and a run makes many tool calls.
func TestToolTimeoutCancelReleasesTimer(t *testing.T) {
	mw := ToolTimeout(ToolTimeoutOptions{Default: time.Hour}).(*toolTimeout)

	ctx, cancel := mw.ToolContext(nil, context.Background(), &core.ToolCall{Name: "t"})
	if cancel == nil {
		t.Fatal("a bounded call came back with no cancel")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("a bounded call came back with no deadline")
	}

	cancel()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("after cancel: ctx.Err() = %v, want context.Canceled", err)
	}
}

// TestToolTimeoutUnboundedPassesContextThrough: when a tool is exempt or has no
// configured bound, the incoming context must be returned untouched, so an
// unbounded agent keeps exactly the context it had before this middleware
// existed (and the run context's tool.Updater capability stays visible).
func TestToolTimeoutUnboundedPassesContextThrough(t *testing.T) {
	mw := ToolTimeout(ToolTimeoutOptions{Exempt: []string{"poll"}}).(*toolTimeout)

	base := context.WithValue(context.Background(), probeKey{}, "v")
	ctx, cancel := mw.ToolContext(nil, base, &core.ToolCall{Name: "poll"})
	if ctx != base {
		t.Fatal("an exempt call derived a new context")
	}
	if cancel != nil {
		t.Fatal("an exempt call returned a cancel it had nothing to justify")
	}
	if ctx.Value(probeKey{}) != "v" {
		t.Fatal("the pass-through context lost its values")
	}
}
