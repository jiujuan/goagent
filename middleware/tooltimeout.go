package middleware

import (
	"context"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// ToolTimeoutOptions configures ToolTimeout. Every field defaults to no bound,
// so a zero-value configuration is a middleware that changes nothing.
type ToolTimeoutOptions struct {
	// Default bounds every call whose tool has no PerTool entry. Zero or
	// negative means no bound.
	Default time.Duration
	// PerTool overrides Default by tool name. An entry that is present but zero
	// or negative unbounds that tool and wins over Default — that is how one
	// long-running job is let out from under a general limit.
	PerTool map[string]time.Duration
	// Exempt names tools that are never bounded. Same effect as a zero PerTool
	// entry, spelled for readability.
	Exempt []string
}

// ToolTimeout gives individual tools a deadline by implementing
// agent.ToolContexter: it wraps one call's context in a timeout, and the loop
// decides what the model hears when that deadline passes (agent.WithToolTimeout
// covers the abandoning side).
//
// It is the per-tool counterpart to WithToolTimeout, which is one number for a
// whole agent. The two combine by taking the earlier deadline: this seam can
// tighten the agent default but never widen it, because a context carries an
// earlier deadline, not a later one. To give a tool more time than the agent
// default allows, set WithToolTimeout(0) and express every bound here instead.
//
// The options are read-only once configured and no per-run state is kept, so one
// instance is safe to share across concurrent runs and batches.
func ToolTimeout(o ToolTimeoutOptions) agent.Middleware {
	return &toolTimeout{
		defaultBound: o.Default,
		perTool:      o.PerTool,
		exempt:       toSet(o.Exempt),
	}
}

type toolTimeout struct {
	agent.BaseMiddleware

	defaultBound time.Duration
	perTool      map[string]time.Duration
	exempt       map[string]bool
}

// bound reports how long a call to name may run. ok is false when the call is
// unbounded — exempt, unlisted with no default, or given a non-positive entry —
// and the duration is then 0 rather than the configured number, so an ignored ok
// cannot smuggle a negative timeout into a context.
func (t *toolTimeout) bound(name string) (time.Duration, bool) {
	if t.exempt[name] {
		return 0, false
	}
	d := t.defaultBound
	if v, ok := t.perTool[name]; ok {
		d = v
	}
	if d <= 0 {
		return 0, false
	}
	return d, true
}

// ToolContext implements agent.ToolContexter. The cancel goes back to the loop,
// which runs it when the call ends — that is what keeps the timer from staying
// attached to the run context for the rest of the run.
func (t *toolTimeout) ToolContext(_ *agent.LoopContext, ctx context.Context, call *core.ToolCall) (context.Context, context.CancelFunc) {
	d, ok := t.bound(call.Name)
	if !ok {
		return ctx, nil
	}
	return context.WithTimeout(ctx, d)
}

var (
	_ agent.Middleware    = (*toolTimeout)(nil)
	_ agent.ToolContexter = (*toolTimeout)(nil)
)
