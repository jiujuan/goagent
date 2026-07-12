package prompt

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/jiujuan/goagent/session"
)

// Built-in section orders, spaced 100 apart so custom sections can slot
// between them.
const (
	orderIdentity     = 100
	orderEnvironment  = 200
	orderToolGuidance = 300
	orderSessionState = 400
)

// Identity renders the agent's base persona/instruction verbatim. It is the
// home for the static prompt that previously lived in Config.Instruction.
func Identity(instruction string) Section {
	return SectionFunc{
		SecName:  "identity",
		SecOrder: orderIdentity,
		RenderFn: func(Context) (string, error) { return instruction, nil },
	}
}

// EnvOption configures the Environment section.
type EnvOption func(*envConfig)

type envConfig struct {
	now func() time.Time
}

// WithNow injects a clock so the Environment section is deterministic in tests.
func WithNow(now func() time.Time) EnvOption {
	return func(c *envConfig) { c.now = now }
}

// Environment renders the runtime environment: current date, OS, and working
// directory. The clock defaults to time.Now and can be overridden with WithNow.
func Environment(opts ...EnvOption) Section {
	cfg := envConfig{now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}
	return SectionFunc{
		SecName:  "environment",
		SecOrder: orderEnvironment,
		RenderFn: func(Context) (string, error) {
			var b strings.Builder
			b.WriteString("# Environment\n")
			fmt.Fprintf(&b, "Date: %s\n", cfg.now().Format("2006-01-02"))
			fmt.Fprintf(&b, "OS: %s\n", runtime.GOOS)
			if cwd, err := os.Getwd(); err == nil {
				fmt.Fprintf(&b, "Working directory: %s", cwd)
			} else {
				b.WriteString("Working directory: (unknown)")
			}
			return b.String(), nil
		},
	}
}

// ToolGuidance lists the agent's tools (name and description) so the model
// knows what it can call. It renders empty when the agent has no tools.
func ToolGuidance() Section {
	return SectionFunc{
		SecName:  "tool_guidance",
		SecOrder: orderToolGuidance,
		RenderFn: func(c Context) (string, error) {
			if len(c.Tools) == 0 {
				return "", nil
			}
			var b strings.Builder
			b.WriteString("# Available tools\n")
			for _, t := range c.Tools {
				fmt.Fprintf(&b, "- %s: %s\n", t.Name(), t.Description())
			}
			return b.String(), nil
		},
	}
}

// SessionState renders selected keys from the session state, in the order
// given. Missing keys are skipped; if no key is present the section is omitted.
func SessionState(keys ...string) Section {
	return SectionFunc{
		SecName:  "session_state",
		SecOrder: orderSessionState,
		RenderFn: func(c Context) (string, error) {
			if c.Session == nil || len(keys) == 0 {
				return "", nil
			}
			var state session.StateReader = c.Session.State()
			if c.SessionSnapshot != nil {
				state = c.SessionSnapshot.State()
			}
			var b strings.Builder
			for _, k := range keys {
				v, ok := state.Get(k)
				if !ok {
					continue
				}
				fmt.Fprintf(&b, "- %s: %v\n", k, v)
			}
			if b.Len() == 0 {
				return "", nil
			}
			return "# Session state\n" + b.String(), nil
		},
	}
}
