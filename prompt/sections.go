package prompt

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
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
	now     func() time.Time
	workDir string
}

// WithNow injects a clock so the Environment section is deterministic in tests.
func WithNow(now func() time.Time) EnvOption {
	return func(c *envConfig) { c.now = now }
}

// WithWorkingDir overrides the directory reported as "Working directory". Use it
// whenever commands actually run somewhere other than the process's cwd — a
// sandboxed or multi-workspace agent — so the model is never told a cwd its
// tools do not use. The default remains os.Getwd.
func WithWorkingDir(dir string) EnvOption {
	return func(c *envConfig) { c.workDir = dir }
}

// Environment renders the runtime environment: current date, OS, and working
// directory. The clock defaults to time.Now and can be overridden with WithNow;
// the directory can be overridden with WithWorkingDir.
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
			dir := cfg.workDir
			if dir == "" {
				cwd, err := os.Getwd()
				if err != nil {
					cwd = "(unknown)"
				}
				dir = cwd
			}
			fmt.Fprintf(&b, "Working directory: %s", dir)
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
			if c.State == nil || c.State.KV == nil || len(keys) == 0 {
				return "", nil
			}
			var b strings.Builder
			for _, k := range keys {
				v, ok := c.State.KV[k]
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
