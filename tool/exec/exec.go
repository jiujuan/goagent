// Package exec provides a ready-to-use run_command tool backed by a
// sandbox.Sandbox. It is the batteries-included counterpart to the sandbox
// primitive, mirroring the tool/web subpackage pattern: construct a sandbox
// with the policy you want, wrap it with RunCommand, and drop the result into
// agent.Config.Tools.
package exec

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
)

// args is the model-facing input schema for run_command.
type args struct {
	Command string   `json:"command" desc:"executable name or path to run"`
	Args    []string `json:"args,omitempty" desc:"command arguments"`
}

// RunCommand builds a run_command tool that executes commands through sb. The
// sandbox's policy (timeout, output cap, working directory, env and command
// allow-lists) governs every invocation. Non-zero exits, timeouts, truncated
// output, and policy violations are reported back to the model as tool errors
// so it can recover; successful runs return the captured output.
func RunCommand(sb sandbox.Sandbox) tool.Tool {
	return tool.New("run_command",
		"Run a command in a restricted sandbox and return its output.",
		func(ctx *tool.Context, in args) (string, error) {
			out, err := sb.Run(ctx, sandbox.Spec{Command: in.Command, Args: in.Args})
			if err != nil {
				// Configuration/policy violation: surface to the model.
				return "", err
			}
			text := format(in, out)
			if out.ExitCode != 0 || out.TimedOut {
				// Report a failed run as a tool error so the model can adjust.
				return "", fmt.Errorf("%s", text)
			}
			return text, nil
		})
}

// format renders an Outcome into a compact, model-readable report.
func format(in args, out *sandbox.Outcome) string {
	var b strings.Builder
	fmt.Fprintf(&b, "$ %s", in.Command)
	for _, a := range in.Args {
		fmt.Fprintf(&b, " %s", a)
	}
	b.WriteByte('\n')

	switch {
	case out.TimedOut:
		fmt.Fprintf(&b, "[timed out after %s]\n", out.Duration)
	default:
		fmt.Fprintf(&b, "[exit %d in %s]\n", out.ExitCode, out.Duration)
	}
	if out.Truncated {
		b.WriteString("[output truncated]\n")
	}
	if len(out.Stdout) > 0 {
		fmt.Fprintf(&b, "stdout:\n%s\n", out.Stdout)
	}
	if len(out.Stderr) > 0 {
		fmt.Fprintf(&b, "stderr:\n%s\n", out.Stderr)
	}
	return strings.TrimRight(b.String(), "\n")
}
