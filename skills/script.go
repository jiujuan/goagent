package skills

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
)

// defaultInterpreters maps a script file extension to the interpreter command
// used to run it. Override or extend with WithInterpreter.
func defaultInterpreters() map[string]string {
	return map[string]string{
		".sh":   "sh",
		".bash": "bash",
		".py":   "python3",
		".js":   "node",
	}
}

// scriptConfig holds the resolved options for ScriptTool.
type scriptConfig struct {
	interp map[string]string
}

// ScriptOption configures ScriptTool.
type ScriptOption func(*scriptConfig)

// WithInterpreter overrides or adds the interpreter command for a file
// extension (the leading dot is optional, e.g. "rb" or ".rb"). Use it to add a
// language (WithInterpreter("rb", "ruby")) or pin a command name to what the
// host and the sandbox allow-list provide (WithInterpreter(".py", "python")).
func WithInterpreter(ext, command string) ScriptOption {
	return func(c *scriptConfig) {
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		c.interp[strings.ToLower(ext)] = command
	}
}

// scriptArgs is the model-facing input schema for run_skill_script.
type scriptArgs struct {
	Skill  string   `json:"skill" desc:"name of the skill that bundles the script (as listed under Active Skills)"`
	Script string   `json:"script" desc:"path of the script within the skill, e.g. \"scripts/build.py\""`
	Args   []string `json:"args,omitempty" desc:"arguments passed to the script"`
}

// ScriptTool builds the run_skill_script tool, which executes a script bundled
// inside a skill (Level 3) through the sandbox sb. The script is read from the
// skill's filesystem — so it works whether skills come from disk or an embed.FS
// — written to a temporary file, and run by the interpreter chosen from its
// extension (.sh, .bash, .py, .js by default; extend with WithInterpreter).
//
// The sandbox governs every run (timeout, output cap, working directory, env
// and command allow-lists), so its Policy.AllowedCommands must include the
// interpreter names (e.g. "sh", "python3"). Unknown skills, out-of-bounds
// script paths, unsupported extensions, and non-zero exits come back as tool
// errors the model can recover from; policy violations surface as Go errors.
//
// Scripts run with the sandbox's working directory as their cwd, not the skill
// directory: a script that needs a bundled data file should have the model read
// it with use_skill rather than open it relative to itself.
func ScriptTool(lib *Library, sb sandbox.Sandbox, opts ...ScriptOption) tool.Tool {
	cfg := scriptConfig{interp: defaultInterpreters()}
	for _, o := range opts {
		o(&cfg)
	}

	return tool.New("run_skill_script",
		"Run a script bundled in a skill (.sh, .bash, .py, .js) in the sandbox and return its output. Give the skill name and the script's path within the skill (e.g. \"scripts/run.py\").",
		func(ctx *tool.Context, in scriptArgs) (string, error) {
			s, ok := lib.Get(strings.TrimSpace(in.Skill))
			if !ok {
				return "", fmt.Errorf("unknown skill %q; use one of the names listed under Active Skills", in.Skill)
			}

			data, err := s.Resource(in.Script)
			if err != nil {
				return "", fmt.Errorf("read script %q from skill %q: %w", in.Script, in.Skill, err)
			}

			ext := strings.ToLower(path.Ext(in.Script))
			command, ok := cfg.interp[ext]
			if !ok {
				return "", fmt.Errorf("unsupported script type %q for %q (supported: %s)", ext, in.Script, supportedExts(cfg.interp))
			}

			tmp, err := writeTempScript(in.Script, data)
			if err != nil {
				return "", err
			}
			defer os.Remove(tmp)

			out, err := sb.Run(ctx, sandbox.Spec{
				Command: command,
				Args:    append([]string{tmp}, in.Args...),
			})
			if err != nil {
				// Configuration/policy violation (e.g. interpreter not allow-listed).
				return "", err
			}

			text := formatScriptRun(command, in, out)
			if out.ExitCode != 0 || out.TimedOut {
				return "", fmt.Errorf("%s", text)
			}
			return text, nil
		})
}

// writeTempScript materializes a script's bytes to a temporary file carrying
// the script's extension, so the interpreter can read it by absolute path
// regardless of the sandbox working directory. The caller removes the file.
func writeTempScript(name string, data []byte) (string, error) {
	f, err := os.CreateTemp("", "skill-script-*"+path.Ext(name))
	if err != nil {
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// supportedExts lists the configured extensions for an error message.
func supportedExts(interp map[string]string) string {
	exts := make([]string, 0, len(interp))
	for e := range interp {
		exts = append(exts, e)
	}
	sort.Strings(exts)
	return strings.Join(exts, ", ")
}

// formatScriptRun renders a sandbox Outcome into a compact, model-readable
// report. It shows the logical command (interpreter + script + args) rather
// than the opaque temp-file path.
func formatScriptRun(command string, in scriptArgs, out *sandbox.Outcome) string {
	var b strings.Builder
	fmt.Fprintf(&b, "$ %s %s", command, in.Script)
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
