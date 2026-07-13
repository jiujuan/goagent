package skills

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
)

// fakeSandbox captures the Spec it is handed and reads back the materialized
// script file, so script execution can be tested without a real interpreter on
// the host. It returns a canned Outcome/error.
type fakeSandbox struct {
	spec    sandbox.Spec
	script  string // contents of the materialized temp file (spec.Args[0])
	tmpPath string
	out     *sandbox.Outcome
	err     error
}

func (f *fakeSandbox) Run(_ context.Context, spec sandbox.Spec) (*sandbox.Outcome, error) {
	f.spec = spec
	if len(spec.Args) > 0 {
		f.tmpPath = spec.Args[0]
		if b, err := os.ReadFile(spec.Args[0]); err == nil {
			f.script = string(b)
		}
	}
	if f.out == nil && f.err == nil {
		f.out = &sandbox.Outcome{ExitCode: 0, Stdout: []byte("ok")}
	}
	return f.out, f.err
}

// callScript invokes a run_skill_script tool with the given args.
func callScript(t *testing.T, tl tool.Tool, args map[string]any) (string, bool) {
	t.Helper()
	raw, _ := json.Marshal(args)
	res, err := tl.Call(&tool.Context{Context: context.Background()}, raw)
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	var b strings.Builder
	for _, p := range res.Content {
		if tx, ok := p.(core.Text); ok {
			b.WriteString(tx.Text)
		}
	}
	return b.String(), res.IsError
}

func loadPDF(t *testing.T) *Library {
	t.Helper()
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	return lib
}

func TestScriptToolInterpreterByExtension(t *testing.T) {
	lib := loadPDF(t)

	tests := []struct {
		script   string
		wantCmd  string
		wantBody string
	}{
		{"scripts/fill.sh", "sh", "filled pdf"},
		{"scripts/fill.py", "python3", "via python"},
	}
	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			fake := &fakeSandbox{}
			tl := ScriptTool(lib, fake)
			out, isErr := callScript(t, tl, map[string]any{"skill": "pdf", "script": tt.script, "args": []string{"x"}})
			if isErr {
				t.Fatalf("unexpected tool error: %s", out)
			}
			if fake.spec.Command != tt.wantCmd {
				t.Errorf("interpreter = %q, want %q", fake.spec.Command, tt.wantCmd)
			}
			// Temp script path is arg[0]; the model-supplied arg follows.
			if len(fake.spec.Args) != 2 || fake.spec.Args[1] != "x" {
				t.Errorf("args = %v, want [<tmp> x]", fake.spec.Args)
			}
			if !strings.Contains(fake.script, tt.wantBody) {
				t.Errorf("materialized script = %q, want it to contain %q", fake.script, tt.wantBody)
			}
			// The report shows the logical script path, not the temp path.
			if !strings.Contains(out, tt.script) || strings.Contains(out, fake.tmpPath) {
				t.Errorf("report should reference %q not the temp path: %s", tt.script, out)
			}
		})
	}
}

func TestScriptToolTempCleanup(t *testing.T) {
	lib := loadPDF(t)
	fake := &fakeSandbox{}
	tl := ScriptTool(lib, fake)
	if _, isErr := callScript(t, tl, map[string]any{"skill": "pdf", "script": "scripts/fill.sh"}); isErr {
		t.Fatal("unexpected error")
	}
	if fake.tmpPath == "" {
		t.Fatal("sandbox never received a script path")
	}
	if _, err := os.Stat(fake.tmpPath); !os.IsNotExist(err) {
		t.Errorf("temp script %q should be removed after the run, stat err = %v", fake.tmpPath, err)
	}
}

func TestScriptToolErrors(t *testing.T) {
	lib := loadPDF(t)
	tl := ScriptTool(lib, &fakeSandbox{})

	cases := []struct {
		name string
		args map[string]any
	}{
		{"unknown skill", map[string]any{"skill": "nope", "script": "scripts/fill.sh"}},
		{"unsupported ext", map[string]any{"skill": "pdf", "script": "forms.md"}},
		{"path escape", map[string]any{"skill": "pdf", "script": "../pdf/SKILL.md"}},
		{"missing script", map[string]any{"skill": "pdf", "script": "scripts/nope.sh"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, isErr := callScript(t, tl, c.args); !isErr {
				t.Errorf("expected a tool error for %s", c.name)
			}
		})
	}
}

func TestScriptToolNonZeroExit(t *testing.T) {
	lib := loadPDF(t)
	fake := &fakeSandbox{out: &sandbox.Outcome{ExitCode: 1, Stderr: []byte("boom")}}
	tl := ScriptTool(lib, fake)
	out, isErr := callScript(t, tl, map[string]any{"skill": "pdf", "script": "scripts/fill.sh"})
	if !isErr {
		t.Fatal("non-zero exit should be a tool error")
	}
	if !strings.Contains(out, "exit 1") || !strings.Contains(out, "boom") {
		t.Errorf("report missing exit/stderr: %s", out)
	}
}

func TestScriptToolWithInterpreterOverride(t *testing.T) {
	lib := loadPDF(t)
	fake := &fakeSandbox{}
	// Pin .py to "python" and add a brand-new extension.
	tl := ScriptTool(lib, fake, WithInterpreter(".py", "python"), WithInterpreter("rb", "ruby"))

	if _, isErr := callScript(t, tl, map[string]any{"skill": "pdf", "script": "scripts/fill.py"}); isErr {
		t.Fatal("unexpected error")
	}
	if fake.spec.Command != "python" {
		t.Errorf("override ignored: interpreter = %q, want python", fake.spec.Command)
	}
}
