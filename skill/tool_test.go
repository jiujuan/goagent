package skill

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// call is a small helper that invokes a tool with JSON args and returns the
// flattened text plus the error flag.
func call(t *testing.T, tl tool.Tool, args map[string]any) (string, bool) {
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

func TestUseSkillTool(t *testing.T) {
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	tl := Tool(lib)

	if tl.Name() != "use_skill" {
		t.Errorf("Name = %q", tl.Name())
	}

	// Level 2: load SKILL.md body, with the advisory allowed-tools line.
	out, isErr := call(t, tl, map[string]any{"name": "pdf"})
	if isErr {
		t.Fatalf("use_skill(pdf) errored: %s", out)
	}
	if !strings.Contains(out, "# Skill: pdf") {
		t.Errorf("missing skill header: %s", out)
	}
	if !strings.Contains(out, "Allowed tools: run_command, use_skill") {
		t.Errorf("missing allowed-tools line: %s", out)
	}
	if !strings.Contains(out, "Working with PDFs") {
		t.Errorf("missing body: %s", out)
	}

	// Level 3: read a bundled resource verbatim (no header wrapping).
	out, isErr = call(t, tl, map[string]any{"name": "pdf", "resource": "forms.md"})
	if isErr {
		t.Fatalf("use_skill(pdf, forms.md) errored: %s", out)
	}
	if !strings.Contains(out, "# Form fields") || strings.Contains(out, "# Skill: pdf") {
		t.Errorf("resource should be raw file contents: %s", out)
	}

	// Unknown skill -> tool error (data, not Go error).
	out, isErr = call(t, tl, map[string]any{"name": "nope"})
	if !isErr {
		t.Errorf("unknown skill should be a tool error, got: %s", out)
	}

	// Escaping resource path -> tool error.
	_, isErr = call(t, tl, map[string]any{"name": "pdf", "resource": "../noname/SKILL.md"})
	if !isErr {
		t.Error("path-escaping resource should be a tool error")
	}
}
