package skills

import (
	"context"
	"encoding/json"
	"reflect"
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

	// Level 2: load SKILL.md body, with the pre-approved allowed-tools line.
	out, isErr := call(t, tl, map[string]any{"name": "pdf"})
	if isErr {
		t.Fatalf("use_skill(pdf) errored: %s", out)
	}
	if !strings.Contains(out, "# Skill: pdf") {
		t.Errorf("missing skill header: %s", out)
	}
	if !strings.Contains(out, "Allowed tools (pre-approved; other tools will ask for human approval): run_command, use_skill") {
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

// TestUseSkillActivatesSkill pins the contract skills.Gate depends on: loading a
// skill returns a state op, applying it makes the skill active, and neither a
// second load nor a resource read duplicates it.
func TestUseSkillActivatesSkill(t *testing.T) {
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	tl := Tool(lib)
	st := &core.State{}

	run := func(args map[string]any) *tool.Result {
		t.Helper()
		raw, _ := json.Marshal(args)
		res, err := tl.Call(&tool.Context{Context: context.Background(), State: st}, raw)
		if err != nil {
			t.Fatalf("Call returned Go error: %v", err)
		}
		st.Apply(res.State...)
		return res
	}

	res := run(map[string]any{"name": "pdf"})
	if res.IsError {
		t.Fatalf("use_skill(pdf) errored: %+v", res.Content)
	}
	if got := Active(st); !reflect.DeepEqual(got, []string{"pdf"}) {
		t.Fatalf("Active = %v, want [pdf]", got)
	}

	run(map[string]any{"name": "pdf"})
	run(map[string]any{"name": "pdf", "resource": "forms.md"})
	if got := Active(st); !reflect.DeepEqual(got, []string{"pdf"}) {
		t.Errorf("Active = %v, want no duplicates after repeat loads", got)
	}

	if res := run(map[string]any{"name": "nope"}); !res.IsError {
		t.Fatalf("unknown skill should be a tool error, got %+v", res)
	} else if len(res.State) != 0 {
		t.Errorf("a failed load must not activate anything: %+v", res.State)
	}
}

// The hand-written tool keeps the model-facing surface the typed constructor
// gave it: same name, description and argument schema.
func TestUseSkillSurfaceUnchanged(t *testing.T) {
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatal(err)
	}
	tl := Tool(lib)

	if tl.Name() != "use_skill" {
		t.Errorf("Name = %q", tl.Name())
	}
	if !strings.Contains(tl.Description(), "Load a skill's full instructions on demand") {
		t.Errorf("Description changed: %q", tl.Description())
	}
	var schema struct {
		Properties map[string]any `json:"properties"`
		Required   []string       `json:"required"`
	}
	if err := json.Unmarshal(tl.Schema(), &schema); err != nil {
		t.Fatalf("Schema: %v", err)
	}
	for _, p := range []string{"name", "resource"} {
		if _, ok := schema.Properties[p]; !ok {
			t.Errorf("schema lost property %q: %s", p, tl.Schema())
		}
	}
	if !reflect.DeepEqual(schema.Required, []string{"name"}) {
		t.Errorf("required = %v, want [name]", schema.Required)
	}
}
