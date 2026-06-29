package skills

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jiujuan/goagent/prompt"
)

func TestPromptSection(t *testing.T) {
	lib := mapLib(t, map[string]string{
		"alpha/SKILL.md": "---\nname: alpha\ndescription: does A\n---\nbody",
		"beta/SKILL.md":  "---\nname: beta\ndescription: does B\n---\nbody",
	})

	sec := PromptSection(lib)
	if sec.Name() != "skills" {
		t.Errorf("Name = %q", sec.Name())
	}
	if sec.Order() <= 300 || sec.Order() >= 400 {
		t.Errorf("Order = %d, want between tool guidance (300) and session state (400)", sec.Order())
	}

	out, err := sec.Render(prompt.Context{Context: context.Background()})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, want := range []string{"# Active Skills", "use_skill", "- alpha: does A", "- beta: does B"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered prompt missing %q:\n%s", want, out)
		}
	}
	// alpha must come before beta (sorted).
	if strings.Index(out, "alpha") > strings.Index(out, "beta") {
		t.Errorf("skills not in sorted order:\n%s", out)
	}
}

func TestPromptSectionEmpty(t *testing.T) {
	empty, err := Load(fstest.MapFS{})
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	out, err := PromptSection(empty).Render(prompt.Context{Context: context.Background()})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if out != "" {
		t.Errorf("empty library should render no section, got %q", out)
	}

	// A nil library must also render empty rather than panic.
	out, _ = PromptSection(nil).Render(prompt.Context{Context: context.Background()})
	if out != "" {
		t.Errorf("nil library should render empty, got %q", out)
	}
}
