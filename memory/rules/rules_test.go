package rules

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/prompt"
)

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestProjectOverridesGlobal(t *testing.T) {
	g := t.TempDir()
	p := t.TempDir()
	write(t, g, "tone.md", "全局：友好")
	write(t, g, "lang.md", "全局：英文")
	write(t, p, "tone.md", "项目：严谨") // same ID overrides global tone

	set, err := Load(g, p)
	if err != nil {
		t.Fatal(err)
	}
	rs := set.Rules()
	if len(rs) != 2 {
		t.Fatalf("got %d rules, want 2 (tone merged): %+v", len(rs), rs)
	}
	// Find tone rule; must be project-sourced.
	var tone Rule
	for _, r := range rs {
		if r.ID == "tone" {
			tone = r
		}
	}
	if tone.Source != "project" || tone.Text != "项目：严谨" {
		t.Errorf("tone not overridden: %+v", tone)
	}
}

func TestMissingDirIsNotError(t *testing.T) {
	set, err := Load(filepath.Join(t.TempDir(), "nope"), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Rules()) != 0 {
		t.Errorf("want empty set")
	}
}

func TestSectionOrderingAndOmit(t *testing.T) {
	empty, _ := Load("", "")
	if out, _ := empty.Section().Render(prompt.Context{}); out != "" {
		t.Errorf("empty rules should render empty, got %q", out)
	}

	g := t.TempDir()
	p := t.TempDir()
	write(t, g, "a.md", "global A")
	write(t, p, "b.md", "project B")
	set, _ := Load(g, p)

	out, _ := set.Section().Render(prompt.Context{})
	gi := strings.Index(out, "global A")
	pi := strings.Index(out, "project B")
	if gi < 0 || pi < 0 || gi > pi {
		t.Errorf("global should precede project:\n%s", out)
	}
	if Order >= 100 {
		t.Errorf("rules Order must be before identity(100)")
	}
}
