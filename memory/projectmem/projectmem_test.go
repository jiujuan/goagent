package projectmem

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/prompt"
)

// writeFile writes content to dir/name, creating dir.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRootToLeafOrder(t *testing.T) {
	root := t.TempDir()
	// Mark repo root so discovery stops here.
	writeFile(t, root, ".git", "")
	writeFile(t, root, "AGENTS.md", "root rules")
	sub := filepath.Join(root, "service", "api")
	writeFile(t, sub, "AGENTS.md", "api rules")

	docs, err := Load(sub)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d docs, want 2", len(docs))
	}
	// Root first, leaf last.
	if docs[0].Content != "root rules" || docs[1].Content != "api rules" {
		t.Errorf("order wrong: %q then %q", docs[0].Content, docs[1].Content)
	}
}

func TestLoadStopsAtGitBoundary(t *testing.T) {
	outer := t.TempDir()
	writeFile(t, outer, "AGENTS.md", "OUTSIDE repo, must not load")
	repo := filepath.Join(outer, "repo")
	writeFile(t, repo, ".git", "")
	writeFile(t, repo, "AGENTS.md", "repo root")

	docs, err := Load(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].Content != "repo root" {
		t.Errorf("boundary not respected: %+v", docs)
	}
}

func TestExpandImports(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".git", "")
	writeFile(t, root, "shared.md", "shared content")
	writeFile(t, root, "AGENTS.md", "top\n@import ./shared.md\nbottom")

	docs, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("got %d docs", len(docs))
	}
	if !strings.Contains(docs[0].Content, "shared content") || !strings.Contains(docs[0].Content, "top") {
		t.Errorf("import not expanded:\n%s", docs[0].Content)
	}
}

func TestSectionRendersAndOmits(t *testing.T) {
	if out, _ := Section(nil).Render(prompt.Context{}); out != "" {
		t.Errorf("no docs should render empty, got %q", out)
	}
	out, _ := Section([]Doc{{Path: "/x/AGENTS.md", Content: "hi"}}).Render(prompt.Context{})
	if !strings.Contains(out, "# 项目记忆") || !strings.Contains(out, "hi") {
		t.Errorf("render:\n%s", out)
	}
}
