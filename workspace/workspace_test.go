package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
)

// newAt builds a workspace rooted at dir with global rules read from an empty
// directory, so a rules file in the test runner's home can never leak in.
func newAt(t *testing.T, cfg Config) *Workspace {
	t.Helper()
	if cfg.GlobalRulesDir == "" {
		cfg.GlobalRulesDir = filepath.Join(t.TempDir(), "no-global-rules")
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return w
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func render(t *testing.T, s prompt.Section) string {
	t.Helper()
	out, err := s.Render(prompt.Context{})
	if err != nil {
		t.Fatalf("render %s: %v", s.Name(), err)
	}
	return out
}

// A workspace rooted at a plain directory reports that directory and hands out
// a handle confined to it.
func TestNewRootsAtDir(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "inside.txt"), "inside")
	mustWriteFile(t, filepath.Join(filepath.Dir(dir), "outside.txt"), "outside")

	w := newAt(t, Config{Dir: dir})

	if abs, _ := filepath.Abs(dir); w.Root() != abs {
		t.Errorf("Root() = %q, want %q", w.Root(), abs)
	}
	if b, err := w.FS().ReadFile("inside.txt"); err != nil || string(b) != "inside" {
		t.Errorf("FS().ReadFile(inside.txt) = %q, %v", b, err)
	}
	if _, err := w.FS().ReadFile("../outside.txt"); err == nil {
		t.Error("FS handle escaped the root")
	}
}

// ResolveRoot walks up to the repo boundary, so a subdirectory start yields the
// repo root; without it the start directory itself is the root.
func TestResolveRootUsesRepoBoundary(t *testing.T) {
	repo := t.TempDir()
	mustWriteFile(t, filepath.Join(repo, ".git", "HEAD"), "ref: refs/heads/main\n")
	pkg := filepath.Join(repo, "internal", "pkg")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}

	w := newAt(t, Config{Dir: pkg, ResolveRoot: true})
	if w.Root() != repo {
		t.Errorf("Root() = %q, want repo root %q", w.Root(), repo)
	}

	plain := newAt(t, Config{Dir: pkg})
	if plain.Root() != pkg {
		t.Errorf("Root() without ResolveRoot = %q, want %q", plain.Root(), pkg)
	}
}

// A non-git start directory must not widen the root to the filesystem root.
func TestResolveRootWithoutGitKeepsDir(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir, ResolveRoot: true})
	if w.Root() != dir {
		t.Errorf("Root() = %q, want %q (no repo here, so the dir itself)", w.Root(), dir)
	}
}

func TestNewRejectsMissingRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-created")
	if _, err := New(Config{Dir: missing, GlobalRulesDir: filepath.Join(t.TempDir(), "none")}); err == nil {
		t.Fatal("New on a missing root should fail: creating it is the caller's decision")
	} else if !strings.Contains(err.Error(), missing) {
		t.Errorf("error should name the directory, got %v", err)
	}
}

func TestToolsAreBoundToRoot(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "a.txt"), "a")
	w := newAt(t, Config{Dir: dir})

	var names []string
	for _, tl := range w.Tools() {
		names = append(names, tl.Name())
	}
	want := "read_file write_file list_dir glob"
	if got := strings.Join(names, " "); got != want {
		t.Errorf("Tools() = %q, want %q", got, want)
	}
}

func TestSectionFacts(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	s := w.Section()
	if s.Order() != Order {
		t.Errorf("Order() = %d, want %d", s.Order(), Order)
	}
	out := render(t, s)
	for _, want := range []string{"# Workspace", "Root: " + dir, "confined to this root", "not jailed"} {
		if !strings.Contains(out, want) {
			t.Errorf("section missing %q:\n%s", want, out)
		}
	}
}

// Sections carries its own ordering so a Builder can sort it: rules (50) before
// project memory (150) before workspace facts (210).
func TestSectionsOrderAndContents(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, ".goagent", "rules", "style.md"), "use tabs")
	mustWriteFile(t, filepath.Join(dir, "AGENTS.md"), "repo conventions")

	w := newAt(t, Config{Dir: dir, ProjectMemory: true})

	secs := w.Sections()
	var names []string
	for _, s := range secs {
		names = append(names, s.Name())
	}
	want := "rules project_memory workspace"
	if got := strings.Join(names, " "); got != want {
		t.Errorf("Sections() = %q, want %q", got, want)
	}

	joined, err := prompt.New().Add(secs...).Build(prompt.Context{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"use tabs", "repo conventions", "Root: " + dir} {
		if !strings.Contains(joined, want) {
			t.Errorf("built prompt missing %q:\n%s", want, joined)
		}
	}
	if strings.Index(joined, "use tabs") > strings.Index(joined, "repo conventions") {
		t.Errorf("rules must render before project memory:\n%s", joined)
	}
}

// Project memory is opt-in, and rules fall back to the conventional
// <root>/.goagent/rules directory.
func TestProjectMemoryIsOptIn(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "AGENTS.md"), "repo conventions")

	w := newAt(t, Config{Dir: dir})
	for _, s := range w.Sections() {
		if s.Name() == "project_memory" {
			t.Error("project memory section present without Config.ProjectMemory")
		}
	}
	if out := render(t, w.Section()); strings.Contains(out, "repo conventions") {
		t.Errorf("project memory leaked into the prompt:\n%s", out)
	}
}

// A project rule overrides the global rule with the same stem.
func TestRulesMergeProjectWins(t *testing.T) {
	dir := t.TempDir()
	global := filepath.Join(t.TempDir(), "global-rules")
	mustWriteFile(t, filepath.Join(global, "style.md"), "global spacing")
	mustWriteFile(t, filepath.Join(dir, ".goagent", "rules", "style.md"), "project spacing")

	w := newAt(t, Config{Dir: dir, GlobalRulesDir: global})

	secs := w.Sections()
	if secs[0].Name() != "rules" {
		t.Fatalf("first section = %s, want rules", secs[0].Name())
	}
	out := render(t, secs[0])
	if !strings.Contains(out, "project spacing") {
		t.Errorf("project rule should win:\n%s", out)
	}
	if strings.Contains(out, "global spacing") {
		t.Errorf("overridden global rule still rendered:\n%s", out)
	}
}

// The sandbox and the environment section must agree on where commands run.
func TestSandboxPolicyFillsWorkDir(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	p, err := w.SandboxPolicy(sandbox.Policy{Timeout: 1})
	if err != nil {
		t.Fatal(err)
	}
	if p.WorkDir != dir {
		t.Errorf("WorkDir = %q, want %q", p.WorkDir, dir)
	}
	if p.Timeout != 1 {
		t.Errorf("other policy fields must survive: Timeout = %v", p.Timeout)
	}

	env := render(t, prompt.Environment(prompt.WithWorkingDir(w.Root())))
	if !strings.Contains(env, "Working directory: "+p.WorkDir) {
		t.Errorf("environment section disagrees with the sandbox policy:\n%s", env)
	}
}

func TestSandboxPolicyRejectsConflictingWorkDir(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	if _, err := w.SandboxPolicy(sandbox.Policy{WorkDir: filepath.Dir(dir)}); err == nil {
		t.Fatal("a conflicting WorkDir must be an error, not a silent override")
	} else if !strings.Contains(err.Error(), dir) {
		t.Errorf("error should name the workspace root, got %v", err)
	}

	// The same directory spelled as the root is not a conflict.
	if _, err := w.SandboxPolicy(sandbox.Policy{WorkDir: dir}); err != nil {
		t.Errorf("identical WorkDir should be accepted: %v", err)
	}
}

func TestSandboxBuilds(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	// The policy is rejected by the backend if the root is not a real directory,
	// so a workspace that opened successfully always satisfies it.
	if _, err := w.Sandbox(sandbox.Policy{}); err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
}

func TestCloseReleasesHandle(t *testing.T) {
	dir := t.TempDir()
	w, err := New(Config{Dir: dir, GlobalRulesDir: filepath.Join(t.TempDir(), "none")})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.FS().ReadFile("anything"); err == nil {
		t.Error("reads must fail after Close")
	}
}
