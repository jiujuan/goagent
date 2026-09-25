package workspace

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/skills"
	"github.com/jiujuan/goagent/tool"
)

// skillTreeAt writes one SKILL.md per entry under root, each in its own
// subdirectory as the loader expects.
func skillTreeAt(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for dir, body := range files {
		mustWriteFile(t, filepath.Join(root, filepath.FromSlash(dir), "SKILL.md"), body)
	}
}

func sectionNames(secs []prompt.Section) string {
	var names []string
	for _, s := range secs {
		names = append(names, s.Name())
	}
	return strings.Join(names, ",")
}

func toolNames(tools []tool.Tool) string {
	var names []string
	for _, tl := range tools {
		names = append(names, tl.Name())
	}
	return strings.Join(names, ",")
}

// The workspace directory shadows a same-named user-level skill, and the
// surviving skills are what the Level-1 section advertises.
func TestNewMergesWorkspaceSkillsOverGlobal(t *testing.T) {
	dir := t.TempDir()
	global := filepath.Join(dir, "user-skills")
	skillTreeAt(t, global, map[string]string{
		"pdf": "---\nname: pdf\ndescription: the global one\n---\nglobal body",
		"csv": "---\nname: csv\ndescription: global only\n---\ncsv body",
	})
	skillTreeAt(t, filepath.Join(dir, userDirName, "skills"), map[string]string{
		"pdf": "---\nname: pdf\ndescription: the workspace one\n---\nworkspace body",
	})

	w := newAt(t, Config{Dir: dir, GlobalSkillsDir: global})

	lib := w.Skills()
	if lib == nil {
		t.Fatal("Skills() = nil, want the merged library")
	}
	if lib.Len() != 2 {
		t.Fatalf("Skills().Len() = %d, want 2 (shadowing replaces, it does not add)", lib.Len())
	}
	s, ok := lib.Get("pdf")
	if !ok {
		t.Fatal("pdf missing from the merge")
	}
	if s.Description != "the workspace one" {
		t.Errorf("pdf description = %q, want the workspace skill to win", s.Description)
	}

	if got := sectionNames(w.Sections()); got != "workspace,skills" {
		t.Errorf("Sections() = %s, want the facts block then the skill list", got)
	}
	secs := w.Sections()
	body := render(t, secs[len(secs)-1])
	if !strings.Contains(body, "pdf: the workspace one") || !strings.Contains(body, "csv: global only") {
		t.Errorf("skills section =\n%s", body)
	}
}

func TestSkillToolsReturnTheSkillPair(t *testing.T) {
	dir := t.TempDir()
	skillTreeAt(t, filepath.Join(dir, userDirName, "skills"), map[string]string{
		"pdf": "---\nname: pdf\ndescription: read pdfs\n---\nbody",
	})
	w := newAt(t, Config{Dir: dir})

	sb, err := w.Sandbox(sandbox.Policy{AllowedCommands: []string{"sh"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolNames(w.SkillTools(sb)); got != "use_skill,run_skill_script" {
		t.Errorf("SkillTools() = %s", got)
	}
	if got := len(w.Tools()); got != 4 {
		t.Errorf("Tools() has %d tools, want the four file tools unaffected by skills", got)
	}
}

// Acceptance (ADR 0022): a root with no skills directory is the normal case, not
// an error, and contributes nothing to any of the four skill surfaces.
func TestNewWithoutSkillsContributesNothing(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir, SkillGate: true})

	if w.Skills() != nil {
		t.Errorf("Skills() = %v, want nil", w.Skills())
	}
	if got := w.SkillTools(nil); got != nil {
		t.Errorf("SkillTools() = %s, want nil", toolNames(got))
	}
	if w.Gate() != nil {
		t.Error("Gate() must stay nil with no skills, even when SkillGate was requested")
	}
	if got := sectionNames(w.Sections()); got != "workspace" {
		t.Errorf("Sections() = %s, want only the facts block", got)
	}
}

// Acceptance (ADR 0022): the gate is opt-in through Config.SkillGate.
func TestGateEnforcesAllowedToolsOnlyWhenRequested(t *testing.T) {
	dir := t.TempDir()
	skillTreeAt(t, filepath.Join(dir, userDirName, "skills"), map[string]string{
		"pdf": "---\nname: pdf\ndescription: pdfs\nallowed-tools: [read_file]\n---\nbody",
	})

	if mw := newAt(t, Config{Dir: dir}).Gate(); mw != nil {
		t.Error("Gate() must be nil when SkillGate was not requested")
	}

	mw := newAt(t, Config{Dir: dir, SkillGate: true}).Gate()
	if mw == nil {
		t.Fatal("Gate() = nil, want the allowed-tools middleware")
	}
	lc := &agent.LoopContext{RunContext: &agent.RunContext{
		State: &core.State{KV: map[string]any{skills.ActiveKey: `["pdf"]`}},
	}}

	if d, err := mw.BeforeTool(lc, &core.ToolCall{Name: "read_file"}); err != nil || d.Kind != core.Continue {
		t.Errorf("listed tool: kind=%v err=%v, want continue", d.Kind, err)
	}
	d, err := mw.BeforeTool(lc, &core.ToolCall{Name: "write_file"})
	if err != nil {
		t.Fatalf("BeforeTool: %v", err)
	}
	if d.Kind != core.Interrupt {
		t.Errorf("unlisted tool: kind = %v, want interrupt", d.Kind)
	}
	if !strings.Contains(d.Reason, "write_file") {
		t.Errorf("interrupt reason = %q, want it to name the tool a human is approving", d.Reason)
	}
}

// A skills directory that exists but holds an unreadable SKILL.md fails
// assembly, like a broken rules file does.
func TestNewFailsOnBrokenSkillFile(t *testing.T) {
	dir := t.TempDir()
	skillTreeAt(t, filepath.Join(dir, userDirName, "skills"), map[string]string{
		"nope": "---\ndescription: no name in the frontmatter\n---\nbody",
	})
	w, err := New(Config{Dir: dir, GlobalSkillsDir: filepath.Join(t.TempDir(), "no-global-skills")})
	if err == nil {
		w.Close()
		t.Fatal("New accepted a skills directory whose SKILL.md has no name")
	}
	if !strings.Contains(err.Error(), "missing 'name'") {
		t.Errorf("err = %v, want it to report the offending skill", err)
	}
}
