package skills

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// skillTree writes one SKILL.md per map entry into a fresh directory and returns
// that directory.
func skillTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for dir, body := range files {
		path := filepath.Join(root, filepath.FromSlash(dir), "SKILL.md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestLoadDirsMergesWithLaterDirsWinning(t *testing.T) {
	global := skillTree(t, map[string]string{
		"pdf": "---\nname: pdf\ndescription: the global one\nallowed-tools: [read_file]\n---\nglobal body",
		"csv": "---\nname: csv\ndescription: global only\n---\ncsv body",
	})
	project := skillTree(t, map[string]string{
		"pdf": "---\nname: pdf\ndescription: the workspace one\nallowed-tools: [read_file, write_file]\n---\nworkspace body",
	})

	lib, err := LoadDirs(global, project)
	if err != nil {
		t.Fatalf("LoadDirs: %v", err)
	}
	if got := lib.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (the workspace skill replaces, it does not add)", got)
	}

	s, ok := lib.Get("pdf")
	if !ok {
		t.Fatal("pdf missing")
	}
	if s.Description != "the workspace one" {
		t.Errorf("Description = %q, want the workspace skill to win", s.Description)
	}
	if want := []string{"read_file", "write_file"}; !reflect.DeepEqual(s.AllowedTools, want) {
		t.Errorf("AllowedTools = %v, want %v", s.AllowedTools, want)
	}
	body, err := s.Instructions()
	if err != nil || body != "workspace body" {
		t.Errorf("Instructions = %q (%v), want the winner's body", body, err)
	}
	if _, ok := lib.Get("csv"); !ok {
		t.Error("a skill only the global tree has must survive the merge")
	}

	// Reversing the argument order reverses the winner: precedence is purely
	// positional, which is how workspace passes global-then-project.
	reversed, err := LoadDirs(project, global)
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := reversed.Get("pdf"); s.Description != "the global one" {
		t.Errorf("reversed merge = %q, want the last directory to win", s.Description)
	}
}

// The conventional paths are optional: a missing directory is not an error, and
// neither is an empty argument.
func TestLoadDirsToleratesMissingDirectories(t *testing.T) {
	existing := skillTree(t, map[string]string{
		"one": "---\nname: one\n---\nbody",
	})

	lib, err := LoadDirs("", filepath.Join(existing, "no-such-dir"), existing)
	if err != nil {
		t.Fatalf("LoadDirs: %v", err)
	}
	if lib.Len() != 1 {
		t.Errorf("Len = %d, want 1", lib.Len())
	}

	empty, err := LoadDirs()
	if err != nil {
		t.Fatalf("LoadDirs() with no directories: %v", err)
	}
	if empty.Len() != 0 {
		t.Errorf("Len = %d, want 0", empty.Len())
	}
}

// A directory that exists but holds a broken skill behaves like Load does: the
// good skills still load and the error explains what was skipped.
func TestLoadDirsReportsBrokenSkillsButKeepsTheRest(t *testing.T) {
	good := skillTree(t, map[string]string{
		"one": "---\nname: one\n---\nbody",
	})
	bad := skillTree(t, map[string]string{
		"noname": "---\ndescription: no name at all\n---\nbody",
		"two":    "---\nname: two\n---\nbody",
	})

	lib, err := LoadDirs(good, bad)
	if err == nil {
		t.Fatal("LoadDirs should report the skipped skill")
	}
	if lib.Len() != 2 {
		t.Errorf("Len = %d, want the 2 usable skills to load anyway", lib.Len())
	}
	for _, name := range []string{"one", "two"} {
		if _, ok := lib.Get(name); !ok {
			t.Errorf("%s missing from the merged library", name)
		}
	}
}

// List ordering is by name across the merge, so a prompt section built from the
// library stays stable regardless of how many directories fed it.
func TestLoadDirsListsSorted(t *testing.T) {
	z := skillTree(t, map[string]string{"d": "---\nname: zulu\n---\nbody"})
	a := skillTree(t, map[string]string{"d": "---\nname: alpha\n---\nbody"})

	lib, err := LoadDirs(z, a)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range lib.List() {
		names = append(names, s.Name)
	}
	if want := []string{"alpha", "zulu"}; !reflect.DeepEqual(names, want) {
		t.Errorf("List order = %v, want %v", names, want)
	}
}
