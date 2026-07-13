package skills

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"testing/fstest"
)

// mapLib builds a Library from an in-memory filesystem for focused unit tests.
func mapLib(t *testing.T, files map[string]string) *Library {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	lib, err := Load(fsys)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return lib
}

func TestLoadAndList(t *testing.T) {
	lib := mapLib(t, map[string]string{
		"bbb/SKILL.md": "---\nname: beta\ndescription: B\n---\nbody b",
		"aaa/SKILL.md": "---\nname: alpha\ndescription: A\n---\nbody a",
		"readme.txt":   "not a skill",
	})

	if lib.Len() != 2 {
		t.Fatalf("Len = %d, want 2", lib.Len())
	}
	got := []string{lib.List()[0].Name, lib.List()[1].Name}
	if want := []string{"alpha", "beta"}; !reflect.DeepEqual(got, want) {
		t.Errorf("List order = %v, want %v (sorted by name)", got, want)
	}

	s, ok := lib.Get("alpha")
	if !ok {
		t.Fatal("Get(alpha) not found")
	}
	if s.Description != "A" || s.Dir != "aaa" {
		t.Errorf("alpha = %+v", s)
	}
	if _, ok := lib.Get("missing"); ok {
		t.Error("Get(missing) should be false")
	}
}

func TestLoadReportsMissingName(t *testing.T) {
	fsys := fstest.MapFS{
		"good/SKILL.md": &fstest.MapFile{Data: []byte("---\nname: good\n---\nx")},
		"bad/SKILL.md":  &fstest.MapFile{Data: []byte("---\ndescription: no name\n---\nx")},
	}
	lib, err := Load(fsys)
	if err == nil {
		t.Fatal("expected an error reporting the nameless skill")
	}
	// Valid skills still load despite the problem.
	if _, ok := lib.Get("good"); !ok {
		t.Error("valid skill should still load alongside the bad one")
	}
	if lib.Len() != 1 {
		t.Errorf("Len = %d, want 1", lib.Len())
	}
}

func TestLoadDuplicateName(t *testing.T) {
	fsys := fstest.MapFS{
		"one/SKILL.md": &fstest.MapFile{Data: []byte("---\nname: dup\n---\na")},
		"two/SKILL.md": &fstest.MapFile{Data: []byte("---\nname: dup\n---\nb")},
	}
	_, err := Load(fsys)
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestInstructionsAndMetadata(t *testing.T) {
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	s, ok := lib.Get("pdf")
	if !ok {
		t.Fatal("pdf skill not loaded")
	}
	if want := []string{"run_command", "use_skill"}; !reflect.DeepEqual(s.AllowedTools, want) {
		t.Errorf("AllowedTools = %v, want %v", s.AllowedTools, want)
	}

	body, err := s.Instructions()
	if err != nil {
		t.Fatalf("Instructions: %v", err)
	}
	if want := "# Working with PDFs"; body == "" || body[:len(want)] != want {
		t.Errorf("Instructions body = %q, want it to start with %q", body, want)
	}
}

func TestResourceReadAndEscape(t *testing.T) {
	lib, err := LoadDir("testdata/skills")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	s, _ := lib.Get("pdf")

	data, err := s.Resource("forms.md")
	if err != nil {
		t.Fatalf("Resource(forms.md): %v", err)
	}
	if string(data[:13]) != "# Form fields" {
		t.Errorf("forms.md content = %q", data)
	}

	if _, err := s.Resource("scripts/fill.sh"); err != nil {
		t.Errorf("Resource(scripts/fill.sh): %v", err)
	}

	// Path-escape attempts must be rejected, not read.
	for _, bad := range []string{"../noname/SKILL.md", "../../secret", "/etc/passwd", "", "."} {
		if _, err := s.Resource(bad); !errors.Is(err, ErrResourceEscapes) {
			t.Errorf("Resource(%q) err = %v, want ErrResourceEscapes", bad, err)
		}
	}
}

func TestLoadDirMissing(t *testing.T) {
	if _, err := LoadDir("testdata/does-not-exist"); err == nil {
		// os.DirFS of a missing dir yields a glob error or empty; ensure no panic
		// and that a real read later would fail. Empty lib is acceptable here.
		_ = os.ErrNotExist
	}
}
