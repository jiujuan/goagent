// Package skill implements a filesystem-based "skill pack" system with
// three-level progressive disclosure, so an agent can pull in domain knowledge,
// workflows, and scripts on demand instead of carrying them all in the prompt.
//
// A skill is a directory containing a SKILL.md file: YAML frontmatter
// (name / description / allowed-tools) followed by Markdown instructions, plus
// optional resource files and scripts. The three levels are:
//
//	Level 1 (always loaded)  — name + description, surfaced in the system prompt
//	                           by PromptSection so the model knows the skill exists.
//	Level 2 (on demand)      — the SKILL.md body, returned by the use_skill tool
//	                           only when the model asks for it.
//	Level 3 (on demand)      — bundled resource files (read via the same tool)
//	                           and scripts (executed via tool/exec + sandbox).
//
// Loading is done over io/fs, so a Library can be built from a real directory
// (LoadDir), an embed.FS compiled into the binary (Load), or a fstest.MapFS in
// tests. The package depends only on prompt, tool, and core — never the other
// way round — mirroring the tool/web and tool/exec batteries-included pattern.
package skill

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
)

// manifest is the conventional filename every skill directory must contain.
const manifest = "SKILL.md"

// Skill is one skill pack. The metadata fields (Name, Description,
// AllowedTools) are parsed at load time for Level 1; the body and resources are
// read lazily so large instructions never sit in memory or the prompt until the
// model actually requests them.
type Skill struct {
	// Name is the unique identifier the model uses to invoke the skill.
	Name string
	// Description is the one-line summary shown in the prompt (Level 1).
	Description string
	// AllowedTools is the advisory tool allow-list from frontmatter. It is
	// surfaced to the model in the loaded instructions but not hard-enforced.
	AllowedTools []string
	// Dir is the skill's directory within fsys (e.g. "pdf").
	Dir string

	fsys fs.FS
}

// Instructions reads the SKILL.md body (Level 2): the Markdown that follows the
// frontmatter, with the fences and metadata stripped.
func (s *Skill) Instructions() (string, error) {
	raw, err := fs.ReadFile(s.fsys, path.Join(s.Dir, manifest))
	if err != nil {
		return "", err
	}
	_, body := splitFrontmatter(raw)
	return body, nil
}

// Resource reads a bundled file from inside the skill's directory (Level 3),
// e.g. "forms.md" or "scripts/run.sh". The name is confined to the skill
// directory: any path that escapes it (via "..", an absolute path, or an empty
// path) is rejected with ErrResourceEscapes.
func (s *Skill) Resource(name string) ([]byte, error) {
	rel := strings.TrimPrefix(strings.ReplaceAll(name, "\\", "/"), "./")
	// fs.ValidPath rejects empty, rooted, and any path containing a ".."
	// element; "." is valid for fs but is not a file, so exclude it too.
	if rel == "." || !fs.ValidPath(rel) {
		return nil, ErrResourceEscapes
	}
	return fs.ReadFile(s.fsys, path.Join(s.Dir, rel))
}

// Errors returned by the skill package.
var (
	// ErrResourceEscapes means a requested resource path resolves outside the
	// skill's own directory.
	ErrResourceEscapes = errors.New("skill: resource path escapes skill directory")
	// ErrNotFound means no skill with the requested name is registered.
	ErrNotFound = errors.New("skill: not found")
)

// Library is an immutable, name-indexed set of loaded skills.
type Library struct {
	byName map[string]*Skill
	names  []string // sorted, for stable List ordering
}

// List returns the skills in stable (name-sorted) order.
func (l *Library) List() []*Skill {
	out := make([]*Skill, len(l.names))
	for i, n := range l.names {
		out[i] = l.byName[n]
	}
	return out
}

// Get returns the skill registered under name, if any.
func (l *Library) Get(name string) (*Skill, bool) {
	s, ok := l.byName[name]
	return s, ok
}

// Len reports how many skills the library holds.
func (l *Library) Len() int { return len(l.names) }

// Load builds a Library by scanning fsys for "*/SKILL.md" and parsing each
// file's frontmatter. Directories whose SKILL.md is missing a name are skipped
// and reported together as an error (the valid skills still load). Duplicate
// names are also reported.
func Load(fsys fs.FS) (*Library, error) {
	matches, err := fs.Glob(fsys, "*/"+manifest)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)

	lib := &Library{byName: map[string]*Skill{}}
	var problems []string

	for _, m := range matches {
		dir := path.Dir(m)
		raw, err := fs.ReadFile(fsys, m)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", dir, err))
			continue
		}
		meta, _ := splitFrontmatter(raw)
		name := metaString(meta, "name")
		if name == "" {
			problems = append(problems, fmt.Sprintf("%s: missing 'name' in frontmatter", dir))
			continue
		}
		if _, dup := lib.byName[name]; dup {
			problems = append(problems, fmt.Sprintf("%s: duplicate skill name %q", dir, name))
			continue
		}
		lib.byName[name] = &Skill{
			Name:         name,
			Description:  metaString(meta, "description"),
			AllowedTools: strList(meta["allowed-tools"]),
			Dir:          dir,
			fsys:         fsys,
		}
		lib.names = append(lib.names, name)
	}
	sort.Strings(lib.names)

	if len(problems) > 0 {
		return lib, fmt.Errorf("skill: load issues:\n  %s", strings.Join(problems, "\n  "))
	}
	return lib, nil
}

// LoadDir is a convenience wrapper that loads skills from a directory on the
// real filesystem.
func LoadDir(root string) (*Library, error) {
	return Load(os.DirFS(root))
}
