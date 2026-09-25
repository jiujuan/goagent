package skills

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
)

// LoadDirs merges the skills found in several directories into one Library.
// Directories are scanned in the order given and a later one replaces an
// earlier skill of the same name, so passing the global directory before the
// workspace directory lets a repo shadow a user-level skill — the same
// precedence memory/rules uses.
//
// A directory that does not exist contributes nothing rather than failing: the
// conventional paths are optional, and "this repo ships no skills" is the
// normal case. Problems inside a directory that does exist (a SKILL.md without
// a name) are reported the way Load reports them: the usable skills still come
// back, alongside an error describing what was skipped.
func LoadDirs(dirs ...string) (*Library, error) {
	merged := map[string]*Skill{}
	var problems []string

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if _, err := os.Stat(dir); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("skill: %s: %w", dir, err)
		}
		lib, err := LoadDir(dir)
		if lib == nil {
			if err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			problems = append(problems, err.Error())
		}
		for _, s := range lib.List() {
			merged[s.Name] = s
		}
	}

	out := &Library{byName: merged}
	for name := range merged {
		out.names = append(out.names, name)
	}
	sort.Strings(out.names)

	if len(problems) > 0 {
		return out, fmt.Errorf("skill: load issues:\n  %s", strings.Join(problems, "\n  "))
	}
	return out, nil
}
