// Package rules provides a two-level behavioral rule system: always-on
// instructions/constraints loaded from a global directory (the user's
// cross-project preferences) and a project directory (repo-specific
// constraints), merged with project taking precedence. Rules render as the
// highest-priority system-prompt section. See ADR 0021.
//
// Note: this is distinct from the transfer/delegation "rules" in agent/transfer.go
// (ADR 0008) — those govern agent hand-off direction, not prompt content.
package rules

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Rule is one behavioral rule. ID is the file stem (used for override matching);
// Source/Scope is "global" or "project"; Text is the rule body.
type Rule struct {
	ID     string
	Scope  string
	Text   string
	Source string
}

// Set is an ordered, precedence-merged collection of rules.
type Set struct {
	rules []Rule
}

// Rules returns the merged rules, global-sourced first then project-sourced,
// each group sorted by ID. The returned slice is a copy.
func (s *Set) Rules() []Rule { return slices.Clone(s.rules) }

// Load reads "*.md" files from globalDir and projectDir, one rule per file
// (ID = file stem). A project rule overrides a global rule with the same ID. A
// missing or empty directory contributes nothing (not an error).
func Load(globalDir, projectDir string) (*Set, error) {
	byID := map[string]Rule{}

	if err := loadDir(globalDir, "global", byID); err != nil {
		return nil, err
	}
	if err := loadDir(projectDir, "project", byID); err != nil {
		return nil, err
	}

	rules := make([]Rule, 0, len(byID))
	for _, r := range byID {
		rules = append(rules, r)
	}
	// Deterministic order: global before project, then by ID.
	slices.SortFunc(rules, func(a, b Rule) int {
		if a.Source != b.Source {
			if a.Source == "global" {
				return -1
			}
			return 1
		}
		return strings.Compare(a.ID, b.ID)
	})
	return &Set{rules: rules}, nil
}

// loadDir reads each *.md in dir as a rule under the given source, writing into
// byID (overwriting same-ID entries, so a later call overrides an earlier one).
func loadDir(dir, source string, byID map[string]Rule) error {
	if dir == "" {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return fmt.Errorf("rules: glob %s: %w", dir, err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("rules: read %s: %w", f, err)
		}
		text := strings.TrimSpace(string(b))
		if text == "" {
			continue
		}
		id := strings.TrimSuffix(filepath.Base(f), ".md")
		byID[id] = Rule{ID: id, Scope: source, Source: source, Text: text}
	}
	return nil
}
