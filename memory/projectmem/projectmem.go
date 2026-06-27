// Package projectmem loads project memory: AGENTS.md files discovered by walking
// up the directory tree from a starting point to the repository root. Nearer
// (deeper) files have higher priority and are rendered after the higher-level
// ones, so a subdirectory can refine or override repo-wide conventions — the
// same hierarchical model as CLAUDE.md. Project memory is versioned with the
// repo and injected as a single high-priority prompt section. See ADR 0020.
package projectmem

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// fileName is the project-memory file discovered at each directory level.
const fileName = "AGENTS.md"

// maxImportDepth bounds @import recursion (cycle/runaway guard).
const maxImportDepth = 8

// Doc is one discovered AGENTS.md, with @import directives already expanded.
type Doc struct {
	Path    string // absolute path to the AGENTS.md
	Content string
}

// Load walks up from startDir collecting AGENTS.md files and returns them
// root-first (leaf last). Discovery stops at the filesystem root or after the
// first directory containing a .git entry (treated as the repo boundary,
// inclusive). Each document's "@import ./other.md" lines are expanded relative
// to the importing file.
func Load(startDir string) ([]Doc, error) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return nil, fmt.Errorf("projectmem: abs %q: %w", startDir, err)
	}

	// Collect candidate directories leaf-first, stopping at the repo boundary.
	var dirs []string
	for {
		dirs = append(dirs, dir)
		if hasGit(dir) {
			break // repo root reached; include it, then stop
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break // filesystem root
		}
		dir = parent
	}

	// Read root-first (reverse of the leaf-first walk) so leaf docs land last.
	var docs []Doc
	for i := len(dirs) - 1; i >= 0; i-- {
		path := filepath.Join(dirs[i], fileName)
		b, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("projectmem: read %s: %w", path, err)
		}
		content, err := expandImports(path, string(b), 0)
		if err != nil {
			return nil, err
		}
		docs = append(docs, Doc{Path: path, Content: strings.TrimRight(content, "\n")})
	}
	return docs, nil
}

// hasGit reports whether dir contains a .git entry (file or directory).
func hasGit(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, ".git"))
	return err == nil
}

// expandImports replaces lines of the form "@import <relpath>" with the
// referenced file's content, resolved relative to fromPath's directory.
func expandImports(fromPath, content string, depth int) (string, error) {
	if depth > maxImportDepth || !strings.Contains(content, "@import") {
		return content, nil
	}
	baseDir := filepath.Dir(fromPath)
	var out strings.Builder
	for _, line := range strings.Split(content, "\n") {
		rel, ok := strings.CutPrefix(strings.TrimSpace(line), "@import ")
		if !ok {
			out.WriteString(line)
			out.WriteString("\n")
			continue
		}
		target := filepath.Join(baseDir, strings.TrimSpace(rel))
		b, err := os.ReadFile(target)
		if err != nil {
			return "", fmt.Errorf("projectmem: @import %s: %w", target, err)
		}
		expanded, err := expandImports(target, string(b), depth+1)
		if err != nil {
			return "", err
		}
		out.WriteString(expanded)
		out.WriteString("\n")
	}
	return out.String(), nil
}
