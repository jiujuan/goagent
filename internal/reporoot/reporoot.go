// Package reporoot resolves which directories make up a working tree: it walks
// up from a starting directory and stops at the first directory containing a
// .git entry (inclusive) or at the filesystem root. It exists so repository-root
// detection has exactly one implementation shared by project memory (ADR 0020)
// and the workspace assembly layer (ADR 0022). It depends only on the standard
// library.
package reporoot

import (
	"fmt"
	"os"
	"path/filepath"
)

// Resolve returns the directory chain from startDir up to the repo boundary,
// leaf-first (startDir at index 0, the boundary last). The walk includes the
// first directory holding a .git entry — file or directory, since a linked
// worktree keeps .git as a file — and stops there; without a .git anywhere it
// stops at the filesystem root, which is then the last element.
//
// Callers that read files along the chain should os.Lstat rather than
// filepath.EvalSymlinks the result: following symlinks can move a caller out of
// the tree it meant to scope itself to.
//
// startDir need not exist: resolution is purely lexical (filepath.Abs), so a
// caller can resolve a root for a directory it is about to create.
func Resolve(startDir string) ([]string, error) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return nil, fmt.Errorf("reporoot: abs %q: %w", startDir, err)
	}

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
	return dirs, nil
}

// Root reports the single directory a working tree is anchored to: the repo
// boundary when the walk found one, otherwise startDir itself (absolute).
// Unlike Resolve it never returns the filesystem root for a non-repo tree,
// because "no repository here" must not silently widen the scope to "/".
func Root(startDir string) (string, error) {
	dirs, err := Resolve(startDir)
	if err != nil {
		return "", err
	}
	if hasGit(dirs[len(dirs)-1]) {
		return dirs[len(dirs)-1], nil
	}
	return dirs[0], nil
}

// hasGit reports whether dir contains a .git entry (file or directory).
func hasGit(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, ".git"))
	return err == nil
}
