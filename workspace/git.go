package workspace

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// gitStatusTimeout bounds the one subprocess this package runs. A wedged
// repository (locked index, slow network mount) must not wedge assembly: the
// probe degrades to StatusErr instead.
const gitStatusTimeout = 5 * time.Second

// GitInfo is a read-only snapshot of the repository a workspace sits in. Every
// field is derived without writing anything: refs are read from the files git
// maintains, and the only subprocess is `git status --porcelain`, which does not
// mutate the repository (see gitEnv for the flags that keep it that way).
//
// Bare repositories, submodules-as-workspace and unexpected on-disk shapes are
// reported as Valid: false rather than guessed at.
type GitInfo struct {
	// Valid is true when a readable repository was found at or above the root.
	Valid bool
	// RepoRoot is the directory holding the .git entry.
	RepoRoot string
	// Branch is the current branch, empty when detached or unknown.
	Branch string
	// HeadCommit is the full 40-character SHA HEAD resolves to, may be empty.
	HeadCommit string
	// Dirty is true when the working tree differs from HEAD or holds untracked
	// files. Only meaningful when StatusErr is nil.
	Dirty bool
	// StatusErr is why the dirty probe failed (git missing, timeout, locked
	// index). Non-fatal: the rest of the snapshot still stands.
	StatusErr error
}

// statusFunc runs the read-only dirty probe for a repository directory. It is a
// parameter of probeGit so tests can exercise the degraded path without hiding
// the git binary from the process.
type statusFunc func(repoDir string) (string, error)

// probeGit walks up from root looking for a git directory and reads what it
// finds. status is the dirty probe; New passes gitStatus.
func probeGit(root string, status statusFunc) GitInfo {
	repoRoot, gitDir, ok := findGitDir(root)
	if !ok {
		return GitInfo{}
	}
	commonDir := commonDir(gitDir)
	if isBare(commonDir) {
		// A bare repository has no working tree to describe.
		return GitInfo{}
	}
	branch, commit, ok := readHead(gitDir, commonDir)
	if !ok {
		return GitInfo{}
	}
	info := GitInfo{Valid: true, RepoRoot: repoRoot, Branch: branch, HeadCommit: commit}
	out, err := status(repoRoot)
	switch {
	case err != nil:
		info.StatusErr = err
	default:
		info.Dirty = strings.TrimSpace(out) != ""
	}
	return info
}

// findGitDir walks up from dir to the first entry named ".git". It may be a
// directory (ordinary repository) or a file holding "gitdir: <path>" (linked
// worktree). Walking stops at the filesystem root.
func findGitDir(dir string) (repoRoot, gitDir string, ok bool) {
	for {
		entry := filepath.Join(dir, ".git")
		info, err := os.Stat(entry)
		switch {
		case err == nil && info.IsDir():
			return dir, entry, true
		case err == nil:
			if target, ok := readGitFile(entry, dir); ok {
				return dir, target, true
			}
			return "", "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", "", false
		}
		dir = parent
	}
}

// readGitFile parses a linked-worktree .git file: "gitdir: <path>", resolved
// against the directory holding it.
func readGitFile(path, baseDir string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	rest, ok := strings.CutPrefix(strings.TrimSpace(string(b)), "gitdir:")
	if !ok {
		return "", false
	}
	target := strings.TrimSpace(rest)
	if target == "" {
		return "", false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(baseDir, target)
	}
	if _, err := os.Stat(filepath.Join(target, "HEAD")); err != nil {
		// A .git file pointing somewhere without HEAD is broken; there is
		// nothing here to describe, so report no repository.
		return "", false
	}
	return filepath.Clean(target), true
}

// commonDir returns where refs live for a git directory. A linked worktree
// keeps HEAD and its index to itself but shares refs through "commondir".
func commonDir(gitDir string) string {
	b, err := os.ReadFile(filepath.Join(gitDir, "commondir"))
	if err != nil {
		return gitDir
	}
	rest := strings.TrimSpace(string(b))
	if rest == "" {
		return gitDir
	}
	return filepath.Clean(filepath.Join(gitDir, rest))
}

// isBare reports whether the repository config declares core.bare = true. This
// is a targeted scan, not an INI parser: git writes the setting in one place
// and nothing here needs the rest of the config.
func isBare(commonDir string) bool {
	b, err := os.ReadFile(filepath.Join(commonDir, "config"))
	if err != nil {
		return false
	}
	inCore := false
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "["):
			inCore = strings.EqualFold(line, "[core]")
		case inCore:
			key, value, found := strings.Cut(line, "=")
			if found && strings.EqualFold(strings.TrimSpace(key), "bare") &&
				strings.EqualFold(strings.TrimSpace(value), "true") {
				return true
			}
		}
	}
	return false
}

// readHead resolves <gitDir>/HEAD against the refs in commonDir. A symbolic
// HEAD yields (branch, commit); a detached one yields ("", commit). ok is false
// when HEAD holds neither, which means a shape this probe will not guess at.
func readHead(gitDir, commonDir string) (branch, commit string, ok bool) {
	b, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		return "", "", false
	}
	content := strings.TrimSpace(string(b))
	if content == "" {
		return "", "", false
	}
	if ref, isRef := strings.CutPrefix(content, "ref:"); isRef {
		ref = strings.TrimSpace(ref)
		commit = readRef(commonDir, ref)
		branch = strings.TrimPrefix(ref, "refs/heads/")
		return branch, commit, true
	}
	if isHash(content) {
		return "", content, true
	}
	return "", "", false
}

// readRef resolves a ref name to a SHA: loose ref file first, then packed-refs.
func readRef(commonDir, ref string) string {
	if b, err := os.ReadFile(filepath.Join(commonDir, filepath.FromSlash(ref))); err == nil {
		if sha := strings.TrimSpace(string(b)); isHash(sha) {
			return sha
		}
	}
	b, err := os.ReadFile(filepath.Join(commonDir, "packed-refs"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "^") {
			continue
		}
		sha, name, found := strings.Cut(line, " ")
		if found && strings.TrimSpace(name) == ref && isHash(sha) {
			return sha
		}
	}
	return ""
}

// isHash reports whether s is a full object id (40 hex chars, or 64 for sha256).
func isHash(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F'))
	}) < 0
}

// gitStatus runs the read-only dirty probe in repoDir.
func gitStatus(repoDir string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gitStatusTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "status", "--porcelain")
	cmd.Env = gitEnv()
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git status in %s: %w: %s", repoDir, err, lastLine(stderr.String()))
	}
	return stdout.String(), nil
}

// gitEnv is the smallest environment that lets git run. It deliberately omits
// the user's config: this is a probe, and it must not be able to reach a
// credential helper or prompt for a password on the agent's behalf.
func gitEnv() []string {
	env := make([]string, 0, 4)
	for _, key := range []string{"PATH", "SYSTEMROOT"} {
		if v := os.Getenv(key); v != "" {
			env = append(env, key+"="+v)
		}
	}
	return append(env,
		"GIT_CONFIG_NOSYSTEM=1", // ignore /etc/gitconfig and its includes
		"GIT_TERMINAL_PROMPT=0", // never block on a credential prompt
		"GIT_OPTIONAL_LOCKS=0",  // status refreshes the index; do not let it write
	)
}

// lastLine returns the final non-empty line of s, for compact error messages.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// describe renders the git facts appended to the Root line of the workspace
// section, or "" when there is no repository. A status failure is reported as
// unknown rather than silently read as clean.
func (g GitInfo) describe(root string) string {
	if !g.Valid {
		return ""
	}
	facts := "git repository"
	if g.RepoRoot != root {
		facts = "in git repository " + g.RepoRoot
	}
	if g.Branch != "" {
		facts += ", branch " + g.Branch
	}
	if g.HeadCommit != "" {
		facts += ", HEAD " + g.HeadCommit[:min(len(g.HeadCommit), 12)]
	}
	switch {
	case g.StatusErr != nil:
		facts += ", working tree status unknown"
	case g.Dirty:
		facts += ", dirty"
	}
	return " (" + facts + ")"
}
