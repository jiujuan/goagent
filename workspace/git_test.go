package workspace

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef01234567"

// fakeStatus stands in for the dirty probe. Tests that only exercise the
// file-reading half of the snapshot must not depend on git being installed, and
// the degraded path (git missing entirely) has to be reachable on purpose.
func fakeStatus(out string, err error) statusFunc {
	return func(string) (string, error) { return out, err }
}

// mkRepo lays out dir/.git by hand: HEAD, loose refs, and a config that looks
// like a non-bare repository unless the test overrides it.
func mkRepo(t *testing.T, dir, head string, refs map[string]string, config string) {
	t.Helper()
	git := filepath.Join(dir, ".git")
	mustWriteFile(t, filepath.Join(git, "HEAD"), head)
	for ref, sha := range refs {
		mustWriteFile(t, filepath.Join(git, filepath.FromSlash(ref)), sha+"\n")
	}
	if config == "" {
		config = "[core]\n\trepositoryformatversion = 0\n\tfilemode = false\n\tbare = false\n"
	}
	mustWriteFile(t, filepath.Join(git, "config"), config)
}

// isolatedTemp returns a temp directory with no .git in any ancestor, or skips:
// on a machine whose home directory is a dotfiles repository, "this is not a
// repository" is not an assertion that can hold.
func isolatedTemp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for d := filepath.Dir(dir); ; {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			t.Skipf("%s is inside repository %s; cannot assert a non-repo case here", dir, d)
		}
		parent := filepath.Dir(d)
		if parent == d {
			return dir
		}
		d = parent
	}
}

func TestProbeGitSymbolicHead(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", map[string]string{"refs/heads/main": testSHA}, "")

	info := probeGit(dir, fakeStatus("", nil))
	if !info.Valid {
		t.Fatalf("snapshot invalid for a fabricated repo: %+v", info)
	}
	if info.RepoRoot != dir {
		t.Errorf("RepoRoot = %q, want %q", info.RepoRoot, dir)
	}
	if info.Branch != "main" {
		t.Errorf("Branch = %q, want main", info.Branch)
	}
	if info.HeadCommit != testSHA {
		t.Errorf("HeadCommit = %q, want %q", info.HeadCommit, testSHA)
	}
	if info.Dirty {
		t.Error("empty porcelain output must read as clean")
	}
}

func TestProbeGitDirtyFromPorcelain(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", map[string]string{"refs/heads/main": testSHA}, "")

	info := probeGit(dir, fakeStatus("?? scratch.txt\n", nil))
	if !info.Dirty {
		t.Errorf("non-empty porcelain must read as dirty: %+v", info)
	}
}

// A detached HEAD has no branch but a commit; the snapshot keeps both facts
// distinguishable.
func TestProbeGitDetachedHead(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, testSHA+"\n", nil, "")

	info := probeGit(dir, fakeStatus("", nil))
	if !info.Valid || info.Branch != "" || info.HeadCommit != testSHA {
		t.Errorf("detached snapshot = %+v, want valid with no branch", info)
	}
}

func TestProbeGitPackedRefsFallback(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", nil, "")
	mustWriteFile(t, filepath.Join(dir, ".git", "packed-refs"),
		"# pack-refs with: peeled fully-peeled sorted \n"+testSHA+" refs/heads/main\n")

	info := probeGit(dir, fakeStatus("", nil))
	if info.HeadCommit != testSHA {
		t.Errorf("HeadCommit = %q, want the packed-refs value %q", info.HeadCommit, testSHA)
	}
}

// A worktree's .git is a file, its HEAD lives in the worktree's own git
// directory, and refs are shared through commondir.
func TestProbeGitLinkedWorktree(t *testing.T) {
	repo := t.TempDir()
	commonGit := filepath.Join(repo, ".git")
	mustWriteFile(t, filepath.Join(commonGit, "config"), "[core]\n\tbare = false\n")
	mustWriteFile(t, filepath.Join(commonGit, "refs", "heads", "feature"), testSHA+"\n")

	wtGit := filepath.Join(commonGit, "worktrees", "feature")
	mustWriteFile(t, filepath.Join(wtGit, "HEAD"), "ref: refs/heads/feature\n")
	mustWriteFile(t, filepath.Join(wtGit, "commondir"), "../../\n")

	wt := t.TempDir()
	mustWriteFile(t, filepath.Join(wt, ".git"), "gitdir: "+wtGit+"\n")

	info := probeGit(wt, fakeStatus("", nil))
	if !info.Valid {
		t.Fatalf("linked worktree not recognised: %+v", info)
	}
	if info.RepoRoot != wt {
		t.Errorf("RepoRoot = %q, want the worktree directory %q", info.RepoRoot, wt)
	}
	if info.Branch != "feature" || info.HeadCommit != testSHA {
		t.Errorf("worktree snapshot = %+v, want feature/%s", info, testSHA)
	}
}

func TestProbeGitWalksUpToRepository(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", map[string]string{"refs/heads/main": testSHA}, "")
	deep := filepath.Join(dir, "internal", "pkg")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}

	info := probeGit(deep, fakeStatus("", nil))
	if !info.Valid || info.RepoRoot != dir {
		t.Fatalf("snapshot = %+v, want the enclosing repository %q", info, dir)
	}
}

func TestProbeGitNotARepository(t *testing.T) {
	dir := isolatedTemp(t)
	probeCalls := 0
	info := probeGit(dir, func(string) (string, error) {
		probeCalls++
		return "", nil
	})
	if info.Valid {
		t.Errorf("snapshot = %+v, want invalid outside any repository", info)
	}
	if probeCalls != 0 {
		t.Errorf("the dirty probe ran %d times for a non-repository", probeCalls)
	}
	if got := info.describe(dir); got != "" {
		t.Errorf("describe() = %q, want empty", got)
	}
}

func TestProbeGitRejectsBare(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", nil, "[core]\n\tbare = true\n")

	if info := probeGit(dir, fakeStatus("", nil)); info.Valid {
		t.Errorf("bare repository accepted: %+v", info)
	}
}

// Before the first commit HEAD names a ref that does not exist yet. That is a
// valid repository with a branch and no commit, not a broken one.
func TestProbeGitRefWithoutCommit(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", nil, "")

	info := probeGit(dir, fakeStatus("", nil))
	if !info.Valid || info.Branch != "main" || info.HeadCommit != "" {
		t.Errorf("snapshot = %+v, want valid with branch main and no commit", info)
	}
}

func TestProbeGitMalformedHead(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "not-a-ref-and-not-a-hash\n", nil, "")

	if info := probeGit(dir, fakeStatus("", nil)); info.Valid {
		t.Errorf("malformed HEAD accepted: %+v", info)
	}
}

// git missing, an index lock, a timeout: the snapshot keeps every fact it read
// from files and reports the dirty state as unknown.
func TestProbeGitStatusFailureDegrades(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", map[string]string{"refs/heads/main": testSHA}, "")

	info := probeGit(dir, fakeStatus("", errors.New("executable file not found in $PATH")))
	if !info.Valid || info.Branch != "main" {
		t.Fatalf("file-read facts must survive a failed status probe: %+v", info)
	}
	if info.StatusErr == nil || info.Dirty {
		t.Errorf("degraded snapshot = %+v, want StatusErr set and Dirty false", info)
	}
	if got := info.describe(dir); !strings.Contains(got, "working tree status unknown") {
		t.Errorf("describe() must not imply clean: %q", got)
	}
}

func TestGitDescribe(t *testing.T) {
	cases := []struct {
		name string
		info GitInfo
		root string
		want string
	}{
		{
			name: "invalid",
			info: GitInfo{},
			root: "/w",
			want: "",
		},
		{
			name: "clean repo at root",
			info: GitInfo{Valid: true, RepoRoot: "/w", Branch: "main", HeadCommit: testSHA},
			root: "/w",
			want: " (git repository, branch main, HEAD 0123456789ab)",
		},
		{
			name: "dirty",
			info: GitInfo{Valid: true, RepoRoot: "/w", Branch: "main", HeadCommit: testSHA, Dirty: true},
			root: "/w",
			want: " (git repository, branch main, HEAD 0123456789ab, dirty)",
		},
		{
			name: "workspace below the repo",
			info: GitInfo{Valid: true, RepoRoot: "/w", Branch: "main", HeadCommit: testSHA},
			root: "/w/pkg",
			want: " (in git repository /w, branch main, HEAD 0123456789ab)",
		},
		{
			name: "detached",
			info: GitInfo{Valid: true, RepoRoot: "/w", HeadCommit: testSHA},
			root: "/w",
			want: " (git repository, HEAD 0123456789ab)",
		},
		{
			name: "no commit yet",
			info: GitInfo{Valid: true, RepoRoot: "/w", Branch: "main"},
			root: "/w",
			want: " (git repository, branch main)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.info.describe(tc.root); got != tc.want {
				t.Errorf("describe() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The short SHA is an abbreviation, not the whole object id; a real 40-char
// value must be cut, not printed raw.
func TestGitDescribesShortCommit(t *testing.T) {
	long := GitInfo{Valid: true, RepoRoot: "/w", HeadCommit: testSHA}.describe("/w")
	if strings.Contains(long, testSHA) {
		t.Errorf("full SHA leaked into the prompt facts: %q", long)
	}
}

func TestNewGitFactsInSection(t *testing.T) {
	dir := t.TempDir()
	mkRepo(t, dir, "ref: refs/heads/main\n", map[string]string{"refs/heads/main": testSHA}, "")

	w := newAt(t, Config{Dir: dir, Git: true})
	if !w.Git().Valid {
		t.Fatalf("git snapshot invalid: %+v", w.Git())
	}
	out := render(t, w.Section())
	if !strings.Contains(out, "Root: "+dir+" (git repository, branch main, HEAD ") {
		t.Errorf("section missing the git facts:\n%s", out)
	}

	// Without Config.Git the facts block stays as it was.
	plain := newAt(t, Config{Dir: dir})
	if plain.Git().Valid {
		t.Error("Git() is valid without Config.Git")
	}
	if out := render(t, plain.Section()); strings.Contains(out, "git repository") {
		t.Errorf("git facts present without Config.Git:\n%s", out)
	}
}

// End-to-end against a real git: the on-disk shapes the file reader assumes
// (HEAD, loose refs, porcelain output, worktree pointer files) are git's, so at
// least one test must check them against the thing itself.
func TestProbeGitAgainstRealRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git not on PATH: %v", err)
	}
	repo := isolatedTemp(t)
	runGit(t, repo, "init", "-q", "-b", "main")
	runGit(t, repo, "config", "user.email", "test@example.com")
	runGit(t, repo, "config", "user.name", "Test")
	runGit(t, repo, "commit", "-q", "--allow-empty", "-m", "first")

	info := probeGit(repo, gitStatus)
	if !info.Valid || info.Branch != "main" || len(info.HeadCommit) != 40 {
		t.Fatalf("snapshot of a fresh repo = %+v", info)
	}
	if info.StatusErr != nil {
		t.Fatalf("dirty probe failed: %v", info.StatusErr)
	}
	if info.Dirty {
		t.Error("freshly committed repo reports dirty")
	}

	mustWriteFile(t, filepath.Join(repo, "untracked.txt"), "x")
	if info := probeGit(repo, gitStatus); !info.Dirty {
		t.Errorf("untracked file must read as dirty: %+v", info)
	}

	wt := filepath.Join(filepath.Dir(repo), "wt-feature")
	runGit(t, repo, "worktree", "add", "-q", "-b", "feature", wt)
	wtInfo := probeGit(wt, gitStatus)
	if !wtInfo.Valid {
		t.Fatalf("real linked worktree not recognised: %+v", wtInfo)
	}
	if wtInfo.Branch != "feature" {
		t.Errorf("worktree branch = %q, want feature", wtInfo.Branch)
	}
	if wtInfo.HeadCommit != info.HeadCommit {
		t.Errorf("worktree HEAD = %q, want the shared commit %q", wtInfo.HeadCommit, info.HeadCommit)
	}
	if abs, _ := filepath.Abs(wt); wtInfo.RepoRoot != abs {
		t.Errorf("worktree RepoRoot = %q, want the worktree dir %q", wtInfo.RepoRoot, abs)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}
