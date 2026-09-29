// Package workspace assembles an agent's working context from one config: the
// directory it works in, the filesystem handle confined to that directory, the
// rules and project memory that describe it, the per-thread artifact store it
// hands out, and the prompt sections and tools that expose all of it to the
// model.
//
// It is an assembly layer, not a new capability: every part comes from an
// existing package (tool/file, memory/rules, memory/projectmem, skills,
// internal/reporoot, sandbox/process) and stays usable on its own. The value
// added is that the three facts a model must agree on — where it is, what it
// may touch, what runs there — are derived from a single root instead of being
// wired by hand and drifting apart.
//
// Containment is exact: os.Root confines path *resolution*, so the framework's
// own file tools cannot leave the root, but a command started through the
// sandbox can still cd out. Sections say so rather than implying a jail. See
// ADR 0022.
package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jiujuan/goagent/internal/reporoot"
	"github.com/jiujuan/goagent/memory/projectmem"
	"github.com/jiujuan/goagent/memory/rules"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/skills"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/tool/file"
	"github.com/jiujuan/goagent/vfs"
)

// userDirName is the conventional per-user and per-repo config directory,
// shared with memory/rules and config's default search path.
const userDirName = ".goagent"

// Config selects which parts a workspace carries. Empty directory fields fall
// back to the conventional paths ($HOME/.goagent/... and <root>/.goagent/...);
// a conventional path that holds no directory simply contributes nothing, so
// there is no explicit "disable" switch — point a field somewhere empty to turn
// a source off.
type Config struct {
	// Dir is where root resolution starts. Empty means the process's own
	// working directory.
	Dir string
	// ResolveRoot walks up to the repository boundary (ADR 0020's rule);
	// without it, Dir's absolute form is the root.
	ResolveRoot bool

	// Rules merge global-then-project, same ID goes to the project.
	GlobalRulesDir  string
	ProjectRulesDir string

	// Skills merge global-then-workspace, same name goes to the workspace — the
	// precedence rules use too. Their Level-1 list joins Sections(); SkillGate
	// additionally enforces each skill's allowed-tools declaration.
	GlobalSkillsDir  string
	ProjectSkillsDir string

	// ProjectMemory loads the AGENTS.md chain from the root. Off by default:
	// injecting whole project memory changes the prompt substantially, so it is
	// the caller's explicit decision.
	ProjectMemory bool

	// Git takes the read-only repository snapshot (branch, HEAD, dirty) and adds
	// it to the workspace facts block. Off by default because the dirty probe
	// runs a subprocess.
	Git bool

	// SkillGate makes Gate() hand back the allowed-tools middleware. Off by
	// default because gating changes which tool calls a run pauses for.
	SkillGate bool
}

// Workspace owns an open filesystem handle, so it has a lifetime: Close it.
type Workspace struct {
	root          string
	fs            *os.Root
	docs          []projectmem.Doc
	rules         *rules.Set
	skills        *skills.Library
	gateRequested bool
	gitInfo       GitInfo
}

// New resolves the root, opens it, and loads the configured parts. A root that
// does not exist is an error: creating the directory a workspace reads and
// writes is the caller's decision, not a side effect of assembly.
func New(cfg Config) (*Workspace, error) {
	root, err := resolveRoot(cfg)
	if err != nil {
		return nil, err
	}
	fs, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("workspace: open root %s: %w", root, err)
	}
	w := &Workspace{root: root, fs: fs}

	set, err := rules.Load(
		globalDir(cfg.GlobalRulesDir, "rules"),
		w.projectDir(cfg.ProjectRulesDir, "rules"))
	if err != nil {
		fs.Close()
		return nil, err
	}
	w.rules = set

	lib, err := skills.LoadDirs(
		globalDir(cfg.GlobalSkillsDir, "skills"),
		w.projectDir(cfg.ProjectSkillsDir, "skills"))
	if err != nil {
		// A skills directory that exists but holds a broken SKILL.md fails
		// assembly, like rules and project memory do: dropping a capability the
		// caller asked for is worse than refusing to start.
		fs.Close()
		return nil, err
	}
	if lib.Len() > 0 {
		// An empty library stays nil: "no skills" is the absent case for Skills,
		// SkillTools, Gate and Sections, and nil states it without counting.
		w.skills = lib
	}
	w.gateRequested = cfg.SkillGate

	if cfg.ProjectMemory {
		docs, err := projectmem.Load(root)
		if err != nil {
			fs.Close()
			return nil, err
		}
		w.docs = docs
	}
	if cfg.Git {
		// A failed dirty probe lands in GitInfo.StatusErr rather than failing
		// assembly: not having git installed must not make a workspace unusable.
		w.gitInfo = probeGit(root, gitStatus)
	}
	return w, nil
}

// resolveRoot turns Config into the single directory everything anchors to.
func resolveRoot(cfg Config) (string, error) {
	dir := cfg.Dir
	if dir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("workspace: no Dir configured and the working directory is unreadable: %w", err)
		}
		dir = cwd
	}
	if cfg.ResolveRoot {
		// Lexical on purpose: a root reached through a symlink would make the
		// reported path differ from the one os.Root confines.
		return reporoot.Root(dir)
	}
	return filepath.Abs(dir)
}

// projectDir is a configured source path, or the conventional one inside the
// repository: <root>/.goagent/<sub>.
func (w *Workspace) projectDir(configured, sub string) string {
	if configured != "" {
		return configured
	}
	return filepath.Join(w.root, userDirName, sub)
}

// globalDir is projectDir's user-level counterpart at $HOME/.goagent/<sub>. It
// returns "" when the host has no user home to look in, which the loaders read
// as "this source contributes nothing".
func globalDir(configured, sub string) string {
	if configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, userDirName, sub)
}

// Root returns the absolute directory the workspace is confined to.
func (w *Workspace) Root() string { return w.root }

// FS returns the confined filesystem handle. It is valid until Close; callers
// that want a narrower scope can take w.FS().Sub(name).
func (w *Workspace) FS() *os.Root { return w.fs }

// Close releases the filesystem handle. The workspace is unusable afterwards.
func (w *Workspace) Close() error {
	if err := w.fs.Close(); err != nil {
		return fmt.Errorf("workspace: close root %s: %w", w.root, err)
	}
	return nil
}

// Tools returns the file tools bound to this workspace's root.
func (w *Workspace) Tools() []tool.Tool { return file.Tools(w.fs) }

// RunFiles attaches a disk-backed artifact store for one thread, rooted at
// <root>/.goagent/files/<thread>. That is the workspace's own config directory,
// so the artifacts stay inside the root the model's file tools are confined to
// and remain readable by them; it is also where a human can look for them.
//
// The store is externally managed (ADR-0027): checkpoints do not copy its bytes,
// so every process resuming the thread re-attaches an equivalent handle through
// agent.WithRunFiles, and time travel does not roll these files back. It holds
// its own os.Root confined to that one directory, so it stays usable after the
// Workspace is closed — and closing it is the caller's job.
func (w *Workspace) RunFiles(threadID string) (*vfs.DirStore, error) {
	dir := filepath.Join(w.root, userDirName, "files", safeThread(threadID))
	store, err := vfs.NewDirStore(dir)
	if err != nil {
		return nil, fmt.Errorf("workspace: run files for thread %s: %w", threadID, err)
	}
	return store, nil
}

// safeThread maps a thread id to one filesystem-safe directory name: characters
// outside [A-Za-z0-9_-] become "_", an empty id becomes "thread". Two ids that
// differ only in mapped characters therefore share a directory, which is the
// property checkpoint.safeName already has for its file names. The two helpers
// are kept apart rather than widening an exported surface for twenty private
// lines (ADR-0027, alternative C).
func safeThread(threadID string) string {
	var b strings.Builder
	for _, r := range threadID {
		switch {
		case r == '-' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "thread"
	}
	return b.String()
}

// Skills returns the library merged global-then-workspace, or nil when no
// skills directory had a skill in it.
func (w *Workspace) Skills() *skills.Library { return w.skills }

// SkillTools returns use_skill and run_skill_script, the latter executing a
// loaded skill's bundled script through sb — so sb's Policy.AllowedCommands must
// name the interpreters (see skills.ScriptTool). It returns nothing when the
// workspace has no skills, since use_skill with an empty list is a tool the
// model can only call incorrectly.
func (w *Workspace) SkillTools(sb sandbox.Sandbox) []tool.Tool {
	if w.skills == nil {
		return nil
	}
	return []tool.Tool{skills.Tool(w.skills), skills.ScriptTool(w.skills, sb)}
}

// Git returns the read-only repository snapshot. It is the zero value (Valid
// false) unless Config.Git was set, and Valid is false when the root is not
// inside a repository — that is a fact, not an error.
func (w *Workspace) Git() GitInfo { return w.gitInfo }

// Sections returns the prompt blocks this workspace contributes: rules, project
// memory, the workspace facts block, and the Level-1 skill list when skills
// loaded. Empty contributions are left out; each section carries its own Order,
// so a Builder sorts them regardless of the order returned here.
func (w *Workspace) Sections() []prompt.Section {
	var out []prompt.Section
	if len(w.rules.Rules()) > 0 {
		out = append(out, w.rules.Section())
	}
	if len(w.docs) > 0 {
		out = append(out, projectmem.Section(w.docs))
	}
	out = append(out, w.Section())
	if w.skills != nil {
		out = append(out, skills.PromptSection(w.skills))
	}
	return out
}
