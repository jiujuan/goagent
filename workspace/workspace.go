// Package workspace assembles an agent's working context from one config: the
// directory it works in, the filesystem handle confined to that directory, the
// rules and project memory that describe it, and the prompt sections and tools
// that expose all of it to the model.
//
// It is an assembly layer, not a new capability: every part comes from an
// existing package (tool/file, memory/rules, memory/projectmem,
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

	"github.com/jiujuan/goagent/internal/reporoot"
	"github.com/jiujuan/goagent/memory/projectmem"
	"github.com/jiujuan/goagent/memory/rules"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/tool/file"
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

	// ProjectMemory loads the AGENTS.md chain from the root. Off by default:
	// injecting whole project memory changes the prompt substantially, so it is
	// the caller's explicit decision.
	ProjectMemory bool

	// Git takes the read-only repository snapshot (branch, HEAD, dirty) and adds
	// it to the workspace facts block. Off by default because the dirty probe
	// runs a subprocess.
	Git bool
}

// Workspace owns an open filesystem handle, so it has a lifetime: Close it.
type Workspace struct {
	root    string
	fs      *os.Root
	docs    []projectmem.Doc
	rules   *rules.Set
	gitInfo GitInfo
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

	set, err := rules.Load(w.rulesDir(cfg), w.projectRulesDir(cfg))
	if err != nil {
		fs.Close()
		return nil, err
	}
	w.rules = set

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

func (w *Workspace) projectRulesDir(cfg Config) string {
	if cfg.ProjectRulesDir != "" {
		return cfg.ProjectRulesDir
	}
	return filepath.Join(w.root, userDirName, "rules")
}

func (w *Workspace) rulesDir(cfg Config) string {
	if cfg.GlobalRulesDir != "" {
		return cfg.GlobalRulesDir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // no user home to look in; global rules contribute nothing
	}
	return filepath.Join(home, userDirName, "rules")
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

// Git returns the read-only repository snapshot. It is the zero value (Valid
// false) unless Config.Git was set, and Valid is false when the root is not
// inside a repository — that is a fact, not an error.
func (w *Workspace) Git() GitInfo { return w.gitInfo }

// Sections returns the prompt blocks this workspace contributes, in whatever
// order the Builder wants (they carry their own Order): rules, project memory,
// and the workspace facts block. Empty contributions are left out.
func (w *Workspace) Sections() []prompt.Section {
	var out []prompt.Section
	if len(w.rules.Rules()) > 0 {
		out = append(out, w.rules.Section())
	}
	if len(w.docs) > 0 {
		out = append(out, projectmem.Section(w.docs))
	}
	return append(out, w.Section())
}
