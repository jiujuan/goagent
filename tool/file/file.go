// Package file provides model-facing filesystem tools bound to one root
// directory. The root is an *os.Root, so containment is enforced by the
// operating system rather than by path string surgery: a name that resolves
// outside the root — including through a symbolic link pointing out of it — is
// refused there, and symbolic links may not be absolute.
//
// Every failure a model can plausibly correct (bad path, escaping path, missing
// file, unsupported encoding) comes back as a tool error the agent loop feeds
// back to the model, per the framework's "tool errors are data" stance.
//
// Traversal and read/write do not cross filesystem boundaries: os.Root confines
// by path resolution only, so a bind mount or symlink target inside the root is
// still reachable. See ADR 0022.
package file

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jiujuan/goagent/tool"
)

// maxReadBytes caps what read_file hands back, so one call cannot swallow the
// model's context. read_file reports the truncation in its output.
const maxReadBytes = 1 << 20

// Tools returns the four tools bound to root, in model-facing order.
func Tools(root *os.Root) []tool.Tool {
	return []tool.Tool{ReadFile(root), WriteFile(root), ListDir(root), Glob(root)}
}

// pathArgs is the input shared by the path-taking tools.
type pathArgs struct {
	Path string `json:"path" desc:"path relative to the working root, e.g. \"src/main.go\"; absolute paths and paths escaping the root are rejected"`
}

// writeArgs takes the full replacement content, not a patch.
type writeArgs struct {
	Path    string `json:"path" desc:"path relative to the working root, e.g. \"result/count.txt\""`
	Content string `json:"content" desc:"complete UTF-8 text to write, replacing any existing file"`
}

type globArgs struct {
	Pattern string `json:"pattern" desc:"glob relative to the working root, e.g. \"src/*.go\" or \"internal/reporoot/*_test.go\"; a single * never crosses a directory separator, so each level is written out"`
}

type listArgs struct {
	Path string `json:"path,omitempty" desc:"directory to list, relative to the working root; empty lists the root itself"`
}

// ReadFile builds read_file: it returns a text file's content, refusing binary.
func ReadFile(root *os.Root) tool.Tool {
	return tool.New("read_file",
		"Read a UTF-8 text file from the working root and return its content.",
		func(_ *tool.Context, in pathArgs) (string, error) {
			name, err := filePath(in.Path)
			if err != nil {
				return "", err
			}
			b, err := root.ReadFile(name)
			if err != nil {
				return "", readError(in.Path, err)
			}
			truncated := false
			if len(b) > maxReadBytes {
				b, truncated = b[:maxReadBytes], true
			}
			if !utf8.Valid(b) {
				return "", fmt.Errorf("%q is not valid UTF-8 text (%d bytes read so far); read it with a command instead", name, len(b))
			}
			if truncated {
				return string(b) + fmt.Sprintf("\n[truncated at %d bytes]", maxReadBytes), nil
			}
			return string(b), nil
		})
}

// WriteFile builds write_file: it writes (or replaces) a file, creating parent
// directories as needed.
func WriteFile(root *os.Root) tool.Tool {
	return tool.New("write_file",
		"Write a UTF-8 text file under the working root, creating parent directories and replacing any existing file.",
		func(_ *tool.Context, in writeArgs) (string, error) {
			name, err := filePath(in.Path)
			if err != nil {
				return "", err
			}
			if dir := path.Dir(name); dir != "." {
				if err := root.MkdirAll(dir, 0o755); err != nil {
					return "", fmt.Errorf("create directories for %q: %w", name, err)
				}
			}
			if err := root.WriteFile(name, []byte(in.Content), 0o644); err != nil {
				return "", fmt.Errorf("write %q: %w (a path that exists as a directory cannot be written)", name, err)
			}
			return fmt.Sprintf("wrote %s (%d bytes)", name, len(in.Content)), nil
		})
}

// ListDir builds list_dir: it returns one entry per line, directories suffixed
// with "/", sorted and relative to the working root.
func ListDir(root *os.Root) tool.Tool {
	return tool.New("list_dir",
		"List the entries of a directory under the working root; directories end in \"/\".",
		func(_ *tool.Context, in listArgs) (string, error) {
			name, err := dirPath(in.Path)
			if err != nil {
				return "", err
			}
			entries, err := fs.ReadDir(root.FS(), name)
			if err != nil {
				return "", fmt.Errorf("list %q: %w", in.Path, err)
			}
			lines := make([]string, 0, len(entries))
			for _, e := range entries {
				if e.IsDir() {
					lines = append(lines, e.Name()+"/")
					continue
				}
				lines = append(lines, e.Name())
			}
			sort.Strings(lines)
			if len(lines) == 0 {
				return "(empty)", nil
			}
			return strings.Join(lines, "\n"), nil
		})
}

// Glob builds glob: it matches a pattern against the working root and returns
// matching paths, one per line.
func Glob(root *os.Root) tool.Tool {
	return tool.New("glob",
		"Match a glob pattern against the working root and return the matching paths.",
		func(_ *tool.Context, in globArgs) (string, error) {
			pattern, err := filePath(in.Pattern)
			if err != nil {
				return "", err
			}
			matches, err := fs.Glob(root.FS(), pattern)
			if err != nil {
				return "", fmt.Errorf("glob %q: %w", in.Pattern, err)
			}
			if len(matches) == 0 {
				return "no matches", nil
			}
			sort.Strings(matches)
			return strings.Join(matches, "\n"), nil
		})
}

// filePath validates a model-supplied path and renders it in the form os.Root
// expects (slash-separated, relative to the root, "." excluded).
//
// The checks here are for legible errors, not for safety: os.Root already
// refuses out-of-root resolution, including via symlinks.
func filePath(p string) (string, error) {
	name, err := relPath(p)
	if err != nil {
		return "", err
	}
	if name == "." {
		return "", fmt.Errorf("%q names the working root itself; pass a file path inside it", p)
	}
	return name, nil
}

// dirPath is filePath but lets an empty path mean the working root.
func dirPath(p string) (string, error) {
	if strings.TrimSpace(p) == "" {
		return ".", nil
	}
	return relPath(p)
}

// relPath rejects absolute paths and root escapes, then cleans the remainder.
func relPath(p string) (string, error) {
	slashed := filepath.ToSlash(strings.TrimSpace(p))
	if slashed == "" {
		return "", fmt.Errorf("missing path; pass one relative to the working root")
	}
	if strings.HasPrefix(slashed, "/") || filepath.VolumeName(slashed) != "" {
		return "", fmt.Errorf("path %q is absolute; pass one relative to the working root", p)
	}
	clean := path.Clean(slashed)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path %q escapes the working root; stay inside it", p)
	}
	return clean, nil
}

// readError turns a failed read into a message that names the likely causes a
// model actually hits: a missing file, a directory, or an escaping path.
func readError(given string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read %q: no such file or directory (check the exact name and case; %w)", given, err)
	}
	return fmt.Errorf("read %q: %w", given, err)
}
