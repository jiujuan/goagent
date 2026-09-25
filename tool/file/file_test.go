package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// callTool invokes a tool with JSON args and returns its rendered text plus the
// IsError flag. The contract under test: a model-correctable failure comes back
// as an error *result*, never as a Go error.
func callTool(t *testing.T, tl tool.Tool, args string) (string, bool) {
	t.Helper()
	res, err := tl.Call(&tool.Context{Context: context.Background()}, json.RawMessage(args))
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	var b strings.Builder
	for _, p := range res.Content {
		if txt, ok := p.(core.Text); ok {
			b.WriteString(txt.Text)
		}
	}
	return b.String(), res.IsError
}

// openRoot returns an *os.Root over a fresh temp dir plus its raw path, for
// seeding files behind the root's back.
func openRoot(t *testing.T) (*os.Root, string) {
	t.Helper()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot(%s): %v", dir, err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("root.Close: %v", err)
		}
	})
	return root, dir
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func TestToolNames(t *testing.T) {
	root, _ := openRoot(t)
	var got []string
	for _, tl := range Tools(root) {
		got = append(got, tl.Name())
		if len(tl.Schema()) == 0 {
			t.Errorf("%s has empty schema", tl.Name())
		}
		if tl.Description() == "" {
			t.Errorf("%s has empty description", tl.Name())
		}
	}
	want := "read_file write_file list_dir glob"
	if strings.Join(got, " ") != want {
		t.Errorf("Tools() = %q, want %q", strings.Join(got, " "), want)
	}
}

func TestReadFile(t *testing.T) {
	root, dir := openRoot(t)
	mustWrite(t, filepath.Join(dir, "hello.txt"), "hello world\n")

	got, isErr := callTool(t, ReadFile(root), `{"path":"hello.txt"}`)
	if isErr {
		t.Fatalf("read_file errored: %s", got)
	}
	if got != "hello world\n" {
		t.Errorf("content = %q, want %q", got, "hello world\n")
	}

	// Backslashes and redundant separators normalise to the same file.
	if got, isErr := callTool(t, ReadFile(root), `{"path":"./hello.txt"}`); isErr || got != "hello world\n" {
		t.Errorf("read_file(./hello.txt) = %q (isErr %v)", got, isErr)
	}

	if got, isErr := callTool(t, ReadFile(root), `{"path":"missing.txt"}`); !isErr {
		t.Errorf("read_file(missing.txt) should error, got %q", got)
	} else if !strings.Contains(got, "no such file or directory") {
		t.Errorf("missing-file error should name the cause, got %q", got)
	}
}

func TestReadFileRefusesBinary(t *testing.T) {
	root, dir := openRoot(t)
	if err := os.WriteFile(filepath.Join(dir, "blob.bin"), []byte{0xff, 0xfe, 0x00, 0x41}, 0o644); err != nil {
		t.Fatal(err)
	}
	got, isErr := callTool(t, ReadFile(root), `{"path":"blob.bin"}`)
	if !isErr {
		t.Fatalf("binary read should error, got %q", got)
	}
	if !strings.Contains(got, "not valid UTF-8") {
		t.Errorf("error should say why, got %q", got)
	}
}

func TestReadFileTruncatesHugeFile(t *testing.T) {
	root, dir := openRoot(t)
	big := strings.Repeat("a", maxReadBytes+1)
	mustWrite(t, filepath.Join(dir, "big.txt"), big)

	got, isErr := callTool(t, ReadFile(root), `{"path":"big.txt"}`)
	if isErr {
		t.Fatalf("read_file errored: %s", got)
	}
	marker := "\n[truncated at 1048576 bytes]"
	if !strings.HasSuffix(got, marker) {
		t.Errorf("oversized read should report truncation, tail = %q", tail(got, 64))
	}
	if n := len(strings.TrimSuffix(got, marker)); n != maxReadBytes {
		t.Errorf("truncated content length = %d, want %d", n, maxReadBytes)
	}
}

func TestWriteFile(t *testing.T) {
	root, dir := openRoot(t)

	got, isErr := callTool(t, WriteFile(root), `{"path":"result/nested/count.txt","content":"42"}`)
	if isErr {
		t.Fatalf("write_file errored: %s", got)
	}
	if !strings.Contains(got, "result/nested/count.txt") {
		t.Errorf("result should name the written path, got %q", got)
	}
	b, err := os.ReadFile(filepath.Join(dir, "result", "nested", "count.txt"))
	if err != nil || string(b) != "42" {
		t.Fatalf("file on disk = %q (err %v), want \"42\"", b, err)
	}

	// A directory cannot be written over, and the failure is data.
	if err := os.Mkdir(filepath.Join(dir, "adir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got, isErr := callTool(t, WriteFile(root), `{"path":"adir","content":"x"}`); !isErr {
		t.Errorf("write over a directory should error, got %q", got)
	}
}

func TestListDir(t *testing.T) {
	root, dir := openRoot(t)
	mustWrite(t, filepath.Join(dir, "b.txt"), "b")
	mustWrite(t, filepath.Join(dir, "a.txt"), "a")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, isErr := callTool(t, ListDir(root), `{}`)
	if isErr {
		t.Fatalf("list_dir(.) errored: %s", got)
	}
	if want := "a.txt\nb.txt\nsub/"; got != want {
		t.Errorf("root listing = %q, want %q", got, want)
	}

	if got, isErr := callTool(t, ListDir(root), `{"path":"sub"}`); isErr || got != "(empty)" {
		t.Errorf("empty dir listing = %q (isErr %v), want \"(empty)\"", got, isErr)
	}

	if got, isErr := callTool(t, ListDir(root), `{"path":"nope"}`); !isErr {
		t.Errorf("listing a missing dir should error, got %q", got)
	}
}

func TestGlob(t *testing.T) {
	root, dir := openRoot(t)
	for _, p := range []string{"main.go", "internal/pkg/a.go", "internal/pkg/a_test.go", "README.md"} {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, full, p)
	}

	got, isErr := callTool(t, Glob(root), `{"pattern":"internal/pkg/*_test.go"}`)
	if isErr {
		t.Fatalf("glob errored: %s", got)
	}
	if got != "internal/pkg/a_test.go" {
		t.Errorf("glob = %q", got)
	}

	got, isErr = callTool(t, Glob(root), `{"pattern":"*.go"}`)
	if isErr || got != "main.go" {
		t.Errorf(`glob("*.go") = %q (isErr %v); a single * must not cross separators`, got, isErr)
	}

	if got, isErr := callTool(t, Glob(root), `{"pattern":"*.rs"}`); isErr || got != "no matches" {
		t.Errorf("unmatched glob = %q (isErr %v), want \"no matches\"", got, isErr)
	}
}

// TestPathGuards pins the model-facing refusals: absolute paths, root escapes,
// and the root itself. Each must be an error result carrying a fixable message.
func TestPathGuards(t *testing.T) {
	root, _ := openRoot(t)

	cases := []struct {
		name, toolName, args, wantSubstr string
		tool                             tool.Tool
	}{
		{"read absolute", "read_file", `{"path":"/etc/passwd"}`, "absolute", ReadFile(root)},
		{"read escape", "read_file", `{"path":"../../secret.txt"}`, "escapes the working root", ReadFile(root)},
		{"read root itself", "read_file", `{"path":"."}`, "working root itself", ReadFile(root)},
		{"read empty", "read_file", `{"path":"   "}`, "missing path", ReadFile(root)},
		{"write escape", "write_file", `{"path":"../out.txt","content":"x"}`, "escapes the working root", WriteFile(root)},
		{"list escape", "list_dir", `{"path":"../"}`, "escapes the working root", ListDir(root)},
		{"glob absolute", "glob", `{"path":"","pattern":"/tmp/*"}`, "absolute", Glob(root)},
		{"glob escape", "glob", `{"pattern":"../**/*"}`, "escapes the working root", Glob(root)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, isErr := callTool(t, tc.tool, tc.args)
			if !isErr {
				t.Fatalf("%s(%s) should error, got %q", tc.toolName, tc.args, got)
			}
			if !strings.Contains(got, tc.wantSubstr) {
				t.Errorf("error %q should mention %q", got, tc.wantSubstr)
			}
		})
	}
}

// TestWindowsDriveLetterIsAbsolute is the Windows half of the absolute-path
// refusal: filepath.VolumeName only recognises "C:" on Windows.
func TestWindowsDriveLetterIsAbsolute(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("drive-letter volumes are a Windows concept (" + runtime.GOOS + ")")
	}
	root, _ := openRoot(t)
	got, isErr := callTool(t, ReadFile(root), `{"path":"C:/Windows/win.ini"}`)
	if !isErr {
		t.Fatalf(`read_file("C:/Windows/win.ini") should error, got %q`, got)
	}
	if !strings.Contains(got, "absolute") {
		t.Errorf("error should name the absolute-path problem, got %q", got)
	}
}

// TestWindowsReservedNames documents the platform-specific refusal os.Root
// adds: model-supplied names like NUL or COM1 never reach the filesystem.
func TestWindowsReservedNames(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows reserved device names (NUL, COM1) do not exist on " + runtime.GOOS)
	}
	root, _ := openRoot(t)
	for _, name := range []string{"NUL", "COM1"} {
		got, isErr := callTool(t, ReadFile(root), `{"path":"`+name+`"}`)
		if !isErr {
			t.Errorf("read_file(%s) should error, got %q", name, got)
		}
	}
}

// TestSymlinkCannotEscape is the containment claim ADR 0022 relies on for
// handing an *os.Root to the model: a link stored inside the root but pointing
// out of it is refused by the OS layer, not by our string checks.
func TestSymlinkCannotEscape(t *testing.T) {
	root, dir := openRoot(t)

	outside := filepath.Join(filepath.Dir(dir), "outside-secret.txt")
	mustWrite(t, outside, "top secret")

	link := filepath.Join(dir, "escape.txt")
	if err := os.Symlink("../outside-secret.txt", link); err != nil {
		t.Skipf("symlinks unavailable on %s: %v", runtime.GOOS, err)
	}

	got, isErr := callTool(t, ReadFile(root), `{"path":"escape.txt"}`)
	if !isErr {
		t.Fatalf("reading through an escaping symlink succeeded: %q", got)
	}
	if strings.Contains(got, "top secret") {
		t.Errorf("leaked outside content: %q", got)
	}
}

// TestSymlinkedDirCannotEscape covers the same escape through a directory link
// in list_dir, which goes via root.FS() rather than a Root method.
func TestSymlinkedDirCannotEscape(t *testing.T) {
	root, dir := openRoot(t)

	outsideDir := filepath.Join(filepath.Dir(dir), "outside-dir")
	if err := os.MkdirAll(filepath.Join(outsideDir, "inner"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(outsideDir, "inner", "secret.txt"), "secret")

	if err := os.Symlink("../outside-dir", filepath.Join(dir, "linked")); err != nil {
		t.Skipf("directory symlinks unavailable on %s: %v", runtime.GOOS, err)
	}

	got, isErr := callTool(t, ListDir(root), `{"path":"linked"}`)
	if isErr {
		return // refused outright: exactly the containment we want
	}
	if strings.Contains(got, "secret.txt") || strings.Contains(got, "inner") {
		t.Fatalf("list_dir escaped the root through a symlink: %q", got)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
