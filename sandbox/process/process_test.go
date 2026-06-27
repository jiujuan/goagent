package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/sandbox"
)

// helperEnv triggers the TestHelperProcess branch below. The sandbox runs with
// an empty environment, so this key must be in the policy's Env allow-list for
// the helper to activate.
const helperEnv = "GO_WANT_HELPER_PROCESS"

// helperSpec builds a Spec that re-executes this test binary in helper mode.
func helperSpec(mode string, extra ...string) sandbox.Spec {
	args := append([]string{"-test.run=TestHelperProcess", "--", mode}, extra...)
	return sandbox.Spec{Command: os.Args[0], Args: args}
}

// basePolicy is a baseline policy that allows the test binary and the helper
// trigger env. Tests override fields as needed.
func basePolicy(t *testing.T) sandbox.Policy {
	t.Helper()
	return sandbox.Policy{
		WorkDir: t.TempDir(),
		Timeout: 5 * time.Second,
		Env:     map[string]string{helperEnv: "1"},
	}
}

// TestHelperProcess is not a real test; it is the subprocess entry point. It
// exits before the testing framework can print anything, keeping captured
// output clean.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	args := os.Args
	for i, a := range os.Args {
		if a == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) == 0 {
		os.Exit(2)
	}
	switch args[0] {
	case "echo":
		fmt.Fprint(os.Stdout, strings.Join(args[1:], " "))
		os.Exit(0)
	case "exit":
		code := 0
		fmt.Sscanf(args[1], "%d", &code)
		fmt.Fprint(os.Stderr, "boom")
		os.Exit(code)
	case "sleep":
		time.Sleep(10 * time.Second)
		os.Exit(0)
	case "flood":
		chunk := strings.Repeat("x", 4096)
		for range 1000 {
			fmt.Fprint(os.Stdout, chunk)
		}
		os.Exit(0)
	case "printenv":
		fmt.Fprint(os.Stdout, os.Getenv(args[1]))
		os.Exit(0)
	case "pwd":
		wd, _ := os.Getwd()
		fmt.Fprint(os.Stdout, wd)
		os.Exit(0)
	default:
		os.Exit(2)
	}
}

func TestRunEchoCapturesStdout(t *testing.T) {
	sb, err := New(basePolicy(t))
	if err != nil {
		t.Fatal(err)
	}
	out, err := sb.Run(context.Background(), helperSpec("echo", "hello", "world"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0", out.ExitCode)
	}
	if got := string(out.Stdout); got != "hello world" {
		t.Errorf("stdout = %q, want %q", got, "hello world")
	}
}

func TestRunNonZeroExit(t *testing.T) {
	sb, err := New(basePolicy(t))
	if err != nil {
		t.Fatal(err)
	}
	out, err := sb.Run(context.Background(), helperSpec("exit", "3"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", out.ExitCode)
	}
	if string(out.Stderr) != "boom" {
		t.Errorf("stderr = %q, want %q", out.Stderr, "boom")
	}
}

func TestRunTimeoutKills(t *testing.T) {
	p := basePolicy(t)
	p.Timeout = 200 * time.Millisecond
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	out, err := sb.Run(context.Background(), helperSpec("sleep"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.TimedOut {
		t.Error("TimedOut = false, want true")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %s, expected the process to be killed promptly", elapsed)
	}
}

func TestRunOutputTruncated(t *testing.T) {
	p := basePolicy(t)
	p.MaxOutputBytes = 1024
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	out, err := sb.Run(context.Background(), helperSpec("flood"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.Truncated {
		t.Error("Truncated = false, want true")
	}
	if total := len(out.Stdout) + len(out.Stderr); int64(total) > p.MaxOutputBytes {
		t.Errorf("captured %d bytes, want <= %d", total, p.MaxOutputBytes)
	}
}

func TestRunCommandNotAllowed(t *testing.T) {
	p := basePolicy(t)
	p.AllowedCommands = []string{"only-this-tool"}
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sb.Run(context.Background(), helperSpec("echo", "hi"))
	if !errors.Is(err, sandbox.ErrCommandNotAllowed) {
		t.Errorf("err = %v, want ErrCommandNotAllowed", err)
	}
}

func TestRunCommandAllowedByBaseName(t *testing.T) {
	p := basePolicy(t)
	p.AllowedCommands = []string{filepath.Base(os.Args[0])}
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sb.Run(context.Background(), helperSpec("echo", "ok")); err != nil {
		t.Errorf("run: %v", err)
	}
}

func TestRunEnvIsolation(t *testing.T) {
	p := basePolicy(t)
	p.Env = map[string]string{helperEnv: "1", "FOO": "bar"}
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	// Allowed key is visible.
	out, err := sb.Run(context.Background(), helperSpec("printenv", "FOO"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(out.Stdout); got != "bar" {
		t.Errorf("FOO = %q, want %q", got, "bar")
	}
	// A key not in the allow-list (and present in the parent) is absent.
	out, err = sb.Run(context.Background(), helperSpec("printenv", "PATH"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(out.Stdout); got != "" {
		t.Errorf("PATH = %q, want empty (isolated)", got)
	}
}

func TestRunWorkDir(t *testing.T) {
	p := basePolicy(t)
	sb, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	out, err := sb.Run(context.Background(), helperSpec("pwd"))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(p.WorkDir)
	got, _ := filepath.EvalSymlinks(strings.TrimSpace(string(out.Stdout)))
	if got != want {
		t.Errorf("workdir = %q, want %q", got, want)
	}
}

func TestRunEmptyCommand(t *testing.T) {
	sb, err := New(basePolicy(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = sb.Run(context.Background(), sandbox.Spec{})
	if !errors.Is(err, sandbox.ErrInvalidSpec) {
		t.Errorf("err = %v, want ErrInvalidSpec", err)
	}
}

func TestNewRejectsBadWorkDir(t *testing.T) {
	if _, err := New(sandbox.Policy{WorkDir: "relative/path"}); !errors.Is(err, sandbox.ErrInvalidWorkDir) {
		t.Errorf("relative: err = %v, want ErrInvalidWorkDir", err)
	}
	if _, err := New(sandbox.Policy{WorkDir: filepath.Join(t.TempDir(), "does-not-exist")}); !errors.Is(err, sandbox.ErrInvalidWorkDir) {
		t.Errorf("missing: err = %v, want ErrInvalidWorkDir", err)
	}
}
