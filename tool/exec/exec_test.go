package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
)

// mockSandbox returns a fixed outcome or error, so these tests exercise the
// Outcome->Result formatting without spawning real processes.
type mockSandbox struct {
	out *sandbox.Outcome
	err error
}

func (m mockSandbox) Run(context.Context, sandbox.Spec) (*sandbox.Outcome, error) {
	return m.out, m.err
}

func call(t *testing.T, sb sandbox.Sandbox, command string) *tool.Result {
	t.Helper()
	tl := RunCommand(sb)
	if tl.Name() != "run_command" {
		t.Fatalf("name = %q, want run_command", tl.Name())
	}
	ctx := &tool.Context{Context: context.Background()}
	res, err := tl.Call(ctx, []byte(`{"command":"`+command+`","args":["a","b"]}`))
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	return res
}

// partText extracts the text of the result's single content part.
func partText(t *testing.T, res *tool.Result) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatal("result has no content")
	}
	txt, ok := res.Content[0].(core.Text)
	if !ok {
		t.Fatalf("content[0] is %T, want core.Text", res.Content[0])
	}
	return txt.Text
}

func TestRunCommandSuccess(t *testing.T) {
	sb := mockSandbox{out: &sandbox.Outcome{ExitCode: 0, Stdout: []byte("hi there")}}
	res := call(t, sb, "echo")
	if res.IsError {
		t.Error("IsError = true, want false on exit 0")
	}
	if text := partText(t, res); !strings.Contains(text, "hi there") || !strings.Contains(text, "exit 0") {
		t.Errorf("text = %q, want stdout and exit 0", text)
	}
}

func TestRunCommandNonZeroIsError(t *testing.T) {
	sb := mockSandbox{out: &sandbox.Outcome{ExitCode: 7, Stderr: []byte("nope")}}
	res := call(t, sb, "false")
	if !res.IsError {
		t.Error("IsError = false, want true on non-zero exit")
	}
	if text := partText(t, res); !strings.Contains(text, "exit 7") {
		t.Errorf("text = %q, want exit 7", text)
	}
}

func TestRunCommandTimeoutIsError(t *testing.T) {
	sb := mockSandbox{out: &sandbox.Outcome{TimedOut: true, ExitCode: -1}}
	res := call(t, sb, "sleep")
	if !res.IsError {
		t.Error("IsError = false, want true on timeout")
	}
	if text := partText(t, res); !strings.Contains(text, "timed out") {
		t.Errorf("text = %q, want timed out", text)
	}
}

func TestRunCommandPolicyViolationIsError(t *testing.T) {
	sb := mockSandbox{err: sandbox.ErrCommandNotAllowed}
	res := call(t, sb, "rm")
	if !res.IsError {
		t.Error("IsError = false, want true on policy violation")
	}
	if text := partText(t, res); !strings.Contains(text, "allow list") {
		t.Errorf("text = %q, want allow-list reason", text)
	}
}
