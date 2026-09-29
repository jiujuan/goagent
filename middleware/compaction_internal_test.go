package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestEstimateTokensScript(t *testing.T) {
	t.Run("ascii 4-per-token", func(t *testing.T) {
		m := []core.Message{core.UserText("hello world")} // 11 other chars → 3 + 4 overhead
		if got := estimateTokens(m); got != 7 {
			t.Fatalf("ascii = %d, want 7", got)
		}
	})
	t.Run("cjk ~1-per-char", func(t *testing.T) {
		m := []core.Message{core.UserText("你好世界")} // 4 wide → 4 + 4 overhead = 8 (old byte/4 would give 3)
		if got := estimateTokens(m); got != 8 {
			t.Fatalf("cjk = %d, want 8", got)
		}
	})
	t.Run("tool call args counted", func(t *testing.T) {
		bare := []core.Message{{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{Name: "abc"}}}}
		withArgs := []core.Message{{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{Name: "abc", Args: json.RawMessage(`{"path":"/very/long/file/name.txt"}`)}}}}
		if estimateTokens(withArgs) <= estimateTokens(bare) {
			t.Fatal("tool-call args must increase the estimate")
		}
	})
}

func TestSafeCutPairing(t *testing.T) {
	msg := func(r core.Role) core.Message { return core.Message{Role: r} }
	cases := []struct {
		name  string
		roles []core.Role
		keep  int
		want  int
	}{
		{"assistant boundary", []core.Role{core.RoleUser, core.RoleAssistant, core.RoleTool, core.RoleTool, core.RoleAssistant, core.RoleTool}, 2, 4},
		{"backs off orphan tool", []core.Role{core.RoleUser, core.RoleAssistant, core.RoleTool, core.RoleTool}, 1, 1},
		{"no boundary above keep", []core.Role{core.RoleAssistant, core.RoleTool, core.RoleTool}, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var msgs []core.Message
			for _, r := range tc.roles {
				msgs = append(msgs, msg(r))
			}
			if got := safeCut(msgs, tc.keep); got != tc.want {
				t.Fatalf("safeCut = %d, want %d", got, tc.want)
			}
		})
	}
}

func newLC(state *core.State, req *llm.Request) *agent.LoopContext {
	return &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: state}, Request: req}
}

func TestCompactionCalibration(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("S") })
	c := Compaction(CompactionOptions{Model: sum, MaxTokens: 1, KeepRecent: 2}).(*compaction)

	st := &core.State{}
	lc := newLC(st, nil)
	if _, ok := loadCalib(lc); ok {
		t.Fatal("fresh state should have no calibration")
	}

	// A request whose real input is double the estimate → factor ≈ 2.
	msgs := make([]core.Message, 8)
	for i := range msgs {
		msgs[i] = core.UserText("12345678")
	}
	req := &llm.Request{Messages: msgs}
	lc.Request = req
	est := c.counter(msgs)
	if _, err := c.AfterModel(lc, &llm.Response{Usage: &core.Usage{InputTokens: est * 2}}); err != nil {
		t.Fatal(err)
	}
	f, ok := loadCalib(lc)
	if !ok || f < 1.9 || f > 2.0 {
		t.Fatalf("calib after first = %f (ok=%v), want ~2.0", f, ok)
	}

	// A second call with ratio 1 blends toward it: 0.7*2 + 0.3*1 = 1.7.
	if _, err := c.AfterModel(lc, &llm.Response{Usage: &core.Usage{InputTokens: est}}); err != nil {
		t.Fatal(err)
	}
	if f, _ := loadCalib(lc); f < 1.6 || f > 1.8 {
		t.Fatalf("calib after blend = %f, want ~1.7", f)
	}

	// An absurd ratio is clamped to 2.0.
	if _, err := c.AfterModel(lc, &llm.Response{Usage: &core.Usage{InputTokens: est * 1000}}); err != nil {
		t.Fatal(err)
	}
	if f, _ := loadCalib(lc); f > 2.0 {
		t.Fatalf("calib exceeded clamp: %f", f)
	}
}

func TestCompactionCalibrationIgnoresMissingUsage(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("S") })
	c := Compaction(CompactionOptions{Model: sum}).(*compaction)
	st := &core.State{}
	lc := newLC(st, &llm.Request{Messages: []core.Message{core.UserText("x")}})
	if _, err := c.AfterModel(lc, &llm.Response{Usage: nil}); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadCalib(lc); ok {
		t.Fatal("nil Usage must not write a calibration")
	}
}
