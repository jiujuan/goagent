package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
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

func TestCompactionRecoveryTarget(t *testing.T) {
	cases := []struct {
		before, limit, want int
	}{
		{100, 0, 50},   // unknown limit: halve the history
		{100, 200, 50}, // a generous limit is still capped by before/2
		{100, 60, 45},  // three quarters of the provider limit
		{100, 40, 30},
		{1, 100, 0}, // nothing left to summarize
	}
	for _, tc := range cases {
		if got := compactionRecoveryTarget(tc.before, tc.limit); got != tc.want {
			t.Errorf("compactionRecoveryTarget(%d, %d) = %d, want %d", tc.before, tc.limit, got, tc.want)
		}
	}
}

func TestForceCut(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("S") })
	c := Compaction(CompactionOptions{Model: sum, Counter: newCounter(10)}).(*compaction)
	lc := newLC(&core.State{}, nil)

	history := []core.Message{
		core.UserText("q"),
		{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: "c1", Name: "fetch"}}},
		{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: "c1", Name: "fetch"}}},
		core.UserText("next"),
		core.AssistantText("answer"),
		core.UserText("again"),
	}

	// Each message costs 10; target/2 = 20 leaves exactly two messages, which
	// start on an assistant boundary after the tool result is backed over.
	cut := forceCut(c, lc, history, 40)
	if cut != 4 {
		t.Fatalf("forceCut = %d, want 4", cut)
	}
	if r := history[cut].Role; r != core.RoleUser && r != core.RoleAssistant {
		t.Fatalf("cut starts on %q, want a user or assistant boundary", r)
	}
	if got := forceCut(c, lc, history, 0); got != 5 {
		t.Fatalf("tight target forceCut = %d, want safeCut(msgs, 1) = 5", got)
	}

	orphans := []core.Message{
		{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: "c1", Name: "fetch"}}},
		{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: "c1", Name: "fetch"}}},
		{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: "c1", Name: "fetch"}}},
	}
	if got := forceCut(c, lc, orphans, 40); got != 0 {
		t.Fatalf("forceCut on an unpairable history = %d, want 0", got)
	}
}

func TestCompactionRecoverContext(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	c := Compaction(CompactionOptions{Model: sum, MaxTokens: 1 << 30, KeepRecent: 2, Counter: newCounter(10)}).(*compaction)
	lc := newLC(&core.State{}, nil)
	history := recoveryHistory(8)

	before := c.calibratedEstimate(lc, history)
	out, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100))
	if !ok {
		t.Fatal("recovery did not run")
	}
	if len(out) != 3 {
		t.Fatalf("recovered history has %d messages, want 3", len(out))
	}
	if out[0].Role != core.RoleUser || !strings.HasPrefix(out[0].Text(), summaryMark) {
		t.Fatalf("first message = %q/%q, want a user summary note", out[0].Role, out[0].Text())
	}
	if after := c.calibratedEstimate(lc, out); after >= before {
		t.Fatalf("estimate after recovery = %d, want less than %d", after, before)
	}
}

func TestCompactionRecoverContextIgnoresNormalThreshold(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	c := Compaction(CompactionOptions{Model: sum, MaxTokens: 1 << 30, KeepRecent: 100, Counter: newCounter(10)}).(*compaction)
	lc := newLC(&core.State{}, nil)
	history := recoveryHistory(8)

	out, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100))
	if !ok {
		t.Fatal("recovery must ignore MaxTokens and KeepRecent")
	}
	if len(out) >= len(history) {
		t.Fatalf("recovered history has %d messages, want fewer than %d", len(out), len(history))
	}
}

func TestCompactionRecoverContextRequestMode(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	c := Compaction(CompactionOptions{Model: sum, MaxTokens: 1 << 30, KeepRecent: 2, Counter: newCounter(10)}).(*compaction)
	b := bus.New()
	ch, cancel := b.Subscribe("compaction-recovery", bus.Lossy)
	defer cancel()
	lc := &agent.LoopContext{
		RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}, Bus: b, Topic: "compaction-recovery"},
		Step:       3,
	}
	history := recoveryHistory(8)

	if _, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100)); !ok {
		t.Fatal("request mode must still recover on overflow")
	}
	cancel()
	events := winDrainEvents(ch)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1 HistoryCompacted", len(events))
	}
	ev, ok := events[0].(core.HistoryCompacted)
	if !ok {
		t.Fatalf("event = %T, want core.HistoryCompacted", events[0])
	}
	if ev.Dropped != 6 || ev.Kept != 2 || ev.EstTokens != 80 || ev.Step != 3 {
		t.Fatalf("event = %+v, want Dropped 6, Kept 2, EstTokens 80, Step 3", ev)
	}
}

func TestCompactionRecoverContextRejectsNoShrink(t *testing.T) {
	t.Run("history too short", func(t *testing.T) {
		sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
		c := Compaction(CompactionOptions{Model: sum, MaxTokens: 1 << 30, KeepRecent: 2, Counter: newCounter(10)}).(*compaction)
		b := bus.New()
		ch, cancel := b.Subscribe("compaction-short", bus.Lossy)
		defer cancel()
		lc := &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}, Bus: b, Topic: "compaction-short"}}
		history := []core.Message{core.UserText("a"), core.UserText("b")}

		out, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100))
		if ok {
			t.Fatal("a summary note that does not shrink the history must be rejected")
		}
		if len(out) != len(history) {
			t.Fatalf("rejected recovery changed the history: %d messages", len(out))
		}
		cancel()
		if n := len(winDrainEvents(ch)); n != 0 {
			t.Fatalf("a rejected recovery published %d events", n)
		}
	})

	t.Run("summary too long", func(t *testing.T) {
		long := mock.New("long", func(*llm.Request) *llm.Response { return mock.Text(strings.Repeat("x", 4000)) })
		c := Compaction(CompactionOptions{Model: long, MaxTokens: 1 << 30, KeepRecent: 2}).(*compaction)
		b := bus.New()
		ch, cancel := b.Subscribe("compaction-long", bus.Lossy)
		defer cancel()
		lc := &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}, Bus: b, Topic: "compaction-long"}}
		history := []core.Message{core.UserText("aaaa"), core.UserText("bbbb"), core.UserText("cccc"), core.UserText("dddd")}

		before := c.calibratedEstimate(lc, history)
		out, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100))
		if ok {
			t.Fatal("an oversized summary must be rejected")
		}
		if c.calibratedEstimate(lc, out) < before {
			t.Fatalf("fixture must not shrink: %d -> %d", before, c.calibratedEstimate(lc, out))
		}
		cancel()
		if n := len(winDrainEvents(ch)); n != 0 {
			t.Fatalf("a rejected recovery published %d events", n)
		}
	})
}

func TestCompactionRecoverContextSummaryFailure(t *testing.T) {
	c := Compaction(CompactionOptions{Model: failingSummaryModel{}, MaxTokens: 1 << 30, KeepRecent: 2, Counter: newCounter(10)}).(*compaction)
	b := bus.New()
	ch, cancel := b.Subscribe("compaction-fail", bus.Lossy)
	defer cancel()
	lc := &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}, Bus: b, Topic: "compaction-fail"}}
	history := recoveryHistory(8)

	out, ok := c.RecoverContext(lc, history, recoveryOverflow(t, 100))
	if ok {
		t.Fatal("a summary error must leave the history unchanged")
	}
	if len(out) != len(history) {
		t.Fatalf("failed recovery changed the history: %d messages", len(out))
	}
	cancel()
	if n := len(winDrainEvents(ch)); n != 0 {
		t.Fatalf("a failed summary published %d events", n)
	}
}

func recoveryHistory(n int) []core.Message {
	msgs := make([]core.Message, n)
	for i := range msgs {
		msgs[i] = core.UserText(fmt.Sprintf("message-%d", i))
	}
	return msgs
}

func recoveryOverflow(t *testing.T, limit int) *llm.ContextLengthError {
	t.Helper()
	err := llm.NewStatusError("test", 400, fmt.Sprintf("maximum context length is %d tokens, however you requested %d tokens", limit, limit*2))
	var cle *llm.ContextLengthError
	if !errors.As(err, &cle) {
		t.Fatalf("status error was not classified as a context length error: %v", err)
	}
	return cle
}

type failingSummaryModel struct{}

func (failingSummaryModel) Name() string { return "failing" }

func (failingSummaryModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, errors.New("summary down"))
	}
}
