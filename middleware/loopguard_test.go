package middleware_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// --- scenarios ---------------------------------------------------------------

// TestLoopGuardWarnThenInterrupt walks the full ladder: repeat the same call,
// get warned, get interrupted, resume twice and end force-stopped by the
// intervention budget.
func TestLoopGuardWarnThenInterrupt(t *testing.T) {
	var fetches int
	fetch := tool.New("fetch", "fetch a doc", func(_ *tool.Context, _ struct{}) (string, error) {
		fetches++
		return "ok", nil
	})
	var warned []bool
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		warned = append(warned, anyWarning(req.Messages))
		return mock.CallTool("c1", "fetch", "{}")
	})

	var detected []core.StuckDetected
	guard := middleware.LoopGuard(middleware.LoopGuardOptions{
		OnDetect: func(rule, reason string, step int) {
			if rule == "" || reason == "" {
				t.Errorf("empty rule/reason in callback: %q %q", rule, reason)
			}
		},
	})
	a := guardAgent(t, model, guard, fetch)

	run := a.Stream(context.Background(), "go")
	var interrupts int
	var pending []core.ApprovalRequest
	var stuck, done, failed bool
	for ev := range collect(run) {
		switch e := ev.(type) {
		case core.StuckDetected:
			stuck = true
			detected = append(detected, e)
		case core.Interrupted:
			interrupts++
			pending = e.Pending
		case core.RunDone:
			done = true
		case core.RunFailed:
			failed = true
		}
	}
	if interrupts != 1 || stuck == false || fetches != 2 {
		t.Fatalf("first wave: interrupts=%d stuck=%v fetches=%d, want 1/true/2", interrupts, stuck, fetches)
	}
	if !warned[2] {
		t.Fatal("the third model call should have seen the [loop-guard] warning")
	}
	if detected[0].Rule != "repeat_call" {
		t.Fatalf("first hit rule = %q, want repeat_call", detected[0].Rule)
	}

	// Resume once with approval. The guard warns again (a fresh warning must
	// always precede an escalation), then the intervention budget (3) is
	// exhausted and the next recurrence stops the run outright.
	run.Decide(agent.Allow(pending[0].CallID))
	run, err := run.Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	interrupts, done, failed = 0, false, false
	for ev := range collect(run) {
		switch ev.(type) {
		case core.Interrupted:
			interrupts++
		case core.RunDone:
			done = true
		case core.RunFailed:
			failed = true
		}
	}
	if interrupts != 0 || !done || failed {
		t.Fatalf("resumed wave: interrupts=%d done=%v failed=%v, want 0/true/false", interrupts, done, failed)
	}
	// fetches: twice in wave 1, once approved on resume, once after the second
	// warning; the stopping call never executes.
	if fetches != 4 {
		t.Fatalf("fetches = %d, want 4", fetches)
	}
}

// TestLoopGuardStopPolicy verifies OnRepeat=LoopGuardStop ends the run without
// any HITL pause.
func TestLoopGuardStopPolicy(t *testing.T) {
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) { return "ok", nil })
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "fetch", "{}")
	})
	guard := middleware.LoopGuard(middleware.LoopGuardOptions{OnRepeat: middleware.LoopGuardStop})
	a := guardAgent(t, model, guard, fetch)

	var interrupted, done, failed, stuck bool
	for ev := range collect(a.Stream(context.Background(), "go")) {
		switch ev.(type) {
		case core.Interrupted:
			interrupted = true
		case core.StuckDetected:
			stuck = true
		case core.RunDone:
			done = true
		case core.RunFailed:
			failed = true
		}
	}
	if interrupted || !stuck || !done || failed {
		t.Fatalf("stop policy: interrupted=%v stuck=%v done=%v failed=%v", interrupted, stuck, done, failed)
	}
}

// TestLoopGuardErrorStreak keeps R1 silent (args differ every step) and proves
// the identical-error streak rule fires and escalates after the warning.
func TestLoopGuardErrorStreak(t *testing.T) {
	type urlArgs struct {
		URL string `json:"url"`
	}
	var calls int
	boom := tool.New("boom", "always fails", func(_ *tool.Context, _ urlArgs) (string, error) {
		return "", errors.New("boom")
	})
	model := mock.New("m", func(*llm.Request) *llm.Response {
		calls++
		return mock.CallTool(fmt.Sprintf("c%d", calls), "boom", fmt.Sprintf(`{"url":"u%d"}`, calls))
	})
	var rules []string
	guard := middleware.LoopGuard(middleware.LoopGuardOptions{
		OnDetect: func(rule, _ string, _ int) { rules = append(rules, rule) },
	})
	a := guardAgent(t, model, guard, boom)

	var interrupted bool
	for ev := range collect(a.Stream(context.Background(), "go")) {
		if _, ok := ev.(core.Interrupted); ok {
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatal("error streak should have escalated to an interrupt")
	}
	// errors are identical in name+first line, so only error_streak hits fire
	for i, r := range rules {
		if r != "error_streak" {
			t.Fatalf("rules[%d] = %q, want error_streak (R1 must stay silent on varying args)", i, r)
		}
	}
}

// TestLoopGuardExemptTools shows the default write_todos exemption.
func TestLoopGuardExemptTools(t *testing.T) {
	var runs int
	todos := tool.New("write_todos", "planning tool", func(_ *tool.Context, _ struct{}) (string, error) {
		runs++
		return "saved", nil
	})
	var turn int
	model := mock.New("m", func(*llm.Request) *llm.Response {
		turn++
		if turn <= 5 {
			return mock.CallTool("c", "write_todos", "{}")
		}
		return mock.Text("finished")
	})
	a := guardAgent(t, model, middleware.LoopGuard(middleware.LoopGuardOptions{}), todos)

	var stuck bool
	res, err := "", error(nil)
	for ev := range collect(a.Stream(context.Background(), "go")) {
		switch e := ev.(type) {
		case core.StuckDetected:
			stuck = true
		case core.RunDone:
			res = e.Result.Message.Text()
		case core.RunFailed:
			err = e.Err
		}
	}
	if stuck || err != nil || res != "finished" || runs != 5 {
		t.Fatalf("exempt tool: stuck=%v err=%v res=%q runs=%d", stuck, err, res, runs)
	}
}

// TestLoopGuardTruncatedLoop covers the max_tokens truncation loop that never
// reaches the BeforeTool gate: the guard stops the run from AfterModel.
func TestLoopGuardTruncatedLoop(t *testing.T) {
	var runs int
	write := tool.New("write", "writes a file", func(_ *tool.Context, _ struct{}) (string, error) {
		runs++
		return "written", nil
	})
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "write", Args: json.RawMessage("{}")},
			}},
			StopReason: llm.StopMaxTokens,
		}
	})
	a := guardAgent(t, model, middleware.LoopGuard(middleware.LoopGuardOptions{}), write)

	var stuck, done, failed, interrupted bool
	var turns int
	for ev := range collect(a.Stream(context.Background(), "go")) {
		switch ev.(type) {
		case core.StuckDetected:
			stuck = true
		case core.TurnDone:
			turns++
		case core.Interrupted:
			interrupted = true
		case core.RunDone:
			done = true
		case core.RunFailed:
			failed = true
		}
	}
	if runs != 0 {
		t.Fatalf("truncated calls must never execute the tool, runs=%d", runs)
	}
	if !stuck || !done || failed || interrupted {
		t.Fatalf("truncated loop: stuck=%v done=%v failed=%v interrupted=%v", stuck, done, failed, interrupted)
	}
	if turns >= 16 {
		t.Fatalf("guard should have stopped the run well before maxTurns, turns=%d", turns)
	}
}

// TestLoopGuardNoFalsePositiveOnProgress runs distinct calls to a working tool
// and must never fire.
func TestLoopGuardNoFalsePositiveOnProgress(t *testing.T) {
	read := tool.New("read", "read a doc", func(_ *tool.Context, a struct{ N int }) (string, error) {
		return fmt.Sprint(a.N), nil
	})
	var turn int
	model := mock.New("m", func(*llm.Request) *llm.Response {
		turn++
		if turn <= 6 {
			return mock.CallTool(fmt.Sprintf("c%d", turn), "read", fmt.Sprintf(`{"N":%d}`, turn))
		}
		return mock.Text("all done")
	})
	a := guardAgent(t, model, middleware.LoopGuard(middleware.LoopGuardOptions{}), read)

	var stuck bool
	for ev := range collect(a.Stream(context.Background(), "go")) {
		if _, ok := ev.(core.StuckDetected); ok {
			stuck = true
		}
	}
	if stuck {
		t.Fatal("distinct calls should not trigger the guard")
	}
}

// TestLoopGuardAcrossProcessResume proves the guard's verdicts survive a JSONL
// checkpoint round-trip: run 1 interrupts, a fresh Agent (simulating a new
// process) resumes the thread, and the budget/marks keep working.
func TestLoopGuardAcrossProcessResume(t *testing.T) {
	dir := t.TempDir()
	var fetches int
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		fetches++
		return "ok", nil
	})
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "fetch", "{}")
	})
	guard := middleware.LoopGuard(middleware.LoopGuardOptions{})

	store1, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := agent.New(agent.WithModel(model), agent.WithTools(fetch), agent.WithMiddleware(guard), agent.WithCheckpointer(store1))
	if err != nil {
		t.Fatal(err)
	}
	run := a1.Stream(context.Background(), "go")
	var pending []core.ApprovalRequest
	var interrupts int
	for ev := range collect(run) {
		if e, ok := ev.(core.Interrupted); ok {
			interrupts++
			pending = e.Pending
		}
	}
	if interrupts != 1 || len(pending) == 0 {
		t.Fatalf("run 1 should interrupt once, got %d", interrupts)
	}
	thread := run.ThreadID

	// Fresh agent + fresh checkpointer over the same directory: the resumed
	// state goes through JSON (map[string]string → map[string]any, int →
	// float64), which the guard must tolerate.
	store2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := agent.New(agent.WithModel(model), agent.WithTools(fetch), agent.WithMiddleware(guard), agent.WithCheckpointer(store2))
	if err != nil {
		t.Fatal(err)
	}
	run2, err := a2.Resume(context.Background(), thread, agent.Allow(pending[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	var done, failed, interrupted2 bool
	for ev := range collect(run2) {
		switch ev.(type) {
		case core.RunDone:
			done = true
		case core.RunFailed:
			failed = true
		case core.Interrupted:
			interrupted2 = true
		}
	}
	if !done || failed || interrupted2 {
		t.Fatalf("resumed run: done=%v failed=%v interrupted=%v, fetches=%d", done, failed, interrupted2, fetches)
	}
}

// --- helpers -----------------------------------------------------------------

func guardAgent(t *testing.T, m llm.Model, guard agent.Middleware, tools ...tool.Tool) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithModel(m), agent.WithTools(tools...), agent.WithMiddleware(guard))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func anyWarning(msgs []core.Message) bool {
	for _, m := range msgs {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), middleware.WarnMarker) {
			return true
		}
	}
	return false
}
