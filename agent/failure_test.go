package agent_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// scriptModel is a model whose behaviour depends on which call it is on, so a
// test can fail the first attempt and answer the next. It records every request
// it receives, which is how the tests read the history the model was handed.
type scriptModel struct {
	mu   sync.Mutex
	reqs []*llm.Request
	pick func(call int, req *llm.Request) (*llm.Response, error)
}

func (m *scriptModel) Name() string { return "script" }

func (m *scriptModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		n := len(m.reqs)
		m.mu.Unlock()
		resp, err := m.pick(n, req)
		if err != nil {
			yield(nil, err)
			return
		}
		yield(resp, nil)
	}
}

func (m *scriptModel) history() [][]core.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]core.Message, len(m.reqs))
	for i, r := range m.reqs {
		out[i] = r.Messages
	}
	return out
}

func (m *scriptModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reqs)
}

var errProvider = errors.New("provider: 503 unavailable")

// digest renders a conversation compactly, including the parts Text() ignores:
// tool calls and tool results.
func digest(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(string(m.Role))
		b.WriteString("=")
		b.WriteString(m.Text())
		for _, p := range m.Parts {
			switch x := p.(type) {
			case core.ToolCall:
				b.WriteString("call(" + x.Name + ")")
			case core.ToolResult:
				b.WriteString(x.Name + "=")
				if len(x.Content) > 0 {
					b.WriteString(x.Content[0].(core.Text).Text)
				}
				if x.IsError {
					b.WriteString("!")
				}
			}
		}
		b.WriteString(";")
	}
	return b.String()
}

// brokenGate errors at BeforeTool for one step, standing in for a permission
// service that is unavailable: the assistant turn asked for tools, and the run
// dies before any of them ran.
type brokenGate struct {
	agent.BaseMiddleware
	failAt atomic.Int32
}

func (g *brokenGate) BeforeTool(lc *agent.LoopContext, _ *core.ToolCall) (core.Directive, error) {
	if g.failAt.Load() == int32(lc.Step) {
		return core.Directive{}, errors.New("policy service unavailable")
	}
	return core.Directive{}, nil
}

// TestModelFailureCheckpointsResumableSeam: a provider failure used to leave the
// thread's newest snapshot at whichever step last succeeded, so the run's input
// and any steering drained for the failed step were lost and Resume replayed from
// an older point.
func TestModelFailureCheckpointsResumableSeam(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	m := &scriptModel{}
	m.pick = func(call int, req *llm.Request) (*llm.Response, error) {
		if call == 1 {
			return nil, errProvider
		}
		return mock.Text("answered from " + string(rune('0'+len(req.Messages)))), nil
	}
	a, err := agent.New(agent.WithModel(m), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "weather?", agent.OnThread("t1"))
	run.Steer(core.UserText("use celsius"))
	_, err = run.Wait()
	if !errors.Is(err, errProvider) {
		t.Fatalf("wait err = %v, want the provider error", err)
	}

	cp, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil {
		t.Fatal("the failed run left no checkpoint to resume from")
	}
	if got := digest(cp.State.Messages); got != "user=weather?;user=use celsius;" {
		t.Fatalf("failure snapshot = %q, want the run input plus the drained steering", got)
	}

	cont, err := a.Resume(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "answered from 2" {
		t.Fatalf("resumed answer = %q", res.Message.Text())
	}
	hist := m.history()
	if got := digest(hist[len(hist)-1]); got != "user=weather?;user=use celsius;" {
		t.Fatalf("resumed model request = %q, want the failed seam replayed", got)
	}
}

// TestFailureAfterAnsweredToolsKeepsWholeTurn: when the batch closed cleanly, the
// seam is the whole conversation — dropping it would make the resumed run re-run
// tools that already succeeded.
func TestFailureAfterAnsweredToolsKeepsWholeTurn(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	calc := tool.New("calc", "adds", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return "7", nil
	})
	m := &scriptModel{}
	m.pick = func(call int, req *llm.Request) (*llm.Response, error) {
		switch call {
		case 1:
			return mock.CallTool("c1", "calc", `{}`), nil
		case 2:
			return nil, errProvider // the batch already has its result
		default:
			return mock.Text("final"), nil
		}
	}
	a, _ := agent.New(agent.WithModel(m), agent.WithTools(calc), agent.WithCheckpointer(store))

	run := a.Stream(ctx, "go", agent.OnThread("t1"))
	if _, err := run.Wait(); !errors.Is(err, errProvider) {
		t.Fatalf("wait err = %v", err)
	}
	cp, _ := store.Latest(ctx, "t1")
	if cp == nil {
		t.Fatal("the failed run left no snapshot to resume from")
	}
	last := cp.State.Messages[len(cp.State.Messages)-1]
	if last.Role != core.RoleTool {
		t.Fatalf("snapshot ends with %v, want the closed tool batch: %s", last.Role, digest(cp.State.Messages))
	}
	if len(cp.State.Messages) != 3 {
		t.Fatalf("snapshot = %s", digest(cp.State.Messages))
	}

	cont, err := a.Resume(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tool ran %d times, want 1: the resumed run must not redo an answered batch", got)
	}
}

// TestFailureWithUnansweredToolCallTrimsTurn: a run that dies between an
// assistant tool call and its results must not persist that call — replaying it
// hands the provider a tool call with no result.
func TestFailureWithUnansweredToolCallTrimsTurn(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	calc := tool.New("calc", "adds", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return "7", nil
	})
	gate := &brokenGate{}
	gate.failAt.Store(0) // the first batch is refused by an unavailable gate

	m := &scriptModel{}
	m.pick = func(call int, req *llm.Request) (*llm.Response, error) {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("final"), nil
		}
		return mock.CallTool("c1", "calc", `{}`), nil
	}
	a, _ := agent.New(
		agent.WithModel(m), agent.WithTools(calc),
		agent.WithMiddleware(gate), agent.WithCheckpointer(store),
	)

	run := a.Stream(ctx, "go", agent.OnThread("t1"))
	_, err := run.Wait()
	if err == nil || !strings.Contains(err.Error(), "policy service unavailable") {
		t.Fatalf("wait err = %v", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tool ran %d times before the gate failed", got)
	}
	cp, _ := store.Latest(ctx, "t1")
	if cp == nil {
		t.Fatal("the failed run left no snapshot to resume from")
	}
	if got := digest(cp.State.Messages); got != "user=go;" {
		t.Fatalf("snapshot = %q, want the unanswered assistant turn trimmed", got)
	}
	if cp.Pending != nil {
		t.Fatalf("snapshot carries pending approvals: %+v", cp.Pending)
	}

	// With the gate healthy, resuming re-asks the model and runs the tool once.
	gate.failAt.Store(-1)
	cont, err := a.Resume(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "final" || ran.Load() != 1 {
		t.Fatalf("resumed: %q, tool ran %d times", res.Message.Text(), ran.Load())
	}
}

// TestMaxTurnsFailureKeepsSeam: running out of budget is a failure with a
// resumable conversation, so the thread can be continued with a larger budget.
func TestMaxTurnsFailureKeepsSeam(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	calc := tool.New("calc", "adds", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return "7", nil
	})
	m := &scriptModel{}
	m.pick = func(call int, req *llm.Request) (*llm.Response, error) {
		return mock.CallTool("c"+string(rune('0'+call)), "calc", `{}`), nil
	}
	a, _ := agent.New(
		agent.WithModel(m), agent.WithTools(calc),
		agent.WithMaxTurns(2), agent.WithCheckpointer(store),
	)

	run := a.Stream(ctx, "go", agent.OnThread("t1"))
	if _, err := run.Wait(); !errors.Is(err, agent.ErrMaxTurnsExceeded) {
		t.Fatalf("wait err = %v, want ErrMaxTurnsExceeded", err)
	}
	cp, _ := store.Latest(ctx, "t1")
	if cp == nil {
		t.Fatal("the failed run left no snapshot to resume from")
	}
	want := "user=go;assistant=call(calc);tool=calc=7;assistant=call(calc);tool=calc=7;"
	if got := digest(cp.State.Messages); got != want {
		t.Fatalf("snapshot = %q, want both closed batches kept", got)
	}
	if cp.Step != 1 {
		t.Fatalf("snapshot step = %d, want the step the budget ran out on", cp.Step)
	}
	if ran.Load() != 2 {
		t.Fatalf("tool ran %d times, want 2", ran.Load())
	}

	m.pick = func(call int, req *llm.Request) (*llm.Response, error) {
		return mock.Text("wrapped up"), nil
	}
	cont, err := a.Resume(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "wrapped up" {
		t.Fatalf("resumed answer = %q", res.Message.Text())
	}
}
