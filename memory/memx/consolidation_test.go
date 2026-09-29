package memx

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/textmem"
)

// factModel is the consolidator's model: it records the transcript it was shown
// and answers with reply(call). An empty reply signals a model error.
type factModel struct {
	reply func(call int) string

	mu     sync.Mutex
	calls  int
	lastIn string
}

func (m *factModel) Name() string { return "fact-model" }

func (m *factModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.mu.Lock()
		m.calls++
		n := m.calls
		m.lastIn = req.Messages[0].Text()
		m.mu.Unlock()

		reply := m.reply(n)
		if reply == "" {
			yield(nil, errors.New("fact model is down"))
			return
		}
		yield(&llm.Response{Message: core.AssistantText(reply)}, nil)
	}
}

func (m *factModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *factModel) transcript() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIn
}

func newFactModel(json string) *factModel {
	return &factModel{reply: func(int) string { return json }}
}

func runContext(ctx context.Context, thread string, msgs ...core.Message) *agent.RunContext {
	return &agent.RunContext{Context: ctx, RunID: "r1", ThreadID: thread, State: &core.State{Messages: msgs}}
}

// finish is the run-end capability as the framework sees it.
func finish(mw agent.Middleware) agent.RunFinisher {
	f, ok := mw.(agent.RunFinisher)
	if !ok {
		panic("Consolidator must implement agent.RunFinisher")
	}
	return f
}

// Mounting the layer writes the run's facts into both stores, and a second
// finish of the same thread does not ask the model again.
func TestConsolidatorWritesAtRunEnd(t *testing.T) {
	ctx := context.Background()
	text, err := textmem.File(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sem := memory.InMemory(embmock.New())
	model := newFactModel(`[
		{"target":"text","name":"tone","desc":"语气","type":"user","content":"回复要简洁"},
		{"target":"semantic","type":"reference","content":"巴黎是法国首都"}
	]`)

	c := finish(Consolidator(ConsolidationConfig{Model: model, Text: text, Semantic: sem}))
	rc := runContext(ctx, "t1", core.UserText("请简洁点"), core.AssistantText("好的"))
	c.FinishRun(rc, core.Result{}, nil)

	if sem.Len() != 1 {
		t.Fatalf("semantic Len = %d, want 1", sem.Len())
	}
	if idx, _ := text.Index(ctx); len(idx) != 1 {
		t.Fatalf("text index = %+v, want 1 entry", idx)
	}

	c.FinishRun(rc, core.Result{}, nil) // same thread, no new messages
	if model.count() != 1 {
		t.Fatalf("model calls = %d, want the second finish skipped", model.count())
	}
}

// Only the messages since the last consolidation reach the model.
func TestConsolidatorIsIncremental(t *testing.T) {
	model := newFactModel(`[]`)
	c := finish(Consolidator(ConsolidationConfig{
		Model: model, Semantic: memory.InMemory(embmock.New()),
	}))

	rc := runContext(context.Background(), "t1", core.UserText("第一轮问题"))
	c.FinishRun(rc, core.Result{}, nil)
	rc.State.Messages = append(rc.State.Messages,
		core.AssistantText("第一轮回答"), core.UserText("第二轮问题"))
	c.FinishRun(rc, core.Result{}, nil)

	if model.count() != 2 {
		t.Fatalf("model calls = %d, want one per finished run", model.count())
	}
	got := model.transcript()
	if !strings.Contains(got, "第二轮问题") {
		t.Fatalf("latest transcript missed the new turn:\n%s", got)
	}
	if strings.Contains(got, "第一轮问题") {
		t.Fatalf("latest transcript replayed an already consolidated turn:\n%s", got)
	}
}

// A canceled run is abandoned: no extra model call.
func TestConsolidatorSkipsCanceledRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	model := newFactModel(`[]`)
	c := finish(Consolidator(ConsolidationConfig{
		Model: model, Semantic: memory.InMemory(embmock.New()),
	}))
	c.FinishRun(runContext(ctx, "t1", core.UserText("x")), core.Result{}, nil)

	if model.count() != 0 {
		t.Fatalf("model calls = %d, want 0 for a canceled run", model.count())
	}
}

// A long transcript is capped from the front, keeping the tail.
func TestConsolidatorCapsTranscript(t *testing.T) {
	model := newFactModel(`[]`)
	c := finish(Consolidator(ConsolidationConfig{
		Model: model, Semantic: memory.InMemory(embmock.New()), MaxTranscript: 200,
	}))

	var msgs []core.Message
	for range 200 {
		msgs = append(msgs, core.UserText(strings.Repeat("很长的记录内容", 20)))
	}
	msgs = append(msgs, core.UserText("最后一条记录"))
	c.FinishRun(runContext(context.Background(), "t1", msgs...), core.Result{}, nil)

	got := model.transcript()
	if n := len([]rune(got)); n > 260 {
		t.Fatalf("transcript not capped: %d runes", n)
	}
	if !strings.Contains(got, "最后一条记录") {
		t.Fatal("the cap must keep the tail, not the head")
	}
	if !strings.Contains(got, "更早的记录已按预算省略") {
		t.Fatal("the cap should be marked in the transcript")
	}
}

// A consolidation failure is logged, never escalated into the run.
func TestConsolidatorLogsFailure(t *testing.T) {
	var logged bytes.Buffer
	model := newFactModel("") // signals a model error
	c := finish(Consolidator(ConsolidationConfig{
		Model:    model,
		Semantic: memory.InMemory(embmock.New()),
		Log:      log.New(&logged, "", 0),
	}))
	c.FinishRun(runContext(context.Background(), "t1", core.UserText("x")), core.Result{}, nil)

	if !strings.Contains(logged.String(), "consolidation failed") {
		t.Fatalf("log = %q, want the failure reported", logged.String())
	}
}

// memx.New mounts the layer and fills its destinations from the other layers.
func TestNewMountsConsolidation(t *testing.T) {
	model := newFactModel(`[{"target":"text","name":"tone","desc":"语气","type":"user","content":"回复要简洁"}]`)
	m, err := New(Config{
		TextMemDir:    t.TempDir(),
		Semantic:      memory.InMemory(embmock.New()),
		RAG:           &RAGConfig{K: 2},
		Consolidation: &ConsolidationConfig{Model: model},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Middleware) != 2 {
		t.Fatalf("middleware = %d, want RAG plus consolidation", len(m.Middleware))
	}
	if _, err := New(Config{Consolidation: &ConsolidationConfig{}}); err == nil {
		t.Fatal("New should require Consolidation.Model")
	}
	// With no destination store at all, consolidation would spend a model call
	// on nowhere.
	if _, err := New(Config{Consolidation: &ConsolidationConfig{Model: model}}); err == nil {
		t.Fatal("New should require a text or semantic destination")
	}

	// End to end: one real run, and the mounted text store holds the fact.
	a, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
			return mock.Text("好的，以后简洁回答")
		})),
		agent.WithMiddleware(m.Middleware...),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "以后回复简洁点"); err != nil {
		t.Fatal(err)
	}
	if model.count() != 1 {
		t.Fatalf("model calls = %d, want exactly one per run", model.count())
	}
	idx, err := m.TextStore.Index(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[0].Name != "tone" {
		t.Fatalf("mounted text store = %+v, want the consolidated fact", idx)
	}
}

// Async consolidation lands after the run has already settled.
func TestConsolidatorAsync(t *testing.T) {
	sem := memory.InMemory(embmock.New())
	model := &factModel{reply: func(int) string {
		time.Sleep(20 * time.Millisecond)
		return `[{"target":"semantic","type":"reference","content":"巴黎是法国首都"}]`
	}}
	c := finish(Consolidator(ConsolidationConfig{Model: model, Semantic: sem, Async: true}))
	c.FinishRun(runContext(context.Background(), "t1", core.UserText("法国首都是哪")), core.Result{}, nil)

	deadline := time.Now().Add(2 * time.Second)
	for sem.Len() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sem.Len() != 1 {
		t.Fatal("async consolidation did not reach the store")
	}
}
