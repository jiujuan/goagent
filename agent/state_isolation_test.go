package agent

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/vfs"
)

func TestSnapshotConsistencyAgentRestore(t *testing.T) {
	t.Run("deep copies a shared checkpoint", func(t *testing.T) {
		stored := &checkpoint.Checkpoint{
			ID: "source", ThreadID: "restore-thread",
			State: core.State{
				Messages: []core.Message{{
					Role: core.RoleUser,
					Parts: []core.Part{
						core.Image{MIME: "image/png", Data: []byte("image")},
						core.ToolCall{ID: "call1", Name: "lookup", Args: []byte(`{"key":"before"}`)},
					},
				}},
				Todos: []core.Todo{{ID: "todo1", Title: "before", Status: "pending"}},
				KV: map[string]any{
					"nested": map[string]any{"value": "before"},
					"bytes":  []byte("bytes"),
				},
			},
		}
		store := &sharedCheckpointStore{cp: stored}
		a, err := New(
			WithModel(mock.New("restore", func(*llm.Request) *llm.Response { return mock.Text("done") })),
			WithMiddleware(mutateRestoredState{}),
			WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := a.Run(context.Background(), "continue", OnThread("restore-thread")); err != nil {
			t.Fatal(err)
		}

		if got := stored.State.KV["nested"].(map[string]any)["value"]; got != "before" {
			t.Fatalf("stored nested KV = %v, want before", got)
		}
		if got := string(stored.State.KV["bytes"].([]byte)); got != "bytes" {
			t.Fatalf("stored KV bytes = %q, want bytes", got)
		}
		if got := string(stored.State.Messages[0].Parts[0].(core.Image).Data); got != "image" {
			t.Fatalf("stored image = %q, want image", got)
		}
		if got := string(stored.State.Messages[0].Parts[1].(core.ToolCall).Args); got != `{"key":"before"}` {
			t.Fatalf("stored tool args = %q", got)
		}
		if got := stored.State.Todos[0].Title; got != "before" {
			t.Fatalf("stored todo = %q, want before", got)
		}
		if got := len(stored.State.Messages); got != 1 {
			t.Fatalf("stored message count = %d, want 1", got)
		}
	})

	t.Run("propagates clone failures", func(t *testing.T) {
		stored := &checkpoint.Checkpoint{
			ID: "bad", ThreadID: "bad-state",
			State: core.State{KV: map[string]any{"bad": make(chan int)}},
		}
		store := &sharedCheckpointStore{cp: stored}
		var modelCalls atomic.Int32
		a, err := New(
			WithModel(mock.New("restore", func(*llm.Request) *llm.Response {
				modelCalls.Add(1)
				return mock.Text("unexpected")
			})),
			WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := a.Run(context.Background(), "continue", OnThread("bad-state")); !errors.Is(err, core.ErrStateNotCloneable) {
			t.Fatalf("Run error = %v, want ErrStateNotCloneable", err)
		}
		if got := modelCalls.Load(); got != 0 {
			t.Fatalf("model calls = %d, want 0", got)
		}
		if _, err := a.Resume(context.Background(), "bad-state"); !errors.Is(err, core.ErrStateNotCloneable) {
			t.Fatalf("Resume error = %v, want ErrStateNotCloneable", err)
		}
	})

	t.Run("file snapshot replaces a retained handle", func(t *testing.T) {
		retained := vfs.NewInState()
		if err := retained.Write("note.txt", []byte("retained")); err != nil {
			t.Fatal(err)
		}
		stored := &checkpoint.Checkpoint{
			ID: "files", ThreadID: "file-priority",
			State:        core.State{Files: retained},
			FileSnapshot: map[string][]byte{},
		}
		store := &sharedCheckpointStore{cp: stored}
		var observed string
		a, err := New(
			WithModel(mock.New("files", func(*llm.Request) *llm.Response { return mock.Text("done") })),
			WithMiddleware(observeRunFile{value: &observed}),
			WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := a.Run(context.Background(), "read", OnThread("file-priority")); err != nil {
			t.Fatal(err)
		}
		if observed != "missing" {
			t.Fatalf("restored file = %q, want explicit empty snapshot", observed)
		}

		override := vfs.NewInState()
		if err := override.Write("note.txt", []byte("override")); err != nil {
			t.Fatal(err)
		}
		observed = ""
		if _, err := a.Run(context.Background(), "read", OnThread("file-priority"), WithRunFiles(override)); err != nil {
			t.Fatal(err)
		}
		if observed != "override" {
			t.Fatalf("restored file with override = %q, want override", observed)
		}
	})
}

func TestSnapshotConsistencyAgentForkFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("from-parent")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, &checkpoint.Checkpoint{
		ID: "parent", ThreadID: "parent-thread", State: core.State{Files: files},
	}); err != nil {
		t.Fatal(err)
	}

	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := reopened.Latest(ctx, "parent-thread")
	if err != nil {
		t.Fatal(err)
	}
	child, err := checkpoint.Fork(parent, "child-thread", "child")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Save(ctx, child); err != nil {
		t.Fatal(err)
	}

	reader, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	readFile := tool.New("read_file", "read a file", func(tctx *tool.Context, _ struct{}) (string, error) {
		data, err := tctx.State.Files.Read("note.txt")
		if err != nil {
			return "", err
		}
		return string(data), nil
	})
	model := mock.New("reader", func(req *llm.Request) *llm.Response {
		if result, ok := mock.LastToolResult(req); ok {
			return mock.Text("read:" + result.Content[0].(core.Text).Text)
		}
		return mock.CallTool("read1", "read_file", `{}`)
	})
	a, err := New(WithModel(model), WithTools(readFile), WithCheckpointer(reader))
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Run(ctx, "read the branch file", OnThread("child-thread"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "read:from-parent") {
		t.Fatalf("branch run lost the forked file: %q", out)
	}
}

func TestSnapshotConsistencyParallelNestedState(t *testing.T) {
	t.Run("branches isolate nested state", func(t *testing.T) {
		mutated := make(chan struct{})
		release := make(chan struct{})
		observedLeak := make(chan bool, 1)

		branchA := &Agent{cfg: config{name: "a"}, runnable: runnableFunc(func(rc *RunContext) runOutcome {
			nested := rc.State.KV["nested"].(map[string]any)
			nested["branch"] = "a"
			close(mutated)
			<-release
			return runOutcome{Result: core.Result{Message: core.AssistantText("a")}}
		})}
		branchB := &Agent{cfg: config{name: "b"}, runnable: runnableFunc(func(rc *RunContext) runOutcome {
			<-mutated
			_, leaked := rc.State.KV["nested"].(map[string]any)["branch"]
			observedLeak <- leaked
			close(release)
			return runOutcome{Result: core.Result{Message: core.AssistantText("b")}}
		})}
		rc := testRunContext(&core.State{KV: map[string]any{
			"nested": map[string]any{"seed": "value"},
		}})

		out := (&parallelRunner{subs: []*Agent{branchA, branchB}}).run(rc)
		if out.Err != nil {
			t.Fatal(out.Err)
		}
		if <-observedLeak {
			t.Fatal("parallel branch observed a sibling's nested KV mutation")
		}
		if got := rc.State.KV["nested"].(map[string]any)["branch"]; got != "a" {
			t.Fatalf("merged nested branch value = %v, want a", got)
		}
	})

	t.Run("clone failure starts no branches", func(t *testing.T) {
		var starts atomic.Int32
		branch := func(name string) *Agent {
			return &Agent{cfg: config{name: name}, runnable: runnableFunc(func(*RunContext) runOutcome {
				starts.Add(1)
				return runOutcome{}
			})}
		}
		rc := testRunContext(&core.State{KV: map[string]any{"bad": make(chan int)}})

		out := (&parallelRunner{subs: []*Agent{branch("a"), branch("b")}}).run(rc)
		if !errors.Is(out.Err, core.ErrStateNotCloneable) {
			t.Fatalf("parallel error = %v, want ErrStateNotCloneable", out.Err)
		}
		if got := starts.Load(); got != 0 {
			t.Fatalf("started branches = %d, want 0", got)
		}
	})
}

func TestSnapshotConsistencySubagentFilesShared(t *testing.T) {
	files := vfs.NewInState()
	rc := testRunContext(&core.State{Files: files})
	sub, err := rc.subRun(core.UserText("task"), TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	deep := rc.deeper()

	if sub.State.Files != files || deep.State.Files != files {
		t.Fatal("subRun or deeper replaced the active file handle")
	}
	if err := sub.State.Files.Write("shared.txt", []byte("shared")); err != nil {
		t.Fatal(err)
	}
	data, err := deep.State.Files.Read("shared.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "shared" {
		t.Fatalf("shared file = %q, want shared", data)
	}
}

type sharedCheckpointStore struct {
	cp *checkpoint.Checkpoint
}

func (s *sharedCheckpointStore) Save(context.Context, *checkpoint.Checkpoint) error { return nil }

func (s *sharedCheckpointStore) Load(context.Context, string, string) (*checkpoint.Checkpoint, error) {
	return s.cp, nil
}

func (s *sharedCheckpointStore) Latest(context.Context, string) (*checkpoint.Checkpoint, error) {
	return s.cp, nil
}

func (s *sharedCheckpointStore) History(context.Context, string) ([]*checkpoint.Checkpoint, error) {
	return []*checkpoint.Checkpoint{s.cp}, nil
}

type mutateRestoredState struct{ BaseMiddleware }

func (mutateRestoredState) BeforeModel(lc *LoopContext) (core.Directive, error) {
	lc.State.KV["nested"].(map[string]any)["value"] = "run"
	lc.State.KV["bytes"].([]byte)[0] = 'X'
	image := lc.State.Messages[0].Parts[0].(core.Image)
	image.Data[0] = 'X'
	call := lc.State.Messages[0].Parts[1].(core.ToolCall)
	call.Args[0] = 'X'
	lc.State.Todos[0].Title = "run"
	return core.Directive{}, nil
}

type observeRunFile struct {
	BaseMiddleware
	value *string
}

func (m observeRunFile) BeforeModel(lc *LoopContext) (core.Directive, error) {
	data, err := lc.State.Files.Read("note.txt")
	if err != nil {
		*m.value = "missing"
	} else {
		*m.value = string(data)
	}
	return core.Directive{}, nil
}

type runnableFunc func(*RunContext) runOutcome

func (f runnableFunc) run(rc *RunContext) runOutcome { return f(rc) }

func testRunContext(state *core.State) *RunContext {
	return &RunContext{
		Context: context.Background(),
		RunID:   "run",
		Bus:     bus.New(),
		Topic:   "test",
		State:   state,
		durable: newDurableRunState(),
	}
}
