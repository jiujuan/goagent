package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestWriteTodosUpdatesState(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "write_todos",
			`{"todos":[{"title":"step A","status":"in_progress"},{"title":"step B"}]}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(agent.WriteTodosTool()),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("t1")); err != nil {
		t.Fatal(err)
	}

	cp, _ := store.Latest(ctx, "t1")
	if cp == nil || len(cp.State.Todos) != 2 {
		t.Fatalf("expected 2 todos in state, got %+v", cp)
	}
	if cp.State.Todos[0].Title != "step A" || cp.State.Todos[0].Status != "in_progress" {
		t.Fatalf("todo 0 wrong: %+v", cp.State.Todos[0])
	}
	if cp.State.Todos[1].Status != "pending" { // defaulted
		t.Fatalf("todo 1 status = %q, want pending", cp.State.Todos[1].Status)
	}
}

func TestRenderTodos(t *testing.T) {
	out := agent.RenderTodos([]core.Todo{
		{Title: "a", Status: "completed"},
		{Title: "b", Status: "in_progress"},
		{Title: "c", Status: "pending"},
	})
	want := "[x] a\n[~] b\n[ ] c"
	if out != want {
		t.Fatalf("render = %q, want %q", out, want)
	}
}
