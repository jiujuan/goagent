package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// WriteTodosTool returns the write_todos planning tool — a context-engineering
// device borrowed from deep agents.
//
// IMPORTANT: write_todos is NOT an execution plan / engine. It executes nothing,
// manages no dependencies, and runs nothing in parallel. It is just a place for
// the model to record and revise its own plan; the todos live in State.Todos
// (captured by checkpoints) and keep a long-horizon agent focused. The MODEL
// reads its plan and decides the next step.
//
// For deterministic execution with dependencies, parallelism, approval and
// resume, compose workflow agents (Sequential/Parallel/Loop) or build a plan
// executor — see examples/planning for the comparison.
func WriteTodosTool() tool.Tool { return writeTodos{} }

type writeTodos struct{}

func (writeTodos) Name() string { return "write_todos" }

func (writeTodos) Description() string {
	return "Record or update your step-by-step plan as a todo list. Call it whenever the plan changes: pass the FULL list each time, marking each item pending / in_progress / completed."
}

func (writeTodos) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"todos":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"status":{"type":"string","enum":["pending","in_progress","completed"]}},"required":["title"]}}},"required":["todos"]}`)
}

func (writeTodos) Call(_ *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		Todos []struct {
			Title  string `json:"title"`
			Status string `json:"status"`
		} `json:"todos"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return tool.ErrorResult("invalid todos: " + err.Error()), nil
	}
	todos := make([]core.Todo, len(in.Todos))
	for i, t := range in.Todos {
		status := t.Status
		if status == "" {
			status = "pending"
		}
		todos[i] = core.Todo{ID: fmt.Sprintf("%d", i+1), Title: t.Title, Status: status}
	}
	// Replace the plan via a state op so it goes through the loop's state mutex
	// (race-safe even if write_todos runs alongside other tools in one turn).
	return &tool.Result{
		Content: []core.Part{core.Text{Text: "Updated plan:\n" + RenderTodos(todos)}},
		State:   []core.StateOp{{Kind: core.OpSetTodos, Value: todos}},
	}, nil
}

// RenderTodos formats a todo list as a checklist, for prompts or display.
func RenderTodos(todos []core.Todo) string {
	if len(todos) == 0 {
		return "(no todos)"
	}
	var b strings.Builder
	for _, t := range todos {
		box := "[ ]"
		switch t.Status {
		case "completed":
			box = "[x]"
		case "in_progress":
			box = "[~]"
		}
		fmt.Fprintf(&b, "%s %s\n", box, t.Title)
	}
	return strings.TrimRight(b.String(), "\n")
}
