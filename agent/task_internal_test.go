package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestTaskBudgetDefaults(t *testing.T) {
	model := mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })

	a, err := New(WithModel(model), WithTaskBudget(TaskBudget{}))
	if err != nil {
		t.Fatal(err)
	}
	want := TaskBudget{MaxDepth: 8, MaxUnits: 64}
	if got := a.cfg.taskBudget; got != want {
		t.Fatalf("normalized task budget = %+v, want %+v", got, want)
	}
	run := a.Stream(context.Background(), "hi")
	if got := run.rc.task.budget; got != want {
		t.Fatalf("root task runtime budget = %+v, want %+v", got, want)
	}
	run.Cancel()

	cases := map[string]TaskBudget{
		"max depth": {MaxDepth: -1},
		"max units": {MaxUnits: -1},
		"max turns": {MaxTurns: -1},
	}
	for name, budget := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(WithModel(model), WithTaskBudget(budget))
			if err == nil {
				t.Fatalf("New accepted a negative %s budget: %+v", name, budget)
			}
			if !strings.Contains(err.Error(), "task budget") {
				t.Fatalf("error = %v, want a task budget error", err)
			}
		})
	}
}
