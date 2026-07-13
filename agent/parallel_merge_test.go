package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// TestParallelMergedKVReachesNextStage proves the deterministic branch merge:
// two parallel branches each write their final text to a distinct output key,
// and a following sequential stage reads BOTH via {{key}} templating — which is
// only possible if the merged branch KV was folded back into the shared State.
func TestParallelMergedKVReachesNextStage(t *testing.T) {
	branchA, err := agent.New(
		agent.WithName("a"),
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("ALPHA") })),
		agent.WithOutputKey("a_out"),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(
		agent.WithName("b"),
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("BETA") })),
		agent.WithOutputKey("b_out"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var sawBoth bool
	consumer, err := agent.New(
		agent.WithInstruction("combine {{a_out}} and {{b_out}}"),
		agent.WithModel(mock.New("c", func(req *llm.Request) *llm.Response {
			if strings.Contains(req.System, "ALPHA") && strings.Contains(req.System, "BETA") {
				sawBoth = true
			}
			return mock.Text("done")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	pipe := agent.NewPipeline("p").
		ThenParallel("fan", branchA, branchB).
		Then(consumer).
		Build()
	if _, err := pipe.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if !sawBoth {
		t.Fatal("consumer stage did not see both merged branch outputs in its prompt")
	}
}

// TestParallelConflictRejected proves that two branches writing the SAME output
// key to different values fail under the default RejectStateConflicts policy.
func TestParallelConflictRejected(t *testing.T) {
	mk := func(name, phrase string) *agent.Agent {
		a, err := agent.New(
			agent.WithName(name),
			agent.WithModel(mock.New(name, func(*llm.Request) *llm.Response { return mock.Text(phrase) })),
			agent.WithOutputKey("shared"), // both write the same key -> conflict
		)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	flow := agent.Parallel("fan", mk("a", "one"), mk("b", "two"))
	if _, err := flow.Run(context.Background(), "go"); err == nil {
		t.Fatal("expected conflict error when two branches write the same key differently")
	}
}

// TestParallelConflictPreferLater proves PreferLaterBranch resolves a same-key
// conflict to the last branch's value instead of erroring.
func TestParallelConflictPreferLater(t *testing.T) {
	mk := func(name, phrase string) *agent.Agent {
		a, err := agent.New(
			agent.WithName(name),
			agent.WithModel(mock.New(name, func(*llm.Request) *llm.Response { return mock.Text(phrase) })),
			agent.WithOutputKey("shared"),
		)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	var saw string
	consumer, err := agent.New(
		agent.WithInstruction("value is {{shared}}"),
		agent.WithModel(mock.New("c", func(req *llm.Request) *llm.Response {
			saw = req.System
			return mock.Text("done")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Sequential("seq",
		agent.ParallelWithOptions("fan", agent.ParallelOptions{StateConflict: agent.PreferLaterBranch},
			mk("a", "one"), mk("b", "two")),
		consumer,
	)
	if _, err := flow.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saw, "two") {
		t.Fatalf("prefer-later should keep last branch value; prompt = %q", saw)
	}
}
