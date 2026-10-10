package agent_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// phase1PlanFailOnSaveStore injects failures only for planRunner.save. Worker,
// planner, and replanner sub-runs use the same store but do not include the
// plan state marker, so their own checkpoints cannot consume a plan boundary.
type phase1PlanFailOnSaveStore struct {
	base checkpoint.Checkpointer

	mu        sync.Mutex
	planSaves int
	fail      map[int]error
}

func (s *phase1PlanFailOnSaveStore) Save(ctx context.Context, cp *checkpoint.Checkpoint) error {
	if _, isPlanCheckpoint := cp.State.KV["__plan__"]; isPlanCheckpoint {
		s.mu.Lock()
		s.planSaves++
		err := s.fail[s.planSaves]
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return s.base.Save(ctx, cp)
}

func (s *phase1PlanFailOnSaveStore) Load(ctx context.Context, threadID, checkpointID string) (*checkpoint.Checkpoint, error) {
	return s.base.Load(ctx, threadID, checkpointID)
}

func (s *phase1PlanFailOnSaveStore) Latest(ctx context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	return s.base.Latest(ctx, threadID)
}

func (s *phase1PlanFailOnSaveStore) History(ctx context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	return s.base.History(ctx, threadID)
}

func (s *phase1PlanFailOnSaveStore) PlanSaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.planSaves
}

func newPhase1PlanFailOnSaveStore(fail map[int]error) *phase1PlanFailOnSaveStore {
	return &phase1PlanFailOnSaveStore{base: checkpoint.NewMemory(), fail: fail}
}

func phase1PlanWorker(t *testing.T, onTask func(string)) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithModel(mock.New("plan-worker", func(req *llm.Request) *llm.Response {
		task := ""
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == core.RoleUser {
				task = req.Messages[i].Text()
				break
			}
		}
		if onTask != nil {
			onTask(task)
		}
		return mock.Text(task)
	})))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPlanSaveFailureAtEveryBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("initial planning", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{1: saveErr})
		planner, err := agent.New(agent.WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
			return mock.Text(`{"nodes":[{"id":"planned","task":"planned"}]}`)
		})))
		if err != nil {
			t.Fatal(err)
		}
		var runs atomic.Int32
		pa := agent.NewLLMPlan(
			"initial-save-failure",
			planner,
			agent.WithWorker(phase1PlanWorker(t, func(string) { runs.Add(1) })),
			agent.WithPlanCheckpointer(store),
		)

		events, runErr := phase1Drain(t, pa.Stream(ctx, "go", agent.OnThread("plan_initial")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := runs.Load(); got != 0 {
			t.Fatalf("worker runs = %d, want 0 after initial plan save failure", got)
		}
	})

	t.Run("replanning", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{2: saveErr})
		replanner, err := agent.New(agent.WithModel(mock.New("replanner", func(*llm.Request) *llm.Response {
			return mock.Text(`{"nodes":[{"id":"extra","task":"extra"}]}`)
		})))
		if err != nil {
			t.Fatal(err)
		}
		var runs atomic.Int32
		pa := agent.NewPlan(
			"replan-save-failure",
			agent.Plan{Nodes: []agent.Node{{ID: "base", Task: "base"}}},
			agent.WithWorker(phase1PlanWorker(t, func(string) { runs.Add(1) })),
			agent.WithReplanner(replanner),
			agent.WithMaxReplanRounds(1),
			agent.WithPlanCheckpointer(store),
		)

		events, runErr := phase1Drain(t, pa.Stream(ctx, "go", agent.OnThread("plan_replan")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := runs.Load(); got != 1 {
			t.Fatalf("worker runs = %d, want only the completed base node", got)
		}
	})

	t.Run("node result", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{1: saveErr})
		var runs atomic.Int32
		pa := agent.NewPlan(
			"result-save-failure",
			agent.Plan{Nodes: []agent.Node{{ID: "node", Task: "node"}}},
			agent.WithWorker(phase1PlanWorker(t, func(string) { runs.Add(1) })),
			agent.WithPlanCheckpointer(store),
		)

		events, runErr := phase1Drain(t, pa.Stream(ctx, "go", agent.OnThread("plan_result")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := runs.Load(); got != 1 {
			t.Fatalf("worker runs = %d, want 1", got)
		}
	})

	t.Run("awaiting node approval", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{1: saveErr})
		var runs atomic.Int32
		pa := agent.NewPlan(
			"awaiting-save-failure",
			agent.Plan{Nodes: []agent.Node{{ID: "approval", Task: "approval", Approve: true}}},
			agent.WithWorker(phase1PlanWorker(t, func(string) { runs.Add(1) })),
			agent.WithPlanCheckpointer(store),
		)

		events, runErr := phase1Drain(t, pa.Stream(ctx, "go", agent.OnThread("plan_awaiting")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := runs.Load(); got != 0 {
			t.Fatalf("worker runs = %d, want 0 before approval persistence", got)
		}
	})

	t.Run("final approval", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{2: saveErr})
		var runs atomic.Int32
		pa := agent.NewPlan(
			"final-save-failure",
			agent.Plan{Nodes: []agent.Node{{ID: "node", Task: "node"}}},
			agent.WithWorker(phase1PlanWorker(t, func(string) { runs.Add(1) })),
			agent.WithFinalApproval(),
			agent.WithPlanCheckpointer(store),
		)

		events, runErr := phase1Drain(t, pa.Stream(ctx, "go", agent.OnThread("plan_final")))
		phase1AssertFailedTerminal(t, events, runErr, saveErr)
		if got := runs.Load(); got != 1 {
			t.Fatalf("worker runs = %d, want 1 before final approval", got)
		}
	})

	t.Run("waits for in flight workers", func(t *testing.T) {
		saveErr := errors.New("plan checkpoint unavailable")
		store := newPhase1PlanFailOnSaveStore(map[int]error{1: saveErr})
		slowStarted := make(chan struct{})
		releaseSlow := make(chan struct{})
		var started sync.Once
		worker := phase1PlanWorker(t, func(task string) {
			if task == "slow" {
				started.Do(func() { close(slowStarted) })
				<-releaseSlow
			}
		})
		pa := agent.NewPlan(
			"drain-inflight-save-failure",
			agent.Plan{Nodes: []agent.Node{{ID: "slow", Task: "slow"}, {ID: "fast", Task: "fast"}}},
			agent.WithWorker(worker),
			agent.WithConcurrency(2),
			agent.WithPlanCheckpointer(store),
		)
		run := pa.Stream(ctx, "go", agent.OnThread("plan_drain"))
		errCh := make(chan error, 1)
		go func() {
			_, err := run.Wait()
			errCh <- err
		}()

		select {
		case <-slowStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("slow worker did not start")
		}
		deadline := time.After(2 * time.Second)
		for store.PlanSaveCount() == 0 {
			select {
			case <-deadline:
				t.Fatal("completed worker did not reach the failed save boundary")
			case <-time.After(time.Millisecond):
			}
		}
		select {
		case err := <-errCh:
			t.Fatalf("plan returned before in-flight worker completed: %v", err)
		case <-time.After(30 * time.Millisecond):
		}

		close(releaseSlow)
		select {
		case err := <-errCh:
			if !errors.Is(err, saveErr) {
				t.Fatalf("run error = %v, want %v", err, saveErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("plan did not return after in-flight worker completed")
		}
	})
}
