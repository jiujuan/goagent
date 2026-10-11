package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

type blockingCommitCoordinator struct {
	checkpoint.ThreadCoordinator
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCommitCoordinator) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return checkpoint.CommitReceipt{}, ctx.Err()
	}
	return s.ThreadCoordinator.Commit(ctx, req)
}

func TestSnapshotConsistencyCommitKeepsConcurrentToolUpdate(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	store := &blockingCommitCoordinator{
		ThreadCoordinator: base,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	begun, err := store.BeginRun(ctx, checkpoint.BeginRunRequest{
		ThreadID: "commit-isolation",
		Owner:    "run",
		Lease:    checkpoint.DefaultLease,
	})
	if err != nil {
		t.Fatal(err)
	}
	rc := &RunContext{
		Context:  ctx,
		ThreadID: "commit-isolation",
		Store:    store,
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	rc.setDurableClaim(&begun.Claim)
	old := checkpoint.ToolExecution{InvocationID: "same", Attempt: 1, State: checkpoint.ToolCompleted}
	newer := checkpoint.ToolExecution{InvocationID: "same", Attempt: 2, State: checkpoint.ToolCompleted}
	rc.addCompletedTool(old)

	done := make(chan error, 1)
	go func() {
		done <- rc.commitCheckpoint(&checkpoint.Checkpoint{
			ID:       "commit-1",
			ThreadID: "commit-isolation",
			State:    *rc.State,
		}, false)
	}()
	<-store.entered
	rc.addCompletedTool(newer)
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	rc.durable.mu.Lock()
	got, ok := rc.durable.executions["same"]
	rc.durable.mu.Unlock()
	if !ok {
		t.Fatal("concurrent tool update was cleared by an older commit")
	}
	if got.Attempt != newer.Attempt {
		t.Fatalf("retained attempt = %d, want %d", got.Attempt, newer.Attempt)
	}
}

type observingCoordinator struct {
	checkpoint.ThreadCoordinator
	commits chan struct{}
}

func (s *observingCoordinator) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	receipt, err := s.ThreadCoordinator.Commit(ctx, req)
	if err == nil {
		s.commits <- struct{}{}
	}
	return receipt, err
}

func TestSnapshotConsistencyNestedOwnership(t *testing.T) {
	tests := map[string]func(*testing.T, checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()){
		"sequential": snapshotConsistencySequential,
		"parallel":   snapshotConsistencyParallel,
		"loop":       snapshotConsistencyLoop,
		"plan":       snapshotConsistencyPlan,
		"as-tool":    snapshotConsistencyAsTool,
		"transfer":   snapshotConsistencyTransfer,
	}

	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			store := &observingCoordinator{
				ThreadCoordinator: checkpoint.NewMemory(),
				commits:           make(chan struct{}, 32),
			}
			root, entered, release := build(t, store)
			defer release()

			threadID := "nested-" + name
			rootDone := make(chan error, 1)
			go func() {
				_, err := root.Stream(context.Background(), "root", OnThread(threadID)).Wait()
				rootDone <- err
			}()

			waitSnapshotConsistencySignal(t, store.commits, "nested checkpoint commit")
			waitSnapshotConsistencySignal(t, entered, "later nested stage")

			var competitorCalls atomic.Int32
			competitor, err := New(
				WithCheckpointer(store),
				WithModel(mock.New("competitor", func(*llm.Request) *llm.Response {
					competitorCalls.Add(1)
					return mock.Text("competitor")
				})),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := competitor.Stream(context.Background(), "blocked", OnThread(threadID)).Wait(); !errors.Is(err, checkpoint.ErrClaimConflict) {
				t.Fatalf("overlapping nested run error = %v, want ErrClaimConflict", err)
			}
			if got := competitorCalls.Load(); got != 0 {
				t.Fatalf("competitor model calls while root owns thread = %d, want 0", got)
			}

			release()
			if err := waitSnapshotConsistencyError(t, rootDone, "root workflow"); err != nil {
				t.Fatal(err)
			}
			if _, err := competitor.Stream(context.Background(), "after", OnThread(threadID)).Wait(); err != nil {
				t.Fatalf("run after root release: %v", err)
			}
			if got := competitorCalls.Load(); got != 1 {
				t.Fatalf("competitor model calls after root release = %d, want 1", got)
			}
		})
	}
}

func snapshotConsistencySequential(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	root := Sequential("sequential", snapshotConsistencyTextAgent(t, "first"), snapshotConsistencyBlockingAgent(t, entered, release))
	root.store = store
	return root, entered, unblock
}

func snapshotConsistencyParallel(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	root := Parallel("parallel", snapshotConsistencyTextAgent(t, "fast"), snapshotConsistencyBlockingAgent(t, entered, release))
	root.store = store
	return root, entered, unblock
}

func snapshotConsistencyLoop(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	var calls atomic.Int32
	worker, err := New(WithModel(mock.New("loop-worker", func(*llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			return mock.Text("first iteration")
		}
		close(entered)
		<-release
		return mock.Text("second iteration")
	})))
	if err != nil {
		t.Fatal(err)
	}
	root := Loop("loop", 2, worker)
	root.store = store
	return root, entered, unblock
}

func snapshotConsistencyPlan(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	worker, err := New(WithModel(mock.New("plan-worker", func(req *llm.Request) *llm.Response {
		if req.Messages[len(req.Messages)-1].Text() == "second" {
			close(entered)
			<-release
		}
		return mock.Text(req.Messages[len(req.Messages)-1].Text())
	})))
	if err != nil {
		t.Fatal(err)
	}
	root := NewPlan(
		"plan",
		Plan{Nodes: []Node{
			{ID: "first", Task: "first"},
			{ID: "second", Task: "second", DependsOn: []string{"first"}},
		}},
		WithWorker(worker),
		WithPlanCheckpointer(store),
	)
	return root, entered, unblock
}

func snapshotConsistencyAsTool(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	child := snapshotConsistencyTextAgent(t, "child done")
	parent, err := New(
		WithCheckpointer(store),
		WithModel(mock.New("tool-parent", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				close(entered)
				<-release
				return mock.Text("parent done")
			}
			return mock.CallTool("delegate-1", "delegate", `{"task":"child work"}`)
		})),
		WithTools(AsTool(child, "delegate", "delegate child work")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return parent, entered, unblock
}

func snapshotConsistencyTransfer(t *testing.T, store checkpoint.ThreadCoordinator) (*Agent, <-chan struct{}, func()) {
	t.Helper()
	entered, release, unblock := snapshotConsistencyBlocker()
	expert, err := New(
		WithName("expert"),
		WithModel(mock.New("expert", func(*llm.Request) *llm.Response { return mock.Text("expert done") })),
	)
	if err != nil {
		t.Fatal(err)
	}
	router, err := New(
		WithName("router"),
		WithModel(mock.New("router", func(*llm.Request) *llm.Response {
			return mock.CallTool("transfer-1", "transfer_to_agent", `{"agent":"expert"}`)
		})),
		WithSubAgents(expert),
	)
	if err != nil {
		t.Fatal(err)
	}
	root := Sequential("transfer", router, snapshotConsistencyBlockingAgent(t, entered, release))
	root.store = store
	return root, entered, unblock
}

func snapshotConsistencyTextAgent(t *testing.T, text string) *Agent {
	t.Helper()
	a, err := New(WithModel(mock.New("text", func(*llm.Request) *llm.Response { return mock.Text(text) })))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func snapshotConsistencyBlockingAgent(t *testing.T, entered chan struct{}, release <-chan struct{}) *Agent {
	t.Helper()
	a, err := New(WithModel(mock.New("blocker", func(*llm.Request) *llm.Response {
		close(entered)
		<-release
		return mock.Text("unblocked")
	})))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func snapshotConsistencyBlocker() (chan struct{}, chan struct{}, func()) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	return entered, release, func() { once.Do(func() { close(release) }) }
}

func waitSnapshotConsistencySignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitSnapshotConsistencyError(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}
