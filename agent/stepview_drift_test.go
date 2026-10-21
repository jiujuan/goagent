package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// These tests pin replay drift detection: a resume without a pending batch
// compares the first newly frozen step with the snapshot the resumed checkpoint
// carried. They live in package agent because the baseline is deliberately
// unexported and the derivation points (deeper/subRun/forBranch) are internal.

func driftTool(desc string) tool.Tool {
	return &viewTestTool{name: "lookup", desc: desc, schema: `{"type":"object","properties":{}}`}
}

func driftTextModel(calls *atomic.Int32, text string) llm.Model {
	return mock.New("drift-model", func(*llm.Request) *llm.Response {
		calls.Add(1)
		return mock.Text(text)
	})
}

func drainDriftRun(t *testing.T, run *Run) ([]core.Event, core.Result, error) {
	t.Helper()
	var events []core.Event
	for ev, _ := range run.Iter() {
		if ev != nil {
			events = append(events, ev)
		}
	}
	res, err := run.Wait()
	return events, res, err
}

func driftEvents(events []core.Event) []core.StepViewDrifted {
	var out []core.StepViewDrifted
	for _, ev := range events {
		if d, ok := ev.(core.StepViewDrifted); ok {
			out = append(out, d)
		}
	}
	return out
}

func eventTypes(events []core.Event) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, fmt.Sprintf("%T", ev))
	}
	return out
}

func saveDriftCheckpoint(t *testing.T, store *checkpoint.Memory, cp *checkpoint.Checkpoint) {
	t.Helper()
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
}

// seedDriftThread runs one completed turn with the given tool and instruction,
// leaving a no-pause checkpoint whose Snapshot becomes the resume baseline.
func seedDriftThread(t *testing.T, store *checkpoint.Memory, thread, instruction string, tools ...tool.Tool) *checkpoint.Checkpoint {
	t.Helper()
	a, err := New(
		WithModel(driftTextModel(new(atomic.Int32), "first done")),
		WithTools(tools...),
		WithCheckpointer(store),
		WithInstruction(instruction),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "hi", OnThread(thread)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	cp, err := store.Latest(context.Background(), thread)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.Snapshot == nil {
		t.Fatalf("seed checkpoint has no snapshot: %+v", cp)
	}
	return cp
}

func TestReplayDriftWarns(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-warn"
	baseline := seedDriftThread(t, store, thread, "same system", driftTool("v1"))

	var calls atomic.Int32
	a, err := New(
		WithModel(driftTextModel(&calls, "second done")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, res, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	if got := res.Message.Text(); got != "second done" {
		t.Fatalf("resumed answer = %q, want second done", got)
	}
	drifts := driftEvents(events)
	if len(drifts) != 1 {
		t.Fatalf("got %d drift events, want 1 (%v)", len(drifts), eventTypes(events))
	}
	got := drifts[0]
	if got.Step != 0 {
		t.Fatalf("drift step = %d, want 0", got.Step)
	}
	if got.BaselineStep != baseline.Snapshot.Step {
		t.Fatalf("baseline step = %d, want %d", got.BaselineStep, baseline.Snapshot.Step)
	}
	if got.ExpectedToolDigest != baseline.Snapshot.ToolDigest {
		t.Fatalf("expected tool digest = %q, want the baseline %q", got.ExpectedToolDigest, baseline.Snapshot.ToolDigest)
	}
	if got.ActualToolDigest == "" || got.ActualToolDigest == got.ExpectedToolDigest {
		t.Fatalf("tool digest did not change: expected %q, actual %q", got.ExpectedToolDigest, got.ActualToolDigest)
	}
	if got.ExpectedSettingsDigest != got.ActualSettingsDigest {
		t.Fatalf("settings digest changed unexpectedly: expected %q, actual %q", got.ExpectedSettingsDigest, got.ActualSettingsDigest)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1 after a warn-only drift", calls.Load())
	}
}

func TestReplayDriftStrictFailsBeforeModel(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-strict"
	baseline := seedDriftThread(t, store, thread, "same system", driftTool("v1"))

	var calls atomic.Int32
	strict, err := New(
		WithModel(driftTextModel(&calls, "never")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
		WithStepSnapshotPolicy(StepSnapshotStrict),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := strict.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	_, _, err = drainDriftRun(t, run)
	if !errors.Is(err, ErrStepViewDrift) {
		t.Fatalf("resumed run error = %v, want ErrStepViewDrift", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("model calls = %d, want 0 before the strict failure", calls.Load())
	}

	failed, err := store.Latest(ctx, thread)
	if err != nil {
		t.Fatal(err)
	}
	if failed == nil || failed.Snapshot == nil {
		t.Fatalf("strict failure left no step snapshot: %+v", failed)
	}
	wantV2 := toolDigest([]llm.ToolSchema{tool.SchemaOf(driftTool("v2"))})
	if failed.Snapshot.ToolDigest != wantV2 {
		t.Fatalf("failure snapshot tool digest = %q, want the current view %q", failed.Snapshot.ToolDigest, wantV2)
	}
	if failed.Snapshot.ToolDigest == baseline.Snapshot.ToolDigest {
		t.Fatalf("failure snapshot kept the old baseline digest %q", baseline.Snapshot.ToolDigest)
	}

	var nextCalls atomic.Int32
	next, err := New(
		WithModel(driftTextModel(&nextCalls, "third done")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err = next.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("second resume: %v", err)
	}
	events, res, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("second resumed run failed: %v", err)
	}
	if got := res.Message.Text(); got != "third done" {
		t.Fatalf("second resumed answer = %q, want third done", got)
	}
	if drifts := driftEvents(events); len(drifts) != 0 {
		t.Fatalf("drift reported against the new baseline: %+v", drifts)
	}
	if nextCalls.Load() != 1 {
		t.Fatalf("model calls after the new baseline = %d, want 1", nextCalls.Load())
	}
}

func TestReplayDriftOffIsSilent(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-off"
	seedDriftThread(t, store, thread, "same system", driftTool("v1"))

	var calls atomic.Int32
	a, err := New(
		WithModel(driftTextModel(&calls, "done")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
		WithStepSnapshotPolicy(StepSnapshotOff),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, _, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	if drifts := driftEvents(events); len(drifts) != 0 {
		t.Fatalf("off policy published %d drift events: %+v", len(drifts), drifts)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", calls.Load())
	}
}

func TestReplayDriftTracksSettings(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-settings"
	baseline := seedDriftThread(t, store, thread, "system v1", driftTool("v1"))

	var calls atomic.Int32
	a, err := New(
		WithModel(driftTextModel(&calls, "done")),
		WithTools(driftTool("v1")),
		WithCheckpointer(store),
		WithInstruction("system v2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, _, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	drifts := driftEvents(events)
	if len(drifts) != 1 {
		t.Fatalf("got %d drift events, want 1 (%v)", len(drifts), eventTypes(events))
	}
	got := drifts[0]
	if got.ExpectedSettingsDigest != baseline.Snapshot.SettingsDigest {
		t.Fatalf("expected settings digest = %q, want the baseline %q", got.ExpectedSettingsDigest, baseline.Snapshot.SettingsDigest)
	}
	if got.ActualSettingsDigest == "" || got.ActualSettingsDigest == got.ExpectedSettingsDigest {
		t.Fatalf("settings digest did not change: expected %q, actual %q", got.ExpectedSettingsDigest, got.ActualSettingsDigest)
	}
	if got.ExpectedToolDigest != got.ActualToolDigest {
		t.Fatalf("tool digest changed although the tool did: expected %q, actual %q", got.ExpectedToolDigest, got.ActualToolDigest)
	}
}

func TestReplayDriftOnlyOnce(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-once"
	seedDriftThread(t, store, thread, "same system", driftTool("v1"))

	var calls atomic.Int32
	twoSteps := mock.New("drift-model", func(req *llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			return mock.CallTool("call-1", "lookup", `{}`)
		}
		return mock.Text("done")
	})
	a, err := New(
		WithModel(twoSteps),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, res, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	if got := res.Message.Text(); got != "done" {
		t.Fatalf("resumed answer = %q, want done", got)
	}
	if calls.Load() != 2 {
		t.Fatalf("model calls = %d, want 2 (tool step then text step)", calls.Load())
	}
	drifts := driftEvents(events)
	if len(drifts) != 1 {
		t.Fatalf("got %d drift events, want exactly 1: %+v", len(drifts), drifts)
	}
	if drifts[0].Step != 0 {
		t.Fatalf("drift step = %d, want the first resumed step 0", drifts[0].Step)
	}
}

func TestReplayDriftLegacySnapshotSkipped(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-legacy"
	saveDriftCheckpoint(t, store, &checkpoint.Checkpoint{
		ID:       "legacy-head",
		ThreadID: thread,
		State: core.State{Messages: []core.Message{
			core.UserText("hi"),
			core.AssistantText("first done"),
		}},
	})

	var calls atomic.Int32
	a, err := New(
		WithModel(driftTextModel(&calls, "done")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, _, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	if drifts := driftEvents(events); len(drifts) != 0 {
		t.Fatalf("legacy checkpoint produced drift: %+v", drifts)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", calls.Load())
	}
}

func TestReplayDriftEmptyBaselineDigestSkipped(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	const thread = "drift-empty"
	saveDriftCheckpoint(t, store, &checkpoint.Checkpoint{
		ID:       "empty-head",
		ThreadID: thread,
		State: core.State{Messages: []core.Message{
			core.UserText("hi"),
			core.AssistantText("first done"),
		}},
		Snapshot: &checkpoint.StepSnapshot{Step: 0},
	})

	var calls atomic.Int32
	a, err := New(
		WithModel(driftTextModel(&calls, "done")),
		WithTools(driftTool("v2")),
		WithCheckpointer(store),
		WithInstruction("same system"),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, thread)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	events, _, err := drainDriftRun(t, run)
	if err != nil {
		t.Fatalf("resumed run failed: %v", err)
	}
	if drifts := driftEvents(events); len(drifts) != 0 {
		t.Fatalf("empty baseline digests produced drift: %+v", drifts)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", calls.Load())
	}
}

func TestUnknownStepSnapshotPolicyRejected(t *testing.T) {
	_, err := New(
		WithModel(driftTextModel(new(atomic.Int32), "done")),
		WithStepSnapshotPolicy(StepSnapshotPolicy(99)),
	)
	if err == nil {
		t.Fatal("New accepted an unknown step snapshot policy")
	}
	if !strings.Contains(err.Error(), "policy") {
		t.Fatalf("error = %q, want it to name the policy", err)
	}
}

func TestReplayBaselineNotInheritedBySubRun(t *testing.T) {
	base := &checkpoint.StepSnapshot{Step: 3, ToolDigest: "tool", SettingsDigest: "settings"}
	rc := &RunContext{
		Context:        context.Background(),
		State:          &core.State{},
		replayBaseline: base,
	}
	if rc.replayBaseline == nil {
		t.Fatal("test setup: parent has no baseline")
	}

	if child := rc.deeper(); child.replayBaseline != nil {
		t.Fatalf("deeper inherited the replay baseline: %+v", child.replayBaseline)
	}
	if child := rc.subRun(core.UserText("child")); child.replayBaseline != nil {
		t.Fatalf("subRun inherited the replay baseline: %+v", child.replayBaseline)
	}
	child, err := rc.forBranch("branch")
	if err != nil {
		t.Fatal(err)
	}
	if child.replayBaseline != nil {
		t.Fatalf("forBranch inherited the replay baseline: %+v", child.replayBaseline)
	}
	if rc.replayBaseline != base {
		t.Fatal("deriving children consumed the parent's baseline")
	}
}
