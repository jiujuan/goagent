package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// TestSegmentedBatchFollowsInjectedSequentialImplementation: segmentation reads
// the sequential flag of the implementation frozen for the step, so a same-name
// injection that is sequential gets its own segment while the static tool it
// replaced stays parallel.
func TestSegmentedBatchFollowsInjectedSequentialImplementation(t *testing.T) {
	group := newGateGroup(2)
	t.Cleanup(group.open)

	var staticRan atomic.Bool
	staticSerial := tool.New("serial", "static parallel implementation", func(*tool.Context, struct{}) (string, error) {
		staticRan.Store(true)
		return "static", nil
	})
	var seqActive atomic.Int32
	injectedSerial := tool.AsSequential(tool.New("serial", "injected sequential implementation", func(*tool.Context, struct{}) (string, error) {
		seqActive.Store(int32(group.act.current()))
		return "injected", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "serial"),
		)),
		agent.WithTools(group.tool("p1"), group.tool("p2"), staticSerial),
		agent.WithMiddleware(&injector{make: func(lc *agent.LoopContext) tool.Tool {
			if lc.Step != 0 {
				return nil
			}
			return injectedSerial
		}}),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		err := waitSegmentStarts(group.started, 2, "plain calls")
		group.open()
		coordErr <- err
	}()

	run := a.Stream(context.Background(), "go")
	var events []string
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolStarted:
			events = append(events, "ToolStarted("+e.Call.Name+")")
		case core.ToolDone:
			events = append(events, "ToolDone("+e.Result.Name+")")
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)

	if peak := group.act.peakValue(); peak != 2 {
		t.Fatalf("peak concurrent tools = %d, want 2: the injected sequential implementation must only serialize its own call", peak)
	}
	if got := seqActive.Load(); got != 0 {
		t.Fatalf("injected serial tool saw %d handlers in flight, want 0", got)
	}
	if staticRan.Load() {
		t.Fatal("the static implementation ran; the injected one must replace it")
	}
	indexOf := func(want string) int {
		for i, ev := range events {
			if ev == want {
				return i
			}
		}
		t.Fatalf("%q missing from event sequence %v", want, events)
		return -1
	}
	if start := indexOf("ToolStarted(serial)"); start < indexOf("ToolDone(p1)") || start < indexOf("ToolDone(p2)") {
		t.Fatalf("serial call started before the parallel segment settled: %v", events)
	}
}

// TestCancelBetweenSegmentsStopsSerialCall: cancelling while a parallel segment
// runs stops the next segment before its serial handler starts.
func TestCancelBetweenSegmentsStopsSerialCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := newGateGroup(2)

	var entered atomic.Int32
	waitsForCancel := func(name string) tool.Tool {
		return tool.New(name, "waits for cancellation", func(tc *tool.Context, _ struct{}) (string, error) {
			entered.Add(1)
			group.started <- name
			<-tc.Done()
			return name, nil
		})
	}
	var seqCalls atomic.Int32
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqCalls.Add(1)
		return "seq", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "seq"),
		)),
		agent.WithTools(waitsForCancel("p1"), waitsForCancel("p2"), seq),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		err := waitSegmentStarts(group.started, 2, "parallel calls before seq")
		cancel()
		coordErr <- err
	}()

	_, runErr := a.Run(ctx, "go")
	awaitSegmentCoordinator(t, coordErr)

	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", runErr)
	}
	if got := entered.Load(); got != 2 {
		t.Fatalf("handlers entered = %d, want the 2 parallel calls", got)
	}
	if got := seqCalls.Load(); got != 0 {
		t.Fatalf("seq handler calls = %d, want 0 after cancellation", got)
	}
}

// segmentStateTool returns a fixed set of state operations alongside its result.
type segmentStateTool struct {
	name string
	ops  []core.StateOp
}

func (s segmentStateTool) Name() string        { return s.name }
func (s segmentStateTool) Description() string { return "returns state operations" }
func (s segmentStateTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (s segmentStateTool) Call(*tool.Context, json.RawMessage) (*tool.Result, error) {
	return &tool.Result{Content: []core.Part{core.Text{Text: s.name}}, State: s.ops}, nil
}

// TestStateOpsVisibleToLaterSerialSegment: state applied by an earlier call is
// visible to a later serial segment, because the segment boundary waits for the
// call to settle.
func TestStateOpsVisibleToLaterSerialSegment(t *testing.T) {
	var seen atomic.Value
	p1 := segmentStateTool{
		name: "p1",
		ops:  []core.StateOp{{Kind: core.OpSetKV, Key: "from_p1", Value: "yes"}},
	}
	seq := tool.AsSequential(tool.New("seq", "reads state", func(tc *tool.Context, _ struct{}) (string, error) {
		v, _ := tc.State.KV["from_p1"].(string)
		seen.Store(v)
		return v, nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(segmentCall("c1", "p1"), segmentCall("c2", "seq"))),
		agent.WithTools(p1, seq),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if got, _ := seen.Load().(string); got != "yes" {
		t.Fatalf("seq saw from_p1 = %q, want %q", got, "yes")
	}
}

type segmentRejectObs struct {
	callID  string
	class   agent.RejectClass
	seqDone bool
}

// segmentRejectSpy mirrors rejectSpy's contract probe: inHook is intentionally
// unlocked so -race exposes a concurrent dispatch. It also records whether the
// serial call had already settled when the callback ran.
type segmentRejectSpy struct {
	agent.BaseMiddleware

	seqDone    *atomic.Bool
	inHook     int
	overlapped bool

	mu       sync.Mutex
	observed []segmentRejectObs
}

func (s *segmentRejectSpy) OnToolReject(lc *agent.LoopContext, r agent.ToolRejection) {
	s.inHook++
	if s.inHook > 1 {
		s.overlapped = true
	}
	s.mu.Lock()
	s.observed = append(s.observed, segmentRejectObs{
		callID:  r.Call.ID,
		class:   r.Class,
		seqDone: s.seqDone.Load(),
	})
	s.mu.Unlock()
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: "reject_" + r.Call.ID, Value: r.Class.String()})
	time.Sleep(5 * time.Millisecond) // wide enough that interleaved dispatch would show
	s.inHook--
}

func (s *segmentRejectSpy) seen() []segmentRejectObs {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.observed)
}

// TestRejectionsSerialAfterSegmentedBatch: rejections inside a segmented batch
// are delivered once the whole batch settled, serially, and the guard's KV write
// lands in the same step's checkpoint.
func TestRejectionsSerialAfterSegmentedBatch(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var seqDone atomic.Bool
	spy := &segmentRejectSpy{seqDone: &seqDone}
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqDone.Store(true)
		return "seq", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "ghost1"),
			segmentCall("c3", "ghost2"),
			segmentCall("c4", "seq"),
		)),
		agent.WithTools(tool.New("p1", "answers at once", func(*tool.Context, struct{}) (string, error) {
			return "p1", nil
		}), seq),
		agent.WithMiddleware(spy),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "go", agent.OnThread("segmented-rejects")); err != nil {
		t.Fatal(err)
	}

	got := spy.seen()
	if len(got) != 2 {
		t.Fatalf("rejections = %d (%v), want 2", len(got), got)
	}
	if spy.overlapped {
		t.Fatal("OnToolReject ran concurrently for a segmented batch")
	}
	for _, obs := range got {
		if obs.class != agent.RejectUnknownTool {
			t.Fatalf("call %s class = %v, want unknown_tool", obs.callID, obs.class)
		}
		if !obs.seqDone {
			t.Fatalf("rejection for %s was dispatched before the serial call settled", obs.callID)
		}
	}

	history, err := store.History(ctx, "segmented-rejects")
	if err != nil {
		t.Fatal(err)
	}
	var seenRejection bool
	for _, cp := range history {
		if !hasToolResult(cp.State.Messages, "unknown tool: ghost1") {
			continue
		}
		if cp.Step != 0 {
			t.Fatalf("rejection first appears at step %d, want step 0", cp.Step)
		}
		for _, id := range []string{"c2", "c3"} {
			if cp.State.KV["reject_"+id] != agent.RejectUnknownTool.String() {
				t.Fatalf("checkpoint at step %d has rejection %s in history but KV = %v", cp.Step, id, cp.State.KV)
			}
		}
		seenRejection = true
		break
	}
	if !seenRejection {
		t.Fatal("no checkpoint carried the rejected calls' results")
	}
}

// TestResumedSegmentedBatchOnlyIncludesApprovedCalls: a resumed batch plans
// segments over the approved calls only, so an unapproved call neither runs nor
// splits the parallel segment around it.
func TestResumedSegmentedBatchOnlyIncludesApprovedCalls(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	mw := &gateOnce{}
	group := newGateGroup(2)
	t.Cleanup(group.open)

	var seqActive atomic.Int32
	var skipCalls atomic.Int32
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqActive.Store(int32(group.act.current()))
		return "seq", nil
	}))
	skip := tool.New("skip", "must not run when denied", func(*tool.Context, struct{}) (string, error) {
		skipCalls.Add(1)
		return "skip", nil
	})

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "skip"),
			segmentCall("c3", "p2"),
			segmentCall("c4", "seq"),
		)),
		agent.WithTools(group.tool("p1"), skip, group.tool("p2"), seq),
		agent.WithMiddleware(mw),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run, pending := pauseOn(t, a, ctx, "go")
	if len(pending) != 4 {
		t.Fatalf("pending calls = %d, want 4", len(pending))
	}
	for _, p := range pending {
		if p.Tool == "skip" {
			run.Decide(agent.Reject(p.CallID, "not needed"))
			continue
		}
		run.Decide(agent.Allow(p.CallID))
	}

	coordErr := make(chan error, 1)
	go func() {
		err := waitSegmentStarts(group.started, 2, "approved calls before seq")
		group.open()
		coordErr <- err
	}()
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range cont.Iter() {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)

	if peak := group.act.peakValue(); peak != 2 {
		t.Fatalf("resumed parallel segment peak = %d, want 2", peak)
	}
	if got := seqActive.Load(); got != 0 {
		t.Fatalf("seq saw %d handlers in flight, want 0", got)
	}
	if got := skipCalls.Load(); got != 0 {
		t.Fatalf("denied tool calls = %d, want 0", got)
	}

	results := latestToolResults(t, store, "t1")
	if len(results) != 4 {
		t.Fatalf("resumed tool results = %d, want 4", len(results))
	}
	if got := toolResultNames(results); !slices.Equal(got, []string{"p1", "skip", "p2", "seq"}) {
		t.Fatalf("result order = %v, want [p1 skip p2 seq]", got)
	}
	for _, tr := range results {
		if tr.Name != "skip" {
			continue
		}
		if !tr.IsError || len(tr.Content) == 0 || !strings.Contains(tr.Content[0].(core.Text).Text, "not needed") {
			t.Fatalf("denied result = %+v, want an error carrying the rejection reason", tr)
		}
	}
}

// TestSegmentEventOrder pins the observable boundary: ToolStarted follows the
// model's call order, a serial call's ToolDone precedes the next segment's
// ToolStarted, and TurnDone stays one per step rather than one per call.
func TestSegmentEventOrder(t *testing.T) {
	plain := func(name string) tool.Tool {
		return tool.New(name, "answers at once", func(*tool.Context, struct{}) (string, error) {
			return name, nil
		})
	}
	seq := tool.AsSequential(plain("seq"))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "seq"),
			segmentCall("c4", "p3"),
		)),
		agent.WithTools(plain("p1"), plain("p2"), seq, plain("p3")),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go")
	var started []string
	var events []string
	turnDone := 0
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolStarted:
			started = append(started, e.Call.Name)
			events = append(events, "ToolStarted("+e.Call.Name+")")
		case core.ToolDone:
			events = append(events, "ToolDone("+e.Result.Name+")")
		case core.TurnStarted:
			events = append(events, fmt.Sprintf("TurnStarted(%d)", e.Step))
		case core.TurnDone:
			turnDone++
			events = append(events, fmt.Sprintf("TurnDone(%d)", e.Step))
		default:
			events = append(events, fmt.Sprintf("%T", ev))
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("event sequence: %v", events)

	if !slices.Equal(started, []string{"p1", "p2", "seq", "p3"}) {
		t.Fatalf("ToolStarted order = %v, want [p1 p2 seq p3]", started)
	}
	indexOf := func(want string) int {
		for i, ev := range events {
			if ev == want {
				return i
			}
		}
		t.Fatalf("%q missing from event sequence %v", want, events)
		return -1
	}
	if indexOf("ToolDone(seq)") > indexOf("ToolStarted(p3)") {
		t.Fatalf("seq's ToolDone came after p3's ToolStarted: %v", events)
	}
	if turnDone != 2 {
		t.Fatalf("TurnDone events = %d, want 2 (one per step, not one per call)", turnDone)
	}
}
