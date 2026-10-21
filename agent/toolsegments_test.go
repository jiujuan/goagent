package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// segmentWait bounds how long a coordinator waits for handlers that must start
// together. It only guards against the old whole-batch serialization or a
// deadlock; the assertions themselves are decided by channel coordination.
const segmentWait = 2 * time.Second

// segmentActivity counts handlers in flight and the peak since the run began.
type segmentActivity struct {
	mu     sync.Mutex
	active int
	peak   int
}

func (a *segmentActivity) enter() {
	a.mu.Lock()
	a.active++
	if a.active > a.peak {
		a.peak = a.active
	}
	a.mu.Unlock()
}

func (a *segmentActivity) leave() {
	a.mu.Lock()
	a.active--
	a.mu.Unlock()
}

func (a *segmentActivity) current() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active
}

func (a *segmentActivity) peakValue() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.peak
}

// gateGroup is a set of handlers that announce their start and block until the
// test opens the gate, so a parallel segment can be observed in flight.
type gateGroup struct {
	act     segmentActivity
	started chan string
	release chan struct{}
	once    sync.Once
}

func newGateGroup(n int) *gateGroup {
	return &gateGroup{
		started: make(chan string, n),
		release: make(chan struct{}),
	}
}

func (g *gateGroup) tool(name string) tool.Tool {
	return tool.New(name, "blocks until the test releases it", func(*tool.Context, struct{}) (string, error) {
		g.act.enter()
		defer g.act.leave()
		g.started <- name
		<-g.release
		return name, nil
	})
}

func (g *gateGroup) open() { g.once.Do(func() { close(g.release) }) }

func waitSegmentStarts(ch <-chan string, n int, what string) error {
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(segmentWait):
			return fmt.Errorf("%s: only %d of %d calls started within %s", what, i, n, segmentWait)
		}
	}
	return nil
}

func waitSegmentSignal(ch <-chan struct{}, what string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(segmentWait):
		return fmt.Errorf("%s did not arrive within %s", what, segmentWait)
	}
}

func awaitSegmentCoordinator(t *testing.T, errc <-chan error) {
	t.Helper()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test coordinator did not finish")
	}
}

func segmentCall(id, name string) core.ToolCall {
	return core.ToolCall{ID: id, Name: name, Args: json.RawMessage(`{}`)}
}

// segmentCallModel answers the first request with calls and every later
// request with plain text.
func segmentCallModel(calls ...core.ToolCall) *mock.Model {
	turn := 0
	return mock.New("segment-caller", func(req *llm.Request) *llm.Response {
		turn++
		if turn > 1 {
			return mock.Text("done")
		}
		parts := make([]core.Part, len(calls))
		for i, c := range calls {
			parts[i] = c
		}
		return &llm.Response{
			Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
			StopReason: llm.StopToolUse,
		}
	})
}

// latestToolResults finds the last tool-role message in the latest checkpoint
// and returns its results.
func latestToolResults(t *testing.T, store checkpoint.Checkpointer, thread string) []core.ToolResult {
	t.Helper()
	cp, err := store.Latest(context.Background(), thread)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil {
		t.Fatalf("checkpoint for thread %q is nil", thread)
	}
	var last *core.Message
	for i := len(cp.State.Messages) - 1; i >= 0; i-- {
		if cp.State.Messages[i].Role == core.RoleTool {
			last = &cp.State.Messages[i]
			break
		}
	}
	if last == nil {
		t.Fatalf("checkpoint has no tool message: %+v", cp.State.Messages)
	}
	results := make([]core.ToolResult, len(last.Parts))
	for i, p := range last.Parts {
		tr, ok := p.(core.ToolResult)
		if !ok {
			t.Fatalf("tool message part[%d] = %#v, want core.ToolResult", i, p)
		}
		results[i] = tr
	}
	return results
}

func toolResultNames(results []core.ToolResult) []string {
	names := make([]string, len(results))
	for i, tr := range results {
		names[i] = tr.Name
	}
	return names
}

// assertToolResultOrder asserts the model-visible results kept its call order.
func assertToolResultOrder(t *testing.T, store checkpoint.Checkpointer, thread string, want ...string) {
	t.Helper()
	results := latestToolResults(t, store, thread)
	if got := toolResultNames(results); !slices.Equal(got, want) {
		t.Fatalf("tool result order = %v, want %v", got, want)
	}
}

// TestSegmentedBatchRunsParallelCallsAroundSerial: consecutive plain calls
// before a sequential tool share one parallel segment, and the sequential call
// starts only after both settled.
func TestSegmentedBatchRunsParallelCallsAroundSerial(t *testing.T) {
	const thread = "segmented-around-serial"
	store := checkpoint.NewMemory()
	group := newGateGroup(2)
	t.Cleanup(group.open)

	var seqActive atomic.Int32
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqActive.Store(int32(group.act.current()))
		return "seq", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "seq"),
		)),
		agent.WithTools(group.tool("p1"), group.tool("p2"), seq),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		err := waitSegmentStarts(group.started, 2, "parallel calls before seq")
		group.open()
		coordErr <- err
	}()

	if _, err := a.Run(context.Background(), "go", agent.OnThread(thread)); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)

	if peak := group.act.peakValue(); peak != 2 {
		t.Fatalf("parallel segment peak = %d, want 2", peak)
	}
	if got := seqActive.Load(); got != 0 {
		t.Fatalf("seq saw %d handlers in flight, want 0", got)
	}
	assertToolResultOrder(t, store, thread, "p1", "p2", "seq")
}

// TestSegmentedBatchRunsGroupsOnBothSidesOfSerial: the calls before and after
// a sequential tool form separate parallel segments. The second segment starts
// only after the sequential call settled, and the two groups never overlap.
func TestSegmentedBatchRunsGroupsOnBothSidesOfSerial(t *testing.T) {
	const thread = "segmented-both-sides"
	store := checkpoint.NewMemory()
	before := newGateGroup(2)
	after := newGateGroup(2)
	t.Cleanup(before.open)
	t.Cleanup(after.open)

	var seqActive atomic.Int32
	var seqFinished atomic.Bool
	var afterSawUnfinished atomic.Bool

	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqActive.Store(int32(before.act.current() + after.act.current()))
		seqFinished.Store(true)
		return "seq", nil
	}))
	afterTool := func(name string) tool.Tool {
		return tool.New(name, "blocks until the test releases it", func(*tool.Context, struct{}) (string, error) {
			after.act.enter()
			defer after.act.leave()
			if !seqFinished.Load() {
				afterSawUnfinished.Store(true)
			}
			after.started <- name
			<-after.release
			return name, nil
		})
	}

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "seq"),
			segmentCall("c4", "p3"),
			segmentCall("c5", "p4"),
		)),
		agent.WithTools(before.tool("p1"), before.tool("p2"), seq, afterTool("p3"), afterTool("p4")),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		if err := waitSegmentStarts(before.started, 2, "parallel calls before seq"); err != nil {
			before.open()
			after.open()
			coordErr <- err
			return
		}
		before.open()
		if err := waitSegmentStarts(after.started, 2, "parallel calls after seq"); err != nil {
			after.open()
			coordErr <- err
			return
		}
		after.open()
		coordErr <- nil
	}()

	if _, err := a.Run(context.Background(), "go", agent.OnThread(thread)); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)

	if peak := before.act.peakValue(); peak != 2 {
		t.Fatalf("first parallel segment peak = %d, want 2", peak)
	}
	if peak := after.act.peakValue(); peak != 2 {
		t.Fatalf("second parallel segment peak = %d, want 2", peak)
	}
	if got := seqActive.Load(); got != 0 {
		t.Fatalf("seq saw %d handlers in flight, want 0", got)
	}
	if afterSawUnfinished.Load() {
		t.Fatal("a call after seq started before seq finished")
	}
	assertToolResultOrder(t, store, thread, "p1", "p2", "seq", "p3", "p4")
}

// segmentOrder records the order handlers started in.
type segmentOrder struct {
	mu    sync.Mutex
	names []string
}

func (o *segmentOrder) record(name string) {
	o.mu.Lock()
	o.names = append(o.names, name)
	o.mu.Unlock()
}

func (o *segmentOrder) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.names)
}

// orderedTool records its start, then stays busy briefly. The sleep only widens
// the window in which an overlapping start would be observed; the order
// assertion comes from the recorded slice.
func orderedTool(name string, o *overlap, order *segmentOrder) tool.Tool {
	return tool.New(name, "records handler start order", func(*tool.Context, struct{}) (string, error) {
		res, err := o.wrap(func() (string, error) {
			order.record(name)
			time.Sleep(40 * time.Millisecond)
			return name, nil
		})()
		return res, err
	})
}

func TestAdjacentSerialCallsStaySequential(t *testing.T) {
	o := &overlap{}
	order := &segmentOrder{}
	s1 := tool.AsSequential(orderedTool("s1", o, order))
	s2 := tool.AsSequential(orderedTool("s2", o, order))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(segmentCall("c1", "s1"), segmentCall("c2", "s2"))),
		agent.WithTools(s1, s2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if peak := o.peak.Load(); peak != 1 {
		t.Fatalf("peak concurrency = %d, want 1 (adjacent serial calls each own a segment)", peak)
	}
	if got := order.list(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("handler order = %v, want [s1 s2]", got)
	}
}

func TestGlobalSequentialModeStaysSerial(t *testing.T) {
	o := &overlap{}
	order := &segmentOrder{}
	p1 := orderedTool("p1", o, order)
	p2 := orderedTool("p2", o, order)

	a, err := agent.New(
		agent.WithModel(segmentCallModel(segmentCall("c1", "p1"), segmentCall("c2", "p2"))),
		agent.WithTools(p1, p2),
		agent.WithToolExecution(agent.ToolSequential),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if peak := o.peak.Load(); peak != 1 {
		t.Fatalf("peak concurrency = %d, want 1 (global ToolSequential)", peak)
	}
	if got := order.list(); !slices.Equal(got, []string{"p1", "p2"}) {
		t.Fatalf("handler order = %v, want [p1 p2]", got)
	}
}

// segmentSettled records tools whose AfterTool pass completed. AfterTool runs
// inside the call's settlement, so it is the observable point a later serial
// segment may rely on.
type segmentSettled struct {
	agent.BaseMiddleware
	mu   sync.Mutex
	seen map[string]bool
}

func (s *segmentSettled) AfterTool(_ *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	s.mu.Lock()
	s.seen[tr.Name] = true
	s.mu.Unlock()
	return core.Directive{}, nil
}

func (s *segmentSettled) has(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[name]
}

// TestSegmentResultsKeepModelOrder: a fast call finishing first does not
// reorder the model-visible history, and the sequential call starts only after
// both earlier calls settled.
func TestSegmentResultsKeepModelOrder(t *testing.T) {
	const thread = "segment-result-order"
	store := checkpoint.NewMemory()
	slowStarted := make(chan struct{}, 1)
	fastStarted := make(chan struct{}, 1)
	fastDone := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	openSlow := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
	t.Cleanup(openSlow)

	settled := &segmentSettled{seen: make(map[string]bool)}
	var seqSawUnsettled atomic.Bool

	slow := tool.New("slow", "waits for the fast call", func(*tool.Context, struct{}) (string, error) {
		slowStarted <- struct{}{}
		<-releaseSlow
		return "slow", nil
	})
	fast := tool.New("fast", "returns immediately", func(*tool.Context, struct{}) (string, error) {
		fastStarted <- struct{}{}
		fastDone <- struct{}{}
		return "fast", nil
	})
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		if !settled.has("slow") || !settled.has("fast") {
			seqSawUnsettled.Store(true)
		}
		return "seq", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "slow"),
			segmentCall("c2", "fast"),
			segmentCall("c3", "seq"),
		)),
		agent.WithTools(slow, fast, seq),
		agent.WithMiddleware(settled),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		if err := waitSegmentSignal(slowStarted, "slow start"); err != nil {
			openSlow()
			coordErr <- err
			return
		}
		if err := waitSegmentSignal(fastStarted, "fast start"); err != nil {
			openSlow()
			coordErr <- err
			return
		}
		if err := waitSegmentSignal(fastDone, "fast completion"); err != nil {
			openSlow()
			coordErr <- err
			return
		}
		openSlow()
		coordErr <- nil
	}()

	if _, err := a.Run(context.Background(), "go", agent.OnThread(thread)); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)

	if seqSawUnsettled.Load() {
		t.Fatal("seq started before both earlier calls settled")
	}
	assertToolResultOrder(t, store, thread, "slow", "fast", "seq")
}
