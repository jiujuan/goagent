package agent_test

import (
	"context"
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
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// runEventCapture drains a Streamed run in the background and keeps every event
// under a lock, so a test can inspect the prefix published while handlers are
// still in flight.
type runEventCapture struct {
	mu     sync.Mutex
	events []core.Event
	done   chan error
}

func captureRunEvents(run *agent.Run) *runEventCapture {
	c := &runEventCapture{done: make(chan error, 1)}
	go func() {
		var err error
		for ev, iterErr := range run.Iter() {
			if ev != nil {
				c.mu.Lock()
				c.events = append(c.events, ev)
				c.mu.Unlock()
			}
			if iterErr != nil {
				err = iterErr
			}
		}
		c.done <- err
	}()
	return c
}

func (c *runEventCapture) snapshot() []core.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]core.Event(nil), c.events...)
}

func (c *runEventCapture) toolStartedNames() []string {
	var names []string
	for _, ev := range c.snapshot() {
		if ts, ok := ev.(core.ToolStarted); ok {
			names = append(names, ts.Call.Name)
		}
	}
	return names
}

// awaitToolStarts waits until at least n ToolStarted events were observed and
// returns the names published so far.
func (c *runEventCapture) awaitToolStarts(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(segmentWait)
	for {
		names := c.toolStartedNames()
		if len(names) >= n {
			return names
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d ToolStarted events arrived: %v", len(names), n, names)
		}
		time.Sleep(time.Millisecond)
	}
}

// await waits for the run to settle and returns the iterator's final error.
func (c *runEventCapture) await(t *testing.T) error {
	t.Helper()
	select {
	case err := <-c.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("run did not finish")
		return nil
	}
}

// gatedCalls builds tools that announce their entry and block until the test
// releases that call, so a segment's in-flight set can be observed.
type gatedCalls struct {
	act      segmentActivity
	started  chan string
	releases map[string]chan struct{}
	once     map[string]*sync.Once
}

func newGatedCalls(names ...string) *gatedCalls {
	g := &gatedCalls{
		started:  make(chan string, len(names)),
		releases: make(map[string]chan struct{}, len(names)),
		once:     make(map[string]*sync.Once, len(names)),
	}
	for _, n := range names {
		g.releases[n] = make(chan struct{})
		g.once[n] = &sync.Once{}
	}
	return g
}

func (g *gatedCalls) tool(name string) tool.Tool {
	return tool.New(name, "waits for the test to release it", func(*tool.Context, struct{}) (string, error) {
		g.act.enter()
		defer g.act.leave()
		g.started <- name
		<-g.releases[name]
		return name, nil
	})
}

func (g *gatedCalls) release(name string) {
	g.once[name].Do(func() { close(g.releases[name]) })
}

// TestToolConcurrencyCapsParallelSegment: with two slots, the first two calls
// start; a third neither enters a handler nor publishes ToolStarted until one
// of the first two is released.
func TestToolConcurrencyCapsParallelSegment(t *testing.T) {
	const thread = "tool-concurrency-cap"
	store := checkpoint.NewMemory()
	names := []string{"t1", "t2", "t3", "t4", "t5"}
	g := newGatedCalls(names...)
	t.Cleanup(func() {
		for _, n := range names {
			g.release(n)
		}
	})

	calls := make([]core.ToolCall, len(names))
	tools := make([]tool.Tool, len(names))
	for i, n := range names {
		calls[i] = segmentCall(fmt.Sprintf("c%d", i+1), n)
		tools[i] = g.tool(n)
	}

	a, err := agent.New(
		agent.WithModel(segmentCallModel(calls...)),
		agent.WithTools(tools...),
		agent.WithToolConcurrency(2),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go", agent.OnThread(thread))
	t.Cleanup(run.Cancel)
	capture := captureRunEvents(run)

	first := map[string]bool{}
	for len(first) < 2 {
		select {
		case name := <-g.started:
			first[name] = true
		case <-time.After(segmentWait):
			t.Fatalf("only %d of 2 initial handlers entered: %v", len(first), first)
		}
	}
	if !first["t1"] || !first["t2"] {
		t.Fatalf("initial handlers = %v, want t1 and t2", first)
	}
	if got := capture.awaitToolStarts(t, 2); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Fatalf("ToolStarted before a release = %v, want exactly [t1 t2]; ToolStarted must follow slot acquisition", got)
	}
	select {
	case name := <-g.started:
		t.Fatalf("call %q entered while both slots were held", name)
	case <-time.After(50 * time.Millisecond):
	}

	g.release("t1")
	select {
	case name := <-g.started:
		if name != "t3" {
			t.Fatalf("handler admitted after one release = %q, want t3", name)
		}
	case <-time.After(segmentWait):
		t.Fatal("t3 did not start after a slot was released")
	}
	if got := capture.awaitToolStarts(t, 3); !slices.Equal(got, []string{"t1", "t2", "t3"}) {
		t.Fatalf("ToolStarted order = %v, want [t1 t2 t3]", got)
	}

	for _, n := range names {
		g.release(n)
	}
	if err := capture.await(t); err != nil {
		t.Fatalf("run: %v", err)
	}
	if peak := g.act.peakValue(); peak != 2 {
		t.Fatalf("peak concurrency = %d, want exactly 2", peak)
	}
	assertToolResultOrder(t, store, thread, names...)
}

// TestToolConcurrencyOneSerializesParallelSegment: one slot makes the parallel
// segment execute one call at a time, and ToolStarted still follows call order.
func TestToolConcurrencyOneSerializesParallelSegment(t *testing.T) {
	const thread = "tool-concurrency-one"
	store := checkpoint.NewMemory()
	names := []string{"t1", "t2", "t3"}
	var act segmentActivity
	var mu sync.Mutex
	var order []string

	tools := make([]tool.Tool, len(names))
	calls := make([]core.ToolCall, len(names))
	for i, name := range names {
		name := name
		tools[i] = tool.New(name, "records its start order", func(*tool.Context, struct{}) (string, error) {
			act.enter()
			defer act.leave()
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			return name, nil
		})
		calls[i] = segmentCall(fmt.Sprintf("c%d", i+1), name)
	}

	a, err := agent.New(
		agent.WithModel(segmentCallModel(calls...)),
		agent.WithTools(tools...),
		agent.WithToolConcurrency(1),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go", agent.OnThread(thread))
	t.Cleanup(run.Cancel)
	capture := captureRunEvents(run)
	if err := capture.await(t); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := capture.toolStartedNames(); !slices.Equal(got, names) {
		t.Fatalf("ToolStarted order = %v, want %v", got, names)
	}
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	mu.Unlock()
	if !slices.Equal(gotOrder, names) {
		t.Fatalf("handler order = %v, want %v", gotOrder, names)
	}
	if peak := act.peakValue(); peak != 1 {
		t.Fatalf("peak concurrency = %d, want 1", peak)
	}
	assertToolResultOrder(t, store, thread, names...)
}

// TestToolConcurrencyZeroIsUnlimited: zero restores the old unbounded segment.
func TestToolConcurrencyZeroIsUnlimited(t *testing.T) {
	const thread = "tool-concurrency-zero"
	g := newGateGroup(3)
	t.Cleanup(g.open)

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "p3"),
		)),
		agent.WithTools(g.tool("p1"), g.tool("p2"), g.tool("p3")),
		agent.WithToolConcurrency(0),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		err := waitSegmentStarts(g.started, 3, "unlimited parallel segment")
		g.open()
		coordErr <- err
	}()

	if _, err := a.Run(context.Background(), "go", agent.OnThread(thread)); err != nil {
		t.Fatal(err)
	}
	awaitSegmentCoordinator(t, coordErr)
	if peak := g.act.peakValue(); peak != 3 {
		t.Fatalf("peak concurrency = %d, want 3 (0 means unlimited)", peak)
	}
}

// TestToolConcurrencySerialSegmentUnaffected: the cap applies to parallel
// segments only; a sequential call still owns its segment exclusively.
func TestToolConcurrencySerialSegmentUnaffected(t *testing.T) {
	const thread = "tool-concurrency-serial"
	store := checkpoint.NewMemory()
	before := newGateGroup(2)
	after := newGateGroup(2)
	t.Cleanup(before.open)
	t.Cleanup(after.open)

	var seqActive atomic.Int32
	seq := tool.AsSequential(tool.New("seq", "runs alone", func(*tool.Context, struct{}) (string, error) {
		seqActive.Store(int32(before.act.current() + after.act.current()))
		return "seq", nil
	}))

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "p1"),
			segmentCall("c2", "p2"),
			segmentCall("c3", "seq"),
			segmentCall("c4", "p3"),
			segmentCall("c5", "p4"),
		)),
		agent.WithTools(before.tool("p1"), before.tool("p2"), seq, after.tool("p3"), after.tool("p4")),
		agent.WithToolConcurrency(2),
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
	assertToolResultOrder(t, store, thread, "p1", "p2", "seq", "p3", "p4")
}

// TestToolConcurrencyReleasesAfterTimeout: a call the loop stops waiting on
// returns its slot immediately, so the next call in the segment still runs.
func TestToolConcurrencyReleasesAfterTimeout(t *testing.T) {
	const thread = "tool-concurrency-timeout"
	store := checkpoint.NewMemory()
	stuck := stuckTool{name: "stuck", delay: 2 * time.Second}
	fast := tool.New("fast", "returns immediately", func(*tool.Context, struct{}) (string, error) {
		return "fast", nil
	})

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "stuck"),
			segmentCall("c2", "fast"),
		)),
		agent.WithTools(stuck, fast),
		agent.WithToolConcurrency(1),
		agent.WithToolTimeout(50*time.Millisecond),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if _, err := a.Run(context.Background(), "go", agent.OnThread(thread)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("run took %s; the timed-out call did not release its slot", elapsed)
	}
	assertToolResultOrder(t, store, thread, "stuck", "fast")
}

// TestToolConcurrencyCanceledWaiter: canceling a run while calls wait for a
// slot ends the batch without publishing more ToolStarted events.
func TestToolConcurrencyCanceledWaiter(t *testing.T) {
	const thread = "tool-concurrency-cancel"
	entered := make(chan string, 3)
	mk := func(name string, block bool) tool.Tool {
		return tool.New(name, "signals handler entry", func(tctx *tool.Context, _ struct{}) (string, error) {
			entered <- name
			if block {
				<-tctx.Done()
				return "", tctx.Err()
			}
			return name, nil
		})
	}

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "t1"),
			segmentCall("c2", "t2"),
			segmentCall("c3", "t3"),
		)),
		agent.WithTools(mk("t1", true), mk("t2", false), mk("t3", false)),
		agent.WithToolConcurrency(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go", agent.OnThread(thread))
	t.Cleanup(run.Cancel)
	capture := captureRunEvents(run)

	select {
	case name := <-entered:
		if name != "t1" {
			t.Fatalf("first handler entered = %q, want t1", name)
		}
	case <-time.After(segmentWait):
		t.Fatal("t1 did not enter within the deadline")
	}

	run.Cancel()
	if err := capture.await(t); err == nil {
		t.Fatal("run ended without a cancellation error")
	}
	if got := capture.toolStartedNames(); !slices.Equal(got, []string{"t1"}) {
		t.Fatalf("ToolStarted = %v, want only [t1]; waiters must not be announced", got)
	}
	select {
	case name := <-entered:
		t.Fatalf("call %q entered its handler after the run was canceled", name)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestToolConcurrencyResultOrder: results stay in model call order even when a
// later call finishes before an earlier one.
func TestToolConcurrencyResultOrder(t *testing.T) {
	const thread = "tool-concurrency-result-order"
	store := checkpoint.NewMemory()
	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	var openOnce sync.Once
	openSlow := func() { openOnce.Do(func() { close(releaseSlow) }) }
	t.Cleanup(openSlow)

	slow := tool.New("slow", "waits for the test", func(*tool.Context, struct{}) (string, error) {
		slowStarted <- struct{}{}
		<-releaseSlow
		return "slow", nil
	})
	fast := tool.New("fast", "returns immediately", func(*tool.Context, struct{}) (string, error) {
		return "fast", nil
	})
	also := tool.New("also", "returns immediately", func(*tool.Context, struct{}) (string, error) {
		return "also", nil
	})

	a, err := agent.New(
		agent.WithModel(segmentCallModel(
			segmentCall("c1", "slow"),
			segmentCall("c2", "fast"),
			segmentCall("c3", "also"),
		)),
		agent.WithTools(slow, fast, also),
		agent.WithToolConcurrency(2),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go", agent.OnThread(thread))
	t.Cleanup(run.Cancel)
	capture := captureRunEvents(run)

	select {
	case <-slowStarted:
	case <-time.After(segmentWait):
		t.Fatal("slow did not start within the deadline")
	}
	if got := capture.awaitToolStarts(t, 3); !slices.Equal(got, []string{"slow", "fast", "also"}) {
		t.Fatalf("ToolStarted order = %v, want [slow fast also]", got)
	}
	openSlow()
	if err := capture.await(t); err != nil {
		t.Fatalf("run: %v", err)
	}
	assertToolResultOrder(t, store, thread, "slow", "fast", "also")
}

// TestToolConcurrencyRejectsNegative: a negative cap is a construction error,
// not a silent zero.
func TestToolConcurrencyRejectsNegative(t *testing.T) {
	_, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
		agent.WithToolConcurrency(-1),
	)
	if err == nil {
		t.Fatal("New accepted tool concurrency -1")
	}
	if !strings.Contains(err.Error(), "tool concurrency") {
		t.Fatalf("error = %v, want it to name tool concurrency", err)
	}
}
