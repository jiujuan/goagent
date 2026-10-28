package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLazyRunTimingSeparatesCreatedAndStarted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		a, err := New(WithModel(mock.New("timing", func(*llm.Request) *llm.Response { calls.Add(1); return mock.Text("done") })))
		if err != nil {
			t.Fatal(err)
		}
		r := a.Stream(context.Background(), "hi")
		before := r.rc.timing.Snapshot()
		if before.CreatedAt.IsZero() || !before.StartRequestedAt.IsZero() || !before.SettledAt.IsZero() || calls.Load() != 0 {
			t.Fatalf("created=%+v calls=%d", before, calls.Load())
		}
		timer := time.NewTimer(time.Second)
		<-timer.C
		r.Wait()
		after := r.rc.timing.Snapshot()
		if after.StartRequestedAt.Sub(before.CreatedAt) != time.Second || after.SettledAt.IsZero() || calls.Load() != 1 {
			t.Fatalf("settled=%+v calls=%d", after, calls.Load())
		}
	})
}
func TestPreparedCancelHasNoStartTiming(t *testing.T) {
	a, err := New(WithModel(mock.New("timing", nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Stream(context.Background(), "hi")
	r.Cancel()
	snapshot := r.rc.timing.Snapshot()
	if snapshot.CreatedAt.IsZero() || snapshot.SettledAt.IsZero() || !snapshot.StartRequestedAt.IsZero() {
		t.Fatalf("timing=%+v", snapshot)
	}
}
func TestDerivedContextsShareTimingRecorder(t *testing.T) {
	a, err := New(WithModel(mock.New("timing", nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Stream(context.Background(), "hi")
	defer r.Cancel()
	deeper := r.rc.deeper()
	child, err := r.rc.subRun(core.UserText("child"), TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := r.rc.forBranch("branch", TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	if r.rc.timing == nil || deeper.timing != r.rc.timing || child.timing != r.rc.timing || branch.timing != r.rc.timing {
		t.Fatal("derived contexts do not share recorder")
	}
	other := a.Stream(context.Background(), "other")
	defer other.Cancel()
	if other.rc.timing == r.rc.timing {
		t.Fatal("independent run reused timing recorder")
	}
}
func TestTimingSettlementRecordedOnce(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())
	ctx, span := tp.Tracer("test").Start(context.Background(), "caller")
	a, err := New(WithModel(mock.New("timing", nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Stream(ctx, "hi")
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(r.Cancel)
	}
	wg.Wait()
	span.End()
	counts := map[string]int{}
	for _, ev := range sr.Ended()[0].Events() {
		counts[ev.Name]++
	}
	if counts["agent.run.created"] != 1 || counts["agent.run.settled"] != 1 || counts["agent.run.start_requested"] != 0 {
		t.Fatalf("events=%v", counts)
	}
}
func TestRunTimingKeepsModelSpanParent(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())
	ctx, root := tp.Tracer("test").Start(context.Background(), "caller")
	a, err := New(WithModel(mock.New("timing", func(*llm.Request) *llm.Response { return mock.Text("done") })))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Stream(ctx, "hi")
	r.Wait()
	root.End()
	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "caller" {
		t.Fatalf("lifecycle created extra spans=%d", len(spans))
	}
	events := spans[0].Events()
	if len(events) < 3 {
		t.Fatalf("lifecycle events=%d", len(events))
	}
}
