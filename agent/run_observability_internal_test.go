package agent

import (
	"context"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"iter"
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

func TestAdmissionWaitRecordedSeparately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, err := New(WithModel(mock.New("timing", func(*llm.Request) *llm.Response { return mock.Text("done") })))
		if err != nil {
			t.Fatal(err)
		}
		release, err := a.gate.acquire(context.Background(), "held", AdmissionFail, 1)
		if err != nil {
			t.Fatal(err)
		}
		r := a.Stream(context.Background(), "hi", OnThread("held"), WithAdmission(AdmissionOptions{Policy: AdmissionQueue, MaxWaiters: 1}))
		r.Start()
		synctest.Wait()
		before := r.rc.timing.Snapshot()
		if before.StartRequestedAt.IsZero() || !before.AdmittedAt.IsZero() {
			t.Fatalf("before admission=%+v", before)
		}
		timer := time.NewTimer(time.Second)
		<-timer.C
		release()
		r.Await(context.Background())
		after := r.rc.timing.Snapshot()
		if after.AdmittedAt.Sub(after.StartRequestedAt) != time.Second || after.FirstModelResponseAt.IsZero() {
			t.Fatalf("after admission=%+v", after)
		}
	})
}

type timingGatedModel struct{ entered, reply chan struct{} }

func (*timingGatedModel) Name() string { return "gated" }
func (m *timingGatedModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		close(m.entered)
		<-m.reply
		if !yield(nil, nil) {
			return
		}
		if !yield(&llm.Response{Partial: true, Message: core.AssistantText("part")}, nil) {
			return
		}
		yield(mock.Text("done"), nil)
	}
}
func TestFirstResponseOnlyAfterModelReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := &timingGatedModel{entered: make(chan struct{}), reply: make(chan struct{})}
		a, err := New(WithModel(model))
		if err != nil {
			t.Fatal(err)
		}
		r := a.Stream(context.Background(), "hi")
		t.Cleanup(func() {
			select {
			case <-model.reply:
			default:
				close(model.reply)
			}
			r.Cancel()
			r.Await(context.Background())
		})
		r.Start()
		<-model.entered
		before := r.rc.timing.Snapshot()
		if !before.FirstModelResponseAt.IsZero() || before.AdmittedAt.IsZero() {
			t.Fatalf("before=%+v", before)
		}
		timer := time.NewTimer(time.Second)
		<-timer.C
		close(model.reply)
		if _, err := r.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
		after := r.rc.timing.Snapshot()
		if after.FirstModelResponseAt.Sub(after.AdmittedAt) != time.Second {
			t.Fatalf("after=%+v", after)
		}
	})
}
func TestPreparedCancelHasNoAdmissionTiming(t *testing.T) {
	a, err := New(WithModel(mock.New("timing", nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Stream(context.Background(), "hi")
	r.Cancel()
	times := r.rc.timing.Snapshot()
	if !times.AdmittedAt.IsZero() || !times.FirstModelResponseAt.IsZero() {
		t.Fatalf("unexecuted timing=%+v", times)
	}
}

type timingResumeStore struct {
	checkpoint.Checkpointer
	entered, release chan struct{}
}

func (s *timingResumeStore) Latest(ctx context.Context, thread string) (*checkpoint.Checkpoint, error) {
	close(s.entered)
	<-s.release
	return s.Checkpointer.Latest(ctx, thread)
}
func TestResumeTimingRetainsPreacquiredAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		memory := checkpoint.NewMemory()
		memory.Save(context.Background(), &checkpoint.Checkpoint{ID: "pause", ThreadID: "resume-timing"})
		store := &timingResumeStore{Checkpointer: memory, entered: make(chan struct{}), release: make(chan struct{})}
		a := &Agent{bus: bus.New(), store: store, runnable: lifecycleRunnable(func(*RunContext) runOutcome { return runOutcome{} })}
		acquiredAt := time.Now()
		resumed := make(chan *Run, 1)
		go func() {
			r, err := a.Resume(context.Background(), "resume-timing")
			if err != nil {
				t.Error(err)
			}
			resumed <- r
		}()
		<-store.entered
		timer := time.NewTimer(time.Second)
		<-timer.C
		close(store.release)
		r := <-resumed
		if r == nil {
			t.Fatal("no resume handle")
		}
		before := r.rc.timing.Snapshot()
		if !before.AdmittedAt.Equal(acquiredAt) || before.CreatedAt.Sub(acquiredAt) != time.Second {
			t.Fatalf("resume=%+v", before)
		}
		r.Wait()
		after := r.rc.timing.Snapshot()
		if !after.AdmittedAt.Equal(acquiredAt) {
			t.Fatalf("start replaced preacquired admission=%+v", after)
		}
	})
}
func TestWorkflowFirstResponseRecordedOnce(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())
	ctx, root := tp.Tracer("test").Start(context.Background(), "caller")
	worker, err := New(WithModel(mock.New("timing", func(*llm.Request) *llm.Response { return mock.Text("done") })))
	if err != nil {
		t.Fatal(err)
	}
	workflow := Sequential("workflow", worker, worker)
	r := workflow.Stream(ctx, "hi")
	if _, err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	root.End()
	n := 0
	for _, ev := range sr.Ended()[0].Events() {
		if ev.Name == "agent.run.first_model_response" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("first responses=%d", n)
	}
}
