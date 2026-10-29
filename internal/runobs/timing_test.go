package runobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/internal/runobs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	creationToStart          = "agent.run.creation_to_start"
	startToAdmission         = "agent.run.start_to_admission"
	admissionToFirstResponse = "agent.run.admission_to_first_response"
	startToSettlement        = "agent.run.start_to_settlement"
)

// These tests replace global providers and must not use t.Parallel.
type telemetry struct {
	ctx    context.Context
	span   trace.Span
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
	traces *sdktrace.TracerProvider
}

func newTelemetry(t *testing.T, options ...sdkmetric.Option) telemetry {
	t.Helper()
	previousMeter := otel.GetMeterProvider()
	previousTracer := otel.GetTracerProvider()
	spans := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	reader := sdkmetric.NewManualReader()
	meters := sdkmetric.NewMeterProvider(append([]sdkmetric.Option{sdkmetric.WithReader(reader)}, options...)...)
	otel.SetMeterProvider(meters)
	otel.SetTracerProvider(traces)
	ctx, span := traces.Tracer("runobs.test").Start(context.Background(), "caller")
	t.Cleanup(func() {
		span.End()
		otel.SetMeterProvider(previousMeter)
		otel.SetTracerProvider(previousTracer)
		if err := meters.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meters: %v", err)
		}
		if err := traces.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown traces: %v", err)
		}
	})
	return telemetry{ctx: ctx, span: span, spans: spans, reader: reader, traces: traces}
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	metrics := make(map[string]metricdata.Metrics)
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if _, exists := metrics[m.Name]; exists {
				t.Fatalf("duplicate metric %q", m.Name)
			}
			metrics[m.Name] = m
		}
	}
	return metrics
}

func assertMetrics(t *testing.T, reader *sdkmetric.ManualReader, want map[string]float64) {
	t.Helper()
	metrics := collectMetrics(t, reader)
	if len(metrics) != len(want) {
		t.Errorf("metrics = %v, want %v", metrics, want)
	}
	for name, duration := range want {
		m, ok := metrics[name]
		if !ok {
			t.Errorf("missing metric %q", name)
			continue
		}
		if m.Unit != "s" {
			t.Errorf("%s unit = %q, want s", name, m.Unit)
		}
		h, ok := m.Data.(metricdata.Histogram[float64])
		if !ok || len(h.DataPoints) != 1 {
			t.Errorf("%s data = %#v, want one float64 histogram point", name, m.Data)
			continue
		}
		p := h.DataPoints[0]
		if p.Count != 1 || p.Sum != duration || p.Sum < 0 {
			t.Errorf("%s count/sum = %d/%v, want 1/%v", name, p.Count, p.Sum, duration)
		}
		if p.Attributes.Len() != 0 {
			t.Errorf("%s metric attributes = %v, want none", name, p.Attributes)
		}
	}
}

func durations(s runobs.Snapshot, names ...string) map[string]float64 {
	all := map[string]float64{
		creationToStart:          s.StartRequestedAt.Sub(s.CreatedAt).Seconds(),
		startToAdmission:         s.AdmittedAt.Sub(s.StartRequestedAt).Seconds(),
		admissionToFirstResponse: s.FirstModelResponseAt.Sub(s.AdmittedAt).Seconds(),
		startToSettlement:        s.SettledAt.Sub(s.StartRequestedAt).Seconds(),
	}
	want := make(map[string]float64, len(names))
	for _, name := range names {
		want[name] = all[name]
	}
	return want
}

func assertEvents(t *testing.T, events []sdktrace.Event, s runobs.Snapshot, phases ...string) {
	t.Helper()
	observed := map[string]time.Time{
		"created":              s.CreatedAt,
		"start_requested":      s.StartRequestedAt,
		"admitted":             s.AdmittedAt,
		"first_model_response": s.FirstModelResponseAt,
		"settled":              s.SettledAt,
	}
	want := make(map[string]bool, len(phases))
	for _, phase := range phases {
		want["agent.run."+phase] = true
	}
	if len(events) != len(phases) {
		t.Errorf("events = %v, want phases %v", events, phases)
	}
	for _, event := range events {
		if !want[event.Name] {
			t.Errorf("unexpected or repeated event %q", event.Name)
		}
		delete(want, event.Name)
		phase := event.Name[len("agent.run."):]
		at := observed[phase]
		if at.IsZero() || !event.Time.Equal(at) {
			t.Errorf("%s time = %v, want observed %v", event.Name, event.Time, at)
		}
		attrs := attribute.NewSet(event.Attributes...)
		value, ok := attrs.Value(attribute.Key(phase + "_at"))
		if !ok || value.AsString() != at.Format(time.RFC3339Nano) {
			t.Errorf("%s timestamp attribute = %v, want %s", event.Name, value, at.Format(time.RFC3339Nano))
		}
	}
	if len(want) != 0 {
		t.Errorf("missing events %v", want)
	}
}

func TestRecorderRecordsOnlyObservedPhases(t *testing.T) {
	cases := []struct {
		name    string
		record  func(*runobs.Recorder)
		phases  []string
		metrics []string
	}{
		{name: "created", record: func(*runobs.Recorder) {}, phases: []string{"created"}},
		{name: "start_only", record: func(r *runobs.Recorder) { r.StartRequested() },
			phases: []string{"created", "start_requested"}, metrics: []string{creationToStart}},
		{name: "settled_without_start", record: func(r *runobs.Recorder) { r.Settled() },
			phases: []string{"created", "settled"}},
		{name: "first_response_without_admission", record: func(r *runobs.Recorder) { r.StartRequested(); r.FirstModelResponse(); r.Settled() },
			phases: []string{"created", "start_requested", "first_model_response", "settled"}, metrics: []string{creationToStart, startToSettlement}},
		{name: "admitted_without_start", record: func(r *runobs.Recorder) { r.Admitted(time.Now()); r.FirstModelResponse() },
			phases: []string{"created", "admitted", "first_model_response"}, metrics: []string{admissionToFirstResponse}},
		{name: "zero_admission_is_absent", record: func(r *runobs.Recorder) { r.Admitted(time.Time{}) }, phases: []string{"created"}},
		{name: "response_before_admission_time", record: func(r *runobs.Recorder) {
			r.StartRequested()
			r.FirstModelResponse()
			r.Admitted(time.Now().Add(time.Hour))
			r.Settled()
		},
			phases: []string{"created", "start_requested", "first_model_response", "admitted", "settled"}, metrics: []string{creationToStart, startToAdmission, startToSettlement}},
		{name: "late_admission_call_with_earlier_time", record: func(r *runobs.Recorder) {
			r.StartRequested()
			at := time.Now()
			r.FirstModelResponse()
			r.Settled()
			r.Admitted(at)
		},
			phases: []string{"created", "start_requested", "first_model_response", "settled", "admitted"}, metrics: []string{creationToStart, startToAdmission, admissionToFirstResponse, startToSettlement}},
		{name: "full", record: func(r *runobs.Recorder) {
			r.StartRequested()
			r.Admitted(time.Now())
			r.FirstModelResponse()
			r.Settled()
		},
			phases: []string{"created", "start_requested", "admitted", "first_model_response", "settled"}, metrics: []string{creationToStart, startToAdmission, admissionToFirstResponse, startToSettlement}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTelemetry(t)
			before := time.Now()
			r := runobs.New(env.ctx, "run-1", "thread-1")
			after := time.Now()
			s := r.Snapshot()
			if s.CreatedAt.IsZero() || s.CreatedAt.Before(before) || s.CreatedAt.After(after) {
				t.Fatalf("CreatedAt = %v, want within New call [%v, %v]", s.CreatedAt, before, after)
			}
			tc.record(r)
			s = r.Snapshot()
			assertEvents(t, env.spans.Started()[0].Events(), s, tc.phases...)
			assertMetrics(t, env.reader, durations(s, tc.metrics...))
			wantSnapshot := s
			s.CreatedAt = time.Time{}
			s.StartRequestedAt = time.Time{}
			s.AdmittedAt = time.Time{}
			s.FirstModelResponseAt = time.Time{}
			s.SettledAt = time.Time{}
			if got := r.Snapshot(); got != wantSnapshot {
				t.Errorf("mutating snapshot changed recorder: got %+v, want %+v", got, wantSnapshot)
			}
		})
	}
}

func TestRecorderIsIdempotentConcurrent(t *testing.T) {
	env := newTelemetry(t)
	r := runobs.New(env.ctx, "run-1", "thread-1")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			<-start
			r.StartRequested()
			_ = r.Snapshot()
		})
	}
	close(start)
	wg.Wait()
	admitted := time.Now()
	begin := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			<-begin
			for j := 0; j < 16; j++ {
				r.StartRequested()
				r.Admitted(admitted)
				r.FirstModelResponse()
				r.Settled()
				_ = r.Snapshot()
			}
		})
	}
	close(begin)
	wg.Wait()
	s := r.Snapshot()
	r.StartRequested()
	r.Admitted(admitted.Add(time.Hour))
	r.FirstModelResponse()
	r.Settled()
	if got := r.Snapshot(); got != s {
		t.Errorf("repeated phases changed snapshot: got %+v, want %+v", got, s)
	}
	assertEvents(t, env.spans.Started()[0].Events(), s, "created", "start_requested", "admitted", "first_model_response", "settled")
	assertMetrics(t, env.reader, durations(s, creationToStart, startToAdmission, admissionToFirstResponse, startToSettlement))
}

func TestRecorderKeepsExistingSpanTree(t *testing.T) {
	env := newTelemetry(t)
	ctx, run := env.traces.Tracer("runobs.test").Start(env.ctx, "agent.run")
	r := runobs.New(ctx, "run-1", "thread-1")
	r.StartRequested()
	_, model := env.traces.Tracer("runobs.test").Start(ctx, "chat mock-model")
	model.End()
	_, tool := env.traces.Tracer("runobs.test").Start(ctx, "tool.test")
	tool.End()
	r.Admitted(time.Now())
	r.FirstModelResponse()
	r.Settled()
	if !run.IsRecording() {
		t.Error("recorder ended the existing span")
	}
	run.End()
	env.span.End()
	spans := env.spans.Ended()
	if len(spans) != 4 || len(env.spans.Started()) != 4 {
		t.Fatalf("recorder changed span count: ended=%d started=%d, want 4", len(spans), len(env.spans.Started()))
	}
	for _, span := range spans {
		switch span.Name() {
		case "caller":
			if span.Parent().IsValid() || len(span.Events()) != 0 {
				t.Errorf("caller changed: parent=%v events=%v", span.Parent(), span.Events())
			}
		case "agent.run":
			if span.Parent().SpanID() != env.span.SpanContext().SpanID() {
				t.Error("run parent changed")
			}
			assertEvents(t, span.Events(), r.Snapshot(), "created", "start_requested", "admitted", "first_model_response", "settled")
		default:
			if span.Parent().SpanID() != run.SpanContext().SpanID() || len(span.Events()) != 0 {
				t.Errorf("%s changed: parent=%v events=%v", span.Name(), span.Parent(), span.Events())
			}
		}
		if span.SpanContext().TraceID() != env.span.SpanContext().TraceID() {
			t.Errorf("%s belongs to a different trace", span.Name())
		}
	}
}

func TestRecorderPreacquiredAdmissionHasNoNegativeDuration(t *testing.T) {
	env := newTelemetry(t)
	admitted := time.Now().Add(-time.Second)
	r := runobs.New(env.ctx, "run-1", "thread-1")
	r.Admitted(admitted)
	assertMetrics(t, env.reader, nil)
	r.StartRequested()
	s := r.Snapshot()
	if !s.AdmittedAt.Equal(admitted) || !s.AdmittedAt.Before(s.CreatedAt) || !s.AdmittedAt.Before(s.StartRequestedAt) {
		t.Fatalf("preacquired admission was changed: %+v, want %v", s, admitted)
	}
	assertMetrics(t, env.reader, durations(s, creationToStart))
	r.FirstModelResponse()
	r.Settled()
	for i := 0; i < 10; i++ {
		r.Admitted(time.Now())
		r.StartRequested()
		r.FirstModelResponse()
		r.Settled()
	}
	s = r.Snapshot()
	assertEvents(t, env.spans.Started()[0].Events(), s, "created", "admitted", "start_requested", "first_model_response", "settled")
	assertMetrics(t, env.reader, durations(s, creationToStart, admissionToFirstResponse, startToSettlement))
}

func TestRecorderDoesNotRecordPayloadOrHighCardinalityMetricAttrs(t *testing.T) {
	env := newTelemetry(t)
	type contentKey string
	ctx := context.WithValue(env.ctx, contentKey("prompt"), "secret prompt payload")
	ctx = context.WithValue(ctx, contentKey("tool_args"), `{"secret":"tool payload"}`)
	r := runobs.New(ctx, "unique-run-123", "unique-thread-456")
	r.StartRequested()
	r.Admitted(time.Now())
	r.FirstModelResponse()
	r.Settled()
	assertMetrics(t, env.reader, durations(r.Snapshot(), creationToStart, startToAdmission, admissionToFirstResponse, startToSettlement))
	assertEvents(t, env.spans.Started()[0].Events(), r.Snapshot(), "created", "start_requested", "admitted", "first_model_response", "settled")
	for _, event := range env.spans.Started()[0].Events() {
		attrs := attribute.NewSet(event.Attributes...)
		runID, _ := attrs.Value("run_id")
		threadID, _ := attrs.Value("thread_id")
		if len(event.Attributes) != 3 || runID.AsString() != "unique-run-123" || threadID.AsString() != "unique-thread-456" {
			t.Errorf("event %s attributes = %v, want IDs and observed time only", event.Name, event.Attributes)
		}
	}
	if len(env.spans.Started()[0].Attributes()) != 0 {
		t.Errorf("IDs or payload leaked to span attributes: %v", env.spans.Started()[0].Attributes())
	}
}

func TestRecorderNilSafe(t *testing.T) {
	var r *runobs.Recorder
	r.StartRequested()
	r.Admitted(time.Now())
	r.FirstModelResponse()
	r.Settled()
	if got := r.Snapshot(); got != (runobs.Snapshot{}) {
		t.Errorf("nil Snapshot = %+v, want zero times", got)
	}
}

// Wrap a real SDK span to exercise a synchronous callback into the recorder.
type reentrantSpan struct {
	trace.Span
	onEvent func()
}

func (s reentrantSpan) AddEvent(name string, options ...trace.EventOption) {
	s.onEvent()
	s.Span.AddEvent(name, options...)
}

func TestRecorderExportsOutsideLock(t *testing.T) {
	var r *runobs.Recorder
	var metricCallbacks, spanCallbacks int
	env := newTelemetry(t, sdkmetric.WithExemplarFilter(exemplar.Filter(func(context.Context) bool {
		if r != nil {
			_ = r.Snapshot()
			metricCallbacks++
		}
		return false
	})))
	span := reentrantSpan{Span: env.span, onEvent: func() {
		if r != nil {
			_ = r.Snapshot()
			spanCallbacks++
		}
	}}
	r = runobs.New(trace.ContextWithSpan(env.ctx, span), "run-1", "thread-1")
	done := make(chan struct{})
	go func() {
		r.StartRequested()
		r.Admitted(time.Now())
		r.FirstModelResponse()
		r.Settled()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("telemetry callback could not read Snapshot: output held recorder lock")
	}
	if metricCallbacks != 4 || spanCallbacks != 4 {
		t.Errorf("metric/span callbacks = %d/%d, want 4/4", metricCallbacks, spanCallbacks)
	}
	assertMetrics(t, env.reader, durations(r.Snapshot(), creationToStart, startToAdmission, admissionToFirstResponse, startToSettlement))
}

func TestRecorderWithoutProviders(t *testing.T) {
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	for _, ctx := range []context.Context{context.Background(), nil} {
		r := runobs.New(ctx, "run-1", "thread-1")
		r.StartRequested()
		r.Admitted(time.Now())
		r.FirstModelResponse()
		r.Settled()
		s := r.Snapshot()
		if s.CreatedAt.IsZero() || s.StartRequestedAt.IsZero() || s.AdmittedAt.IsZero() || s.FirstModelResponseAt.IsZero() || s.SettledAt.IsZero() {
			t.Errorf("no-op provider lost observed times: %+v", s)
		}
	}
}
