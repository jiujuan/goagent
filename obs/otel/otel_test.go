package otelobs_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	otelobs "github.com/jiujuan/goagent/obs/otel"
	"github.com/jiujuan/goagent/tool"
)

func weatherTool() tool.Tool {
	return tool.New("get_weather", "weather",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (string, error) {
			return in.City + ": sunny", nil
		})
}

// weatherModel calls a tool on the first turn, then answers.
func weatherModel() llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done: " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"Beijing"}`)
	})
}

// errModel always fails, to exercise the OnError span path.
type errModel struct{}

func (errModel) Name() string { return "err-model" }
func (errModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, errors.New("boom"))
	}
}

func newProviders() (*tracetest.SpanRecorder, *sdkmetric.ManualReader, *sdktrace.TracerProvider, *sdkmetric.MeterProvider) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	return sr, mr, tp, mp
}

func TestSpanTree(t *testing.T) {
	sr, mr, tp, mp := newProviders()
	obs := otelobs.New(otelobs.WithTracerProvider(tp), otelobs.WithMeterProvider(mp))

	a, err := agent.New(
		agent.WithModel(weatherModel()),
		agent.WithTools(weatherTool()),
		agent.WithModelOptions(llm.WithModel("mock-model")),
		agent.WithMiddleware(obs),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Root span; child model/tool spans must nest under it.
	ctx, root := tp.Tracer("test").Start(context.Background(), "agent.run")
	if _, err := a.Run(ctx, "weather?"); err != nil {
		t.Fatal(err)
	}
	root.End()

	spans := sr.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}

	chat, ok := byName["chat mock-model"]
	if !ok {
		t.Fatalf("missing model span; got %v", names(spans))
	}
	toolSpan, ok := byName["tool.get_weather"]
	if !ok {
		t.Fatalf("missing tool span; got %v", names(spans))
	}

	rootID := root.SpanContext().SpanID()
	if chat.Parent().SpanID() != rootID {
		t.Errorf("model span parent = %v, want root %v", chat.Parent().SpanID(), rootID)
	}
	if toolSpan.Parent().SpanID() != rootID {
		t.Errorf("tool span parent = %v, want root %v", toolSpan.Parent().SpanID(), rootID)
	}

	if got := attrString(chat, "gen_ai.request.model"); got != "mock-model" {
		t.Errorf("model attr = %q, want mock-model", got)
	}
	if got := attrString(chat, "gen_ai.response.finish_reason"); got == "" {
		t.Errorf("missing finish_reason on model span")
	}

	// Token usage metric must have been recorded.
	if !hasMetric(t, mr, "gen_ai.client.token.usage") {
		t.Errorf("token usage metric not recorded")
	}
	if !hasMetric(t, mr, "agent.tool.calls") {
		t.Errorf("tool calls metric not recorded")
	}
}

func TestErrorPath(t *testing.T) {
	sr, mr, tp, mp := newProviders()
	obs := otelobs.New(otelobs.WithTracerProvider(tp), otelobs.WithMeterProvider(mp))

	a, err := agent.New(
		agent.WithModel(errModel{}),
		agent.WithModelOptions(llm.WithModel("err-model")),
		agent.WithMiddleware(obs),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "hi"); err == nil {
		t.Fatal("expected run error")
	}

	var chat sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "chat err-model" {
			chat = s
		}
	}
	if chat == nil {
		t.Fatalf("model span not ended on error path; got %v", names(sr.Ended()))
	}
	if chat.Status().Code != codes.Error {
		t.Errorf("model span status = %v, want Error", chat.Status().Code)
	}
	if !hasMetric(t, mr, "agent.llm.errors") {
		t.Errorf("error counter not recorded")
	}
}

func TestNoProviderIsNoOp(t *testing.T) {
	// With the default (global no-op) providers, the middleware must not panic
	// and the run must still succeed.
	obs := otelobs.New()
	a, err := agent.New(
		agent.WithModel(weatherModel()),
		agent.WithTools(weatherTool()),
		agent.WithMiddleware(obs),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "weather?"); err != nil {
		t.Fatal(err)
	}
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func hasMetric(t *testing.T, r *sdkmetric.ManualReader, name string) bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return true
			}
		}
	}
	return false
}
