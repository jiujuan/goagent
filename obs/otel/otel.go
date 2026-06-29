// Package otelobs provides an OpenTelemetry integration for goagent as a
// pluggable agent.Middleware. It emits a span per model call ("chat <model>")
// and per tool call ("tool.<name>"), records GenAI-semconv metrics (token
// usage, operation/tool duration, errors), and offers WithTraceLogging to
// correlate the active trace with the zerolog-based logger.
//
// It depends only on the OpenTelemetry *API* (trace, metric). The SDK —
// exporters and providers — is assembled by the caller (see examples/otel),
// keeping this package unopinionated about the export backend.
//
// Wiring: the middleware also implements agent.ModelContexter, so the loop
// injects the model span into the context passed to the provider. The run-root
// span ("agent.run") is started by the caller with the standard otel API;
// because RunContext embeds context.Context, the model and tool spans nest
// under it automatically — including across subagents.
package otelobs

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/logger"
)

const (
	scopeName = "github.com/jiujuan/goagent/obs/otel"

	// Attribute keys, aligned with the OpenTelemetry GenAI semantic conventions.
	attrOperationName = "gen_ai.operation.name"
	attrRequestModel  = "gen_ai.request.model"
	attrTemperature   = "gen_ai.request.temperature"
	attrMaxTokens     = "gen_ai.request.max_tokens"
	attrFinishReason  = "gen_ai.response.finish_reason"
	attrTokenType     = "gen_ai.token.type"
	attrToolName      = "gen_ai.tool.name"
	attrErrorType     = "error.type"
	attrToolError     = "error"
	attrPrompt        = "gen_ai.prompt"
	attrCompletion    = "gen_ai.completion"
	attrToolArgs      = "gen_ai.tool.arguments"

	opChat = "chat"
)

// config holds the middleware's resolved settings.
type config struct {
	tp            trace.TracerProvider
	mp            metric.MeterProvider
	recordContent bool
	redact        func(string) string
}

// Option configures the middleware.
type Option func(*config)

// WithTracerProvider sets the TracerProvider. Defaults to otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.tp = tp }
}

// WithMeterProvider sets the MeterProvider. Defaults to otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.mp = mp }
}

// WithRecordContent toggles recording of prompt/completion text and tool
// arguments as span attributes. Off by default because that content is often
// PII. When on, pair it with WithRedactor.
func WithRecordContent(on bool) Option {
	return func(c *config) { c.recordContent = on }
}

// WithRedactor sets a function applied to any recorded content (prompt,
// completion, tool arguments) before it lands on a span. Only consulted when
// WithRecordContent is on.
func WithRedactor(fn func(string) string) Option {
	return func(c *config) { c.redact = fn }
}

// New returns an agent.Middleware that emits OpenTelemetry traces and metrics.
// The returned value also implements agent.ModelContexter.
func New(opts ...Option) agent.Middleware {
	c := config{tp: otel.GetTracerProvider(), mp: otel.GetMeterProvider()}
	for _, o := range opts {
		o(&c)
	}
	if c.tp == nil {
		c.tp = otel.GetTracerProvider()
	}
	if c.mp == nil {
		c.mp = otel.GetMeterProvider()
	}

	m := &mw{
		cfg:    c,
		tracer: c.tp.Tracer(scopeName),
	}
	meter := c.mp.Meter(scopeName)
	// Instruments returned by the API are always non-nil and safe to call even
	// if construction reports an error, so a no-op provider degrades cleanly.
	m.tokens, _ = meter.Int64Histogram("gen_ai.client.token.usage",
		metric.WithUnit("{token}"), metric.WithDescription("Tokens consumed per model call"))
	m.genDur, _ = meter.Float64Histogram("gen_ai.client.operation.duration",
		metric.WithUnit("s"), metric.WithDescription("Model call duration"))
	m.toolDur, _ = meter.Float64Histogram("agent.tool.duration",
		metric.WithUnit("s"), metric.WithDescription("Tool call duration"))
	m.toolCnt, _ = meter.Int64Counter("agent.tool.calls",
		metric.WithUnit("{call}"), metric.WithDescription("Tool calls executed"))
	m.errCnt, _ = meter.Int64Counter("agent.llm.errors",
		metric.WithUnit("{error}"), metric.WithDescription("Model call errors"))
	return m
}

type mw struct {
	agent.BaseMiddleware
	cfg    config
	tracer trace.Tracer

	tokens  metric.Int64Histogram
	genDur  metric.Float64Histogram
	toolDur metric.Float64Histogram
	toolCnt metric.Int64Counter
	errCnt  metric.Int64Counter

	models sync.Map // *agent.LoopContext -> *modelRec
	tools  sync.Map // callID string      -> *toolRec
}

type modelRec struct {
	span  trace.Span
	start time.Time
	model string
}

type toolRec struct {
	span  trace.Span
	start time.Time
	name  string
}

var (
	_ agent.Middleware     = (*mw)(nil)
	_ agent.ModelContexter = (*mw)(nil)
)

// ModelContext starts the model span and returns a context carrying it, so the
// provider call (and any downstream traceparent) nests under it. lc.Request is
// populated by this point in the loop, so model and decoding params are known.
func (m *mw) ModelContext(lc *agent.LoopContext, ctx context.Context) context.Context {
	model := requestModel(lc.Request)
	ctx, span := m.tracer.Start(ctx, opChat+" "+model,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(attrOperationName, opChat),
			attribute.String(attrRequestModel, model),
		),
	)
	if req := lc.Request; req != nil {
		if req.Options.Temperature != 0 {
			span.SetAttributes(attribute.Float64(attrTemperature, req.Options.Temperature))
		}
		if req.Options.MaxTokens != 0 {
			span.SetAttributes(attribute.Int(attrMaxTokens, req.Options.MaxTokens))
		}
		if m.cfg.recordContent {
			span.SetAttributes(attribute.String(attrPrompt, m.content(lastUserText(req.Messages))))
		}
	}
	m.models.Store(lc, &modelRec{span: span, start: time.Now(), model: model})
	return ctx
}

// AfterModel ends the model span on the success path, recording token usage and
// finish reason.
func (m *mw) AfterModel(lc *agent.LoopContext, r *llm.Response) (core.Directive, error) {
	v, ok := m.models.LoadAndDelete(lc)
	if !ok {
		return core.Directive{}, nil
	}
	rec := v.(*modelRec)
	attrs := []attribute.KeyValue{
		attribute.String(attrOperationName, opChat),
		attribute.String(attrRequestModel, rec.model),
	}
	if r != nil {
		if r.StopReason != "" {
			rec.span.SetAttributes(attribute.String(attrFinishReason, string(r.StopReason)))
		}
		if r.Usage != nil {
			tokenAttrs := func(kind string) metric.MeasurementOption {
				return metric.WithAttributes(
					attribute.String(attrOperationName, opChat),
					attribute.String(attrRequestModel, rec.model),
					attribute.String(attrTokenType, kind),
				)
			}
			m.tokens.Record(lc, int64(r.Usage.InputTokens), tokenAttrs("input"))
			m.tokens.Record(lc, int64(r.Usage.OutputTokens), tokenAttrs("output"))
			rec.span.SetAttributes(
				attribute.Int("gen_ai.usage.input_tokens", r.Usage.InputTokens),
				attribute.Int("gen_ai.usage.output_tokens", r.Usage.OutputTokens),
			)
		}
		if m.cfg.recordContent {
			rec.span.SetAttributes(attribute.String(attrCompletion, m.content(r.Message.Text())))
		}
	}
	m.genDur.Record(lc, time.Since(rec.start).Seconds(), metric.WithAttributes(attrs...))
	rec.span.End()
	return core.Directive{}, nil
}

// OnError ends the model span on the failure path (AfterModel does not run when
// the provider errors).
func (m *mw) OnError(lc *agent.LoopContext, err error) (core.Directive, error) {
	v, ok := m.models.LoadAndDelete(lc)
	if !ok {
		return core.Directive{}, nil
	}
	rec := v.(*modelRec)
	rec.span.RecordError(err)
	rec.span.SetStatus(codes.Error, err.Error())
	m.errCnt.Add(lc, 1, metric.WithAttributes(
		attribute.String(attrRequestModel, rec.model),
		attribute.String(attrErrorType, errorType(err)),
	))
	m.genDur.Record(lc, time.Since(rec.start).Seconds(), metric.WithAttributes(
		attribute.String(attrOperationName, opChat),
		attribute.String(attrRequestModel, rec.model),
		attribute.String(attrErrorType, errorType(err)),
	))
	rec.span.End()
	return core.Directive{}, nil
}

// BeforeTool starts a tool span as a child of the run-root span found in the
// loop context.
func (m *mw) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	ctx := lc.RunContext.Context
	_, span := m.tracer.Start(ctx, "tool."+call.Name,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String(attrToolName, call.Name)),
	)
	if m.cfg.recordContent && len(call.Args) > 0 {
		span.SetAttributes(attribute.String(attrToolArgs, m.content(string(call.Args))))
	}
	m.tools.Store(call.ID, &toolRec{span: span, start: time.Now(), name: call.Name})
	return core.Directive{}, nil
}

// AfterTool ends the tool span. It may run concurrently across a parallel tool
// batch; the sync.Map keyed by CallID keeps each end independent.
func (m *mw) AfterTool(lc *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	v, ok := m.tools.LoadAndDelete(tr.CallID)
	if !ok {
		return core.Directive{}, nil
	}
	rec := v.(*toolRec)
	attrs := []attribute.KeyValue{
		attribute.String(attrToolName, rec.name),
		attribute.Bool(attrToolError, tr.IsError),
	}
	if tr.IsError {
		rec.span.SetStatus(codes.Error, "tool reported an error")
	}
	rec.span.SetAttributes(attribute.Bool(attrToolError, tr.IsError))
	m.toolCnt.Add(lc, 1, metric.WithAttributes(attrs...))
	m.toolDur.Record(lc, time.Since(rec.start).Seconds(), metric.WithAttributes(attrs...))
	rec.span.End()
	return core.Directive{}, nil
}

// content applies the configured redactor (if any) to text destined for a span.
func (m *mw) content(s string) string {
	if m.cfg.redact != nil {
		return m.cfg.redact(s)
	}
	return s
}

// WithTraceLogging binds the active span's trace_id/span_id onto the logger
// stored in ctx, so every log line emitted via logger.From(ctx) downstream
// correlates with the trace. A no-op when ctx carries no valid span.
func WithTraceLogging(ctx context.Context) context.Context {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ctx
	}
	l := logger.From(ctx).
		WithStr("trace_id", sc.TraceID().String()).
		WithStr("span_id", sc.SpanID().String())
	return logger.Into(ctx, l)
}

func requestModel(req *llm.Request) string {
	if req != nil && req.Options.Model != "" {
		return req.Options.Model
	}
	return "unknown"
}

func lastUserText(msgs []core.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleUser {
			return msgs[i].Text()
		}
	}
	return ""
}

func errorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	default:
		return reflect.TypeOf(err).String()
	}
}
