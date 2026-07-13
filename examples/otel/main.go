// Command otel demonstrates the OpenTelemetry integration (obs/otel) end to end.
// It wires stdout exporters, starts a root "agent.run" span, runs a small
// tool-using agent on the offline mock model, and prints the resulting span
// tree and metrics to stdout — no API key required.
//
//	go run ./examples/otel
//
// In production you would swap the stdout exporters for OTLP (Jaeger, Tempo,
// Prometheus, …) and keep everything else identical.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/logger"
	otelobs "github.com/jiujuan/goagent/obs/otel"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	ctx := context.Background()

	// 1. Assemble the OTel SDK. The middleware depends only on the OTel API;
	//    exporters/providers live here in the application, not in the library.
	tp, mp, shutdown := setup(ctx)
	defer shutdown()

	// 2. Build an agent that calls a tool, with the otel middleware attached.
	weather := tool.New("get_weather", "查询某城市天气",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"城市名"`
		}) (string, error) {
			return in.City + "：晴 25°C", nil
		})

	model := mock.New("demo", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("查到了，" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"北京"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是天气助手。"),
		agent.WithTools(weather),
		agent.WithModelOptions(llm.WithModel("demo")), // 让 span 记录真实 model 名
		agent.WithMiddleware(otelobs.New()),           // 默认用全局 TracerProvider/MeterProvider
	)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Start the run-root span; model/tool spans nest under it automatically.
	//    Seed a logger, then WithTraceLogging binds trace_id/span_id onto it so
	//    every subsequent log line correlates with the trace.
	ctx = logger.Into(ctx, logger.L())
	ctx, root := tp.Tracer("examples/otel").Start(ctx, "agent.run")
	ctx = otelobs.WithTraceLogging(ctx)
	logger.From(ctx).Info().Msg("starting run") // 这行日志带 trace_id

	answer, err := a.Run(ctx, "北京天气怎么样？")
	root.End()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n🤖 %s\n\n--- 下面是导出的 span 与指标 ---\n", answer)
	_ = mp // providers flushed by shutdown()
}

// setup builds stdout-backed Tracer/Meter providers, registers them as the
// globals the middleware reads by default, and returns a flush/shutdown func.
func setup(ctx context.Context) (*sdktrace.TracerProvider, *sdkmetric.MeterProvider, func()) {
	res := resource.NewSchemaless() // a real app sets service.name etc.

	traceExp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	metricExp, err := stdoutmetric.New()
	if err != nil {
		log.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(time.Hour))), // 手动 flush，避免演示中途打印
		sdkmetric.WithResource(res),
	)

	// Register as globals so otelobs.New() (no options) picks them up.
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	return tp, mp, func() {
		shCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shCtx)
		_ = mp.Shutdown(shCtx) // flushes the periodic reader to stdout
	}
}
