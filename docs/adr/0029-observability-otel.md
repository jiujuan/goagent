# ADR 0029：可观测性系统 —— OpenTelemetry 中间件（Traces + Metrics + 日志关联）

状态：提议

> 本文给 v2 增加**企业级可观测性**：一个独立子包 `obs/otel`，以可插拔的 `agent.Middleware`
> 形态，为每次模型调用与工具调用产出 OpenTelemetry span，记录对齐 GenAI 语义约定的指标
> （token、时长、错误），并把 trace_id 注入既有 zerolog 日志，使 **trace / metrics / logs
> 三者可互相 join**。核心库**不 import 任何 otel 库**，仅暴露一个 otel-无关的可选接缝
> `ModelContexter`。

---

## 背景

goagent 从开源框架走向**企业级生产应用**，最缺的不是功能数量，而是非功能性能力，其中
**可观测性是 P0**：

1. **没有链路追踪**：[middleware/tracing.go](../../middleware/tracing.go) 只是个 `log.Printf`
   观察者——能看到"模型回复了/工具结束了"，但无法把一次 run 拆成 agent → llm → tool 的
   带时序、可下钻的 span 树，更无法跨 subagent / 跨服务关联。
2. **没有指标**：token 消耗（[core/usage.go](../../core/usage.go) 已统计）、调用时长、工具
   成功率、错误率都没有导出口，无法接 Prometheus/OTel 看板告警。
3. **trace 与日志割裂**：[logger](../../logger) 是结构化日志（zerolog），但日志行不带 trace_id，
   出问题时无法从一条日志跳到对应链路。

生产环境出问题时无法回答："这次 run 慢在哪一步 / 花了多少 token / 哪个 subagent 或 tool
失败了"。

## 决策

新建顶层子包 `obs/otel`（包名 `otelobs`），**只依赖 OpenTelemetry API**（`otel/trace`、
`otel/metric`），**不依赖 SDK**。exporter / provider 由调用方在 `main` 装配（见
[examples/otel](../../examples/otel)），库不绑定导出后端——这是 otel-go 的惯例，也让 `middleware`
包保持零依赖（`middleware` 自身从不 import 其子包）。

### 一、span 树形态

```
agent.run                    ← 调用方用标准 otel API 起（tracer.Start）
├── chat <model>  (step 0)   ← llm span：model / 参数 / token / finish_reason
├── tool.get_weather         ← tool span：name / error
├── chat <model>  (step 1)
└── ...
```

`RunContext` 内嵌 `context.Context` 且 `deeper/subRun/forBranch` 都复用它，故子 span 自动
正确嵌套，**含 subagent 链路**。run-root span 由调用方起（hooks 无 run 级生命周期）。

### 二、otel-无关的 core 接缝 `ModelContexter`

中间件 hooks 无法替换传给 `model.Generate` 的 ctx，要把 span（及 W3C traceparent）注入
provider 调用，需一个接缝。在 [agent/middleware.go](../../agent/middleware.go) 新增可选能力接口：

```go
type ModelContexter interface {
    ModelContext(lc *LoopContext, ctx context.Context) context.Context
}
```

`Stack.ModelContext` 折叠所有实现该接口的中间件；[agent/loop.go](../../agent/loop.go) 在
`ModifyRequest` 后取 `genCtx := l.mw.ModelContext(lc, rc.Context)` 传给 `Generate`。核心从不
import otel；未实现该接口的中间件折叠为 no-op，行为不变。

### 三、span 生命周期（贴合 hooks 时序）

关键时序 `ModifyRequest → ModelContext → Generate → AfterModel`，且 `AfterModel` 先于工具执行。
故：

- **model span**：在 `ModelContext` 起（此时 `lc.Request` 已就绪，能读 model/参数），在
  `AfterModel`（成功）或 `OnError`（失败，AfterModel 不会执行）收。按 `*LoopContext` 指针
  作 key（每步唯一且 Before/After 同一指针）。
- **tool span**：在 `BeforeTool` 起、`AfterTool` 收。`AfterTool` 在并行工具批次中**并发执行**，
  按 `ToolResult.CallID` 作 key，用 `sync.Map` + `LoadAndDelete` 保证并发安全与 end-once。

因 model span 在工具执行前已结束，tool span 与 llm span **平级**挂在 run-root 下，而非嵌套。

### 四、指标（对齐 OTel GenAI semconv）

| 指标 | 类型 | 关键属性 |
|---|---|---|
| `gen_ai.client.token.usage` | Histogram | `gen_ai.token.type`(input/output), `gen_ai.request.model` |
| `gen_ai.client.operation.duration` | Histogram(s) | `gen_ai.operation.name`, `gen_ai.request.model`, `error.type?` |
| `agent.tool.duration` | Histogram(s) | `gen_ai.tool.name`, `error` |
| `agent.tool.calls` | Counter | `gen_ai.tool.name`, `error` |
| `agent.llm.errors` | Counter | `gen_ai.request.model`, `error.type` |

### 五、日志关联

`otelobs.WithTraceLogging(ctx)` 从 `trace.SpanContextFromContext(ctx)` 取 trace_id/span_id，
绑到 [logger](../../logger) 的 ctx logger（复用 `logger.Into/From` + zerolog `WithStr`），使
后续 `logger.From(ctx)` 的每条日志自动带 trace_id。

### 六、顺带修复：AfterModel 现可观测 Usage / StopReason

实现时发现 [agent/loop.go](../../agent/loop.go) 旧代码用合成的 `&llm.Response{Message: final}`
调用 `AfterModel`，**丢弃了 Usage 与 StopReason**——任何压缩/成本类中间件都看不到 token。
已改为 `streamModel` 返回完整的最终 `*llm.Response` 并透传给 `AfterModel`，是一处真实修复。

## API

```go
func New(opts ...Option) agent.Middleware           // 同时实现 agent.ModelContexter
func WithTracerProvider(tp trace.TracerProvider) Option
func WithMeterProvider(mp metric.MeterProvider) Option
func WithRecordContent(on bool) Option              // 记录 prompt/completion/args（PII！）默认关
func WithRedactor(fn func(string) string) Option    // 内容脱敏器
func WithTraceLogging(ctx context.Context) context.Context
```

默认用全局 `otel.GetTracerProvider()` / `otel.GetMeterProvider()`，故 `otelobs.New()` 零配置即可。

## 影响

- 新增：`obs/otel/`（实现 + 测试）、`examples/otel/`、本 ADR。
- 修改：`agent/middleware.go`（+`ModelContexter`/`Stack.ModelContext`）、`agent/loop.go`
  （+`ModelContext` 透传、`streamModel` 返回完整 Response）、`go.mod`（otel API + 测试/示例用 SDK）。
- `middleware` 包仍零依赖；核心库不 import otel。

## 已知限制（未来增强）

1. **真实 model 名**：取自 `lc.Request.Options.Model`，仅在用 `WithModel` 时有值，否则记
   `"unknown"`（中间件拿不到 `l.model.Name()`）。未来一行修复：给 `llm.Response` 加
   `Model string` 或 provider 回填。
2. **tool 出站调用不嵌套**：本版不向 tool 执行注入 span ctx，故 tool 内部 HTTP span 不挂在
   tool span 下。tool span 本身正常记录。未来增强：tool ctx 接缝。
3. **run-root span 归调用方**：hooks 无 run 级生命周期；未来可加 Bus 观察者（消费
   `RunStarted/RunDone/RunFailed`）产出 run 级 span 与时长。

## 替代方案（未采用）

- **直接在核心打 otel**：违反"核心零外部依赖"，且把导出后端绑死进库。
- **把 otel 放进 `middleware` 包**：会让所有 import `middleware` 的用户被动拉入 otel 依赖。
- **仅做 `log.Printf` 增强**：无 span 树、无指标、无关联，达不到企业排障要求。
