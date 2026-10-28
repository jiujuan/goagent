# goagent 与 Genkit Go（重写版）架构分层 / 功能 / 非功能对比

## 0. 文档信息

| 项 | 内容 |
|---|---|
| 对比对象 A | goagent，`D:\codeproject\mygo\goagent`，分支 main，HEAD `45b2d8d` |
| 对比对象 B | Genkit 官方 Go SDK，`D:\codeproject\mygo\genkit-ai\genkit\go`，commit `72639f74f`，module 路径 `github.com/firebase/genkit/go`（`go/go.mod:1`），Go 1.25.0 |
| 调研方式 | 三个并行只读子代理分别盘 B 的分层、功能、非功能；一个子代理盘 A；所有写入本文的行号在落笔前用 `sed`/`grep` 逐条复核（见附录 A） |
| 体量实测 | A：`.go` 321 个 / 54,528 行（含测试），其中非测试 204 个 / 31,161 行，测试 117 文件 / 539 个 Test 函数，示例 49 个目录。<br>B：全目录 190,267 行（含测试），非测试按包为 `plugins` 144 文件 39,402 行、`ai` 34 文件 20,420 行、`genkit` 13 文件 6,199 行、`core` 32 文件 6,341 行、`internal` 21 文件 4,278 行；测试 216 文件、Test/Example/Benchmark 共 1,836 个 |
| 结论摘要 | B 的价值是"一个 Action 原语复用 schema/校验/追踪/HTTP/DevUI"和"不可变 Agent + 每次新建运行对象 + 断连继续跑"；A 的价值是跨进程持久化治理与上下文/预算治理。A 的优先缺口在模型面（结构化输出、reasoning）与交付面（无服务端），以及断连继续跑 |

> 本文只讨论 Genkit 的 Go SDK。Genkit 的 JS/Python 侧与 `genkit-tools` CLI 只在涉及 Go 侧行为时提及。与 Aster v0.35.0 的对比另见对话记录，未合并进本文。

---

## 1. 前提勘误：本地这份 Go SDK 是重写后的版本

网上大量中文资料描述的是 Genkit 老版 Go SDK 的结构，与本地 checkout 不符。落笔前先记住四点差异，否则对照会错：

1. **`go/flows` 包已不存在**。目录只有 `ai/ core/ genkit/ internal/ plugins/ samples/ tests/`。老版带 Start/Resume/Interrupt/flowStateStore 的状态机 Flow 被删薄为 `core/flow.go`（198 行）：Flow 现在是 Action 的包装（`core/flow.go:29`），"步骤"只是 trace span + 结果缓存（`core/flow.go:88,105,111`）。全仓 `grep StateStore` 零命中，即 **checkpoint / 断点恢复语义已从 flow 层移除**。
2. 老版的 `ChatRequest`、`MessageInfo`、`RawMetadata`、`GenerateXXXCallback` 均已改名或删除。请求类型现在叫 `ModelRequest`（`ai/gen.go:344`），响应叫 `ModelResponse`（`ai/gen.go:360`）。
3. Agent 能力全部在实验包 **`ai/exp`**：`ai/exp/agent.go` 3,604 行、`ai/exp/session.go` 881 行、`ai/exp/localstore/`。不显式打开 `genkit.WithExperimental()`，构造器直接 panic（`genkit/exp/experimental.go:32`）。
4. 网上常见的"genkit go 支持 DefineChat / flows 状态机持久化"这类描述在本 checkout 上不成立。

---

## 2. Genkit Go 的分层架构

### 2.1 包与依赖方向

```
core/api      纯接口层：Action / Plugin / Registry / ActionType 枚举 / Environment
   ↑
internal/     registry 唯一实现 + base(JSON schema 反射与校验) + genkitbridge(破环)
   ↑          + cmd/jsonschemagen(从 TS zod 定义生成 Go 类型与错误码)
core/         泛型 Action[In, Out, Stream]：执行时自动包 span + 入出参校验 + metrics
   ↑
ai/           类型化包装：Model / Tool / Embedder / Retriever / Evaluator / Prompt / Formatter
   ↑          ai/exp：Agent / SessionRunner / SessionStore / localstore
genkit/       入口聚合：Init(opts) + reflection/dev 桥 + HTTP Handler + DefineXxx 便捷函数
   ↑
plugins/      19 个顶层目录（含 internal 共享层），可反向 import genkit 取强类型入口
```

依赖方向严格单向，`core` 不 import `ai`（grep 零命中）。插件可以 import `genkit`，例如 `plugins/ollama/ollama.go:196` 的参数类型是 `*genkit.Genkit`。

### 2.2 五条值得借鉴的结构性决策

**决策 1：一切能力皆 Action（单原语）。**
接口 `api.Action{Registerable; Name; RunJSON; RunJSONWithTelemetry; Desc}`（`core/api/action.go:32`）；`ActionType` 常量块从 `core/api/action.go:107` 起，含 `ActionTypeModel`（`:113`）到 `ActionTypeCustom`（`:120`）共 19 种。`ai.Model`、`ai.Tool`、Flow 都只是对 `core.Action[具体请求类型, 具体响应类型, 流分片类型]`（`core/action.go:54`）的类型化包装。
收益：一次注册同时获得 JSON Schema 推导（`core/action.go:198 schemaFor` → `internal/base/json.go:99 InferJSONSchema`）、trace span、入出参 schema 校验、指标、HTTP 端点、DevUI 元数据。这些横切能力只实现一遍，任何新组件类型自动继承。

**决策 2：静态登记 + 动态懒解析的两段式注册。**
`Init` 顺序执行：每个 plugin 的 `Init(ctx) []Action` 返回的 action 逐个登记（`genkit/genkit.go:303` 起）。未命中的 action key 走 `Registry.ResolveAction`（`internal/registry/registry.go:244`）：先读锁查表，未命中则取 `resolveMu` 写锁（`:250`）再查一次，然后交给实现了 `api.DynamicPlugin` 的插件现场构造并登记。
实例：Ollama 首次使用某模型时才构造（`plugins/ollama/ollama.go:616`），Google GenAI 用声明式 catalog 按需构造（`plugins/googlegenai/actions.go:54,59`）。
效果是"插件登记"与"provider 客户端建立"解耦：`Init` 不会因为某个 provider 的密钥缺失或网络不可达而失败。

**决策 3：接口收口在 `core/api`，实现私有在 `internal`。**
Registry 接口全在 `core/api/registry.go:27`，唯一实现 `internal/registry/registry.go`（395 行）。`ai`/`genkit`/`plugins` 三方共享接口，实现不外泄，用户无法绕过校验直接改注册表。

**决策 4：子注册表实现请求级作用域。**
`Registry.NewChild()`（`internal/registry/registry.go:85`）创建继承父表的子表。Generate 期为中间件注入的工具建子表（`ai/generate.go:538`，另有 `:1365,1374` 两处同类调用），中间件贡献的工具不会污染全局注册表。

**决策 5：用 init 注入破 Go 的循环 import + 用代码生成对齐跨语言契约。**
`internal/genkitbridge`（`:39-53`）暴露 `RegistryOf`/`SeedContextForRegistry`，由 `genkit/bridge.go` 的 `init()` 回填，解决 `core` 与 `ai` 互相需要的小片段。类型化错误码则由 `internal/cmd/jsonschemagen` 从 TS zod 生成的 `genkit-schema.json` 产出（`core/core.go:19-22` 的 go:generate 声明），保证 Go/JS/DevUI 三方 schema 同源。

### 2.3 这些决策的复杂度代价

| 代价 | 证据 |
|---|---|
| 泛型三参 + `struct{}` 哨兵类型，读代码门槛高 | `core/action.go:54`，以及 `:97,208-213` 的无流分支 |
| 重复注册直接 panic，使 `Init` 顺序敏感、测试需要手搓 registry | `internal/registry/registry.go:104-137` |
| 懒解析把 provider 的网络调用推迟到首个请求，错误暴露晚；静态登记与动态解析两条路径都要各自维护 | `internal/registry/registry.go:244-276` |
| dev/prod 分支（reflection、telemetry 导出）散落在 `Init` 内部 | `genkit/genkit.go:303-433` |
| `DefineXxx` 与 `NewXxx` + `RegisterAction` 双轨命名，`genkit` 与 `genkit/exp` 双包 | `genkit/genkit.go:451,524` |

---

## 3. 易用性

| 维度 | 实测结论 | 证据 |
|---|---|---|
| 最小可运行 agent | 11 行：`genkitx.DefineAgent(g, name, aix.InlinePrompt{ai.WithModel(model), ai.WithSystem(...)}, aix.WithSessionStore(...), aix.WithDescription[any](...))` | `go/samples/basic-agents/pirate.go:30-40` |
| 选项风格 | 统一的 `WithXxx` 函数族，`ai/option.go` 1,182 行：`WithModel:302`、`WithMaxTurns:341`、`WithToolChoice:351`、`WithOutputType:705`、`WithStreaming:817`、`WithResume:1075` | 同上 |
| 泛型用在刀刃上 | `Action[In,Out,Stream]`、`Agent[State]`、`Flow[In,Out,Stream]`、`DefineSchemaFor[T]`，所以"类型不匹配"是编译期错误 | `core/action.go:54`、`ai/exp/agent.go:102` |
| 名字写错何时报错 | **运行期**：`ErrModelNotFound` / `ErrToolNotFound` | `ai/generate.go:499` 一带、`:2035` |
| provider 配置形态 | `any` + JSON 往返（无强类型 config），schema 靠反射推断 | `internal/registry/registry.go:67-75`、`core/core.go:68-71` |
| 启动失败处理 | `Init` 不返回 error，选项/插件/reflection 失败一律 panic；**没有 `Must()` 体系**，调用方无法优雅降级 | `genkit/genkit.go:311,320,339,420` |
| 实验 API 门禁 | 未打 `genkit.WithExperimental()` 时构造 agent/flow 直接 panic | `genkit/exp/experimental.go:32` |
| 工具链心智负担 | 官方推荐 `genkit start -- go run main.go`，reflection 默认仅 `GENKIT_ENV=dev`；`GENKIT_REFLECTION_ENABLED=true` 可在生产显式打开，`genkit.Handler` 本身无环境限制 | `go/README.md:826`、`genkit/reflection_config.go:92-93`、`genkit/servers.go:100` |

评价：**代码写起来短，运行起来依赖工具链**。它的"易用"有一半来自反射与自动生成（schema 自动推、trace 自动上报），代价是要把 CLI + DevUI + reflection 当成默认工作流，否则运行期错误（名字写错、schema 反射失败）缺少定位手段。

---

## 4. 高并发设计

这是本次对比中最值得抄的一块。

**4.1 注册表并发安全。** 双锁：`mu sync.RWMutex` 保护 4 张 map，`resolveMu sync.RWMutex` 保护懒解析（`internal/registry/registry.go:36-37`）。`ResolveAction` 是标准双检锁（`:244-276`），并发首次调用同一组件不会重复构造。
**弱点**：所有未命中的解析共用一把写锁（`:250`），冷启动或大量动态模型场景下这些调用被串行化。

**4.2 "共享的不可变、可变的独享"。** `ai/exp.Agent[State]` 构造后字段全不可变（`ai/exp/agent.go:775`）；每次调用经 `:1182` 走 `newAgentRuntime` 新建 `SessionRunner[State]`（`:102`），把 turnIndex、lastSnapshotID、usage 合计这类**序列可变状态挂在 per-run 对象上**，用 `snapMu` / `usageMu` / Session `mu` 三把锁加一个 fnDone channel 划分 happens-before，并且**逐字段注释写清它由哪个 goroutine 独占**（`:102-194`）。
这正是 goagent 曾写过一版"Agent + Runtime 分离"设计文档（已删除，`45b2d8d`）想解决的问题。Genkit 的答案是：**不池化、不 reset，每次新建运行对象**。

**4.3 断连继续跑（detach）。** 工作上下文用 `context.WithoutCancel(clientCtx)` 派生（`ai/exp/agent.go:1496`），配 watcher goroutine 与 `detachOnce` 原子仲裁"客户端断开"与"显式 detach"两种取消来源（`:1511-1532`）；断开后继续执行并把结果写成 pending 快照（`:1969,2022`）。HTTP 侧的 SSE 长连同样 detach（`genkit/servers.go:375`）。恢复走 `ai/exp/agent.go:3236` 的 resume 选项路径。

**4.4 工具并行执行。** `handleToolRequests`（`ai/generate.go:1984`）对响应里的每个 tool request 起一个 goroutine（`:2009` 附近），结果经 `resultChan` 按 index 归位，panic 被 recover 成工具错误；resume 路径同样并行（`:2932`）。流式 chunk 由 `toolStreamer`（`:1837`）串行化回调。
**弱点**：无 errgroup、无 worker 池、无并发上限——一条含 N 个 tool call 的响应就起 N 个无界 goroutine。

**4.5 验证强度。** 216 个测试文件、1,836 个 Test/Example/Benchmark 函数，含断连与多 goroutine 场景（例如双向 action 测试专门覆盖 WithoutCancel 之后的持久化）。但 CI 只跑 `go test -C go -v ./...`（`.github/workflows/go.yml:80`），**不带 `-race`**——上面那套精细锁分工在流水线层面没有被竞态检测器验证过。

---

## 5. 可扩展性

**5.1 插件契约极小。** `api.Plugin{Name() string; Init(ctx) []Action}`（`core/api/plugin.go:27`）；懒建模再实现 `api.DynamicPlugin{ListActions; ResolveAction}`；中间件类插件实现 `ai.MiddlewarePlugin{Middlewares}`（`ai/middleware.go:288`），由 `Init` 做类型断言后登记。
参照体量：最小真实 provider `plugins/anthropic/anthropic.go` 305 行；最小非模型插件 `plugins/localvec/localvec.go` 286 行单文件。

**5.2 组件类型对用户开放。** `ActionTypeCustom`（`core/api/action.go:120`）加上 `genkit.RegisterAction`（`genkit/genkit.go:451`）/`DefineValue`（`:476`），允许注册自定义 kind 的能力，而不限于 Model/Tool/Embedder/Retriever/Evaluator。

**5.3 中间件是洋葱式三层钩子 + 工具注入。** `ai.Hooks{Tools []Tool; WrapGenerate; WrapModel; WrapTool}`（`ai/middleware.go:31`）：`WrapGenerate` 每次工具循环迭代调用一次（N 轮调 N+1 次），能同时看到累积请求、迭代序号、流回调，并在返回的 tool request 上补 Ref 以便下一轮把响应配回请求；`WrapTool` 注释明确写了会被并发调用。core 层还有通用 `Middleware[In,Out,Stream]`（`core/middleware.go:21`）可包任意 action。
插件侧已用这套钩子实现了 fallback（209 行）、retry（189 行）、tool_approval（335 行）、soft_tool_errors（79 行）、filesystem、skills，以及 exp 下的 agents 委派（982 行）与 compression（2,290 行：摘要压缩 + 去重 + 工具输出截断）。

**5.4 对外服务面。** `genkit.Handler`（`genkit/servers.go:100`）把任意已注册 action 变成 HTTP 端点，SSE 流式与 durable streaming 在同一处（`:375`）；reflection 输出 action 元数据供 DevUI/跨语言前端消费。这一层是插件生态与前端能长出来的根因。

---

## 6. Genkit Go 功能清单（按实现厚度分类）

**实现厚实**

| 能力 | 位置 | 行数 |
|---|---|---|
| 生成 API：`Generate` / `GenerateText` / `GenerateData[Out]` / `GenerateStream` / `GenerateDataStream`，含工具循环、resume、跨轮 usage 求和 | `ai/generate.go:1336,1475,1496,1561,1619` | 3,101 |
| 选项族 | `ai/option.go` | 1,182 |
| 工具与 HITL：`NewTool`（`:472`）、`InterruptWith`（`:313`）、restart/respond、multipart 工具响应、分片流（`ai/tool/tool.go:115 SendChunk`） | `ai/tools.go` | 1,292 |
| Prompt + dotprompt 模板（变量插值、少样本、tools、模型绑定） | `ai/prompt.go:95` | 1,295 |
| 结构化输出：反射推 schema、5 种内置格式（text/json/jsonl/array/enum）、请求前注入格式指令（`ai/format.go:140`）、响应后校验（`jsonHandler.ParseOutput`，`:310`） | `ai/format.go` | 663 |
| 消息与部件：text/data/media/reasoning(带 signature)/custom/resource/toolRequest/toolResponse/partial/interrupt/restart | `ai/document.go`（`:371` 为 partial） | 866 |
| Agent 运行时：`SessionRunner`、会话快照、心跳/过期/abort、后台 detached turn、`localstore` 内存与文件实现 | `ai/exp/agent.go`、`ai/exp/session.go:220` | 3,604 + 881 |
| 插件生态：19 个顶层目录。模型类 `anthropic`、`googlegenai`、`vertexai`、`ollama`、`compat_oai`（子插件 openai/anthropic/dashscope/deepseek/kimi/openrouter/xai/zai 共 8 个）；向量与检索 `localvec`、`pinecone`、`weaviate`、`postgresql`、`alloydb`、`vertexai/vectorsearch`、`firebase`；工具类 `mcp`、`middleware`、`a2ui`、`systemone`；运维 `server`、`googlecloud` | `go/plugins/` | 39,402 |
| MCP：客户端 Stdio/SSE/StreamableHTTP 三种传输（`plugins/mcp/client.go:31,38,43`）、resources（静态 + 模板 + 分页）、prompts 可转成 `ai.Prompt`；反向可把 Genkit 工具暴露为 MCP server（`plugins/mcp/server.go:316 ServeStdio`） | `plugins/mcp/` | — |

**接近接口壳**

- `ai/retriever.go`（`:42` 起，220 行）、`ai/embedder.go`、`ai/evaluator.go`（461 行，含单条与 Batch 两套接口）：主体是对 Action 的转发。
- `core/flow.go`（198 行）：仅 span + 结果缓存。
- `plugins/evaluators`：只有 regex / deepEqual / JSONata 三个指标（`ConfigureMetric`，`evaluators.go:82`），**LLM judge 只在注释里**。
- `ai/exp/inline.go`（50 行）、`ai/exp/ref.go`（49 行）：类型别名。
- `plugins/mcp` 的 HTTP 服务端传输需要用户自己接 mcp-go 的 transport（`plugins/mcp/server.go:329-331`）。

**明确未找到（不要按这些下结论）**

- 文档分块 chunking / splitter：全仓无实现。
- 结构化输出解析失败后的"一次修正重试"：`jsonHandler.ParseOutput` 校验失败直接返回 error（`ai/format.go:310` 起），无 fixup 机制。
- `ConstraintPart`、handoff / transfer 原语：无。
- 内建历史裁剪 / 摘要 / token 计数：由 `aix.WithStateTransform`（`ai/exp/option.go:204`）与 `plugins/middleware/exp/compression.go` 承担，非框架内建。
- 事件溯源式跨进程 replay、fencing / 写者协调：无。
- `go/` 目录无 CHANGELOG；无 testcontainers；`Init` 无 error 通道。

---

## 7. goagent 现状（作为对照基线）

**分层**：`core`（Message/Event/State/Directive/Usage）→ `llm`（Model 接口 `llm/model.go:18`、Request `:27`、Response `:43`、ToolSchema `:70`）→ `tool`（`tool/tool.go:18` 接口、`:60` Updater、`tool/function.go:21` 泛型 `New[In,Out]` 反射生成 schema、`tool/validate.go:21` 参数校验、`tool/mcp/mcp.go:55` Connect）→ `agent`（`agent/agent.go:26` Agent + `agent/agent.go:37` 应用默认 maxTurns（常量在 `agent/loop.go:25`，值为 16）；`agent/run.go:20` 非阻塞 Run 句柄，`Iter:50` / `Wait:72`；`agent/runtime.go:22` RunContext；`agent/loopctx.go:13` LoopContext）→ `middleware`（治理）→ 外围 `checkpoint`/`memory`/`queue`/`workspace`/`vfs`/`sandbox`/`obs`/`logger`/`skills`/`eval`/`embeddings`/`config`。

**Agent 与运行态**：`Agent` 是配置 + 可运行句柄，每次 `Run/Stream` 新建 `core.State`（`core/state.go:12`），校验与消息冻结在句柄创建时完成、Store 访问推迟到首次观察（`agent/agent.go:136` 的注释即此语义）。同线程多写者由 `WithThreadCoordinationRequired`（`agent/options.go:126`）与 `agent/durable_runtime.go:21` 的 claim 协调。

**中间件形态**：五个按序相位钩子 `BeforeModel / ModifyRequest / AfterModel / BeforeTool / AfterTool`（`agent/middleware.go:22`）+ 扩展接口 `ToolRejecter`(`:83`)、`ToolPolicyChecker`(`:94`)、`RunFinisher`(`:109`)、`ModelContexter`(`:120`)、`ToolContexter`(`:141`)、`HistoryCompacter`(`:154`)；返回 `core.Directive`（`core/directive.go:7`，Kind 见 `:18`）参与控制流。注释明确 AfterTool 可能在并行批中并发执行（`agent/middleware.go:20-21`）。

**治理与恢复**：`middleware/window.go:190` 三种窗口策略（差异说明见 `:301-305`）、`middleware/compaction.go:56`、`middleware/runbudget.go:83`、`middleware/loopguard.go:76`、`middleware/argguard.go:87`、`middleware/rag.go:28`；韧性包装 `middleware/retry.go:28`、`middleware/fallback.go:33`、`middleware/circuit.go:74`、`middleware/ratelimit.go:24`。持久化侧 `checkpoint/durable.go:15` 的 Durable 接口含 inbox 投递、Commit、resume 协调与审批决策，`checkpoint/file.go` 是 JSONL 文件实现；HITL 走 `agent/hitl.go:35 Decide` + `:65 Resume`。多 agent：`agent/transfer.go:83` 合成 `transfer_to_agent` 工具、`agent/subagent.go:22` AsTool 隔离上下文、`agent/plan.go:147 NewPlan`（DAG，节点并发默认 8）与 `:162 NewLLMPlan`（planner + replanner）。观测：`obs/otel/otel.go:94 New(...) agent.Middleware`。配置：`config/config.go:130 Load`（viper）。

**依赖**：仅 go-redis、zerolog、viper、otel（exporter 只有 stdout）——`go.mod:6-16`。

---

## 8. 逐项对照

### 8.1 架构分层

| 维度 | Genkit Go | goagent | 判断 |
|---|---|---|---|
| 统一原语 | 一切皆 Action，19 种 kind，schema/trace/校验/HTTP/DevUI 免费复用 | `Runnable` 统一 LLM 循环 / 工作流 / DAG / 计划，共用 `Run/Stream/Resume` 面；prompt、tool 不是可注册可调用资产 | Genkit 面广且横切复用彻底；goagent 抽象更薄但外横切能力各自实现一遍 |
| 接口与实现隔离 | `core/api` 纯接口 + `internal/registry` 私有实现 | 接口分散在各包（`llm.Model`、`tool.Tool`、`checkpoint.Checkpointer`），无统一注册表 | Genkit 更严格；goagent 靠构造函数注入，无全局状态 |
| 配置与运行态分离 | 不可变 `Agent` + 每次新建 `SessionRunner`（`ai/exp/agent.go:775,1182,102`） | `Agent` 配置 + 每次新建 `*Run`/`RunContext`/`core.State`（`agent/agent.go:136`、`core/state.go:12`） | 思路一致，形态不同；Genkit 的字段级并发注释值得抄 |
| 作用域隔离 | 子注册表 `NewChild`（`internal/registry/registry.go:85`）承载中间件注入的工具 | run 作用域动态工具表 `LoopContext.AddTool`（`agent/loopctx.go:56`）+ 每步合并广告 | 功能等价；Genkit 靠继承表、goagent 靠 per-run map |
| 依赖方向 | 严格单向，插件可反向 import 入口包 | 单向且无环：`core` 不依赖任何上层 | 平手 |
| 交付层 | `genkit.Handler` + SSE + durable streaming + reflection | 无服务端；`internal/sse` 只是 provider 响应的解析客户端 | Genkit 明显领先，这是 goagent 最大缺口 |

### 8.2 功能

| 功能 | Genkit Go | goagent |
|---|---|---|
| 结构化输出 | 完整（反射 schema + 5 格式 + 校验） | **无**（`llm/options.go` 无 schema/ResponseFormat 字段） |
| reasoning / thinking | 部件级支持带 signature，provider 侧接线 | 只有数据模型 `core/message.go:40 Thinking`，llm 层无 provider 触发或解析 |
| 工具并行 | 有，但无上限、无池 | `WithToolExecution`（`agent/options.go:96`）+ `ToolParallel` 默认（`:137`）+ 工具自声明串行例外（`agent/exectools.go:514`）+ 超时 abandon 宽限 2ms（`agent/exectools.go:195-200`） |
| 工具结果流式更新 | `SendChunk` + multipart | `tool.Updater`（`tool/tool.go:60`）→ `ToolUpdate` 事件（`agent/exectools.go:505`） |
| 参数校验 | gojsonschema 全量校验 | `tool.Validate`（type/required/items，未知关键字宽松，`tool/validate.go:21`）+ ArgGuard 拒绝分类 |
| HITL | 数据化 interrupt part + restart/respond | `Run.Decide/Resume`（`agent/hitl.go:35,65`）+ 批准批次交回 loop 执行 + seam 回退 |
| 断连继续跑 | 有（`WithoutCancel` + 原子仲裁 + pending 快照） | **无**（`WithoutCancel` 只出现在 `agent/write_guard.go:202` 与 `memory/memx/consolidation.go:100`，均非运行主链路） |
| 跨进程恢复与写者协调 | 无 fencing；SessionStore 仅 `localstore` 内存/文件 | **有**：claim + fencing + inbox 重投递 + 工具幂等（`checkpoint/durable.go:15`）+ 跨进程恢复测试 |
| MCP | 官方 mcp-go，Stdio/SSE/StreamableHTTP + resources + prompts + 反向 server | 自研客户端，仅 tools + Stdio（`tool/mcp/mcp.go:55`） |
| Prompt 模板 | dotprompt 文件 + 变量 + 少样本 + 模型绑定（`ai/prompt.go:95`） | `prompt/` 包 + `skills` 三层渐进披露（`skills/skill.go:44`，含 Gate） |
| 上下文窗口治理 | 靠插件 compression（2,290 行） | 三策略 Window + Compaction + RunBudget + LoopGuard，各带独立测试 |
| 长期记忆 | 无内建（靠 retriever 插件） | `memory/` + memx 分层 + 自动 RAG（`middleware/rag.go:28`）+ 运行末固化 |
| 评测 | regex/deepEqual/JSONata 三个指标，LLM judge 未落地 | `eval/` Scorer/Harness/Judge/Rule/Loop 成体系（`eval/embed.go:14-16` 的 SemanticSimilarity 仍是返回 `errTODO` 的桩） |
| provider 数量 | 4 个原生 + 8 个 OpenAI 兼容子插件 + ollama | anthropic / openaicompat / agnes / mock |
| 内置编码工具集 | filesystem、skills、git 相关中间件 | `tool/file`、`tool/exec`、`tool/web`（较少） |
| 沙箱 | 依赖工具与 provider 侧 | `sandbox/` 进程命令策略，注释明确容器后端留待未来（`sandbox/sandbox.go:9`） |
| 后台任务 | 无队列（detach turn 承担长跑） | `queue/`：内存 + Redis Stream + Worker 池（`queue/worker_pool.go`） |

### 8.3 非功能

| 维度 | Genkit Go | goagent |
|---|---|---|
| 并发正确性证据 | 设计精细但 CI 不跑 `-race`（`.github/workflows/go.yml:80`） | 测试含并发/故障注入/跨进程恢复（`agent/thread_consistency_test.go` 等），本地可跑 `-race` |
| 启动失败处理 | panic，无 error 通道 | `agent.New(...)` 返回 error |
| 配置类型安全 | `any` + JSON 往返 | viper 结构体 + 强类型 `llm.Model` |
| 扩展新 provider 成本 | 2 个方法，最小 305 行 | 实现 `llm.Model` 接口 + 手写请求/响应映射 |
| 可观测 | OTel 全链路 + DevUI trace | `obs/otel/otel.go:94` 中间件，exporter 仅 stdout |
| 体量 | 非测试约 76,640 行 | 非测试 31,161 行 |
| 测试密度 | 1,836 个函数 / 190,267 行 ≈ 每千行 9.6 | 539 个 / 54,528 行 ≈ 每千行 9.9 |

---

## 9. goagent 可改进 / 可新增（按性价比排序）

每项给落点文件、做法、验收方式。GA-01、GA-02 建议各自单开 ADR（编号从 ADR-0033 起）。

### GA-01 结构化输出（P0）
- **缺口证据**：`llm/options.go` 无 schema/ResponseFormat 字段；`agent/` 仅有 `outputKey` 存原文。
- **做法**：`llm.Request` 增加 Output 描述（JSON Schema + 指令注入开关）；由 `tool/function.go:21` 已有的类型→schema 反射能力复用（配套的 `tool/registry.go:10 SchemaOf` 把 Tool 转成 `llm.ToolSchema`）；`llm/openaicompat`、`llm/anthropic` 分别落地原生 json_schema 与 tool-forcing 两条路径；`llm/mock` 支持按 schema 校验回放。响应侧在 `agent/loop.go` 的 AfterModel 之后加一个校验步骤，失败时以 constraint 类消息回灌并**重试一次**（Genkit 没有这一步，`ai/format.go:310` 直接返回 error，加上即超过它）。
- **验收**：三 provider 各一条集成用例 + mock 的"首次不合规、重试后合规"用例 + 预算计数计入重试。

### GA-02 reasoning / thinking 接线（P0）
- **缺口证据**：`core/message.go:40` 有 `Thinking` 部件，`grep Thinking llm/` 零命中。
- **做法**：`llm.Options` 增 `Thinking`（enabled/budget/effort），anthropic 走 extended thinking + signature 透传，openaicompat 解析 `reasoning_content`；事件流增加 thinking 增量事件，`obs/otel` 默认对 thinking 内容做 redact。
- **验收**：mock 回放含 signature 的响应，断言 signature 在下一轮请求中原样回传（reasoning 模型对此敏感）。

### GA-03 断连继续跑 detach（P1）
- **缺口证据**：运行主链路无 `WithoutCancel`；客户端取消即整跑报废。
- **做法**：参照 `ai/exp/agent.go:1496`：`Run` 内部把工作 ctx 从调用 ctx 用 `context.WithoutCancel` 派生，另设显式 `Run.Cancel()` 作为真正的取消通道；用一次性原子标志仲裁"观察者断开"与"显式取消"；断开后完成当前 step 并把 pending 快照写入 checkpointer（复用 `checkpoint/durable.go:15` 的 Commit）。
- **验收**：`Iter()` 提前退出后，`Agent.Resume(thread)` 能拿到后续结果；显式 `Cancel()` 仍在 2ms 宽限内停止（对齐 `agent/exectools.go:195-200`）。

### GA-04 最小服务交付面（P1）
- **缺口证据**：全仓 `net/http` 只出现在客户端（provider 调用、`tool/web`、`embeddings/openaicompat`）。
- **做法**：新增 `service/` 包，只做三件事——`POST /agents/{name}/runs`、`GET /runs/{id}/events`（SSE，映射现有 `Run.Iter`）、`POST /runs/{id}/decisions`（映射 `Run.Decide` + `Resume`）。鉴权与多租户先不做，只留一个 `Middleware func(http.Handler) http.Handler` 挂载点。
- **验收**：一个端到端测试：起 httptest server，跑 mock agent，断言 SSE 事件序列与进程内 `Iter()` 完全一致（含 ToolUpdate 与 Interrupted）。
- **同时修文档**：`README.md:35` 说"bus…Redis 后端可跨进程发送进度事件"，但 `bus/*.go` 内无 redis 依赖，跨进程实际是 `queue/redis_pubsub.go` 与 `queue/redis_stream.go`。措辞要改，或把 queue 的 pub/sub 明确包装成 bus 的一个后端。

### GA-05 内置工具包与 MCP 能力补齐（P2）
- **做法**：新增 `tool/builtin/`（grep/glob/read/edit/write/todowrite），全部走现有 `workspace` 路径约束 + `sandbox` 策略 + ArgGuard；MCP 侧补 resources 与 prompts 两类客户端能力（`tool/mcp/protocol.go` 目前只覆盖 tools），并加 SSE/StreamableHTTP 传输以对齐 `plugins/mcp/client.go:31,38,43`。
- **验收**：MCP 用官方 test server 跑一遍 resources/list 与 prompts/list；builtin 工具在 `examples/` 里加一个"跨文件检索并改一处"的端到端示例。

### GA-06 向量后端与评测补桩（P2）
- **做法**：`memory/` 增 pgvector 后端（或 qdrant）；把 `eval/embed.go:14-16` 的 `SemanticSimilarity` 桩实现掉（`embeddings.Embedder` 已够用），并给 `embeddings/` 补第二个真实实现。
- **验收**：eval 至少一个真实嵌入驱动的用例；pgvector 走 docker 起的容器集成测试，默认 build tag 关闭。

### GA-07 权限分级与规则记忆（P2）
- **做法**：`tool.Tool` 增可选 capability 声明（readonly / destructive / network），`middleware/permission.go:51` 的 `Permission(rules...)` 据此决定默认放行或询问；把"本线程内永久允许"的决策落到 checkpointer（复用 `checkpoint/durable.go:15` 已有的 decisions 通道）。
- **验收**：同一工具第二次调用不再中断；`obs/otel` 记录允许规则来源。

### GA-08 编排可序列化（P3，可选）
- **做法**：给 `agent/plan.go:147` 的 DAG 与 `agent/workflow.go` 加一层 YAML/JSON 描述格式（节点、依赖、join 语义 wait/first、gate），加载后编译成现有 Plan；保留 Go 构造路径不变。
- **理由**：这是 Genkit 也没有的东西（其 flow 已删薄），但对"外部配置驱动执行"有真实价值；放在最后是因为它需要新语法与校验，成本明显高于前七项。

---

## 10. 明确不建议照搬 Genkit 的部分

| 不照搬 | 理由 |
|---|---|
| `Init` 全程 panic | goagent `agent.New()` 返回 error 是更好的契约（`genkit/genkit.go:303-433`） |
| `any` + JSON 往返的配置 | 失去编译期检查，且错误只在运行期暴露（`internal/registry/registry.go:67-75`） |
| 全局注册表 + `DefineXxx`/`NewXxx` 双轨 | goagent 现在是构造函数注入、无全局状态，测试隔离更简单；引入注册表等于重做一半 API |
| `ai/exp` 那种 3,604 行 SessionRunner + `WithExperimental` 门禁 | 此前已定取舍：不引入实验包双轨（`genkit/exp/experimental.go:32` 的 panic 门禁意味着稳定性承诺为零） |
| reflection + DevUI + `genkit start` 工具链 | 在没有前端产品诉求前是纯负担；GA-04 的最小 HTTP+SSE 已覆盖真实需求 |
| 一切皆 Action 的字面照搬 | 值得学的是"横切能力只实现一遍"这个效果；goagent 已有 middleware 承担横切，替换原语的成本远大于收益 |
| 无上限并行工具 | Genkit 自己的弱点（`ai/generate.go:2009`），goagent 现有 `ToolExecMode` + 串行例外 + 超时 abandon 更完整，应保持 |
| 事件溯源式 replay / 快照塞进 part metadata | 既有取舍不变：快照只进 checkpointer，`core.RunFailed` 形状不动 |

---

## 附录 A：引用核对方法与本次修正

- 核对方法：把全部 `文件:行号 + 期望关键字` 写成清单，逐条用 `sed -n "Np"` 取该行、用 `grep -E` 断言关键字，共抽查 122 条，通过 114 条；未通过的 8 条都是"范围起始行差几行"，已重新用 `grep -n` 定位真实行号后写入正文（例如 `NewChild` 调用实际在 `ai/generate.go:538` 而非 536；`schemaFor` 在 `core/action.go:198` 而非 164；`runStep` 在 `core/flow.go:111`；`ActionType` 常量块起于 `core/api/action.go:107`）。
- 本次修正的统计口径：goagent 的"321 文件 / 54,528 行"是**含测试**的全仓数字；非测试是 204 文件 / 31,161 行。Genkit 侧"约 17.8 万行"的早期说法已按包重测（非测试合计 76,640 行，含测试 190,267 行），测试函数计数按 `^func (Test|Example|Benchmark)` 得 1,836 个。
- 一处易混淆事实已在正文标注：goagent 的 `bus` 包**没有** Redis 后端（`grep -rn redis bus/*.go` 无命中），跨进程事件在 `queue` 包；`README.md:35` 的表述需要修订（见 GA-04）。
- 未采信的信息：子代理报告中"plugins 17 个顶层目录""可实例化插件 ≥25"这类无法用一条命令复核的说法，正文改用实测值"19 个顶层目录（含 internal 共享层）"与"compat_oai 下 8 个 OpenAI 兼容子插件"。
- 本文写作与核对过程均为只读，未修改两个仓库的任何源码文件。
