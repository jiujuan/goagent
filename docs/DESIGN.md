# goagent 设计文档

> 配套：可视化的分层图与一次 turn 的时序图见 [ARCHITECTURE.md](ARCHITECTURE.md)；
> 把全部能力串起来的可运行示例见 [examples/full](../examples/full)（`go run ./examples/full`）。

goagent 是一个模块化、可扩展、易用的 Go Agent 框架。其架构融合了三个参考框架的精华：

- **earendil-works/pi**：内部消息 ↔ wire 格式的边界转换、上下文压缩、统一错误流。
- **google/adk-go**：`iter.Seq2` 全栈流式原语、事件Event 驱动模型（Event.Actions）、可插拔服务、泛型工具、workflow agents。
- **tmc/langchaingo**：小接口 + functional options、中心化的共享词汇表（`core`）、决策与执行分离、provider 子包隔离。

## 一句话理念

> **一切皆事件流；事件即数据；能力即中间件；接口尽量小。**

## 分层

```
┌─────────────────────────────────────────────────────────┐
│  L4  runner      编排：会话解析、用户消息、事件提交           │
├─────────────────────────────────────────────────────────┤
│  L3  agent       决策单元：LLMAgent / Sequential/Parallel/Loop│
├─────────────────────────────────────────────────────────┤
│  L2  turn 引擎    LLMAgent.Run：model ↔ tool 循环（流式）     │
├─────────────────────────────────────────────────────────┤
│  L1  core/llm/tool   Message · Event · Stream · Model · Tool │
└─────────────────────────────────────────────────────────┘
 横切：callbacks · session(Store/State) · (规划中) middleware/memory
```

每一层都返回 `core.Stream`（即 `iter.Seq2[*core.Event, error]`），消费者用
`for ev, err := range stream` 驱动，`break`/提前返回即可干净地停止上游。

## 包职责

| 包 | 职责 | 关键类型 |
|---|---|---|
| `core` | 共享词汇表，打破 import 循环 | `Message`/`Part`（含 `Image`/`Video`）、`Event`/`Actions`/`Progress`、`Stream` |
| `llm` | provider 无关的模型接口 + functional options | `Model`、`ImageModel`、`VideoModel`、`Request`/`Response`、`Option` |
| `llm/mock` | 可编程的测试用 provider（无网络） | `Model`、`Responder` |
| `llm/anthropic` | Anthropic Messages API（原生协议） | `Model` |
| `llm/openaicompat` | OpenAI 兼容协议（DeepSeek/agnes/OpenAI 共用） | `Model`、`DeepSeek`/`Agnes`/`OpenAI` |
| `llm/agnes` | Agnes 图片/视频生成（异步轮询 + 可恢复） | `ImageModel`、`VideoModel`、`Image`/`Video` |
| `tool` | 工具契约 + 泛型构造 + 反射 JSON Schema | `Tool`、`New[In,Out]`、`Context` |
| `session` | 线性 append-only 会话与状态；内存/JSONL 文件后端 | `Session`、`State`、`Store`、`InMemory`、`NewFileStore` |
| `agent` | Agent 契约与内建实现 | `Agent`、`LLMAgent`、`Sequential/Parallel/Loop`、`ImageAgent`/`VideoAgent` |
| `queue` | 独立后台执行器：队列 + 总线 + worker（只依赖 `core`） | `Queue`/`MemQueue`、`Bus`/`MemBus`、`Worker`、`Job` |
| `embeddings` | 文本向量化抽象（provider 子包） | `Embedder`、`mock`、`openaicompat` |
| `memory` | 长期记忆 / RAG：向量库 + 检索工具 + RAG 中间件 | `Store`、`InMemory`、`SearchTool`、`NewRAG` |
| `runner` | 编排与持久化；后台 agent 桥接 | `Runner`、`EnqueueAgent`、`BusKey` |
| `middleware` | 模型装饰器（能力即中间件） | `Middleware`、`Chain`、`Compaction`、`Retry`、`RateLimit`、`Steering` |
| `callbacks` | 可观测 hook（可选，零开销） | `Handler`、`NoopHandler` |
| `internal/sse` | SSE 解析（流式 provider 复用） | `Scan` |

## 一次 turn 的控制流

```
runner.Run
  └─ store.GetOrCreate(session)
  └─ 提交 user 事件 ──────────────► yield
  └─ agent.Run(ictx)               （LLMAgent）
        loop（≤ MaxSteps）:
          ├─ 从本地 history 构造 llm.Request（含 tool schemas）
          ├─ model.Generate ──► 逐个 Response 转成 Event ──► yield
          │                      （partial 不提交，final 进 history）
          ├─ 若无 tool call → 可选写 OutputKey → 结束
          └─ 否则并发执行工具（保持调用顺序）──► tool 事件 ──► yield ──► 继续 loop
  └─ 每个非 partial 事件 ──► store.Append（事务应用 Actions.StateDelta）──► yield
```

**关键点**：LLMAgent 维护自己的本地 `history`，turn 内的 assistant/tool 消息先
本地累积再 yield，因此其正确性**不依赖** runner 何时提交——runner 与 agent 解耦。

## 可扩展点（全是接口）

- `llm.Model`：接新模型 = 写一个子包。
- `llm.ImageModel` / `llm.VideoModel`：接新生成模型（gpt-image/Gemini/Flux 等）= 写一个子包；用 `agent.Image`/`agent.Video` 包成 Agent。
- `tool.Tool`：接新能力 = `tool.New[In,Out]` 或自定义实现。
- `agent.Agent`：接新编排策略 = 实现 4 方法接口。
- `queue.Queue` / `queue.Bus`：接持久化队列/分布式总线（Redis/DB）= 实现小接口，worker 不动。
- `session.Store`：接新持久化（JSONL/DB/会话树）= 实现 `Store`。
- `callbacks.Handler`：接日志/追踪/指标。
- （规划中）`middleware`：上下文压缩、重试、限流、steering，以装饰器叠加到 turn 引擎。

## 与参考框架的取舍

详见 `docs/adr/`：
- [0001](adr/0001-event-stream-primitive.md) 用 `iter.Seq2` 作为唯一流式原语
- [0002](adr/0002-message-wire-boundary.md) 内部消息模型与 wire 格式在边界处转换
- [0003](adr/0003-linear-session.md) 线性 append-only 会话（树作为后续可选扩展）
- [0004](adr/0004-side-effects-as-actions.md) 事件Event 建模为 `Event.Actions`，由 runner 事务提交
- [0005](adr/0005-provider-isolation.md) provider 隔离为子包
- [0022](adr/0022-media-generation.md) 图片/视频生成：能力接口 + 模型即 Agent + 独立后台队列

## 路线图

- [x] L1–L4 闭环：LLMAgent + 工具调用 + 线性会话
- [x] providers：mock / anthropic / openaicompat(deepseek/agnes)
- [x] 图片/视频生成：`llm.ImageModel`/`VideoModel` + `llm/agnes` + `agent.Image`/`Video` + 独立 `queue` 后台执行 — ADR 0022
- [x] 流式 provider（SSE 增量）— ADR 0006
- [x] `middleware` 模型装饰器：compaction / retry / ratelimit / steering — ADR 0007/0011
- [x] LLM 驱动委派（`transfer_to_agent` 自动工具）— ADR 0008
- [x] `session` JSONL 文件后端（落盘 + 重放恢复）— ADR 0009
- [x] `memory` 长期记忆 + RAG（向量库 + 检索工具 + RAG 中间件）— ADR 0010
- [x] 委派方向规则（sub/parent/peer）+ 返回父 agent + 防环深度保护 — ADR 0008
- [x] `prompt` 可组合 Section 构建 system prompt（Identity/Environment/ToolGuidance/SessionState）— ADR 0014
- [ ] 会话树（分支/fork）后端 — 见 [SESSION-TREE.md](SESSION-TREE.md)
  - [x] 阶段 1：`Event.ParentID` + Session 路径投影（leaf→root）+ State 沿路径重放机制
  - [x] 阶段 2：`TreeStore`（fork/checkout/branches）+ 分支切换沿路径重放 State + 活动叶 `.refs.json` 落盘
  - [x] 阶段 3：重摘要节点化（`Event.SummarizesTo` 投影 + `session.Summarize` + `middleware.Resummarize` 持久化压缩）
- [ ] 真实向量库后端 + 文档切分/会话入库
