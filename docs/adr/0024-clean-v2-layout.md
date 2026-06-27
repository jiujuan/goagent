# ADR 0024：v2 干净重构——分层包结构、无 v1 包袱

状态：进行中（Stage 1 已落地）。本分支 `feat/v2-clean-rewrite` 直接重构，不再双轨；
v1 完整保存在远程 `v0.0.1` 分支与 `v0.0.1` tag。本文取代 [ADR 0023](0023-v2-architecture.md)
的双轨迁移路径，保留其设计理念（Bus 观测 / Checkpointer 持久 / Directive 控制 /
Spec·Runtime 分离 / loop-hook middleware）。

---

## 背景

[ADR 0023](0023-v2-architecture.md) 给出 v2 设计，但要求与 v1 双轨并存，逼出三处妥协：
Event union 躲进独立 `event` 包、`stateAdapter` 把 `core.State` 桥接成 v1 `session.State`、
`AgentSpec` 暂寄 `runtime`。既然 v1 已安全归档在 `v0.0.1`（branch + tag），就**直接重构**，
把这些妥协全部消除。

| 双轨期妥协 | 干净 v2 |
|---|---|
| Event union 单独 `event` 包 | **回归 `core/event.go`** |
| `stateAdapter` 桥接 session.State | **`tool.Context.State` 直接是 `*core.State`** |
| 工具控制信号只能走中间件 | **`tool.Result` 原生带 `Control *Directive` + `State []StateOp`** |
| `AgentSpec` 寄居 `runtime` | 收进 `agent` 包,与 Middleware/Loop 同居 |
| v1 `Agent interface{Run}` 仍在 | 删除;改 Spec 数据 + 包内 sealed `Runnable` |
| session 存事件日志供重放 | 改 checkpoint 快照;session 退化为线程索引（后续 stage） |

---

## 决策：分层包结构

```
goagent/
├── core/                  # L0 词汇层 · 零内部依赖
│   ├── message.go         #   Message · Part(sealed) · ToolCall · ToolResult
│   ├── messagejson.go     #   Part 的 tagged-envelope JSON 编解码
│   ├── event.go           #   Event sealed union(RunStarted/MessageDelta/.../RunFailed)
│   ├── directive.go       #   Directive · DirectiveKind · Resolve(优先级折叠)
│   ├── state.go           #   State · Todo · StateOp · FileStore 接口
│   ├── usage.go           #   Usage · ProgressInfo
│   └── id.go              #   NewID
│
├── llm/                   # L1 模型契约 · provider 隔离(原样保留)
│   ├── model.go options.go image.go video.go
│   └── anthropic/ openaicompat/ agnes/ mock/
│
├── tool/                  # L1 能力契约
│   ├── tool.go            #   Tool · Result{Content,IsError,Control,State} · Context{ctx,State,CallID}
│   ├── function.go schema.go registry.go
│
├── bus/                   # L2 观测 · pub/sub over channels
│   ├── bus.go             #   Topic · Lossy/Lossless · Subscribe/Publish
│   └── iter.go            #   Adapt/Iter(push→pull,终止于 RunDone/RunFailed/Interrupted)
│
├── checkpoint/            # L2 持久化 · 快照树
│   ├── checkpoint.go      #   Checkpointer · Checkpoint(ParentID 树) · PendingHITL
│   ├── memory.go          #   内存后端
│   └── branch.go          #   Fork / time-travel
│
├── vfs/                   # L2 虚拟文件系统后端 · 实现 core.FileStore
│   └── instate.go         #   InState(文件随 State,被 checkpoint 捕获)
│
├── agent/                 # L3 执行模型 · 互递归内聚簇(打破依赖环的关键)
│   ├── spec.go            #   Spec(纯数据) · LoopPolicy · ToolExecMode
│   ├── middleware.go      #   Middleware 接口(6 钩子) · BaseMiddleware · Stack(洋葱+折叠)
│   ├── runnable.go        #   sealed Runnable · Compile · Drive
│   ├── loopctx.go         #   RunContext · LoopContext · steeringQueue
│   ├── loop.go            #   AgentLoop 相位机 + 每步 checkpoint
│   └── exectools.go       #   并发/串行工具执行,读 Result.Control/State
│
├── runtime/               # L4 引擎 + 句柄
│   ├── runtime.go         #   Runtime{bus,store} · New · Compile
│   ├── agent.go           #   Agent 句柄 · Start · Resume
│   └── run.go             #   Run 句柄:Iter/Events/Wait/Steer/Cancel(惰性驱动,零丢事件)
│
├── examples/quickstart/   # L5 demo
└── docs/adr/              #   本文
```

### 依赖 DAG（无环）

```
L0  core ◄──────────────────────────────── 所有包
L1  llm → core            tool → core,llm
L2  bus → core            checkpoint → core         vfs → core
L3  agent → core,llm,tool,bus,checkpoint
L4  runtime → agent,bus,checkpoint,vfs    middleware → agent(后续 stage)
L5  examples → runtime,agent,...
```

**破环关键**：`agent` 包**定义** `Middleware` 接口、`RunContext`、`AgentLoop`，但只依赖抽象
`bus.Bus` / `checkpoint.Checkpointer`，**不 import `runtime`**。`runtime` 反过来 import
`agent`。因此 `Spec`(含 `[]Middleware`)、`Middleware`、`LoopContext`、`AgentLoop` 必须
**同居 `agent` 包**——这是内聚，不是缺陷。workflow 与 LLM agent 统一用包内 sealed `Runnable`
派发，公开 API 仍是数据（`agent.Spec`）。

---

## 关键契约

```go
// core/state.go —— 工具直接读写,无 session 桥接
type State struct { Messages []Message; Todos []Todo; Files FileStore; KV map[string]any }

// tool/tool.go —— 工具原生表达控制与状态变更
type Result struct {
    Content []core.Part
    IsError bool
    Control *core.Directive   // escalate/stop/transfer/interrupt
    State   []core.StateOp    // 立即可见的状态变更
}
type Context struct { context.Context; State *core.State; CallID string }

// agent/spec.go —— 纯数据
type Spec struct {
    Name, Description, Instruction string
    Model llm.Model; Tools []tool.Tool; SubAgents []Spec
    Middleware []Middleware; Loop LoopPolicy; ModelOptions []llm.Option
}

// runtime —— 用户入口
rt := runtime.New(runtime.Config{})
ag := rt.Compile(spec)
run, _ := ag.Start(ctx, runtime.RunRequest{ThreadID:"t1", Message: core.UserText("...")})
for ev, err := range run.Iter() { ... }   // 或 run.Events(bus.Lossy) 多订阅
res, _ := run.Wait()                       // 结算
```

### Run 句柄的惰性驱动（零丢事件）

`Start` **不立即驱动** loop；`Iter()`/`Wait()` 先 `Subscribe` 再 `drive()`（`sync.Once`），
保证消费者订阅在事件产生之前——消除 push 模型的启动竞态。`Wait()` 用 `select{<-done; <-ch}`
既能自驱动（无 Iter 消费者时）又不会在已结束时泄漏 goroutine。

---

## AgentLoop 相位机

每步：PrepareTurn(排空 steering + BeforeModel) → CallModel(ModifyRequest→流式→AfterModel)
→ ExecuteTools(BeforeTool 门禁→并发/串行执行→读 Result.Control/State + AfterTool) →
Checkpoint(每步快照) → ApplyDirectives(Resolve 折叠后分派 Stop/Escalate/Transfer)。
BeforeTool 返回 `Interrupt` → 落 `PendingHITL` 快照 + 发 `Interrupted` 事件 + 暂停。

「可控」六条均对应代码明确点：有界(MaxSteps)、可暂停/恢复(每步 checkpoint)、可中断
(BeforeTool→Interrupt)、可操控(PrepareTurn 排空 steering)、可观测(每相位 publish)、
策略化(LoopPolicy)。

---

## demo

| demo | 覆盖 | 状态 |
|---|---|---|
| `quickstart` | Spec→Compile→Start→Iter 最小闭环 | ✅ |
| `streaming-bus` | 一个 Run 多订阅者(UI Lossy / tracing Lossless) | 待 Stage |
| `resume` | 跨进程 Resume(thread) 续跑 | 引擎已支持,demo 待写 |
| `hitl` | BeforeTool→Interrupt→Pending→Approve 续跑 | 暂停已通,Approve 待 Stage |
| `branch` | History/Fork time-travel | 引擎已支持,demo 待写 |
| `workflow` | Pipeline.Then/ThenParallel/ThenLoop | 待 Stage 2 |
| `deepagent` | 规划 + 子智能体隔离 + vfs 卸载 | 待 Stage 3 |

---

## 分阶段重构计划

- **Stage 1（已完成）**：core(契约定稿) · llm(原样保留) · tool(重写契约) · bus · checkpoint ·
  vfs(InState) · agent(spec/middleware/loop/exectools/runnable) · runtime(engine/agent/run) ·
  quickstart。`go build/vet/gofmt` 干净;core/bus/checkpoint/runtime 测试绿(含 Start/Iter/Wait、
  Wait-only、Resume、HITL 中断+Pending 快照)。
- **Stage 2**：workflow agent（Sequential/Parallel/Loop/Pipeline）+ transfer 方向规则 +
  subagent 上下文隔离;`agent` 包内补 workflow runner（统一走 sealed `Runnable`）。
- **Stage 3**：concrete `middleware` 包（compaction/rag/retry/ratelimit/steering/hitl-approve/
  summarization/permission/planning），全部实现 `agent.Middleware`。
- **Stage 4**：从 `v0.0.1` 移植并适配 prompt / skill / sandbox / memory(作为 RAG middleware) /
  embeddings / scheduler(背景执行) / session(线程索引) / checkpoint 文件后端。
- **Stage 5**：补齐 demo（streaming-bus / resume / hitl / branch / workflow / deepagent / media）
  与 README、ARCHITECTURE 文档。

---

## 后果

- **不可逆**：v1 代码已从本分支删除（安全网在 `v0.0.1`）。Stage 2–5 会从 `v0.0.1` 逐包移植。
- **包数更少、依赖更直、无桥接代码**：core 吸收 Event、tool 原生带 Control/State、执行模型
  收进 `agent` 一包、runner→runtime、queue→scheduler、session 退化。
- **`.gitignore` 调整**：本分支取消忽略 `docs/`，使 ADR 随代码入库。
- 维持 v1 的核心纪律：`core` 为依赖根、包间尽量无环、接口尽量小、零外部依赖（除 provider）。

---

## 备选方案

- **继续双轨（ADR 0023）**：保留妥协与桥接代码，认知负担长期存在。既有 `v0.0.1` 归档，无需双轨。
- **保留 v1 `Agent` 接口**：与「Spec 是数据」冲突;改为 sealed `Runnable` 内部派发。
- **照搬 LangGraph channel/reducer**：心智过重;只取分支/checkpoint，并发用分支隔离 + join。
