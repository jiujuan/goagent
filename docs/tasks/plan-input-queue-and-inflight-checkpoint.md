# P1-5 输入队列与 P1-6 工具批次在途检查点实施计划

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文只覆盖 P1-5（输入队列、投递阶段、抢占）和 P1-6（工具批次在途检查点）。

**目标：** 按照 [输入队列与工具批次在途检查点设计稿](../analysis/input-queue-and-inflight-checkpoint-design.md) §5–§11 完成两件事。第一件：给临时消息和持久收件箱加投递阶段（`before_model` / `after_model` / `after_tools` / `next_turn`），用统一的读取接口接入现有两个投递点，给临时消息一个显式回执状态，并提供默认关闭的模型调用抢占。第二件：在工具批次通过全部执行前检查之后、任何 handler 启动之前写一次在途检查点，崩溃后用 `Agent.Resume` 按台账状态恢复该批次，保证每个计划调用恰好一条结果。

**架构：** 在 `core` 包新增阶段、来源、回执状态类型和四个事件（`InputDelivered`、`ModelPreempted`、`ToolBatchInFlight`、`ToolBatchRecovered`）。在 `agent` 包给队列条目加阶段、给票据加状态、新增按条件取走的队列方法，以及一个逻辑输入队列视图（新文件 `agent/input.go`，不是新存储）；抢占用单次尝试的取消上下文实现，重试留在同一步。在 `checkpoint` 包给收件箱记录加阶段，新增 `Checkpoint.InFlight` 及其存储校验、深拷贝、提交摘要和元数据投影；`RunStatus` 与查询层用独立字段报告在途，不复用 `Paused`。恢复入口在 `Agent.Resume` 内按 `cp.InFlight` 分流，执行仍复用 `execTools`。

**技术栈：** Go 1.25（本机 go1.26.x）、标准库、现有 `llm/mock`、`checkpoint.Memory`、`checkpoint.File`、bus。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-22。
- 基线：`9e5df25`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./... -count=1` 确认基线。
- 设计依据：[input-queue-and-inflight-checkpoint-design.md](../analysis/input-queue-and-inflight-checkpoint-design.md) §5–§11。前置已落地项：P0-1 冻结步骤视图、P0-2 任务循环、P0-3 工具分段并发、P0-4 上下文超限重试。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：P1-5、P1-6。P1-7 实时状态投影、P1-8 会话忙时策略、工具并发调度本身不在本计划内。

当前工作区有未跟踪的分析文档、`.scratch/` 目录和几份未入库的计划文档（包括本文和设计稿）。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。任务卡里的提交只包含该卡列出的代码和测试；本文与设计稿的入库由用户另行安排。

## 1. 现有能力与实施边界

### 1.1 现在两条输入通道怎么走

1. `Agent.Stream` 返回的 `Run.Steer` 把消息压入该运行的临时队列并返回票据（[agent/run.go:146](../../agent/run.go#L146)），`RunContext.Steer` 直接委托队列的 `push`（[agent/runtime.go:128](../../agent/runtime.go#L128)）。队列条目只有消息和票据两项（[agent/steering.go:83](../../agent/steering.go#L83)），入队见 [agent/steering.go:100](../../agent/steering.go#L100)，整体取走见 [agent/steering.go:113](../../agent/steering.go#L113)，原子「取走或关闭」见 [agent/steering.go:127](../../agent/steering.go#L127)，关闭见 [agent/steering.go:144](../../agent/steering.go#L144)。
2. 票据只有两种结算结果：已投递（带步骤号）见 [agent/steering.go:159](../../agent/steering.go#L159)，未投递（带错误）见 [agent/steering.go:166](../../agent/steering.go#L166)。未结算时 `Outcome()` 返回零值（[agent/steering.go:55](../../agent/steering.go#L55)），没有「排队中」这个可查询状态。
3. 运行中的读取点只有两个：`runStep` 的 Phase 1 整体取空临时队列并追加历史（[agent/loop.go:259](../../agent/loop.go#L259)），然后读取整个持久收件箱（[agent/loop.go:265](../../agent/loop.go#L265)）；终稿后 `settleTurn` 再整体取空一次以决定是否续跑（[agent/loop.go:216](../../agent/loop.go#L216)），并只检查收件箱是否有未确认消息（[agent/loop.go:222](../../agent/loop.go#L222)）。
4. 队列由 `run` 的 defer 关闭（[agent/loop.go:103](../../agent/loop.go#L103)），`Run.Cancel` 也关闭（[agent/run.go:155](../../agent/run.go#L155)）。`drainOrClose` 是原子的（[agent/loop.go:236](../../agent/loop.go#L236)），关闭瞬间到达的消息不会悬空。
5. 持久收件箱的 `InboxMessage` 只有 ID 和消息（[checkpoint/durable.go:60](../../checkpoint/durable.go#L60)），`DeliveryReceipt` 只有 ID 和「是否重复」（[checkpoint/durable.go:65](../../checkpoint/durable.go#L65)）。内存存储在 `Deliver` 时按 ID 去重、同 ID 不同内容报冲突（[checkpoint/durable_memory.go:65](../../checkpoint/durable_memory.go#L65)），去重只比较消息体（[checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419)）；文件存储语义相同（[checkpoint/file.go:183](../../checkpoint/file.go#L183)）。消费在 Phase 1 登记确认 ID（[agent/durable_runtime.go:98](../../agent/durable_runtime.go#L98)），确认随下一次检查点提交原子生效（[agent/durable_runtime.go:366](../../agent/durable_runtime.go#L366)）。

缺口：没有投递阶段；模型流开始后无法打断；临时消息没有「排队中」状态；持久消息没有「已被某个步骤消费」的可观察方式。

### 1.2 现在一个工具批次怎么走

1. 入口 `execTools` 要求已有冻结步骤视图，否则直接报错（[agent/exectools.go:42](../../agent/exectools.go#L42)、[agent/exectools.go:43](../../agent/exectools.go#L43)），按 `buildToolCallSegments` 分段（[agent/exectools.go:149](../../agent/exectools.go#L149)）。
2. 每个调用在 `callOne` 里检查广播、解析、参数（[agent/exectools.go:310](../../agent/exectools.go#L310)），计算调用编号（[agent/exectools.go:348](../../agent/exectools.go#L348)、哈希实现见 [agent/exectools.go:426](../../agent/exectools.go#L426)），持久存储存在时先 `ClaimTool`（[agent/exectools.go:351](../../agent/exectools.go#L351)）：`claimed`/`unknown` 返回结果未知（[agent/exectools.go:360](../../agent/exectools.go#L360)），`completed` 直接回放并跳过 `AfterTool`（[agent/exectools.go:367](../../agent/exectools.go#L367)、[agent/exectools.go:375](../../agent/exectools.go#L375)），`pending` 或新记录才执行 handler（[agent/exectools.go:399](../../agent/exectools.go#L399)）。
3. 执行前检查在循环里、`execTools` 之前：`CheckToolPolicy`（[agent/loop.go:483](../../agent/loop.go#L483)）与逐个 `BeforeTool`（[agent/loop.go:486](../../agent/loop.go#L486)、改写参数见 [agent/loop.go:487](../../agent/loop.go#L487)）。
4. 检查点写在整批之后（Phase 4，[agent/loop.go:516](../../agent/loop.go#L516)、[agent/loop.go:519](../../agent/loop.go#L519)）。在此之前，含工具调用的助手消息只存在于运行内存中。
5. 批次中途出错时只追加已报告的结果再 `fail`（[agent/loop.go:509](../../agent/loop.go#L509)、[agent/loop.go:512](../../agent/loop.go#L512)）；`fail` 调 `seam` 删掉末尾未回答的助手工具调用消息（[agent/loop.go:641](../../agent/loop.go#L641)、[agent/loop.go:653](../../agent/loop.go#L653)）。
6. 唯一会提前写「待执行工具」的路径是人工审批暂停：`interrupt` 把 `PendingHITL` 与 `pendingExecutions` 一起提交（[agent/loop.go:601](../../agent/loop.go#L601)、[agent/loop.go:614](../../agent/loop.go#L614)），执行记录构造函数见 [agent/exectools.go:441](../../agent/exectools.go#L441)。

缺口：批次中途崩溃时头部检查点没有助手工具调用消息；重新发起调用会生成新调用编号，旧编号的副作用成为孤儿；没有一条记录把「哪些调用属于同一次执行」固定下来。

### 1.3 本计划要解决的问题

- **输入没有阶段。** 所有消息都在下一个 Phase 1 立刻投递，无法表达「这轮回答结束后再处理」。
- **临时消息回执不完整。** 调用方只能等 `Outcome()`，看不到「排队中」，也无法区分「已拒绝」与「还没结算」。
- **模型调用不可打断。** 用户明确要求停止当前生成时，只能等这次调用结束。
- **工具批次缺少执行前持久边界。** 崩溃后无法区分「已完成、可能已发生、从未开始」。
- **崩溃恢复会丢会话合法性。** 重问模型时助手调用消息可能悬空，或者结果与调用不配对。
- **在途与暂停混用会误报。** 若复用 `Pending`/`Pause`，界面会把「程序崩溃后等待恢复」显示成「等待人工审批」。

### 1.4 可观察行为变化

| 场景 | 现在 | 本计划之后 | 原因 |
| --- | --- | --- | --- |
| `Steer` / `Deliver` 的默认行为 | 下一个 Phase 1 立即投递 | 不变（等价于 `before_model`） | 空阶段规范化为 `before_model` |
| `next_turn` 的临时消息 | 无法表达 | 工具批次后的 Phase 1 保留，终稿后的 `settleTurn` 或下一轮投递 | 阶段是最晚投递时间 |
| 票据状态查询 | 只有 `Outcome()` 零值 | `State()` 返回 `queued`/`delivered`/`rejected` | 新增显式回执状态 |
| 同 ID、同消息、不同阶段的 `Deliver` | 判定为重复 | 判定为冲突并报错 | 去重同时比较阶段 |
| `SteerWith(Preempt=true)` 且抢占已开启、正在生成 | 不存在 | 取消当前这次模型调用，同一步重试；`ModelPreempted` + `InputDelivered{Preempted:true}` | 新增可选能力，默认关闭 |
| 提供方忽略取消、仍返回终稿 | 不存在 | 使用终稿，不重试；消息留在队列等下一次投递 | 成功优先 |
| 模型流结束但没有终稿且无错误 | 返回空响应并被当成成功 | 返回 `ok=false`，调用方报 `ErrModelNoFinal` | 修正既有行为 |
| 上下文已取消时的 `OnError` | 仍然调用 | 不再调用 | 取消不是提供方错误 |
| 持久化顶层运行的工具批次 | Phase 4 写一次检查点 | 执行前多写一次在途检查点，Phase 4 仍写 | 建立崩溃可恢复边界 |
| 批次执行中崩溃后 `Resume` | 重新问模型 | 按台账恢复原批次：`completed` 回放、`claimed`/`unknown` 返回结果未知、`pending` 执行 | 不重复副作用 |
| 头部检查点带 `InFlight` 时开新运行 | 无此状态 | `BeginRun` / `prepare` 返回 `ErrResumeRequired` | 必须先恢复 |
| `Fork` 一个在途检查点 | 无此状态 | 拒绝 | 台账不随分叉复制 |
| 批次中途失败的检查点内容 | 助手调用消息被 `seam` 删掉 | 每个计划调用补齐一条结果后再写 | 会话保持合法 |
| 工具密集步骤的观察者 | 每步一个快照 | 每步两个快照，其中一个是 `InFlight` 头 | 多一次执行前提交 |

### 1.5 边界

本计划不做四件事：不把四个阶段做成四个真实插入口；不让持久消息参与抢占；不实现自动恢复（崩溃后是否调用 `Resume` 仍由上层决定）；不改变工具台账状态机和 `ClaimTool` 语义。完整清单见 §9。

## 2. 已核实的事实

下表行号在基线 `9e5df25` 逐条核对过。执行时若行号漂移，按函数名和代码内容定位，不要按行号硬改。

### 2.1 输入侧（core 与 agent）

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `SteerOutcome` 字段（`Delivered`/`Step`/`Err`） | [agent/steering.go:27](../../agent/steering.go#L27) | 新增 `State` 字段的位置 |
| `SteerTicket` 含 `mu`、`settled`、`outcome`、`done` | [agent/steering.go:39](../../agent/steering.go#L39) | `State()` 由 `settled`+`outcome` 推导 |
| `Outcome()` 未结算返回零值 | [agent/steering.go:55](../../agent/steering.go#L55) | 保持兼容，不改此方法 |
| `settle` 已保证恰好结算一次 | [agent/steering.go:71](../../agent/steering.go#L71) | 新状态不需要额外锁 |
| `steerEntry{msg, ticket}` | [agent/steering.go:83](../../agent/steering.go#L83) | 加 `phase` 字段 |
| `push`、`drain`、`drainOrClose`、`close` | [agent/steering.go:100](../../agent/steering.go#L100)、[113](../../agent/steering.go#L113)、[127](../../agent/steering.go#L127)、[144](../../agent/steering.go#L144) | 新增 `pushWith`/`takeReady`/`takePhase`，保留这四个 |
| `settleDelivered` / `settleUndelivered` | [agent/steering.go:159](../../agent/steering.go#L159)、[166](../../agent/steering.go#L166) | 结算时写 `State` |
| `Run.Steer`、`Run.Cancel`、`drive` | [agent/run.go:146](../../agent/run.go#L146)、[152](../../agent/run.go#L152)、[233](../../agent/run.go#L233) | 新增 `Run.SteerWith`；`Cancel` 关闭原因不变 |
| `RunContext` 字段表（含 `resumed`、`replayBaseline`、`durable`、`steering`、`topLevel`、`dynamic`） | [agent/runtime.go:20](../../agent/runtime.go#L20) 到 [agent/runtime.go:60](../../agent/runtime.go#L60) | 新增 `continuedTurn`、抢占登记字段、`inFlight` |
| `deeper`/`subRun`/`forBranch` 逐字段构造、不复制 `steering` | [agent/runtime.go:65](../../agent/runtime.go#L65)、[84](../../agent/runtime.go#L84)、[106](../../agent/runtime.go#L106) | 派生上下文不需要新的队列指针 |
| `RunContext.Steer` | [agent/runtime.go:128](../../agent/runtime.go#L128) | 新增 `SteerWith` |
| `RunContext.publish` 是单 goroutine 事件出口 | [agent/runtime.go:131](../../agent/runtime.go#L131) | 抢占登记需要独立锁，事件仍走 `publish` |
| `Agent.Deliver` 校验与委托 | [agent/inbox.go:15](../../agent/inbox.go#L15) | 新增 `DeliverWith` |
| `config` 结构 | [agent/options.go:16](../../agent/options.go#L16) | 新增 `inputPreemption`、`inFlightCheckpoint` |
| `New` 与 `newRunHandleWithID`（`topLevel: a.loop != nil`） | [agent/agent.go:37](../../agent/agent.go#L37)、[agent/agent.go:236](../../agent/agent.go#L236) | 默认值种在这里；配置复制到 `AgentLoop` |
| `taskState{step,maxStep,history,system,usePrompt}` | [agent/loop.go:90](../../agent/loop.go#L90) 到 [agent/loop.go:96](../../agent/loop.go#L96) | 新增 `newTurn` |
| `prepareRun` 返回 `taskState` | [agent/loop.go:115](../../agent/loop.go#L115)、[agent/loop.go:181](../../agent/loop.go#L181) | `newTurn` 初值来源 |
| `runStep` Phase 1 两次读取 | [agent/loop.go:249](../../agent/loop.go#L249)、[259](../../agent/loop.go#L259)、[265](../../agent/loop.go#L265) | 换成逻辑输入队列 |
| Phase 2 请求组装与冻结 | [agent/loop.go:300](../../agent/loop.go#L300)、[305](../../agent/loop.go#L305)、[308](../../agent/loop.go#L308)、[311](../../agent/loop.go#L311) | 抢占重试复用同一重建方式 |
| 超限恢复循环（`attempts`、`retryReq`、中途检查点 `Release:false`） | [agent/loop.go:330](../../agent/loop.go#L330) 到 [agent/loop.go:393](../../agent/loop.go#L393) | 抢占分支插进同一个 `for` |
| 超限恢复请求重建 | [agent/loop.go:355](../../agent/loop.go#L355) 到 [agent/loop.go:360](../../agent/loop.go#L360) | 抢占重试照此形状 |
| 终稿后 `settleTurn` 与续跑 | [agent/loop.go:433](../../agent/loop.go#L433)、[438](../../agent/loop.go#L438)、[447](../../agent/loop.go#L447) | 阶段投递与票据结算 |
| `max_tokens` 截断路径继续下一步 | [agent/loop.go:463](../../agent/loop.go#L463) 到 [agent/loop.go:477](../../agent/loop.go#L477) | 该路径 `newTurn=false` |
| `settleTurn` 全文 | [agent/loop.go:210](../../agent/loop.go#L210) 到 [agent/loop.go:244](../../agent/loop.go#L244) | 预算耗尽先拒后关 |
| `streamModel` 无条件返回 `ok=true` | [agent/loop.go:546](../../agent/loop.go#L546)、[550](../../agent/loop.go#L550)、[565](../../agent/loop.go#L565) | 改成 `gotFinal` |
| `commitOptions` 字段 | [agent/loop.go:571](../../agent/loop.go#L571) 到 [agent/loop.go:578](../../agent/loop.go#L578) | 新增 `InFlight` |
| `checkpoint` 把 `commitOptions` 拼成 `Checkpoint` | [agent/loop.go:583](../../agent/loop.go#L583) 到 [agent/loop.go:599](../../agent/loop.go#L599) | 传递新字段 |
| `interrupt` 写 `Pending`+`Pause`+执行记录 | [agent/loop.go:601](../../agent/loop.go#L601) 到 [agent/loop.go:625](../../agent/loop.go#L625) | 在途不复用它 |
| `fail` 与 `seam` | [agent/loop.go:636](../../agent/loop.go#L636)、[641](../../agent/loop.go#L641)、[653](../../agent/loop.go#L653) | 失败路径补齐后再 `fail` |
| `runOutcome` 与 `inputCloseReason` | [agent/runnable.go:22](../../agent/runnable.go#L22)、[46](../../agent/runnable.go#L46) | 抢占不新增结束原因 |

### 2.2 工具与恢复侧（agent）

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `execTools` 要求 `lc.view != nil` | [agent/exectools.go:42](../../agent/exectools.go#L42)、[43](../../agent/exectools.go#L43) | 恢复必须先重建视图再调用 |
| `run(i,c)` 内的写权限检查与 `toolOutcome` 汇总 | [agent/exectools.go:68](../../agent/exectools.go#L68) 到 [agent/exectools.go:147](../../agent/exectools.go#L147) | 计数点 |
| 完成记录写内存台账 | [agent/exectools.go:128](../../agent/exectools.go#L128) | 恢复提交时随检查点落盘 |
| 分段入口 | [agent/exectools.go:149](../../agent/exectools.go#L149) | 恢复批次同样分段 |
| `reportedToolResults` 按顺序压缩 | [agent/exectools.go:186](../../agent/exectools.go#L186) | 失败补齐需要「已报告」集合 |
| `toolOutcome` 字段（`skipAfter`、`execution`、`rejection`、`noResult`） | [agent/exectools.go:264](../../agent/exectools.go#L264) 到 [agent/exectools.go:277](../../agent/exectools.go#L277) | 新增 `class` 字段 |
| `callOne` 的广播/解析/参数检查 | [agent/exectools.go:310](../../agent/exectools.go#L310) 到 [agent/exectools.go:346](../../agent/exectools.go#L346) | 拒绝计数 |
| `ClaimTool` 四种结果分支 | [agent/exectools.go:351](../../agent/exectools.go#L351) 到 [agent/exectools.go:379](../../agent/exectools.go#L379) | 恢复语义完全复用 |
| handler 调用 | [agent/exectools.go:399](../../agent/exectools.go#L399) | 执行计数 |
| `toolInvocation`（线程+步骤+调用 ID+工具名+参数哈希） | [agent/exectools.go:426](../../agent/exectools.go#L426) | 恢复同步骤才能命中旧编号 |
| `pendingExecutions` 生成 `pending` 台账 | [agent/exectools.go:441](../../agent/exectools.go#L441) | 在途提交直接复用 |
| `toolAttempt`、`resultFromExecution`、`unknownToolResult`、`rejected`、`errResult` | [agent/exectools.go:459](../../agent/exectools.go#L459)、[466](../../agent/exectools.go#L466)、[479](../../agent/exectools.go#L479)、[490](../../agent/exectools.go#L490)、[497](../../agent/exectools.go#L497) | 失败补齐复用，无需新写结果构造 |
| `Agent.Resume` 主体（Latest→validatePause→preflight→Claim→克隆状态→建句柄） | [agent/hitl.go:79](../../agent/hitl.go#L79) 到 [agent/hitl.go:208](../../agent/hitl.go#L208) | 在途分支插在这里 |
| `validatePause` 只认 `RecoveryReplayModel`/`RecoveryResumeTools` | [agent/hitl.go:210](../../agent/hitl.go#L210) 到 [agent/hitl.go:227](../../agent/hitl.go#L227) | 在途没有 `Pause`，不经此函数 |
| `preflightResumeTools` 的形状（广播名 + 当前静态表） | [agent/hitl.go:236](../../agent/hitl.go#L236) 到 [agent/hitl.go:258](../../agent/hitl.go#L258) | 在途预检照此形状 |
| `resumeBatch` 字段与 `newResumeBatch` | [agent/hitl.go:262](../../agent/hitl.go#L262) 到 [agent/hitl.go:283](../../agent/hitl.go#L283) | 新增并列的 `inFlightBatch` |
| `runResumed` 重新经过 `BeforeTool` | [agent/hitl.go:301](../../agent/hitl.go#L301) | 在途恢复不重跑，二者不共用 |
| `lastAssistant` | [agent/hitl.go:383](../../agent/hitl.go#L383) | 在途匹配助手消息可复用 |
| `durableStore`、`addInboxAcks`、`pendingInboxAcks`、`addCompletedTool` | [agent/durable_runtime.go:83](../../agent/durable_runtime.go#L83)、[98](../../agent/durable_runtime.go#L98)、[125](../../agent/durable_runtime.go#L125)、[139](../../agent/durable_runtime.go#L139) | 输入队列与恢复执行都用现成方法 |
| `commitCheckpoint` / `commitCheckpointLocked`（确认、占用、执行记录一起提交；取消后兜底） | [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339)、[355](../../agent/durable_runtime.go#L355)、[366](../../agent/durable_runtime.go#L366)、[392](../../agent/durable_runtime.go#L392) | 在途提交不需要改这里的协议 |
| `Run.prepare` 协调器分支与非协调器 `restore` 分支 | [agent/run_prepare.go:23](../../agent/run_prepare.go#L23) 到 [agent/run_prepare.go:76](../../agent/run_prepare.go#L76)，非协调分支见 [agent/run_prepare.go:59](../../agent/run_prepare.go#L59) 到 [agent/run_prepare.go:63](../../agent/run_prepare.go#L63) | 非协调存储要新增在途检查（`latest` 只在 `restoreCurrent` 分支可得） |
| `RunStatus` 的 `Paused` 计算 | [agent/run_status.go:66](../../agent/run_status.go#L66) | 保持表达式，另加 `InFlight` |
| `buildTerminalRecord` 拒绝 `Pause`/`Pending` | [agent/terminal.go:161](../../agent/terminal.go#L161)、[176](../../agent/terminal.go#L176) | 增加拒绝 `InFlight` |
| `StepView.snapshot()` | [agent/stepview.go:168](../../agent/stepview.go#L168) | 在途提交带快照 |
| `resumeStepView(rb *resumeBatch)`：从快照复制摘要、只查静态表 | [agent/stepview.go:340](../../agent/stepview.go#L340) 到 [agent/stepview.go:383](../../agent/stepview.go#L383) | 需抽出一个接受 `(step, calls, snapshot)` 的版本 |
| `freezeStepView` 不调用任何中间件，只重读工具表并校验 schema | [agent/stepview.go:388](../../agent/stepview.go#L388) 到 [agent/stepview.go:429](../../agent/stepview.go#L429) | 抢占重试可以调用它 |
| `checkReplayDrift` 消费基线 | [agent/stepview.go:437](../../agent/stepview.go#L437) | 在途恢复不设基线 |
| `toolDigest` | [agent/stepview.go:511](../../agent/stepview.go#L511) | 在途预检要自己算当前摘要 |
| `ToolView` 的 `Schemas`/`Advertised`/`Resolve`/`knownName` | [agent/stepview.go:201](../../agent/stepview.go#L201)、[217](../../agent/stepview.go#L217)、[230](../../agent/stepview.go#L230)、[249](../../agent/stepview.go#L249) | 预检与补齐的判断依据 |
| `LoopContext` 字段（`view`、`approved`，无统计字段） | [agent/loopctx.go:13](../../agent/loopctx.go#L13) 到 [agent/loopctx.go:22](../../agent/loopctx.go#L22) | 需新增未导出的 `stats *batchStats` |
| `checkWriteAccess` | [agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13) | 在途写入前必须先查 |
| `RunResult.Paused` 只由中断结果填充 | [agent/result.go:22](../../agent/result.go#L22) 到 [agent/result.go:41](../../agent/result.go#L41) | 在途不改它，队列不会误报暂停 |

### 2.3 存储侧（checkpoint、core、llm、queue）

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `Durable` 接口（`Deliver`/`Inbox`/`Claim`/`ClaimTool`/`ToolExecution`/`Commit`/`ReleaseClaim`/`Decisions`） | [checkpoint/durable.go:15](../../checkpoint/durable.go#L15)、读取见 [29](../../checkpoint/durable.go#L29) | 预扫描用现成的 `ToolExecution` |
| `InboxMessage`、`DeliveryReceipt` | [checkpoint/durable.go:60](../../checkpoint/durable.go#L60)、[65](../../checkpoint/durable.go#L65) | 加 `Phase`、`State` |
| `CommitRequest`、`ToolExecution` 与四个台账状态 | [checkpoint/durable.go:95](../../checkpoint/durable.go#L95)、[124](../../checkpoint/durable.go#L124)、[116](../../checkpoint/durable.go#L116) 到 [118](../../checkpoint/durable.go#L118) | 在途提交沿用 |
| 内存 `Deliver`（同 ID 冲突报错，无 sentinel）、`Inbox` 插入序 | [checkpoint/durable_memory.go:65](../../checkpoint/durable_memory.go#L65)、[87](../../checkpoint/durable_memory.go#L87) | 阶段校验加在这里 |
| `sameInboxMessage` 只比较消息体 | [checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419) | 增加规范化阶段比较 |
| 文件 `Deliver`、`Inbox`、`ClaimTool`、`ToolExecution` | [checkpoint/file.go:183](../../checkpoint/file.go#L183)、[211](../../checkpoint/file.go#L211)、[438](../../checkpoint/file.go#L438)、[516](../../checkpoint/file.go#L516) | 与内存语义对齐 |
| `validateJournalRecord` 的 `recordInboxDeliver` / `recordCommit` 分支 | [checkpoint/file.go:743](../../checkpoint/file.go#L743) 到 [checkpoint/file.go:776](../../checkpoint/file.go#L776) | 校验阶段与在途字段 |
| `Checkpoint` 字段顺序（`Pending`、`Pause`、`Snapshot`、`Completion`） | [checkpoint/checkpoint.go:45](../../checkpoint/checkpoint.go#L45)、`Pause` 见 [65](../../checkpoint/checkpoint.go#L65) | `InFlight` 放在 `Pause` 之后 |
| `PendingHITL`、`Pause`、`EffectivePauseID`（无 `Pause` 时为 `checkpoint-<id>`） | [checkpoint/checkpoint.go:123](../../checkpoint/checkpoint.go#L123)、[130](../../checkpoint/checkpoint.go#L130)、[144](../../checkpoint/checkpoint.go#L144) | 在途用同一个 `EffectivePauseID` 做认领 |
| `StepSnapshot` 字段（`Step`、`Advertised`、`ToolDigest`、`SettingsDigest`、`Model`） | [checkpoint/checkpoint.go:36](../../checkpoint/checkpoint.go#L36) 到 [checkpoint/checkpoint.go:42](../../checkpoint/checkpoint.go#L42) | 预检依据 |
| `freezeCheckpoint` 已调用 `validateLineage` | [checkpoint/snapshot.go:13](../../checkpoint/snapshot.go#L13)、[18](../../checkpoint/snapshot.go#L18) | `validateWorkState` 加在同一处 |
| `cloneCheckpoint` 逐字段深拷贝 | [checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30) 到 [checkpoint/snapshot.go:48](../../checkpoint/snapshot.go#L48) | 新增 `cloneInFlight` |
| `cloneSnapshotBytes` 已存在 | [checkpoint/snapshot.go:127](../../checkpoint/snapshot.go#L127) | 拷贝 `ToolCall.Args` |
| `commitDigestCheckpoint` 的可省略字段策略 | [checkpoint/coordinator.go:42](../../checkpoint/coordinator.go#L42) 到 [checkpoint/coordinator.go:63](../../checkpoint/coordinator.go#L63) | 新字段必须 `omitempty` |
| `commitDigestToolCall` 已存在 | [checkpoint/coordinator.go:99](../../checkpoint/coordinator.go#L99) | 在途摘要直接复用 |
| `commitRequestDigest` | [checkpoint/coordinator.go:137](../../checkpoint/coordinator.go#L137) | 接入新字段 |
| `ErrResumeRequired` 文案是 "paused thread requires resume" | [checkpoint/coordinator.go:18](../../checkpoint/coordinator.go#L18) | 改为不特指暂停 |
| 两个 `BeginRun` 的拒绝条件 | [checkpoint/coordinator_memory.go:26](../../checkpoint/coordinator_memory.go#L26)、[checkpoint/coordinator_file.go:23](../../checkpoint/coordinator_file.go#L23) | 换成 `RequiresResume(head)` |
| `Fork` 及其清空 `Pending`/`Pause`/`Completion` | [checkpoint/branch.go:17](../../checkpoint/branch.go#L17)、[45](../../checkpoint/branch.go#L45) | 增加在途拒绝 |
| `CheckpointMetadata` 与 `checkpointMetadataOf` | [checkpoint/query.go:54](../../checkpoint/query.go#L54)、[150](../../checkpoint/query.go#L150)、`Paused` 见 [158](../../checkpoint/query.go#L158) | 新增 `InFlight` 投影 |
| `metaFields` 用 `*json.RawMessage` 只问存在 | [checkpoint/metadata_index.go:60](../../checkpoint/metadata_index.go#L60) 到 [checkpoint/metadata_index.go:76](../../checkpoint/metadata_index.go#L76) | 新增 `in_flight` |
| `projectJournalLine` 与 `summaryPauseMarker` | [checkpoint/metadata_index.go:409](../../checkpoint/metadata_index.go#L409)、[450](../../checkpoint/metadata_index.go#L450)、[469](../../checkpoint/metadata_index.go#L469) | 新增 `summaryInFlightMarker` |
| 事件类型清单与 marker 区 | [core/event.go:202](../../core/event.go#L202) 起 | 新事件加 marker |
| `eventWire` 已有 `Step`、`Phase`、`Count`、`Reason` | [core/eventjson.go:18](../../core/eventjson.go#L18) 到 [core/eventjson.go:43](../../core/eventjson.go#L43) | 复用；新增 `source`、`message_ids`、`preempted`、`emitted`、四个计数 |
| `MarshalEvent` 把调用列表包成助手消息（`ToolStarted`） | [core/eventjson.go:61](../../core/eventjson.go#L61)、[84](../../core/eventjson.go#L84) | `ToolBatchInFlight` 同样传输 |
| `UnmarshalEvent` | [core/eventjson.go:134](../../core/eventjson.go#L134) | 新增四个分支 |
| `Message.ToolCalls()` | [core/message.go:112](../../core/message.go#L112) | 解码调用列表 |
| `llm.Model.Generate` 只有上下文可取消这一种中止手段 | [llm/model.go:23](../../llm/model.go#L23) | 抢占用 `context.WithCancel` |
| 队列把 `detail.Paused` 映射成 `StatusPaused` | [queue/run_worker.go:286](../../queue/run_worker.go#L286)、[344](../../queue/run_worker.go#L344)、[360](../../queue/run_worker.go#L360) | 在途不设 `Paused`，只需回归测试 |

### 2.4 现成测试夹具

| 夹具 | 位置 | 用途 |
| --- | --- | --- |
| `steeringBlockingTool`（阻塞工具） | [agent/steering_test.go:20](../../agent/steering_test.go#L20) | 在工具阶段投递消息 |
| `turnGatedModel` + `modelGate`（可按次阻塞的模型，记录每次请求） | [agent/task_loop_test.go:23](../../agent/task_loop_test.go#L23)、[38](../../agent/task_loop_test.go#L38) | 抢占与投递时序 |
| `countingInboxStore`、`blockingInboxStore`、`failingInboxStore` | [agent/task_loop_test.go:317](../../agent/task_loop_test.go#L317)、[371](../../agent/task_loop_test.go#L371)、[510](../../agent/task_loop_test.go#L510) | 收件箱阶段过滤与读取失败 |
| `claimRecordingStore`（记录每次 `Commit` 与 `ReleaseClaim`） | [agent/task_loop_test.go:582](../../agent/task_loop_test.go#L582) | 在途提交的 `Release:false` 断言 |
| `phase1FailOnSaveStore`（按第 N 次 `Save` 注入失败） | [agent/phase1_failure_test.go:21](../../agent/phase1_failure_test.go#L21) | 非持久存储路径的提交失败 |
| `viewShapeTool`（可控 schema 的工具） | [agent/stepview_test.go:325](../../agent/stepview_test.go#L325) | 冻结失败与摘要漂移 |
| `gateOnce`、`stopTool` | [agent/resume_exec_test.go:22](../../agent/resume_exec_test.go#L22)、[211](../../agent/resume_exec_test.go#L211) | 审批与终止指令 |
| `overflowError`、`perMessageCounter` | [agent/overflow_test.go](../../agent/overflow_test.go)、[agent/overflow_test.go:801](../../agent/overflow_test.go#L801) | 超限恢复与抢占并发 |
| `mock.New`/`mock.Text`/`mock.CallTool`/`mock.Partial` | [llm/mock/mock.go:27](../../llm/mock/mock.go#L27)、[48](../../llm/mock/mock.go#L48)、[57](../../llm/mock/mock.go#L57)、[68](../../llm/mock/mock.go#L68) | 脚本化模型 |
| `newFileRecoveryStore`（重开同一目录的文件存储） | [checkpoint/file_recovery_test.go:653](../../checkpoint/file_recovery_test.go#L653) | 在途记录的跨进程断言 |
| `TestSnapshotConsistencyBeginRunPaused` 等 BeginRun 契约测试 | [checkpoint/coordinator_contract_test.go:102](../../checkpoint/coordinator_contract_test.go#L102) | 在途拒绝条件加在同一族 |
| `TestLegacyCheckpointDigestUnchanged` | [checkpoint/stepview_test.go:153](../../checkpoint/stepview_test.go#L153) | 旧提交摘要不变的断言放同一族 |
| `TestSnapshotConsistencyForkDropsPause` | [checkpoint/fork_consistency_test.go:194](../../checkpoint/fork_consistency_test.go#L194) | `Fork` 拒绝在途的对照 |

测试包名注意：`checkpoint/coordinator_contract_test.go`、`checkpoint/metadata_contract_test.go`、`checkpoint/file_recovery_test.go` 是包内测试（`package checkpoint`）；`checkpoint/durable_test.go`、`checkpoint/memory_test.go`、`checkpoint/fork_consistency_test.go`、`checkpoint/stepview_test.go`、`checkpoint/snapshot_contract_test.go`、`checkpoint/file_test.go` 是外部测试（`package checkpoint_test`）。`agent/steering_internal_test.go` 是包内测试，`agent/steering_test.go`、`agent/task_loop_test.go` 是外部测试。新测试按需要放对包，内部方法（`takeReady`、`pushWith`、`validateWorkState`）只能在包内测试里调用。

### 2.5 对设计稿的按当前代码修正

设计稿 §5、§6 的伪代码假设了四处并不存在的代码，实施时按本节口径落地，其余保持设计稿原样。

| 设计稿写法 | 当前代码事实 | 本计划的做法 |
| --- | --- | --- |
| §6.3.4 `rlc.batchStats = &batchStats{}`；§6.3.6「计数由 `batchStats` 在 `callOne` 中累加」 | `LoopContext` 没有 `stats` 字段（[agent/loopctx.go:13](../../agent/loopctx.go#L13)），`execTools` 与 `callOne` 现在不做任何分类计数 | 新增 `batchStats` 类型和 `LoopContext.stats *batchStats`（nil 时不计数）；分类信息加在 `toolOutcome.class` 上，由 `callOne` 的各个返回点填写，`run(i,c)` 汇总。见 §4.10 |
| §6.3.3 `view := l.resumeStepViewFor(cp.Step, cp.InFlight.Calls, cp.Snapshot)` | 现有一致函数是 `resumeStepView(rb *resumeBatch)`（[agent/stepview.go:340](../../agent/stepview.go#L340)） | 抽出 `resumeStepViewFor(step int, calls []core.ToolCall, snapshot *checkpoint.StepSnapshot) *StepView`，`resumeStepView` 改成薄封装并委托它，避免两套视图重建逻辑 |
| §6.2.1 伪代码里的 `cloneToolCalls(calls)` | `agent` 包没有该函数 | 在 `agent/inflight.go` 新增 `cloneToolCalls`，逐条复制 `Args` 字节；不复用 `pendingExecutions` 的输入，避免在途记录与台账记录的参数来源不同 |
| §5.2.4 伪代码里的 `rc.publishDeliveredInputs(...)` | 不存在 | 新增 `agent/input.go` 内的两个小 helper（临时/持久各一），职责仅是组装并发布 `core.InputDelivered`；不引入新的发布通道，仍用 `rc.publish` |

另有两点不是缺口、但需要执行时确认理解一致：

- 设计稿 §5.4.4 说「抢占重试不重新执行 `freezeStepView` 的中间件」。当前 `freezeStepView` 本身不调用任何中间件（[agent/stepview.go:388](../../agent/stepview.go#L388) 到 [agent/stepview.go:429](../../agent/stepview.go#L429)），它重读工具表并校验 schema。抢占重试**要**调用它（与超限恢复一致，[agent/loop.go:361](../../agent/loop.go#L361)），不重跑的是 `BeforeModel`、`ModifyRequest`、`CompactHistory`。
- 收件箱 ID 冲突当前没有 sentinel 错误，只有格式化文本（[checkpoint/durable_memory.go:78](../../checkpoint/durable_memory.go#L78)、[checkpoint/file.go:195](../../checkpoint/file.go#L195)）。本计划保持不加 sentinel，测试断言用「错误非 nil 且包含该 ID」，避免把内部文本变成契约。

## 3. 锁定的设计决定

以下决定来自设计稿，本计划不重新讨论；标「本计划补充」的是设计稿留白、实施时必须固定的口径。

### D1 阶段是最晚投递时间，不是插入口

四个阶段只有两个物理投递点：Phase 1（`runStep` 开头，[agent/loop.go:249](../../agent/loop.go#L249)）和 `settleTurn`（[agent/loop.go:210](../../agent/loop.go#L210)）。`after_model` / `after_tools` 在 `newTurn=false` 的 Phase 1 投递即视为满足；`next_turn` 只在 `newTurn=true` 的 Phase 1 或 `settleTurn` 投递。消息可以早于阶段投递，不能晚于阶段允许的时间。

### D2 阶段是闭集，非法值直接拒绝

`NormalizeInputPhase` 把空值转成 `before_model`，未知值返回 `core.ErrUnknownInputPhase`。入队、`DeliverWith`、文件日志校验三处都拒绝，不猜测、不降级。

### D3 临时消息先于持久消息，同一来源先进先出

保持 [agent/loop.go:259](../../agent/loop.go#L259) 与 [agent/loop.go:265](../../agent/loop.go#L265) 的现有顺序。不按阶段重排，不保证跨通道全局顺序。

### D4 `newTurn` 由循环显式携带，读取点不自行推断

`taskState.newTurn` 一个字段决定阶段过滤。取值：全新运行 `true`；`Agent.Resume` 产生的运行 `false`；`settleTurn` 续跑 `true`；工具批次后、`max_tokens` 后、审批恢复批次后、在途批次后 `false`。

**本计划补充：** 恢复运行用 `RunContext.continuedTurn bool` 传递，由 `Agent.Resume` 设置，`prepareRun` 读一次后即视为已消费；`Resume` 的两种分支（有审批批次、无审批批次）都设置它。

### D5 抢占默认关闭，只作用于临时 `before_model` 消息，每步最多一次

`WithInputPreemption(true)` 才生效。`Preempt=true` 且阶段不是 `before_model` → `ErrPreemptionPhase`；未开启 → `ErrPreemptionDisabled`；没有活动调用或本步已抢占过 → 按普通 `before_model` 入队且不报错。持久消息不参与抢占。

### D6 成功优先、运行取消优先

拿到终稿就用终稿，不重试（[agent/loop.go:337](../../agent/loop.go#L337) 的 `err == nil` 判断在抢占判断之前）。`rc.Context.Err() != nil` 时不进入抢占重试。判断顺序固定为：`ok` → 运行取消 → 抢占 → 超限恢复 → 失败。

### D7 抢占重试不重跑请求侧中间件，但重新冻结视图

不重跑 `BeforeModel`、`ModifyRequest`、`CompactHistory`；请求从当前视图的 `System`/`Tools`/`Options` 加新 `Messages` 重建，与超限恢复同形（[agent/loop.go:355](../../agent/loop.go#L355)）。重新调用 `freezeStepView`（[agent/stepview.go:388](../../agent/stepview.go#L388)）和 `ModelContext`。抢占重试写一个 `Release:false` 的中间检查点，步骤号不变。

### D8 `streamModel` 用 `gotFinal` 决定 `ok`

没有终稿时返回 `ok=false`；上下文已取消时不再调用 `OnError`（现在无条件调用，[agent/loop.go:553](../../agent/loop.go#L553)）。两个行为变化都写进 §8.2。

### D9 票据同时提供 `Outcome()` 与 `State()`

`SteerOutcome` 增加 `State` 字段，`SteerTicket.State()` 在未结算时返回 `queued`。`Outcome()` 未结算仍返回零值（[agent/steering.go:55](../../agent/steering.go#L55) 的语义不变）。参数错误返回 `nil` 票据加错误，不产生 `rejected` 票据。

### D10 持久消息没有「已消费」同步回执

`DeliveryReceipt.State` 恒为 `queued`。消费由两件事共同观察：ID 从 `Inbox` 消失，以及事件流出现带该 ID 的 `InputDelivered`。语义是至少一次。

### D11 收件箱去重同时比较规范化后的阶段

`sameInboxMessage`（[checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419)）比较消息体和阶段。同 ID、同消息、不同阶段是冲突。冲突错误保持现有文本风格，不新增 sentinel（见 §2.5）。

### D12 在途检查点是台账边界，不是暂停

`InFlight` 与 `Pending`、`Pause`、`Completion` 互斥，校验放在 `freezeCheckpoint`（[checkpoint/snapshot.go:13](../../checkpoint/snapshot.go#L13)）里，而不是只放在循环里。`RunStatus.Paused` 表达式不变（[agent/run_status.go:66](../../agent/run_status.go#L66)），新增 `RunStatus.InFlight`。恢复入口是 `Agent.Resume`，且拒绝显式审批参数。

### D13 在途写入位置固定

全部执行前检查（`CheckToolPolicy` [agent/loop.go:483](../../agent/loop.go#L483)、`BeforeTool` [agent/loop.go:486](../../agent/loop.go#L486)）之后、`execTools`（[agent/loop.go:507](../../agent/loop.go#L507)）之前，并且先过 `checkWriteAccess`（[agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13)）。提交失败就不启动任何 handler，直接走 `fail`。

### D14 只对持久化且顶层的运行写

条件：`rc.topLevel` 为真（[agent/runtime.go:54](../../agent/runtime.go#L54)）、存储实现 `checkpoint.Durable`（[agent/durable_runtime.go:83](../../agent/durable_runtime.go#L83)）、配置开关开启。子代理、分支、非持久 `Checkpointer`、审批恢复批次都不写。默认开启。

### D15 恢复不重跑执行前检查，只重放已批准的批次

恢复执行不调用 `CheckToolPolicy`、不调用 `BeforeTool`；`AfterTool` 仍然运行，但 `completed` 回放因为 `skipAfter`（[agent/exectools.go:275](../../agent/exectools.go#L275)）不运行它，与现有幂等回放一致。

### D16 恢复在同一逻辑步骤执行

`toolInvocation` 含步骤号（[agent/exectools.go:426](../../agent/exectools.go#L426)），只有用 `ib.step` 才能命中旧台账记录。恢复后下一步从 `ib.step + 1` 开始（与审批批次恢复的现有做法一致，[agent/loop.go:160](../../agent/loop.go#L160)）。

### D17 台账判断决定摘要检查强度

先只读扫描每个调用的台账状态（[checkpoint/durable.go:29](../../checkpoint/durable.go#L29)）：任一调用为 `pending` 或记录不存在 → 可能执行，必须严格比较当前工具摘要与保存摘要；全部为 `completed`/`claimed`/`unknown` → 不会启动 handler，跳过摘要比较。读取失败直接拒绝恢复。

### D18 在途与助手消息只按 ID、名称、顺序匹配

`BeforeTool` 可改写参数（[agent/loop.go:487](../../agent/loop.go#L487)），所以助手消息里是模型原始参数，`InFlight.Calls` 里是改写后的参数，不逐字节比较。调用编号与 handler 一律用 `InFlight.Calls`。

### D19 批次中途失败先补齐再 `fail`

写过在途检查点的批次失败时，为每个计划调用补齐一条结果（已完成回放、可能已发生返回未知、从未开始返回未执行），使 `seam`（[agent/loop.go:653](../../agent/loop.go#L653)）不会删除助手调用消息。补齐读取用 `context.WithoutCancel`。

### D20 冻结视图是恢复时的权威

恢复用 `cp.Snapshot` 的广播名与摘要重建视图（现有 `resumeStepView` 的做法，[agent/stepview.go:340](../../agent/stepview.go#L340)），实现只从当前静态表解析；运行时注入的工具无法恢复，必须拒绝。当前摘要要另调 `toolDigest`（[agent/stepview.go:511](../../agent/stepview.go#L511)）计算，不能读复制过快照的视图字段。

### D21 新字段一律可省略

`Checkpoint.InFlight`、`InboxMessage.Phase`、`CheckpointMetadata.InFlight`、提交摘要子结构、`eventWire` 新字段全部 `omitempty`。旧记录读回零值即现有行为，旧提交摘要字节不变（依据 [checkpoint/coordinator.go:52](../../checkpoint/coordinator.go#L52) 的既有注释口径）。

### D22 `RequiresResume` 是语义判断，不等于「已暂停」

`Pending`、`Pause`、`InFlight` 任一存在即需要先恢复。`BeginRun` 两处（[checkpoint/coordinator_memory.go:26](../../checkpoint/coordinator_memory.go#L26)、[checkpoint/coordinator_file.go:23](../../checkpoint/coordinator_file.go#L23)）、非协调存储的 `prepare`（[agent/run_prepare.go:61](../../agent/run_prepare.go#L61)）、`Fork`（[checkpoint/branch.go:17](../../checkpoint/branch.go#L17)）、`buildTerminalRecord`（[agent/terminal.go:176](../../agent/terminal.go#L176)）都用它或直接用 `InFlight != nil`。`ErrResumeRequired` 文案改为不特指暂停（[checkpoint/coordinator.go:18](../../checkpoint/coordinator.go#L18)）。

### D23 事件不替代检查点

`ToolBatchInFlight` 在提交成功后发布，`ToolBatchRecovered` 在恢复提交成功后发布，`ModelPreempted` 与 `InputDelivered{Preempted:true}` 按设计稿顺序发布。崩溃恢复只读存储。

## 4. 契约

代码骨架如下，命名与本节一致；任务卡按本节实现，不再自行发明名字。

### 4.1 `core/input.go`（新文件）

```go
package core

import (
	"errors"
	"fmt"
)

// InputPhase is the latest point at which an input may enter a run. The empty
// value means InputBeforeModel.
type InputPhase string

const (
	InputBeforeModel InputPhase = "before_model"
	InputAfterModel  InputPhase = "after_model"
	InputAfterTools  InputPhase = "after_tools"
	InputNextTurn    InputPhase = "next_turn"
)

// InputSource records which physical channel an input arrived on.
type InputSource string

const (
	InputVolatile InputSource = "volatile"
	InputDurable  InputSource = "durable"
)

// InputState is the delivery receipt state of one input.
type InputState string

const (
	InputQueued    InputState = "queued"
	InputDelivered InputState = "delivered"
	InputRejected  InputState = "rejected"
)

var ErrUnknownInputPhase = errors.New("core: unknown input phase")

// NormalizeInputPhase maps the empty value to InputBeforeModel and refuses
// anything outside the closed set.
func NormalizeInputPhase(p InputPhase) (InputPhase, error) { ... }
```

### 4.2 队列与票据（`agent/steering.go`、`agent/run.go`、`agent/runtime.go`）

```go
type SteerOptions struct {
	Phase   core.InputPhase
	Preempt bool
	Reason  string
}

type SteerOutcome struct {
	Delivered bool
	Step      int
	Err       error
	State     core.InputState // delivered / rejected after settlement
}

func (t *SteerTicket) State() core.InputState // queued while unsettled

type steerEntry struct {
	msg    core.Message
	ticket *SteerTicket
	phase  core.InputPhase
}

func (q *steeringQueue) push(msg core.Message) (*SteerTicket, error)      // 委托 pushWith，保持不变
func (q *steeringQueue) pushWith(msg core.Message, opts SteerOptions) (*SteerTicket, error)
func (q *steeringQueue) drain() []steerEntry                              // 保持不变
func (q *steeringQueue) takeReady(newTurn bool) []steerEntry              // newTurn=false 时保留 next_turn
func (q *steeringQueue) takePhase(phase core.InputPhase) []steerEntry     // 抢占只取 before_model
```

`takeReady` / `takePhase` 在同一把锁内完成「筛选 + 删除」，剩余条目相对顺序不变。`settleDelivered` 写 `State: core.InputDelivered`，`settleUndelivered` 写 `State: core.InputRejected`。

公开入口：

```go
func (r *Run) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error)
func (r *Run) Steer(msg core.Message) (*SteerTicket, error)                 // = SteerWith(msg, SteerOptions{})
func (rc *RunContext) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error)
func (rc *RunContext) Steer(msg core.Message) (*SteerTicket, error)         // 保持不变
```

`pushWith` 的校验顺序：队列已关闭 → 阶段规范化 → `Preempt` 且阶段不是 `before_model` → `Preempt` 且配置未开启。前两类返回错误票据为 `nil`。

### 4.3 收件箱阶段（`checkpoint/durable.go`、两个存储、`agent/inbox.go`）

```go
type InboxMessage struct {
	ID      string          `json:"id"`
	Message core.Message    `json:"message"`
	Phase   core.InputPhase `json:"phase,omitempty"`
}

type DeliveryReceipt struct {
	ID        string
	Duplicate bool
	State     core.InputState // 恒为 InputQueued
}

func (a *Agent) DeliverWith(ctx context.Context, threadID string, msg checkpoint.InboxMessage,
	phase core.InputPhase) (checkpoint.DeliveryReceipt, error)
func (a *Agent) Deliver(...)  // = DeliverWith(..., core.InputBeforeModel)
```

`Memory.Deliver`、`File.Deliver` 在入口调用 `core.NormalizeInputPhase` 并把规范化结果写入存储；`validateJournalRecord` 的 `recordInboxDeliver` 分支校验阶段合法（[checkpoint/file.go:745](../../checkpoint/file.go#L745)）；`sameInboxMessage` 比较规范化阶段。`Inbox` 的返回顺序保持插入序。

### 4.4 逻辑输入队列（`agent/input.go`，新文件）

```go
type inputQueue struct {
	volatile *steeringQueue
	durable  checkpoint.Durable
	threadID string
}

func (rc *RunContext) inputs() inputQueue // 由现有字段临时拼装，不新增存储字段

func (q inputQueue) takeVolatile(newTurn bool) []steerEntry
func (q inputQueue) takeVolatilePhase(phase core.InputPhase) []steerEntry
func (q inputQueue) takeDurable(ctx context.Context, newTurn bool) ([]checkpoint.InboxMessage, error)

func (rc *RunContext) publishDeliveredVolatile(step int, entries []steerEntry, preempted bool)
func (rc *RunContext) publishDeliveredDurable(step int, msgs []checkpoint.InboxMessage, preempted bool)
```

`takeDurable` 读整个 `Inbox`、跳过 `rc.pendingInboxAcks()` 已登记的 ID（[agent/durable_runtime.go:125](../../agent/durable_runtime.go#L125)）、按阶段筛选；不确认、不改顺序、读取失败原样返回。

Phase 1 的形状（替换 [agent/loop.go:259](../../agent/loop.go#L259) 到 [agent/loop.go:278](../../agent/loop.go#L278)）：

```go
q := rc.inputs()
if entries := q.takeVolatile(st.newTurn); len(entries) > 0 {
	for _, e := range entries {
		history = append(history, e.msg)
	}
	settleDelivered(entries, step)
	rc.publishDeliveredVolatile(step, entries, false)
}
inbox, err := q.takeDurable(rc, st.newTurn)
if err != nil {
	return l.fail(rc, step, history, nil, err), st, false
}
if len(inbox) > 0 {
	ids := make([]string, 0, len(inbox))
	for _, msg := range inbox {
		history = append(history, msg.Message)
		ids = append(ids, msg.ID)
	}
	rc.addInboxAcks(ids)
	rc.publishDeliveredDurable(step, inbox, false)
}
```

`settleTurn`（[agent/loop.go:210](../../agent/loop.go#L210)）继续使用 `drain()`，不改读取顺序；持久收件箱仍只做「是否存在未确认消息」的判断。

### 4.5 抢占登记（`agent/runtime.go`、`agent/loop.go`）

```go
type modelAttempt struct {
	step      int
	cancel    context.CancelFunc
	preempted bool
	done      bool
	reason    string
}

// RunContext 新增
continuedTurn bool
modelMu       sync.Mutex
modelAttempt  *modelAttempt
preemptOnce   map[int]bool   // 每个步骤最多生效一次

func (rc *RunContext) beginModelAttempt(ctx context.Context, step int) (context.Context, *modelAttempt)
func (rc *RunContext) endModelAttempt(a *modelAttempt) (preempted bool)
func (rc *RunContext) requestPreemption(reason string) bool
```

**本计划补充：** 「每步一次」用 `preemptOnce` 这个按步骤号记账的集合实现，而不是单个 `preemptStep` 字段——运行期间步骤号单调递增，集合需要回收。简化做法：只保留当前步骤一位（`preemptedStep int`），因为同一 `RunContext` 内不会回到旧步骤；本计划采用后者，字段为 `preemptedStep int`，`-1` 初始表示未发生。**实现要求写在 IQ-05。**

`requestPreemption` 在锁内判断「有活动尝试、该尝试未结束、本步骤未抢占过」，满足则置 `preempted` 与 `reason`、记录 `preemptedStep`、返回 `true`；调用方在锁外执行 `cancel`。派生上下文（`deeper`、`subRun`、`forBranch`）不复制这些字段。

模型调用循环的判断顺序：

```go
resp, ok, emitted, err := l.streamModel(genCtx, rc, lc, req)
preempted := rc.endModelAttempt(attempt)
if err == nil && ok { /* 成功优先：使用 resp */ }
if preempted && rc.Context.Err() == nil { /* 抢占重试，见 §5.4.4 设计稿伪代码 */ }
if err == nil { err = ErrModelNoFinal }
/* 既有超限恢复与失败路径 */
```

### 4.6 在途存储模型（`checkpoint/inflight.go` 新文件等）

```go
type InFlightBatch struct {
	Step  int             `json:"step"`
	Calls []core.ToolCall `json:"calls"`
}

// Checkpoint 新增（放在 Pause 之后、Snapshot 之前，checkpoint.go:65 附近）
InFlight *InFlightBatch `json:"in_flight,omitempty"`

func RequiresResume(cp *Checkpoint) bool // Pending || Pause || InFlight
var ErrInFlightConflict = errors.New("checkpoint: in-flight batch conflicts with pause or terminal state")
```

`validateWorkState(cp)` 在 `freezeCheckpoint` 内、`validateLineage` 之后调用（[checkpoint/snapshot.go:18](../../checkpoint/snapshot.go#L18)），检查：与其他工作状态互斥、`InFlight.Step == cp.Step`、调用非空、`cp.Snapshot` 存在且步骤一致、每个调用的 ID 与名称非空且名称出现在 `Snapshot.Advertised` 中。不要求调用 ID 唯一。

`cloneInFlight` 逐条复制 `Args`（复用 [checkpoint/snapshot.go:127](../../checkpoint/snapshot.go#L127) 的 `cloneSnapshotBytes`）。提交摘要新增 `commitDigestInFlight{Step, Calls []commitDigestToolCall}`（复用 [checkpoint/coordinator.go:99](../../checkpoint/coordinator.go#99)），不排序调用。`CheckpointMetadata.InFlight`、`metaFields.InFlight *json.RawMessage`、`summaryInFlightMarker` 三处投影同步。

### 4.7 在途写入（`agent/loop.go`、`agent/options.go`）

`commitOptions` 增加 `InFlight *checkpoint.InFlightBatch`（[agent/loop.go:571](../../agent/loop.go#L571)），`checkpoint()` 把它带进 `Checkpoint`（[agent/loop.go:590](../../agent/loop.go#L590)）。写入位置与设计稿 §6.2.1 一致，内容为：`State.Messages`（已含助手调用消息）、`Snapshot`、`InFlight{Step, Calls: cloneToolCalls(calls)}`、`Executions: pendingExecutions(rc.ThreadID, step, batchCalls)`、`Release:false`、无 `Pending`/`Pause`。

### 4.8 在途恢复（`agent/hitl.go`、`agent/stepview.go`、`agent/loop.go`）

```go
type inFlightBatch struct {
	step     int
	calls    []core.ToolCall
	snapshot *checkpoint.StepSnapshot
	final    core.Message
}

func (a *Agent) preflightInFlight(ctx context.Context, cp *checkpoint.Checkpoint) (*inFlightBatch, error)
func (a *Agent) inFlightMayExecute(ctx context.Context, d checkpoint.Durable, cp *checkpoint.Checkpoint) (bool, error)
func matchInFlightMessage(messages []core.Message, calls []core.ToolCall) (core.Message, error)
func (l *AgentLoop) resumeStepViewFor(step int, calls []core.ToolCall, snapshot *checkpoint.StepSnapshot) *StepView
var ErrInFlightUnrecoverable = errors.New("agent: in-flight tool batch cannot be recovered safely")
```

`Agent.Resume` 的分流顺序：`Latest` → 冲突检查 → 拒绝显式审批 → `preflightInFlight`（只读）→ 现有 `Claim` → 克隆状态 → 建句柄 → `run.rc.continuedTurn = true`、`run.rc.inFlight = batch`、**不**设置 `replayBaseline`。无在途时现有路径完全不变。

`prepareRun`（[agent/loop.go:115](../../agent/loop.go#L115)）在现有 `rb` 分支之前处理 `rc.inFlight`，形状见设计稿 §6.3.4：不调 `CheckToolPolicy`/`BeforeTool`，用 `resumeStepViewFor` 赋 `rlc.view`，调 `execTools`，`execErr != nil` 时先 `completeFailedBatch`，结果入历史，提交普通检查点（清 `InFlight`），发布 `ToolBatchRecovered`，`startStep = ib.step + 1`，`newTurn=false`。

### 4.9 失败补齐（`agent/inflight_failure.go`，新文件）

```go
func (l *AgentLoop) completeFailedBatch(rc *RunContext, step int, calls []core.ToolCall, results []core.Part) []core.Part
```

规则与设计稿 §6.4.2 一致：已报告的结果保留；台账 `completed` 用 `resultFromExecution` 回放（[agent/exectools.go:466](../../agent/exectools.go#L466)）；`claimed`/`unknown` 用 `unknownToolResult`（[agent/exectools.go:479](../../agent/exectools.go#L479)）；`pending`/缺失/读失败用 `errResult`（[agent/exectools.go:497](../../agent/exectools.go#L497)）。输出顺序等于 `calls` 顺序。读取上下文用 `context.WithoutCancel(rc.Context)`。

### 4.10 计数（`agent/exectools.go`、`agent/loopctx.go`）

```go
type outcomeClass int

const (
	classNone outcomeClass = iota
	classReplayed
	classExecuted
	classUnknown
	classRejected
)

type batchStats struct {
	mu              sync.Mutex
	Replayed, Executed, Unknown, Rejected int
}

// toolOutcome 新增 class outcomeClass
// LoopContext 新增 stats *batchStats（nil 时不计数）
```

`callOne` 在各返回点标注类别：广播/解析/参数/准备失败 → `classRejected`；`ErrToolOutcomeUnknown` → `classUnknown`；`completed` 回放 → `classReplayed`；handler 返回（成功或错误）→ `classExecuted`；写权限失败与 `noResult` 路径保持 `classNone`。`run(i,c)` 末尾调用 `lc.stats.add(out.class)`，方法对 nil 接收者安全。四个计数是诊断值，不保证相加等于调用数，事件文档要写明。

### 4.11 事件与线格式（`core/event.go`、`core/eventjson.go`）

```go
type InputDelivered struct {
	Step       int
	Source     InputSource
	Phase      InputPhase
	Count      int
	MessageIDs []string
	Preempted  bool
}

type ModelPreempted struct {
	Step    int
	Emitted bool
	Count   int
	Reason  string
}

type ToolBatchInFlight struct {
	Step  int
	Calls []ToolCall
}

type ToolBatchRecovered struct {
	Step                         int
	Replayed, Executed, Unknown, Rejected int
}
```

`eventWire` 新增：`source`、`message_ids`、`preempted`、`emitted`、`replayed`、`executed`、`unknown`、`rejected`；复用已有 `step`、`phase`、`count`、`reason`、`message`（[core/eventjson.go:18](../../core/eventjson.go#L18)）。`ToolBatchInFlight` 用助手消息包装调用列表传输，形状与 `ToolStarted` 一致（[core/eventjson.go:84](../../core/eventjson.go#L84)），解码用 `Message.ToolCalls()`（[core/message.go:112](../../core/message.go#L112)）。类型字符串：`input_delivered`、`model_preempted`、`tool_batch_in_flight`、`tool_batch_recovered`。每个事件加 marker 方法（[core/event.go:202](../../core/event.go#L202) 起）与往返测试；往返测试放 `core/stuckevent_test.go`（该文件已承载较新事件的往返测试）。

### 4.12 错误清单

```go
// core
var ErrUnknownInputPhase = errors.New("core: unknown input phase")

// agent
var (
	ErrPreemptionDisabled = errors.New("agent: input preemption is disabled")
	ErrPreemptionPhase    = errors.New("agent: only before_model input may preempt a model call")
	ErrModelNoFinal       = errors.New("agent: model stream ended without a final response")
	ErrInFlightUnrecoverable = errors.New("agent: in-flight tool batch cannot be recovered safely")
)

// checkpoint
var ErrInFlightConflict = errors.New("checkpoint: in-flight batch conflicts with pause or terminal state")
```

复用 `ErrRunClosed`、`ErrMaxTurnsExceeded`（[agent/loop.go:15](../../agent/loop.go#L15)）、`checkpoint.ErrResumeRequired`、`checkpoint.ErrDurableUnavailable`、`ErrToolNotAdvertised`、`ErrToolUnavailable`、`ErrStepViewDrift`、`checkpoint.ErrToolOutcomeUnknown`。恢复失败统一可匹配 `ErrInFlightUnrecoverable`，具体原因用 `%w` 保留；错误文本只带调用 ID 与工具名，不带参数。

### 4.13 配置

```go
// agent/config
inputPreemption   bool // 默认 false
inFlightCheckpoint bool // 默认 true，在 New 的默认值处种下

func WithInputPreemption(enabled bool) Option
func WithInFlightCheckpoint(enabled bool) Option
```

## 5. 交付顺序与提交

### 5.1 预检

```bash
git status --short
git log -1 --oneline
go build ./...
go vet ./...
go test ./... -count=1
```

确认基线为 `9e5df25`，工作区里只有已知的未跟踪文档与 `.scratch/`。如果出现额外的已修改文件，先弄清来源，不要清理或覆盖。

### 5.2 顺序

| 顺序 | 任务卡 | 内容 | 依赖 | 建议提交信息 |
| --- | --- | --- | --- | --- |
| 1 | IQ-01 | `core` 阶段、来源、回执状态与规范化 | 无 | `feat(core): add input delivery phases` |
| 2 | IQ-02 | 队列阶段、按条件取走、票据状态、`SteerWith` 与两个选项 | IQ-01 | `feat(agent): carry delivery phases on steering input` |
| 3 | IQ-03 | 逻辑输入队列、`newTurn`、两个投递点接入、`InputDelivered` | IQ-02 | `feat(agent): deliver inputs by phase` |
| 4 | IQ-04 | 收件箱阶段、`DeliverWith`、去重与文件校验 | IQ-01 | `feat(checkpoint): record delivery phase on inbox messages` |
| 5 | IF-01 | `InFlightBatch`、校验、克隆、摘要、元数据、协调器、`Fork` | 无 | `feat(checkpoint): model in-flight tool batches` |
| 6 | IF-02 | 执行前写入、配置开关、`ToolBatchInFlight` | IF-01 | `feat(agent): checkpoint a tool batch before execution` |
| 7 | IF-03 | `Resume` 分流、预检、认领 | IF-01 | `feat(agent): preflight an in-flight batch on resume` |
| 8 | IF-04 | 恢复执行、`batchStats`、`ToolBatchRecovered` | IF-02、IF-03 | `feat(agent): recover an in-flight tool batch on resume` |
| 9 | IF-05 | 批次中途失败补齐 | IF-02 | `feat(agent): complete a failed in-flight batch before failing` |
| 10 | IQ-05 | 抢占登记、`streamModel` 无终稿、重试、`ModelPreempted` | IQ-02、IQ-03 | `feat(agent): preempt a model call with volatile input` |
| 11 | IF-06 | `RunStatus.InFlight`、查询与侧车投影、队列与文档 | IF-01 | `feat(agent): report in-flight batches separately from pauses` |
| 12 | IF-07 | 崩溃注入矩阵、示例、README、全量门禁 | 全部 | `test(agent): cover in-flight recovery across crashes` |

顺序说明：IF 系列与 IQ-01..04 互不依赖，但抢占（IQ-05）改变模型调用循环的控制流，放在在途恢复之后合并，避免同时引入两处不确定性——这与设计稿 §11.2 的建议一致。每张卡提交前至少运行该卡的验证命令；不要把多张卡混在一个 commit；不要把未跟踪的分析/计划文档加进提交；commit message 不带任务编号。

## 6. 任务卡

### IQ-01 `core` 输入阶段类型

**目标：** `core` 包提供阶段、来源、回执状态三个字符串类型和 `NormalizeInputPhase`，构成闭集；不依赖 `agent` 和 `checkpoint`。

**先写的测试：** 新建 `core/input_test.go`（包 `core_test`）：

1. `TestNormalizeInputPhase`：表驱动覆盖空值 → `before_model`；四个合法值原样返回；`"before-model"`、`"BEFORE_MODEL"`、`"later"`、`" "` 四种非法输入 → 错误且 `errors.Is(err, core.ErrUnknownInputPhase)`，返回值是空阶段（不是原值）。
2. `TestInputPhaseValuesAreStable`：断言四个常量的字符串分别是 `before_model`、`after_model`、`after_tools`、`next_turn`；来源常量是 `volatile`、`durable`；状态常量是 `queued`、`delivered`、`rejected`。这三个集合会被 JSON 与外部系统按字面量消费，改名要有意识地发生。
3. `TestErrUnknownInputPhaseText`：断言错误串以 `core: ` 开头（与本包其他错误风格一致），不锁完整文本。

**实现要求：**

- 新建 `core/input.go`，内容按 §4.1；`NormalizeInputPhase` 的 default 分支用 `fmt.Errorf("%w: %q", ErrUnknownInputPhase, p)`。
- 每个类型和常量写一行注释，说明「空值等价于 `before_model`」「状态是投递回执」这两点即可，不写长段落。
- 不在本卡引入任何 `agent`/`checkpoint` 依赖，也不在本卡加事件字段。

**验证命令：**

```bash
go test ./core -run 'TestNormalizeInputPhase|TestInputPhaseValues|TestErrUnknownInputPhase' -count=1
go test ./core -count=1
gofmt -l core/input.go core/input_test.go
go vet ./core
```

**完成标准：** 三个测试通过；core 全量测试通过；`gofmt -l` 不列出本卡两个新文件；`go vet` 无输出。

**反向验证（实现完成后逐条用 Edit 还原）：**

| 改错 | 预期失败 |
| --- | --- |
| 把空值分支删掉（`if p == ""` 整段） | `TestNormalizeInputPhase` 的空值用例 |
| `default` 分支返回 `p, nil` 而不是错误 | 四个非法输入用例 |
| 把 `InputAfterModel` 的字面量写成 `"after-model"` | `TestInputPhaseValuesAreStable` |
| 错误串去掉 `core: ` 前缀 | `TestErrUnknownInputPhaseText` |

### IQ-02 队列阶段、票据状态与 `SteerWith`

**目标：** 队列条目携带阶段；`takeReady` / `takePhase` 按条件取走并保持剩余顺序；票据有 `queued`/`delivered`/`rejected` 三态；`SteerWith` 与两个配置项落地，但抢占本卡只校验不生效。

**先写的测试：** `agent/steering_internal_test.go`（包 `agent`，追加到现有文件）：

1. `TestSteeringQueuePushWithNormalizesPhase`：`pushWith(msg, SteerOptions{})` 后条目阶段是 `core.InputBeforeModel`；显式四个合法值原样保存；非法阶段返回 `nil` 票据和 `core.ErrUnknownInputPhase`，队列长度不变（用 `len(q.entries)` 断言）。
2. `TestSteeringQueueTakeReadyKeepsNextTurn`：按 `next_turn`、`before_model`、`next_turn`、`after_tools` 顺序入队；`takeReady(false)` 返回 `before_model`、`after_tools` 两条且顺序不变，剩余两条仍是 `next_turn` 且相对顺序不变；再 `takeReady(true)` 取走全部。
3. `TestSteeringQueueTakeReadyNewTurnTakesAll`：四种阶段各一条，`takeReady(true)` 一次取空，返回顺序等于入队顺序。
4. `TestSteeringQueueTakePhase`：混合阶段入队后 `takePhase(core.InputBeforeModel)` 只取走 `before_model`，重复调用返回 nil；其余条目顺序不变。
5. `TestSteerTicketStateLifecycle`：新票据 `State()==queued` 且 `Outcome()` 是零值；`settleDelivered` 后 `State()==delivered`、`Outcome().State==delivered`、`Delivered==true`、`Step` 正确；`settleUndelivered(ErrRunClosed)` 后 `State()==rejected`、`Err` 保留；重复结算不改变第一次结果。
6. `TestSteeringQueuePreemptValidation`（需要一个可注入配置的构造点）：`Preempt=true` 且阶段是 `next_turn` → `ErrPreemptionPhase`；`Preempt=true` 且配置关闭 → `ErrPreemptionDisabled`；两次校验都不入队（`len(q.entries)==0`）。实现方式：给 `steeringQueue` 加一个未导出字段 `preemptionEnabled bool`，由 `pushWith` 读取；在测试里直接构造 `&steeringQueue{preemptionEnabled: true}` 与零值队列，避免依赖 `Agent`。
7. `TestSteeringQueueClosedRejectsPushWith`：先 `close(ErrRunClosed)`，再 `pushWith` → `nil, ErrRunClosed`（关闭判断优先于阶段校验）。
8. `agent/steering_test.go`（包 `agent_test`）新增 `TestSteerKeepsLegacyDeliveryTiming`：一次普通运行里 `Run.Steer(core.UserText("x"))` 仍被下一个 Phase 1 投递，`ticket.Outcome().Delivered==true`；这条守住「不改变 `Steer` 行为」。

**实现要求：**

- `agent/steering.go`：按 §4.2 加 `SteerOptions`、`steerEntry.phase`、`SteerOutcome.State`、`SteerTicket.State()`、`pushWith`、`takeReady`、`takePhase`、`preemptionEnabled` 字段；`push` 保留并委托 `pushWith(msg, SteerOptions{})`；`drain`、`drainOrClose`、`close` 不改语义；`settleDelivered` / `settleUndelivered` 写 `State`。
- `takeReady(false)` 的实现是一次遍历分成「取走」与「保留」两个切片，然后 `q.entries = kept`；不排序、不去重。
- `agent/runtime.go`：`RunContext.SteerWith` 委托 `rc.steering.pushWith`；`Steer` 保持一行委托。`RunContext` 结构体不改字段（队列的开关由 `newRunHandleWithID` 在构造时写入）。**注意**：`steering` 是值字段（[agent/runtime.go:49](../../agent/runtime.go#L49)），新增字段要在句柄构造处赋值，不能在 `deeper`/`subRun`/`forBranch` 里复制队列。
- `agent/run.go`：`Run.SteerWith` 一行委托 `rc.SteerWith`；`Run.Steer` 改为委托 `SteerWith(msg, SteerOptions{})`，行为不变。
- `agent/options.go`：`config.inputPreemption bool` + `WithInputPreemption(enabled bool) Option`（放在既有 `With…` 区域，注释写「默认关闭」）。
- `agent/agent.go`：`New` 的默认配置不需要额外赋值（零值即关闭）；`newRunHandleWithID`（[agent/agent.go:236](../../agent/agent.go#L236)）构造 `RunContext` 时把 `a.cfg.inputPreemption` 写进 `rc.steering.preemptionEnabled`。
- 本卡**不**实现取消逻辑：`Preempt=true` 且开启时只是入队，`SteerOptions.Reason` 暂不使用（IQ-05 接）。这一点在 `WithInputPreemption` 的注释里写明「本选项只允许 `before_model` 临时消息请求取消当前模型调用」。

**验证命令：**

```bash
go test ./agent -run 'TestSteering|TestSteer' -count=1
go test ./agent -count=1
go test -race ./agent -run 'TestSteering' -count=1
go vet ./agent
gofmt -l agent/steering.go agent/steering_internal_test.go agent/runtime.go agent/run.go agent/options.go agent/agent.go
```

**完成标准：** 新增 8 个测试通过；`agent` 全量测试与 `-race` 通过；既有 `TestSteeringQueue*`、`TestSteer*` 用例不改动预期即继续通过；`go vet` 无输出；`gofmt -l` 不列出本卡文件。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| `takeReady(false)` 也取走 `next_turn` | `TestSteeringQueueTakeReadyKeepsNextTurn` |
| `takeReady` 取走后不写回 `kept`（保留项丢失） | 同上的第二次 `takeReady(true)` 断言 |
| `takePhase` 取走全部再按阶段过滤（破坏剩余顺序） | `TestSteeringQueueTakePhase` |
| `State()` 在未结算时返回 `""` | `TestSteerTicketStateLifecycle` |
| `push` 不委托 `pushWith`（阶段丢失） | `TestSteeringQueuePushWithNormalizesPhase`、`TestSteerKeepsLegacyDeliveryTiming` |
| 阶段校验放在关闭判断之前 | `TestSteeringQueueClosedRejectsPushWith` |
| `preemptionEnabled` 没接到配置 | `TestSteeringQueuePreemptValidation` 的开关两个用例 |

### IQ-03 投递阶段接入两个投递点

**目标：** `newTurn` 贯穿循环，Phase 1 与 `settleTurn` 通过逻辑输入队列按阶段投递，临时消息与持久消息各自发布 `InputDelivered`；`Steer` 与 `Deliver` 的默认行为不变。

**先写的测试：**

1. 新建 `agent/input.go` 的单元测试放 `agent/input_internal_test.go`（包 `agent`）：
   - `TestInputQueueTakeDurableFiltersPhaseAndSkipsAcks`：用内存 `checkpoint.NewMemory()` 直接构造 `inputQueue`，投递 `before_model`、`next_turn` 各一条；`takeDurable(ctx, false)` 只返回 `before_model`；手工登记确认后（`rc.addInboxAcks`）再调用返回空；`takeDurable(ctx, true)` 返回两条并保持插入顺序。
   - `TestInputQueueWithoutDurableStore`：`durable` 为 nil 时 `takeDurable` 返回 `(nil, nil)`。
2. 新建 `agent/input_test.go`（包 `agent_test`），夹具复用 `turnGatedModel`（[agent/task_loop_test.go:38](../../agent/task_loop_test.go#L38)）、`steeringBlockingTool`（[agent/steering_test.go:20](../../agent/steering_test.go#L20)）与 `mock.New` 编写的「第一次返回工具调用、之后返回文本」应答函数：
   - `TestPhaseDeliveryAfterToolBatchKeepsNextTurn`：模型第一步调用工具、第二步给终稿。在第一步的 Phase 1 之前入队四种阶段各一条；断言第一次模型请求（`m.reqs[0]`）只含三条（`before_model`/`after_model`/`after_tools`），`next_turn` 那条出现在第二次模型请求里；四条票据最终都是 `delivered`。
   - `TestPhaseDeliveryAtFinalAnswerSettlesAll`：终稿前入队四种阶段各一条；`settleTurn` 全部投递、运行续跑一步；断言续跑那一轮的 Phase 1 不再重复投递（第二次请求里这些文本只出现一次）。
   - `TestVolatileInputsPrecedeDurableInputs`：临时队列一条、收件箱一条（都 `before_model`），断言历史顺序是临时在前，并检查两条 `InputDelivered` 事件的 `Source` 分别是 `volatile`、`durable`。
   - `TestInputDeliveredEventFields`：持久消息两条带 ID；断言一条事件 `Count==2`、`MessageIDs` 等于两个 ID、`Phase=="before_model"`、`Preempted==false`；临时消息事件 `MessageIDs` 为空。
   - `TestResumedRunFirstStepIsNotNewTurn`：先跑一次让收件箱留下 `next_turn` 消息（不消费），再对该线程 `Resume`（无审批批次）；断言恢复后第一步的 Phase 1 不投递 `next_turn`，直到出现终稿并续跑或下一次运行才投递。
   - `TestMaxTokensContinuationIsNotNewTurn`：模型返回带工具调用且 `StopReason==llm.StopMaxTokens`（构造方式见 [agent/loop.go:463](../../agent/loop.go#L463) 的判定），同批入队 `next_turn` 消息；断言截断后的下一步 Phase 1 仍未投递它，终稿那一步之后才投递。
   - `TestInboxReadFailureStillFailsStep`：用 `failingInboxStore`（[agent/task_loop_test.go:510](../../agent/task_loop_test.go#L510)）让 `Inbox` 报错；断言运行以该错误失败，且已投递的临时票据按现有方式结算。
3. 回归项：`agent/task_loop_test.go` 现有 `TestSteer*`、`TestSettleTurn*`、`TestContinuation*`、`TestConsumedInbox*` 系列必须在不改预期的前提下通过（本卡只把 `drain()` 换成 `takeReady(true)` 等价路径）。

**实现要求：**

- `agent/loop.go`：
  - `taskState` 增加 `newTurn bool`（[agent/loop.go:90](../../agent/loop.go#L90)）。
  - `prepareRun` 返回值填 `newTurn`：`rc.continuedTurn` 为真 → `false`；否则 `true`（[agent/loop.go:181](../../agent/loop.go#L181)）。审批恢复批次分支与新增的在途分支一样保持 `false` 语义——本卡只需保证 `rb != nil` 时最终 `taskState.newTurn=false`。
  - Phase 1 两段读取换成 §4.4 的形状（`q.takeVolatile(st.newTurn)` 与 `q.takeDurable(rc, st.newTurn)`），保留 `settleDelivered(entries, step)` 与 `rc.addInboxAcks(ids)` 的现有位置。
  - 三个 `taskState` 续跑构造点补 `newTurn`：`settleTurn` 续跑处 `true`（[agent/loop.go:447](../../agent/loop.go#L447)）、`max_tokens` 处 `false`（[agent/loop.go:470](../../agent/loop.go#L470)）、Phase 5 之后工具批次续跑处 `false`（[agent/loop.go:530](../../agent/loop.go#L530)）。
  - `settleTurn` 不改读取顺序，仍用 `drain()`（[agent/loop.go:216](../../agent/loop.go#L216)）；预算耗尽分支的 `settleUndelivered` 保持（[agent/loop.go:212](../../agent/loop.go#L212)）。
- `agent/input.go`：新增 `inputQueue`、`rc.inputs()`、`takeVolatile`、`takeVolatilePhase`、`takeDurable`、`publishDeliveredVolatile`、`publishDeliveredDurable`（§4.4）。`takeVolatilePhase` 本卡只实现不使用，IQ-05 接。
- `agent/runtime.go`：`RunContext` 增加 `continuedTurn bool`，注释写明「只在恢复路径设置，表示当前轮次延续」；`deeper`/`subRun`/`forBranch` 不复制。
- `agent/hitl.go`：`Resume` 的两个出口（无待审批批次的提前返回 [agent/hitl.go:191](../../agent/hitl.go#L191) 与设置 `resumed` 的返回 [agent/hitl.go:207](../../agent/hitl.go#L207)）都设置 `run.rc.continuedTurn = true`。
- `core/event.go` + `core/eventjson.go`：新增 `InputDelivered`（§4.11），类型串 `input_delivered`；`eventWire` 加 `source`、`message_ids`、`preempted`；marker 方法加进 [core/event.go:202](../../core/event.go#L202) 起的区块。
- `core/stuckevent_test.go`：新增 `TestInputDeliveredEventRoundTrip`（全字段非零往返相等）与 `TestInputDeliveredEventOmitsEmptyFields`（`Count=0`、`MessageIDs=nil`、`Preempted=false` 时对应键不存在；用 `json.Unmarshal` 到 `map[string]any` 断言键缺失，不要手写期望 JSON 串）。
- 事件里的 `Phase` 用规范化后的值；解码时不在事件层做合法性校验（投递时已校验）。

**验证命令：**

```bash
go test ./agent -run 'TestInputQueue|TestPhaseDelivery|TestVolatileInputs|TestInputDelivered|TestResumedRunFirstStep|TestMaxTokensContinuation|TestInboxReadFailure' -count=1
go test ./agent -run 'TestSteer|TestSettleTurn|TestContinuation|TestConsumedInbox|TestInboxRead' -count=1
go test ./core -run 'TestInputDelivered' -count=1
go test ./agent ./core ./checkpoint ./eval -count=1
go test -race ./agent -run 'TestPhaseDelivery|TestInputQueue|TestSteer' -count=1
go vet ./agent ./core
```

**完成标准：** 新增用例全过；三组回归用例不改预期即通过；`-race` 通过；`go vet` 无输出；`agent`、`core`、`checkpoint`、`eval` 全量通过。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| Phase 1 用 `drain()` 代替 `takeReady(st.newTurn)` | `TestPhaseDeliveryAfterToolBatchKeepsNextTurn` |
| `max_tokens` 续跑处填 `newTurn: true` | `TestMaxTokensContinuationIsNotNewTurn` |
| `Resume` 不设 `continuedTurn` | `TestResumedRunFirstStepIsNotNewTurn` |
| `takeDurable` 不过滤已登记确认的 ID | `TestInputQueueTakeDurableFiltersPhaseAndSkipsAcks`、`TestConsumedInbox*` 回归 |
| 先读持久消息再读临时消息 | `TestVolatileInputsPrecedeDurableInputs` |
| 临时消息事件也填 `MessageIDs` | `TestInputDeliveredEventFields` |
| 去掉 `publishDeliveredDurable` 调用 | `TestInputDeliveredEventFields` |

### IQ-04 持久收件箱的阶段

**目标：** 收件箱记录带阶段；`DeliverWith` 可指定阶段；同 ID 同消息不同阶段判为冲突；旧记录（无 `phase`）按 `before_model` 读取；`DeliveryReceipt.State` 恒为 `queued`。

**先写的测试：**

1. `checkpoint/durable_test.go`（包 `checkpoint_test`，与 `TestInboxHistoryAndAckCommitTogether` [checkpoint/durable_test.go:51](../../checkpoint/durable_test.go#L51) 同族）新增：
   - `TestInboxPhaseRoundTripMemoryAndFile`：对两种存储各跑一次——`Deliver` 一条无阶段的消息，`Inbox` 读回的阶段是 `core.InputBeforeModel`（写入时已规范化）；再 `DeliverWith(..., core.InputNextTurn)` 一条，读回 `next_turn`；断言插入顺序保持。
   - `TestInboxSameIDDifferentPhaseConflicts`：同 ID、同消息，先以 `before_model` 投递成功，再以 `next_turn` 投递 → 返回错误且错误文本包含该 ID；`Inbox` 仍只有第一条。对内存与文件两种存储各跑一遍。
   - `TestInboxSameIDSamePhaseIsDuplicate`：同 ID、同消息、同阶段（一个是空、一个是显式 `before_model`）→ `Duplicate==true`、`State==core.InputQueued`、不报错。这条同时验证「空值与显式值不算冲突」。
   - `TestInboxUnknownPhaseRejected`：`DeliverWith(..., "later")` → `core.ErrUnknownInputPhase`，`Inbox` 无变化。
   - `TestInboxLegacyRecordReadsAsBeforeModel`：内存存储直接放入一条 `Phase` 为零值的记录（模拟旧数据），`Inbox` 读回后 `takeDurable` 语义由 IQ-03 覆盖，这里只断言 `Phase` 字段原样为空——**空值读取时不强制改写**，规范化发生在投递点。
2. `checkpoint/file_test.go`（包 `checkpoint_test`）新增 `TestFileJournalValidatesInboxPhase`：向文件日志写入一条阶段非法的 `recordInboxDeliver` 记录（复用该文件里已有的「写坏日志再打开」手法，若没有则用 `File.appendRecordLocked` 的等价公开路径 `Deliver` 加手工改日志文件），重开后 `Inbox` 返回校验错误。若现有测试没有可复用的损坏日志夹具，本用例改为单测 `validateJournalRecord`：把它移进 `package checkpoint` 的测试文件（`checkpoint/file_recovery_test.go` 已是包内测试）直接调用。
3. `agent/phase2_durable_test.go`（包 `agent_test`，`TestDurableInboxDelivery` 在 [agent/phase2_durable_test.go:28](../../agent/phase2_durable_test.go#L28)）新增 `TestDeliverWithAgentPhase`：`Agent.DeliverWith` 带 `next_turn` 的消息后，运行第一步不消费、终稿续跑后消费；`Agent.Deliver` 与 `DeliverWith(..., core.InputBeforeModel)` 行为一致（同一条断言两次跑，比较历史）。
4. `agent/phase2_durable_test.go` 新增 `TestDeliverRejectsUnknownPhase`：`Agent.DeliverWith` 传非法阶段返回 `core.ErrUnknownInputPhase`，且未写入收件箱。

**实现要求：**

- `checkpoint/durable.go`：`InboxMessage` 加 `Phase core.InputPhase \`json:"phase,omitempty"\``；`DeliveryReceipt` 加 `State core.InputState`（[checkpoint/durable.go:60](../../checkpoint/durable.go#L60)、[65](../../checkpoint/durable.go#L65)）。注意 `checkpoint` 包已依赖 `core`，方向合法。
- `checkpoint/durable_memory.go`：`Deliver` 入口规范化阶段并写回 `msg`（[checkpoint/durable_memory.go:65](../../checkpoint/durable_memory.go#L65)）；`sameInboxMessage` 先比消息体再比 `NormalizeInputPhase` 后的阶段（[checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419)）；冲突文本在现有句子上追加阶段，但不新增 sentinel（见 §2.5）。
- `checkpoint/file.go`：`Deliver` 同样在入口规范化（[checkpoint/file.go:183](../../checkpoint/file.go#L183)）；`validateJournalRecord` 的 `recordInboxDeliver` 分支调用 `core.NormalizeInputPhase` 校验（[checkpoint/file.go:745](../../checkpoint/file.go#L745)）；`Inbox` 不做改写，保持原样返回。
- `agent/inbox.go`：`DeliverWith(ctx, threadID, msg, phase)` 把 `msg.Phase` 设为传入阶段后走现有校验与委托；`Deliver` 委托 `DeliverWith(..., core.InputBeforeModel)`（[agent/inbox.go:15](../../agent/inbox.go#L15)）。
- 两处 `DeliveryReceipt` 构造点（内存与文件共四处：新增、重复、以及各自的返回处）都要填 `State: core.InputQueued`。
- 不改 `Commit`、不改确认协议，阶段过滤已在 IQ-03 的 `takeDurable` 完成。

**验证命令：**

```bash
go test ./checkpoint -run 'TestInbox|TestFileJournalValidatesInboxPhase' -count=1
go test ./agent -run 'TestDeliverWith|TestDeliverRejectsUnknownPhase|TestDurableInboxDelivery' -count=1
go test ./checkpoint ./agent -count=1
gofmt -l checkpoint/durable.go checkpoint/durable_memory.go checkpoint/file.go agent/inbox.go
go vet ./checkpoint ./agent
```

**完成标准：** 新增 7 个用例通过；`checkpoint`、`agent` 全量通过；旧数据（无 `phase`）读取用例通过；`gofmt` 与 `vet` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| `sameInboxMessage` 不比阶段 | `TestInboxSameIDDifferentPhaseConflicts` 得到「重复」而非错误 |
| `Deliver` 不规范化就存储（空阶段直接落盘） | `TestInboxPhaseRoundTripMemoryAndFile` 的第一段断言 |
| `DeliverWith` 校验放在存储之后 | `TestInboxUnknownPhaseRejected` 的「`Inbox` 无变化」断言 |
| `DeliveryReceipt.State` 没填 | `TestInboxSameIDSamePhaseIsDuplicate` |
| `validateJournalRecord` 不校验阶段 | `TestFileJournalValidatesInboxPhase` |
| 阶段比较用原始值而非规范化值 | `TestInboxSameIDSamePhaseIsDuplicate` 的空/显式混合子断言 |

### IF-01 在途批次的存储模型

**目标：** `checkpoint` 能表示、校验、克隆、摘要、投影一个 `InFlightBatch`；`RequiresResume` 统一「必须先恢复」的判断；旧记录与旧提交摘要字节不变。本卡不接线到 `agent` 循环。

**先写的测试：**

1. `checkpoint/inflight_contract_test.go`（新文件，包 `checkpoint_test`）：
   - `TestInFlightCoexistsWithNothingElse`：表驱动，对内存与文件两种存储各构造一个带 `InFlight` 的合法检查点（必须同时带 `Snapshot{Step, Advertised, ToolDigest}`、`State.Messages` 末尾是助手工具调用消息），`Save`/`Commit` 成功；再分别加上 `Pending`、`Pause`、`Completion` 三种组合，每一种都被拒绝且可匹配新 sentinel `checkpoint.ErrInFlightConflict`。
   - `TestInFlightRequiresSnapshot`：`InFlight` 非空但 `Snapshot==nil`、`Snapshot.Step != cp.Step`、`Snapshot.ToolDigest==""`、`InFlight.Step != cp.Step`、`len(Calls)==0`、`Call.ID==""`、`Call.Name` 不在 `Advertised` 中——七种各一例，全部拒绝。
   - `TestInFlightCallIDsMayRepeat`：两条同 ID、同工具、不同参数的调用（且都出现在 `Advertised` 里）通过校验，确认没有多余的唯一性约束。
   - `TestRequiresResume`：`nil` → false；只有 `State` → false；`Pending` / `Pause` / `InFlight` 任一 → true。
   - `TestInFlightCloneIsolatesArgs`（该用例需要访问未导出的 `cloneCheckpoint`，放进 `checkpoint/snapshot_contract_test.go` 若它是包内测试，否则放进 `checkpoint/inflight_internal_test.go` 新文件、包 `checkpoint`）：`cloneInFlight` 后修改原 `Calls[0].Args` 的底层字节，副本不变。
2. `checkpoint/stepview_test.go` 新增 `TestInFlightDigestRetry`（照 `TestStepSnapshotDigestRetry` [checkpoint/stepview_test.go:112](../../checkpoint/stepview_test.go#L112) 的写法，用 `newSnapshotContractStore` [checkpoint/snapshot_files_contract_test.go:213](../../checkpoint/snapshot_files_contract_test.go#L213)）：同一条带 `InFlight` 的检查点提交两次不冲突；交换 `Calls` 顺序后再提交 → `checkpoint.ErrCheckpointConflict`；改一条参数 → 同样冲突。
3. `checkpoint/coordinator_contract_test.go`（包 `checkpoint`）新增 `TestCommitDigestOmitsInFlightWhenAbsent`：`json.Marshal(commitDigestCheckpoint{})` 的输出不含 `"in_flight"` 键；含 `InFlight` 时含该键。这条直接证明「旧提交字节不变」（设计稿不变量 25）。
4. `checkpoint/coordinator_contract_test.go` 新增 `TestSnapshotConsistencyBeginRunInFlight`（紧挨现有 `TestSnapshotConsistencyBeginRunPaused` [checkpoint/coordinator_contract_test.go:102](../../checkpoint/coordinator_contract_test.go#L102)）：头部带 `InFlight` 时两种存储的 `BeginRun` 都返回 `checkpoint.ErrResumeRequired`；错误文本不再出现 "paused"（用 `strings.Contains(err.Error(), "paused")` 断言 false）。
5. `checkpoint/fork_consistency_test.go` 新增 `TestForkRejectsInFlight`（对照 `TestSnapshotConsistencyForkDropsPause` [checkpoint/fork_consistency_test.go:194](../../checkpoint/fork_consistency_test.go#L194)）：父检查点带 `InFlight` 时 `Fork` 返回 `ErrInFlightConflict`；不带时行为不变。
6. `checkpoint/metadata_contract_test.go` 新增 `TestMetadataInFlightProjection`（紧挨 `TestMetadataRoundTrip` [checkpoint/metadata_contract_test.go:124](../../checkpoint/metadata_contract_test.go#L124)）：带 `InFlight` 的头部元数据 `Paused==false`、`InFlight==true`；带 `Pause` 的 `Paused==true`、`InFlight==false`；两者不同时为 true。
7. `checkpoint/metadata_index_test.go` 新增 `TestFileMetadataRebuildInFlight`（照 `TestFileMetadataRebuild` [checkpoint/metadata_index_test.go:142](../../checkpoint/metadata_index_test.go#L142)）：删除侧车后重扫日志，带在途记录的线程投影出 `InFlight==true && Paused==false`，与主存储 `Latest` 的判断一致。
8. `checkpoint/durable_test.go` 新增 `TestFileJournalValidatesInFlightRecord`：写入的 `recordCommit` 检查点带 `InFlight` 时，重开后 `Latest` 与侧车一致；带 `InFlight`+`Pause` 的记录在写入时被拒绝（不需要手工损坏日志，因为写入路径已有校验）。

**实现要求：**

- 新建 `checkpoint/inflight.go`：`InFlightBatch`、`ErrInFlightConflict`、`validateWorkState`、`RequiresResume`、`cloneInFlight`（§4.6）。`cloneInFlight` 复用 `cloneSnapshotBytes`（[checkpoint/snapshot.go:127](../../checkpoint/snapshot.go#L127)）。
- `checkpoint/checkpoint.go`：`Checkpoint` 在 `Pause`（[checkpoint/checkpoint.go:65](../../checkpoint/checkpoint.go#L65)）之后插入 `InFlight *InFlightBatch \`json:"in_flight,omitempty"\``，注释写清「不是暂停，恢复入口是 `Agent.Resume`」。
- `checkpoint/snapshot.go`：`freezeCheckpoint` 在 `validateLineage` 之后调用 `validateWorkState`（[checkpoint/snapshot.go:18](../../checkpoint/snapshot.go#L18)）；`cloneCheckpoint` 增加 `out.InFlight = cloneInFlight(cp.InFlight)`（[checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30)）。
- `checkpoint/coordinator.go`：`commitDigestCheckpoint` 加 `InFlight *commitDigestInFlight \`json:"in_flight,omitempty"\``（[checkpoint/coordinator.go:42](../../checkpoint/coordinator.go#L42)），新增 `commitDigestInFlight{Step int, Calls []commitDigestToolCall}`（复用 [checkpoint/coordinator.go:99](../../checkpoint/coordinator.go#L99)）；`commitRequestDigest` 填充它，**不排序** `Calls`；`ErrResumeRequired` 文本改为 `checkpoint: thread requires resume`（[checkpoint/coordinator.go:18](../../checkpoint/coordinator.go#L18)）。
- `checkpoint/coordinator_memory.go:26`、`checkpoint/coordinator_file.go:23`：拒绝条件换成 `head != nil && RequiresResume(head)`。
- `checkpoint/branch.go`：`Fork` 在复制之前拒绝 `parent.InFlight != nil`（[checkpoint/branch.go:17](../../checkpoint/branch.go#L17)），错误包装 `ErrInFlightConflict`；现有的清空 `Pending`/`Pause`/`Completion` 保持不变（[checkpoint/branch.go:45](../../checkpoint/branch.go#L45)）。
- `checkpoint/query.go`：`CheckpointMetadata` 加 `InFlight bool \`json:"in_flight,omitempty"\``（[checkpoint/query.go:54](../../checkpoint/query.go#L54)）；`checkpointMetadataOf` 填 `InFlight: cp.InFlight != nil`，`Paused` 表达式不动（[checkpoint/query.go:158](../../checkpoint/query.go#L158)）。
- `checkpoint/metadata_index.go`：`metaFields` 加 `InFlight *json.RawMessage \`json:"in_flight"\``（[checkpoint/metadata_index.go:60](../../checkpoint/metadata_index.go#L60)）；新增 `summaryInFlightMarker(fields)` 返回 `&InFlightBatch{Step: fields.Step}` 或 nil；`projectJournalLine` 把它传给 `checkpointMetadataOf`（[checkpoint/metadata_index.go:450](../../checkpoint/metadata_index.go#L450)）。
- `checkpoint/file.go`：`validateJournalRecord` 的 `recordCommit` 分支不用改——提交检查点在写入前已由 `freezeCheckpoint` 校验；若文件存储存在绕过 `freezeCheckpoint` 的日志重放路径，需在 `applyJournalRecord` 前调用 `validateWorkState`，实施时先确认这条路径是否存在，存在才加，并把结论写进执行记录。

**验证命令：**

```bash
go test ./checkpoint -run 'TestInFlight|TestRequiresResume|TestCommitDigestOmitsInFlight|TestSnapshotConsistencyBeginRunInFlight|TestForkRejectsInFlight|TestMetadataInFlight|TestFileMetadataRebuildInFlight|TestFileJournalValidatesInFlight' -count=1
go test ./checkpoint -count=1
go test -race ./checkpoint -count=1
gofmt -l checkpoint/inflight.go checkpoint/inflight_contract_test.go checkpoint/checkpoint.go checkpoint/snapshot.go checkpoint/coordinator.go checkpoint/coordinator_memory.go checkpoint/coordinator_file.go checkpoint/branch.go checkpoint/query.go checkpoint/metadata_index.go checkpoint/file.go
go vet ./checkpoint
```

**完成标准：** 新增用例全过；`checkpoint` 全量与 `-race` 通过；`TestLegacyCheckpointDigestUnchanged`、`TestLegacyDigestRetry`、`TestSnapshotConsistencyBeginRunPaused`、`TestFork*` 既有用例不改预期即通过（旧数据兼容的证据）；`gofmt`、`vet` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| `freezeCheckpoint` 不调 `validateWorkState` | `TestInFlightCoexistsWithNothingElse` 的三个冲突子例、`TestInFlightRequiresSnapshot` |
| 去掉「必须有 `Snapshot`」这条 | `TestInFlightRequiresSnapshot` 的前三个子例 |
| `cloneInFlight` 直接 `out.Calls = in.Calls`（共享底层数组） | `TestInFlightCloneIsolatesArgs` |
| 摘要里对 `Calls` 排序 | `TestInFlightDigestRetry` 的顺序交换子例 |
| `commitDigestInFlight` 不带 `omitempty` | `TestCommitDigestOmitsInFlightWhenAbsent` |
| `BeginRun` 仍只判 `Pending`/`Pause` | `TestSnapshotConsistencyBeginRunInFlight` |
| `Fork` 只清空 `InFlight` 不拒绝 | `TestForkRejectsInFlight` |
| `metaFields` 不读 `in_flight` | `TestFileMetadataRebuildInFlight` |
| `RequiresResume` 漏 `InFlight` | `TestRequiresResume`、`TestSnapshotConsistencyBeginRunInFlight` |

### IF-02 工具批次执行前写入在途检查点

**目标：** 持久化且顶层的运行在 `execTools` 之前写一次在途检查点，提交失败就不执行任何 handler；默认开启，可整体关闭；成功后发布 `ToolBatchInFlight`。

**先写的测试：** 新建 `agent/inflight_test.go`（包 `agent_test`），夹具：

- `inFlightModel`：`mock.New` 的应答函数，第一次返回 `mock.CallTool("c1","rec",...)` 加第二条 `mock.CallTool("c2","rec",...)`（两个可并发执行的工具），看到工具结果后返回 `mock.Text("done")`。
- `recordingTool`：记录每次 `Call` 的调用与 `InvocationID`（从 `tool.Context` 读），供「handler 未启动」断言。
- `failCommitStore`（本卡新增夹具）：包裹 `checkpoint.NewMemory()` 的 `Durable`，按第 N 次 `Commit` 注入错误，其余委托；写法对照 `phase1FailOnSaveStore`（[agent/phase1_failure_test.go:21](../../agent/phase1_failure_test.go#L21)）但实现 `checkpoint.Durable` 全部方法。

用例：

1. `TestInFlightCheckpointWrittenBeforeHandlers`：模型给出两个调用；第一个工具的 `Call` 里读 `store.Latest`，断言此刻头部 `InFlight != nil`、`InFlight.Step==0`、`len(Calls)==2`、`State.Messages` 末尾是助手工具调用消息、`Pending==nil`、`Pause==nil`、`Snapshot.ToolDigest!=""`；同时断言台账里两个调用都是 `checkpoint.ToolPending`（用 `durable.ToolExecution` 读，编号由测试自己用 `tool.Context.InvocationID` 提供）。
2. `TestInFlightCallsUseRewrittenArgs`：一个中间件在 `BeforeTool` 里把参数 `{}` 改成 `{"v":1}`；断言 `InFlight.Calls[0].Args` 是改写后的值，而 `State.Messages` 末尾助手消息里的参数仍是 `{}`；两个调用编号不同源——断言台账记录的 `InvocationID` 与改写后参数算出的编号一致。
3. `TestInFlightCommitFailureBlocksHandlers`：`failCommitStore` 让第一次 `Commit`（即 Phase 1 之后的第一个提交点）失败；断言两个工具一次都没被调用、运行以该错误失败、没有 `ToolStarted` 事件、头部没有 `InFlight`。
4. `TestInFlightSkippedForPlainCheckpointer`：只实现 `Checkpointer` 的存储（`phase1FailOnSaveStore` 即可）；断言运行正常完成且 `Save` 次数与该存储的既有基线一致（每步一次），头部永无 `InFlight`。
5. `TestInFlightDisabledByOption`：`agent.WithInFlightCheckpoint(false)`；断言 `Commit` 次数比默认少一次、工具照常执行、行为回到基线。
6. `TestInFlightNotWrittenForSubRun`：把带工具的 agent 用 `agent.AsTool` 挂进父 agent（现有子运行写法见 [agent/runtime.go:84](../../agent/runtime.go#L84) 的 `subRun`），断言子运行期间父线程头部不带 `InFlight`，父运行自己写的那一次带。
7. `TestInFlightCommitKeepsClaim`：用 `claimRecordingStore`（[agent/task_loop_test.go:582](../../agent/task_loop_test.go#L582)）断言提交序列 `ReleaseClaim` 为 `[false, ..., true]`：在途那一次不释放、步骤结束那次释放。
8. `TestInFlightConfirmsThisStepAcks`：先用 `Agent.Deliver` 投一条 `before_model`，运行第一步读取并投递后进入工具批次；断言在途那次提交的 `AckMessageIDs` 含该 ID，`Inbox` 随之后；运行结束后该消息不再出现。
9. `TestInFlightEventPublishedAfterCommit`：断言 `ToolBatchInFlight` 事件存在，`Step` 正确，`Calls` 是改写后的参数；并断言事件发布时机晚于 `Commit`（用 `claimRecordingStore` 记录提交序号 + bus 订阅记录事件序号比较）。
10. `TestInFlightRejectedBeforeExecutionChecksPass`：`BeforeTool` 返回 `core.Interrupt` 的运行，头部是 `Pending`+`Pause`，不是 `InFlight`（对照现有 `TestPause*` 写法）；`BeforeTool` 返回 `Stop` 时不写 `InFlight`。

**实现要求：**

- `agent/options.go`：`config.inFlightCheckpoint bool` + `WithInFlightCheckpoint(enabled bool) Option`；`agent/agent.go` 的 `New` 默认值处设为 `true`（[agent/agent.go:37](../../agent/agent.go#L37)），注释写明「仅对持久化且顶层的运行生效」。
- `agent/loop.go`：`AgentLoop` 加 `inFlightCheckpoint bool`，`newLoop` 从 `config` 复制（[agent/loop.go:31](../../agent/loop.go#L31)、[agent/loop.go:51](../../agent/loop.go#L51)）。
- `agent/loop.go`：在 `BeforeTool` 循环结束与 `execTools` 调用之间插入 §4.7 的写入块（位置：[agent/loop.go:505](../../agent/loop.go#L505) 之后、[agent/loop.go:507](../../agent/loop.go#L507) 之前）。条件三连：`l.inFlightCheckpoint && rc.topLevel`、`if durable, ok := rc.durableStore(); ok`、`rc.checkWriteAccess()` 先行。内容：`rc.State.Messages = history`，`batchCalls := cloneToolCalls(calls)`，`commitOptions{Step: step, Snapshot: lc.viewSnapshot(), InFlight: &checkpoint.InFlightBatch{Step: step, Calls: batchCalls}, Executions: pendingExecutions(rc.ThreadID, step, batchCalls)}`（`Release` 保持零值 false）。提交成功后 `rc.publish(core.ToolBatchInFlight{Step: step, Calls: cloneToolCalls(batchCalls)})`；提交失败走 `l.fail`。
- `agent/loop.go`：`commitOptions` 增加 `InFlight *checkpoint.InFlightBatch`（[agent/loop.go:571](../../agent/loop.go#L571)），`checkpoint()` 传递它（[agent/loop.go:590](../../agent/loop.go#L590)）；`rc.Store == nil` 的早退分支（[agent/loop.go:584](../../agent/loop.go#L584)）对 `InFlight != nil` 的处理与 `Pending`/`Pause` 一致：返回 `ErrCheckpointUnavailable`（没有存储就没有可恢复边界，也不该继续执行工具）。
- 新建 `agent/inflight.go`：`cloneToolCalls([]core.ToolCall) []core.ToolCall`（逐条复制 `Args` 字节）。
- `core/event.go` + `core/eventjson.go`：新增 `ToolBatchInFlight{Step int; Calls []ToolCall}`，类型串 `tool_batch_in_flight`，编码用助手消息包装 `Calls`（照 `ToolStarted`，[core/eventjson.go:84](../../core/eventjson.go#L84)），解码用 `w.Message.ToolCalls()`（[core/message.go:112](../../core/message.go#L112)）；marker 与往返测试进 `core/stuckevent_test.go`（含一条「Calls 为空」的 `omitempty` 断言）。
- 不改 Phase 4 的提交（[agent/loop.go:519](../../agent/loop.go#L519) 不带 `InFlight`，新头部自然清除在途）。

**验证命令：**

```bash
go test ./agent -run 'TestInFlight' -count=1
go test ./core -run 'TestToolBatchInFlight' -count=1
go test ./agent ./core ./checkpoint ./eval -count=1
go test -race ./agent -run 'TestInFlight|TestTool|TestPause' -count=1
go vet ./agent ./core
gofmt -l agent/inflight.go agent/inflight_test.go agent/loop.go agent/options.go agent/agent.go core/event.go core/eventjson.go core/stuckevent_test.go
```

**完成标准：** 10 个用例通过；既有工具、审批、投递类测试不改预期即通过；`-race` 通过；`gofmt`、`vet` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 把写入移到 `execTools` 之后 | `TestInFlightCheckpointWrittenBeforeHandlers`（工具内读到的头部没有 `InFlight`） |
| 写入移到 `BeforeTool` 循环之前 | `TestInFlightCallsUseRewrittenArgs`、`TestInFlightRejectedBeforeExecutionChecksPass` |
| 去掉 `checkWriteAccess` 前置 | `TestInFlightCommitFailureBlocksHandlers`（取消场景）——若该用例覆盖不到，补一条「提交前取消运行」子例并把结论写进执行记录 |
| `Release` 改成 true | `TestInFlightCommitKeepsClaim` |
| `Executions` 漏传 | 用例 1 的 `pending` 台账断言 |
| 去掉 `durableStore` 条件 | `TestInFlightSkippedForPlainCheckpointer` |
| 去掉 `rc.topLevel` 条件 | `TestInFlightNotWrittenForSubRun` |
| 事件在提交前发布 | `TestInFlightEventPublishedAfterCommit` |
| 摘要字段 `InFlight` 未接（IF-01 遗漏时） | 用例 3 的第二次同内容提交不报冲突 |

### IF-03 `Resume` 识别在途批次并预检

**目标：** `Agent.Resume` 见到 `InFlight` 头部时走独立分支：先只读校验、再认领线程、把批次交给恢复运行；显式审批、工作流 Agent、非持久存储、动态工具、无法匹配的助手消息都在认领前后各自的位置被拒绝。本卡不执行批次（IF-04 做）。

**先写的测试：** 新建 `agent/inflight_resume_test.go`（包 `agent_test`），夹具：

- `seedInFlightHead(t, store, threadID, step int, calls []core.ToolCall, snapshot *checkpoint.StepSnapshot, msgs []core.Message) *checkpoint.Checkpoint`：直接 `store.Save` 一个合法在途头部（`State.Messages` 以助手工具调用消息结尾、`Snapshot.Advertised` 含全部调用名、`ToolDigest` 非空）。
- `pairFor(calls)`：由调用列表构造对应的助手消息。

用例：

1. `TestResumeDetectsInFlight`：种子头部后 `Resume`；断言返回的 `Run` 非空、`run.rc` 里批次已登记（通过 IF-04 的执行结果或本卡用一条只读断言：`agent.RunStatus` 的 `InFlight==true`，若 IF-06 未落地则改为断言 `store.Latest` 仍是同一个在途头部、运行没有失败）、认领已建立（`durable.Decisions`/`Claim` 记录里 `PauseID == "checkpoint-<cp.ID>"`，见 [checkpoint/checkpoint.go:144](../../checkpoint/checkpoint.go#L144)）。
2. `TestResumeInFlightRejectsApprovals`：`Resume(ctx, tid, agent.Approval{CallID:"c1", Approve:true})` → 错误可匹配 `ErrInFlightUnrecoverable`，文本含「approvals」，且没有建立认领（内存存储的 `Claim` 状态仍为空，用 `BeginRun` 观察或直接读存储的未导出状态）。
3. `TestResumeInFlightRejectsConflictingHead`：手工构造（绕过存储校验用 `checkpoint.Memory.Save` 会拒绝，所以改为在测试里直接改头部指针不可行）→ 用包内测试 `checkpoint` 的 `validateWorkState` 已覆盖（IF-01），本卡改为断言 `Resume` 对一个带 `InFlight` 且 `Snapshot.Step != cp.Step` 的头部（手工绕过 `freeze` 不可能）…… 因此本用例的实际写法是：**跳过**，并在执行记录里说明冲突头部无法被写入存储，所以 `Resume` 不需要重复校验；保留一个 `TestResumeInFlightNeedsDurableStore`：只实现 `Checkpointer` 的存储上带 `InFlight` 的头部 → `Resume` 返回 `ErrInFlightUnrecoverable`（原因含 durable）。
4. `TestResumeInFlightRequiresResolvableTools`：快照 `Advertised` 含 `"rec"`，但当前 agent 工具表没有 `rec` → `ErrToolUnavailable` 且 `errors.Is(err, ErrInFlightUnrecoverable)`。
5. `TestResumeInFlightRequiresAdvertised`：`InFlight.Calls` 里有个名字不在 `Snapshot.Advertised` → `ErrToolNotAdvertised`（注：这种记录在 IF-01 的存储校验下写不进去，所以本用例通过 `checkpoint` 包内测试构造；`agent` 侧只需断言预检函数自身的判断，可把该检查写成包内测试 `agent/inflight_internal_test.go` 调用未导出的 `preflightInFlight`）。
6. `TestResumeInFlightMatchesAssistantMessage`：三种坏历史各一例——末尾不是助手消息、调用条数不同、第 2 条 ID 不匹配 → `ErrInFlightUnrecoverable`；参数不同但 ID/名称/顺序一致 → 通过（这是核心断言）。
7. `TestResumeInFlightPreflightBeforeClaim`：预检失败时不得留下认领。用 `claimRecordingStore` 包一层内存存储（[agent/task_loop_test.go:582](../../agent/task_loop_test.go#L582)，它记录 `Commit`；认领记录需要扩一个 `claims []checkpoint.ClaimRequest`——本卡允许给该夹具加只读记录，不改现有断言）。
8. `TestResumeInFlightDoesNotSetReplayBaseline`：恢复运行起来后第一次模型请求不做漂移检查。做法：`StepSnapshotStrict` 策略（[agent/options.go:140](../../agent/options.go#L140)）下把 agent 的工具 schema 换成与快照不同但名称一致的实现；若基线被误设，运行会在第一步失败。断言：IF-04 之前该用例可写成「运行进入工具批次执行而不报 `ErrStepViewDrift`」。
9. `TestResumeInFlightSummaryDriftBlocksExecution`：当前工具摘要与快照不同 + 台账里有一个 `pending` → 拒绝恢复，错误含 `ErrStepViewDrift` 与 `ErrInFlightUnrecoverable`。
10. `TestResumeInFlightSummaryDriftAllowsPureReplay`：摘要不同但台账里所有调用都是 `completed` → 允许恢复（IF-04 落地前本用例断言「`Resume` 不返回错误」，把批次实际执行留给 IF-04 的用例覆盖）。
11. `TestResumeInFlightLedgerReadFailureRejects`：用一个 `ToolExecution` 返回错误的 `Durable` 包装（本卡新增 `brokenLedgerStore`）→ 恢复被拒，错误可匹配 `ErrInFlightUnrecoverable`。
12. `TestWorkflowAgentRejectsInFlight`（放 `agent/inflight_internal_test.go`，包 `agent`：工作流 Agent 要带存储只能用内部构造，现成写法见 [agent/durable_runtime_internal_test.go:161](../../agent/durable_runtime_internal_test.go#L161) 的 `snapshotConsistencySequential` 与 [agent/workflow.go:77](../../agent/workflow.go#L77) 的 `wrapWorkflowWithStore`）：`a.loop == nil` 的工作流 Agent `Resume` 一个在途头部 → 拒绝，且**没有**往历史里追加「未执行」的工具结果（对照现有工作流审批分支，[agent/hitl.go:193](../../agent/hitl.go#L193) 到 [agent/hitl.go:202](../../agent/hitl.go#L202) 会写「未执行」结果，在途路径不能照抄）。

**实现要求：**

- `agent/runtime.go`：`RunContext` 增加 `inFlight *inFlightBatch`（注释：与 `resumed` 并列，由 `Agent.Resume` 设置，被 `prepareRun` 消费一次）。`deeper`/`subRun`/`forBranch` 不复制。
- 新建 `agent/inflight.go` 内追加 `inFlightBatch` 结构（§4.8）与 `matchInFlightMessage`。
- `agent/hitl.go`：`Resume` 在 `preflightResumeTools` 调用点之前（[agent/hitl.go:93](../../agent/hitl.go#L93)）加入在途分流：
  - `if cp.InFlight != nil { ... }`，内部顺序：冲突检查（`cp.Pending != nil || cp.Pause != nil || cp.Completion != nil` → `ErrInFlightUnrecoverable`）、`len(approvals) > 0` → 拒绝、`a.loop == nil` → 拒绝、存储不是 `Durable` → 拒绝、`matchInFlightMessage(state.Messages, cp.InFlight.Calls)`（注意：状态克隆发生在句柄构造处，所以历史匹配要在 `cloneState` 之后做——**实施时把预检拆成两段**：认领前的只读校验做工具可解析性、摘要与台账扫描；认领后做历史匹配，失败时按现有模式 `ReleaseClaim` 再返回错误）。
  - 认领继续使用现有 `durable.Claim`（[agent/hitl.go:103](../../agent/hitl.go#L103)）与 `EffectivePauseID`。
  - 成功路径设置 `run.rc.continuedTurn = true`（与 IQ-03 一致）、`run.rc.inFlight = batch`，不设置 `replayBaseline`（对照 [agent/hitl.go:188](../../agent/hitl.go#L188)）。
  - `preflightInFlight` 的七项检查按 §4.8 的表实现；`inFlightMayExecute` 用 `durable.ToolExecution` 只读扫描（[checkpoint/durable.go:29](../../checkpoint/durable.go#L29)），编号用 `toolInvocation(cp.ThreadID, cp.Step, call)`（[agent/exectools.go:426](../../agent/exectools.go#L426)）。
  - 当前摘要：`l.resumeStepViewFor(...).Tools().Schemas()` 交给 `toolDigest`（[agent/stepview.go:511](../../agent/stepview.go#L511)），**不要**读视图里从快照复制的 `Digest().ToolDigest`（[agent/stepview.go:355](../../agent/stepview.go#L355)）。
- `agent/stepview.go`：抽出 `resumeStepViewFor(step int, calls []core.ToolCall, snapshot *checkpoint.StepSnapshot) *StepView`，`resumeStepView(rb)` 改为委托（[agent/stepview.go:340](../../agent/stepview.go#L340)）。这是 §2.5 第二项修正的落地位置。
- 新错误 `ErrInFlightUnrecoverable` 定义在 `agent/inflight.go`，文案见 §4.12；所有包装用 `%w`，错误串只带调用 ID 与工具名。

**验证命令：**

```bash
go test ./agent -run 'TestResumeInFlight|TestWorkflowAgentRejectsInFlight' -count=1
go test ./agent -count=1
go test -race ./agent -run 'TestResume' -count=1
go vet ./agent
gofmt -l agent/inflight.go agent/inflight_resume_test.go agent/inflight_internal_test.go agent/hitl.go agent/stepview.go agent/runtime.go
```

**完成标准：** 12 个用例通过（其中按实现要求被合并或跳过的用例，要在执行记录里写明理由，不留空用例）；现有 `TestResume*`、`TestPause*` 用例不改预期即通过；`-race`、`vet`、`gofmt` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 把在途分流放到 `Claim` 之后（预检失败留下认领） | `TestResumeInFlightPreflightBeforeClaim` |
| `matchInFlightMessage` 比较 `Args` | `TestResumeInFlightMatchesAssistantMessage` 的「参数不同」核心断言 |
| 摘要检查无条件执行（不看台账） | `TestResumeInFlightSummaryDriftAllowsPureReplay` |
| 摘要检查完全去掉 | `TestResumeInFlightSummaryDriftBlocksExecution` |
| 台账读取失败当作「不会执行」放行 | `TestResumeInFlightLedgerReadFailureRejects` |
| 在途分支设置 `replayBaseline` | `TestResumeInFlightDoesNotSetReplayBaseline` |
| 接受显式审批（只忽略不报错） | `TestResumeInFlightRejectsApprovals` |
| 用视图里复制的旧摘要做比较 | `TestResumeInFlightSummaryDriftBlocksExecution` |

### IF-04 恢复执行在途批次

**目标：** `prepareRun` 消费 `rc.inFlight`：在同一逻辑步骤执行批次，不重跑执行前检查，`completed` 回放、`claimed`/`unknown` 返回未知、`pending` 执行；结果先落盘再结束或续跑；发布 `ToolBatchRecovered`。

**先写的测试：** 新建 `agent/inflight_exec_test.go`（包 `agent_test`），夹具沿用 IF-03 的 `seedInFlightHead` 与 IF-02 的 `recordingTool`，并新增 `ledgerState(t, durable, threadID, step, call) checkpoint.ToolExecutionState` 读取 helper。用例：

1. `TestInFlightRecoveryExecutesPending`：头部在途、台账 `pending`；恢复后 `recordingTool` 恰好被调一次、历史里有一条含两个 `ToolResult` 的工具消息、新的头部 `InFlight==nil` 且 `Step==ib.step`、下一步从 `step+1` 开始（断言模型只被调用一次且发生在 `step+1`，或直接断言最终历史的顺序）。
2. `TestInFlightRecoveryReplaysCompleted`：预先把台账写成 `completed`（`durable.ClaimTool` + 完成记录随 `Commit` 落盘，或直接构造 `ToolExecution` 提交），恢复时 handler 零次调用、结果与存储里的一致、`AfterTool` 中间件计数为 0（`skipAfter` 生效，见 [agent/exectools.go:375](../../agent/exectools.go#L375)）。
3. `TestInFlightRecoveryReportsUnknown`：台账 `claimed`（用 `ClaimTool` 制造，之后不提交完成记录）→ handler 零次调用、结果是「结果未知」文本、恢复提交里该调用不写完成记录（对照现有 `TestToolOutcomeUnknown*` 系列的断言口径，见 [agent/exectools.go:360](../../agent/exectools.go#L360)）。
4. `TestInFlightRecoveryMixedBatch`：三个调用分别是 `completed`、`claimed`、`pending`；断言恰好三条结果、顺序与 `InFlight.Calls` 一致、`ToolBatchRecovered{Replayed:1, Executed:1, Unknown:1, Rejected:0}`。
5. `TestInFlightRecoverySkipsExecutionChecks`：一个统计型中间件记录 `CheckToolPolicy` 与 `BeforeTool` 的调用次数；恢复运行时两者计数都为 0（与现有审批恢复分支的对照：那条路径 `BeforeTool` 会计 1 次）。
6. `TestInFlightRecoveryUsesSameStepNumbers`：断言恢复后 `ToolExecution` 的 `InvocationID` 与崩溃前测试自己算出的编号一致（证明 `rlc.Step = ib.step`，[agent/exectools.go:426](../../agent/exectools.go#L426)）。
7. `TestInFlightRecoveryDirectiveEndsRun`：某个 handler 返回 `core.Stop` 控制指令；断言结果先提交（头部 `InFlight==nil`、历史含工具消息）、运行以该控制结束、发布过 `ToolBatchRecovered`。
8. `TestInFlightRecoveryCommitFailureKeepsHead`：`failCommitStore` 让恢复提交失败；断言运行失败、头部仍是原在途检查点、已执行的 handler 不会被重复执行（第二次恢复时该调用变成 `claimed`/`completed` 而不是重新执行）。
9. `TestInFlightRecoveryNoPauseEvents`：断言恢复过程没有 `Interrupted` 事件、没有第二个 `TurnStarted{Step: ib.step}`，下一步的 `TurnStarted` 属于 `ib.step+1`。
10. `TestInFlightRecoveryKeepsPhaseSemantics`：结合 IQ-03——恢复运行时 `Run.Steer(core.UserText("x"))`（`before_model`）在恢复后的第一个 Phase 1 投递；`next_turn` 的那条不投递。断言 `RunContext.continuedTurn` 生效。
11. `TestInFlightRecoveryFileStore`：用 `checkpoint.NewFile(t.TempDir())` 重复用例 1，证明跨重开的记录可读（配合 `newFileRecoveryStore` [checkpoint/file_recovery_test.go:653](../../checkpoint/file_recovery_test.go#L653) 的思路，但本用例在 `agent` 包内直接重开目录即可）。

**实现要求：**

- `agent/loop.go`：`prepareRun` 在现有 `rb` 分支之前插入 `if ib := rc.inFlight; ib != nil { ... }`（[agent/loop.go:123](../../agent/loop.go#L123) 之前），形状按 §4.8 与设计稿 §6.3.4：
  - `rlc := &LoopContext{RunContext: rc, Step: ib.step, MaxTurns: l.maxTurns, History: history}`，`rlc.view = l.resumeStepViewFor(ib.step, ib.calls, ib.snapshot)`，`rlc.stats = &batchStats{}`（§4.10）。
  - 调 `l.execTools(rc, rlc, ib.calls)`；`l.mw.ToolReject(rlc, rejects)` 保持与普通步骤相同的位置（批次汇合后、写检查点前，见 [agent/loop.go:508](../../agent/loop.go#L508) 的注释理由）。
  - `execErr != nil` 时先 `results = l.completeFailedBatch(rc, ib.step, ib.calls, results)`（IF-05 提供；本卡若先行合并，可先直接 `fail`，但需要在执行记录里标注）。
  - 结果非空则追加 `core.Message{Role: core.RoleTool, Parts: results}`；随后 `rc.State.Messages = history` 并提交 `commitOptions{Step: ib.step, Snapshot: ib.snapshot, Release: next.Kind != core.Continue}`（不带 `InFlight` 即清除）。
  - 提交成功后 `rc.publish(core.ToolBatchRecovered{...})`；`next.Kind != core.Continue` 时返回 `runOutcome{Result: core.Result{Message: ib.final}, Control: next}`；否则 `startStep = ib.step + 1`。
  - 不调 `CheckToolPolicy`、不调 `BeforeTool`、不调 `freezeStepView`、不调 `checkReplayDrift`、不发布 `Interrupted`。
- `agent/exectools.go`：`toolOutcome` 加 `class outcomeClass`；在返回点标注类别（§4.10）；`run(i,c)` 末尾 `lc.stats.add(out.class)`。`rejected(...)` 产生的 handler 错误结果要覆盖成 `classExecuted`（handler 已运行），未运行前的拒绝保持 `classRejected`。
- `agent/loopctx.go`：`LoopContext` 加 `stats *batchStats`（[agent/loopctx.go:13](../../agent/loopctx.go#L13)），`batchStats` 定义在 `agent/exectools.go`，`add` 对 nil 接收者安全、内部加锁。
- `core/event.go` + `core/eventjson.go`：`ToolBatchRecovered`（类型串 `tool_batch_recovered`），线字段新增 `replayed`、`executed`、`unknown`、`rejected`；注释写明四个计数是诊断值、不保证守恒；往返测试进 `core/stuckevent_test.go`。

**验证命令：**

```bash
go test ./agent -run 'TestInFlightRecovery' -count=1
go test ./core -run 'TestToolBatchRecovered' -count=1
go test ./agent ./checkpoint ./core ./eval -count=1
go test -race ./agent -run 'TestInFlight' -count=1
go vet ./agent ./core
```

**完成标准：** 11 个用例通过；IF-03 的用例 8、10 从「不报错」升级为完整断言；现有审批恢复与工具幂等回放测试不改预期即通过；`-race`、`vet`、`gofmt` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 恢复时补跑 `CheckToolPolicy`/`BeforeTool` | `TestInFlightRecoverySkipsExecutionChecks` |
| `rlc.Step` 用 `ib.step+1` | `TestInFlightRecoveryUsesSameStepNumbers`、用例 2 的回放命中 |
| 去掉 `resumeStepViewFor` 直接 `lc.view = nil` | 全部执行用例（`execTools` 在 [agent/exectools.go:43](../../agent/exectools.go#L43) 报错） |
| `completed` 回放时不跳过 `AfterTool` | `TestInFlightRecoveryReplaysCompleted` |
| `claimed` 自动重试 | `TestInFlightRecoveryReportsUnknown` |
| 恢复提交带 `InFlight` | 用例 1、7、8 的「头部已清除」断言 |
| 事件在提交前发布 | 用例 7、8 |
| 结果顺序按完成先后拼装 | 用例 4 |
| 续跑第一步设 `newTurn=true` | `TestInFlightRecoveryKeepsPhaseSemantics` |

### IF-05 批次中途失败时补齐结果

**目标：** 写过在途检查点的批次在执行中途失败时，先给每个计划调用补一条结果，再走 `fail`；失败检查点末尾是工具结果消息，助手调用消息不再被 `seam` 删掉。

**先写的测试：** 新建 `agent/inflight_failure_test.go`（包 `agent_test`），沿用 IF-02/IF-04 夹具；工具夹具需要「一个成功、一个报错」的组合（`okTool` + `errTool`）与「第二个调用 panic 前先返回错误」两类。

1. `TestFailedInFlightBatchKeepsPairing`：两个调用，第一个返回结果、第二个返回错误；断言运行失败，但头部检查点的 `State.Messages` 末尾是含两条 `ToolResult` 的工具消息，倒数第二条是带两个调用的助手消息；`InFlight==nil`；`errors.Is(runErr, toolErr)`。
2. `TestFailedInFlightBackfillUsesLedger`：手工把第二个调用的台账置为 `claimed`（先 `ClaimTool` 不落完成记录），批处理时第一个调用报错触发失败路径；断言第二个调用的结果是「结果未知」文本而不是「未执行」，且没有第二次 handler 调用。
3. `TestFailedInFlightBackfillReplaysCompleted`：某调用已 `completed`（随前一次提交落盘），失败补齐回放它的结果。
4. `TestFailedInFlightBackfillPendingIsNotExecuted`：某调用仍是 `pending`（handler 从未启动）；断言它的结果是「未执行」文本，且 handler 计数为 0（**不能**在补齐时执行）。
5. `TestFailedInFlightLedgerReadFailure`：`brokenLedgerStore`（IF-03 引入）让 `ToolExecution` 报错；断言未报告的调用结果是「未执行 + 读取失败」文本，历史配对仍合法（调用数与结果数相等）。
6. `TestFailedInFlightBackfillWithoutDurable`：非持久存储没有在途头，因此该路径不启用；断言失败检查点末尾被 `seam` 裁成助手消息之前的形态（即现有行为回归，证明开关只影响写过在途的批次）。
7. `TestFailedInFlightAfterCancel`：运行取消导致批次中途停止（`cancelledResult` 路径，[agent/exectools.go:297](../../agent/exectools.go#L297)）；断言取消后仍补齐了结果并写了失败检查点（补齐读取用 `context.WithoutCancel`，提交本身已有取消兜底，见 [agent/durable_runtime.go:392](../../agent/durable_runtime.go#L392)）。
8. `TestFailedInCommitLeavesRecoverableHead`：失败检查点也写不进去（`failCommitStore` 同时让在途提交成功、后续提交失败）；断言头部仍在途，随后 `Resume` 走 IF-04 路径：`claimed` 返回未知、`pending` 执行、已 `completed` 回放，不重复副作用。

**实现要求：**

- 新建 `agent/inflight_failure.go` 的 `completeFailedBatch`（§4.9）。要点：
  - 先按 `CallID` 建已报告结果索引（结果类型断言 `core.ToolResult`），保持 `calls` 顺序输出。
  - `durable` 不存在时统一 `errResult(call, "not executed: the run stopped before this call started")`。
  - 台账分支：`completed` 用 `resultFromExecution`，失败退化为「stored tool result is invalid…」；`claimed`/`unknown` 用 `unknownToolResult(call, ex.IdempotencyKey)`；其余用「未执行」；读取错误用「未执行 + 原因」。
  - 读取上下文：`context.WithoutCancel(rc.Context)`。
- `agent/loop.go` 两处接线：
  - 普通步骤：`execErr != nil` 且本步写过在途检查点时，在追加结果之前把缺失结果补进 `results`（[agent/loop.go:509](../../agent/loop.go#L509) 到 [agent/loop.go:513](../../agent/loop.go#L513)）。需要知道「本步写过在途」：`runStep` 内在写入块成功时置局部 `wroteInFlight := true`，不要读全局状态。
  - 恢复路径：IF-04 的 `execErr != nil` 分支同样先补齐（`ib.calls` 已知）。
- 补齐只影响写进历史的消息；不向台账写「未执行」结果（避免锁死未来重试），完成记录仍由现有 `addCompletedTool` 路径提交（[agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139)）。
- 不改 `seam`（[agent/loop.go:653](../../agent/loop.go#L653)）与 `fail` 的其余行为。

**验证命令：**

```bash
go test ./agent -run 'TestFailedInFlight|TestFailedInCommit' -count=1
go test ./agent ./checkpoint ./eval -count=1
go test -race ./agent -run 'TestFailedInFlight' -count=1
go vet ./agent
gofmt -l agent/inflight_failure.go agent/inflight_failure_test.go agent/loop.go
```

**完成标准：** 8 个用例通过；现有失败路径与 `seam` 相关测试（`agent/failure_test.go`、`agent/phase1_failure_test.go`）不改预期即通过；`-race`、`vet`、`gofmt` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 去掉补齐调用，直接 `fail` | 用例 1（末尾变成助手消息）、用例 2、3、4 |
| 补齐时对 `pending` 执行 handler | 用例 4 |
| `claimed` 当作未执行 | 用例 2 |
| 把补齐的「未执行」结果写进台账 | 用例 3 之后再次恢复时该调用不再回放（补一条断言或扩到用例 8） |
| 用 `rc.Context` 而不是 `WithoutCancel` 读台账 | 用例 7 |
| 补齐输出按完成顺序而非 `calls` 顺序 | 用例 1 的结果顺序断言 |
| 用全局变量判断「本步写过在途」 | 用例 6（非持久存储被误判） |

### IQ-05 模型调用抢占

**目标：** 开启后，`SteerWith(Preempt=true)` 取消当前这次模型调用，在同一步把 `before_model` 临时消息追加进历史、写一个 `Release:false` 的中间检查点、重新发起调用；发布 `ModelPreempted` 与 `InputDelivered{Preempted:true}`；成功优先与运行取消优先成立；默认关闭。

**先写的测试：**

1. 新建 `agent/preempt_internal_test.go`（包 `agent`）：
   - `TestBeginEndModelAttempt`：`beginModelAttempt` 返回的上下文可取消且派生自传入上下文；`endModelAttempt` 后登记被清空、返回的 `preempted` 与 `requestPreemption` 的结果一致；对已结束 attempt 调用 `requestPreemption` 返回 false。
   - `TestRequestPreemptionOncePerStep`：同一步第二次 `requestPreemption` 返回 false；步骤号前进后可以再次成功。
   - `TestRunContextDerivedContextsDoNotShareAttempt`：`deeper()`/`forBranch()` 出来的上下文里 `requestPreemption` 为 false（没有活动尝试）。
2. 新建 `agent/preempt_test.go`（包 `agent_test`），模型夹具用 `turnGatedModel` + `modelGate`（[agent/task_loop_test.go:23](../../agent/task_loop_test.go#L23)、[38](../../agent/task_loop_test.go#L38)），并在 `Generate` 内监听 `ctx.Done()` 后以「无终稿、无错误」结束流（模拟遵守取消的提供方）；另需要一个 `ignoreCancelModel` 证明成功优先。
   - `TestPreemptCancelsAndRetriesSameStep`：第一次调用阻塞；`SteerWith(core.UserText("stop"), SteerOptions{Preempt:true, Reason:"user"})`；断言第二次请求的 `Messages` 含该文本、票据 `Step==0`（同一步）、`Delivered==true`；`TurnStarted` 只出现一次（`[0]`），事件顺序是 `ModelPreempted` → `InputDelivered`；最终结果来自第二次调用。
   - `TestPreemptEventFields`：抢占前已发布过 `MessageDelta` 时 `ModelPreempted.Emitted==true`、`Count` 等于本次进入重试的消息数、`Reason` 透传；未发布增量时 `Emitted==false`。
   - `TestPreemptSuccessWins`：`ignoreCancelModel` 收到取消后仍返回终稿；断言用终稿、只有一次模型调用记录为「重试未发生」、没有 `ModelPreempted` 之后的第二次请求、消息留在队列并在下一个投递点进入（配合 IQ-03 的阶段断言）。
   - `TestPreemptCancelledByRunWins`：`SteerWith(Preempt)` 同时 `Run.Cancel()`；断言运行按取消结束、没有第二次调用、票据 `State()==rejected`。
   - `TestPreemptOnlyBeforeModelPhase`：队列里同时有 `before_model` 与 `after_tools`、`next_turn` 各一条；抢占后第一次重试请求只含 `before_model` 那条，其余两条在后续投递点出现。
   - `TestPreemptDisabledReturnsError`：不开选项时 `SteerWith(..., Preempt:true)` → `ErrPreemptionDisabled`、`nil` 票据，且队列无变化（后续投递不含该文本）。
   - `TestPreemptWrongPhaseReturnsError`：开启选项但阶段 `next_turn` → `ErrPreemptionPhase`。
   - `TestPreemptWhileNoActiveCallQueues`：在工具执行阶段（用 `steeringBlockingTool`）请求抢占 → 不报错，消息按普通 `before_model` 在下一步 Phase 1 投递。
   - `TestPreemptTwiceInOneStep`：同一步连续两次抢占请求 → 只取消一次；第二条消息排队等下一次投递。
   - `TestStreamModelWithoutFinal`：一个只产出 `Partial` 就结束流的模型；未抢占时运行以 `ErrModelNoFinal` 失败；抢占开启且被取消时进入重试。这条覆盖 §8.2 的行为变化 1。
   - `TestPreemptDoesNotReportOnError`：统计型中间件记录 `OnError` 次数；取消路径（抢占与运行取消）都不调用它。这条对应 §8.2 的行为变化 2。
   - `TestPreemptCheckpointFailureFailsRun`：`failCommitStore` 让抢占重试的中间提交失败；断言没有第二次模型调用、票据 `rejected`、运行以该错误失败。
   - `TestPreemptAndOverflowRetryCountedSeparately`：第一次被抢占、重试请求返回超限错误且有恢复中间件；断言两条恢复路径都生效、事件各一条、模型共调用三次。
   - `-race`：抢占与模型正常结束并发跑 20 次（`for i:=0;i<20;i++` 子测试），断言每次要么成功要么一次重试，不出现两次成功消费。
3. `core/stuckevent_test.go` 新增 `TestModelPreemptedEventRoundTrip` 与 `omitempty` 用例。

**实现要求：**

- `agent/runtime.go`：按 §4.5 加 `modelAttempt` 类型、`modelMu`、`modelAttempt`、`preemptedStep int`（初值 `-1`，在句柄构造处赋值——注意 `RunContext` 是逐字段构造的，见 [agent/runtime.go:65](../../agent/runtime.go#L65) 起的三个派生函数，**不要**用 struct-copy），以及 `beginModelAttempt`、`endModelAttempt`、`requestPreemption`。
- `agent/input.go`：`runStep` 的 Phase 1 保持用 `takeVolatile`；新增抢占分支用 `q.takeVolatilePhase(core.InputBeforeModel)`（即 `steeringQueue.takePhase`）。
- `agent/steering.go`：`SteerTicket`/队列不改；`pushWith` 已在本卡前完成校验，本卡只把 `Preempt` 从「校验通过」升级为「尝试触发取消」：`RunContext.SteerWith` 在 `pushWith` 成功后，若 `opts.Preempt && rc.cfg 开启` 则调用 `rc.requestPreemption(opts.Reason)`。配置读取方式：`RunContext` 增加未导出 `preemptionEnabled bool`（由 `newRunHandleWithID` 写入，与 IQ-02 的队列字段二选一，**实施时统一放在 `RunContext`**，队列的 `preemptionEnabled` 字段改由 `pushWith` 的参数传入或保留 IQ-02 写法——以最终不留重复状态为准，执行记录里说明选择）。
- `agent/loop.go`：
  - `streamModel` 改成 `gotFinal` 版本（§4.5 与设计稿 §5.4.6），并在 `genCtx.Err() != nil` 时跳过 `l.mw.OnError`（现有无条件调用在 [agent/loop.go:553](../../agent/loop.go:553)）。
  - 模型调用循环（[agent/loop.go:335](../../agent/loop.go#L335) 起的 `for`）在 `err == nil && ok` 之后插入抢占分支，顺序为：成功优先 → `preempted && rc.Context.Err()==nil` → 抢占重试 → `err==nil` 时补 `ErrModelNoFinal` → 既有超限恢复 → `fail`。
  - 抢占重试块：取 `before_model` 条目、追加历史、发布 `ModelPreempted`、从 `view` 重建 `retryReq`（`System`/`Tools`/`Options` 不变，`Messages` 换新历史，见 [agent/loop.go:355](../../agent/loop.go#L355)）、`freezeStepView`、写 `Release:false` 检查点、`settleDelivered(entries, step)`、发布 `InputDelivered{Source:volatile, Phase:before_model, Preempted:true}`、更新 `lc.view/lc.Request/lc.History/req`、`genCtx = l.mw.ModelContext(lc, rc.Context)` 并重新 `beginModelAttempt`、`continue`。
  - 冻结失败或提交失败时 `settleUndelivered(entries, ErrRunClosed)` 后 `fail`。
  - 抢占不消耗 `attempts`（超限预算）计数，两个计数器分开。
- `agent/errors`：新增 `ErrPreemptionDisabled`、`ErrPreemptionPhase`、`ErrModelNoFinal`（§4.12）。
- 事件：`ModelPreempted{Step, Emitted, Count, Reason}`，类型串 `model_preempted`；`eventWire` 加 `emitted`。
- 文档：在 `core/event.go` 的 `MessageDelta`/`MessageDone` 注释上补一句「一次尝试被取消后不会有 `MessageDone`，消费者收到 `ModelPreempted` 要丢弃本次已渲染的增量」，并在 IF-07 的示例里演示。

**验证命令：**

```bash
go test ./agent -run 'TestPreempt|TestBeginEndModelAttempt|TestRequestPreemption|TestRunContextDerivedContexts|TestStreamModelWithoutFinal' -count=1
go test ./core -run 'TestModelPreempted|TestInputDelivered' -count=1
go test ./agent ./core ./checkpoint ./eval -count=1
go test -race ./agent -run 'TestPreempt|TestSteer|TestContextOverflow' -count=1
go vet ./agent ./core
gofmt -l agent/preempt_test.go agent/preempt_internal_test.go agent/runtime.go agent/loop.go agent/input.go agent/steering.go core/event.go core/eventjson.go
```

**完成标准：** 14 个外部用例 + 3 个内部用例通过；`TestContextOverflow*`（P0-4）不改预期即通过，证明两条恢复路径共存；`-race` 通过；`gofmt`、`vet` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 抢占判断放在 `ok` 之前（抢占优先于成功） | `TestPreemptSuccessWins` |
| 去掉 `rc.Context.Err()==nil` 守卫 | `TestPreemptCancelledByRunWins` |
| 重试时重跑 `ModifyRequest` | 需要一条计数断言——用 `TestPreemptOnlyBeforeModelPhase` 之外的 `countingMW`；若无对应断言，补一条并写进执行记录 |
| 抢占重试取走全部阶段 | `TestPreemptOnlyBeforeModelPhase` |
| 中间检查点 `Release:true` | `TestPreemptCheckpointFailureFailsRun` 之后的租约断言（若无对应断言，补一条） |
| 每步一次限制去掉（只判活动尝试） | `TestPreemptTwiceInOneStep` |
| `streamModel` 仍无条件 `ok=true` | `TestStreamModelWithoutFinal` |
| 取消路径仍调 `OnError` | `TestPreemptDoesNotReportOnError` |
| 抢占后票据步骤号写 `step+1` | `TestPreemptCancelsAndRetriesSameStep` |
| 未开启时也执行取消 | `TestPreemptDisabledReturnsError` |

### IF-06 状态、投影与队列语义

**目标：** `RunStatus.InFlight` 独立报告在途；查询与侧车投影一致；队列不会把在途当暂停；文档写清「在途不是暂停」。

**先写的测试：** 新建 `agent/inflight_status_test.go`（包 `agent_test`）：

1. `TestRunStatusReportsInFlight`：种子一个在途头部（复用 IF-03 的 `seedInFlightHead`）；断言 `RunStatus` 的 `InFlight==true`、`Paused==false`、`Latest.InFlight != nil`。
2. `TestRunStatusPausedUnchanged`：审批暂停头部 → `Paused==true`、`InFlight==false`；`max_tokens` 或普通步骤头部 → 两者都 false。
3. `TestQueueWorkerDoesNotMarkInFlightAsPaused`（`queue` 包，照现有 `queue` 测试的夹具写法）：worker 驱动一个在批次中途被杀的运行，运行结果 `RunResult.Paused==false`；任务记录不因在途变成 `StatusPaused`（[queue/run_worker.go:360](../../queue/run_worker.go#L360)）。若现有队列测试夹具难以制造在途头（需要真实崩溃），退化为直接对 `RunResult{Paused:false}` 调用 `terminalTaskRecord` 的表驱动单测，并在执行记录里说明。
4. `TestCheckpointMetadataInFlightVsPaused`（`checkpoint` 包已在 IF-01 覆盖投影，本卡补一条 `agent` 侧读取）：`agent.Session.History`（[agent/session.go:77](../../agent/session.go#L77)，返回 `checkpoint.MetadataPage`）读到的头部元数据里 `Paused` 与 `InFlight` 互斥；`Session.GetHead`（[agent/session.go:61](../../agent/session.go#L61)）读到的原始头部 `InFlight != nil`。
5. `TestRecordCompletionRejectsInFlightHead`（紧挨现有 `TestRecordCompletionRequiresOwnBoundary` [agent/terminal_test.go:579](../../agent/terminal_test.go#L579)）：在途头部上 `Agent.RecordCompletion` 被拒绝；对照 `TestRecordCompletionRejectsInvalidRun` [agent/terminal_test.go:525](../../agent/terminal_test.go#L525) 的写法。
6. `TestSessionForkFromRejectsInFlight`（对照 `TestSessionForkDropsExternalFiles` [agent/session_test.go:416](../../agent/session_test.go#L416)）：`Session.ForkFrom`（[agent/session.go:106](../../agent/session.go#L106)）对带 `InFlight` 的父检查点返回错误，证明拒绝在 `agent` 侧也生效而不只在 `checkpoint.Fork`。
7. `TestPrepareRejectsInFlightHead`（`agent` 包内部测试，放 `agent/inflight_status_internal_test.go`）：存储只实现 `checkpoint.Checkpointer`（不是 `ThreadCoordinator`）、头部带 `InFlight` 时，一次带输入的新运行在 `Run.prepare` 的 `restore` 分支就被拒绝，错误可匹配 `checkpoint.ErrResumeRequired`（位置见 [agent/run_prepare.go:59](../../agent/run_prepare.go#L59) 到 [agent/run_prepare.go:63](../../agent/run_prepare.go#L63)）；任何工具 handler 都未启动。

**实现要求：**

- `agent/run_status.go`：`RunStatus` 增加 `InFlight bool`，注释写明「头部是一个工具批次执行前的边界；不需要人工审批，下一步是 `Agent.Resume`」；`RunStatus` 计算处新增 `status.InFlight = latest.Completion == nil && latest.InFlight != nil`，现有 `Paused` 表达式一字不改（[agent/run_status.go:66](../../agent/run_status.go#L66)）。
- `agent/terminal.go`：`buildTerminalRecord` 的拒绝条件加 `base.InFlight != nil`（[agent/terminal.go:176](../../agent/terminal.go#L176)），错误文案说明「批次未解决，先恢复」。
- `agent/run_prepare.go`：非协调存储分支在 `restore` 之后检查 `latest.InFlight != nil` → 返回包装 `checkpoint.ErrResumeRequired` 的错误（位置：[agent/run_prepare.go:59](../../agent/run_prepare.go#L59) 到 [agent/run_prepare.go:63](../../agent/run_prepare.go#L63)）。注意这条只在 `restoreCurrent` 为真的新运行路径生效，协调存储路径由 `BeginRun` 负责（IF-01）。
- `queue/run_worker.go`：不改代码，只补回归测试与注释：`recordCheckpoint` 的 `detail.Paused` 跳过条件（[queue/run_worker.go:286](../../queue/run_worker.go#L286)）与 `terminalTaskRecord` 的映射（[queue/run_worker.go:360](../../queue/run_worker.go#L360)）都不看 `RunStatus.InFlight`。
- 文档：`docs/` 里已有的状态/暂停说明处补一段「暂停 vs 在途」的对照（设计稿 §6.7 的表可直接引用），并在 `agent/run_status.go` 顶部注释里指向该表。

**验证命令：**

```bash
go test ./agent -run 'TestRunStatus|TestRecordCompletion|TestSessionForkFrom|TestPrepareRejectsInFlight|TestInFlight' -count=1
go test ./queue -count=1
go test ./agent ./checkpoint ./queue -count=1
go vet ./agent ./queue
gofmt -l agent/run_status.go agent/terminal.go agent/run_prepare.go agent/inflight_status_test.go
```

**完成标准：** 7 个用例通过；`queue` 全量通过；`RunStatus` 既有断言（`agent/run_status_test.go`）与既有 `TestRecordCompletion*` 用例不改预期即通过；`vet`、`gofmt` 干净。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| `Paused` 表达式并入 `InFlight` | `TestRunStatusReportsInFlight`、`TestRunStatusPausedUnchanged` |
| 去掉终态记录的在途拒绝 | `TestRecordCompletionRejectsInFlightHead` |
| 非协调存储不检查在途 | `TestPrepareRejectsInFlightHead` |
| `buildTerminalRecord` 只查 `Pause` 不查 `Pending` | 既有用例（回归守卫） |

### IF-07 崩溃注入矩阵、示例与全量门禁

**目标：** 用一张崩溃时间表覆盖在途边界的六个时刻；补一个可运行示例演示阶段投递与在途恢复；README 增补；跑全量门禁。

**先写的测试：** `agent/inflight_crash_test.go`（包 `agent_test`），每个时刻一个用例，统一断言「恢复后每个计划调用恰好一条结果、无重复副作用、头部最终无 `InFlight`」：

1. `TestCrashBeforeInFlightCommit`：在途提交失败（`failCommitStore` 注入在第一次 `Commit`）→ handler 零调用；重新运行从模型步开始，工具被重新规划（允许调用编号变化），无孤儿执行记录被自动执行。
2. `TestCrashAfterCommitBeforeHandlers`：提交成功后立刻取消运行 → 恢复时 `pending` 调用被执行一次。
3. `TestCrashMidBatch`：第一个调用完成、第二个未启动时中断（`errTool`/取消组合）→ 恢复后第一个回放、第二个执行。
4. `TestCrashAfterBatchBeforePhase4`：批次完成但 Phase 4 提交失败 → 恢复时全部回放，历史不重复追加工具消息。
5. `TestCrashAfterPhase4`：普通头部无 `InFlight`，恢复后直接进模型步，不再执行批次。
6. `TestCrashDuringRecoveryCommit`：恢复提交失败 → 再次恢复仍按台账处理，`claimed` 不重试。
7. `TestInFlightSurvivesFileStoreReopen`：`checkpoint.NewFile(t.TempDir())` 写入在途头，关闭句柄、重开目录（`checkpoint.NewFile(dir)` 再构造一次），`RunStatus.InFlight==true`，`Resume` 成功执行。
8. `TestInFlightMatrixBackwardChecks`：见下方反向验证，把五类「去掉守卫」的断言集中登记，避免散落在各卡。

**示例：** 新建 `examples/input-phases/main.go` 与 `examples/inflight-recovery/main.go`（各带 `go run` 可用、离线 mock 模型、不联网）：

- `input-phases`：演示四种阶段的投递时点与票据状态变化，打印 `TurnStarted`、`InputDelivered`（source/phase/count）与票据状态。
- `inflight-recovery`：用 `checkpoint.NewFile(t.TempDir)` 等价的临时目录演示——第一次运行在批次中途因工具报错失败，打印头部 `InFlight` 与台账状态；随后 `Resume` 恢复并打印 `ToolBatchRecovered` 计数与最终历史。两个例子都要在结束时打印退出码 0，且不留临时文件（用 `defer os.RemoveAll`）。

**文档：** README 示例表新增两行（格式照 [README.md:187](../../README.md#L187) 的 `task-loop` 行），并在「会话快照与并发」附近新增一节「输入的投递阶段与崩溃恢复」，正文包含：四个阶段的语义（最晚投递时间）、`Steer`/`SteerWith` 差别、抢占默认关闭、在途与暂停的对照（引用设计稿 §6.7）、恢复入口是 `Agent.Resume`、以及「`claimed` 需要人工查询外部系统」这一运维含义。若 `docs/` 有事件目录清单，四个新事件都要登记。

**全量门禁（本卡最后一步，逐条落盘到 `.scratch/if07-gate.log` 再解析，不要用 `| tail`）：**

```bash
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./agent ./checkpoint ./core ./queue ./middleware -count=1
gofmt -l agent checkpoint core queue examples/input-phases examples/inflight-recovery
git diff --check
git status --short
go run ./examples/input-phases
go run ./examples/inflight-recovery
```

`gofmt -l` 只允许列出既有 CRLF 文件（本仓库既有现象，见 `docs/` 相关说明），新文件必须是 LF 且不出现在输出里；不要为消除告警重排既有文件。`git status --short` 用来确认每笔提交只含该卡列出的文件。

**完成标准：** 8 个崩溃用例通过；两个示例退出码 0 且输出可读；全仓 build/vet/test/`-race` 绿；`gofmt` 与 `git diff --check` 无本计划引入的问题；README 与事件文档更新。

**反向验证（对应设计稿 §10.6 第 7 条，逐条实施并把结果写进 §10.2）：**

| 去掉的守卫 | 应当失败的用例 |
| --- | --- |
| 去掉在途提交 | `TestCrashAfterCommitBeforeHandlers`、`TestInFlightRecoveryExecutesPending`（头部没有批次，恢复路径不触发） |
| 去掉 `takeReady` 的 `next_turn` 保留 | `TestPhaseDeliveryAfterToolBatchKeepsNextTurn` |
| 去掉每步一次抢占限制 | `TestPreemptTwiceInOneStep`（观察到两次取消） |
| 去掉「可能执行才严格比较摘要」的判断 | `TestResumeInFlightSummaryDriftAllowsPureReplay` |
| 去掉失败补齐 | `TestFailedInFlightBatchKeepsPairing`（末尾出现助手工具调用消息） |
| 去掉 `sameInboxMessage` 的阶段比较 | `TestInboxSameIDDifferentPhaseConflicts` |
| 去掉 `RequiresResume`（回到只判 `Pending`/`Pause`） | `TestSnapshotConsistencyBeginRunInFlight` |
| 去掉 `Fork` 的在途拒绝 | `TestForkRejectsInFlight` |

## 7. 验收矩阵

编号对应设计稿 §7 的不变量；「证据」列是可直接检索的测试名。

| 编号 | 验收点 | 卡片 | 证据 |
| --- | --- | --- | --- |
| V1 | 阶段闭集：空值规范化、非法值拒绝 | IQ-01 | `TestNormalizeInputPhase` |
| V2 | 队列按条件取走且保持剩余顺序 | IQ-02 | `TestSteeringQueueTakeReadyKeepsNextTurn`、`TestSteeringQueueTakePhase` |
| V3 | 票据恰好结算一次、三态可查、`Outcome()` 兼容 | IQ-02 | `TestSteerTicketStateLifecycle` |
| V4 | `Steer`/`Deliver` 默认行为不变 | IQ-02、IQ-04 | `TestSteerKeepsLegacyDeliveryTiming`、`TestDurableInboxDelivery` |
| V5 | `next_turn` 不在 `newTurn=false` 的 Phase 1 投递 | IQ-03 | `TestPhaseDeliveryAfterToolBatchKeepsNextTurn`、`TestMaxTokensContinuationIsNotNewTurn` |
| V6 | 终稿后 `settleTurn` 投递全部阶段 | IQ-03 | `TestPhaseDeliveryAtFinalAnswerSettlesAll` |
| V7 | 临时消息先于持久消息 | IQ-03 | `TestVolatileInputsPrecedeDurableInputs` |
| V8 | `InputDelivered` 字段（source/phase/count/IDs/preempted）正确 | IQ-03 | `TestInputDeliveredEventFields`、`TestInputDeliveredEventRoundTrip` |
| V9 | 持久消息只有提交成功后才离开 `Inbox`，至少一次 | IQ-02、IQ-04 | `TestInputQueueTakeDurableFiltersPhaseAndSkipsAcks`、`TestInFlightConfirmsThisStepAcks` |
| V10 | 同 ID 同消息不同阶段是冲突；旧记录按 `before_model` | IQ-04 | `TestInboxSameIDDifferentPhaseConflicts`、`TestInboxPhaseRoundTripMemoryAndFile` |
| V11 | `InFlight` 与 `Pending`/`Pause`/`Completion` 互斥，且校验在存储层 | IF-01 | `TestInFlightCoexistsWithNothingElse` |
| V12 | 在途要求快照、步骤一致、调用非空且名字已广播 | IF-01 | `TestInFlightRequiresSnapshot` |
| V13 | 旧提交摘要字节不变；新字段进入摘要且顺序敏感 | IF-01 | `TestCommitDigestOmitsInFlightWhenAbsent`、`TestInFlightDigestRetry` |
| V14 | 克隆隔离参数字节 | IF-01 | `TestInFlightCloneIsolatesArgs` |
| V15 | `BeginRun`、`prepare`、`Fork`、终态记录都拒绝在途头 | IF-01、IF-06 | `TestSnapshotConsistencyBeginRunInFlight`、`TestForkRejectsInFlight`、`TestRecordCompletionRejectsInFlightHead`、`TestPrepareRejectsInFlightHead` |
| V16 | 在途写入发生在全部执行前检查之后、handler 之前 | IF-02 | `TestInFlightCheckpointWrittenBeforeHandlers`、`TestInFlightRejectedBeforeExecutionChecksPass` |
| V17 | 在途记录的是改写后参数，助手消息保留原始参数 | IF-02 | `TestInFlightCallsUseRewrittenArgs` |
| V18 | 提交失败则 handler 零启动 | IF-02 | `TestInFlightCommitFailureBlocksHandlers` |
| V19 | 只对持久化且顶层运行写入；开关可关 | IF-02 | `TestInFlightSkippedForPlainCheckpointer`、`TestInFlightNotWrittenForSubRun`、`TestInFlightDisabledByOption` |
| V20 | 在途提交不释放占用；确认同批生效 | IF-02 | `TestInFlightCommitKeepsClaim`、`TestInFlightConfirmsThisStepAcks` |
| V21 | 恢复预检：助手消息按 ID/名称/顺序匹配，参数不参与 | IF-03 | `TestResumeInFlightMatchesAssistantMessage` |
| V22 | 恢复预检拒绝：无工具表、非持久存储、显式审批、工作流 Agent、台账读取失败 | IF-03 | `TestResumeInFlightRejectsApprovals`、`TestResumeInFlightNeedsDurableStore`、`TestWorkflowAgentRejectsInFlight`、`TestResumeInFlightLedgerReadFailureRejects` |
| V23 | 摘要漂移只在可能执行时阻止恢复 | IF-03 | `TestResumeInFlightSummaryDriftBlocksExecution`、`TestResumeInFlightSummaryDriftAllowsPureReplay` |
| V24 | 恢复同一步执行，不重跑执行前检查 | IF-04 | `TestInFlightRecoveryUsesSameStepNumbers`、`TestInFlightRecoverySkipsExecutionChecks` |
| V25 | `completed` 回放且跳过 `AfterTool`；`claimed`/`unknown` 不执行；`pending` 执行一次 | IF-04 | `TestInFlightRecoveryReplaysCompleted`、`TestInFlightRecoveryReportsUnknown`、`TestInFlightRecoveryExecutesPending` |
| V26 | 每个计划调用恰好一条结果，顺序与批次一致 | IF-04、IF-05 | `TestInFlightRecoveryMixedBatch`、`TestFailedInFlightBatchKeepsPairing` |
| V27 | 失败检查点末尾是工具消息，无悬空助手调用 | IF-05 | `TestFailedInFlightBatchKeepsPairing`、`TestFailedInFlightBackfillPendingIsNotExecuted` |
| V28 | `ToolBatchInFlight`/`ToolBatchRecovered` 在提交成功后发布 | IF-02、IF-04 | `TestInFlightEventPublishedAfterCommit`、`TestInFlightRecoveryDirectiveEndsRun` |
| V29 | `RunStatus.InFlight` 与 `Paused` 互斥；主存储与侧车投影一致 | IF-06、IF-01 | `TestRunStatusReportsInFlight`、`TestFileMetadataRebuildInFlight` |
| V30 | 抢占默认关闭；只对临时 `before_model` 生效；每步最多一次 | IQ-05 | `TestPreemptDisabledReturnsError`、`TestPreemptOnlyBeforeModelPhase`、`TestPreemptTwiceInOneStep` |
| V31 | 成功优先、运行取消优先 | IQ-05 | `TestPreemptSuccessWins`、`TestPreemptCancelledByRunWins` |
| V32 | 抢占重试不重跑请求侧中间件，写 `Release:false` 中间检查点、步骤号不变 | IQ-05 | `TestPreemptCancelsAndRetriesSameStep`、`TestPreemptCheckpointFailureFailsRun` |
| V33 | 被抢占尝试不发布 `MessageDone`，`ModelPreempted.Emitted` 正确 | IQ-05 | `TestPreemptEventFields` |
| V34 | `streamModel` 无终稿返回 `ok=false`；取消不调 `OnError` | IQ-05 | `TestStreamModelWithoutFinal`、`TestPreemptDoesNotReportOnError` |
| V35 | 抢占与超限恢复各自计数、可共存 | IQ-05 | `TestPreemptAndOverflowRetryCountedSeparately` |
| V36 | 崩溃矩阵六个时刻均可恢复且不重复副作用 | IF-07 | `TestCrashBeforeInFlightCommit` 到 `TestCrashDuringRecoveryCommit` |
| V37 | 文件存储重开后在途记录可读可恢复 | IF-07 | `TestInFlightSurvivesFileStoreReopen` |
| V38 | 旧数据（无 `in_flight`、无 `phase`）正常读取 | IF-01、IQ-04 | `TestLegacyCheckpointDigestUnchanged`、`TestInboxLegacyRecordReadsAsBeforeModel` |
| V39 | 无数据竞争 | 全部 | `go test -race ./agent ./checkpoint` |
| V40 | 示例与文档反映新语义 | IF-07 | `go run ./examples/input-phases`、`go run ./examples/inflight-recovery` |

## 8. 风险、兼容性与测试环境

### 8.1 API 兼容

保持：`Run.Steer`、`RunContext.Steer`、`Agent.Deliver`、`SteerTicket.Outcome`、`RunStatus` 既有字段语义、`checkpoint.Fork`/`BeginRun` 对旧记录的行为。新增：`SteerWith`、`DeliverWith`、`core.InputPhase/InputSource/InputState`、`checkpoint.InFlightBatch/Checkpoint.InFlight/RequiresResume`、`WithInputPreemption`、`WithInFlightCheckpoint`、四个事件、`RunStatus.InFlight`、`CheckpointMetadata.InFlight`。构造函数命名沿用仓库既有 `New*` 习惯，本计划不引入 `Open*`/`Init*` 命名的新构造函数。

### 8.2 行为变化（需要进发布说明）

1. `streamModel` 在没有终稿时返回 `ok=false`，调用方报 `ErrModelNoFinal`；以前返回空响应并当成成功（[agent/loop.go:550](../../agent/loop.go#L550)、[agent/loop.go:565](../../agent/loop.go#L565)）。
2. 上下文已取消时不再调用 `OnError`（[agent/loop.go:553](../../agent/loop.go#L553)）；依赖「取消也上报错误」的观测逻辑要调整。
3. `Agent.Resume` 产生的运行第一个 Phase 1 的 `newTurn=false`；启用 `next_turn` 后消息会保留到当前轮结束。
4. 头部带 `InFlight` 时 `BeginRun` 与非协调存储的 `prepare` 返回 `ErrResumeRequired`；`checkpoint.Fork` 拒绝该头部。
5. `ErrResumeRequired` 文本去掉「paused」字样（[checkpoint/coordinator.go:18](../../checkpoint/coordinator.go#L18)）。按文本匹配错误的调用方需要改为 `errors.Is`。
6. 每个持久化顶层工具批次多一次提交：工具密集步骤的提交次数约翻倍，大参数批次会增大检查点体积。`WithInFlightCheckpoint(false)` 可关闭。
7. `sameInboxMessage` 比较阶段：同 ID 不同阶段从「重复」变成「冲突」，是更严格的正确性要求。
8. 观察者会在同一 step 看到两个快照（一个带 `InFlight`，一个不带），与 P0-4 的中途检查点同类，但频率更高（每个工具批次）。
9. 在途记录会持久化 `BeforeTool` 改写后的参数，可能包含敏感内容；沿用存储层既有的访问控制与保留策略。

### 8.3 测试环境注意

- Windows 上多数既有文件是 CRLF，`gofmt -l` 会列出它们；本计划的新文件一律 LF，验证命令只检查本卡改动的文件，不要重排既有文件。
- 崩溃类用例不要用裸 `git stash`/工作区共享目录；每个用例用 `t.TempDir()`。
- `checkpoint.Memory` 只能在进程内模拟恢复，跨进程持久性断言必须用 `checkpoint.NewFile`（设计稿 §12.1 第 12 条）。
- `examples/inflight-recovery` 必须离线（mock 模型 + 临时目录），不得访问网络或真实提供方。
- `-race` 至少要覆盖：多生产者入队、抢占与模型结束并发、并行工具段与恢复提交。

### 8.4 与其他计划的关系

P0-1 的冻结视图、P0-2 的 `settleTurn` 顺序、P0-3 的分段并发、P0-4 的超限恢复循环是本计划的既有前提；任何一处冲突以「保留既有语义、新增分支」为原则，不重排现有阶段顺序。设计稿 §12.2 的七个待定问题（取消原因是否给提供方、`InputRejected` 事件、事件是否携带内容、`after_*` 是否需要精确触发、策略版本进摘要、运维跳过开关、`RunStatus` 报调用数量）本计划一律不实现。

## 9. 明确不做的事

- 不把四个阶段做成四个真实插入口；不在模型请求与工具结果之间插入消息。
- 不把工具结果、系统提示纳入输入队列。
- 不让持久消息参与抢占；不实现跨进程抢占或提供方式中止 API。
- 不实现自动恢复、进程内活动运行登记表、实时状态投影（P1-7）、会话忙时排队（P1-8）。
- 不实现工具幂等性声明；不按工具名挑选择在途开关。
- 不改工具台账状态机、`ClaimTool` 语义、工具并发分段策略。
- 不做台账全表按步骤扫描（用批次覆盖代替）。
- 不在恢复路径重跑 `CheckToolPolicy`/`BeforeTool`。
- 不让 `Fork` 清空 `InFlight` 后放行；不把在途塞进 `Pause.Phase`。
- 不新增 `InputRejected` 事件，不在事件里携带消息正文。

## 10. 执行记录

### 10.1 任务记录

| 任务 | 状态 | 提交 | 验证命令 | 结果 | 备注 |
| --- | --- | --- | --- | --- | --- |
| IQ-01 | 已完成 | `c42a4af` | 卡面 4 条 | 全绿；卡面反向验证 4 条已于本轮复跑，全部被杀 | 状态常量改名见 10.3 F1 |
| IQ-02 | 已完成 | `48c997a` | 卡面 5 条 | 全绿；`agent` 全量与 `-race` 通过 | 补强用例见 10.3 F3 |
| IQ-03 | 已完成 | `ff0121c` | 卡面 6 条 | 全绿；`agent ./core ./checkpoint ./eval` 全量与 `-race` 通过；三组回归用例未改预期 | 持久阶段过滤推迟到 IQ-04，见 10.3 F2、F4、F5 |
| IQ-04 | 已完成 | `d1d5660` | 卡面 5 条 + `go build ./...`、`go vet ./checkpoint ./agent ./core`、`go test ./...`、`-race` 两个子集 | 全绿；卡面反向验证 9 条本轮逐条执行，全部被杀 | 用例与实现取舍见 10.3 F6…F11 |
| IF-01 | 已完成 | `bd0f1ee` | 卡面 5 条 + `go build ./...`、`go vet ./checkpoint ./agent ./queue ./middleware`、`go test ./...`、`go test -race ./checkpoint` | 全绿；新增 11 个用例（含 4 个子例组）全部执行通过；`TestLegacyCheckpointDigestUnchanged`、`TestLegacyDigestRetry`、`TestSnapshotConsistencyBeginRunPaused`、`TestFork*` 未改预期即通过 | 日志重放确实绕过 `freezeCheckpoint`，已在重放校验里补门禁；见 10.3 F13…F16 |
| IF-02 | 已完成 | `988cec6` | 卡面 6 条 + `go test ./...`、`go vet ./agent ./core`、`-race` 子集 | 全绿；卡面 10 个用例与补强用例 `TestInFlightSkipsUnadvertisedBatch` 通过；卡面反向验证 8 条 + 新增 1 条本轮逐条执行，全部被杀 | 卡面用例 6、用例 1 的前提有误，见 10.3 F17、F18；未广告名批次跳过写入见 F19；两条既有计数断言改了预期，见 F20 |
| IF-03 | 已完成 | `3453163` | 卡面 5 条 + `go test ./...`、`go vet ./agent`、`-race` 子集 | 全绿；卡面 12 个用例（含卡面建议跳过的冲突头部用例，已补实现）+ 补强 `TestResumeRegistersInFlightBatch` 通过；卡面反向验证 8 条 + 补强 1 条本轮逐条执行，全部被杀 | 用例 1 的断言方式与用例 3 的跳过建议有变，见 F21、F22；基线用例需先绕开摘要预检，见 F23 |
| IF-04 | 已完成 | `cc386b2` | 卡面 5 条 + `go test ./...`、`go vet ./agent ./core`、`-race` 子集 | 全绿；卡面 11 个用例 + `core` 往返 2 个用例通过；卡面反向验证 9 条本轮逐条执行，全部被杀；IF-03 用例 8、10 已升级为完整断言 | 混合批次的执行模式与事件顺序断言有调整，见 F25、F26；计数边界见 F27；失败分支暂直接 `fail`，见 F24 |
| IF-05 | 已完成 | `73ec95c` | 卡面 5 条 + `go test ./...` | 全绿；卡面 8 个用例 + 补强 4 个（共 12 条）通过；卡面反向验证 7 条本轮逐条执行，全部被杀；`agent/failure_test.go`、`agent/phase1_failure_test.go` 未改预期即通过 | 卡面用例 2–5 改为包内直接调用补齐函数，失败触发改用过期租约与取消，见 F28；其余取舍见 F29…F33 |
| IQ-05 | 已完成 | `dd6191b` | 卡面 6 条 + `go test ./...` | 全绿；14 个外部用例 + 3 个内部用例 + `core` 往返 2 个用例通过；`TestContextOverflow*` 未改预期即通过；卡面反向验证 10 条本轮逐条执行，全部被杀 | 配置开关继续留在队列、取消运行静默结束的报错口径见 F34、F35；实现期发现见 F36…F38 |
| IF-06 | 已完成 | `f0d9f50` | 卡面 5 条 + `go test ./agent ./checkpoint ./queue`、`go vet ./agent ./queue`、`gofmt` | 全绿；卡面 7 个用例 + 补强 `TestBuildTerminalRecordRejectsPendingBoundary` 通过；`RunStatus` 既有断言与既有 `TestRecordCompletion*` 未改预期即通过；卡面反向验证 4 条本轮逐条执行，全部被杀 | 队列用例按卡面授权退化为 `terminalTaskRecord` 表驱动（F42）；对照表落 README（F43）；两条反向验证预期有偏差（F39、F41） |
| IF-07 | 已完成 | `298f0c6`（示例输出补强 `43edc44`） | 卡面 8 条 + 全量门禁（build/vet/test/race/gofmt/diffcheck/status + 两个示例，落盘 `.scratch/if07-gate.log`） | 全绿；8 个崩溃用例通过；8 条反向验证本轮逐条执行，全部被杀；`gofmt` 仅列既有 CRLF 文件；两个示例退出码 0 | 卡面两条崩溃预期与完成记录落盘时机不符（F44）；守卫用例名不存在、三条跨包用例无法登记（F45）；示例改为模拟进程消失（F46）；反向验证第 1 条预期少一条（F49）；文件存储无 `Close`（F50）；其余实现期修正见 F47、F48、F51 |

### 10.2 反向验证记录

| 卡片 | 改错 | 观察到的失败用例 | 还原后是否通过 |
| --- | --- | --- | --- |
| IQ-01 | 删掉 `NormalizeInputPhase` 的空值分支 | `TestNormalizeInputPhase`（`NormalizeInputPhase(""): core: unknown input phase: ""`） | 是 |
| IQ-01 | `default` 分支返回原值而不是空阶段 | `TestNormalizeInputPhase`（`"before-model"` 用例） | 是 |
| IQ-01 | `InputAfterModel` 字面量写成 `"after-model"` | `TestInputPhaseValuesAreStable` | 是 |
| IQ-01 | 错误串去掉 `core: ` 前缀 | `TestErrUnknownInputPhaseText` | 是 |
| IQ-02 | 卡面表 7 条（阶段过滤、`kept` 写回、`takePhase` 保序、`State()`、`push` 委托、校验顺序、开关接线） | 逐条对应卡面预期的 7 个用例；其中「开关接线」一条当时存活，补 `TestRunHandleWiresPreemptionOption` 后被杀 | 是（10.3 F3） |
| IQ-03 | 卡面表 7 条（Phase 1 用 `drain()`、`max_tokens` 填 `newTurn: true`、`Resume` 不设 `continuedTurn`、`takeDurable` 不过滤确认、读取顺序、临时事件填 `MessageIDs`、去掉持久发布） | 逐条对应卡面预期用例；其中「`Resume` 不设 `continuedTurn`」在原用例下存活，改写 `TestResumedRunFirstStepIsNotNewTurn` 后被杀 | 是（10.3 F4） |
| IQ-04 | `sameInboxMessage` 不比阶段 | `TestInboxSameIDDifferentPhaseConflicts` | 是 |
| IQ-04 | `normalizeInboxMessage` 不写回阶段（两存储都不规范化） | `TestInboxPhaseRoundTripMemoryAndFile`、`TestInboxSameIDSamePhaseIsDuplicate` | 是 |
| IQ-04 | 内存存储先落记录再校验阶段 | `TestInboxUnknownPhaseRejected` | 是 |
| IQ-04 | 重复回执不填 `State` | `TestInboxSameIDSamePhaseIsDuplicate` | 是 |
| IQ-04 | `validateJournalRecord` 不校验阶段 | `TestFileJournalValidatesInboxPhase` | 是 |
| IQ-04 | 重放时不规范化旧记录阶段 | `TestFileJournalLegacyInboxRecordReadsAsBeforeModel` | 是 |
| IQ-04 | `takeDurable` 不看 `newTurn` | `TestInputQueueTakeDurableKeepsNextTurnMidTurn`、`TestDeliverWithAgentPhase` | 是 |
| IQ-04 | 持久投递事件把阶段硬编码成 `before_model` | `TestPublishDeliveredDurableGroupsByPhase`、`TestDeliverWithAgentPhase` | 是 |
| IQ-04 | `DeliverWith` 丢掉传入的阶段 | `TestDeliverWithAgentPhase`、`TestDeliverRejectsUnknownPhase` | 是 |
| IF-01 | `freezeCheckpoint` 不调 `validateWorkState` | `TestInFlightCoexistsWithNothingElse` 的 pending/pause/completion 三个子例（内存与文件都失败） | 是 |
| IF-01 | 去掉「必须有步骤快照」这条 | `TestInFlightRequiresSnapshot` 的 no_snapshot、snapshot_step_differs、snapshot_has_no_tool_digest 三个子例 | 是 |
| IF-01 | `cloneInFlight` 直接 `out.Calls = in.Calls` | `TestInFlightCloneIsolatesArgs`（第一次跑该变异存活，改用值快照断言后被杀，见 10.3 F16） | 是 |
| IF-01 | 提交摘要把 `Calls` 按 ID 排序 | `TestInFlightDigestRetry` 的顺序交换子例（内存与文件） | 是 |
| IF-01 | `commitDigestInFlight` 字段不带 `omitempty` | `TestCommitDigestOmitsInFlightWhenAbsent` | 是 |
| IF-01 | 内存 `BeginRun` 仍只判 `Pending`/`Pause` | `TestSnapshotConsistencyBeginRunInFlight/memory` | 是 |
| IF-01 | 文件 `BeginRun` 仍只判 `Pending`/`Pause` | `TestSnapshotConsistencyBeginRunInFlight/file` | 是 |
| IF-01 | `Fork` 不拒绝在途父记录 | `TestForkRejectsInFlight` | 是 |
| IF-01 | `metaFields` 读不到 `in_flight` 键 | `TestMetadataInFlightProjection/file`、`TestFileMetadataRebuildInFlight` | 是 |
| IF-01 | `RequiresResume` 漏掉 `InFlight` | `TestRequiresResume`、`TestSnapshotConsistencyBeginRunInFlight`（内存与文件） | 是 |
| IF-01 | 日志重放不校验在途不变量 | `TestFileJournalReplayValidatesInFlightRecord` | 是 |
| IF-02 | 把写入移到 `execTools` 之后 | `TestInFlightCheckpointWrittenBeforeHandlers`（handler 内读到的头部没有 `InFlight`） | 是 |
| IF-02 | 写入移到 `BeforeTool` 循环之前 | `TestInFlightCallsUseRewrittenArgs`（在途参数仍是 `{}`）、`TestInFlightRejectedBeforeExecutionChecksPass`（中断/停止前多写了一次在途提交） | 是 |
| IF-02 | 去掉 `checkWriteAccess` 前置 | `TestInFlightCommitFailureBlocksHandlers` 的「提交前取消」子例（取消后仍写了在途头） | 是 |
| IF-02 | `Release` 改成 true | `TestInFlightCommitKeepsClaim`（提交序列变成 `[true false true]`） | 是 |
| IF-02 | `Executions` 漏传 | `TestInFlightCheckpointWrittenBeforeHandlers`（在途提交带 0 条台账） | 是 |
| IF-02 | 去掉 `durableStore` 条件 | `TestInFlightSkippedForPlainCheckpointer`（3 次 `Save` 都带 `InFlight`） | 是 |
| IF-02 | 去掉 `rc.topLevel` 条件 | `TestInFlightNotWrittenForSubRun`（委派运行的 handler 读到父线程头部带 `InFlight`） | 是 |
| IF-02 | 事件在提交前发布 | `TestInFlightEventPublishedAfterCommit`（提交被闸门挡住时已看到事件） | 是 |
| IF-02 | 去掉「未广告名批次跳过写入」条件 | `TestInFlightSkipsUnadvertisedBatch`（运行以 `ErrInFlightConflict` 失败） | 是 |
| IF-02 | 提交摘要不含 `InFlight`（IF-01 层） | 本卡未重复执行；IF-01 轮已由 `TestInFlightDigestRetry` 杀死 | — |
| IF-03 | 在途分流放到 `Claim` 之后（预检失败留下认领） | `TestResumeInFlightPreflightBeforeClaim`（`claims = 1 after preflight failed, want 0`） | 是 |
| IF-03 | `matchInFlightMessage` 比较 `Args` | `TestResumeInFlightMatchesAssistantMessage/rewritten_args_still_match`（`call 0 does not match the recorded batch`） | 是 |
| IF-03 | 摘要检查无条件执行（不看台账） | `TestResumeInFlightSummaryDriftAllowsPureReplay`（`want success`，实际 `ErrStepViewDrift`） | 是 |
| IF-03 | 摘要检查完全去掉 | `TestResumeInFlightSummaryDriftBlocksExecution`（`Resume after a tool change = <nil>`） | 是 |
| IF-03 | 台账读取失败当作「不会执行」放行 | `TestResumeInFlightLedgerReadFailureRejects`（`Resume with a failing ledger = <nil>`） | 是 |
| IF-03 | 在途分支设置 `replayBaseline` | `TestResumeInFlightDoesNotSetReplayBaseline`（第一步报 `step view drifted ... tools changed: true`） | 是 |
| IF-03 | 接受显式审批（只忽略不报错） | `TestResumeInFlightRejectsApprovals`（`Resume with approvals = <nil>`） | 是 |
| IF-03 | 用视图里复制的旧摘要做比较 | `TestResumeInFlightSummaryDriftBlocksExecution`（`Resume after a tool change = <nil>`） | 是 |
| IF-03 | 认领后历史匹配失败不释放认领（补强，卡面表外） | `TestResumeInFlightMatchesAssistantMessage` 三个坏历史子例（再认领报 `claim is held by another owner`） | 是 |
| IF-04 | 恢复时补跑 `CheckToolPolicy`/`BeforeTool` | `TestInFlightRecoverySkipsExecutionChecks`（`CheckToolPolicy=0 BeforeTool=1`） | 是 |
| IF-04 | `rlc.Step` 用 `ib.step+1` | `TestInFlightRecoveryUsesSameStepNumbers`、`TestInFlightRecoveryReplaysCompleted`、`TestInFlightRecoveryReportsUnknown`、`TestInFlightRecoveryMixedBatch`、`TestInFlightRecoveryCommitFailureKeepsHead` | 是 |
| IF-04 | 去掉 `resumeStepViewFor`，`rlc.view` 保持 nil | 全部 11 个恢复用例（`execTools` 报 `requires a frozen step view`） | 是 |
| IF-04 | `completed` 回放时不跳过 `AfterTool` | `TestInFlightRecoveryReplaysCompleted`（`AfterTool ran 2 times`） | 是 |
| IF-04 | `claimed` 自动重试（把 `ErrToolOutcomeUnknown` 当成可执行） | `TestInFlightRecoveryReportsUnknown`、`TestInFlightRecoveryMixedBatch`（handler 多跑了 claimed 调用） | 是 |
| IF-04 | 恢复提交带 `InFlight` | `TestInFlightRecoveryExecutesPending`、`TestInFlightRecoveryReplaysCompleted`、`TestInFlightRecoveryReportsUnknown`、`TestInFlightRecoveryMixedBatch`、`TestInFlightRecoveryDirectiveEndsRun`（找不到已结算的同步骤提交） | 是 |
| IF-04 | 事件在提交前发布 | `TestInFlightRecoveryDirectiveEndsRun`（收到事件时头部仍带 `InFlight`）、`TestInFlightRecoveryCommitFailureKeepsHead`（提交失败仍发布了事件） | 是 |
| IF-04 | 结果按完成先后拼装 | `TestInFlightRecoveryMixedBatch`（慢的首个调用被排到最后，第一条结果变成 c3） | 是 |
| IF-04 | 续跑第一步设 `newTurn=true` | `TestInFlightRecoveryKeepsPhaseSemantics`（首个请求同时含 `now` 与 `later`） | 是 |
| IF-05 | 去掉两处补齐调用，`execErr != nil` 时直接 `fail` | `TestFailedInFlightBatchKeepsPairing`、`TestFailedInFlightAfterCancel`、`TestFailedInFlightRecoveryBackfill`（`seam` 删掉助手调用消息，末尾没有工具结果） | 是 |
| IF-05 | 补齐时对 `pending` 调用 `ClaimTool`（「执行 handler」的近似变异，见 10.3 F30） | `TestFailedInFlightBackfillPendingIsNotExecuted`（认领计数由 0 变 1）、`TestFailedInFlightBackfillLeavesPendingRetryable`（重试时 b 不再执行）、`TestFailedInCommitLeavesRecoverableHead`（恢复后 b 不再执行） | 是 |
| IF-05 | `claimed` 当作未执行 | `TestFailedInFlightBackfillUsesLedger`（结果文案变成「未执行」） | 是 |
| IF-05 | 把补齐结果经 `addCompletedTool` 写进台账 | `TestFailedInFlightBackfillLeavesPendingRetryable`（第二次运行的 b 直接回放，handler 总数停在 1） | 是（10.3 F31） |
| IF-05 | 用 `rc.Context` 而不是 `context.WithoutCancel` 读台账 | `TestFailedInFlightAfterCancel`（取消后读台账报错，b 的文案不是「未执行」） | 是 |
| IF-05 | 补齐输出先放已报告结果、再追加缺失结果（不再按 `calls` 顺序） | `TestFailedInFlightBackfillOrdersAroundReportedResults`（期望 a、b、c，实际 b、a、c） | 是 |
| IF-05 | 用全局开关 + 当前 `calls` 判断是否补齐（不记本步是否写过在途） | `TestFailedInFlightBackfillWithoutDurable`（非持久存储也补了结果，末尾不再是助手消息形态） | 是（10.3 F32） |
| IQ-05 | 抢占判断放在 `ok` 之前（`err == nil && ok && !preempted`） | `TestPreemptSuccessWins`（`a completed attempt must not be reported as preempted`） | 是 |
| IQ-05 | 去掉 `rc.Context.Err() == nil` 守卫 | `TestPreemptCancelledByRunWins`（取消后仍发布 `ModelPreempted`，用例在「取消的运行不得报告抢占重试」处失败） | 是 |
| IQ-05 | 抢占重试重跑 `ModifyRequest` | `TestPreemptCancelsAndRetriesSameStep`（`middleware counts before/modify/modelCtx = 1/2/2`） | 是 |
| IQ-05 | 抢占重试取走全部阶段（`takeVolatile(true)`） | `TestPreemptOnlyBeforeModelPhase`（`model calls = 2, want 3`） | 是 |
| IQ-05 | 中间检查点 `Release:true` | `TestPreemptCheckpointFailureFailsRun`（`release sequence = [true true]`） | 是 |
| IQ-05 | 去掉每步一次限制（只判活动尝试） | `TestPreemptTwiceInOneStep`（出现两条 `ModelPreempted`） | 是 |
| IQ-05 | `streamModel` 恢复旧行为：空终稿并返回 `ok=true` | `TestStreamModelWithoutFinal/no_preemption`（`run error = <nil>, want ErrModelNoFinal`） | 是 |
| IQ-05 | 取消路径无条件调用 `OnError` | `TestPreemptDoesNotReportOnError` 的 preempted、run cancelled 两个子例（各 1 次） | 是 |
| IQ-05 | 抢占票据步骤写 `step+1` | `TestPreemptCancelsAndRetriesSameStep`（`ticket = {Delivered:true Step:1 ...}`） | 是 |
| IQ-05 | 未开启时也执行取消（删掉 `pushWith` 的开关校验） | `TestPreemptDisabledReturnsError`（`SteerWith(preempt) = <nil>`） | 是 |
| IF-06 | `Paused` 表达式并入 `InFlight` | 只有 `TestRunStatusReportsInFlight`（它同时断言 `InFlight==true`、`Paused==false`）；`TestRunStatusPausedUnchanged` 的两种头部都不含「仅在途」，没有按卡面预期一并失败（见 10.3 F39） | 是 |
| IF-06 | 去掉终态记录的在途拒绝 | `TestRecordCompletionRejectsInFlightHead`（先撞上存储提交错误 `record completion: commit at revision 1: 存储不可用`，说明它试图覆盖在途边界） | 是 |
| IF-06 | 非协调存储不检查在途 | `TestPrepareRejectsInFlightHead`（运行进入模型步，最终报 `agent: loop exceeded MaxTurns`）。直接删掉检查会让 `fmt` 未使用、编译失败，改用 `if false && ...` 的可编译变异（见 10.3 F40） | 是 |
| IF-06 | `buildTerminalRecord` 只查 `Pause` 不查 `Pending` | `agent` 包既有用例全绿，卡面所称的回归守卫不存在；补 `TestBuildTerminalRecordRejectsPendingBoundary` 后该变异被杀（见 10.3 F41） | 是（补强后） |
| IF-07 | 去掉在途提交（`loop.go` 的在途写入改成 `if false`） | `TestCrashAfterCommitBeforeHandlers`（`handlers ran 2 times before the crash was observed`）、`TestInFlightCheckpointWrittenBeforeHandlers`（`handler 0 saw head without an in-flight batch`）；`TestInFlightRecoveryExecutesPending` 未失败，它直接种入在途头（见 10.3 F49） | 是 |
| IF-07 | `takeReady` 不再保留 `next_turn`（`if false && e.phase == ...`） | `TestSteeringQueueTakeReadyKeepsNextTurn`（`takeReady(false) took 4 entries, want 2`）、`TestVolatilePhasesSurviveToolBatch`（`result = "mid", want final`）；卡面写的用例名不存在（见 10.3 F45） | 是 |
| IF-07 | 去掉每步一次抢占限制（删 `rc.preemptedStep == attempt.step`） | `TestPreemptTwiceInOneStep`（两条 `ModelPreempted`）、`TestRequestPreemptionOncePerStep`（`a retry in an already preempted step was preempted again`） | 是 |
| IF-07 | 摘要检查无条件执行（把 `if mayExecute` 改成恒真分支） | `TestResumeInFlightSummaryDriftAllowsPureReplay`（`Resume ... = agent: ... different tool table, want success`） | 是 |
| IF-07 | 去掉失败补齐（`if false && execErr != nil && wroteInFlight`） | `TestFailedInFlightBatchKeepsPairing`（`last message = ... want 2 tool results`，只剩已报告的一条结果） | 是 |
| IF-07 | `sameInboxMessage` 不比阶段（`if false && a.Phase != b.Phase`） | `TestInboxSameIDDifferentPhaseConflicts` 的 memory/file 两个子例（`a phase change under the same ID must conflict, not redeliver`） | 是 |
| IF-07 | `RequiresResume` 只判 `Pending`/`Pause` | `TestSnapshotConsistencyBeginRunInFlight` 的 memory/file 两个子例（`BeginRun error = <nil>, want ErrResumeRequired`） | 是 |
| IF-07 | `Fork` 不拒绝在途父记录（`if false && parent.InFlight != nil`） | `TestForkRejectsInFlight`（`Fork error = <nil>, want ErrInFlightConflict`） | 是 |

### 10.3 实施期与文档不同的事实

| 编号 | 事实 | 处理方式 |
| --- | --- | --- |
| F1 | 设计稿 §5 与计划 §4.1 都把状态常量写成 `InputDelivered`，与同卡的 `core` 事件类型 `InputDelivered` 撞名，同包内无法共存 | 状态常量改名 `InputStateQueued`/`InputStateDelivered`/`InputStateRejected`，字符串值不变；已同步 `agent/steering.go` 与其测试 |
| F2 | IQ-03 卡面要求 `takeDurable` 本卡就按阶段过滤，但 `checkpoint.InboxMessage` 那时没有 `Phase` 字段，无阶段可滤 | 保持卡序，把持久过滤落到 IQ-04；IQ-03 的 `takeDurable` 形参 `newTurn` 已存在并注释「暂未生效」，IQ-04 落地 |
| F3 | 删除 `newRunHandleWithID` 里 `a.cfg.inputPreemption → steering.preemptionEnabled` 的接线后，IQ-02 的 8 个用例全部通过 | 新增包内用例 `TestRunHandleWiresPreemptionOption`（直接构造句柄并检查队列开关），该变异随后被杀 |
| F4 | `Resume` 不设 `continuedTurn` 的变异在原 `TestResumedRunFirstStepIsNotNewTurn` 下存活；且恢复后第一步是 step 0 不是 step 1 | 用例改为驱动前同时入队 `next_turn` 与 `before_model` 两条，断言恢复首个请求只含 `before_model`、票据步骤分别是 0 与 1 |
| F5 | `Agent.Stream` 返回的句柄要到首次 `Iter`/`Wait` 才开始驱动，「先注入再跑」与「跑起来再注入」是两种投递点；`max_tokens` 截断路径不调用工具 handler，把注入挂在工具上会永久停住 | 阶段用例统一改成：先驱动、在指定模型调用处停住后注入；新增夹具 `blockedCallModel`/`newBlockedCallModel`/`injectWhileParked`（`agent/input_test.go`） |
| F6 | `takeDurable` 只隐藏「已登记确认」的记录，读取本身不会从收件箱移除 | 内部用例 `TestInputQueueTakeDurableKeepsNextTurnMidTurn` 在中途读取后显式 `rc.addInboxAcks`，再断言新轮次只取到 `next_turn` 那条 |
| F7 | 卡面建议「旧记录读取时不改写、规范化只发生在投递点」；但重放不改写会让升级后同一 ID 显式带 `before_model` 的重试被判为冲突 | 改为重放期规范化（内存与文件都在写入/重放时把阶段补成显式值，磁盘文件不改），`TestFileJournalLegacyInboxRecordReadsAsBeforeModel` 同时钉住旧记录读到 `before_model` 与升级后重试仍是重复 |
| F8 | 卡面写 `Agent.Deliver` 委托 `DeliverWith(..., core.InputBeforeModel)`，那会让直接给 `InboxMessage.Phase` 赋值的调用方丢掉阶段 | `Deliver` 委托 `DeliverWith(..., msg.Phase)`：空阶段仍由存储规范化为 `before_model`，与卡面默认行为等价 |
| F9 | `TestFileJournalValidatesInboxPhase` 卡面放在外部包 `checkpoint/file_test.go`，但它要直接往日志里写坏记录，只有包内测试可行 | 落在 `checkpoint/file_recovery_test.go`（包 `checkpoint`），新增夹具 `appendJournalLine`（O_APPEND|O_CREATE，沿用该文件既有的写坏日志手法） |
| F10 | 卡面对 `TestDeliverWithAgentPhase` 的期望（运行第一步不消费 `next_turn`）不成立：新运行的第一步本身就是新轮次，会立即投递 | 改为在第二步的模型调用处停住、此时才投递 `next_turn` 记录，覆盖轮次中途的过滤；同时断言两个阶段的 `InputDelivered` 事件各带自己的 ID |
| F11 | `checkpoint/file_recovery_test.go` 与仓内若干既有测试文件在本机是 CRLF，`gofmt -l` 一直标记它们（HEAD 里是 LF，`git diff` 只有本卡插入行） | 不重排既有文件；卡的 `gofmt` 检查只列本卡新建/改动的 LF 文件，新增行沿用所在文件原有行尾 |
| F12 | `sameInboxMessage` 的冲突没有 sentinel 错误，只有 `fmt.Errorf` 文本（设计稿 §5 只要求比较阶段，未规定错误类型） | 用例断言错误串包含消息 ID，不锁完整文本；本轮不新增 sentinel |
| F13 | 卡面最后一条要求确认「文件存储是否存在绕过 `freezeCheckpoint` 的日志重放路径」——存在：`decodeLine` → `validateJournalRecord` → `applyJournalRecord` 完全不经过 `freezeCheckpoint` | 在 `validateJournalRecord` 的 `recordCommit` 分支调用 `validateWorkState`（`applyJournalRecord` 无错误返回，只能在校验层拦），并加 `TestFileJournalReplayValidatesInFlightRecord` 钉住 |
| F14 | 卡面用例 8 把「正常在途记录重放一致」和「损坏日志被拒」写在同一条 `checkpoint/durable_test.go` 里，但损坏日志必须直接写 `journalRecord`，只有包内测试可行 | 拆成两条：`durable_test.go`（包 `checkpoint_test`）留在写入路径与重放往返；`file_recovery_test.go`（包 `checkpoint`）加 `TestFileJournalReplayValidatesInFlightRecord`，复用本轮新增的 `appendJournalLine` 夹具 |
| F15 | `latestFileCheckpoint` 返回 `*checkpointRecord`（内嵌 `*Checkpoint`），`RequiresResume(*Checkpoint)` 不能直接收 | 文件协调器写成 `head != nil && RequiresResume(head.Checkpoint)`；内存侧 `latestLocked` 本来就返回 `*Checkpoint`，`RequiresResume` 自身对 nil 返回 false |
| F16 | `TestInFlightCloneIsolatesArgs` 第一版把初值存成 `original := cp.InFlight.Calls[0].Args`（切片头），别名共享时两边同时被改，断言恒真、克隆共享的变异存活 | 初值改存 `string(...)` 值快照，另加「克隆的 Args 必须与初值不同」的反向检查与 `Name` 改写检查；改后变异被杀 |
| F17 | 卡面用例 6 让用 `agent.AsTool` 验证「子运行不写在途」；但 `AsTool` 走的是 `child.Run`（[agent/subagent.go:53](../../agent/subagent.go#L53)），是另一个线程上的全新顶层运行，`topLevel` 仍为 true，碰不到该条件 | 改用 transfer 委派：`rc.deeper()` 共享父线程与存储但 `topLevel=false`（[agent/transfer.go:47](../../agent/transfer.go#L47)）；委派模型按请求次数脚本化，因为委派历史已带父级 transfer 的工具结果，`LastToolResult` 会直接收尾 |
| F18 | 卡面用例 1 要求在 handler 里读到台账两条都是 `ToolPending`；实际 `callOne` 在调用 handler 前先 `ClaimTool`（[agent/exectools.go:351](../../agent/exectools.go#L351)），handler 运行时已是 `claimed` | 改为断言在途提交的 `CommitRequest.ToolExecutions` 两条都是 `ToolPending`，并断言台账的 `InvocationID` 与 handler 读到的一致（台账与改写后参数同源） |
| F19 | IF-01 的存储不变量要求每个在途调用的工具名都在 `Snapshot.Advertised` 里，但模型可以给出未广告或未知的工具名；这类批次现有语义是逐条拒绝、运行继续，整批写入会让运行以 `ErrInFlightConflict` 失败（`TestNotAdvertisedRejectionClass`、`TestUnknownToolRejectionClassUnchanged`、`TestRejectionsSerialAfterSegmentedBatch`、`TestRunStatusErrorsAndCopies/副本不污染历史` 全部变红） | 批次里只要有一个未广告名就跳过在途写入（`inFlightCallsAdvertised`），照旧走逐条拒绝；未广告调用本来不产生副作用，没有可恢复的工作，代价是含这类调用的混合批次失去崩溃恢复。新增 `TestInFlightSkipsUnadvertisedBatch` 钉住 |
| F20 | 卡面「既有工具、审批、投递类测试不改预期即通过」对计数断言不成立：在途提交使每个工具批次多一条检查点 | `TestNormalStepCheckpointCarriesSnapshot` 由 2 条改为 3 条（并断言第 0 条带在途批次、按 `Snapshot.Step` 对齐模型请求），`TestLineageNormal` 由 3 条改为 5 条；其余既有测试未改 |
| F21 | 卡面用例 3 建议跳过冲突头部，因为存储校验写不进去；但冲突检查的调用点仍可测：包内测试可直接构造带 `InFlight`+`Pause` 的头部调用 `resumeInFlight` | 实现 `TestResumeInFlightRejectsConflictingHead`（包内，不经过存储），断言 `ErrInFlightUnrecoverable`；未跳过 |
| F22 | 卡面用例 1 建议用 `RunStatus.InFlight`（IF-06 未落地）或「头部不变」断言；前者不可用，后者不足以证明批次已登记 | 拆成两条：外部 `TestResumeDetectsInFlight` 断言认领 `PauseID == "checkpoint-<head.ID>"` 且头部不变；包内 `TestResumeRegistersInFlightBatch` 断言 `run.rc.inFlight` 的 step/calls/snapshot/final、`continuedTurn`、无 `replayBaseline`、`preparation.claim` 非空 |
| F23 | 卡面用例 8 的做法（严格策略 + 同名不同 schema）会先被本卡的摘要预检拒绝（台账为空 ⇒ 可能执行 ⇒ 严格比对），根本到不了「第一步是否漂移」；必须先把台账写成 `completed`，让预检走「纯回放、不比对摘要」分支 | `TestResumeInFlightDoesNotSetReplayBaseline` 种一条 `completed` 台账；这样若误设基线，运行第一步仍会报 `ErrStepViewDrift`，变异已杀 |
| F24 | 卡面允许 IF-04 在 `execErr != nil` 时先直接 `fail`，等 IF-05 再补缺失结果，但要求在实施记录里标注 | 按此落地：[agent/loop.go:141](../../agent/loop.go#L141) 直接 `l.fail(rc, ib.step, history, ib.snapshot, execErr)`；IF-05 已按此改为先 `completeFailedBatch` 再失败（[agent/loop.go:139](../../agent/loop.go#L139)）。恢复分支在入口清空 `rc.inFlight`，所以失败提交同样清除在途头 |
| F25 | 卡面用例 4 若沿用 `newRecoveryAgent` 的 `ToolSequential`，完成顺序恒等于调用顺序，卡面「结果按完成先后拼装」的变异无法被杀死 | 混合批次改用默认并行执行，并把唯一需要 handler 的调用放在第一位、通过 `recordingTool.onCall` 延迟 30ms，使完成顺序必然是该调用最后；其余恢复用例保持 `ToolSequential`，这样「两个 pending 调用按 c1、c2 各执行一次」的调用顺序断言才成立（并行下 handler 启动顺序不定） |
| F26 | 卡面预期「事件在提交前发布」由用例 7、8 杀死，但两例原先只断言事件出现过，没有任何观察发布与提交先后的位置 | 用例 7 在收到 `ToolBatchRecovered` 时读 `store.Latest`，断言头部已是结算后的原始步骤且无 `InFlight`；用例 8 统计失败提交期间的事件数，必须为 0 |
| F27 | 设计稿 §4.10 只给出四类计数的语义，没有写边界；一个调用可能同时符合两种直觉（handler 报错既是 `rejected` 又是已执行；`claimed` 既像拒绝又像未知） | 定死口径：进入 handler 的成功、handler 错误、超时、取消都计 `Executed`（即使同时产生 `ToolRejection`）；`completed` 回放计 `Replayed`；`ErrToolOutcomeUnknown` 计 `Unknown`；参数/工具表/策略前拒绝计 `Rejected`；认领失败、二次写检查失败等不落入四类。该口径写进 `ToolBatchRecovered` 注释并声明「诊断值、不守恒」，见 [core/event.go:68](../../core/event.go#L68) |
| F28 | 卡面用例 2–5 的前提「第二个调用返回错误触发失败路径」不成立：handler 返回的 Go 错误在 [agent/exectools.go:455](../../agent/exectools.go#L455) 被转成 `rejected(c, RejectHandlerError, ...)` 结果，运行继续。真正会中途失败且可能没有结果的路径是写检查失败（[agent/exectools.go:392](../../agent/exectools.go#L392)、[agent/exectools.go:434](../../agent/exectools.go#L434) 的 `noResult`）与派发前停止（[agent/exectools.go:159](../../agent/exectools.go#L159)、[agent/exectools.go:170](../../agent/exectools.go#L170)） | 用例 1、6、7、8 改用两种真实触发：过期的认领租约（`ErrStaleFencingToken`）与 `AfterTool` 里取消运行；用例 2–5 无法从外部驱动出「批次执行到一半、台账处于 claimed/completed/pending 且未提交」的状态，改为包内直接调用 `completeFailedBatch` |
| F29 | 12 条用例的前置条件不同：2–5 要直接构造 `RunContext` 与台账，1、6、7、8 走完整运行 | 拆成两个文件：`agent/inflight_failure_internal_test.go`（包 `agent`）放用例 2–5 与两条顺序/换序补强；`agent/inflight_failure_test.go`（包 `agent_test`）放用例 1、6、7、8 与 `TestFailedInFlightRecoveryBackfill`、`TestFailedInFlightBackfillLeavesPendingRetryable`；补齐函数在 [agent/inflight_failure.go:22](../../agent/inflight_failure.go#L22)，普通步骤接线在 [agent/loop.go:606](../../agent/loop.go#L606)，恢复路径接线在 [agent/loop.go:139](../../agent/loop.go#L139) |
| F30 | 卡面反向验证第 2 条要求「补齐时对 `pending` 执行 handler」，但 `completeFailedBatch` 只有台账和调用，没有工具表，执行不了 handler | 用 `ClaimTool`（执行的起点）近似：它同样改变台账，能同时杀死「补齐不能碰台账」与「恢复后 b 应执行」两类断言；据此把该条标为近似变异 |
| F31 | 卡面反向验证第 4 条预期「写进台账后再次恢复时该调用不再回放」，但补齐结果只并入 `results`，正常失败提交不写台账；要观察到写台账必须先让失败提交也成功 | 新增 `TestFailedInFlightBackfillLeavesPendingRetryable`：第一次运行取消并成功写失败检查点，第二次运行关掉在途写入、只重发 b；若补齐把 b 记成 `completed`，第二次会回放而不是执行 |
| F32 | 反向验证第 7 条第一次改法（全局开关 + `inFlightCalls`）未杀死用例 6：本步没写在途时 `inFlightCalls` 是 nil，`len(calls)==0` 直接返回 | 改成全局开关 + 当前批次的 `calls` 后才失败；还原用备份 `cp` 覆盖并 `cmp` 核对 |
| F33 | 卡面用例 8 期望恢复时同时覆盖 `claimed`→未知、`pending`→执行、`completed`→回放；但同一批次里「已完成且已落盘」的记录无法自然出现：完成记录先留在 `RunContext` 内存表（[agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139)），随下一次提交才落盘，失败提交失败时一并丢失 | 用例 8 覆盖 `claimed`→未知与 `pending`→执行；`completed` 回放仍由 IF-04 的 `TestInFlightRecoveryReplaysCompleted` 覆盖 |
| F34 | 卡面把配置开关写成二选一（`RunContext` 字段或 IQ-02 的队列字段）；实现时若两处都放会留下重复状态 | 保留 `steeringQueue.preemptionEnabled` 作为唯一来源：`pushWith` 在入队时完成开关与阶段校验，`RunContext.SteerWith` 只在入队成功后调用 `requestPreemption`，不再增加 `RunContext` 字段。派生上下文（`deeper`/`subRun`/`forBranch`）的新队列开关为关闭，但外部 `Run` 句柄只操作顶层 `RunContext`，故抢占只发生在顶层运行 |
| F35 | 设计稿 §5.4.4 在「没有终稿且没有错误」时直接补 `ErrModelNoFinal`；但运行已被取消、提供方遵守取消并静默结束时，按字面实现会把取消报成 `ErrModelNoFinal` | 改为：`err == nil` 时先看 `rc.Context.Err()`，非空则用它，否则才用 `ErrModelNoFinal`。`TestPreemptCancelledByRunWins` 因此仍能以 `context.Canceled` 结束；其余路径（未取消、未抢占）行为不变 |
| F36 | `endModelAttempt` 无条件取消尝试上下文后，超限恢复守卫若继续读 `attemptCtx.Err()` 会恒为真：第一次跑 `TestPreemptAndOverflowRetryCountedSeparately` 时超限恢复没有发生 | 在 `endModelAttempt` 之前先取 `attemptCanceled := attemptCtx.Err() != nil`，守卫改用该值。原口径「取消后不恢复」不变，只是读取时机提前 |
| F37 | `TestPreemptSettleRace` 初版断言「票据 delivered ⇒ 一定发生了一次重试」。第一轮 20 次全部失败：测试放行闸门与 `SteerWith` 都可能早于模型 `Generate`，消息直接进入第一次调用的 Phase 1（1 次请求、结果 `first`、票据 Step 0） | delivered 分支同时接受「1 次请求且结果 `first`」与「2 次请求且结果 `retry`」；仍断言消息恰好被一次请求携带、`ModelPreempted` 至多一条 |
| F38 | 等待运行错误的用例不能用 `collectRun`：它在 `Iter` 收到终止错误时直接 `t.Fatalf`（[agent/task_loop_test.go:144](../../agent/task_loop_test.go#L144)），会遮蔽 `run.Wait()` 的错误 | `TestStreamModelWithoutFinal/no_preemption` 与 `TestPreemptDoesNotReportOnError/provider_error` 改用 `Agent.Run` 直接取错误 |
| F39 | 卡面反向验证第 1 条预期 `TestRunStatusReportsInFlight` 与 `TestRunStatusPausedUnchanged` 同时失败；实际只有前者失败——后者的审批暂停头本来就带 `Pause`，普通完成头三项皆空，都不构成「仅在途」反例 | 不为凑预期重复加种：`TestRunStatusReportsInFlight` 已断言在途头的 `Paused==false`，正是该变异的反例；实施记录如实登记只有一条用例被杀 |
| F40 | 卡面反向验证第 3 条「非协调存储不检查在途」直接删除检查后，`fmt` 导入变成未使用，包编译失败（编译失败不算变异被杀） | 保留导入，改成 `if false && plainHead != nil && plainHead.InFlight != nil` 的可编译变异；该变异下用例失败，还原后通过 |
| F41 | 卡面反向验证第 4 条写「既有用例（回归守卫）」，但把 `buildTerminalRecord` 的 `Pending` 从拒绝条件里去掉后，`agent` 包既有用例全部通过，没有用例覆盖「仅 `Pending` 边界」 | 补包内用例 `TestBuildTerminalRecordRejectsPendingBoundary`（[agent/inflight_status_internal_test.go:111](../../agent/inflight_status_internal_test.go#L111)）：写入仅带 `Pending` 的边界、读回后直接调用 `buildTerminalRecord`，断言错误串含 `pause point`；该变异随后被杀 |
| F42 | 卡面用例 3 要求 worker 驱动一个「在批次中途被杀的运行」，但进程内测试制造不出残留的在途头：取消或失败会走失败补齐并写下常规检查点，进程存活时 head 不会停在 `InFlight`；真正的在途头来自进程在在途提交之后、Phase 4 提交之前退出 | 按卡面授权的退路，`TestQueueWorkerDoesNotMarkInFlightAsPaused`（[queue/run_worker_publish_test.go:96](../../queue/run_worker_publish_test.go#L96)）直接对 `terminalTaskRecord` 做表驱动：成功、失败、取消三种 `RunResult`（`Paused=false`）都不得映射成 `StatusPaused`/`FinishReasonPaused`，另加 `Paused=true` 的对照子例；映射代码不改，只在 `recordCheckpoint` 与暂停分支补注释 |
| F43 | 卡面要求把「暂停 vs 在途」对照表放到 `docs/` 已有的状态/暂停说明处；本仓已入库的 `docs/` 只有旧 ADR 与旧计划，本计划的设计稿在未入库的 `docs/analysis/`，已入库的状态/暂停说明实际在 README 的「后台运行的终态与状态查询」一节（[README.md:308](../../README.md#L308)） | 对照表与条目写进 README 该节，`RunStatus` 顶部注释指向这一节（[agent/run_status.go:13](../../agent/run_status.go#L13)）；`docs/analysis` 与执行计划不纳入提交 |
| F44 | 卡面用例 3/4 期望「批次中途崩溃后第一个调用回放」「Phase 4 失败后全部回放」；完成结果先留在 `RunContext` 的内存表，随下一次成功提交才落账（[agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139)），崩溃时台账仍是 `claimed` | 按「`claimed` 不重放、报结果未知」实现：`TestCrashMidBatch`（[agent/inflight_crash_test.go:282](../../agent/inflight_crash_test.go#L282)）断言 `executed=1 unknown=1`，`TestCrashAfterBatchBeforePhase4`（[agent/inflight_crash_test.go:345](../../agent/inflight_crash_test.go#L345)）断言 `unknown=2 executed=0`；两者仍断言每个调用恰好一条结果、handler 不重复执行 |
| F45 | 卡面反向验证写的 `TestPhaseDeliveryAfterToolBatchKeepsNextTurn` 在仓库中不存在；`TestInboxSameIDDifferentPhaseConflicts`、`TestSnapshotConsistencyBeginRunInFlight`、`TestForkRejectsInFlight` 位于 `checkpoint_test` 包，`agent_test` 无法调用 | 用例 8 的登记表只列五条同包守卫，`next_turn` 用实际用例 `TestVolatilePhasesSurviveToolBatch`（[agent/input_test.go:198](../../agent/input_test.go#L198)）；三条跨包用例留在 §10.2 直接执行 |
| F46 | 卡面示例要求「第一次运行在批次中途因工具报错失败，头部仍有 `InFlight`」；工具报错会走失败补齐并写下一个不带 `InFlight` 的常规检查点（V27），进程存活时头部不会停在在途 | `examples/inflight-recovery` 改为模拟进程消失：包装存储让第一次提交返回回执加错误、之后提交全部失败，运行停在在途边界且 handler 零启动；重开目录后打印 `RunStatus.InFlight` 并 `Resume` |
| F47 | `TestCrashDuringRecoveryCommit`（[agent/inflight_crash_test.go:450](../../agent/inflight_crash_test.go#L450)）初版用崩溃运行留下的已取消 context 调 `Resume`，第一次恢复报 `context canceled`，不是预期的结算提交错误 | 新进程的恢复改用 `context.Background()`，与其余恢复用例的 `mustResume` 一致 |
| F48 | `examples/inflight-recovery` 初版断言工具调用顺序为 c1、c2；该 Agent 没开 `ToolSequential`，批次内工具并发执行，实际顺序可能反过来 | 调用集合改为按 ID 计数，断言每个调用恰好一次；最终工具结果的顺序仍断言 c1、c2 |
| F49 | 卡面反向验证第 1 条预期去掉在途提交后 `TestInFlightRecoveryExecutesPending` 也失败；该用例直接种入在途头，不经过写入路径 | 如实登记只有 `TestCrashAfterCommitBeforeHandlers` 与 `TestInFlightCheckpointWrittenBeforeHandlers` 失败，不额外造一条依赖写入路径的用例 |
| F50 | 卡面用例 7 写「关闭句柄、重开目录」，但 `checkpoint.File` 没有 `Close` 方法（[checkpoint/file.go:74](../../checkpoint/file.go#L74) 只有 `NewFile`） | 直接对同一目录再构造一个 `checkpoint.NewFile(dir)`，用新实例读在途头并恢复 |
| F51 | 卡面示例要求打印台账状态；逐条查询需要未导出的 `toolInvocation` 生成的 invocation ID（[agent/exectools.go:485](../../agent/exectools.go#L485)），示例不应复制内部哈希规则 | 示例改为打印落盘头部的 `InFlight`、`RunStatus`、`ToolBatchRecovered` 计数与 handler 调用次数，足以证明 pending 执行一次、claimed 不重放；不直接打印逐条台账状态 |








