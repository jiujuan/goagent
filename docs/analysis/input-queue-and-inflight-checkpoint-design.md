# 输入队列与工具批次在途检查点架构设计（P1-5 / P1-6）

| 项目 | 内容 |
|---|---|
| 状态 | 设计稿，尚未实现 |
| 日期 | 2026-10-22 |
| 基线 | `9e5df25` |
| 范围 | P1-5 合并输入通道、投递阶段、抢占；P1-6 工具批次执行前写一次在途检查点 |
| 不在范围 | P1-7 实时状态投影、P1-8 会话忙时策略、P0 已落地项、工具并发调度本身 |
| 关联文档 | [Codex core 运行时优化建议](./codex-core-runtime-optimization-proposal.md#L127)、[工具调用并发设计](./tool-call-concurrency-design.md)、[上下文超限重试设计](./context-overflow-retry-design.md)、[StepView 与任务循环草案](./stepview-and-task-loop-design-draft.md) |

> 本文所有行号以基线 `9e5df25` 为准。本文只描述设计，不修改代码、不改变现有公开 API 或存储格式。

## 1. 结论

P1-5 和 P1-6 都建立在现有运行时之上，不替换 P0 已经落地的 StepView、外层任务循环、按调用粒度并发和上下文超限恢复。

P1-5 的最终形态：

- 新增一个**逻辑输入队列**，把临时消息（`steeringQueue`）和持久收件箱（`checkpoint.Durable`）放在同一个读取接口后面。物理存储不变：临时消息仍然不落盘，持久收件箱仍然是持久层。
- 每条输入带来源、投递阶段和回执。阶段只有四个值：`before_model`、`after_model`、`after_tools`、`next_turn`，空值按 `before_model` 处理。
- 四个阶段**不新增四个物理插入口**。运行中只有两个安全投递点：`PrepareTurn`（下一步模型调用前）和 `settleTurn`（终稿之后、下一轮开始前）。`after_model` / `after_tools` 表示“不晚于某个事件之后”，不是“必须在模型与工具结果之间插入消息”。
- 抢占做成可选能力，默认关闭。只允许临时消息、只允许 `before_model` 阶段、每个步骤最多生效一次。它取消的是当前这一次模型尝试，不是整个运行，也不是当前轮。
- `Steer` 保持现有签名和行为；新增 `SteerWith` 接收投递选项。持久收件箱新增 `DeliverWith` 接收阶段，`Deliver` 保持等价于 `before_model`。
- 回执分两层：临时消息的 ticket 有 `queued` / `delivered` / `rejected` 三个状态；持久消息的 `Deliver` 只返回“已接受（可重复）”，消费由“消息从 Inbox 消失 + `InputDelivered` 事件”共同观察，语义是至少一次。

P1-6 的最终形态：

- 在工具批次通过全部执行前检查（`CheckToolPolicy`、`BeforeTool`）之后、任何 handler 启动之前，写一次普通检查点，携带助手工具调用消息、改写后的调用列表和 `pending` 执行记录。这个检查点用新的 `Checkpoint.InFlight` 字段表示，不使用 `Pending` 或 `Pause`。
- 它**不是暂停**：没有人工审批，`RunStatus.Paused` 仍为 false，新增 `RunStatus.InFlight` 单独报告；恢复入口是 `Agent.Resume`，不是审批流。
- 崩溃恢复按台账状态处理每个调用：`completed` 直接回放结果，`claimed` / `unknown` 绝不自动执行，返回“结果未知”，`pending` 认领后执行。每个调用最终恰好一条结果，助手工具调用消息不会悬空。
- 恢复在同一逻辑步骤执行，调用编号（`toolInvocation`）因此与崩溃前一致；恢复不重跑 `CheckToolPolicy` 和 `BeforeTool`，只执行已经批准的批次。
- 只对持久化且处于顶层的运行写这个检查点；普通 `Checkpointer`、子代理、分支、以及人工审批恢复批次都不写。默认开启，可按 Agent 配置关闭。

两个改动的关系：

- P1-5 解决“输入什么时候进入、是否可以打断模型”。
- P1-6 解决“模型决定调用工具之后、批次结束之前崩溃，恢复时怎样不重复副作用、不丢消息”。
- 两者在时间上不重叠：抢占只发生在模型调用期间，在途检查点只发生在工具批次之前。一个步骤内可以先被抢占重试，再进入工具批次并写一次在途检查点。

## 2. 目标与非目标

### 2.1 目标

1. 统一读取临时消息和持久收件箱，避免每个投递点各写一套过滤、确认和关闭逻辑。
2. 给每条输入一个明确的投递阶段，保证“下一轮才处理”的消息不会提前进入当前模型调用。
3. 给临时消息一个可观察的回执，调用方不再只能靠“没报错”猜测消息是否被消费。
4. 提供可选的模型调用抢占：用户明确要求停止当前生成时，取消当前流、保留已到达的输入并在同一步重试，不结束运行。
5. 在工具批次执行前建立持久边界：崩溃后头部检查点包含计划执行的调用，恢复时能区分“已完成、可能已发生、从未开始”。
6. 保证恢复后的会话记录合法：每个已计划的工具调用恰好有一条结果，助手工具调用消息不会因为缺少结果而被丢弃或重放。
7. 让“在途”与“暂停”在状态、事件、元数据和存储校验上完全分开，界面不会误报“等待人工审批”。
8. 保持旧检查点、旧收件箱记录和旧提交摘要可读、可重试，不要求数据迁移。

### 2.2 非目标

- 不把四个阶段做成四个真实插入口。模型请求与工具结果之间没有安全的消息插入点，见 §5.2。
- 不把工具结果或系统提示当作输入队列的来源。它们由循环或中间件产生，入口已经明确，不需要再排队。
- 不实现持久消息的抢占。持久消息已经写入收件箱，取消模型调用不能撤回它的确认语义；它只参与正常投递。
- 不实现“只对非幂等工具开启在途检查点”。当前没有幂等性声明，见 §9。
- 不改变工具台账 `pending` / `claimed` / `completed` / `unknown` 的状态机，也不改变 `ClaimTool` 的语义。
- 不实现自动恢复。队列或进程崩溃后是否调用 `Resume` 仍由上层决定；本文只提供可恢复的边界和状态。
- 不实现进程内活动运行登记表、跨进程实时投影、会话忙时排队（P1-7 / P1-8）。
- 不保证跨生产者的全局顺序。只保证同一来源、同一阶段内按入队顺序投递。
- 不保证持久输入的恰好一次消费。确认与检查点原子提交，但传输失败后的重投仍可能造成重复，语义是至少一次。

## 3. 现状

### 3.1 临时消息通道

`RunContext` 持有一个 `steering steeringQueue`，见 [agent/runtime.go:49](../../agent/runtime.go#L49)。`Agent.Stream` 返回的 `Run.Steer` 把消息压入这个队列并返回 ticket，见 [agent/run.go:146](../../agent/run.go#L146)；`RunContext.Steer` 直接委托给队列，见 [agent/runtime.go:128](../../agent/runtime.go#L128)。

队列本身只有“消息 + ticket”两项，见 [agent/steering.go:83](../../agent/steering.go#L83)；入队、整体取出、取出或关闭、关闭分别见 [agent/steering.go:100](../../agent/steering.go#L100)、[agent/steering.go:113](../../agent/steering.go#L113)、[agent/steering.go:127](../../agent/steering.go#L127)、[agent/steering.go:144](../../agent/steering.go#L144)。ticket 的结算是“已投递（带步骤号）”或“未投递（带错误）”，见 [agent/steering.go:159](../../agent/steering.go#L159) 和 [agent/steering.go:166](../../agent/steering.go#L166)。

运行中只有两处读取临时消息：

- `PrepareTurn`：每一步进入模型调用前整体取空，追加到历史并把 ticket 标记为已投递，见 [agent/loop.go:259](../../agent/loop.go#L259)。
- `settleTurn`：模型给出终稿后整体取空，有消息就继续下一轮，见 [agent/loop.go:216](../../agent/loop.go#L216)。

队列在运行结束时由 `run` 的 defer 关闭，见 [agent/loop.go:103](../../agent/loop.go#L103)；`Cancel` 也会关闭队列，见 [agent/run.go:155](../../agent/run.go#L155)。由于 `drainOrClose` 是一次原子操作，关闭瞬间到达的消息不会悬空，见 [agent/loop.go:236](../../agent/loop.go#L236)。

现状的缺口：

- 没有阶段。所有消息都在下一个 `PrepareTurn` 立刻投递，无法表达“这轮回答结束后再处理”。
- 没有抢占。模型流一旦开始，临时消息只能等这次调用结束。
- ticket 只有“已投递 / 未投递”，没有“排队中”的显式状态；调用方在 `Outcome()` 上看到的是零值，见 [agent/steering.go:55](../../agent/steering.go#L55)。

### 3.2 持久收件箱

`checkpoint.Durable` 提供 `Deliver`、`Inbox`、`Commit`，见 [checkpoint/durable.go:15](../../checkpoint/durable.go#L15)。`InboxMessage` 只有 ID 和消息，见 [checkpoint/durable.go:60](../../checkpoint/durable.go#L60)；`DeliveryReceipt` 只有 ID 和“是否重复”，见 [checkpoint/durable.go:65](../../checkpoint/durable.go#L65)。

`Agent.Deliver` 校验线程和消息 ID，要求存储实现 `Durable`，然后委托，见 [agent/inbox.go:15](../../agent/inbox.go#L15)。内存存储在 `Deliver` 时按 ID 去重，见 [checkpoint/durable_memory.go:65](../../checkpoint/durable_memory.go#L65)；`Inbox` 按插入顺序返回，见 [checkpoint/durable_memory.go:87](../../checkpoint/durable_memory.go#L87)。文件存储走同样的语义，见 [checkpoint/file.go:183](../../checkpoint/file.go#L183) 和 [checkpoint/file.go:211](../../checkpoint/file.go#L211)。

消费发生在 `PrepareTurn`：读取整个 Inbox，追加到历史，登记待确认 ID，见 [agent/loop.go:265](../../agent/loop.go#L265)。确认不是立刻写入，而是跟下一次检查点提交一起原子生效，见 [agent/durable_runtime.go:98](../../agent/durable_runtime.go#L98) 和 [agent/durable_runtime.go:366](../../agent/durable_runtime.go#L366)。`settleTurn` 只检查 Inbox 是否还有未确认消息来决定是否续跑，不把消息追加到历史，见 [agent/loop.go:222](../../agent/loop.go#L222)。

现状的缺口：

- 没有阶段。Inbox 中的所有消息都会在下一个 `PrepareTurn` 进入历史。
- 没有“已消费”回执。`Deliver` 只能证明消息被接受，不能证明某个步骤消费了它。
- 去重只比较消息体，不比较阶段，见 [checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419)。

### 3.3 工具批次

工具执行入口是 `execTools`，见 [agent/exectools.go:42](../../agent/exectools.go#L42)。它要求一个已经冻结的步骤视图，见 [agent/exectools.go:43](../../agent/exectools.go#L43)，决定并发分段，见 [agent/exectools.go:149](../../agent/exectools.go#L149)，并为每个调用执行：

1. 检查工具是否在本步广播、能否解析、参数是否合法，见 [agent/exectools.go:313](../../agent/exectools.go#L313)。
2. 计算调用编号，见 [agent/exectools.go:348](../../agent/exectools.go#L348)。
3. 持久化存储存在时，先 `ClaimTool`，见 [agent/exectools.go:351](../../agent/exectools.go#L351)。已完成的结果直接回放，见 [agent/exectools.go:367](../../agent/exectools.go#L367)；已认领但未完成返回“结果未知”，见 [agent/exectools.go:360](../../agent/exectools.go#L360)；待执行或新调用进入执行。
4. 调用 handler，见 [agent/exectools.go:399](../../agent/exectools.go#L399)，把完成记录放进本运行的内存台账，见 [agent/exectools.go:128](../../agent/exectools.go#L128)。

检查点写入发生在整批结束之后，即 Phase 4，见 [agent/loop.go:516](../../agent/loop.go#L516)。在这之前，包含工具调用的助手消息只存在于运行内存中；崩溃恢复只能看到上一份检查点。执行记录是另一条线：`ClaimTool` 可能已经把台账从 `pending` 推到 `claimed`，但没有对应的会话记录。`toolInvocation` 把线程、步骤、调用 ID、工具名和参数一起哈希，见 [agent/exectools.go:426](../../agent/exectools.go#L426)。

人工审批暂停是当前唯一会提前写“待执行工具”的路径：`interrupt` 把 `PendingHITL` 和 `pendingExecutions` 一起提交，见 [agent/loop.go:601](../../agent/loop.go#L601) 和 [agent/loop.go:614](../../agent/loop.go#L614)。这不是工具批次的一般边界，只覆盖了“等待审批、尚未执行”的情况。

现状的缺口：

- 批次中途崩溃时，头部检查点没有助手工具调用消息，恢复时模型只能重新发起调用。
- 重新发起时，调用 ID 或参数只要变化，就生成新的调用编号；旧编号可能已经产生副作用，成为孤儿。
- 没有一条“批次计划”把“哪些调用属于同一次执行”固定下来，恢复无法按原批次判断每个调用的状态。

### 3.4 提案与当前代码的差异

提案写于 P0 落地之前，部分行号和判断已经过时。下面这些差异直接影响 P1-5 / P1-6 的设计。

| 提案中的说法 | 当前代码事实 | 对设计的影响 |
|---|---|---|
| 两条输入通道在 [agent/loop.go:181](../../agent/loop.go#L181)、[agent/loop.go:184](../../agent/loop.go#L184) 分两次读取 | 当前 `PrepareTurn` 在 [agent/loop.go:259](../../agent/loop.go#L259) 和 [agent/loop.go:265](../../agent/loop.go#L265) 读取；`settleTurn` 在 [agent/loop.go:216](../../agent/loop.go#L216) 和 [agent/loop.go:222](../../agent/loop.go#L222) 读取 | 设计以当前代码为准，不按旧行号改 |
| 工具执行前登记在 [agent/exectools.go:341](../../agent/exectools.go#L341) | `ClaimTool` 调用在 [agent/exectools.go:351](../../agent/exectools.go#L351) | 在途检查点必须在这之后、handler 之前写 |
| 助手工具调用消息到批次结束才写检查点，[agent/loop.go:331](../../agent/loop.go#L331) | Phase 4 检查点在 [agent/loop.go:516](../../agent/loop.go#L516) | 需要新增执行前检查点，不是移动 Phase 4 |
| 在途记录只在审批暂停时写，[agent/loop.go:400](../../agent/loop.go#L400) | 审批暂停的 `pendingExecutions` 在 [agent/loop.go:614](../../agent/loop.go#L614) | 审批暂停仍然保留，不复用在途字段 |
| 调用编号在 [agent/exectools.go:416](../../agent/exectools.go#L416) | 实际在 [agent/exectools.go:426](../../agent/exectools.go#L426) | 恢复必须回到同一逻辑步骤才能命中旧编号 |
| 输入来源包括工具结果、系统提示 | 工具结果由循环追加，系统提示在请求组装时渲染，见 [agent/loop.go:300](../../agent/loop.go#L300) | 只把临时消息和持久收件箱当作输入来源 |
| 四个投递阶段可以在同一步内分别插入 | 助手工具调用消息和工具结果必须成对连续出现；中间插消息会让提供方收到非法会话 | 四个阶段压缩为两个安全投递点，阶段只表示“最晚何时投递” |
| 持久收件箱可以提供已消费回执 | Inbox 只保存未确认消息；`Deliver` 只返回接受或重复 | 消费用“从 Inbox 消失 + `InputDelivered` 事件”观察，语义是至少一次 |
| 可以先只对非幂等工具开启 | 工具接口没有幂等性声明，只有调用编号里的幂等键 | 默认对所有工具批次开启，提供整体关闭开关 |
| 在途状态可以做成一个阶段 | `Paused` 由 `Pending` / `Pause` 推导，见 [agent/run_status.go:66](../../agent/run_status.go#L66) | 新增独立字段和状态，绝不把在途当成暂停 |
| 恢复时扫描执行记录，把检查点没覆盖的登记标为未知 | 台账没有按步骤扫描的接口，只有按调用编号读取，见 [checkpoint/durable.go:29](../../checkpoint/durable.go#L29) | 用“批次覆盖”代替全表扫描：批次记录了本次执行的全部计划调用 |

其他会影响设计的代码事实：

- 两个内置存储都实现了 `ThreadCoordinator`，`BeginRun` 只拒绝 `Pending` / `Pause`，见 [checkpoint/coordinator_memory.go:26](../../checkpoint/coordinator_memory.go#L26) 和 [checkpoint/coordinator_file.go:23](../../checkpoint/coordinator_file.go#L23)。
- `StepView` 冻结时记录广告出去的工具名、工具摘要、设置摘要和模型，见 [agent/stepview.go:388](../../agent/stepview.go#L388)；`resumeStepView` 直接复制保存的工具摘要，见 [agent/stepview.go:355](../../agent/stepview.go#L355)。在途恢复如果要比对工具是否变化，必须另外计算当前摘要。
- `BeforeTool` 可以改写参数，见 [agent/loop.go:487](../../agent/loop.go#L487)。因此助手消息里的参数是模型原始参数，在途批次里的参数是改写后的参数，两者不能逐字节比较。
- `streamModel` 在没有任何最终响应时仍返回一个空的 `llm.Response` 和 `ok=true`，见 [agent/loop.go:550](../../agent/loop.go#L550) 和 [agent/loop.go:565](../../agent/loop.go#L565)。抢占需要区分“被取消、没有终稿”和“正常返回”。
- `fail` 会调用 `seam` 去掉末尾未回答的助手工具调用消息，见 [agent/loop.go:641](../../agent/loop.go#L641) 和 [agent/loop.go:653](../../agent/loop.go#L653)。批次中途失败时，如果只调用 `fail`，会话记录会丢掉助手调用，但副作用可能已经发生。
- `Fork` 会清空 `Pending`、`Pause`、`Completion`，见 [checkpoint/branch.go:45](../../checkpoint/branch.go#L45)，但不会复制工具台账。带在途批次的父检查点不能被直接分叉。
- 元数据投影只记录 `Paused`，见 [checkpoint/query.go:158](../../checkpoint/query.go#L158)；侧车重建用 `summaryPauseMarker` 模拟暂停，见 [checkpoint/metadata_index.go:469](../../checkpoint/metadata_index.go#L469)。在途需要同样的投影支持。
- 提交摘要把检查点内容编码后排序哈希，见 [checkpoint/coordinator.go:137](../../checkpoint/coordinator.go#L137)。新增字段必须可省略，否则旧提交的重试摘要会变化。

## 4. 设计原则

1. **只在安全会话边界投递。** 模型请求和工具结果之间没有合法的消息插入点。任何“立即插入”的语义都必须落到下一次 `PrepareTurn` 或 `settleTurn`。
2. **阶段是“最晚投递时间”，不是控制协议。** 消息可以早于阶段投递，不能晚于阶段允许的时间。这样实现简单，也不会产生非法会话。
3. **临时消息先于持久消息。** 保持现有 `PrepareTurn` 的读取顺序；同一来源内按入队顺序。跨生产者的全局顺序不做保证。
4. **抢占取消一次模型尝试，不取消运行。** 运行上下文、线程占用、步号、已投递的输入都不变；抢占只让当前这次提供方调用停下，然后在同一步重新组装请求。
5. **在途检查点是台账边界，不是暂停。** 它没有审批、没有 `Pause`、没有 `Pending`；`RunStatus.Paused` 不变，恢复入口是 `Resume`。
6. **执行前检查只跑一次。** `CheckToolPolicy` 和 `BeforeTool` 在写检查点之前完成；恢复不再重跑，避免重复副作用和参数二次改写。
7. **输入至少一次，结果恰好一条。** 持久输入可能重复投递，消费方必须有幂等处理；工具批次恢复必须为每个计划调用留下且只留下一条结果。
8. **冻结视图是权威。** 恢复用保存的步骤视图重建本步工具表；不重新读取当步的可变配置来“修正”历史。
9. **先持久化，再产生副作用。** 在途检查点提交成功之前，任何 handler 都不能启动；提交失败就结束运行，不执行工具。
10. **失败路径也要留下合法会话。** 批次中途出错时，先补齐每个调用的结果，再写失败检查点；不能让助手工具调用消息悬空。

## 5. P1-5 核心设计

### 5.1 数据模型

#### 5.1.1 公共类型

新增 `core/input.go`，只放阶段、来源、回执状态和错误，不依赖 `agent` 或 `checkpoint`。

```go
package core

import (
	"errors"
	"fmt"
)

// InputPhase 表示一条输入最晚在什么时候进入运行。
// 空值等价于 InputBeforeModel。
type InputPhase string

const (
	InputBeforeModel InputPhase = "before_model"
	InputAfterModel  InputPhase = "after_model"
	InputAfterTools  InputPhase = "after_tools"
	InputNextTurn    InputPhase = "next_turn"
)

// InputSource 表示输入从哪条物理通道来。
type InputSource string

const (
	InputVolatile InputSource = "volatile"
	InputDurable  InputSource = "durable"
)

// InputState 是投递回执。
type InputState string

const (
	InputQueued    InputState = "queued"
	InputDelivered InputState = "delivered"
	InputRejected  InputState = "rejected"
)

var ErrUnknownInputPhase = errors.New("core: unknown input phase")

func NormalizeInputPhase(p InputPhase) (InputPhase, error) {
	if p == "" {
		return InputBeforeModel, nil
	}
	switch p {
	case InputBeforeModel, InputAfterModel, InputAfterTools, InputNextTurn:
		return p, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownInputPhase, p)
	}
}
```

约束：

- 这四个阶段是**闭集**。未知阶段在入队或持久投递时直接拒绝，不猜测、不降级。
- 空阶段只在读取旧记录时出现，按 `before_model` 处理。
- `InputSource` 只用于事件和回执展示，不参与调度。

#### 5.1.2 临时消息的选项与 ticket

新增 `SteerOptions`，并给 `SteerTicket` 增加显式状态。现有的 `Steer` 行为保持等价于 `SteerWith(msg, SteerOptions{})`。

```go
// agent/steering.go

type SteerOptions struct {
	Phase   core.InputPhase
	Preempt bool
	Reason  string
}

type SteerOutcome struct {
	Delivered bool
	Step      int
	Err       error
	// State 是结算后的回执状态：delivered 或 rejected。
	// 未结算时 Outcome 仍返回零值，State() 返回 queued。
	State core.InputState
}

func (t *SteerTicket) State() core.InputState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.settled {
		return core.InputQueued
	}
	if t.outcome.Err != nil {
		return core.InputRejected
	}
	return core.InputDelivered
}
```

`SteerOutcome` 增加 `State` 不改变 `Outcome()` 的兼容行为：未结算时仍返回零值。调用方要问“现在是什么状态”用 `State()`，要等结果用 `Wait()` / `Outcome()`。

队列条目增加阶段：

```go
type steerEntry struct {
	msg    core.Message
	ticket *SteerTicket
	phase  core.InputPhase
}
```

`push` 保留，内部委托给 `pushWith`：

```go
func (q *steeringQueue) push(msg core.Message) (*SteerTicket, error) {
	return q.pushWith(msg, SteerOptions{})
}

func (q *steeringQueue) pushWith(msg core.Message, opts SteerOptions) (*SteerTicket, error) {
	phase, err := core.NormalizeInputPhase(opts.Phase)
	if err != nil {
		return nil, err
	}
	// 入队、生成 ticket、保存 phase，全部在同一把锁内。
}
```

`drain()` 保持“取走全部”的语义，供 `settleTurn` 和关闭路径使用。新增两个按条件取走的方法：

```go
// takeReady 取走本投递点可以投递的条目：
// newTurn 为 true 时取全部；为 false 时保留 next_turn。
func (q *steeringQueue) takeReady(newTurn bool) []steerEntry

// takePhase 只取指定阶段的条目，供抢占使用。
func (q *steeringQueue) takePhase(phase core.InputPhase) []steerEntry
```

两者都在队列锁内完成“筛选 + 删除”，保持剩余条目的相对顺序。

#### 5.1.3 持久收件箱的阶段

`checkpoint.InboxMessage` 增加阶段字段：

```go
type InboxMessage struct {
	ID      string          `json:"id"`
	Message core.Message    `json:"message"`
	Phase   core.InputPhase `json:"phase,omitempty"`
}
```

- 新写入时先规范化，空值写成 `before_model`。
- 读取旧记录时，空值按 `before_model` 处理。
- `sameInboxMessage` 必须同时比较消息体和**规范化后的阶段**。同一个 ID、同样的消息、不同阶段是冲突，不是重复。
- `memory.Deliver`、`File.Deliver` 在入口校验阶段；文件日志的 `validateJournalRecord` 也要校验，见 [checkpoint/file.go:743](../../checkpoint/file.go#L743)。

`DeliveryReceipt` 增加状态，但只表示“已接受”：

```go
type DeliveryReceipt struct {
	ID        string
	Duplicate bool
	// State 恒为 queued：消息已进入收件箱，尚未被某个步骤消费。
	State core.InputState
}
```

持久消息没有“已消费”同步回执。消费的观察方式是：

1. 该 ID 不再出现在 `Inbox` 中；
2. 事件流上出现对应的 `InputDelivered`，事件带消息 ID。

两者要一起看，因为确认与检查点原子提交，但传输失败后的重投仍可能造成重复。语义是至少一次，不是恰好一次。

`Agent.Deliver` 保持原签名。新增：

```go
func (a *Agent) DeliverWith(
	ctx context.Context,
	threadID string,
	msg checkpoint.InboxMessage,
	phase core.InputPhase,
) (checkpoint.DeliveryReceipt, error)
```

`Deliver` 等价于 `DeliverWith(..., core.InputBeforeModel)`。

#### 5.1.4 逻辑输入队列

逻辑队列不是新的存储，也不是 `RunContext` 的新字段。它是由现有字段临时拼出的视图：

```go
type inputQueue struct {
	volatile *steeringQueue
	durable  checkpoint.Durable
	threadID string
}

func (rc *RunContext) inputs() inputQueue {
	durable, _ := rc.durableStore()
	return inputQueue{
		volatile: &rc.steering,
		durable:  durable,
		threadID: rc.ThreadID,
	}
}
```

这样做的好处：

- 不需要维护新字段，也就不会在 `deeper`、`subRun`、`forBranch` 这些派生上下文中出现指针失效。
- 临时消息仍然每个执行上下文独立；持久收件箱仍然共享。
- 测试可以直接构造 `inputQueue` 验证筛选逻辑，不需要启动整个 run。

队列提供三个读取方法：

```go
func (q inputQueue) takeVolatile(newTurn bool) []steerEntry
func (q inputQueue) takeVolatilePhase(phase core.InputPhase) []steerEntry
func (q inputQueue) takeDurable(ctx context.Context, newTurn bool) ([]checkpoint.InboxMessage, error)
```

`takeDurable` 的职责：

1. 读取 `Inbox`；
2. 跳过本运行已经登记确认、但尚未提交的 ID（复用 `pendingInboxAcks`，见 [agent/durable_runtime.go:125](../../agent/durable_runtime.go#L125)）；
3. 按阶段筛选；
4. 返回消息，由调用方追加历史并登记确认。

它**不**在读取时确认，确认仍由下一次检查点提交完成。读取失败原样返回，由循环走失败路径。

### 5.2 投递点与阶段

#### 5.2.1 只有两个安全投递点

一个步骤的会话顺序是固定的：

```text
PrepareTurn: 输入 → 历史
CallModel:   请求 → 助手消息（可能是终稿，也可能带工具调用）
ExecuteTools: 助手工具调用消息 → 工具结果消息
Checkpoint:  写入步骤结果
```

模型请求和工具结果之间不能插入用户消息，否则提供方会收到“助手要求调用工具、下一条却不是工具结果”的非法会话。因此：

- `before_model`、`after_model`、`after_tools` 的物理投递点都是**下一次 `PrepareTurn`**。
- `next_turn` 的物理投递点是**下一次 `newTurn=true` 的 `PrepareTurn`**。
- 终稿之后没有下一次模型调用时，三个“本轮内”阶段和 `next_turn` 一起落到 `settleTurn`。

#### 5.2.2 就绪矩阵

| 读取点 | newTurn | before_model | after_model | after_tools | next_turn |
|---|---:|---|---|---|---|
| 首次运行的 `PrepareTurn` | true | 投递 | 投递 | 投递 | 投递 |
| 工具批次后的 `PrepareTurn` | false | 投递 | 投递 | 投递 | 保留 |
| `max_tokens` 截断后的 `PrepareTurn` | false | 投递 | 投递 | 投递 | 保留 |
| 人工审批/在途批次恢复后的 `PrepareTurn` | false | 投递 | 投递 | 投递 | 保留 |
| 终稿后的 `settleTurn` | — | 投递 | 投递 | 投递 | 投递 |
| 抢占 | false | 投递 | 保留 | 保留 | 保留 |

关于这张表的说明：

- `after_model` / `after_tools` 是**上界**。在 `newTurn=false` 的 `PrepareTurn` 投递它们，表示“上一个模型调用/工具批次已经结束”，即使当前步骤是因为 `max_tokens` 截断而没有真正执行工具，也不会造成非法会话：此时工具调用已经用错误结果闭合。
- `next_turn` 在 `settleTurn` 投递是安全的，因为 `settleTurn` 本身就是当前轮的结束点；投递后下一轮从 `newTurn=true` 开始。
- 抢占只投递 `before_model`。其他阶段的消息留在队列里，等下一次正常投递点。

#### 5.2.3 newTurn 的来源

给 `taskState` 增加一个字段：

```go
type taskState struct {
	step      int
	maxStep   int
	history   []core.Message
	system    string
	usePrompt bool
	newTurn   bool
}
```

规则：

- 全新运行：`newTurn=true`。
- `Agent.Resume` 产生的运行：`newTurn=false`。恢复是继续原来的轮次，不是新的一轮；即使恢复时没有待执行批次、下一步是重新问模型，也仍然属于同一轮。
- `settleTurn` 续跑：`newTurn=true`。
- 工具批次后、`max_tokens` 后、人工审批恢复批次后、在途批次恢复后：`newTurn=false`。
- `PrepareTurn` 只读取这个值，不在读取时自行推断。

`RunContext` 增加一个只在恢复路径设置的标记，例如 `continuedTurn bool`，由 `Agent.Resume` 设置，见 [agent/hitl.go:167](../../agent/hitl.go#L167)。`prepareRun` 用它决定第一个步骤的 `newTurn`，见 [agent/loop.go:181](../../agent/loop.go#L181)。

#### 5.2.4 PrepareTurn 的读取顺序

保持现有顺序：先临时消息，后持久收件箱。原因是临时消息不落盘、延迟最低；持久收件箱可能较慢，不能阻塞临时消息进入本步。

```go
func (l *AgentLoop) runStep(rc *RunContext, st taskState) (...) {
	// ...
	rc.publish(core.TurnStarted{Step: step})

	q := rc.inputs()
	if entries := q.takeVolatile(st.newTurn); len(entries) > 0 {
		for _, e := range entries {
			history = append(history, e.msg)
		}
		settleDelivered(entries, step)
		rc.publishDeliveredInputs(step, core.InputVolatile, entriesToInputs(entries), false)
	}
	if inbox, err := q.takeDurable(rc, st.newTurn); err != nil {
		return l.fail(rc, step, history, nil, err), st, false
	} else if len(inbox) > 0 {
		for _, msg := range inbox {
			history = append(history, msg.Message)
		}
		rc.addInboxAcks(inboxIDs(inbox))
		rc.publishDeliveredInputs(step, core.InputDurable, inboxToInputs(inbox), false)
	}
	// CompactHistory、BeforeModel、组装请求……
}
```

细节：

- `settleDelivered(entries, step)` 在追加成功后立即结算 ticket，与现有行为一致。
- 持久消息只登记确认，不结算 ticket（它没有 ticket）。
- `InputDelivered` 在消息进入工作历史后发布。对持久消息来说，这表示“进入本步历史”，不表示“确认已经落盘”。
- `takeDurable` 的读取顺序就是 Inbox 的插入顺序；不按阶段重新排序。同一来源内保持先进先出。
- 如果同一阶段有多条消息，事件里的 `Count` 是条数，`MessageIDs` 只对持久消息填充。

#### 5.2.5 settleTurn 的处理

`settleTurn` 的现有顺序不变：先检查预算，再取临时消息，再看持久收件箱是否存在未确认消息，最后做一次原子关闭检查。

```go
func (l *AgentLoop) settleTurn(rc *RunContext, st taskState, history []core.Message) (...) {
	if st.step+1 >= st.maxStep {
		settleUndelivered(rc.steering.drain(), ErrMaxTurnsExceeded)
		rc.steering.close(ErrRunClosed)
		return history, nil, false, nil
	}
	if entries := rc.steering.drain(); len(entries) > 0 {
		for _, e := range entries {
			history = append(history, e.msg)
		}
		return history, entries, true, nil
	}
	if durable, ok := rc.durableStore(); ok {
		inbox, err := durable.Inbox(rc, rc.ThreadID)
		if err != nil {
			return history, nil, false, err
		}
		pending := rc.pendingInboxAcks()
		for _, msg := range inbox {
			if _, consumed := pending[msg.ID]; consumed {
				continue
			}
			return history, nil, true, nil
		}
	}
	late, _ := rc.steering.drainOrClose(ErrRunClosed)
	// ...
}
```

与阶段相关的两点：

1. `settleTurn` 使用 `drain()` 而不是 `takeReady`。终稿意味着当前轮已经结束，所有阶段都可以进入下一轮，包括 `next_turn`。
2. 持久收件箱只做“是否存在未确认消息”的判断，不追加。存在 `next_turn` 消息也会让运行继续；真正的追加发生在下一轮 `PrepareTurn`，那时 `newTurn=true`。

持久消息在 `settleTurn` 不追加、不确认，是现有设计：`settleTurn` 的目标是尽快决定是否续跑，而不是读慢存储。阶段筛选在 `PrepareTurn` 完成。

#### 5.2.6 阶段与 turn 预算

`maxTurns` 仍然是整个运行的步数预算，续跑会消耗步数。`next_turn` 不能绕过预算：预算用完时，`settleTurn` 先结算并拒绝所有临时消息，见 [agent/loop.go:211](../../agent/loop.go#L211)。

持久消息不受 turn 预算影响：它们留在 Inbox 中，下一次运行可以继续消费。临时消息在预算耗尽时被拒绝，ticket 状态为 `rejected`。

### 5.3 回执状态机

#### 5.3.1 临时消息

```text
pushWith 成功
    │
    ▼
 queued ──投递成功──▶ delivered
    │
    └──预算耗尽 / 运行关闭 / 取消 / 检查点失败──▶ rejected
```

规则：

- `queued` 只在 `State()` 中出现；`Outcome()` 在未结算时仍返回零值，保持旧测试和旧调用方的行为。
- 每次结算恰好一次。`settle` 内部检查 `settled`，重复调用无效果。
- `delivered` 的 `Step` 是消息进入历史的那一步。抢占重试的 `Step` 是同一步，不是下一步。
- `rejected` 必须带错误：`ErrRunClosed`、`ErrMaxTurnsExceeded`、检查点错误或运行停止原因。
- `SteerWith` 在入队前发现参数错误时返回 `nil` ticket 和错误，不产生 `rejected` ticket；这与现有 `Steer` 在队列关闭时返回 `nil, ErrRunClosed` 的行为一致。

#### 5.3.2 持久消息

```text
DeliverWith 成功
    │  receipt.State = queued, Duplicate 表示同一 ID 同内容同阶段已存在
    ▼
Inbox 中可见
    │  某个 PrepareTurn 追加历史并登记确认
    ▼
下一次检查点提交成功
    │  消息 ID 从 Inbox 消失
    ▼
事件流出现 InputDelivered（可能早于确认提交）
```

注意：

- `InputDelivered` 表示消息已经进入某个步骤的工作历史，模型会看到它；它不保证确认已经提交。
- “已消费”没有同步回执。生产方要确认消费，应同时观察 Inbox 和事件流，并接受至少一次语义。
- 如果检查点提交失败，消息仍在 Inbox，下一次运行会再次投递。调用方不能因为看到过一次 `InputDelivered` 就假定消息只会出现一次。

### 5.4 抢占

#### 5.4.1 API 与配置

```go
func (r *Run) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error)
func (rc *RunContext) SteerWith(msg core.Message, opts SteerOptions) (*SteerTicket, error)

func (r *Run) Steer(msg core.Message) (*SteerTicket, error) {
	return r.SteerWith(msg, SteerOptions{})
}
```

新增 Agent 选项：

```go
// WithInputPreemption 打开模型调用抢占，默认关闭。
func WithInputPreemption(enabled bool) Option
```

`config` 增加 `inputPreemption bool`，见 [agent/options.go:16](../../agent/options.go#L16)；`newRunHandleWithID` 把值复制到 `RunContext`，见 [agent/agent.go:236](../../agent/agent.go#L236)。

抢占只对临时消息生效。持久消息即使带 `Preempt` 也没有意义：`DeliverWith` 没有抢占参数，持久层也不参与模型调用的取消。

#### 5.4.2 参数校验

| 输入 | 结果 |
|---|---|
| 阶段为空 | 规范化为 `before_model` |
| 阶段未知 | `nil, core.ErrUnknownInputPhase` |
| `Preempt=true` 且阶段不是 `before_model` | `nil, ErrPreemptionPhase` |
| `Preempt=true` 且 Agent 未开启抢占 | `nil, ErrPreemptionDisabled` |
| 队列已关闭 | `nil, ErrRunClosed` |
| 队列可用但没有活动模型调用 | 消息按普通 `before_model` 入队，不报错 |
| 本步骤已经生效过一次抢占 | 消息按普通 `before_model` 入队，不报错 |

“没有活动调用”和“本步骤已经抢占过”不返回错误，是刻意的：

- 抢占是“如果正在生成就停下”的意图，不是协议保证。模型可能刚好已经结束，调用方不应该因为这个竞态拿到失败。
- 一个步骤只需要一次取消就能让消息尽快进入重试；第二次请求继续取消没有收益，只会增加竞态。

#### 5.4.3 活动模型尝试登记

`RunContext` 增加一组只在模型调用期间使用的字段：

```go
type modelAttempt struct {
	step      int
	cancel    context.CancelFunc
	preempted bool
	done      bool
	reason    string
}

// RunContext
modelMu      sync.Mutex
modelAttempt *modelAttempt
preempted    bool
preemptStep  int
```

方法：

```go
func (rc *RunContext) beginModelAttempt(ctx context.Context, step int) (context.Context, *modelAttempt)
func (rc *RunContext) endModelAttempt(a *modelAttempt) (preempted bool)
func (rc *RunContext) requestPreemption(reason string) bool
```

- `beginModelAttempt` 用 `context.WithCancel` 从传入的上下文派生一次尝试上下文，并登记为当前活动尝试。
- `endModelAttempt` 标记 `done`、清空当前登记，并返回这次尝试是否被抢占。调用方必须在读取最终响应之后调用它，确保“成功优先”的判断看到的是完整结果。
- `requestPreemption` 在锁内检查：是否存在活动尝试、该尝试是否已结束、本步骤是否已经抢占过。满足条件时设置 `preempted`、`preemptStep` 和尝试的 `reason`，返回 `true`；调用方在锁外执行 `cancel`。
- 每个步骤只需要记录一次；不同步骤自然覆盖。`RunContext` 在 `deeper`、`subRun`、`forBranch` 中重新构造，不复制互斥锁和登记，见 [agent/runtime.go:65](../../agent/runtime.go#L65)。

#### 5.4.4 循环中的处理顺序

模型调用循环需要区分四种结果：

1. 拿到最终响应（`ok=true`）：成功优先。即使抢占已经生效，也使用这个响应；抢占消息仍在队列里，等下一次正常投递点。
2. 没有最终响应且被抢占，运行上下文未取消：进入抢占重试。
3. 没有最终响应且未被抢占：返回 `ErrModelNoFinal`，走失败路径。
4. 出错：如果运行上下文已取消，走现有停止/失败路径；如果被抢占，进入抢占重试；否则走现有错误处理和上下文超限恢复。

伪代码：

```go
for {
	genCtx, attempt := rc.beginModelAttempt(l.mw.ModelContext(lc, rc.Context), step)
	resp, ok, emitted, err := l.streamModel(genCtx, rc, lc, req)
	preempted := rc.endModelAttempt(attempt)

	if err == nil && ok {
		finalResp = resp
		final = resp.Message
		break
	}
	if preempted && rc.Context.Err() == nil {
		entries := rc.steering.takePhase(core.InputBeforeModel)
		for _, e := range entries {
			history = append(history, e.msg)
		}
		rc.publish(core.ModelPreempted{
			Step: step, Emitted: emitted, Count: len(entries), Reason: attempt.reason,
		})
		retryReq := &llm.Request{
			System:   view.System(),
			Messages: history,
			Tools:    view.Tools().Schemas(),
			Options:  view.Options(),
		}
		retryView, ferr := l.freezeStepView(lc, retryReq)
		if ferr != nil {
			settleUndelivered(entries, ErrRunClosed)
			return l.fail(rc, step, history, lc.viewSnapshot(), ferr), st, false
		}
		historyAfter := history
		rc.State.Messages = historyAfter
		if cerr := l.checkpoint(rc, commitOptions{
			Step: step, Snapshot: retryView.snapshot(), Release: false,
		}); cerr != nil {
			settleUndelivered(entries, ErrRunClosed)
			return l.fail(rc, step, historyAfter, retryView.snapshot(), cerr), st, false
		}
		settleDelivered(entries, step)
		rc.publish(core.InputDelivered{
			Step: step, Source: core.InputVolatile,
			Phase: core.InputBeforeModel, Count: len(entries), Preempted: true,
		})
		view, lc.view = retryView, retryView
		lc.Request, req = retryReq, retryReq
		lc.History = history
		continue
	}
	if err == nil {
		err = ErrModelNoFinal
	}
	// 原有上下文超限恢复和失败处理。
}
```

关键点：

- 抢占重试**不重新执行** `BeforeModel`、`ModifyRequest`、`freezeStepView` 的中间件。请求从当前冻结视图重建：`System`、`Tools`、`Options` 保持不变，`Messages` 换成追加了抢占消息的历史。这与上下文超限恢复的重建方式一致，见 [agent/loop.go:355](../../agent/loop.go#L355)。
- 抢占重试重新派生 `ModelContext`，保证观测上的 span 属于新一次尝试。
- 抢占重试写入一个 `Release:false` 的中间检查点。它不是最终状态，不释放线程占用；如果此时进程崩溃，恢复会从这一步重新问模型，而不是保留半次模型调用。
- 抢占消息在同一步结算，`SteerOutcome.Step` 是当前步骤号。
- 抢占重试不消耗上下文超限恢复预算；两个循环分别计数。
- 抢占消息进入历史后，如果重试仍然失败，`InputDelivered` 已经发布但线程可能回退。这与现有 `PrepareTurn` 的临时消息语义一致：临时消息不保证跨失败持久化。

#### 5.4.5 成功优先与运行取消优先

三种竞态必须有明确优先级：

| 竞态 | 结果 |
|---|---|
| 抢占生效后，提供方仍返回最终响应 | 使用最终响应；不重试；抢占消息留在队列，下一次正常投递 |
| 抢占生效后，运行上下文被取消 | 运行取消优先；不重试；按取消/失败路径结束；ticket 由关闭路径结算 |
| 抢占生效后，提供方返回空结果（无最终响应、无错误） | 视为被取消；运行上下文未取消则重试 |
| 抢占请求到达时模型刚结束 | 抢占不生效；消息按普通 `before_model` 排队 |
| 抢占请求到达时本步骤已经抢占过 | 消息按普通 `before_model` 排队 |

“成功优先”靠 `endModelAttempt` 之后先判断 `ok` 实现：只要拿到了最终响应，就不进入抢占分支。

“运行取消优先”靠 `rc.Context.Err() != nil` 判断：抢占尝试的上下文是运行上下文的子上下文，运行取消会同时取消它；此时不能把取消误判成抢占重试。

#### 5.4.6 streamModel 的调整

当前 `streamModel` 总是返回一个非空的 `final`（初始化为空响应），见 [agent/loop.go:550](../../agent/loop.go#L550)，并在结束时无条件返回 `ok=true`，见 [agent/loop.go:565](../../agent/loop.go#L565)。需要改成：

```go
func (l *AgentLoop) streamModel(...) (final *llm.Response, ok bool, emitted bool, err error) {
	var gotFinal bool
	for resp, err := range l.model.Generate(genCtx, req) {
		if err != nil {
			if genCtx.Err() == nil {
				_, _ = l.mw.OnError(lc, err)
			}
			return nil, false, emitted, err
		}
		if resp.Partial {
			emitted = true
			rc.publish(core.MessageDelta{Delta: resp.Message})
			continue
		}
		final = resp
		gotFinal = true
		emitted = true
		rc.publish(core.MessageDone{Message: resp.Message, Usage: resp.Usage})
	}
	return final, gotFinal, emitted, nil
}
```

两个行为变化：

- 没有最终响应时返回 `ok=false`，由调用方决定是重试（抢占）还是失败（`ErrModelNoFinal`）。
- 上下文已经取消时不再调用 `OnError`。抢占和运行取消都是取消路径，不应该被当作提供方错误上报。这是一个观测行为变化，写进兼容性说明。

#### 5.4.7 事件与界面契约

抢占会产生两类事件：

- `ModelPreempted`：告诉界面“当前这次模型尝试被取消，已经输出的增量要丢弃”。
- `InputDelivered`（`Preempted=true`）：告诉界面“这些消息已经进入重试”。

界面/流式消费方的契约：

1. 收到 `ModelPreempted` 时，丢弃当前这次尝试已经渲染的增量文本/工具调用草稿。`Emitted=false` 表示没有增量需要丢弃。
2. 随后的 `MessageDelta` / `MessageDone` 属于新的尝试，重新开始渲染。
3. 如果没有收到 `ModelPreempted`，增量仍然是同一次尝试，不能重置。
4. 被取消的尝试不会发布 `MessageDone`；只有成功拿到最终响应才会发布。

这个契约必须写进事件文档和示例，否则消费者会把两次尝试的增量拼在一起。

#### 5.4.8 持久消息不参与抢占

持久消息不参与抢占的原因：

- 它在 `Deliver` 时已经进入收件箱，生产方可能已经收到“已接受”。取消模型调用不能撤回这个接受。
- 抢占需要把消息从队列取出、追加历史、写检查点；持久消息的确认与检查点绑定，取消尝试时还没有可确认的步骤边界。
- 持久消息的语义是“至少一次、按阶段投递”，不是“尽快打断”。让它等待下一个投递点更简单，也不会产生“取消后消息还在 Inbox”与“消息已进入历史”的双重状态。

### 5.5 事件

新增两个事件，定义在 `core/event.go`：

```go
// InputDelivered 表示输入已经进入某个步骤的工作历史。
// Source 是 volatile 或 durable；Phase 是规范化后的阶段。
// MessageIDs 只对持久消息填充；Preempted 表示这次投递由抢占触发。
type InputDelivered struct {
	Step       int
	Source     InputSource
	Phase      InputPhase
	Count      int
	MessageIDs []string
	Preempted  bool
}

// ModelPreempted 表示当前模型尝试被抢占取消，随后会在同一步重试。
// Emitted 表示取消前是否已经发布过增量；Count 是本次进入重试的消息数。
type ModelPreempted struct {
	Step    int
	Emitted bool
	Count   int
	Reason  string
}
```

事件标记加进 `isEvent` 列表，见 [core/event.go:204](../../core/event.go#L204)。

JSON 线格式在 `eventWire` 增加：

```go
Source     string   `json:"source,omitempty"`
MessageIDs []string `json:"message_ids,omitempty"`
Preempted  bool     `json:"preempted,omitempty"`
Emitted    bool     `json:"emitted,omitempty"`
```

复用已有的 `Step`、`Phase`、`Count`、`Reason` 字段，见 [core/eventjson.go:18](../../core/eventjson.go#L18)。`MarshalEvent` 和 `UnmarshalEvent` 各加两个 case，见 [core/eventjson.go:61](../../core/eventjson.go#L61) 和 [core/eventjson.go:134](../../core/eventjson.go#L134)。

要求：

- 每种新事件都有往返测试：编码后解码，字段逐一相等。
- `Source` / `Phase` 用字符串传输；解码时保留原值，不在事件层做合法性校验。合法性在入队和投递时已经检查。
- `MessageIDs` 只用于持久消息；临时消息为空切片或 nil。
- `Count` 与 `MessageIDs` 的长度在持久消息场景下相等；临时消息场景只有 `Count`。

### 5.6 错误

新增错误：

```go
// agent
var (
	ErrPreemptionDisabled = errors.New("agent: input preemption is disabled")
	ErrPreemptionPhase    = errors.New("agent: only before_model input may preempt a model call")
	ErrModelNoFinal       = errors.New("agent: model stream ended without a final response")
)
```

复用：

- `core.ErrUnknownInputPhase`：阶段非法。
- `ErrRunClosed`：运行已关闭，消息被拒绝。
- `ErrMaxTurnsExceeded`：预算耗尽，临时消息被拒绝。

错误处理约定：

- 参数错误返回 `nil` ticket，不产生“已拒绝”的 ticket；调用方从函数返回值即可知道消息没有入队。
- 已入队但最终未投递的消息，ticket 状态为 `rejected`，错误放在 `Outcome().Err`。
- `ErrModelNoFinal` 只用于“没有抢占、也没有错误，但提供方没有给出最终响应”的情况。抢占路径优先重试。

## 6. P1-6 核心设计

### 6.1 数据模型

#### 6.1.1 InFlightBatch

新增 `checkpoint/inflight.go`：

```go
package checkpoint

import "github.com/jiujuan/goagent/core"

// InFlightBatch 是一次已经通过执行前检查、即将执行的工具批次。
// Calls 保存的是 BeforeTool 改写后的调用，参数用于重建调用编号；
// 它不是助手消息里的原始参数。
type InFlightBatch struct {
	Step  int             `json:"step"`
	Calls []core.ToolCall `json:"calls"`
}
```

`Checkpoint` 增加字段：

```go
// InFlight 标记一个已经计划、尚未产生结果的工具批次。
// 它不是暂停：没有审批、没有 Pending，恢复入口是 Agent.Resume。
// 写入后由记录结果的检查点清除。
InFlight *InFlightBatch `json:"in_flight,omitempty"`
```

放在 `Pause` 之后、`Snapshot` 之前，见 [checkpoint/checkpoint.go:65](../../checkpoint/checkpoint.go#L65)。

#### 6.1.2 互斥与校验

`InFlight` 与其他工作状态互斥：

- 不能同时有 `Pending`（人工审批）。
- 不能同时有 `Pause`（暂停）。
- 不能同时有 `Completion`（终态记录）。

新增校验函数，在 `freezeCheckpoint` 中调用，见 [checkpoint/snapshot.go:13](../../checkpoint/snapshot.go#L13)：

```go
func validateWorkState(cp *Checkpoint) error {
	if cp.InFlight == nil {
		return nil
	}
	if cp.Pending != nil || cp.Pause != nil || cp.Completion != nil {
		return fmt.Errorf("%w: in-flight batch cannot coexist with pending, pause or completion", ErrInFlightConflict)
	}
	if cp.InFlight.Step != cp.Step {
		return fmt.Errorf("%w: in-flight step %d does not match checkpoint step %d", ErrInFlightConflict, cp.InFlight.Step, cp.Step)
	}
	if len(cp.InFlight.Calls) == 0 {
		return fmt.Errorf("%w: in-flight batch has no calls", ErrInFlightConflict)
	}
	if cp.Snapshot == nil || cp.Snapshot.Step != cp.Step || cp.Snapshot.ToolDigest == "" {
		return fmt.Errorf("%w: in-flight batch requires the step snapshot that produced it", ErrInFlightConflict)
	}
	advertised := make(map[string]struct{}, len(cp.Snapshot.Advertised))
	for _, name := range cp.Snapshot.Advertised {
		advertised[name] = struct{}{}
	}
	for _, call := range cp.InFlight.Calls {
		if call.ID == "" || call.Name == "" {
			return fmt.Errorf("%w: in-flight call requires ID and name", ErrInFlightConflict)
		}
		if _, ok := advertised[call.Name]; !ok {
			return fmt.Errorf("%w: in-flight call %q uses tool %q that the step did not advertise", ErrInFlightConflict, call.ID, call.Name)
		}
	}
	return nil
}
```

校验放在存储层而不是只放在循环里，原因：

- 自定义存储、手工构造的检查点、分支复制都会经过 `freezeCheckpoint`；只靠循环无法保证所有写入口。
- 互斥关系是存储不变量，不只是运行时约定。
- 摘要计算依赖字段已经合法，先校验再哈希。

不要求调用 ID 唯一。模型可能产生重复 ID；当前 `execTools` 按顺序处理，恢复也按顺序匹配。要求唯一会拒绝本来可以运行的批次。

#### 6.1.3 克隆

`cloneCheckpoint` 必须深拷贝 `InFlight`，见 [checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30)：

```go
func cloneInFlight(in *InFlightBatch) *InFlightBatch {
	if in == nil {
		return nil
	}
	out := *in
	out.Calls = make([]core.ToolCall, len(in.Calls))
	for i, call := range in.Calls {
		call.Args = json.RawMessage(cloneSnapshotBytes(call.Args))
		out.Calls[i] = call
	}
	return &out
}
```

原因：`core.ToolCall.Args` 是 `json.RawMessage`，切片共享会让检查点持有调用方的可变字节；`freezeCheckpoint` 的契约是“成为可见记录后不再受外部修改影响”。

#### 6.1.4 状态与元数据

`RunStatus` 增加字段：

```go
// InFlight 表示头部检查点是一个工具批次执行前的边界。
// 它不是暂停：没有待审批调用，下一步是 Agent.Resume。
InFlight bool
```

设置方式：

```go
status.Paused = latest.Completion == nil && (latest.Pause != nil || latest.Pending != nil)
status.InFlight = latest.Completion == nil && latest.InFlight != nil
```

`Paused` 的表达式保持不变，见 [agent/run_status.go:66](../../agent/run_status.go#L66)。在途检查点没有 `Pause` / `Pending`，所以 `Paused` 天然为 false。

`checkpoint.CheckpointMetadata` 增加 `InFlight bool`，见 [checkpoint/query.go:54](../../checkpoint/query.go#L54)：

```go
func checkpointMetadataOf(cp *Checkpoint) CheckpointMetadata {
	// ...
	Paused:   cp.Pending != nil || cp.Pause != nil,
	InFlight: cp.InFlight != nil,
}
```

侧车投影的 `metaFields` 增加原始字段，见 [checkpoint/metadata_index.go:60](../../checkpoint/metadata_index.go#L60)：

```go
InFlight *json.RawMessage `json:"in_flight"`
```

`projectJournalLine` 用 `summaryInFlightMarker` 把“存在”转换成非 nil 的 `InFlightBatch`，再交给 `checkpointMetadataOf`，见 [checkpoint/metadata_index.go:450](../../checkpoint/metadata_index.go#L450)。

#### 6.1.5 RequiresResume

新增一个语义帮助函数：

```go
func RequiresResume(cp *Checkpoint) bool {
	return cp != nil && (cp.Pending != nil || cp.Pause != nil || cp.InFlight != nil)
}
```

用于：

- `Memory.BeginRun` 和 `File.BeginRun` 的拒绝条件；
- `Agent.prepare` 对非 `ThreadCoordinator` 存储的防御性检查；
- 文档和查询层判断“这个线程不能直接开新运行”。

注意：`RequiresResume` 不表示“已暂停”。暂停仍然是 `Pending` / `Pause`；在途只是“必须先恢复，不能直接开新运行”。

### 6.2 写入路径

#### 6.2.1 写入位置

在 `CheckToolPolicy` 和全部 `BeforeTool` 之后、`execTools` 之前，见 [agent/loop.go:483](../../agent/loop.go#L483)、[agent/loop.go:486](../../agent/loop.go#L486) 和 [agent/loop.go:507](../../agent/loop.go#L507)：

```go
if err := l.mw.CheckToolPolicy(lc, calls); err != nil {
	return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
}
for i := range calls {
	d, err := l.mw.BeforeTool(lc, &calls[i])
	if err != nil {
		return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
	}
	switch d.Kind {
	case core.Interrupt:
		return l.interrupt(rc, step, history, lc.viewSnapshot(), calls, "before_tool", d.Reason, checkpoint.RecoveryResumeTools), st, false
	case core.Stop, core.Escalate, core.Transfer:
		// 现有路径：没有工具运行，写普通检查点并结束。
	}
}

if l.inFlightCheckpoint && rc.topLevel {
	if _, ok := rc.durableStore(); ok {
		if err := rc.checkWriteAccess(); err != nil {
			return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
		}
		batchCalls := cloneToolCalls(calls)
		rc.State.Messages = history
		if err := l.checkpoint(rc, commitOptions{
			Step:       step,
			Snapshot:   lc.viewSnapshot(),
			InFlight:   &checkpoint.InFlightBatch{Step: step, Calls: batchCalls},
			Executions: pendingExecutions(rc.ThreadID, step, batchCalls),
		}); err != nil {
			return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
		}
		rc.publish(core.ToolBatchInFlight{Step: step, Calls: cloneToolCalls(batchCalls)})
	}
}

results, dirs, rejects, execErr := l.execTools(rc, lc, calls)
// ...
```

顺序不能变：

- **必须在全部执行前检查之后。** `CheckToolPolicy` 可能拒绝整批；`BeforeTool` 可能中断、停止或改写参数。检查点要记录的是“最终决定执行的那批调用”，不是模型原始请求。
- **必须在任何 handler 启动之前。** `execTools` 内部会启动 worker；一旦启动，崩溃窗口就打开了。检查点提交成功之前不能进入 `execTools`。
- **必须在 `checkWriteAccess` 之后。** 运行已经取消时不应该再写一个“计划执行”的边界；`commitCheckpointLocked` 在上下文取消时会改用 `Background`，如果不先检查，取消后仍可能留下在途头。
- **只对顶层且持久化的运行写。** 子代理和分支共享父线程，写入会污染父线程的恢复边界；普通 `Checkpointer` 没有工具台账，无法判断 `claimed` 和 `pending`。

#### 6.2.2 写入内容

一次在途提交包含：

| 字段 | 内容 | 原因 |
|---|---|---|
| `Step` | 当前步骤号 | 与调用编号的步骤部分一致 |
| `State.Messages` | 追加助手工具调用消息后的历史 | 恢复时助手消息必须已经在会话里 |
| `Snapshot` | 当前冻结的步骤视图 | 恢复时重建工具表、判断摘要是否变化 |
| `InFlight` | `{Step, Calls}`，`Calls` 是 `BeforeTool` 改写后的调用 | 恢复按原批次执行，调用编号保持一致 |
| `Executions` | 每个调用的 `pending` 记录 | 台账先于副作用落到存储；恢复可以据此认领 |
| `Pending` / `Pause` | nil | 这不是暂停，没有审批 |
| `Release` | false | 运行还在继续，释放占用会允许别的运行进入 |
| 确认 ID | 本步已登记确认的持久输入 | 这些输入已经影响了助手消息，应该与它一起提交 |

助手消息里的参数是模型原始参数；`InFlight.Calls` 里的参数是 `BeforeTool` 改写后的参数。两者**不能逐字节比较**：

- 恢复匹配助手消息和批次时，只比较调用 ID、工具名和顺序；
- 计算调用编号、调用 handler 时使用 `InFlight.Calls` 的参数；
- 这样既保留了模型看到的原始请求，也保留了实际执行使用的参数。

#### 6.2.3 提交失败

如果提交失败：

- 不进入 `execTools`，任何 handler 都不启动；
- 走 `fail`，写入失败检查点（如果存储还能写）；
- 已登记的确认 ID 不会随在途提交生效，下次恢复会重新投递持久输入；
- 调用方看到运行失败，不会看到“工具已执行”的事件。

提交结果可能是“存储返回错误但实际已经提交”。这种情况下：

- 不会执行工具（循环认为失败）；
- 后续失败检查点如果没有写成功，头部可能保留在途记录；
- 下一次恢复看到的是 `pending` 调用，会执行它们。这是安全的：原运行从未执行过这些调用，执行它们符合模型当时的计划。
- 如果失败检查点写成功，它会清除在途标记；台账里可能留下 `pending` 记录，但它们不会被自动执行，因为新的运行从下一步开始，调用编号不同。这些记录会变成孤儿，但不产生副作用。

#### 6.2.4 正常路径的清除

Phase 4 的普通检查点不设置 `InFlight`，见 [agent/loop.go:516](../../agent/loop.go#L516)。新检查点成为头部后，旧的 `InFlight` 自然不再可见。

不需要显式的“清除”操作，也不需要在存储层修改旧记录。检查点是只追加的历史，头部决定当前状态。

#### 6.2.5 与人工审批恢复批次的关系

人工审批恢复批次不写在途检查点：

- 它的执行前边界已经是 `Pause` + `Pending` 检查点，见 [agent/hitl.go:204](../../agent/hitl.go#L204)；
- `runResumed` 会重新经过 `BeforeTool`，见 [agent/hitl.go:301](../../agent/hitl.go#L301)，这与在途恢复不同；
- 如果再写一个 `InFlight`，同一批次会同时有 `Pending` 和 `InFlight`，违反互斥；
- 恢复入口也不同：审批恢复走 `rb` 批次，在途恢复走 `inFlight` 批次。

#### 6.2.6 配置

新增：

```go
// WithInFlightCheckpoint 控制工具批次执行前是否写检查点。
// 默认开启；关闭后行为回到 Phase 4 才写检查点。
func WithInFlightCheckpoint(enabled bool) Option
```

`config` 增加 `inFlightCheckpoint bool`，见 [agent/options.go:16](../../agent/options.go#L16)。`Agent.New` 的默认配置里设为 `true`，见 [agent/agent.go:37](../../agent/agent.go#L37)。

关闭后的行为：

- 不写额外的检查点，工具批次仍然走现有的 `ClaimTool` 和 Phase 4；
- 已存在的在途头仍然可以恢复（恢复逻辑不依赖这个开关）；
- 崩溃窗口回到现状：可能留下 `claimed` 记录，但头部没有批次信息。

### 6.3 恢复路径

#### 6.3.1 入口判断

`Agent.Resume` 在读取头部之后先判断 `cp.InFlight`，见 [agent/hitl.go:79](../../agent/hitl.go#L79)：

```go
cp, err := a.store.Latest(ctx, threadID)
// ...
if cp.InFlight != nil {
	if cp.Pending != nil || cp.Pause != nil || cp.Completion != nil {
		return nil, fmt.Errorf("%w: checkpoint %s carries conflicting work state", ErrInFlightUnrecoverable, cp.ID)
	}
	if len(approvals) > 0 {
		return nil, fmt.Errorf("%w: an in-flight batch takes no approvals", ErrInFlightUnrecoverable)
	}
	batch, err := a.preflightInFlight(ctx, cp)
	if err != nil {
		return nil, err
	}
	// 走现有的 Claim 获取流程，然后设置 rc.inFlight。
	// 不设置 replayBaseline。
	run.rc.continuedTurn = true
	run.rc.inFlight = batch
	return run, nil
}
```

顺序：

1. 在取得线程占用之前做只读校验，避免“先占用再发现不可恢复”。
2. 校验通过后再走现有 `durable.Claim`，使用 `checkpoint.EffectivePauseID(cp)`；在途没有 `Pause.ID`，因此是 `checkpoint-<cp.ID>`，见 [checkpoint/checkpoint.go:144](../../checkpoint/checkpoint.go#L144)。
3. 设置 `continuedTurn=true`。
4. **不设置** `replayBaseline`。在途恢复的下一步是执行批次，不是模型重放；设置基线会让恢复后的第一次模型请求与在途步骤的视图比较，产生错误的漂移报告。

#### 6.3.2 恢复前校验

`preflightInFlight` 必须全部通过：

| 检查 | 失败结果 |
|---|---|
| `a.loop != nil`，当前 Agent 有工具表 | `ErrInFlightUnrecoverable` |
| `cp.Snapshot != nil` 且 `cp.Snapshot.Step == cp.Step` | `ErrInFlightUnrecoverable` |
| `cp.InFlight.Step == cp.Step` | `ErrInFlightUnrecoverable` |
| `len(cp.InFlight.Calls) > 0` | `ErrInFlightUnrecoverable` |
| 存储实现 `checkpoint.Durable` | `ErrDurableUnavailable` / `ErrInFlightUnrecoverable` |
| 每个调用名都在 `cp.Snapshot.Advertised` 中 | `ErrToolNotAdvertised` |
| 每个调用名都能在当前静态工具表中解析 | `ErrToolUnavailable` |
| 状态历史末尾是助手工具调用消息 | `ErrInFlightUnrecoverable` |
| 助手消息的调用 ID、工具名、顺序与 `InFlight.Calls` 一致 | `ErrInFlightUnrecoverable` |
| 至少一个调用可能执行时，当前工具摘要等于保存的摘要 | `ErrStepViewDrift` / `ErrInFlightUnrecoverable` |

匹配助手消息时：

```go
func matchInFlightMessage(messages []core.Message, calls []core.ToolCall) (core.Message, error) {
	if len(messages) == 0 {
		return core.Message{}, fmt.Errorf("%w: no assistant message to recover", ErrInFlightUnrecoverable)
	}
	last := messages[len(messages)-1]
	if last.Role != core.RoleAssistant {
		return core.Message{}, fmt.Errorf("%w: last message is %q, not assistant", ErrInFlightUnrecoverable, last.Role)
	}
	declared := last.ToolCalls()
	if len(declared) != len(calls) {
		return core.Message{}, fmt.Errorf("%w: assistant declared %d calls, batch has %d", ErrInFlightUnrecoverable, len(declared), len(calls))
	}
	for i := range calls {
		if declared[i].ID != calls[i].ID || declared[i].Name != calls[i].Name {
			return core.Message{}, fmt.Errorf("%w: call %d does not match the recorded batch", ErrInFlightUnrecoverable, i)
		}
		// 不比较 Args：BeforeTool 可能已经改写。
	}
	return last, nil
}
```

为什么只比较 ID、工具名和顺序：

- `BeforeTool` 可能改写参数，见 [agent/loop.go:487](../../agent/loop.go#L487)；
- 模型原始参数在助手消息里，实际执行参数在 `InFlight.Calls` 里；
- 调用编号由实际执行参数计算，恢复必须用 `InFlight.Calls`；
- 只要 ID、工具名和顺序一致，结果就可以按顺序组装回助手消息。

动态工具：

- 快照里可能有运行时注入的工具名，但静态工具表在恢复时不一定有；
- 当前 `resumeStepView` 只从静态表解析，见 [agent/stepview.go:340](../../agent/stepview.go#L340)；
- 因此动态工具的在途批次无法恢复，必须返回 `ErrToolUnavailable`，不能静默跳过或重新调用模型。

工作流 Agent：

- 工作流 Agent 没有自己的工具表，见 [agent/hitl.go:193](../../agent/hitl.go#L193)；
- 它无法执行在途批次，也无法判断哪些调用已经产生副作用；
- 直接拒绝，不写“未执行”的结果（写“未执行”可能是错的）。

#### 6.3.3 摘要检查的策略

严格比较“当前工具摘要 == 保存的工具摘要”可能过度阻止安全恢复：如果所有调用都已经 `completed` / `claimed` / `unknown`，恢复不会启动任何 handler，工具实现变化不影响安全。

因此先用 `Durable.ToolExecution` 做只读预扫描，见 [checkpoint/durable.go:29](../../checkpoint/durable.go#L29)：

```go
func (a *Agent) inFlightMayExecute(
	ctx context.Context,
	durable checkpoint.Durable,
	cp *checkpoint.Checkpoint,
) (bool, error) {
	for _, call := range cp.InFlight.Calls {
		invocationID, _ := toolInvocation(cp.ThreadID, cp.Step, call)
		ex, found, err := durable.ToolExecution(ctx, cp.ThreadID, invocationID)
		if err != nil {
			return false, fmt.Errorf("%w: read tool execution %s: %v", ErrInFlightUnrecoverable, invocationID, err)
		}
		if !found || ex.State == checkpoint.ToolPending {
			return true, nil
		}
	}
	return false, nil
}
```

规则：

- 任何调用是 `pending` 或台账里找不到：可能执行，必须严格校验。
- 全部是 `completed` / `claimed` / `unknown`：不会启动 handler，跳过摘要比较。
- 读取台账失败：无法判断安全性，直接拒绝恢复。
- 可能执行且 `Snapshot.ToolDigest` 为空：拒绝。内建写入一定带摘要；空摘要说明记录不是正常路径写的，不能冒风险。
- 可能执行且当前摘要不同：拒绝。当前摘要要用当前静态表重新计算，不能直接用 `resumeStepView` 复制过来的旧摘要。

计算当前摘要：

```go
view := l.resumeStepViewFor(cp.Step, cp.InFlight.Calls, cp.Snapshot)
current := toolDigest(view.Tools().Schemas())
```

`resumeStepView` 会把保存的摘要复制进视图，见 [agent/stepview.go:355](../../agent/stepview.go#L355)；所以必须另外调用 `toolDigest`，不能读 `view.Digest().ToolDigest`。

#### 6.3.4 恢复执行

在 `prepareRun` 中优先处理在途批次，见 [agent/loop.go:115](../../agent/loop.go#L115)：

```go
if ib := rc.inFlight; ib != nil {
	rc.inFlight = nil
	rlc := &LoopContext{RunContext: rc, Step: ib.step, MaxTurns: l.maxTurns, History: history}
	rlc.view = l.resumeStepViewFor(ib.step, ib.calls, ib.snapshot)
	rlc.batchStats = &batchStats{}

	// 注意：不调用 CheckToolPolicy，不调用 BeforeTool。
	results, dirs, rejects, execErr := l.execTools(rc, rlc, ib.calls)
	l.mw.ToolReject(rlc, rejects)
	if execErr != nil {
		results = l.completeFailedBatch(rc, ib, results)
	}
	if len(results) > 0 {
		history = append(history, core.Message{Role: core.RoleTool, Parts: results})
	}
	if execErr != nil {
		return taskState{}, l.fail(rc, ib.step, history, ib.snapshot, execErr), false
	}

	rc.State.Messages = history
	next := core.Resolve(dirs...)
	if err := l.checkpoint(rc, commitOptions{
		Step: ib.step, Snapshot: ib.snapshot, Release: next.Kind != core.Continue,
	}); err != nil {
		return taskState{}, runOutcome{Err: err}, false
	}
	rc.publish(core.ToolBatchRecovered{
		Step: ib.step,
		Replayed: stats.Replayed, Executed: stats.Executed,
		Unknown: stats.Unknown, Rejected: stats.Rejected,
	})
	if next.Kind != core.Continue {
		return taskState{}, runOutcome{Result: core.Result{Message: ib.final}, Control: next}, false
	}
	startStep = ib.step + 1
}
```

细节：

- **不重跑执行前检查。** 批次在写检查点之前已经通过了 `CheckToolPolicy` 和全部 `BeforeTool`；重新运行可能产生第二次审批、第二次参数改写或第二次策略副作用。恢复只执行已经决定的批次。
- **不重新冻结步骤视图。** 用 `resumeStepViewFor` 从保存的快照和当前静态表重建；不调用 `freezeStepView`，因为那会重新读取当步配置并产生新的摘要。
- **AfterTool 仍然运行。** 它是执行后的观察/改写钩子，不是执行前检查。已完成回放（`skipAfter`）不会运行 AfterTool，与现有幂等回放一致。
- **结果按调用顺序组装。** `execTools` 返回的结果已经按顺序压缩；恢复必须保证每个调用一条，见 §6.4。
- **同一个逻辑步骤。** 恢复调用 `execTools` 时 `rlc.Step = ib.step`，所以 `toolInvocation` 与崩溃前一致，`ClaimTool` 能命中旧记录。
- **下一步从 `ib.step + 1` 开始。** 不在 `ib.step` 再问模型；批次的结果属于这一步。
- **检查点清除 InFlight。** 恢复提交不设置 `InFlight`，新头部回到普通工作状态。
- **如果批次产生结束指令，先提交结果再返回。** 与普通 Phase 4 一样，结果必须先持久化，再结束运行。
- **不发布 `Interrupted`。** 在途不是暂停；恢复执行不会触发暂停事件。`ToolBatchRecovered` 是唯一的恢复事件。
- **不发布 `TurnStarted` / `TurnDone`。** 原步骤的 `TurnStarted` 已经在崩溃前发布过；恢复是同一逻辑步骤的继续。下一步的模型调用会发布自己的 `TurnStarted`。

#### 6.3.5 ClaimTool 的四种结果

恢复时每个调用都会经过 `callOne` → `ClaimTool`，见 [agent/exectools.go:351](../../agent/exectools.go#L351)：

| 台账状态 | `ClaimTool` 行为 | 恢复动作 | 结果 |
|---|---|---|---|
| `completed` | 返回完成记录 | 回放已存结果，跳过 AfterTool | 复用原结果 |
| `claimed` | 转为 `unknown`，返回 `ErrToolOutcomeUnknown` | 不执行 handler | 结果未知，提示查询外部系统 |
| `unknown` | 返回 `ErrToolOutcomeUnknown` | 不执行 handler | 结果未知 |
| `pending` | 转为 `claimed`，`Attempt++` | 执行 handler | 新结果 |
| 不存在 | 新建 `claimed` | 执行 handler | 新结果 |

`pending` 的执行是安全的：它表示 `ClaimTool` 从未成功过，handler 没有被调用。`claimed` 不能自动重试：`ClaimTool` 成功但完成记录没有提交，handler 可能已经产生副作用。

这个判断与现有 `unknownToolResult` 的语义一致，见 [agent/exectools.go:479](../../agent/exectools.go#L479)。

#### 6.3.6 事件

`ToolBatchRecovered` 在恢复检查点成功之后发布：

```go
type ToolBatchRecovered struct {
	Step     int
	Replayed int
	Executed int
	Unknown  int
	Rejected int
}
```

计数由 `batchStats` 在 `callOne` 中累加：

- `Replayed`：命中 `completed` 并回放；
- `Executed`：进入 handler（无论 handler 返回成功还是错误）；
- `Unknown`：`claimed` / `unknown`，没有执行；
- `Rejected`：在 handler 之前被参数、工具表或写入检查拒绝。

计数是诊断信息，不是严格分区：某些写入检查失败可能不落入四个计数中的任何一个。事件文档要说明这一点，避免调用方把它当作守恒等式。

`ToolBatchInFlight` 在原始写入成功后发布：

```go
type ToolBatchInFlight struct {
	Step  int
	Calls []ToolCall
}
```

它让观察者知道“批次已经计划并持久化，handler 即将开始”。`Calls` 是改写后的参数，和助手 `MessageDone` 里的原始参数可能不同。

### 6.4 批次中途失败

#### 6.4.1 现有失败路径的问题

`execTools` 在部分调用完成、部分调用报错时返回：

- 已经报告的结果（按顺序压缩）；
- 结束指令；
- 拒绝列表；
- 合并后的错误。

循环当前只追加已报告的结果，然后调用 `fail`，见 [agent/loop.go:509](../../agent/loop.go#L509) 和 [agent/loop.go:512](../../agent/loop.go#L512)。`fail` 调用 `seam`，如果历史末尾是助手工具调用消息，就把它删掉，见 [agent/loop.go:641](../../agent/loop.go#L641)。

在写了在途检查点之后，这会产生两个问题：

1. 头部检查点里已经有助手工具调用消息，但失败检查点用 `seam` 把它删掉，两个检查点的会话不一致。
2. 已经执行的调用结果可能丢失；模型下次重新发起调用，调用编号变化，旧结果无法回放。

#### 6.4.2 补齐结果

当 `execErr != nil` 且本步写过在途检查点时，在 `fail` 之前补齐每个计划调用的结果：

```go
func (l *AgentLoop) completeFailedBatch(
	rc *RunContext,
	ib *inFlightBatch,
	results []core.Part,
) []core.Part {
	byCall := make(map[string]core.ToolResult, len(results))
	for _, p := range results {
		if tr, ok := p.(core.ToolResult); ok {
			byCall[tr.CallID] = tr
		}
	}
	ctx := context.WithoutCancel(rc.Context)
	durable, hasDurable := rc.durableStore()
	out := make([]core.Part, 0, len(ib.calls))
	for _, call := range ib.calls {
		if tr, ok := byCall[call.ID]; ok {
			out = append(out, tr)
			continue
		}
		if !hasDurable {
			out = append(out, errResult(call, "not executed: the run stopped before this call started"))
			continue
		}
		invocationID, _ := toolInvocation(rc.ThreadID, ib.step, call)
		ex, found, err := durable.ToolExecution(ctx, rc.ThreadID, invocationID)
		switch {
		case err != nil:
			out = append(out, errResult(call, "not executed: the tool ledger could not be read: "+err.Error()))
		case found && ex.State == checkpoint.ToolCompleted:
			if tr, ok := resultFromExecution(ex, call); ok {
				out = append(out, tr)
			} else {
				out = append(out, errResult(call, "stored tool result is invalid and cannot be replayed"))
			}
		case found && (ex.State == checkpoint.ToolClaimed || ex.State == checkpoint.ToolUnknown):
			out = append(out, unknownToolResult(call, ex.IdempotencyKey))
		default:
			out = append(out, errResult(call, "not executed: the run stopped before this call started"))
		}
	}
	return out
}
```

规则：

- 已经报告的结果保持原样，不重复、不覆盖。
- `completed` 回放原结果。
- `claimed` / `unknown` 返回“结果未知”，绝不执行。
- `pending` / 不存在返回“未执行”。
- 台账读取失败返回“未执行 + 读取错误”，不把失败当成成功。
- 使用 `context.WithoutCancel`，因为此时运行上下文可能已经取消，但补齐会话记录仍然必须完成。检查点提交本身已经有取消后的 `Background` 兜底，见 [agent/durable_runtime.go:392](../../agent/durable_runtime.go#L392)。

补齐后的工具结果消息包含每个调用恰好一条结果。`fail` 的 `seam` 看到末尾是工具结果消息，不会删除助手消息，见 [agent/loop.go:653](../../agent/loop.go#L653)。

#### 6.4.3 失败检查点

补齐之后调用 `fail`：

- `State.Messages` 包含助手工具调用消息 + 完整的工具结果消息；
- 检查点不设置 `InFlight`，清除在途边界；
- `Release:true`，释放线程占用；
- `durableRunState` 中已经完成的执行会随失败检查点一起提交，见 [agent/durable_runtime.go:366](../../agent/durable_runtime.go#L366)；
- `claimed` / `unknown` 的记录保持原状，不写“未执行”结果到台账，避免锁定未来重试。

如果失败检查点写失败：

- 头部可能仍然是在途检查点；
- 下一次恢复会重新执行 `pending`，`claimed` 仍然返回未知；
- 不会重复执行 `claimed`，因为 `ClaimTool` 会把它转成 `unknown`；
- 已经完成但未提交的执行在内存里丢失，恢复时表现为 `claimed` / `unknown`，这是保守但安全的结果。

#### 6.4.4 关闭开关时的行为

`WithInFlightCheckpoint(false)` 时没有在途头，`completeFailedBatch` 不启用。行为回到现状：部分结果加 `seam`。这是兼容路径，不作为推荐配置。

### 6.5 存储、投影与协调器

| 位置 | 改动 | 原因 |
|---|---|---|
| `checkpoint/snapshot.go` `freezeCheckpoint` | 调用 `validateWorkState` | 所有写入和分叉都经过这里；互斥关系是存储不变量 |
| `checkpoint/snapshot.go` `cloneCheckpoint` | 深拷贝 `InFlight` 和参数 | 防止外部修改可见记录 |
| `checkpoint/coordinator.go` `commitDigestCheckpoint` | 增加 `InFlight *commitDigestInFlight json:"in_flight,omitempty"` | 提交摘要覆盖新字段，旧提交字节不变 |
| `checkpoint/coordinator.go` `commitRequestDigest` | 构造新的摘要子结构 | 保证摘要可重试、可比较 |
| `checkpoint/branch.go` `Fork` | 拒绝 `parent.InFlight != nil` | 台账不会随分叉复制；分叉会丢副作用证据 |
| `checkpoint/coordinator_memory.go` `BeginRun` | 拒绝条件改为 `RequiresResume(head)` | 在途线程不能直接开新运行 |
| `checkpoint/coordinator_file.go` `BeginRun` | 同上 | 两个内置存储语义一致 |
| `checkpoint/coordinator.go` `ErrResumeRequired` 文本 | 从“paused thread”改为“thread” | 在途不是暂停，但同样需要恢复 |
| `agent/run_prepare.go` `prepare` | 非协调存储恢复后检查 `latest.InFlight` | 普通 `Checkpointer` 也不能绕过在途边界 |
| `agent/terminal.go` `buildTerminalRecord` | 拒绝 `InFlight != nil` | 终态记录不能覆盖未解决的批次 |
| `checkpoint/query.go` `CheckpointMetadata` | 增加 `InFlight` | 查询层区分暂停和在途 |
| `checkpoint/query.go` `checkpointMetadataOf` | 投影 `InFlight` | 头部元数据可直接判断 |
| `checkpoint/metadata_index.go` `metaFields` | 增加原始 `in_flight` 字段 | 侧车重建不读完整会话 |
| `checkpoint/metadata_index.go` `projectJournalLine` | 增加在途标记 | 侧车与主存储投影一致 |
| `checkpoint/file.go` `validateJournalRecord` | 校验收件箱阶段和提交检查点 | 文件日志损坏时尽早失败 |
| `agent/run_status.go` | 增加 `InFlight` | 界面不把在途当暂停 |
| `queue/run_worker.go` | 不把在途映射为 `StatusPaused` | 队列状态仍由 `RunResult.Paused` 决定，在途不设置它 |

#### 6.5.1 提交摘要

新增摘要类型：

```go
type commitDigestInFlight struct {
	Step  int                    `json:"step"`
	Calls []commitDigestToolCall `json:"calls"`
}
```

`commitDigestToolCall` 已经存在，见 [checkpoint/coordinator.go:99](../../checkpoint/coordinator.go#L99)。不要在摘要里排序调用：顺序是恢复匹配的一部分。

`omitempty` 保证：

- 没有在途字段的旧提交，摘要字节与之前完全一致；
- 重试旧提交仍然幂等；
- 新旧存储可以混合读取。

#### 6.5.2 Fork

`Fork` 在复制父检查点之前拒绝：

```go
if parent.InFlight != nil {
	return nil, fmt.Errorf("%w: cannot fork an in-flight checkpoint", ErrInFlightConflict)
}
```

原因：

- 工具台账不随检查点复制；
- 分叉线程的调用编号包含新线程 ID，旧台账记录不会命中；
- 父线程的在途调用可能已经产生副作用，分叉会制造一个“看起来相同、实际没有执行记录”的新线程；
- 安全做法是先恢复父线程、清除在途，再分叉。

#### 6.5.3 BeginRun 与 prepare

两个 `BeginRun` 使用 `RequiresResume`：

```go
if head != nil && RequiresResume(head) {
	return BeginRunResult{}, ErrResumeRequired
}
```

非协调存储的 `prepare` 在 `restore` 之后检查：

```go
if latest != nil && latest.InFlight != nil {
	return fmt.Errorf("agent: thread %q has an in-flight tool batch; resume it first: %w", threadID, checkpoint.ErrResumeRequired)
}
```

这样即使存储没有 `ThreadCoordinator`，新的运行也不会越过在途边界直接开始。

#### 6.5.4 终态记录

`buildTerminalRecord` 现在拒绝 `Pause` / `Pending`，见 [agent/terminal.go:176](../../agent/terminal.go#L176)。增加 `InFlight`：

- 终态记录只能写在已经解决的边界上；
- 在途批次没有“完成”或“失败”的结论，不能写终态；
- 调用方应先恢复并让批次产生结果，再记录终态。

#### 6.5.5 队列状态

`queue/run_worker.go` 的 `recordCheckpoint` 只跳过 `detail.Paused`，见 [queue/run_worker.go:286](../../queue/run_worker.go#L286)；`terminalTaskRecord` 把 `Paused` 映射为 `StatusPaused`，见 [queue/run_worker.go:360](../../queue/run_worker.go#L360)。

在途不设置 `RunResult.Paused`，因此：

- 不会写成 `StatusPaused`；
- 不会触发“等待人工审批”的界面；
- 如果 worker 崩溃，运行记录停在运行中，是否自动恢复由上层决定。本文不实现自动恢复。

### 6.6 事件

新增：

```go
// ToolBatchInFlight 表示工具批次已经通过执行前检查并写入在途检查点。
type ToolBatchInFlight struct {
	Step  int
	Calls []ToolCall
}

// ToolBatchRecovered 表示在途批次已经恢复执行并提交结果。
// 四个计数是诊断值，不保证相加等于 Calls 数。
type ToolBatchRecovered struct {
	Step     int
	Replayed int
	Executed int
	Unknown  int
	Rejected int
}
```

线格式：

- `ToolBatchInFlight` 复用 `Message` 字段，把调用列表包成助手消息，与 `ToolStarted` 的传输方式一致，见 [core/eventjson.go:84](../../core/eventjson.go#L84)。解码时用 `w.Message.ToolCalls()`，见 [core/message.go:112](../../core/message.go#L112)。
- `ToolBatchRecovered` 增加四个整数线字段：`replayed`、`executed`、`unknown`、`rejected`。

要求：

- 两个事件都有往返测试；
- `ToolBatchInFlight` 在提交成功后发布，不在提交前发布；
- `ToolBatchRecovered` 在恢复检查点成功后发布；
- 事件不替代检查点，崩溃恢复只看存储，不看事件。

### 6.7 暂停与在途的对比

| 维度 | 人工审批暂停 | 在途批次 |
|---|---|---|
| 存储字段 | `PendingHITL` + `Pause` | `InFlight` |
| 是否等待外部决定 | 是 | 否 |
| `RunStatus.Paused` | true | false |
| `RunStatus.InFlight` | false | true |
| 事件 | `Interrupted` | `ToolBatchInFlight` / `ToolBatchRecovered` |
| 恢复入口 | `Resume` + 审批 | `Resume` |
| 恢复时是否重跑 `BeforeTool` | 是（`runResumed`） | 否 |
| 恢复时是否使用台账 | 是 | 是 |
| 恢复的步骤 | `rb.step` | `ib.step` |
| 下一步 | `rb.step + 1` | `ib.step + 1` |
| 可否分叉 | `Fork` 清空 Pending 后允许 | 拒绝 |
| 新运行能否越过 | 否（`BeginRun` 拒绝） | 否（`BeginRun` / `prepare` 拒绝） |
| 终态记录 | 拒绝 | 拒绝 |
| 是否允许审批参数 | 是 | 否（拒绝显式 approvals） |

### 6.8 错误

新增：

```go
// checkpoint
var ErrInFlightConflict = errors.New("checkpoint: in-flight batch conflicts with pause or terminal state")

// agent
var ErrInFlightUnrecoverable = errors.New("agent: in-flight tool batch cannot be recovered safely")
```

复用：

- `checkpoint.ErrResumeRequired`：线程需要恢复，不能直接开新运行。
- `checkpoint.ErrDurableUnavailable`：存储没有台账能力。
- `ErrToolNotAdvertised` / `ErrToolUnavailable`：工具不在快照或当前静态表。
- `ErrStepViewDrift`：工具摘要变化。
- `ErrRunClosed` / `ErrMaxTurnsExceeded`：输入拒绝。

错误包装原则：

- 恢复失败统一可匹配 `ErrInFlightUnrecoverable`；
- 具体原因用 `%w` 保留，如 `ErrToolUnavailable`、`ErrStepViewDrift`；
- 不在错误文本里泄露工具参数内容，只带调用 ID 和工具名。

## 7. 不变量

以下不变量应当被测试直接断言，而不是只写在文档里。

**输入队列**

1. 每条进入模型请求的消息只来自临时消息或持久收件箱；工具结果和系统提示不经过输入队列。
2. 同一投递点先投递临时消息，再投递持久消息；同一来源内保持入队顺序。
3. `next_turn` 消息不会在 `newTurn=false` 的 `PrepareTurn` 投递。
4. `settleTurn` 投递全部临时消息；预算耗尽时全部拒绝，不投递。
5. 临时 ticket 恰好结算一次；未结算时 `Outcome()` 为零值，`State()` 为 `queued`。
6. 持久消息只有在下一次检查点提交成功后才从 `Inbox` 消失；投递语义是至少一次。
7. 同一 ID、同样的消息、不同阶段是冲突，不是重复。

**抢占**

8. 每个步骤最多只有一个模型尝试被抢占取消。
9. 抢占只投递临时 `before_model` 消息，不投递持久消息、`after_model`、`after_tools`、`next_turn`。
10. 收到最终响应时，即使抢占已经生效，也不重试（成功优先）。
11. 运行上下文已取消时，不进入抢占重试（运行取消优先）。
12. 抢占重试不重跑 `BeforeModel`、`ModifyRequest`、`freezeStepView` 的中间件。
13. 抢占重试写入 `Release:false` 的中间检查点，且不改变步骤号。
14. 被抢占的尝试不发布 `MessageDone`，只发布 `ModelPreempted` 和增量。

**在途检查点**

15. 在途检查点只在全部执行前检查通过之后、任何 handler 启动之前写入。
16. 在途检查点提交成功之前，不启动任何 handler。
17. `InFlight.Calls` 是 `BeforeTool` 改写后的调用；助手消息保留模型原始参数；恢复只按 ID、名称、顺序匹配。
18. 在途检查点不与 `Pending`、`Pause`、`Completion` 共存。
19. `RunStatus.Paused` 在在途状态下保持 false；`RunStatus.InFlight` 为 true。
20. 恢复在同一逻辑步骤执行，不重跑 `CheckToolPolicy` / `BeforeTool`。
21. `completed` 只回放，`claimed` / `unknown` 绝不自动执行，`pending` / 缺失才执行。
22. 恢复后每个计划调用恰好一条结果；失败检查点不留下悬空的助手工具调用消息。
23. 在途只对持久化、顶层的运行写入。
24. `Fork`、`BeginRun`、`RecordCompletion` 都拒绝在途头。

**存储与投影**

25. 没有 `InFlight` 的旧提交，摘要字节不变。
26. 侧车重建后的元数据与主存储投影一致：`Paused` 与 `InFlight` 不会同时为 true。
27. 旧收件箱记录（无 `phase`）按 `before_model` 读取；新写入显式保存阶段。

## 8. 兼容性与迁移

### 8.1 默认值与开关

| 能力 | 默认 | 开关 | 关闭后的行为 |
|---|---|---|---|
| 输入阶段 | 所有旧消息等价于 `before_model` | 无 | 不适用 |
| 抢占 | 关闭 | `WithInputPreemption(true)` | 消息只排队，不取消模型调用 |
| 在途检查点 | 开启（仅持久、顶层） | `WithInFlightCheckpoint(false)` | 回到 Phase 4 才写检查点 |
| 持久收件箱阶段 | 空值按 `before_model` | `DeliverWith` | 旧 `Deliver` 等价于 `before_model` |

### 8.2 数据兼容

- `Checkpoint.InFlight` 是新增的可省略字段；旧记录读回 nil，行为与之前一致。
- `InboxMessage.Phase` 是新增的可省略字段；旧记录读回空值，按 `before_model` 处理。
- `CheckpointMetadata.InFlight` 是新增的可省略字段；旧侧车没有该字段时读回 false。
- `eventWire` 只增加字段，不删除、不改名；旧事件解码不受影响。
- 提交摘要新增字段使用 `omitempty`；旧提交重试的摘要不变。
- `checkpoint.Fork` 对旧记录没有影响；只有父检查点带 `InFlight` 时才拒绝。

### 8.3 API 兼容

保持：

- `Run.Steer(msg)`：签名、返回值、投递时机不变。
- `Agent.Deliver(ctx, threadID, msg)`：签名不变，等价于 `before_model`。
- `RunContext.Steer(msg)`：不变。
- `SteerTicket.Outcome()`：未结算时仍返回零值；已结算字段不变（新增 `State` 是附加字段）。
- `RunStatus` 的现有字段语义不变；`InFlight` 是新增字段。

新增：

- `Run.SteerWith` / `RunContext.SteerWith`
- `Agent.DeliverWith`
- `core.InputPhase` / `core.InputSource` / `core.InputState`
- `checkpoint.InFlightBatch` / `Checkpoint.InFlight` / `checkpoint.RequiresResume`
- `WithInputPreemption` / `WithInFlightCheckpoint`
- 四个事件

### 8.4 行为变化

以下变化需要写进发布说明：

1. `streamModel` 在没有最终响应时返回 `ok=false`，由调用方报 `ErrModelNoFinal`；以前会返回空响应并当成成功。这是修正，不是兼容性破坏，但自定义调用方（如果有）会看到新错误。
2. `OnError` 不再在上下文已经取消时调用。抢占和运行取消都属于取消路径；依赖“取消也上报 OnError”的观测逻辑需要调整。
3. `Agent.Resume` 产生的运行，第一个 `PrepareTurn` 的 `newTurn` 为 false。以前没有阶段概念，这个字段不存在；启用 `next_turn` 后，消息会保留到当前轮结束。
4. `BeginRun` 在头部带 `InFlight` 时返回 `ErrResumeRequired`。以前这种头部不存在；新写入后，直接开新运行会被拒绝，必须先 `Resume`。
5. `checkpoint.Fork` 拒绝在途头。以前没有这种头；新行为避免分叉丢失副作用证据。
6. 每个工具批次多一次提交（默认开启）。工具密集的步骤提交次数大约翻倍；可通过 `WithInFlightCheckpoint(false)` 关闭。
7. `sameInboxMessage` 比较阶段。同一 ID、不同阶段的重复投递从“重复”变成“冲突”，这是更严格的正确性要求。

### 8.5 迁移步骤

不需要数据迁移。建议的启用顺序：

1. 先落存储模型（`InFlight`、校验、克隆、摘要、元数据、协调器），保持循环不写。
2. 落输入阶段和回执，先不打开抢占；观察 `next_turn` 的投递行为。
3. 落抢占，默认关闭；在需要的产品路径显式打开。
4. 落工具批次写入和恢复，默认开启；用崩溃注入测试验证。
5. 最后补查询、状态、队列文档和示例。

## 9. 备选方案

| 方案 | 为什么不做 |
|---|---|
| A. 四个阶段各做一个真实插入口 | 模型请求和工具结果之间没有合法插入点；强行插入会产生非法会话。阶段只作为最晚投递时间 |
| B. 抢占时取消整个运行，再 `Resume` | 会发布 `RunFailed` / `RunInterrupted`，丢失当前步的部分输出，还要重新建立占用；用户体验和成本都更差 |
| C. 调用提供方的中止 API | `llm.Model` 只暴露 `Generate(ctx, req)`，见 [llm/model.go:23](../../llm/model.go#L23)；没有跨提供方的中止接口，新增会扩大接口面 |
| D. 用 `PendingHITL` + `Pause` 表示在途 | 会被 `RunStatus.Paused` 和界面当成等待审批；`Resume` 会要求审批参数；语义错误 |
| E. 在执行前检查之前写在途 | 检查可能中断、停止或改写参数；记录的是模型原始批次，不是实际执行批次，恢复会执行错误的调用 |
| F. 每个工具调用写一次检查点 | 并发批次会被串行化，提交次数按调用数增长；一个批次边界足够覆盖恢复需求 |
| G. 只对非幂等工具开启 | 工具接口没有幂等性声明；按名字猜测不可靠。默认全开、整体可关 |
| H. 恢复时重跑 `CheckToolPolicy` / `BeforeTool` | 可能二次审批、二次改写参数、二次产生策略副作用；批次在崩溃前已经批准 |
| I. 恢复时全表扫描台账，按步骤找孤儿 | 台账没有按步骤扫描的接口；批次本身就是本次执行的全集。只要“检查点先于 handler”成立，扫描不增加信息 |
| J. 持久消息也参与抢占 | 持久消息已经进入收件箱，取消不能撤回接受；确认必须跟检查点绑定，抢占时没有可确认的边界 |
| K. `Fork` 时清空 `InFlight` | 会制造一个没有台账、没有副作用证据的新线程；安全做法是拒绝 |
| L. 恢复时总是严格比较工具摘要 | 全部调用都已完成/已认领时不会执行 handler，摘要变化不影响安全；先按台账判断是否可能执行，再决定是否严格比较 |
| M. 把在途阶段写进 `Pause.Phase` | `Pause` 是暂停的封闭集合，见 [checkpoint/checkpoint.go:157](../../checkpoint/checkpoint.go#L157)；混入在途会让恢复路径和状态判断都变复杂 |

## 10. 测试与验收矩阵

### 10.1 P1-5 输入队列

| 用例 | 场景 | 期望 |
|---|---|---|
| 阶段规范化 | 空、四个合法值、未知值 | 空→`before_model`；合法值原样；未知值 `ErrUnknownInputPhase` |
| `takeReady(newTurn=true)` | 四种阶段混合入队 | 全部取走，顺序不变 |
| `takeReady(newTurn=false)` | 四种阶段混合 | 取走前三种，保留 `next_turn`；剩余顺序不变 |
| `takePhase` | 混合阶段 | 只取指定阶段，其他保留 |
| 工具批次后投递 | 模型返回工具调用，再进入 `PrepareTurn` | `after_model` / `after_tools` 投递；`next_turn` 保留 |
| 终稿后投递 | 终稿 + 四种阶段 | `settleTurn` 全部投递，运行继续 |
| 持久阶段过滤 | Inbox 有 `next_turn`，当前 `newTurn=false` | 不追加、不确认；运行结束后下一轮再投递 |
| 持久阶段重复 | 同 ID、同消息、不同阶段 | 冲突错误 |
| 旧记录兼容 | 无 `phase` 的 Inbox | 按 `before_model` 投递 |
| 临时先于持久 | 两边同时有消息 | 历史顺序：临时消息在前 |
| ticket 状态 | 入队、投递、关闭、预算耗尽 | `queued` → `delivered` / `rejected`；`Outcome()` 未结算为零值 |
| 事件 | 投递临时/持久消息 | `InputDelivered` 的 source、phase、count、IDs 正确 |
| 关闭竞态 | 投递与关闭并发 | 消息要么投递并结算，要么拒绝；不悬空 |
| `-race` | 多生产者、多投递点 | 无数据竞争 |

### 10.2 P1-5 抢占

| 用例 | 场景 | 期望 |
|---|---|---|
| 基本抢占 | 阻塞模型，`SteerWith(Preempt)` | 取消当前调用；同一步重试；消息进入第二次请求 |
| 事件 | 基本抢占 | 先 `ModelPreempted`，后 `InputDelivered{Preempted:true}`；没有第二次 `TurnStarted` |
| 成功优先 | 模型忽略取消并返回终稿 | 使用终稿；不重试；消息在下一个投递点进入 |
| 运行取消优先 | 抢占同时 `Cancel` | 不重试；运行按取消结束；ticket 拒绝 |
| 每步一次 | 连续两次抢占 | 只取消一次；第二次消息排队 |
| 无活动调用 | 工具执行期间抢占 | 消息排队，不报错 |
| 关闭抢占 | `WithInputPreemption(false)` | `ErrPreemptionDisabled`，无 ticket |
| 阶段错误 | `Preempt` + `next_turn` | `ErrPreemptionPhase`，无 ticket |
| 空结果 | 模型流结束但没有终稿 | 被抢占→重试；未抢占→`ErrModelNoFinal` |
| 检查点失败 | 抢占重试的中间检查点失败 | ticket 拒绝；运行失败；不进入第二次模型调用 |
| 部分增量 | 抢占前已有 `MessageDelta` | `ModelPreempted.Emitted=true`；无 `MessageDone` |
| 持久消息 | Inbox 有 `before_model`，同时抢占 | 持久消息不参与取消；下次投递点进入 |
| 上下文超限 | 抢占后重试遇到长度错误 | 分别计数；超限恢复仍可用 |
| `-race` | 抢占与模型结束并发 | 只有一个结果：成功或重试，不双发 |

### 10.3 P1-6 写入

| 用例 | 场景 | 期望 |
|---|---|---|
| 写入时机 | 工具开始时读取头部 | `InFlight` 非 nil；助手消息在；调用是改写后的；执行记录为 pending |
| 检查中断 | `BeforeTool` 返回中断 | 头部是 `Pending` + `Pause`，不是 `InFlight` |
| 检查停止 | `BeforeTool` 返回停止 | 不写 `InFlight`；写普通结束检查点 |
| 提交失败 | 存储在在途提交时报错 | handler 未调用；运行失败；没有 `ToolStarted` |
| 运行取消 | 提交前取消 | 不写 `InFlight`；不执行工具 |
| 开关关闭 | `WithInFlightCheckpoint(false)` | 不产生额外提交；Phase 4 行为不变 |
| 非持久存储 | 普通 `Checkpointer` | 不写 `InFlight` |
| 子代理/分支 | `topLevel=false` | 不写 `InFlight` |
| 人工审批恢复 | `runResumed` 批次 | 不写 `InFlight` |
| 确认 | 本步有持久输入 | 在途提交同时确认；恢复后不重复投递 |
| 事件 | 提交成功 | 发布 `ToolBatchInFlight`，调用为改写后参数 |
| 互斥校验 | 手工构造 `InFlight` + `Pause` | 存储拒绝 `ErrInFlightConflict` |
| 快照校验 | 手工构造无快照/无摘要/空调用 | 存储拒绝 |
| 克隆 | 修改原调用参数 | 已提交检查点不受影响 |
| 摘要 | 旧提交与新增字段 | 旧摘要字节不变；新字段改变摘要 |

### 10.4 P1-6 恢复

| 用例 | 场景 | 期望 |
|---|---|---|
| `pending` 执行 | 头部在途，台账 pending | 执行一次；结果进入历史；`InFlight` 清除；下一步为 `step+1` |
| `claimed` 未知 | 台账 claimed | 不执行；返回“结果未知”；不自动重试 |
| `unknown` 未知 | 台账 unknown | 不执行；返回“结果未知” |
| `completed` 回放 | 台账 completed | 回放结果；不执行；不跑 AfterTool |
| 混合批次 | completed + claimed + pending | 每个调用恰好一条结果；事件计数正确 |
| 不重跑检查 | 恢复时统计 `CheckToolPolicy` / `BeforeTool` 调用 | 计数为 0 |
| 助手消息匹配 | ID/名称/顺序一致，参数不同 | 恢复成功；使用批次参数 |
| 助手消息不匹配 | 少一条/名称不同/最后不是助手 | `ErrInFlightUnrecoverable` |
| 工具被移除 | 快照有名称，静态表没有 | `ErrToolUnavailable` |
| 工具未广播 | 快照 Advertised 没有名称 | `ErrToolNotAdvertised` |
| 摘要变化 + 有 pending | 当前摘要不同 | 拒绝恢复 |
| 摘要变化 + 全部 claimed/completed | 不会执行 handler | 允许恢复 |
| 审批参数 | `Resume(..., Approval{})` | 拒绝 |
| 工作流 Agent | `a.loop == nil` | 拒绝 |
| 非持久存储 | 头部带 `InFlight` | 拒绝 |
| 结束指令 | 工具返回停止 | 先提交结果；再结束；`InFlight` 清除 |
| 恢复提交失败 | 恢复检查点写入失败 | 运行失败；下次恢复仍按台账处理；不重复执行 |
| 队列状态 | `RunStatus` | `Paused=false`，`InFlight=true` |
| 事件 | 恢复成功 | `ToolBatchRecovered` 在提交后发布 |
| `-race` | 内存/文件存储 | 无数据竞争 |

### 10.5 失败路径与存储

| 用例 | 场景 | 期望 |
|---|---|---|
| 批次中途错误 | 一个工具报错，另一个已完成 | 每个调用一条结果；失败检查点末尾是工具结果；`InFlight` 清除 |
| 台账读取失败 | 失败路径读取执行记录报错 | 结果为“未执行 + 读取错误”；会话仍合法 |
| 失败检查点失败 | 在途头保留 | 下次恢复：claimed→未知，pending→执行；不重复副作用 |
| `Fork` | 父检查点带 `InFlight` | 拒绝 |
| `BeginRun` | 头部带 `InFlight` | `ErrResumeRequired`（内存/文件） |
| `prepare` | 非协调存储，头部带 `InFlight` | `ErrResumeRequired` |
| `RecordCompletion` | 头部带 `InFlight` | 拒绝 |
| 元数据查询 | 在途头 | `Paused=false`，`InFlight=true` |
| 侧车重建 | 删除侧车后重扫日志 | 与主投影一致 |
| 文件日志 | 收件箱阶段/在途字段 | 往返后一致；损坏记录被拒绝 |
| 旧数据 | 无 `in_flight`、无 `phase` | 正常读取；摘要不变 |

### 10.6 验收标准

1. `go test ./...` 全绿。
2. `go test -race ./agent ./checkpoint` 全绿。
3. `gofmt -l` 对新文件无输出；不重排既有 CRLF 文件。
4. 新事件往返测试全部通过。
5. 崩溃注入矩阵覆盖：在途提交前、提交后执行前、执行中、Phase 4 前、Phase 4 后、恢复提交前、恢复提交后。
6. 现有工具并发、超限恢复、HITL、分支、队列测试无回归。
7. 反向检查至少覆盖：
   - 去掉在途提交，批次执行时头部仍无批次（测试应失败）；
   - 去掉 `takeReady` 的 `next_turn` 保留，阶段测试应失败；
   - 去掉每步一次抢占限制，连续抢占测试应观察到两次取消；
   - 去掉摘要检查，工具变化 + pending 的测试应执行 handler（测试应失败）；
   - 去掉失败路径补齐，失败检查点末尾应出现助手工具调用（测试应失败）；
   - 去掉 `sameInboxMessage` 的阶段比较，同 ID 不同阶段的测试应变成重复（测试应失败）。

## 11. 实施拆分

### 11.1 任务表

| 任务 | 内容 | 依赖 | 主要文件 | 验收 |
|---|---|---|---|---|
| IQ-01 | `core` 阶段/来源/状态类型与校验 | 无 | `core/input.go`、`core/input_test.go` | 阶段闭集、错误、空值规范化 |
| IQ-02 | 队列阶段、`takeReady` / `takePhase`、ticket 状态 | IQ-01 | `agent/steering.go`、`agent/steering_internal_test.go` | 队列和 ticket 矩阵 |
| IQ-03 | 逻辑输入队列、`PrepareTurn` / `settleTurn` 接入、`newTurn` | IQ-02 | `agent/loop.go`、`agent/runtime.go`、`agent/hitl.go`、`agent/input_test.go` | 投递矩阵、顺序、续跑 |
| IQ-04 | 持久收件箱阶段、`DeliverWith`、去重 | IQ-01 | `checkpoint/durable.go`、`durable_memory.go`、`file.go`、`agent/inbox.go` | 阶段过滤、冲突、旧记录兼容 |
| IQ-05 | 抢占登记、`streamModel` 无终稿、循环重试、事件 | IQ-02、IQ-03 | `agent/loop.go`、`agent/steering.go`、`agent/options.go`、`agent/preempt_test.go` | 抢占矩阵、`-race` |
| IF-01 | `InFlightBatch`、校验、克隆、摘要、元数据、协调器 | 无 | `checkpoint/*` | 存储契约、旧摘要不变 |
| IF-02 | 执行前写入、配置、事件 | IF-01 | `agent/loop.go`、`agent/options.go`、`agent/inflight_test.go` | 写入时机、失败路径 |
| IF-03 | `Resume` 检测、预校验、Claim、`rc.inFlight` | IF-01 | `agent/hitl.go`、`agent/run_prepare.go` | 校验矩阵 |
| IF-04 | 恢复执行、`ToolBatchRecovered`、下一步 | IF-02、IF-03 | `agent/loop.go`、`agent/hitl.go` | pending/claimed/completed 矩阵 |
| IF-05 | 批次中途失败补齐、失败检查点 | IF-02 | `agent/loop.go`、`agent/inflight_failure_test.go` | 每个调用一条结果 |
| IF-06 | `RunStatus.InFlight`、查询/侧车、队列文档 | IF-01 | `agent/run_status.go`、`checkpoint/query.go`、`metadata_index.go`、`queue/run_worker.go` | 状态与投影 |
| IF-07 | 崩溃注入矩阵、文件存储、`-race`、示例 | IF-04、IF-05、IF-06 | `agent/*_test.go`、`checkpoint/*_test.go`、`examples/` | 验收标准 1–7 |

### 11.2 建议顺序

1. IQ-01 → IQ-02：先把阶段和队列语义固定。
2. IQ-03 → IQ-04：接入两个投递点，再补持久层阶段。
3. IF-01：存储模型独立落地，可以先不接线。
4. IF-02 → IF-03 → IF-04：写入、恢复入口、恢复执行。
5. IF-05：失败路径补齐。
6. IQ-05：抢占最后做，避免和恢复路径同时引入不确定性。
7. IF-06 → IF-07：状态、投影、崩溃矩阵和示例。

抢占与在途没有硬依赖，可以并行开发；但建议抢占最后合并，因为它改变了模型调用循环的控制流，容易掩盖在途恢复的问题。

## 12. 风险与待定问题

### 12.1 风险

1. **提供方不严格遵守取消。** 有些实现可能忽略上下文取消，继续返回终稿。此时“成功优先”会使用终稿，抢占没有缩短延迟。需要在文档中说明取消是尽力而为，不承诺时延上限。
2. **部分增量已经发给消费者。** 抢占无法撤回已经发布的 `MessageDelta`；必须依赖 `ModelPreempted` 让消费者重置。任何漏处理该事件的消费者都会拼接两次尝试的文本。
3. **抢占检查点失败会导致运行失败。** 这是为了不在没有持久边界的情况下继续重试；代价是抢占路径多了一个失败点。
4. **持久输入确认与 `InputDelivered` 的时序不完全一致。** 事件表示进入历史，确认在检查点提交时生效。消费者不能把事件当作持久确认。
5. **跨生产者顺序不确定。** 临时消息和持久消息来自不同通道，只能保证临时先于持久；同一来源内先进先出。对顺序敏感的业务需要在消息内容里带序号。
6. **在途检查点增加存储写入。** 工具密集的步骤提交次数增加约一倍；大参数批次会增加检查点体积。可以用 `WithInFlightCheckpoint(false)` 关闭，但要接受崩溃窗口回到现状。
7. **在途批次包含改写后的参数。** 这些参数可能包含敏感内容；检查点会保存它们。需要在存储层沿用现有的访问控制和保留策略。
8. **工具实现变化无法完全检测。** 摘要覆盖 schema，不覆盖实现行为；同名同 schema 但副作用变化的工具无法被摘要发现。恢复前应保持工具版本稳定。
9. **`claimed` 保守处理会要求人工确认。** 即使工具实际上从未执行，恢复也只返回“结果未知”。这是为了不重复副作用，代价是恢复可能需要人工查询外部系统。
10. **工具被移除会阻止恢复。** 当前 `resumeStepView` 只从静态表解析，见 [agent/stepview.go:340](../../agent/stepview.go#L340)；即使所有调用都已完成，只要名字无法解析，预校验仍会拒绝。后续可以考虑“只回放已完成结果、不需要实现”的优化，但本轮不做。
11. **动态工具不能跨恢复。** 运行时注入的工具没有持久实现，恢复必须拒绝。调用方应在暂停/崩溃恢复前把动态工具转成静态工具，或接受人工处理。
12. **非持久存储无法提供崩溃安全。** `checkpoint.Memory` 只能在进程内模拟恢复；真正的崩溃安全需要文件存储或自定义持久存储。测试要明确这一点。
13. **队列不自动恢复在途。** 本文只提供 `RunStatus.InFlight` 和 `Resume`，不改变 worker 的自动重试策略。如果产品要求“崩溃后自动继续”，需要另做 P1-7/P1-8 的设计。
14. **失败检查点可能写不进去。** 在存储故障时，头部可能保留在途；下一次恢复会按台账处理。设计上安全，但需要监控“在途头长期未解决”的线程。
15. **`next_turn` 会延长运行。** 只要 Inbox 里有未确认消息（包括 `next_turn`），`settleTurn` 就会继续消耗步数。`maxTurns` 仍然是硬上限，配置时要考虑这一点。

### 12.2 待定问题

1. 抢占是否需要暴露“取消原因”给提供方？当前只把 `Reason` 放进 `ModelPreempted` 和尝试记录，不改变模型请求。
2. 是否需要 `InputRejected` 事件？当前临时消息用 ticket、持久消息用错误和 Inbox 状态表达拒绝，暂不新增事件。
3. `InputDelivered` 是否应该携带消息内容？当前只带计数和 ID，避免事件流复制大消息；消费者可以从生产者侧关联。
4. `after_model` / `after_tools` 是否需要在未来变成精确事件触发？当前按最晚投递时间处理，简单且不会产生非法会话；如果产品需要“工具批次刚结束就插入”，应另做设计。
5. 是否需要在摘要里加入工具执行的策略版本？当前只有 schema 摘要；策略变化无法检测。这属于后续的执行策略统一（P2-10）。
6. 是否需要为在途恢复提供“强制跳过已完成结果”的运维开关？当前不做，安全优先。
7. 是否需要在 `RunStatus` 中同时报告“在途调用的数量/工具名”？当前只报告布尔值，避免读完整检查点；查询层可以用 `Latest` 自行读取。

## 13. 代码锚点

以下锚点用于实现时定位，均在基线 `9e5df25` 上核对。

| 位置 | 内容 |
|---|---|
| [agent/loop.go:98](../../agent/loop.go#L98) | `run` 入口与队列关闭 |
| [agent/loop.go:115](../../agent/loop.go#L115) | `prepareRun`，恢复批次和初始状态 |
| [agent/loop.go:191](../../agent/loop.go#L191) | `runTask` 外层循环 |
| [agent/loop.go:210](../../agent/loop.go#L210) | `settleTurn` |
| [agent/loop.go:249](../../agent/loop.go#L249) | `runStep` |
| [agent/loop.go:259](../../agent/loop.go#L259) | `PrepareTurn` 读取临时消息 |
| [agent/loop.go:265](../../agent/loop.go#L265) | `PrepareTurn` 读取持久收件箱 |
| [agent/loop.go:300](../../agent/loop.go#L300) | 组装模型请求 |
| [agent/loop.go:305](../../agent/loop.go#L305) | 请求的系统提示、历史和工具 |
| [agent/loop.go:311](../../agent/loop.go#L311) | 冻结步骤视图 |
| [agent/loop.go:330](../../agent/loop.go#L330) | 模型调用重试循环 |
| [agent/loop.go:355](../../agent/loop.go#L355) | 上下文超限恢复的请求重建 |
| [agent/loop.go:371](../../agent/loop.go#L371) | 超限恢复的中间检查点 |
| [agent/loop.go:413](../../agent/loop.go#L413) | 追加助手消息 |
| [agent/loop.go:433](../../agent/loop.go#L433) | 终稿后调用 `settleTurn` |
| [agent/loop.go:438](../../agent/loop.go#L438) | 终稿检查点与 `Release` |
| [agent/loop.go:463](../../agent/loop.go#L463) | `max_tokens` 截断路径 |
| [agent/loop.go:483](../../agent/loop.go#L483) | `CheckToolPolicy` |
| [agent/loop.go:486](../../agent/loop.go#L486) | `BeforeTool` 循环 |
| [agent/loop.go:495](../../agent/loop.go#L495) | 审批中断 |
| [agent/loop.go:507](../../agent/loop.go#L507) | 调用 `execTools` |
| [agent/loop.go:516](../../agent/loop.go#L516) | Phase 4 检查点 |
| [agent/loop.go:546](../../agent/loop.go#L546) | `streamModel` |
| [agent/loop.go:571](../../agent/loop.go#L571) | `commitOptions` |
| [agent/loop.go:583](../../agent/loop.go#L583) | `checkpoint` |
| [agent/loop.go:601](../../agent/loop.go#L601) | `interrupt` |
| [agent/loop.go:636](../../agent/loop.go#L636) | `fail` |
| [agent/loop.go:653](../../agent/loop.go#L653) | `seam` |
| [agent/steering.go:27](../../agent/steering.go#L27) | `SteerOutcome` |
| [agent/steering.go:39](../../agent/steering.go#L39) | `SteerTicket` |
| [agent/steering.go:100](../../agent/steering.go#L100) | `push` |
| [agent/steering.go:113](../../agent/steering.go#L113) | `drain` |
| [agent/steering.go:127](../../agent/steering.go#L127) | `drainOrClose` |
| [agent/steering.go:144](../../agent/steering.go#L144) | `close` |
| [agent/steering.go:159](../../agent/steering.go#L159) | `settleDelivered` |
| [agent/steering.go:166](../../agent/steering.go#L166) | `settleUndelivered` |
| [agent/run.go:146](../../agent/run.go#L146) | `Run.Steer` |
| [agent/run.go:152](../../agent/run.go#L152) | `Run.Cancel` |
| [agent/run.go:233](../../agent/run.go#L233) | `drive` 生命周期 |
| [agent/runtime.go:20](../../agent/runtime.go#L20) | `RunContext` |
| [agent/runtime.go:49](../../agent/runtime.go#L49) | `steering` 字段 |
| [agent/runtime.go:128](../../agent/runtime.go#L128) | `RunContext.Steer` |
| [agent/exectools.go:42](../../agent/exectools.go#L42) | `execTools` |
| [agent/exectools.go:68](../../agent/exectools.go#L68) | 单调用 worker |
| [agent/exectools.go:128](../../agent/exectools.go#L128) | 完成记录写入内存台账 |
| [agent/exectools.go:149](../../agent/exectools.go#L149) | 并发分段 |
| [agent/exectools.go:310](../../agent/exectools.go#L310) | `callOne` |
| [agent/exectools.go:351](../../agent/exectools.go#L351) | `ClaimTool` |
| [agent/exectools.go:360](../../agent/exectools.go#L360) | 结果未知 |
| [agent/exectools.go:367](../../agent/exectools.go#L367) | 完成回放 |
| [agent/exectools.go:426](../../agent/exectools.go#L426) | `toolInvocation` |
| [agent/exectools.go:441](../../agent/exectools.go#L441) | `pendingExecutions` |
| [agent/exectools.go:479](../../agent/exectools.go#L479) | `unknownToolResult` |
| [agent/hitl.go:79](../../agent/hitl.go#L79) | `Agent.Resume` |
| [agent/hitl.go:101](../../agent/hitl.go#L101) | 恢复占用声明 |
| [agent/hitl.go:184](../../agent/hitl.go#L184) | 无待审批批次的分支 |
| [agent/hitl.go:204](../../agent/hitl.go#L204) | 构造审批恢复批次 |
| [agent/hitl.go:236](../../agent/hitl.go#L236) | 恢复工具预检 |
| [agent/hitl.go:301](../../agent/hitl.go#L301) | `runResumed` |
| [agent/durable_runtime.go:83](../../agent/durable_runtime.go#L83) | `durableStore` |
| [agent/durable_runtime.go:98](../../agent/durable_runtime.go#L98) | 登记收件箱确认 |
| [agent/durable_runtime.go:125](../../agent/durable_runtime.go#L125) | 待确认集合 |
| [agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139) | 完成工具记录 |
| [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339) | `commitCheckpoint` |
| [agent/durable_runtime.go:355](../../agent/durable_runtime.go#L355) | `commitCheckpointLocked` |
| [agent/durable_runtime.go:366](../../agent/durable_runtime.go#L366) | 收集确认、占用和执行记录 |
| [agent/run_prepare.go:23](../../agent/run_prepare.go#L23) | `Run.prepare` |
| [agent/run_status.go:48](../../agent/run_status.go#L48) | `Agent.RunStatus` |
| [agent/run_status.go:66](../../agent/run_status.go#L66) | `Paused` 计算 |
| [agent/terminal.go:161](../../agent/terminal.go#L161) | `buildTerminalRecord` |
| [agent/terminal.go:176](../../agent/terminal.go#L176) | 终态记录拒绝暂停 |
| [agent/stepview.go:168](../../agent/stepview.go#L168) | `snapshot` |
| [agent/stepview.go:340](../../agent/stepview.go#L340) | `resumeStepView` |
| [agent/stepview.go:388](../../agent/stepview.go#L388) | `freezeStepView` |
| [agent/stepview.go:437](../../agent/stepview.go#L437) | `checkReplayDrift` |
| [agent/stepview.go:511](../../agent/stepview.go#L511) | `toolDigest` |
| [checkpoint/checkpoint.go:45](../../checkpoint/checkpoint.go#L45) | `Checkpoint` |
| [checkpoint/checkpoint.go:65](../../checkpoint/checkpoint.go#L65) | `Pause` 字段 |
| [checkpoint/checkpoint.go:123](../../checkpoint/checkpoint.go#L123) | `PendingHITL` |
| [checkpoint/checkpoint.go:130](../../checkpoint/checkpoint.go#L130) | `Pause` |
| [checkpoint/checkpoint.go:144](../../checkpoint/checkpoint.go#L144) | `EffectivePauseID` |
| [checkpoint/durable.go:15](../../checkpoint/durable.go#L15) | `Durable` 接口 |
| [checkpoint/durable.go:29](../../checkpoint/durable.go#L29) | `ToolExecution` 读取 |
| [checkpoint/durable.go:60](../../checkpoint/durable.go#L60) | `InboxMessage` |
| [checkpoint/durable.go:65](../../checkpoint/durable.go#L65) | `DeliveryReceipt` |
| [checkpoint/durable.go:95](../../checkpoint/durable.go#L95) | `CommitRequest` |
| [checkpoint/durable.go:124](../../checkpoint/durable.go#L124) | `ToolExecution` |
| [checkpoint/durable_memory.go:65](../../checkpoint/durable_memory.go#L65) | 内存 `Deliver` |
| [checkpoint/durable_memory.go:87](../../checkpoint/durable_memory.go#L87) | 内存 `Inbox` |
| [checkpoint/durable_memory.go:272](../../checkpoint/durable_memory.go#L272) | 内存 `ClaimTool` |
| [checkpoint/durable_memory.go:333](../../checkpoint/durable_memory.go#L333) | 内存 `ToolExecution` |
| [checkpoint/durable_memory.go:419](../../checkpoint/durable_memory.go#L419) | `sameInboxMessage` |
| [checkpoint/file.go:183](../../checkpoint/file.go#L183) | 文件 `Deliver` |
| [checkpoint/file.go:211](../../checkpoint/file.go#L211) | 文件 `Inbox` |
| [checkpoint/file.go:438](../../checkpoint/file.go#L438) | 文件 `ClaimTool` |
| [checkpoint/file.go:516](../../checkpoint/file.go#L516) | 文件 `ToolExecution` |
| [checkpoint/file.go:743](../../checkpoint/file.go#L743) | 文件日志校验 |
| [checkpoint/coordinator.go:42](../../checkpoint/coordinator.go#L42) | 提交摘要结构 |
| [checkpoint/coordinator.go:137](../../checkpoint/coordinator.go#L137) | `commitRequestDigest` |
| [checkpoint/coordinator_memory.go:26](../../checkpoint/coordinator_memory.go#L26) | 内存 `BeginRun` 拒绝 |
| [checkpoint/coordinator_file.go:23](../../checkpoint/coordinator_file.go#L23) | 文件 `BeginRun` 拒绝 |
| [checkpoint/snapshot.go:13](../../checkpoint/snapshot.go#L13) | `freezeCheckpoint` |
| [checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30) | `cloneCheckpoint` |
| [checkpoint/branch.go:17](../../checkpoint/branch.go#L17) | `Fork` |
| [checkpoint/branch.go:45](../../checkpoint/branch.go#L45) | `Fork` 清空工作状态 |
| [checkpoint/query.go:54](../../checkpoint/query.go#L54) | `CheckpointMetadata` |
| [checkpoint/query.go:150](../../checkpoint/query.go#L150) | `checkpointMetadataOf` |
| [checkpoint/metadata_index.go:60](../../checkpoint/metadata_index.go#L60) | `metaFields` |
| [checkpoint/metadata_index.go:409](../../checkpoint/metadata_index.go#L409) | `projectJournalLine` |
| [checkpoint/metadata_index.go:469](../../checkpoint/metadata_index.go#L469) | `summaryPauseMarker` |
| [core/event.go:37](../../core/event.go#L37) | `TurnStarted` |
| [core/event.go:82](../../core/event.go#L82) | `Interrupted` |
| [core/event.go:202](../../core/event.go#L202) | 事件标记区 |
| [core/eventjson.go:18](../../core/eventjson.go#L18) | 事件线格式 |
| [core/eventjson.go:61](../../core/eventjson.go#L61) | `MarshalEvent` |
| [core/eventjson.go:134](../../core/eventjson.go#L134) | `UnmarshalEvent` |
| [core/message.go:112](../../core/message.go#L112) | `ToolCalls` |
| [llm/model.go:23](../../llm/model.go#L23) | `Model.Generate` |
| [queue/run_worker.go:286](../../queue/run_worker.go#L286) | 队列记录检查点 |
| [queue/run_worker.go:344](../../queue/run_worker.go#L344) | `terminalTaskRecord` |
| [queue/run_worker.go:360](../../queue/run_worker.go#L360) | `Paused` 映射 |
