# 运行时并发加固与 Task 类别设计草稿

- 文档类型：设计草稿，尚未实现。
- 日期：2026-10-22。
- 代码基线：`main` 分支提交 `9e5df25`。本文行号按写作时的文件内容核对，后续改动后需要重新核对。
- 适用范围：goagent 的 `agent` 运行时、工具执行、工作流、DAG 计划、同线程准入。
- 关联文档：
  - [工具调用粒度并发调度设计（P0-3）](tool-call-concurrency-design.md)：工具分段算法已经实现，本文只补“并行上限”，不重复分段设计。
  - [输入队列与工具批次在途检查点架构设计（P1-5 / P1-6）](input-queue-and-inflight-checkpoint-design.md)：输入投递、抢占、在途检查点，本文不重复。
  - [Codex core 运行时对 goagent 的优化建议](codex-core-runtime-optimization-proposal.md)：P1-7 实时状态投影、P1-8 会话忙时策略，以及第七节的边界约束。
  - [StepView 与外层任务循环设计草稿](stepview-and-task-loop-design-draft.md)：外层任务循环和步骤循环的术语。
  - [会话与执行模型：概念、语义及关系](../design/session-execution-concepts.md)：Session、Thread、Turn、Run、Checkpoint、RunContext 的既有定义。

本文是设计草稿。所有工作量数字都是估算，不是实测结果；所有默认值都是草稿取值，需要在实现阶段用压测和真实调用数据确认。

---

## 1. 结论

要做的事情可以分成五块，前四块是用户指定的优化，第五块是把 `Task` 作为一层运行期类别接进来：

| 项目 | 结论 | 是否改变默认行为 |
| --- | --- | --- |
| 并行工具上限 | 新增 `WithToolConcurrency`，默认每步并行段最多 8 个调用；`0` 表示不限制；串行段不受影响。 | 是。默认峰值从“调用数”变成 8。 |
| 子 Agent / 工作流预算 | 新增任务树共享的 `TaskBudget`：深度、子任务总数、模型调用总数；`AsTool`、`Parallel`、DAG 节点、planner 都在创建子环境时扣预算。 | 是。默认深度 8、子任务 64、模型调用不限。 |
| 失败时取消兄弟分支 | `Parallel` 默认在真实失败时取消兄弟分支；`Plan` 的 `FailFast` 从“不再启动新节点”扩展为“取消在途节点”。 | 是。分支和节点的收尾时间、事件顺序会变。 |
| 同线程准入与默认协调 | 在持久 claim 之上加进程内准入队列。`Session.Send` 默认排队；`Agent.Run`、`Agent.Stream`、`Resume` 默认快速失败。跨进程仍以 claim 为准。 | 是。`Session.Send` 不再直接返回 `ErrClaimConflict`。 |
| `Task` 类别 | 只加运行期元数据、任务树和预算，不新增持久化任务表，不复制 Codex 的“新任务替换旧任务”。 | 新增 API 和事件，不改现有 Run 生命周期。 |

一句话概括：

> 持久 claim 仍然是同线程写入的唯一正确性来源；这次加的是它上面的资源上限、失败传播、准入排队和任务树元数据。

---

## 2. 目标与非目标

### 2.1 目标

1. 一个工具批次里，并行段的同时执行数有明确上限，而且这个上限不会把串行段、工具超时和工具放弃逻辑弄坏。
2. 子 Agent、工作流分支、DAG 节点和 planner 共享一棵任务树预算，能限制嵌套深度、子任务总数和模型调用总数。
3. 并行分支或 DAG 节点失败时，兄弟分支能尽快收到取消信号；最终错误仍然是根因，不是取消产生的次要错误。
4. 同一线程上的第二个运行有明确的去向：快速失败、排队，或者由调用方显式选择；不再只有“占用冲突”一种结果。
5. 给运行期加一个薄的 `Task` 类别，让预算、深度、父子关系、事件和日志有统一说法，同时不引入新的持久化主键。
6. 保持现有 Checkpoint、claim、fencing、HITL 暂停恢复和事件基数的语义。

### 2.2 非目标

以下内容明确不在本文范围内：

- 不用进程内互斥替代 `checkpoint.ThreadCoordinator`。跨进程写入安全仍然由 claim、fencing token 和 revision 决定。
- 不照搬 Codex 的“同一 Session 只有一个活动 Task，新 Task 到达就中止旧 Task”的替换语义。goagent 的线程有持久历史和工具副作用，中止旧运行不能安全回滚。
- 不实现跨进程排队。本文的准入队列只在单个进程内有效；另一个进程同时写入时仍然返回 `checkpoint.ErrClaimConflict`。
- 不强行终止已经进入的 goroutine。Go 不能杀死正在运行的函数；所有取消都是协作式的，工具不检查 context 就会继续运行。
- 不实现 token 级预算。当前模型返回的 usage 是事后信息，不能可靠地在请求前扣减；本文只做深度、子任务数、模型调用数三种可数预算。
- 不新增持久化 Task 表、Session 聚合表或新的 RunRecord 类型。需要跨进程查询时继续用现有 RunRecord 和 checkpoint。
- 不把 `Parallel` 分支改成独立线程。本文仍然沿用 `forBranch` 克隆 State、共享 Store 和 ThreadID 的模型。
- 不实现“忙时把输入追加到当前运行”。那需要 P1-5 的输入队列和 P1-7 的进程内运行登记表；本文只做运行级准入排队。

---

## 3. 现状与代码证据

### 3.1 当前运行层次

现有代码的层次关系是：

```text
Session / Agent
  └─ Run                       一次执行的生命周期句柄
       └─ RunContext           本次执行的运行环境
            └─ Runnable        LLM 循环、Sequential、Parallel、Loop、Plan
                 └─ Turn/Step  一次模型调用及它触发的工具
                      └─ 工具分段 → 工具调用
```

- `Run` 的惰性驱动、准备、终结事件在 [agent/run.go:233](../../agent/run.go#L233) 的 `drive`。
- 同线程 claim 在 [agent/run_prepare.go:39](../../agent/run_prepare.go#L39) 到 [agent/run_prepare.go:57](../../agent/run_prepare.go#L57) 取得。
- `RunContext` 的字段在 [agent/runtime.go:20](../../agent/runtime.go#L20) 到 [agent/runtime.go:60](../../agent/runtime.go#L60)。
- 三种派生环境：`deeper`、`subRun`、`forBranch` 分别在 [agent/runtime.go:65](../../agent/runtime.go#L65)、[agent/runtime.go:84](../../agent/runtime.go#L84)、[agent/runtime.go:106](../../agent/runtime.go#L106)。

### 3.2 并行工具：分段已经有，上限没有

`buildToolCallSegments` 在 [agent/toolsegments.go:22](../../agent/toolsegments.go#L22) 把调用分成串行段和并行段。并行段当前的执行方式是“一个调用一个 goroutine，全部启动后等全部结束”：

- [agent/exectools.go:167](../../agent/exectools.go#L167) 开始一个并行段。
- [agent/exectools.go:174](../../agent/exectools.go#L174) 先发布 `ToolStarted`。
- [agent/exectools.go:176](../../agent/exectools.go#L176) 到 [agent/exectools.go:179](../../agent/exectools.go#L179) 为每个调用启动 goroutine。
- [agent/exectools.go:181](../../agent/exectools.go#L181) 等待整段结束。

因此，只要模型一次返回 50 个可并行调用，就会同时出现 50 个等待中的调用。代码里没有上限字段，`WithToolExecution` 只决定“并行还是全串行”，见 [agent/options.go:109](../../agent/options.go#L109) 到 [agent/options.go:113](../../agent/options.go#L113)。

另一个关键事实：每个调用内部还会再启动一个 handler goroutine，并允许在超时后放弃等待，见 [agent/exectools.go:86](../../agent/exectools.go#L86) 到 [agent/exectools.go:94](../../agent/exectools.go#L94)，放弃宽限是 [agent/exectools.go:201](../../agent/exectools.go#L201) 的 `abandonGrace`。所以“并行上限”只能约束循环还在等待的调用数，不能约束底层已经跑起来的 goroutine 数。这个限制必须在文档和测试里写清楚。

### 3.3 子 Agent 和工作流：有深度约束，没有统一预算

- `AsTool` 在 [agent/subagent.go:22](../../agent/subagent.go#L22) 包装子 Agent；真正执行在 [agent/subagent.go:38](../../agent/subagent.go#L38) 的 `Call`，到 [agent/subagent.go:53](../../agent/subagent.go#L53) 调用 `child.Run`。
- `Parallel` 在 [agent/workflow.go:141](../../agent/workflow.go#L141) 的 `run` 里为每个分支创建一个 `RunContext`，然后每个分支一个 goroutine，最后在 [agent/workflow.go:164](../../agent/workflow.go#L164) 等全部结束。
- DAG 计划只有自己的并发上限：默认值在 [agent/plan.go:148](../../agent/plan.go#L148) 是 8；调度循环在 [agent/plan.go:258](../../agent/plan.go#L258) 到 [agent/plan.go:286](../../agent/plan.go#L286)。
- 唯一的深度约束是 transfer 的 `maxTransferDepth = 8`，见 [agent/transfer.go:19](../../agent/transfer.go#L19) 和 [agent/transfer.go:39](../../agent/transfer.go#L39)。`AsTool`、`Parallel`、`Plan` 都不走这个计数。

所以现在可以构造这样的结构：

```text
父 Agent
  └─ 一个工具批次并行调用 8 个 AsTool
       └─ 每个子 Agent 再并行调用 8 个 AsTool
            └─ 继续……
```

工具上限和 DAG 并发上限都只管一层，管不了整棵树的子任务总数。

### 3.4 失败传播：先等完，再报错

`Parallel.merge` 在 [agent/workflow.go:174](../../agent/workflow.go#L174) 才开始看错误，而 `run` 在 [agent/workflow.go:164](../../agent/workflow.go#L164) 已经等完了所有分支。合并时按声明顺序返回第一个错误或控制结果，见 [agent/workflow.go:177](../../agent/workflow.go#L177) 到 [agent/workflow.go:189](../../agent/workflow.go#L189)。

DAG 计划的 `FailFast` 只做到“不再启动新节点”：

- [agent/plan.go:259](../../agent/plan.go#L259) 在 `failFastErr != nil` 时停止启动。
- 已经在运行的节点不会被取消，仍然在 [agent/plan.go:319](../../agent/plan.go#L319) 逐个收结果。
- [agent/plan.go:358](../../agent/plan.go#L358) 每次失败都会覆盖 `failFastErr`，最终错误取决于哪个结果最后被收走。

这意味着一个分支失败后，兄弟分支可能还要跑几分钟，期间继续产生模型费用、工具副作用和检查点写入。

### 3.5 同线程：只有持久 claim，没有进程内准入

当前流程是：

1. `Run.drive` 调用 `prepare`。
2. `prepare` 如果发现 store 实现了 `checkpoint.ThreadCoordinator`，在 [agent/run_prepare.go:44](../../agent/run_prepare.go#L44) 调 `BeginRun`。
3. File store 的 `BeginRun` 在 [checkpoint/coordinator_file.go:9](../../checkpoint/coordinator_file.go#L9) 检查活动 claim：
   - 被别人持有且在租约内：返回 `ErrClaimConflict`，见 [checkpoint/coordinator_file.go:17](../../checkpoint/coordinator_file.go#L17) 到 [checkpoint/coordinator_file.go:20](../../checkpoint/coordinator_file.go#L20)。
   - 线程停在暂停 checkpoint：返回 `ErrResumeRequired`，见 [checkpoint/coordinator_file.go:22](../../checkpoint/coordinator_file.go#L22) 到 [checkpoint/coordinator_file.go:25](../../checkpoint/coordinator_file.go#L25)。
4. 拿到 claim 后，写保护在 [agent/write_guard.go:67](../../agent/write_guard.go#L67) 启动续租，续租周期是租约的一半，见 [agent/write_guard.go:301](../../agent/write_guard.go#L301) 到 [agent/write_guard.go:306](../../agent/write_guard.go#L306)。
5. `Session.Send` 每次都新建一个运行，见 [agent/session.go:47](../../agent/session.go#L47) 到 [agent/session.go:55](../../agent/session.go#L55)；同线程冲突直接由 `BeginRun` 返回。

README 已经写明这个行为：[README.md:291](../../README.md#L291) 到 [README.md:293](../../README.md#L293)。Memory/File 默认支持协调；第三方 store 只有实现 `ThreadCoordinator` 才有同等保证；可以用 [agent/options.go:166](../../agent/options.go#L166) 的 `WithThreadCoordinationRequired(true)` 在 `agent.New` 时强制检查。

缺的不是“有没有占用”，而是“第二个请求应该怎么处理”。现在只有快速失败，且失败发生在模型和工具开始之前，调用方除了重试没有别的选择。

### 3.6 Task 类别：概念存在，类型不存在

现有概念文档已经区分：

- Session：调用入口。
- Thread：历史归属。
- Turn：一次用户交互，可以跨暂停和恢复。
- Run：一次具体执行。
- RunContext：运行环境。

见 [session-execution-concepts.md](../design/session-execution-concepts.md) 的第 1 节、第 4 节和第 5 节。

代码里没有 `Task` 类型，也没有任务树。`TurnID` 只用于关联，不承担预算和准入。`Run` 是执行句柄，不是可以派生子任务的节点。`deeper`、`subRun`、`forBranch` 只复制环境，不记录“这个子环境属于哪个任务、深度多少、还剩多少预算”。

### 3.7 与既有设计的分工

本文不重复下面几件事：

- 工具分段、串行标记、事件顺序：已经由 `tool-call-concurrency-design.md` 设计并实现。
- 输入队列、投递阶段、抢占、在途检查点：由 `input-queue-and-inflight-checkpoint-design.md` 设计。
- 进程内运行登记表和忙时追加：`codex-core-runtime-optimization-proposal.md` 的 P1-7、P1-8 提出方向，本文只落地其中的“运行级准入队列”，不做“追加到当前运行”。
- 外层任务循环和 StepView：由 `stepview-and-task-loop-design-draft.md` 设计。本文的 Task 类别是它的下层元数据，不替代它。
- 不把 durable claim 换成进程内互斥，也不照搬 Codex 的 rollout 持久化，理由见 `codex-core-runtime-optimization-proposal.md` 第七节。

---

## 4. 设计原则

1. **正确性只认持久 claim。** 准入队列、任务树、预算计数都是进程内资源控制；可以丢，可以重建，不能成为写入正确性的依据。
2. **默认值要有边界，但必须可关。** 新上限默认开启，调用方可以显式调整或关闭；不能出现“只能改代码才能恢复旧行为”的情况。
3. **取消是协作式的。** 兄弟分支收到 context 取消后尽快返回；不检查 context 的工具仍然可能继续运行，最终等待时间由工具超时或调用方的 context 决定。
4. **根因优先。** 取消产生的错误不能覆盖真正的失败；合并只在所有分支都正常完成时发生。
5. **预算在创建子环境时扣。** 不能在子任务已经跑了一半才检查；也不能因为一次失败把已经扣掉的额度退回，否则重试会绕过上限。
6. **不改变事件基数。** 一次顶层运行仍然只有一个 `RunStarted` 和一个终结事件；新增的事件只做观察，不承担控制。
7. **不引入新的持久化主键。** Task 先做运行期元数据；需要跨进程查询时，复用现有 RunRecord、TurnID 和 checkpoint 元数据。
8. **默认行为变化要单独成批。** 工具上限、失败取消、准入排队都会改变调用方能看到的行为，不能和内部重构混在同一个提交里。

---

## 5. Task 类别怎么加

### 5.1 先把 Task 定义清楚

本文的 `Task` 不是 Codex 里那个“可以替换的活动任务”，也不是新的持久化实体。它是一条运行期记录，回答四个问题：

1. 这次执行属于哪条线程、哪个 Turn？
2. 它的父任务是谁，嵌套深度是多少？
3. 它是什么执行形式：普通对话、工作流，还是 DAG 计划？
4. 它和它的后代共享哪一份预算？

对应关系：

```text
Thread
  └─ Turn（用户交互，已有 TurnID）
       └─ Task（运行期逻辑单元，本文新增）
            ├─ Run（一次执行；暂停恢复会产生新的 Run 和新的根 Task，但 TurnID 不变）
            │    └─ Step → 工具调用
            └─ child Task（AsTool、Parallel 分支、DAG 节点）
                 └─ Run 或 RunContext 派生环境
```

关键区别：

- **Task 不是 Turn。** 一个 Turn 可以包含多个 Run（暂停、恢复），每个 Run 有自己的根 Task，但它们的 TurnID 相同。
- **Task 不是 Run。** Run 是可观察、可取消的执行句柄；Task 是预算和父子关系的归属。一个 Task 对应一个 Run 或一个派生环境；多个 Run 不共享同一个 TaskID，只共享 TurnID。
- **Task 不是新的 Thread。** `Parallel` 分支和 DAG 节点仍然共享父线程的 ThreadID、Store 和事件 Topic；`AsTool` 子运行本来就用临时线程隔离，Task 记录的是这条派生关系，不改变它的隔离语义。
- **Task 不是持久化记录。** 当前不写 Task 表。TaskID 只用于运行期事件、日志和预算；进程重启后重新生成。

### 5.2 最小类型

在 `agent` 包新增：

```go
// TaskKind 描述一次执行的主要形式。它不是业务任务类型，
// 不包含 review、compact 这类还没有独立执行路径的类别。
type TaskKind string

const (
	TaskKindChat     TaskKind = "chat"
	TaskKindWorkflow TaskKind = "workflow"
	TaskKindPlan     TaskKind = "plan"
)

// TaskInfo 是一个运行期任务的只读描述。
type TaskInfo struct {
	ID       string
	ParentID string
	ThreadID string
	TurnID   string
	Kind     TaskKind
	Depth    int
}
```

`Run` 增加只读访问：

```go
func (r *Run) Task() TaskInfo

// Agent 包内方法：返回配置的任务形式，空值按 TaskKindChat 处理。
func (a *Agent) taskKind() TaskKind
```

普通运行的 TurnID 在 `prepare` 解析 head 时确定，恢复运行沿用暂停快照的 TurnID；因此句柄刚创建、还没驱动时，`Task().TurnID` 可能为空。TaskID 在句柄创建时就已生成。

`RunContext` 内部增加：

```go
type RunContext struct {
	// ...既有字段

	task *taskRuntime // 共享的任务树：预算、根任务、计数
	info TaskInfo     // 当前任务的身份
}
```

`taskRuntime` 不导出：

```go
type taskRuntime struct {
	rootID string
	budget TaskBudget
	units  atomic.Int64 // 已经创建的子任务数
	turns  atomic.Int64 // 已经发起的模型调用数
}
```

`TaskBudget` 是对外配置：

```go
type TaskBudget struct {
	// MaxDepth 是任务树的最大嵌套深度。0 表示使用默认值。
	MaxDepth int
	// MaxUnits 是一棵任务树最多创建多少个子任务。0 表示使用默认值。
	MaxUnits int
	// MaxTurns 是一棵任务树最多发起多少次模型请求。0 表示不限制。
	MaxTurns int
}
```

草稿默认值：

| 字段 | 默认值 | 理由 |
| --- | --- | --- |
| `MaxDepth` | 8 | 和现有 `maxTransferDepth` 一致，先避免两套深度语义差得太远。 |
| `MaxUnits` | 64 | 能覆盖常见的大计划，又能挡住失控扇出。这个数字需要实测后调整。 |
| `MaxTurns` | 0 | 不改变现有行为；需要总预算的调用方显式设置。 |

对应的 Option：

```go
func WithTaskBudget(b TaskBudget) Option
```

`config` 同时增加 `taskKind TaskKind` 和 `budget TaskBudget` 两个字段：`New` 填 `TaskKindChat`，`NewPlan` 填 `TaskKindPlan`，工作流构造函数填 `TaskKindWorkflow`；`budget` 为配置值，未设置时使用默认值。

工作流 Agent 不走 `agent.New`，所以 `wrapWorkflowWithStore` 也要填这两个字段，否则工作流根运行的 Kind 会是空串、预算会是零值。

本批工作流根运行只使用默认预算：`Parallel`、`Loop` 这类构造函数没有接收 `Option` 的入口，无法直接套用 `WithTaskBudget`。需要自定义时，应给对应构造函数补一个显式参数，或者先把它挂到父运行下（`AsTool`、transfer）由父树统一预算；本文不扩展现有构造函数签名。

### 5.3 Task 在哪里创建

| 创建点 | 现有代码 | Task 关系 |
| --- | --- | --- |
| 顶层 `Run` | [agent/agent.go:236](../../agent/agent.go#L236) 的 `newRunHandleWithID` | 创建根 Task，生成 TaskID，深度 0，新建 `taskRuntime`。 |
| `AsTool` 子运行 | [agent/subagent.go:53](../../agent/subagent.go#L53) 的 `child.Run` | 从父 `RunContext` 继承 `taskRuntime`，父 TaskID 作为 `ParentID`，深度加 1。 |
| `transfer` | [agent/transfer.go:39](../../agent/transfer.go#L39) 的 `rc.deeper()` | 同一个 Task，不增加 Task 深度；仍然受 `maxTransferDepth` 约束。 |
| `Parallel` 分支 | [agent/runtime.go:106](../../agent/runtime.go#L106) 的 `forBranch` | 每个分支创建一个子 Task，深度加 1。 |
| DAG 节点 | [agent/plan.go:385](../../agent/plan.go#L385) 的 `execNode` 里的 `rc.subRun` | 每个节点创建一个子 Task，深度加 1。 |
| planner / replanner | [agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454) 的 `rc.subRun` | 和 DAG 节点一样算一个子任务。 |
| `Sequential` / `Loop` 子 Agent | [agent/workflow.go:89](../../agent/workflow.go#L89)、[agent/workflow.go:112](../../agent/workflow.go#L112) | 共享父 `RunContext`，不创建新 Task；它们是同一个 Task 内的顺序阶段。 |

`deeper` 不增加 Task 深度，因为 transfer 是“同一条对话换一个 Agent 继续”，不是派生独立子任务。它仍然有独立的 `transferDepth`，防止 Agent 之间来回转移。

### 5.4 派生环境的改动

当前 `subRun` 和 `forBranch` 只复制环境，不检查预算。要让预算无法绕过，建议把预算检查放进派生函数本身：

```go
// 旧：
func (rc *RunContext) subRun(input core.Message) *RunContext

// 新：
func (rc *RunContext) subRun(input core.Message, kind TaskKind) (*RunContext, error)
// forBranch 现在已经是 (*RunContext, error)，补上 kind 和 admitChild。

// 并行和计划取消需要一个绑定自己 context 的派生环境：
func (rc *RunContext) subRunWithContext(ctx context.Context, input core.Message, kind TaskKind) (*RunContext, error)
func (rc *RunContext) forBranchWithContext(ctx context.Context, branch string, kind TaskKind) (*RunContext, error)
```

`kind` 由调用方决定：`Parallel` 传分支 Agent 的 `taskKind()`，`execNode` 传 `w.taskKind()`，planner/replanner 传各自 Agent 的 `taskKind()`。拿不到具体 Agent 时用 `TaskKindChat`。

不带 context 的 `subRun` / `forBranch` 等价于传入 `rc.Context` 的包装。两套函数共用同一份实现，区别只有子环境的 `Context` 字段：`Parallel` 传分支 context，Plan 传节点 context，其余调用方继续传 `rc.Context`。

字段怎么传：

- `deeper` 复制 `task` 和 `info`，保持同一个 TaskID；transfer 不是新任务。
- `subRun*` 和 `forBranch*` 把 `admitChild` 返回的子 `TaskInfo` 写入新环境，同时让新环境指向父 `taskRuntime`。
- 子环境拿不到父 Task（例如直接调用 `AsTool` 的测试）时，按根任务新建 `taskRuntime` 和 `TaskInfo`。

两者都在内部调用：

```go
func (rc *RunContext) admitChild(kind TaskKind) (TaskInfo, error)
```

`admitChild` 做三件事：

1. 检查 `info.Depth + 1 <= budget.MaxDepth`。
2. 用 CAS 循环增加 `units`：读当前值，超过上限直接返回错误，否则 `CompareAndSwap`，失败重试。不会出现“先加后回退”的中间态。
3. 生成子 TaskInfo：新 TaskID、父 TaskID、Kind 按调用方传入、Depth 加 1；共享线程时再继承 ThreadID/TurnID。

共享同一线程的派生（`subRun`、`forBranch`）直接继承 ThreadID/TurnID。`AsTool` 的子运行会另起一个临时线程，`admitChild` 先只登记 TaskID、ParentID、Kind、Depth；ThreadID 和 TurnID 由子运行创建环境时填入，避免事件里出现“子任务挂在父线程、子运行却在另一个线程”的矛盾。

如果当前 `RunContext` 没有 `task`（手工构造的环境或直接调用 `AsTool` 的测试），`admitChild` 先用默认预算建一个根任务，再按上面的步骤派生子任务。

`subRun` 的调用点需要处理第二个返回值。当前调用点不多，主要是：

- `AsTool`：子 Agent 走的是 `child.Run`，不走 `subRun`，需要单独接入。
- `Parallel`：已经通过 `forBranch` 返回 error，接入成本低。
- DAG 的 `execNode`、`plan`、replanner：需要把预算错误向上返回。

`AsTool` 的接入方式：

```go
parent := runContextOf(tctx.Context) // 见 5.5
if parent == nil {
	return tool.ErrorResult("sub-agent requires a task-scoped run context"), nil
}
child, err := parent.admitChild(s.child.taskKind())
if err != nil {
	return tool.ErrorResult(err.Error()), nil
}
answer, err := s.child.Run(tctx, in.Task, withTaskParent(parent, child))
```

`Agent.taskKind()` 返回该 Agent 配置的任务形式（默认 `chat`），这样父任务树登记的 Kind 和子运行自己发布的 `TaskStarted` 一致。

`withTaskParent` 是包内私有的 `RunOption`，把已经登记的子 TaskInfo 和父任务树带进子运行：

```go
func withTaskParent(parent *RunContext, child TaskInfo) RunOption {
	return func(rc *RunConfig) {
		rc.childTask = &child   // RunConfig 新增的未导出字段
		rc.task = parent.task   // RunConfig 新增的未导出字段
	}
}
```

`streamConfigured` 把这两个字段传给 `newRunHandleWithID`（可以加参数，也可以写入返回的 `Run`）。后者看到 `childTask` 时不再新建根 Task，直接用这份信息，并把 ThreadID 改成子运行自己的 ThreadID；TurnID 在 `prepare` 解析后填入。没有 `childTask` 时按根任务处理，从 Agent 配置读 `budget` 和 `taskKind`。整棵树共用一个 `taskRuntime`，所以子 Agent 自己的 `WithTaskBudget` 只在它作为根运行时生效，不覆盖父树的预算。

这里要把预算错误变成 `tool.ErrorResult`，而不是让整个父运行失败。模型看到“预算已用尽”后可以收敛回答；父运行是否失败由模型和中间件决定。

### 5.5 怎么从 `tool.Context` 找到父任务

`AsTool` 只拿到 `*tool.Context`，拿不到 `*RunContext`。现有代码已经有一个类似的可选能力：`tool.Updater`，由 `agent` 侧的包装 context 实现，见 [tool/tool.go:60](../../tool/tool.go#L60) 到 [tool/tool.go:62](../../tool/tool.go#L62)。

本文不新增公开的 tool 接口，而是在 `agent` 包内加一个私有载体：

```go
type runContextCarrier interface {
	agentRunContext() *RunContext
}
```

`callOne` 在构造 `tool.Context` 时使用的包装 context 实现这个方法，见 [agent/exectools.go:391](../../agent/exectools.go#L391) 到 [agent/exectools.go:398](../../agent/exectools.go#L398)。`AsTool` 做类型断言，拿不到就按“没有任务树”处理，使用默认预算新建一棵树。

这样做的原因：

- 不把 `*RunContext` 暴露到 `tool` 包，避免工具作者直接改运行环境。
- 不引入第二套全局注册表。
- 和现有 `Updater` 的可选能力模式一致。
- 直接调用 `AsTool` 的测试或脚本没有父运行，仍然可以工作，只是每次新建一棵任务树。

### 5.6 Task 事件

第一阶段只新增两个事件：

```go
type TaskStarted struct {
	TaskID   string
	ParentID string
	ThreadID string
	Kind     string
	Depth    int
}

type TaskDone struct {
	TaskID string
	Kind   string
	Status string // "done" | "failed" | "canceled" | "paused"
	Err    error
}
```

发布规则：

- 根 Task：由 `Run.drive` 在 `RunStarted` 之后发布 `TaskStarted`；在终结事件之前发布 `TaskDone`。
- 子 `Run`（`AsTool`）：由子运行自己的 `drive` 发布，父运行不重复发布。
- `Parallel` 分支、DAG 节点：没有独立 `Run` 句柄，由分支/节点启动点发布 `TaskStarted`，结果落定时发布 `TaskDone`。
- `transfer`：不发布新的 Task 事件，因为它没有新 Task。
- `Sequential` / `Loop` 子 Agent：不发布，因为共享父 Task。

`TaskDone.Status` 和 `runOutcome` 的映射：

| runOutcome | Status |
| --- | --- |
| `Err == nil` 且没有中断 | `done` |
| `Err != nil` | `failed` |
| `Control.Kind == Interrupt` | `paused` |
| 被兄弟分支取消 | `canceled` |

事件只做观察，不参与预算和合并。事件消费者不能用 `TaskStarted/TaskDone` 判断线程是否可写，仍然要看 claim 和 checkpoint。

### 5.7 Task 的持久化边界

本文不把 Task 写进 checkpoint。原因：

1. 现在没有跨进程查询 Task 的需求；跨进程查询 Run 已经有 RunRecord。
2. TaskID 是运行期生成的，写进快照会让恢复后的 TaskID 和新的运行期 ID 冲突。
3. TurnID 已经能回答“这一轮用户交互”，不需要再造一个持久主键。

如果以后需要“一个会话里的任务列表”，优先在 RunRecord 上加 `TaskKind`、`ParentRunID` 这类字段，或者用现有 Turns 查询，不新建 Task 表。这个决定和概念文档第 9 节的边界一致。

### 5.8 为什么不直接做 Codex 那种 Task 替换

Codex 的 Session 里只有一个活动 Task，新 Task 到达会中止旧 Task。goagent 不能直接照搬，原因：

- 旧运行可能已经执行了外部工具，无法回滚。
- 旧运行可能持有 claim 和写保护，强行替换会让 checkpoint 链断裂。
- goagent 的 HITL 暂停是有意结束一个 Run，再用新 Run 恢复；这和“替换任务”是两种语义。
- 现有设计已经明确“不要急着抽任务类型层级”，见 `codex-core-runtime-optimization-proposal.md` 第七节。

所以本文只加 Task 元数据和预算，不加替换。需要“忙时处理”时，用第 9 节的准入策略，不杀旧运行。

---

## 6. 优化一：并行工具上限

### 6.1 当前问题

当前并行段的上限等于调用数。模型返回多少可并行调用，就同时启动多少个。对下面几类工具，这会直接放大资源占用：

- 每个调用都会访问同一个限流 API。
- 每个调用都会写文件或数据库。
- 每个调用都会启动子进程。
- 每个调用都会再次调用模型（例如 `AsTool`）。

分段算法已经把串行工具隔离出来了，所以缺的只是并行段内部的名额。

### 6.2 API 与默认值

新增：

```go
// WithToolConcurrency 限制一个并行工具段内同时执行的调用数。
// n <= 0 表示不限制；默认 8。串行段不受影响。
func WithToolConcurrency(n int) Option
```

`config` 增加 `toolConcurrency int`，`AgentLoop` 增加同名字段，和 `toolExec`、`toolTimeout` 的传递方式一致，见 [agent/options.go:16](../../agent/options.go#L16) 到 [agent/options.go:45](../../agent/options.go#L45) 以及 [agent/loop.go:31](../../agent/loop.go#L31) 到 [agent/loop.go:49](../../agent/loop.go#L49)。

默认值放在 `agent.New` 的默认配置里，和 `maxTurns`、`contextOverflowRetry` 一起设置，见 [agent/agent.go:38](../../agent/agent.go#L38)。

关于 `0` 的语义：

- `WithToolConcurrency(0)` 表示不限制，用于显式恢复旧行为。
- 负数在 `agent.New` 时拒绝，避免“负数被静默当成 0”的歧义。
- 工作流 Agent 不走 `agent.New`，所以 `wrapWorkflowWithStore` 也要填入默认值。

### 6.3 执行算法

只在并行段里加名额，不改分段算法，也不改串行段。伪代码：

```go
func (l *AgentLoop) execTools(...) (...) {
	segments := buildToolCallSegments(...)

	for _, seg := range segments {
		if seg.serial {
			// 保持现状：检查写权限、发布 ToolStarted、同步执行
			continue
		}

		limit := l.toolConcurrency
		if limit <= 0 || limit > len(seg.indexes) {
			limit = len(seg.indexes)
		}
		slots := make(chan struct{}, limit)
		for i := 0; i < limit; i++ {
			slots <- struct{}{}
		}

		var wg sync.WaitGroup
		for _, i := range seg.indexes {
			select {
			case <-slots:
			case <-rc.Context.Done():
				recordRunErr(rc.Context.Err())
				dispatchStopped = true
			}
			if dispatchStopped {
				break
			}

			if err := rc.checkWriteAccess(); err != nil {
				slots <- struct{}{} // 没启动，归还名额
				recordRunErr(err)
				dispatchStopped = true
				break
			}

			rc.publish(core.ToolStarted{Call: calls[i]})
			wg.Add(1)
			go func(i int, c core.ToolCall) {
				defer wg.Done()
				defer func() { slots <- struct{}{} }()
				run(i, c)
			}(i, calls[i])
		}
		wg.Wait()
	}
}
```

三个关键点：

1. **名额在启动 goroutine 之前拿。** 这样 `ToolStarted` 只在调用真正被允许开始时发布。等待名额的调用不会提前出现在事件流里。
2. **名额在 `run` 返回时归还，不在 handler 真正退出时归还。** `run` 可能在工具超时后放弃等待，此时 handler 还在跑；名额必须归还，否则整个批次会被一个不听话的工具永久卡住。代价是上限只约束“循环还在等待的调用”，不约束底层 goroutine。
3. **拿名额的循环是同步的。** 因此同一段内 `ToolStarted` 仍然按模型调用顺序发布；完成顺序和 `ToolDone` 顺序仍然可以不同。串行段之前的并行段仍然要等 `wg.Wait`，串行调用不会和它们重叠。

### 6.4 事件顺序

现有测试固定了这些边界：

- 同一并行段内 `ToolStarted` 按调用顺序。
- 串行调用的 `ToolDone` 必须在下一个段的 `ToolStarted` 之前。
- `TurnDone` 每步只有一个，不按调用数增加。

加上限后：

- 同一并行段内 `ToolStarted` 仍然按调用顺序，但第 N+1 个调用可能在第 1 个调用 `ToolDone` 之后才 `ToolStarted`。
- `ToolDone` 的顺序本来就不保证，和上限无关。
- 串行段不变。
- 不新增事件。`ToolStarted` 的语义从“即将执行”收紧为“已经拿到名额、即将执行”。

如果 UI 需要区分“已排队但未开始”，不应靠 `ToolStarted`；后续可以单独加 `ToolQueued`，但不属于本文范围。

### 6.5 与超时、放弃、审批的关系

- **工具超时**：`callCtx` 取消后，`run` 通过 `awaitAbandoned` 返回，名额归还；handler 可能还在跑。和现在的 `abandonGrace` 语义一致。
- **整批取消**：`rc.Context` 取消时，等待名额的调用会走 `rc.Context.Done()` 分支；已经启动的调用收到自己的 `callCtx` 取消。不会因为拿不到名额而永远阻塞。
- **审批暂停**：审批发生在 `BeforeTool` 之前，工具批次还没进入 `execTools`；上限不影响审批。恢复批次执行时同样走上限。
- **`ToolSequential` / `SequentialTool`**：串行段只有一个调用，不需要名额；`ToolSequential` 的全局串行语义不变。
- **子 Agent 工具**：每个子 Agent 有自己的 `AgentLoop` 和自己的上限，不做跨任务树的全局名额。原因是父工具可能在等子 Agent，如果共享全局名额会死锁。需要全局工具上限时另做，不混在本文里。

### 6.6 测试与验收

必须覆盖：

1. `n=2`，5 个阻塞工具调用：任意时刻最多 2 个 handler 在跑；前 2 个 `ToolStarted` 后，第 3 个要等一个完成才出现。
2. `n=1`：并行段退化为按顺序执行，但 `ToolStarted` 仍然按调用顺序。
3. `n=0`：恢复旧行为，所有调用可以同时开始。
4. 串行工具夹在并行调用之间：串行调用仍然独占自己的段，前后并行段的峰值不超过上限。
5. 工具超时：一个 handler 不返回，另一个调用仍能拿到名额；批次不会永久阻塞。
6. 批次取消：等待名额的调用拿到 `context.Canceled` 结果或整批错误，不产生多余 `ToolStarted`。
7. 结果顺序：`results` 仍然按模型调用顺序排列，不按完成顺序。
8. 反向验证：把名额归还删掉，超时用例必须失败；把 `ToolStarted` 移到拿名额之前，事件顺序用例必须失败。

验收门禁：

- `go test -race ./agent` 通过。
- 现有分段事件顺序测试不改断言仍然通过（默认上限 8 大于测试中的 2 个调用）。
- 新增上限测试在 `-race` 下通过。

---

## 7. 优化二：子 Agent / 工作流预算

### 7.1 为什么需要树级预算

单层上限有三个漏洞：

1. `AsTool` 每个子 Agent 都有自己的循环，父层的工具上限管不到子层。
2. `Parallel` 的每个分支可以继续 `Parallel`，分支数按层乘。
3. DAG 节点可以调用 worker，worker 又可以 `AsTool`；计划自己的并发上限只约束当前计划。

所以预算必须挂在一棵任务树上，而不是挂在单个循环上。

### 7.2 预算的三个维度

| 维度 | 含义 | 默认 | 扣减时机 |
| --- | --- | --- | --- |
| 深度 `MaxDepth` | 子任务最多嵌套几层 | 8 | 创建子 Task 前检查 |
| 子任务数 `MaxUnits` | 一棵树最多创建多少个子 Task | 64 | 创建子 Task 时扣 |
| 模型调用数 `MaxTurns` | 一棵树最多发起多少次模型请求 | 0（不限） | 每次真正发起模型请求前扣 |

不把“并发数”放进 `TaskBudget`，原因见 7.6。并发由三个已有或新增的局部机制控制：

- 工具批次：第 6 节的 `WithToolConcurrency`。
- DAG 计划：已有的 `WithConcurrency`，默认 8。
- `Parallel`：新增 `ParallelOptions.MaxConcurrency`，0 表示不限制，建议按场景显式设置。

### 7.3 扣减 API

```go
type TaskBudgetError struct {
	Dimension string // "depth" | "units" | "turns"
	Limit     int
	Used      int
	ParentID  string
	Kind      TaskKind
}

func (e *TaskBudgetError) Error() string
func (e *TaskBudgetError) Unwrap() error { return ErrTaskBudgetExceeded }

var ErrTaskBudgetExceeded = errors.New("agent: task budget exceeded")
```

内部接口：

```go
func (rc *RunContext) admitChild(kind TaskKind) (TaskInfo, error)
func (rc *RunContext) consumeTurn() error
```

`admitChild`：

1. 读当前 TaskInfo.Depth。
2. 检查 `Depth+1 <= MaxDepth`。
3. CAS 循环增加 `task.units`；如果 `old+1 > MaxUnits`，直接返回 `TaskBudgetError{Dimension: "units"}`，不做自增。
4. 生成子 TaskInfo，写入父 TaskID、Kind、Depth+1；共享线程的派生再写 ThreadID/TurnID，`AsTool` 的子运行留给子运行填。
5. 不在这里等待并发名额；并发由调用方的局部机制控制。

`consumeTurn`：

1. 如果 `MaxTurns == 0`，直接返回 nil。
2. CAS 增加 `task.turns`，超过上限返回 `TaskBudgetError{Dimension: "turns"}`。
3. 在 `runStep` 真正发起模型请求之前调用；被抢占、未真正发出的请求不应扣。具体位置是构造完请求、准备调用模型时，而不是在 `PrepareTurn` 开始时。

预算错误是硬错误，不是“静默跳过”。调用方必须决定怎么处理。

### 7.4 各接入点

#### 7.4.1 `AsTool`

`AsTool.Call` 在 [agent/subagent.go:38](../../agent/subagent.go#L38) 执行。接入后：

1. 从 `tool.Context` 找到父 `RunContext`。
2. `admitChild(s.child.taskKind())`。
3. 把子 TaskInfo 和共享的 `taskRuntime` 作为不导出的 RunOption 传给 `child.Run`。
4. 如果预算不足，返回 `tool.ErrorResult`，让模型看到原因。
5. 如果子运行成功，正常返回最终文本。

子运行内部的 `TaskKind` 用子 Agent 自己的类型（普通 Agent 是 `chat`，工作流是 `workflow`，计划是 `plan`），`ParentID` 指向父 Task。这样事件里既能看出它是被派生的，也能看出它执行的是什么。

#### 7.4.2 `Parallel`

`Parallel` 在 [agent/workflow.go:141](../../agent/workflow.go#L141) 创建分支。`forBranch` 内部调用 `admitChild`：

- 预算检查在启动每个分支前进行。某个分支预算不足时，不再启动后续分支，并取消已经启动的分支；已经启动的分支可能已经执行了一部分工作，不能假设“零副作用”。
- 所有分支共享同一个 `taskRuntime`，所以总子任务数是全树累计。
- 分支内部的 `AsTool`、嵌套 `Parallel` 继续从这棵树扣。

`ParallelOptions` 增加：

```go
type ParallelOptions struct {
	StateConflict  StateConflictPolicy
	Failure        FailurePolicy
	MaxConcurrency int // 0 = 不限制；>0 = 同时执行的分支上限
}
```

`MaxConcurrency` 只限制同时执行的分支数。分支在获得执行名额时才调用 `forBranchWithContext`，所以排队等待的分支还没有创建子环境，也没有扣子任务额度；这不改变全树的总预算，只改变扣减发生的时刻。`Failure` 的取值和取消语义见 8.2。

#### 7.4.3 DAG 计划

`execNode` 在 [agent/plan.go:385](../../agent/plan.go#L385) 用 `rc.subRun` 创建节点环境。`subRun` 接入 `admitChild` 后：

- 节点在启动前扣子任务额度。
- planner 和 replanner 也走 `subRun`，因此同样计数。
- 节点重试会再次调用 `execNode`，每次重试都扣一个子任务（重试是新的执行）。
- 如果 `admitChild` 返回预算错误，`applyResult` 应按 `FailFast` 处理；`ContinueOnError` 下也不能无限重试同一个预算错误，应该在第一次预算错误后停止该节点并跳过依赖。

计划自己的 `WithConcurrency` 仍然生效；预算只负责总数和深度，不替代并发上限。

#### 7.4.4 transfer

`transfer` 使用 [agent/transfer.go:39](../../agent/transfer.go#L39) 的 `rc.deeper()`，不创建新的 Task，也不扣 `MaxUnits`。它仍然受 `maxTransferDepth` 约束。

如果以后希望 transfer 也计入总深度，可以把 `maxTransferDepth` 合并进 `TaskBudget.MaxDepth`，但本文不合并，避免一次改动同时改两套语义。

### 7.5 预算耗尽后的行为

| 场景 | 行为 |
| --- | --- |
| `AsTool` 预算不足 | 返回 `tool.ErrorResult`，父循环继续；模型可以调整策略。 |
| `Parallel` 启动分支时预算不足 | 不再启动后续分支，取消已启动分支，整个工作流返回 `TaskBudgetError`。 |
| DAG 节点预算不足，`FailFast` | 节点标记失败，计划停止并取消在途节点（见第 8 节）。 |
| DAG 节点预算不足，`ContinueOnError` | 节点标记失败，依赖它的节点跳过；不重试预算错误。 |
| 模型调用预算不足 | 当前步骤返回错误；已经完成的结果保留在 checkpoint 中，可由调用方决定是否 Resume。 |

预算耗尽后，已经启动的子任务不会被强制终止；调用方可以选择继续等待、取消父 context，或者让工作流按失败策略收尾。

### 7.6 为什么不做共享并发信号量

一个自然的想法是：整棵任务树共享一个 `MaxConcurrentChildren` 信号量。本文不做，原因是死锁：

```text
父任务持有 1 个名额，正在等子任务
子任务要再拿 1 个名额才能启动
如果 MaxConcurrentChildren = 1，子任务永远拿不到
```

可以选择“父任务在等子任务时释放名额”或“按深度预留名额”，但这会让预算语义变得难解释，也会和工具上限、Plan 并发上限互相影响。本文改成：

- 深度限制防止无限嵌套。
- 子任务总数限制整棵树的规模。
- 工具上限、Plan 并发、`ParallelOptions.MaxConcurrency` 分别约束每一层的并发。

这样没有跨层阻塞，也不需要在父任务等待时做名额转让。

### 7.7 测试与验收

1. `AsTool` 嵌套 9 层（默认深度 8）：第 9 层返回预算错误，前 8 层正常结束。
2. 一个 `Parallel` 有 5 个分支，每个分支再 `AsTool`：总子任务数按 5 + 5 累计，超过 `MaxUnits` 时后续启动被拒绝。
3. `MaxUnits=1` 时，第二个 `AsTool` 返回 `tool.ErrorResult`，父运行不一定失败。
4. DAG 节点重试两次后触发预算上限：重试次数和预算计数一致，不会无限重试。
5. `MaxTurns=3`：第三次模型请求后下一次请求返回预算错误；暂停恢复会重新开一棵预算树（见 7.8）。
6. `transfer` 不增加 Task 深度，但仍然受 `maxTransferDepth` 限制。
7. 反向验证：把 `admitChild` 的 CAS 改成普通自增并去掉上限检查，嵌套用例必须失败；把预算错误改成成功返回，`Parallel` 用例必须失败。

### 7.8 预算和暂停恢复

预算属于 `RunContext`，不写 checkpoint。暂停后恢复会产生新的 `Run` 和新的 `taskRuntime`，预算从默认值重新开始。这是有意的：

- 暂停可能持续很久，保留旧的计数器没有意义。
- 恢复是新的执行尝试，应该重新受预算保护。
- 如果调用方需要跨恢复的总预算，应在应用层记录，不要把计数器塞进 State。

这个限制要写进 README 和 `WithTaskBudget` 的注释。

---

## 8. 优化三：失败时取消兄弟分支

### 8.1 当前行为

`Parallel`：

- 所有分支启动。
- 等所有分支结束。
- 按声明顺序找第一个错误或控制结果。
- 如果有错误，不合并任何分支的 KV 和文本。

`Plan` 的 `FailFast`：

- 第一个节点失败后不再启动新节点。
- 已经在运行的节点继续跑。
- 每个失败都会覆盖 `failFastErr`，最终错误可能是最后一个失败，而不是根因。

### 8.2 策略

`ParallelOptions` 增加：

```go
type FailurePolicy int

const (
	// CancelSiblings 在第一个真实失败出现时取消其他分支。
	CancelSiblings FailurePolicy = iota
	// WaitForAll 保留旧行为：等所有分支结束再合并/报错。
	WaitForAll
)
```

默认用 `CancelSiblings`。`Parallel` 构造器和 `ParallelWithOptions` 的零值都表示它。需要旧行为的调用方显式传 `WaitForAll`。

`Plan` 不新增策略：`FailFast` 的语义改为“不再启动新节点，并取消在途节点”；`ContinueOnError` 不变。

### 8.3 `Parallel` 算法

```go
func (p *parallelRunner) run(rc *RunContext) runOutcome {
	baseline := cloneKV(rc.State.KV)

	branchCtx, cancelBranches := context.WithCancel(rc.Context)
	defer cancelBranches()

	limit := p.opts.MaxConcurrency
	if limit <= 0 || limit > len(p.subs) {
		limit = len(p.subs)
	}

	results := make(chan branchResult, len(p.subs))
	children := make([]*RunContext, len(p.subs))
	var collected []branchResult
	var failure, launchErr error
	next, inflight := 0, 0

	for next < len(p.subs) || inflight > 0 {
		// 1. 有名额、没有停止时，按声明顺序继续启动。
		for next < len(p.subs) && inflight < limit && launchErr == nil && branchCtx.Err() == nil &&
			(failure == nil || p.opts.Failure == WaitForAll) {
			i := next
			child, err := rc.forBranchWithContext(branchCtx, p.subs[i].cfg.name, p.subs[i].taskKind())
			if err != nil {
				launchErr = err
				cancelBranches()
				break
			}
			children[i] = child
			next++
			inflight++
			go func(i int, child *RunContext) {
				out := runChild(child, p.subs[i].runnable)
				results <- branchResult{index: i, out: out, canceled: branchCtx.Err() != nil}
			}(i, child)
		}

		if inflight == 0 {
			break
		}

		// 2. 收一个结果，腾出名额；真实失败会停止后续启动。
		r := <-results
		inflight--
		collected = append(collected, r)
		if failure == nil && isRealFailure(r.out) {
			failure = r.out.Err
			if p.opts.Failure == CancelSiblings {
				cancelBranches()
			}
		}
	}

	if failure != nil || launchErr != nil || branchCtx.Err() != nil {
		return runOutcome{Err: chooseRootError(collected, branchCtx, launchErr)}
	}
	outs := make([]runOutcome, len(p.subs))
	for _, r := range collected {
		outs[r.index] = r.out
	}
	return p.merge(rc, baseline, children, outs)
}
```

辅助类型：

```go
type branchResult struct {
	index    int
	out      runOutcome
	canceled bool // 分支返回时，分支 context 是否已经取消
}

func isRealFailure(out runOutcome) bool { return out.Err != nil }
```

要点：

1. **取消的只是分支 context，不是父 `rc.Context`。** 父运行仍然可以收尾、写检查点、发布终结事件。
2. **仍然等待所有已启动的分支返回。** 取消是协作式的，不能假设分支立刻退出；循环必须在所有已启动 goroutine 结束后才能安全读取它们的 State。`CancelSiblings` 下没有启动的分支不会在失败后补发。
3. **不合并失败运行。** 只要存在真实失败，就返回错误，不应用任何分支的 KV，不拼接文本。
4. **HITL 暂停不算失败。** `Control.Kind == Interrupt` 不触发取消；暂停需要保留其他分支的结果和 checkpoint。`Stop`、`Escalate` 也不触发取消，保持现有控制语义。
5. **只有 `CancelSiblings` 才取消。** `WaitForAll` 只是等所有分支结束，然后按旧逻辑选错误；它不会因为一个分支失败就停止启动后续分支。
6. **启动和收结果在同一个循环里交错。** `MaxConcurrency` 通过限制 `inflight` 实现。`CancelSiblings` 下，一个分支失败后会停止启动后续分支；不设上限时，所有分支在收集第一个结果前就已启动，这是旧行为，但失败仍会在结果到达时尽早取消剩余分支。

### 8.4 根因选择

取消会产生 `context.Canceled` 或包装了它的错误。不能让这些错误覆盖根因。`chooseRootError(collected, branchCtx, fallback)` 的规则（`branchCtx` 派生自 `rc.Context`，父 context 取消时它的 `Err()` 就是父错误）：

1. 如果父 `rc.Context` 已经取消，返回父 context 的错误；这是外部取消，不是分支失败。
2. 否则，收集所有 `out.Err != nil` 且不是取消错误的结果。
3. 按分支声明顺序选择最小的索引作为根因；这样即使完成顺序不稳定，最终错误也是确定的。
4. 如果所有错误都是取消导致的，但父 context 没有取消，返回第一个取消错误（通常是分支自己的工具或中间件取消了 context）。
5. 如果没有任何分支错误（例如某个分支在启动前就因预算或克隆失败），返回 `fallback`；取消错误不能覆盖它。正常失败路径的 `fallback` 是 nil。
6. 如果根因分支有 `Control`，优先返回错误；错误和暂停同时出现时，以错误为准，因为暂停无法安全恢复一个已经失败的分支。

“取消错误”的判定不能只看 `errors.Is(err, context.Canceled)`，因为有些工具会把取消包装成自己的错误。建议在 `branchResult` 里记录“这个分支的结果是在分支 context 已取消之后返回的”，再结合 `errors.Is` 判断。这样能减少误判，但不能做到绝对精确；文档要说明这个限制。

### 8.5 `Plan` 算法

在 `planRunner.run` 里增加一个只在 `FailFast` 时创建的节点 context：

```go
nodeCtx := rc.Context
var cancelNodes context.CancelFunc
if p.cfg.policy == FailFast {
	nodeCtx, cancelNodes = context.WithCancel(rc.Context)
	defer cancelNodes()
}
```

启动节点时把 `nodeCtx` 传给 `execNode`，`execNode` 用 `rc.subRunWithContext(nodeCtx, input, w.taskKind())`。

`applyResult` 按下面的顺序处理：

1. 节点成功：标记 `done`，写输出，行为不变。
2. 节点失败，但计划已经停止（`failFastErr` 已设置，或错误由已经取消的 `nodeCtx` 导致）：
   - 取消导致的错误：标记 `canceled`，发布 `PlanNodeDone{Status: "canceled"}`。
   - 其他错误：标记 `failed`，发布 `PlanNodeDone{Status: "failed"}`，但不覆盖 `failFastErr`。
   - 两种情况都不重试。
3. 节点失败，计划仍在运行：
   - 如果错误不可重试（目前是 `TaskBudgetError`），直接进入第 4 步，不消耗 `MaxRetries`。
   - 如果 `st.Attempts[id] < node.MaxRetries`，增加次数并标记 `pending`，发布 `retry`；这是原有的重试语义，`FailFast` 下也先重试。
   - 否则进入第 4 步。
4. 节点最终失败：
   - 标记 `failed`，发布 `PlanNodeDone{Status: "failed"}`。
   - `FailFast` 且 `failFastErr` 尚未设置时，设置 `failFastErr` 并调用 `cancelNodes()`；已经设置时只记录当前节点失败，不覆盖根因。
   - `ContinueOnError` 下不设置 `failFastErr`，也不取消其他节点。

关键点：`FailFast` 只在“一个节点重试耗尽”或“遇到不可重试错误”时取消兄弟节点，不在第一次瞬时失败时取消。一次会成功的重试不该打断其他节点。

调度循环的其它部分不变：

- `failFastErr != nil` 时不再启动新节点。
- 继续从 `results` 收在途节点。
- 每收一个结果仍然调用 `save`，保证暂停/失败边界可恢复。
- `inflight == 0` 时返回 `failFastErr`。

`PlanNodeDone` 的合法状态从 `"done" | "failed" | "skipped" | "retry"` 扩展为再加 `"canceled"`，同步更新 [core/event.go:96](../../core/event.go#L96) 到 [core/event.go:102](../../core/event.go#L102) 的注释。

### 8.6 取消的边界

- **不取消父运行。** 父运行要负责收尾、写 checkpoint、发布 `RunFailed`。
- **不保证回滚。** 已经写入外部系统的工具副作用不会消失；已经提交的 checkpoint 也不会删除。取消只阻止后续步骤和未开始的工具。
- **不保证立即返回。** 工具不检查 context 时，分支会继续运行；`Parallel` 会等它结束。只有工具超时、调用方 context 或工具自己的协作能让它提前退出。
- **不影响 HITL 暂停。** 暂停不是失败，不能被兄弟分支的失败取消掉，否则审批状态会丢。
- **和检查点共享写入锁。** 分支和父运行共享 `durableRunState` 的 `lineageMu`，见 [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339) 到 [agent/durable_runtime.go:353](../../agent/durable_runtime.go#L353)。取消不会绕过这个锁；正在提交的分支会完成当前提交再返回。
- **工具结果未完成。** 如果取消发生在工具已经 claim 但还没写回结果的时刻，checkpoint 里可能留下未完成或未知的工具记录。恢复时仍然走现有 `ErrToolOutcomeUnknown` 和恢复策略，不新增特殊处理。

### 8.7 事件

新增：

```go
type BranchCanceled struct {
	Name   string
	Reason error
}
```

`Parallel` 在取消兄弟分支时发布，每个被取消的分支一条。`Plan` 用 `PlanNodeDone{Status: "canceled"}`，不重复发 `BranchCanceled`。

根因仍然通过 `RunFailed.Err` 和 `PlanNodeDone{Status:"failed"}` 表达。`BranchCanceled` 只用于观察，不参与错误选择。

### 8.8 测试与验收

1. 分支 A 立刻失败，分支 B 阻塞在工具里并监听 context：B 收到取消，`Parallel` 在合理时间内返回 A 的错误。
2. 分支 A 失败，分支 B 的工具不监听 context：B 仍然跑完；最终错误仍然是 A，不会变成 B 的取消错误。
3. 两个分支几乎同时失败：最终错误按声明顺序确定，测试不依赖完成顺序。
4. HITL 暂停：一个分支暂停，另一个分支继续完成；`Parallel` 返回暂停，不取消兄弟分支。
5. `WaitForAll`：分支 A 失败后 B 仍然完整跑完，最终错误选择保持旧行为。
6. Plan `FailFast`：两个节点在跑，一个节点重试耗尽后最终失败；另一个收到取消，状态为 `canceled`，最终错误是失败节点，不被覆盖。
7. Plan `FailFast` 重试：节点 A 第一次失败但还有重试次数，不应取消节点 B；A 重试成功后计划继续。
8. Plan `ContinueOnError`：失败节点不影响独立分支，不取消任何节点。
9. 反向验证：删除 `cancelNodes()` 调用，取消用例必须在超时内失败；把 `failFastErr` 改回“每次覆盖”，根因用例必须失败；把 `TaskBudgetError` 当成可重试错误，预算用例必须超时或重试次数超标。

---

## 9. 优化四：同线程准入与默认协调

### 9.1 当前协议

同线程写入的权威协议是：

1. `BeginRun` 检查活动 claim。
2. 暂停线程返回 `ErrResumeRequired`，必须走 `Resume`。
3. claim 有租约，写保护每半个租约续租一次。
4. 提交用 revision 和 fencing token 防止旧持有者写入。

这套协议解决的是“谁能写”。它不解决“第二个请求在进程内应该等、失败还是合并输入”。当前只有失败。

### 9.2 准入层的位置

在 `Agent` 上增加一个进程内 `threadGate`，位置在 `Run.prepare` 调 `BeginRun` 之前：

```text
Session.Send / Agent.Run / Resume
  └─ threadGate.Acquire(threadID, policy)
       ├─ 空闲：立即拿到名额
       ├─ 忙 + Fail：返回忙错误
       └─ 忙 + Queue：进入 FIFO 等待
            └─ 拿到名额
                 └─ BeginRun / Claim（持久 claim 仍然是正确性来源）
                      └─ 执行
                           └─ 释放名额（终结或取消）
```

`threadGate` 只在一个 `Agent` 实例内共享。两个 `Agent` 共享同一个 Store 时，进程内的 gate 不共享；跨进程更不共享。它们最终仍然由 `BeginRun` 的 claim 冲突兜底。

### 9.3 策略类型

```go
type AdmissionPolicy int

const (
	// AdmissionFail 在发现同线程已有活动运行时立即失败。
	AdmissionFail AdmissionPolicy = iota
	// AdmissionQueue 在发现同线程已有活动运行时进入 FIFO 等待。
	AdmissionQueue
)

type AdmissionOptions struct {
	Policy     AdmissionPolicy
	MaxWaiters int // 每个线程最多排多少个；默认 16
}

// WithAdmission 设置本次 Run/Stream 的准入策略；Session.Send 也接受它。
func WithAdmission(o AdmissionOptions) RunOption
```

默认值：

| 入口 | 默认策略 | 理由 |
| --- | --- | --- |
| `Session.Send` | `AdmissionQueue` | Session 是会话入口，同一会话的两个 Send 排队比直接失败更符合调用方预期。 |
| `Agent.Run` / `Agent.Stream` | `AdmissionFail` | 这是显式执行入口，调用方通常希望立刻知道线程忙。 |
| `Agent.Resume` | `AdmissionFail` | 恢复是明确的继续操作；如果线程正忙，先让调用方处理，避免拿着 claim 排队。 |
| 用户显式 `WithAdmission` | 按配置 | 覆盖上面的默认（只适用于 `Run` / `Stream` / `Session.Send`；本批 `Resume` 不支持按次覆盖）。 |

入口默认值不能写死在 gate 里，否则 Session 和 Agent 没法不同。做法是让 `RunConfig` 带一个未导出的“是否显式设置”标记：

```go
type RunConfig struct {
	// ...既有字段
	admission    AdmissionOptions
	admissionSet bool
}

func WithAdmission(o AdmissionOptions) RunOption {
	return func(r *RunConfig) { r.admission, r.admissionSet = o, true }
}
```

`resolveRunConfig` 把默认值填成 `AdmissionFail` + `MaxWaiters: 16`；`Session.Send` 在调用 `streamConfigured` 之前检查 `admissionSet`，未显式设置时改成 `AdmissionQueue`。`Agent.Run` / `Agent.Stream` 直接使用 `resolveRunConfig` 的结果，所以保持 `AdmissionFail`。选定后的 `AdmissionOptions` 由 `streamConfigured` 写进 `Run` 的未导出字段，`Run.prepare` 和 `Run.Cancel` 都从那里读。`Resume` 不经过 `RunConfig`，本批固定使用 `AdmissionFail`，不提供按次覆盖；以后需要时再加 `ResumeOption`，不要改动现有 `Resume(ctx, threadID, approvals...)` 的变参签名。`MaxWaiters <= 0` 一律归一化为 16，所以 gate 内部不需要再判断“0 是否等于不限制”。

这个默认分工是本文的推荐方案。它保留 `Agent.Run` 现有的 `ErrClaimConflict` 行为，只改变 `Session.Send`。如果团队更希望全局默认排队，只需要把 `Session.Send` 的默认上移到 `Agent`，但要把 README 和测试一起改。

### 9.4 gate 的数据结构

```go
type threadGate struct {
	mu         sync.Mutex
	busy       map[string]bool
	waiters    map[string][]*gateWaiter
	maxWaiters int
}

type gateWaiter struct {
	ready   chan struct{}
	granted bool
}
```

获取：

```go
func (g *threadGate) acquire(ctx context.Context, threadID string, policy AdmissionPolicy) (release func(), err error) {
	g.mu.Lock()

	if !g.busy[threadID] {
		// 队列里有等待者时，即使当前空闲也要排队，保证 FIFO。
		if policy == AdmissionQueue && len(g.waiters[threadID]) > 0 {
			return g.enqueueLocked(ctx, threadID)
		}
		g.busy[threadID] = true
		g.mu.Unlock()
		return g.releaser(threadID), nil
	}

	if policy == AdmissionFail {
		g.mu.Unlock()
		return nil, fmt.Errorf("agent: thread %q is busy: %w", threadID, checkpoint.ErrClaimConflict)
	}

	return g.enqueueLocked(ctx, threadID)
}
```

排队：

```go
func (g *threadGate) enqueueLocked(ctx context.Context, threadID string) (func(), error) {
	if g.maxWaiters > 0 && len(g.waiters[threadID]) >= g.maxWaiters {
		g.mu.Unlock()
		return nil, ErrAdmissionQueueFull
	}
	w := &gateWaiter{ready: make(chan struct{})}
	g.waiters[threadID] = append(g.waiters[threadID], w)
	g.mu.Unlock()

	select {
	case <-w.ready:
		return g.releaser(threadID), nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			// 已经轮到它了，但调用方先取消了：把名额让给下一个。
			g.mu.Unlock()
			g.release(threadID)
			return nil, ctx.Err()
		}
		removeWaiter(g.waiters[threadID], w)
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}
```

释放：

```go
func (g *threadGate) release(threadID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for len(g.waiters[threadID]) > 0 {
		w := g.waiters[threadID][0]
		g.waiters[threadID] = g.waiters[threadID][1:]
		if w.granted {
			continue
		}
		w.granted = true
		close(w.ready)
		return // busy 保持 true，名额直接转交
	}
	g.busy[threadID] = false
}
```

关键点：

- 释放时如果队列里有等待者，`busy` 保持 true，名额直接转交，避免“刚释放又被新来的抢占”。
- 等待者取消后，如果它已经被授予名额，必须把名额继续转交，不能丢掉。
- `release` 用 `sync.Once` 包装，保证每个运行只释放一次。
- 每个线程一个队列；不同线程互不阻塞。

### 9.5 获取和释放的时机

#### 9.5.1 普通运行

`Run.prepare` 在 `BeginRun` 之前获取：

```go
func (r *Run) prepare() (err error) {
	if release, err := r.acquireAdmission(); err != nil {
		return err
	} else {
		r.gateRelease = release
	}
	defer func() {
		if err != nil {
			r.releaseAdmission()
		}
	}()

	// 原有逻辑：BeginRun、恢复 checkpoint、启动写保护
}
```

`Run.drive` 在所有出口调用 `releaseAdmission`。包括：

- `startErr != nil`（没有获取过名额，释放是空操作）。
- `prepare` 失败。
- 运行正常结束、失败、暂停。
- `Run.Cancel` 在驱动前取消一个已经准备好的 Resume。

#### 9.5.2 Resume

`Resume` 在 `durable.Claim` 之前获取名额，见 [agent/hitl.go:79](../../agent/hitl.go#L79) 到 [agent/hitl.go:113](../../agent/hitl.go#L113)：

1. `threadGate.Acquire`。
2. `store.Latest` 和暂停校验。
3. `durable.Claim`。
4. 构造 `Run`，把名额释放函数挂到 `Run` 上。
5. 如果 `Run` 一直没有被驱动，`Cancel` 必须同时释放 claim 和名额。

为什么要先拿名额再 claim：

- 如果先 claim 再排队，Resume 会在等待期间持有一个不续租的 claim，租约会过期，其他进程可能抢走。
- 先排队不持有 claim，失败或取消的代价小。
- 拿到名额后再 claim，仍然可能被跨进程 claim 冲突挡住；这时释放名额并返回错误。

#### 9.5.3 排队等待与取消

- 调用方的 context 取消时，gate 返回 `ctx.Err()`，运行失败，不产生 checkpoint。
- 等待期间不发布 `RunStarted` 之外的模型/工具事件；`RunStarted` 已经在 `drive` 开始时发布，事件消费者会看到“已启动但未准入”。如果要更精确，可以新增 `RunQueued` 事件，见 9.7。
- 排队时间不受 `RunBudget.MaxDuration` 约束，因为运行预算从实际执行开始计算；调用方要用自己的 context 控制等待上限。

### 9.6 跨进程和第三方 store

- gate 只在进程内有效。另一个进程同时写同一线程时，`BeginRun` 或 `Claim` 仍然返回 `ErrClaimConflict`。
- 第三方 store 没有实现 `ThreadCoordinator` 时，gate 仍然能在本进程内串行化同一 Agent 的普通运行；但跨进程写入没有保证。`WithThreadCoordinationRequired(true)` 仍然用于在 `agent.New` 时强制要求协调能力。
- 不建议把 gate 当成 claim 的替代品。gate 进程崩溃就丢，claim 和 revision 仍然决定磁盘上的历史。
- File store 的跨进程 claim 已经有 journal 和 fencing token，见 [checkpoint/durable.go:15](../../checkpoint/durable.go#L15) 到 [checkpoint/durable.go:55](../../checkpoint/durable.go#L55)；准入层不修改这些。

### 9.7 重入和死锁

最危险的情况是：一个工具在运行中调用同一个 Agent 的 `Run`，并且指定同一个线程。

- 旧行为：`BeginRun` 返回 `ErrClaimConflict`。
- 新行为：如果父运行已经持有 gate，子运行会排队等自己，永远不返回。

必须在获取 gate 之前检测重入：

```go
type threadScopeKey struct{}

type threadScope struct {
	threadID string
}

func withThreadScope(ctx context.Context, threadID string) context.Context {
	return context.WithValue(ctx, threadScopeKey{}, threadScope{threadID: threadID})
}

func checkReentrant(ctx context.Context, threadID string) error {
	if s, ok := ctx.Value(threadScopeKey{}).(threadScope); ok && s.threadID == threadID {
		return ErrAdmissionReentrant
	}
	return nil
}
```

`newRunHandle` 在创建运行 context 时写入 scope。`prepare` 和 `Resume` 在获取 gate 前检查。

这样：

- 同线程重入立即返回 `ErrAdmissionReentrant`，不会挂死。
- 子 Agent 用新线程不受影响。
- 不同 Agent、同一线程的重入也会被检测到，因为 scope 只认线程 ID。

新增错误：

```go
var ErrAdmissionQueueFull = errors.New("agent: same-thread admission queue is full")
var ErrAdmissionReentrant = errors.New("agent: reentrant run on the same thread")
```

忙错误通过 `fmt.Errorf(..., %w, checkpoint.ErrClaimConflict)` 包装，这样现有 `errors.Is(err, checkpoint.ErrClaimConflict)` 判断仍然有效。

### 9.8 事件与可观测性

建议新增：

```go
type RunQueued struct {
	RunID    string
	ThreadID string
	Waiters  int
}

type RunAdmitted struct {
	RunID    string
	ThreadID string
	Waited   time.Duration
}
```

发布规则：

- `RunQueued`：进入等待队列时发布，`Waiters` 是当前队列长度（含自己）。
- `RunAdmitted`：拿到名额时发布；如果无需等待，`Waited` 为 0。
- 这两个事件不替代 `RunStarted`；`RunStarted` 仍然在 `drive` 开始时发布。

如果不想增加两个事件，也可以先只加日志。但 UI 需要区分“排队中”和“准备中”，事件更直接。第一阶段可以先不加，等 P1-7 的运行登记表一起做。

### 9.9 测试与验收

1. 同一个 `Session` 并发两次 `Send`：两个运行按 FIFO 执行，第二个看到第一个提交后的 head。
2. 同一个 `Agent` 的两个 `Session` 指向同一线程：仍然排队，因为 gate 挂在 Agent 上。
3. `AdmissionFail`：第二个运行立即返回包装了 `checkpoint.ErrClaimConflict` 的错误。
4. 队列满：第 `MaxWaiters+1` 个等待返回 `ErrAdmissionQueueFull`。
5. 等待者 context 取消：从队列移除，不执行，不产生 checkpoint；名额转交给下一个。
6. 第一个运行暂停：释放 gate；第二个运行可以开始，随后 `BeginRun` 返回 `ErrResumeRequired`，不会被 gate 挡住。
7. Resume 在活动运行期间调用：固定快速失败，不进入队列；失败发生在 `Claim` 之前，不持有 claim。
8. 同线程重入：工具内调用同一 Agent 的同线程运行，返回 `ErrAdmissionReentrant`，不挂死。
9. 不同线程：互不阻塞。
10. 跨进程：两个进程同时写同一线程，仍然有一方拿到 `ErrClaimConflict`；gate 不改变这个结果。
11. 反向验证：删掉 release 的“名额转交”逻辑，FIFO 用例必须挂起或失败；删掉重入检查，重入用例必须超时。

---

## 10. 组合后的不变量

实现完成后，下面这些必须一直成立：

1. 持久 claim 仍是跨进程写入的唯一正确性来源；gate 和预算不参与磁盘提交的裁决。
2. 每个子 Task 都有父 TaskID 和 Depth；Depth 严格等于父 Depth 加 1。
3. `transfer` 不创建新 Task，不增加 Task 深度；它仍受 `maxTransferDepth` 约束。
4. 一个子任务额度只在创建子环境成功时扣一次；已创建的子任务在失败和取消后不退还算额度。`admitChild` 因超限而没有执行的那次自增不算创建成功。
5. `MaxTurns` 只在模型请求真正发出前扣；被抢占或未发出的请求不扣。
6. 当 `WithToolConcurrency > 0` 时，工具并行段的在途等待数不超过该值；串行段不受影响。
7. 工具名额在循环停止等待时归还，包括超时和放弃；不要求底层 handler 已经退出。
8. 失败取消只取消兄弟分支的 context，不取消父运行。
9. `Parallel` 只有在所有分支都正常完成时才合并 KV 和文本；失败或暂停都不合并。
10. 根因错误不会被取消错误覆盖；`Parallel` 的根因按声明顺序确定，`Plan` 的根因只设置一次。
11. HITL 暂停不是失败，不触发兄弟取消。
12. gate 等待不持有 claim；Resume 先拿 gate 再 claim。
13. 同一线程的重入运行立即失败，不进入队列。
14. 一次顶层运行仍然只有一个 `RunStarted` 和一个终结事件；新增事件不改变这个基数。
15. 任务预算不写 checkpoint；暂停恢复重新开始预算。

---

## 11. 事件与错误清单

### 11.1 新增错误

| 错误 | 产生位置 | 调用方处理 |
| --- | --- | --- |
| `ErrTaskBudgetExceeded` | `admitChild`、`consumeTurn` | `AsTool` 转成工具错误；工作流/计划按失败策略处理。 |
| `ErrAdmissionQueueFull` | gate 队列满 | 返回给调用方；可以重试或改用 `AdmissionFail`。 |
| `ErrAdmissionReentrant` | 同线程重入 | 返回给工具；工具应改用新线程或子 Agent。 |
| `checkpoint.ErrClaimConflict`（包装） | `AdmissionFail` 或跨进程冲突 | 保持现有 `errors.Is` 判断。 |

### 11.2 新增事件

| 事件 | 发布者 | 用途 |
| --- | --- | --- |
| `TaskStarted` | 根 Run、子 Run、Parallel/Plan 启动点 | 任务树和预算观察。 |
| `TaskDone` | 同上 | 任务结束状态。 |
| `BranchCanceled` | `Parallel` | 兄弟分支被取消。 |
| `RunQueued` / `RunAdmitted` | gate（可选，第一阶段可只记日志） | 准入等待观察。 |

### 11.3 需要更新的现有注释和文档

- [core/event.go:96](../../core/event.go#L96) 的 `PlanNodeDone` 状态列表加 `"canceled"`。
- [agent/options.go:109](../../agent/options.go#L109) 的 `WithToolExecution` 注释说明并行段还受 `WithToolConcurrency` 限制。
- [agent/workflow.go:28](../../agent/workflow.go#L28) 到 [agent/workflow.go:33](../../agent/workflow.go#L33) 的 `Parallel` 注释说明失败取消策略。
- [agent/runtime.go:80](../../agent/runtime.go#L80) 和 [agent/runtime.go:101](../../agent/runtime.go#L101) 的派生环境注释说明 Task 深度和预算。
- [README.md:291](../../README.md#L291) 到 [README.md:293](../../README.md#L293) 的同线程段落，补充 Session 排队和 Agent 快速失败的区别。
- `docs/analysis/tool-call-concurrency-design.md` 加一段“上限”说明，指向本文，不重写分段设计。

---

## 12. 兼容性与迁移

### 12.1 默认行为变化

| 变化 | 影响 | 迁移方式 |
| --- | --- | --- |
| 并行工具默认最多 8 个 | 高峰并发下降，批次总耗时可能上升 | `WithToolConcurrency(0)` 恢复不限制；或调到合适值。 |
| `Parallel` 默认取消兄弟分支 | 失败时其他分支可能提前结束 | `ParallelOptions{Failure: WaitForAll}` 恢复旧行为。 |
| `Plan` 的 `FailFast` 取消在途节点 | 失败时节点状态可能变成 `canceled` | 需要等所有节点的调用方改用 `ContinueOnError` 或自行收尾。 |
| `Session.Send` 默认排队 | 同一 Session 的并发 Send 不再立即报错 | `WithAdmission(AdmissionOptions{Policy: AdmissionFail})` 恢复快速失败。 |
| 默认任务预算 | 超过深度 8 或子任务 64 的树会失败 | `WithTaskBudget` 调大；0 字段表示默认，需要明确的不限制语义时用 `MaxTurns=0` 和足够大的深度/子任务数。 |

### 12.2 API 兼容

- 新增 Option 和字段都是追加，不删除现有 API。
- `subRun` 的签名从单返回值变成双返回值，这是包内改动，不影响外部用户；但所有包内调用点要一起改。
- `ParallelOptions` 增加字段，结构体字面量如果使用位置参数会编译失败；仓库内应统一使用字段名。需要检查现有测试和示例。
- `RunContext` 增加未导出字段，不影响外部构造；外部本来就不能直接构造完整运行时。
- `Run.Task()` 是新增方法，不影响现有接口。

### 12.3 存储兼容

- 不新增 checkpoint 字段，不改变 revision、fencing token、claim 结构。
- 不改变 `PendingHITL`、`Pause`、`StepSnapshot` 的格式。
- 不改变 Memory/File store 的行为。
- TaskID 不持久化，所以恢复后的 TaskID 和暂停前不同；文档要说明。

---

## 13. 分阶段实施与估算

以下数字都是估算，不是实测。估算假设实现者熟悉现有 agent 包，测试覆盖正常路径、取消路径和恢复路径，不包含大规模性能压测。

### 13.1 阶段划分

| 阶段 | 内容 | 依赖 | 估算生产代码 | 估算测试代码 | 估算时间 |
| --- | --- | --- | --- | --- | --- |
| A | 并行工具上限 | 无 | 80–150 行 | 250–400 行 | 2–3 天 |
| B | Task 元数据 + TaskBudget + 子环境接入 | A 可独立；B 不依赖 A | 350–550 行 | 500–800 行 | 4–6 天 |
| C | Parallel / Plan 失败取消 | B 的 Task 信息可选 | 200–320 行 | 350–550 行 | 3–4 天 |
| D | 同线程准入 gate + 默认协调 | 无；建议在 B/C 后做 | 250–400 行 | 400–650 行 | 4–5 天 |
| E | 文档、示例、迁移、回归 | A–D | 100–200 行（示例/注释） | 100–200 行 | 2–3 天 |
| 合计 |  |  | 约 1,000–1,600 行 | 约 1,600–2,600 行 | 约 3–4 周 |

如果要把四项都做成可配置、可观察、带完整反向验证，生产和测试代码可能接近 2,000–2,800 行；如果只做最小可用路径，可以压到 800–1,200 行生产代码。两者都不包括把运行模型整体改成 Codex Task 模型。

### 13.2 推荐顺序

1. **A：并行工具上限。** 改动最局部，能先验证“名额获取、事件顺序、超时放弃”三件事。
2. **B：Task 元数据和预算。** 先把任务树接进 `subRun`、`forBranch`、`AsTool`，再做失败取消，否则取消没有统一的子任务边界。
3. **C：失败取消。** 依赖 B 的派生环境和 `subRunWithContext`；先做 `Parallel`，再做 `Plan`。
4. **D：准入 gate。** 这是行为变化最大的一项，放最后，单独提交、单独更新 README 和测试。
5. **E：文档和迁移。** 每个阶段都要更新对应段落，不要留到最后一次性补。

### 13.3 不建议的范围

不建议在这批里做：

- 把 `Run` 改成 Codex 的 Task 替换模型。
- 给每个分支开独立线程和独立 checkpoint 链。
- 跨进程排队。
- token 级预算。
- 工具暴露级别和按需加载。
- 统一的工具执行策略和沙箱。

这些都会把代码量和验证面扩大一个数量级，而且和本文四个优化没有直接依赖。

---

## 14. 测试策略与验收门禁

### 14.1 单元测试

- `buildToolCallSegments` 不受影响；新增上限只测 `execTools`。
- `admitChild` 的深度、子任务数、CAS 并发。
- `consumeTurn` 的预算边界和并发。
- gate 的 FIFO、取消、队列满、名额转交。
- 根因选择函数：声明顺序、取消过滤、父 context 取消。

### 14.2 集成测试

- 工具批次：2 个调用、5 个调用、超时、取消、串行夹并行。
- 子 Agent：深度 1、深度 8、深度 9、宽扇出、预算耗尽后模型继续回答。
- `Parallel`：失败取消、`WaitForAll`、HITL 暂停、不监听 context 的兄弟。
- `Plan`：`FailFast` 取消、`ContinueOnError`、节点重试、计划保存失败。
- 准入：Session 并发、Agent 并发、队列满、取消、Resume 快速失败、重入。

### 14.3 竞态和泄漏

- 所有新增测试跑 `-race`。
- 检查 goroutine 泄漏：取消后所有分支 goroutine 都应该在测试超时前退出；不监听 context 的工具用显式 gate 控制，不用 sleep。
- 检查事件基数：一次运行一个 `RunStarted`、一个终结事件；`TaskStarted/TaskDone` 成对。
- 检查 checkpoint：取消和预算失败后，head 仍然可读，Resume 的行为符合现有规则。

### 14.4 反向验证

每个关键判据都要做一次“故意改错”的验证：

| 改动 | 应失败的用例 |
| --- | --- |
| 删除工具名额归还 | 超时/放弃用例永久阻塞或峰值超过上限 |
| 把 `ToolStarted` 移到拿名额之前 | 并行上限的事件顺序用例 |
| 删除 `admitChild` 的上限检查 | 深度 9、宽扇出用例 |
| 删除根因过滤 | 兄弟取消用例拿到 `context.Canceled` |
| 删除 `cancelNodes` | Plan 失败取消用例超时 |
| 删除 gate 的名额转交 | FIFO 用例挂起 |
| 删除重入检查 | 重入用例超时 |

### 14.5 验收门禁

- `go test ./...` 全绿。
- `go test -race ./agent ./checkpoint/...` 全绿。
- 新增示例可以重复运行，不依赖随机完成顺序。
- README 和关联设计文档的链接、行号、行为描述同步更新。

---

## 15. 风险与待定问题

### 15.1 风险

1. **默认值选错。** 工具上限 8、子任务 64 都是草稿值。没有真实负载数据前，不能声称这些值最优。实现前应至少统计常见工具批次的调用数、子 Agent 扇出和 DAG 节点数。
2. **取消被误判为根因。** 工具可能把 `context.Canceled` 包装成自己的错误；根因过滤只能减少误判，不能完全消除。需要在测试里覆盖“工具包装取消”的情况。
3. **取消不能回滚副作用。** 兄弟分支可能已经写了文件、发了请求、改了数据库。本文只保证不再启动后续步骤，不保证外部状态一致。
4. **检查点交错。** Parallel 分支和 Plan 节点共享父线程的 Store；取消后，已经提交的分支检查点仍然留在历史里。最终合并会丢弃分支 KV，但历史中可能有中间状态。这个风险在现有 `Parallel`/`Plan` 里就存在，本文没有消除它。
5. **Session 默认排队可能隐藏故障。** 一个长时间不结束的运行会让后续 Send 一直等待。调用方必须用 context 控制等待时间，或者显式选择 `AdmissionFail`。
6. **重入检测只覆盖同线程。** 工具里调用另一个 Agent 的另一个线程是允许的；如果它再回到原线程，才会被检测到。复杂的循环调用仍可能造成资源堆积，需要预算和 context 兜底。
7. **预算树在暂停恢复后重置。** 这会让跨恢复的总量不受限；如果产品需要长期总量，必须在应用层记录。

### 15.2 待定问题

1. `WithToolConcurrency` 默认值取 8、16 还是不限制？本文取 8，理由是常见批次的规模，但没有实测。
2. `Parallel` 默认取消，还是默认等待、由调用方显式开启？本文取取消，理由是失败后继续跑通常没有收益；对副作用敏感的工作流可能希望默认等待。
3. `Session.Send` 默认排队，还是 `Agent` 全局默认排队？本文取前者，以保留 `Agent.Run` 的快速失败语义。
4. `TaskBudget.MaxUnits` 默认 64 是否足够覆盖真实 DAG？大型计划可能需要按 Agent 配置调大。
5. TaskID 是否需要持久化？本文不持久化；如果 UI 需要跨进程稳定标识，应优先扩展现有 RunRecord，而不是新建 Task 表。
6. 是否要把 `maxTransferDepth` 合并进 `TaskBudget.MaxDepth`？本文保持两套，减少一次改动的影响面。
7. 是否要在第一阶段就发 `RunQueued`/`RunAdmitted`？本文标为可选，取决于 UI 需求。
8. 是否需要共享的任务树 token 预算？需要先有可靠的请求前 token 估算，否则只会得到事后统计，不能阻止超支。

---

## 16. 代码锚点

### 16.1 工具执行

- [agent/exectools.go:42](../../agent/exectools.go#L42)：`execTools` 入口。
- [agent/exectools.go:68](../../agent/exectools.go#L68)：单次调用的执行和放弃逻辑。
- [agent/exectools.go:167](../../agent/exectools.go#L167)：并行段启动。
- [agent/exectools.go:176](../../agent/exectools.go#L176)：并行调用的 goroutine。
- [agent/exectools.go:181](../../agent/exectools.go#L181)：等待并行段结束。
- [agent/exectools.go:201](../../agent/exectools.go#L201)：`abandonGrace`。
- [agent/exectools.go:310](../../agent/exectools.go#L310)：`callOne`。
- [agent/toolsegments.go:22](../../agent/toolsegments.go#L22)：分段构造。

### 16.2 任务派生

- [agent/runtime.go:20](../../agent/runtime.go#L20)：`RunContext` 字段。
- [agent/runtime.go:65](../../agent/runtime.go#L65)：`deeper`。
- [agent/runtime.go:84](../../agent/runtime.go#L84)：`subRun`。
- [agent/runtime.go:106](../../agent/runtime.go#L106)：`forBranch`。
- [agent/subagent.go:38](../../agent/subagent.go#L38)：`AsTool.Call`。
- [agent/transfer.go:19](../../agent/transfer.go#L19)：`maxTransferDepth`。
- [agent/transfer.go:39](../../agent/transfer.go#L39)：transfer 的深度检查。

### 16.3 工作流和计划

- [agent/workflow.go:141](../../agent/workflow.go#L141)：`parallelRunner.run`。
- [agent/workflow.go:164](../../agent/workflow.go#L164)：等待所有分支。
- [agent/workflow.go:174](../../agent/workflow.go#L174)：`merge`。
- [agent/plan.go:148](../../agent/plan.go#L148)：计划默认并发 8。
- [agent/plan.go:258](../../agent/plan.go#L258)：计划调度循环。
- [agent/plan.go:282](../../agent/plan.go#L282)：节点 goroutine。
- [agent/plan.go:319](../../agent/plan.go#L319)：收集节点结果。
- [agent/plan.go:348](../../agent/plan.go#L348)：`applyResult`。
- [agent/plan.go:385](../../agent/plan.go#L385)：`execNode`。
- [agent/plan.go:511](../../agent/plan.go#L511)：计划保存。

### 16.4 同线程准入和 claim

- [agent/run_prepare.go:39](../../agent/run_prepare.go#L39)：`prepare` 里的协调查找。
- [agent/run_prepare.go:44](../../agent/run_prepare.go#L44)：`BeginRun`。
- [agent/write_guard.go:67](../../agent/write_guard.go#L67)：写保护启动。
- [agent/write_guard.go:218](../../agent/write_guard.go#L218)：续租循环。
- [agent/write_guard.go:301](../../agent/write_guard.go#L301)：续租间隔。
- [checkpoint/coordinator_file.go:9](../../checkpoint/coordinator_file.go#L9)：File 的 `BeginRun`。
- [checkpoint/coordinator_file.go:17](../../checkpoint/coordinator_file.go#L17)：活动 claim 冲突。
- [checkpoint/coordinator_file.go:22](../../checkpoint/coordinator_file.go#L22)：暂停线程要求恢复。
- [checkpoint/durable.go:15](../../checkpoint/durable.go#L15)：`Durable` 接口。
- [checkpoint/durable.go:55](../../checkpoint/durable.go#L55)：默认租约 30 秒。
- [agent/session.go:47](../../agent/session.go#L47)：`Session.Send`。
- [agent/hitl.go:79](../../agent/hitl.go#L79)：`Agent.Resume`。
- [agent/hitl.go:103](../../agent/hitl.go#L103)：Resume 的 `Claim`。

### 16.5 生命周期和事件

- [agent/run.go:233](../../agent/run.go#L233)：`drive`。
- [agent/run.go:152](../../agent/run.go#L152)：`Cancel`。
- [agent/agent.go:236](../../agent/agent.go#L236)：`newRunHandleWithID`。
- [core/event.go:17](../../core/event.go#L17)：`RunStarted`。
- [core/event.go:25](../../core/event.go#L25)：`RunDone`。
- [core/event.go:31](../../core/event.go#L31)：`RunFailed`。
- [core/event.go:56](../../core/event.go#L56)：`ToolStarted`。
- [core/event.go:65](../../core/event.go#L65)：`ToolDone`。
- [core/event.go:94](../../core/event.go#L94)：`PlanNodeStarted`。
- [core/event.go:98](../../core/event.go#L98)：`PlanNodeDone`。
- [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339)：`commitCheckpoint`。

---

## 17. 结束语

这五项改动的关系可以再压缩成一句话：

> 先把任务树说清楚，再在任务树上做预算；工具上限和兄弟取消都是任务树上的局部资源控制；同线程准入是线程入口的排队；持久 claim 始终不动。

实现时建议一次只改一项，每项都带反向验证和文档更新。尤其是第 8 节的失败取消和第 9 节的默认排队，都会改变调用方看到的行为，不能和内部重构混在一起提交。
