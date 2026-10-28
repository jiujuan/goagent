# 运行时并发加固实施计划（Task 类别 + 四项优化）

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文覆盖设计稿 §5、§6、§7、§8、§9 全部内容；§5 Task 类别先做，它是后四项的基础。

**目标：**按照 [runtime-concurrency-hardening-design.md](../analysis/runtime-concurrency-hardening-design.md) 的设计，先在运行时里补出 `Task` 这一层身份，再在它上面完成四项加固：

1. 并行工具段最多同时执行固定数量的调用，默认 8，串行段不受影响。
2. 子 Agent、工作流、DAG 节点共享一棵任务树的深度、子任务数、模型调用数预算。
3. `Parallel` 默认在一个分支失败时取消兄弟分支；`Plan` 的 `FailFast` 取消在途节点，并保留第一个根因。
4. 同一个 `Agent` 上同线程的运行增加进程内准入：`Session.Send` 默认排队，`Agent.Run/Stream` 默认快速失败，`Resume` 固定快速失败；持久 claim 仍是跨进程正确性的唯一来源。

**架构：**运行时层次从 `Thread → Turn → Run → Step` 扩展为 `Thread → Turn → Task → Run / 派生 RunContext → Step → tool segment → tool call`。`Task` 只是运行期元数据：不建表、不写 checkpoint、不替换旧任务。一个 `Run` 有一个根 `Task`；暂停恢复产生新的根 `Task`，但复用同一个 `TurnID`。`AsTool`、`Parallel` 分支、DAG 节点和 planner/replanner 各自派生一个子 `Task`，共享同一个 `taskRuntime` 预算计数器。工具上限是每个 `AgentLoop` 的局部名额，预算挂在任务树上，失败取消只取消兄弟分支的 context，同线程准入挂在 `Agent` 上且只在本进程内有效。

**技术栈：**Go 1.25（本机 go1.26.4）、标准库、现有 mock LLM、checkpoint Memory/File、bus。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-22。
- 基线：`43edc44 feat(examples): print turn starts and delivery sources`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./agent -count=1` 确认基线。
- 设计依据：[runtime-concurrency-hardening-design.md](../analysis/runtime-concurrency-hardening-design.md) §5–§9、§11–§14。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：先做 §5 Task 类别；然后 §6 并行工具上限、§7 任务树预算、§8 失败取消、§9 同线程准入。设计与任务卡的对应关系见第 5 节。
- 当前工作区有未跟踪的分析文档、`.scratch/` 目录和几份未入库的计划文档。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。任务卡里的提交只包含该卡列出的代码、测试和注释。

## 1. 现有能力与实施边界

### 1.1 现在一次运行怎么执行

1. `Agent.New` 填入默认配置，构造 `AgentLoop`；工作流和计划由 `wrapWorkflowWithStore` 直接构造 `Agent`，没有循环（[agent/agent.go:37](../../agent/agent.go#L37)、[agent/agent.go:64](../../agent/agent.go#L64)、[agent/workflow.go:77](../../agent/workflow.go#L77)）。
2. `Stream` 解析 `RunConfig`，创建 `RunContext` 和 `Run` 句柄；`Run` 的 `drive` 负责 `RunStarted`、`prepare`、执行、终结事件和 claim 释放（[agent/agent.go:145](../../agent/agent.go#L145)、[agent/agent.go:236](../../agent/agent.go#L236)、[agent/run.go:240](../../agent/run.go#L240)）。
3. `prepare` 在真正执行前调用 `BeginRun` 取同线程写权限，解析 head 和 `TurnID`，启动写保护（[agent/run_prepare.go:25](../../agent/run_prepare.go#L25)、[agent/run_prepare.go:47](../../agent/run_prepare.go#L47)、[agent/run_prepare.go:93](../../agent/run_prepare.go#L93)）。
4. 普通的派生环境只有三种：`deeper`（transfer，共享 State）、`subRun`（隔离 State 的 DAG 节点/planner/replanner）、`forBranch`（克隆 State 的 Parallel 分支），都不登记任何任务身份（[agent/runtime.go:85](../../agent/runtime.go#L85)、[agent/runtime.go:105](../../agent/runtime.go#L105)、[agent/runtime.go:128](../../agent/runtime.go#L128)）。
5. `AsTool` 通过 `child.Run` 启动一个临时线程的隔离子运行，父运行拿不到子运行的任务身份（[agent/subagent.go:38](../../agent/subagent.go#L38)、[agent/subagent.go:53](../../agent/subagent.go#L53)）。
6. 工具调用已经按模型调用顺序分段：串行调用单独成段，连续非串行调用组成并行段；并行段当前对调用数没有上限（[agent/exectools.go:150](../../agent/exectools.go#L150)、[agent/exectools.go:157](../../agent/exectools.go#L157)、[agent/exectools.go:168](../../agent/exectools.go#L168)）。
7. `Parallel` 先创建全部分支上下文，再全部启动，最后等所有分支结束；`merge` 按声明顺序找第一个错误或控制结果（[agent/workflow.go:141](../../agent/workflow.go#L141)、[agent/workflow.go:156](../../agent/workflow.go#L156)、[agent/workflow.go:174](../../agent/workflow.go#L174)）。
8. `Plan` 的 `FailFast` 只是不再启动新节点，已经在运行的节点继续跑；每个失败都会覆盖 `failFastErr`（[agent/plan.go:190](../../agent/plan.go#L190)、[agent/plan.go:258](../../agent/plan.go#L258)、[agent/plan.go:348](../../agent/plan.go#L348)）。
9. 同线程并发只有持久 claim：`Session.Send` 和 `Agent.Stream` 最终都走 `BeginRun`，冲突返回 `checkpoint.ErrClaimConflict`（[agent/session.go:47](../../agent/session.go#L47)、[checkpoint/coordinator.go:36](../../checkpoint/coordinator.go#L36)、[checkpoint/durable.go:40](../../checkpoint/durable.go#L40)）。
10. `middleware.RunBudget` 已经在单次运行内限制 turns/tokens/cost/duration，并能把超限变成收尾回答（[middleware/runbudget.go:38](../../middleware/runbudget.go#L38)、[middleware/runbudget.go:49](../../middleware/runbudget.go#L49)、[middleware/runbudget.go:83](../../middleware/runbudget.go#L83)）。它和本文新增的任务树预算不是一回事。

### 1.2 五个缺口

- **没有任务身份。** 事件里只能看到 Run、Step、工具，无法回答“这条事件属于哪个父任务、嵌套几层、是 chat 还是 workflow/plan”。
- **预算没有树级归属。** transfer 有 `maxTransferDepth`，但 AsTool、Parallel、DAG 节点都没有统一的深度、子任务数、模型调用数上限；一棵树可以按层扇出。
- **工具并行没有上限。** 并行段的上限等于调用数；模型返回 50 个调用就同时启动 50 个 handler。
- **失败不取消兄弟。** `Parallel` 要等所有分支结束；`Plan` 的在途节点继续跑，且根因会被后续失败覆盖。
- **同线程只有“谁写”的协议，没有“第二个请求怎么办”。** 现在只有立即失败，没有进程内排队，也没有重入检测。

### 1.3 可观察行为变化

| 场景 | 现在 | 本计划之后 | 恢复旧行为的方式 |
| --- | --- | --- | --- |
| 一个并行工具段有 N 个调用 | 最多 N 个同时执行 | 默认最多 8 个；`0` 表示不限制 | `WithToolConcurrency(0)` |
| 串行工具夹在并行调用之间 | 串行段独占，前后并行段并发 | 不变；上限不作用于串行段 | 无需迁移 |
| 任务树嵌套超过 8 层或子任务超过 64 个 | 没有统一限制 | 返回 `TaskBudgetError` | `WithTaskBudget` 调大；0 字段使用默认值 |
| 任务树模型调用数超过 `MaxTurns` | 没有统一限制 | 下一次模型请求前返回预算错误 | 保持 `MaxTurns: 0` |
| `Parallel` 一个分支失败 | 等所有分支结束，按声明顺序报错 | 默认取消其他分支，仍等待已启动分支退出 | `ParallelOptions{Failure: WaitForAll}` |
| `Plan` 的 `FailFast` 节点最终失败 | 在途节点继续跑，错误可能被覆盖 | 取消在途节点，保留第一个根因，节点状态可为 `canceled` | 改用 `ContinueOnError` 或自行收尾 |
| 同一 `Session` 并发两次 `Send` | 第二个立即 `ErrClaimConflict` | 第二个默认 FIFO 排队 | `WithAdmission(AdmissionOptions{Policy: AdmissionFail})` |
| 同一 `Agent` 并发两个 `Stream` | 第二个 `ErrClaimConflict` | 仍然快速失败，错误包装 `ErrClaimConflict` | 不变 |
| `Resume` 遇到线程正忙 | 取决于 claim | 固定快速失败，不排队、不先拿 claim | 本批不提供按次覆盖 |
| 事件流 | 无 Task 事件 | 根任务、子 Run、Parallel 分支、DAG 节点新增 `TaskStarted/TaskDone`；Parallel 新增 `BranchCanceled` | 无关闭开关；消费者忽略未知事件 |

### 1.4 不做什么

- 不做 Codex 那种“新 Task 替换旧 Task”。旧运行可能有外部副作用和 claim，不能被替换。
- 不把 Task 写进 checkpoint，不新增 Task 表，不改 RunRecord 格式。
- 不做跨进程排队；进程内 gate 不替代 claim。
- 不做跨任务树的全局工具信号量。父工具可能正在等子 Agent，共享名额会死锁。
- 不做 token 级任务树预算；token/cost/duration 仍由 `middleware.RunBudget` 负责。
- 不给 `Parallel`/`Loop`/`Sequential` 构造函数增加 `Option` 入口；本批工作流根只使用默认预算。
- 不为 planner/replanner 发布 Task 事件；它们计入子任务预算，但没有独立 Task 事件。
- 不新增 `RunQueued`/`RunAdmitted` 事件。排队状态本批只能从 `RunStarted` 之后没有模型/工具事件推断。
- 不改变 `tool.SequentialTool`、`ToolExecMode`、`WithToolExecution` 的语义。
- 不改变 checkpoint、revision、fencing token、claim 结构。

## 2. 已核实事实

### 2.1 代码锚点

下表是实施前核对过的代码位置。执行时如果行号漂移，按函数名和代码内容定位，不要按行号硬改。

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `Agent` 结构只有配置、循环、父 Agent、runnable、bus、store | [agent/agent.go:27](../../agent/agent.go#L27) | 增加 gate 和任务配置 |
| `New` 的默认配置和合法性检查 | [agent/agent.go:37](../../agent/agent.go#L37) | 增加默认预算、工具上限、负数校验 |
| `RunConfig` 目前只有 ThreadID/Message/Files | [agent/agent.go:96](../../agent/agent.go#L96) | 增加准入和任务种子字段 |
| `Stream` 和 `resolveRunConfig` | [agent/agent.go:145](../../agent/agent.go#L145)、[agent/agent.go:149](../../agent/agent.go#L149) | 准入默认值和重入检查 |
| `streamConfigured` 在句柄创建时冻结消息和错误 | [agent/agent.go:157](../../agent/agent.go#L157) | 把 `startErr` 用于重入/校验失败 |
| `newRunHandleWithID` 构造 `RunContext` | [agent/agent.go:236](../../agent/agent.go#L236) | 根 Task、任务种子、线程作用域 |
| `RunContext` 的字段和派生函数 | [agent/runtime.go:20](../../agent/runtime.go#L20)、[agent/runtime.go:85](../../agent/runtime.go#L85)、[agent/runtime.go:105](../../agent/runtime.go#L105)、[agent/runtime.go:128](../../agent/runtime.go#L128) | 增加 `task`、`info`、`taskMu` |
| `RunContext.publish` | [agent/runtime.go:230](../../agent/runtime.go#L230) | 发布 Task 事件 |
| `Run.drive` 的生命周期和终结分支 | [agent/run.go:240](../../agent/run.go#L240)、[agent/run.go:255](../../agent/run.go#L255)、[agent/run.go:269](../../agent/run.go#L269)、[agent/run.go:274](../../agent/run.go#L274)、[agent/run.go:278](../../agent/run.go#L278) | Task 事件和 gate 释放 |
| `Run.Cancel` 在未驱动时释放 claim | [agent/run.go:159](../../agent/run.go#L159) | 同时释放 gate |
| `prepare` 的 BeginRun、turn 解析、写保护 | [agent/run_prepare.go:25](../../agent/run_prepare.go#L25)、[agent/run_prepare.go:47](../../agent/run_prepare.go#L47)、[agent/run_prepare.go:93](../../agent/run_prepare.go#L93) | gate 获取和 TurnID 填任务 |
| `runStep` 的模型请求和 attempt 循环 | [agent/loop.go:309](../../agent/loop.go#L309)、[agent/loop.go:368](../../agent/loop.go#L368)、[agent/loop.go:390](../../agent/loop.go#L390)、[agent/loop.go:400](../../agent/loop.go#L400)、[agent/loop.go:401](../../agent/loop.go#L401) | `consumeTurn` 的插入点 |
| 恢复批次和普通批次都调用 `execTools` | [agent/loop.go:140](../../agent/loop.go#L140)、[agent/loop.go:671](../../agent/loop.go#L671) | 工具上限覆盖两条路径 |
| `execTools` 的 run 闭包、handler goroutine、统计 | [agent/exectools.go:42](../../agent/exectools.go#L42)、[agent/exectools.go:68](../../agent/exectools.go#L68)、[agent/exectools.go:87](../../agent/exectools.go#L87)、[agent/exectools.go:96](../../agent/exectools.go#L96) | 名额在 `run` 返回后归还 |
| `ToolDone` 在 `run` 末尾发布 | [agent/exectools.go:147](../../agent/exectools.go#L147) | 事件顺序 |
| 分段和并行段启动 | [agent/exectools.go:150](../../agent/exectools.go#L150)、[agent/exectools.go:157](../../agent/exectools.go#L157)、[agent/exectools.go:168](../../agent/exectools.go#L168)、[agent/exectools.go:175](../../agent/exectools.go#L175)、[agent/exectools.go:182](../../agent/exectools.go#L182) | 改造为带名额的并行段 |
| 超时放弃和宽限窗口 | [agent/exectools.go:202](../../agent/exectools.go#L202) | 名额不能等 handler 退出才归还 |
| `toolCallCtx` 和 `tool.Context` 构造 | [agent/exectools.go:232](../../agent/exectools.go#L232)、[agent/exectools.go:445](../../agent/exectools.go#L445) | 放入父运行载体 |
| `tool.Context`、`Updater`、`SequentialTool` | [tool/tool.go:46](../../tool/tool.go#L46)、[tool/tool.go:60](../../tool/tool.go#L60)、[tool/tool.go:88](../../tool/tool.go#L88) | 不新增公开接口 |
| `AsTool` 入口和子运行 | [agent/subagent.go:38](../../agent/subagent.go#L38)、[agent/subagent.go:53](../../agent/subagent.go#L53) | 父任务登记和子 Task 种子 |
| transfer 深度和 `deeper` | [agent/transfer.go:19](../../agent/transfer.go#L19)、[agent/transfer.go:39](../../agent/transfer.go#L39)、[agent/transfer.go:47](../../agent/transfer.go#L47) | 同一 Task，不扣子任务 |
| `ParallelOptions` 和 `parallelRunner.run` | [agent/workflow.go:59](../../agent/workflow.go#L59)、[agent/workflow.go:141](../../agent/workflow.go#L141) | 增加失败策略和并发上限 |
| `Parallel` 先建后跑、最后 merge | [agent/workflow.go:149](../../agent/workflow.go#L149)、[agent/workflow.go:156](../../agent/workflow.go#L156)、[agent/workflow.go:164](../../agent/workflow.go#L164)、[agent/workflow.go:174](../../agent/workflow.go#L174) | 改成边启动边收结果 |
| `wrapWorkflowWithStore` 直接构造 Agent | [agent/workflow.go:77](../../agent/workflow.go#L77) | 补 taskKind、预算、工具上限默认值 |
| `Plan` 的调度器、收集、保存 | [agent/plan.go:190](../../agent/plan.go#L190)、[agent/plan.go:209](../../agent/plan.go#L209)、[agent/plan.go:240](../../agent/plan.go#L240)、[agent/plan.go:288](../../agent/plan.go#L288)、[agent/plan.go:319](../../agent/plan.go#L319)、[agent/plan.go:511](../../agent/plan.go#L511) | 节点 context 和取消 |
| `applyResult` 的重试和 FailFast 覆盖 | [agent/plan.go:348](../../agent/plan.go#L348)、[agent/plan.go:350](../../agent/plan.go#L350)、[agent/plan.go:357](../../agent/plan.go#L357) | 状态机和根因保留 |
| `execNode`、planner、replanner 的 `subRun` | [agent/plan.go:385](../../agent/plan.go#L385)、[agent/plan.go:393](../../agent/plan.go#L393)、[agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454) | 子任务登记 |
| `skipBlocked` 的依赖状态 | [agent/plan.go:469](../../agent/plan.go#L469) | 增加 `canceled` 依赖 |
| `Resume` 的 Latest、校验、Claim、句柄 | [agent/hitl.go:79](../../agent/hitl.go#L79)、[agent/hitl.go:106](../../agent/hitl.go#L106)、[agent/hitl.go:170](../../agent/hitl.go#L170) | 准入和释放路径 |
| `resumeInFlight` 的 Claim 和句柄 | [agent/hitl.go:221](../../agent/hitl.go#L221)、[agent/hitl.go:233](../../agent/hitl.go#L233)、[agent/hitl.go:265](../../agent/hitl.go#L265) | 准入和释放路径 |
| `Session.Send` | [agent/session.go:47](../../agent/session.go#L47) | 默认排队 |
| `PlanNodeDone` 状态列表 | [core/event.go:126](../../core/event.go#L126) | 增加 `canceled` |
| `BudgetExceeded` 是 middleware 的 per-run 事件 | [core/event.go:145](../../core/event.go#L145) | 和任务树预算区分 |
| 事件接口和 isEvent 列表 | [core/event.go:12](../../core/event.go#L12)、[core/event.go:265](../../core/event.go#L265) | 新增事件必须登记 |
| 事件 JSON 的 wire 和两个 switch | [core/eventjson.go:18](../../core/eventjson.go#L18)、[core/eventjson.go:71](../../core/eventjson.go#L71)、[core/eventjson.go:126](../../core/eventjson.go#L126)、[core/eventjson.go:164](../../core/eventjson.go#L164)、[core/eventjson.go:212](../../core/eventjson.go#L212) | 新增事件必须能序列化 |
| `RunBudgetOptions.MaxTurns` 是单次运行上限 | [middleware/runbudget.go:49](../../middleware/runbudget.go#L49) | 和任务树 `MaxTurns` 区分 |
| `ErrClaimConflict` 和 Claim 请求 | [checkpoint/durable.go:40](../../checkpoint/durable.go#L40)、[checkpoint/durable.go:96](../../checkpoint/durable.go#L96) | 忙错误包装它 |
| `setLineageStart` 和 `runTurnID` | [agent/durable_runtime.go:191](../../agent/durable_runtime.go#L191)、[agent/durable_runtime.go:205](../../agent/durable_runtime.go#L205) | 根 Task 的 TurnID |
| `NewPipeline` 最终落到 `Sequential` | [agent/pipeline.go:21](../../agent/pipeline.go#L21)、[agent/pipeline.go:49](../../agent/pipeline.go#L49) | workflow kind |
| README 的同线程 claim 说明 | [README.md:293](../../README.md#L293) | 文档更新 |
| 现有并行工具测试夹具 | [agent/toolsegments_test.go:27](../../agent/toolsegments_test.go#L27)、[agent/toolsegments_test.go:62](../../agent/toolsegments_test.go#L62)、[agent/toolsegments_test.go:69](../../agent/toolsegments_test.go#L69)、[agent/toolsegments_test.go:120](../../agent/toolsegments_test.go#L120)、[agent/toolsegments_test.go:126](../../agent/toolsegments_test.go#L126) | 工具上限测试 |
| 现有重叠计数和慢工具 | [agent/seqexec_test.go:18](../../agent/seqexec_test.go#L18)、[agent/seqexec_test.go:68](../../agent/seqexec_test.go#L68)、[agent/seqexec_test.go:123](../../agent/seqexec_test.go#L123) | 上限和串行边界 |
| 现有计划失败测试 | [agent/plan_test.go:17](../../agent/plan_test.go#L17)、[agent/plan_test.go:79](../../agent/plan_test.go#L79)、[agent/plan_test.go:88](../../agent/plan_test.go#L88) | 计划取消回归 |
| 现有子 Agent 测试 | [agent/subagent_test.go:14](../../agent/subagent_test.go#L14) | AsTool 预算和任务 |
| 现有审批门 | [agent/phase1_interrupt_test.go:91](../../agent/phase1_interrupt_test.go#L91) | HITL 不取消兄弟 |
| 现有慢工具和超时放弃 | [agent/tooltimeout_abandon_test.go:21](../../agent/tooltimeout_abandon_test.go#L21)、[agent/tooltimeout_abandon_test.go:171](../../agent/tooltimeout_abandon_test.go#L171) | 名额超时归还 |
| 现有 Parallel 合并测试 | [agent/workflow_test.go:39](../../agent/workflow_test.go#L39) | 失败取消后仍要合并成功路径 |
| 现有同线程冲突测试 | [agent/thread_consistency_test.go:179](../../agent/thread_consistency_test.go#L179) | 快速失败仍返回 `ErrClaimConflict` |
| 事件 JSON 往返测试 | [core/eventjson_test.go:8](../../core/eventjson_test.go#L8) | 新增事件加入列表 |
| 现有 `subRun`/`forBranch` 测试调用点 | [agent/task_loop_internal_test.go:103](../../agent/task_loop_internal_test.go#L103)、[agent/task_loop_internal_test.go:109](../../agent/task_loop_internal_test.go#L109)、[agent/state_isolation_test.go:264](../../agent/state_isolation_test.go#L264)、[agent/stepview_drift_test.go:446](../../agent/stepview_drift_test.go#L446)、[agent/stepview_drift_test.go:449](../../agent/stepview_drift_test.go#L449)、[agent/preempt_internal_test.go:96](../../agent/preempt_internal_test.go#L96) | 签名变化要一起改 |

### 2.2 对设计稿的按当前代码修正

1. **重入检查不能放在 `prepare`。** `newRunHandleWithID` 会把本运行的线程作用域写进 `RunContext.Context`（[agent/agent.go:236](../../agent/agent.go#L236)），`prepare` 再检查就会看到自己，永远误判重入。改成在 `streamConfigured` 创建句柄之前、以及 `Resume` 获取 gate 之前，用调用方 context 检查。作用域仍然装进运行 context，供工具内重入检测使用。
2. **`chooseRootError` 需要父 context。** 只看分支结果无法区分“父运行被外部取消”和“兄弟失败导致的分支取消”。签名改为 `chooseRootError(parentCtx, collected, fallback)`，第一步先看 `parentCtx.Err()`。
3. **`admitChild` 不填 ThreadID/TurnID。** 共享线程的 `subRun`/`forBranch` 在派生时填父的 ThreadID/TurnID；`AsTool` 子运行由子运行创建环境时填自己的 ThreadID，`prepare` 再填自己的 TurnID。这样事件里不会出现“子任务挂在父线程、子运行却在另一个线程”。
4. **根 `TaskStarted` 在 `prepare` 成功之后发布。** 设计稿只说“在 `RunStarted` 之后”。放到 `prepare` 之后才能带上 `TurnID`，而且不会在 prepare 失败时留下一个没有 `TaskDone` 的开始事件。prepare 失败时本运行不发 Task 事件。
5. **`consumeTurn` 每次模型 attempt 都扣。** 设计稿说“被抢占、未真正发出的请求不应扣”，但循环无法可靠判断请求是否已经发出（`beginModelAttempt` 之后才调用 `streamModel`）。本计划按 attempt 扣：抢占重试、上下文溢出重试、普通重试都算一次模型请求。这样 `MaxTurns` 是上界，不会低估。
6. **`MaxWaiters` 按每次 acquire 传入。** 设计稿的 gate 结构把它放在 gate 上，但 `AdmissionOptions` 是每次 Run 的配置。允许每次运行不同，队列长度检查使用到达者自己的上限。
7. **`Resume` 先拿 gate 再读 Latest。** 设计稿 §9.5.2 的顺序就是 gate 在 Claim 之前；把 gate 提到 `Resume` 开头，能让“线程忙”在读取、校验之前快速失败，也符合设计 §9.9 的第 7 条。preflight 失败时释放 gate。
8. **planner/replanner 计入子任务，但本批不发 Task 事件。** 设计稿 §5.6 只列了 Parallel 分支和 DAG 节点；planner/replanner 已有 `PlanNodeStarted/PlanNodeDone` 的专用节点 ID，不重复发 Task 事件。它们的预算扣减不受影响。
9. **工作流根只使用默认预算。** `Parallel`/`Loop`/`Sequential` 没有 `Option` 入口；本批不扩展现有构造函数签名。需要自定义时，把工作流挂到父运行下，由父树统一预算。
10. **`RunQueued`/`RunAdmitted` 本批不做。** 设计稿允许第一阶段只记日志。为了不扩大事件面，本批不新增这两个事件，排队状态通过 `RunStarted` 之后没有模型/工具事件来观察。
11. **工具名额在启动 goroutine 之前同步获取。** 这样 `ToolStarted` 只在真正拿到名额后发布，并且同一并行段内的 `ToolStarted` 仍按模型调用顺序发布。等待名额时也要响应 `rc.Context` 取消。
12. **根 Task 的 `canceled` 状态。** 设计稿的 `TaskDone.Status` 有 `"canceled"`；本计划把“运行错误是 `context.Canceled`”也映射为 `"canceled"`，不只限于兄弟分支取消。
13. **根因选择不依赖“分支返回时 context 是否已取消”。** 设计稿建议用 `branchResult.canceled` 辅助排除取消错误。但该标记会把“取消后返回的真实失败”也排除，导致声明顺序靠前的根因被跳过；`TestParallelRootErrorDeterministic` 正好覆盖这种情况。本计划只用 `errors.Is(err, context.Canceled)` 判定取消错误，`branchResult` 不保留 `canceled` 字段；分支是否被兄弟取消用 `BranchCanceled` 事件和 `PlanNodeDone` 状态表达。

## 3. 锁定的设计决定

### D1 Task 是运行期身份，不落盘，也不替换旧任务

`Task` 只回答“哪条线程、哪个 Turn、父任务是谁、什么形式、多深、共享哪份预算”。它不写 checkpoint，不建表，不提供“新任务中止旧任务”。暂停恢复产生新的根 TaskID，TurnID 不变；transfer 继续同一个 Task。

**为什么：**旧运行可能已经执行外部工具、持有 claim 和写保护；替换会破坏 checkpoint 链。当前也没有跨进程查询 Task 的需求。

### D2 根 TaskID 在句柄创建时生成，TurnID 在 `prepare` 时补

`newRunHandleWithID` 生成 `task_...` 的根 ID，写入 `RunContext.info`。`prepare` 解析出 `turnID` 后调用 `setTaskTurn` 补 TurnID。`Run.Task()` 在驱动前返回空 TurnID 是允许的。

**为什么：**`Stream` 不读 checkpoint，TurnID 只能在真正执行时确定。TaskID 提前生成，便于句柄创建后立刻查询和发事件。

### D3 一个 Run 一个 Task，共享线程的派生继承 ThreadID/TurnID

- 顶层 Run、AsTool 子 Run：各自有根 Task。
- `subRun`、`forBranch`：派生环境创建子 Task，父 TaskID 是 ParentID，Depth 加 1，ThreadID/TurnID 继承父。
- `deeper`（transfer）：复制同一个 Task 和 TaskInfo，不增加 Depth。
- `Sequential`/`Loop` 子 Agent：共享父 RunContext，不创建 Task。

**为什么：**Task 是预算和父子关系的归属，不是执行句柄。共享线程的派生必须能回答“属于哪个 Turn”；AsTool 另起线程，必须等子运行自己知道线程和 Turn。

### D4 TaskInfo 的读写有锁，`Run.Task()` 返回副本

`RunContext` 增加 `task *taskRuntime`、`info TaskInfo`、`taskMu sync.Mutex`。`taskInfo()` 在锁下返回副本；`setTaskTurn` 在锁下补 TurnID；派生函数在创建子环境时读取父信息。`Run.Task()` 调用 `r.rc.taskInfo()`。

**为什么：**根 Task 的 TurnID 在 `prepare` 写入，`Run.Task()` 可能从另一个 goroutine 读；`Parallel` 的多个分支也可能同时派生。没有锁会产生数据竞争。

### D5 `taskRuntime` 全树共享，计数器用原子操作

```go
type taskRuntime struct {
	rootID string
	budget TaskBudget
	units  atomic.Int64
	turns  atomic.Int64
}
```

`admitChild` 用 CAS 循环增加 `units`，先判断上限再自增，不做“先加后回退”。`consumeTurn` 同样先判断再 CAS。`MaxDepth`、`MaxUnits` 的 0 表示默认值（8、64），`MaxTurns` 的 0 表示不限制。`ensureTask` 也在 `taskMu` 下检查并创建，避免同一环境被并发派生时竞争。

子 Agent 即使配置了自己的 `WithTaskBudget`，作为子任务运行时也只使用父树的预算；只有它自己成为根运行时才生效。

**为什么：**Parallel 分支和 AsTool 子运行可能同时创建子任务；普通自增会超过上限。CAS 不会出现中间态。

### D6 预算是硬错误

`TaskBudgetError` 实现 `Unwrap() error` 返回 `ErrTaskBudgetExceeded`。`AsTool` 把预算错误转成 `tool.ErrorResult`，让模型看到并调整策略；`Parallel` 返回预算错误；DAG 节点预算错误不重试，`FailFast` 下停止计划，`ContinueOnError` 下标记失败并跳过依赖；模型调用预算错误结束当前运行。

**为什么：**预算耗尽不是“静默跳过”。调用方必须知道哪一维超了，才能决定调大预算、换策略或结束。

### D7 预算和 `middleware.RunBudget` 是两层

- `TaskBudget.MaxTurns`：整棵任务树的模型请求总数上限，硬错误，不区分 token，不做收尾。
- `middleware.RunBudget.MaxTurns`：单次运行的模型调用上限，可进入 wrap-up 模式，并发布 `core.BudgetExceeded`。

两层可以同时开。任务树预算在 `runStep` 发起请求前扣；middleware 的预算仍按自己的逻辑记录和收尾。文档和注释必须写清两者区别。

### D8 工具上限是每个 `AgentLoop` 的并行段名额

`WithToolConcurrency(n)`：默认 8；`n == 0` 不限制；`n < 0` 在 `agent.New` 拒绝。名额只在并行段使用，串行段不拿名额。每个并行段新建一个容量为 `min(n, len(segment))` 的 channel，启动前同步取名额，`run` 返回后归还。

**为什么：**跨任务树共享名额会死锁：父任务占着名额等子任务，子任务又拿不到名额。按段限制只约束当前循环。

### D9 名额在 `run` 返回时归还，不在 handler 退出时归还

`run` 在工具超时后会通过 `awaitAbandoned` 返回，但 handler 可能还在跑（[agent/exectools.go:202](../../agent/exectools.go#L202)）。名额必须跟着 `run` 返回，否则一个不听话的工具会永久占住名额。代价是上限只约束“循环还在等待的调用”，不约束底层 goroutine。

**为什么：**这是现有超时放弃语义的直接结果；等 handler 退出会让上限变成永久阻塞点。

### D10 `ToolStarted` 在拿到名额之后发布

等待名额的调用不能提前出现在事件流里。同一并行段内的 `ToolStarted` 仍按模型调用顺序发布，因为取名额的循环是同步的。`ToolDone` 顺序本来就不保证。

**为什么：**现有测试固定了“同一并行段内 ToolStarted 按调用顺序”和“串行调用的 ToolDone 在下一个段 ToolStarted 之前”。把取名额放在发布之前，两个边界都保住。

### D11 `Parallel` 默认取消兄弟分支，`WaitForAll` 恢复旧行为

`FailurePolicy` 的零值是 `CancelSiblings`；`ParallelOptions{Failure: WaitForAll}` 保留“等所有分支结束再报错”。取消只取消分支 context，不取消父 `rc.Context`；父运行仍要收尾、写 checkpoint、发终结事件。

**为什么：**失败后继续等所有兄弟会浪费时间和资源；但父运行不能跟着取消，否则没有机会记录终态。

### D12 取消后仍等待已启动分支，不合并失败运行

`Parallel` 在第一个真实失败出现时停止启动新分支并取消其他分支，但循环要等所有已启动分支返回。存在真实失败时不应用任何分支的 KV、不拼接文本。HITL 暂停、Stop、Escalate 不算失败，不触发取消。

**为什么：**取消是协作式的，分支可能不立即退出；不等待就读它们的 State 会产生数据竞争。暂停需要保留其他分支的结果和 checkpoint，不能被当成失败。

### D13 根因按声明顺序确定，父取消优先

`chooseRootError`：

1. 父 `rc.Context` 已取消，返回父错误。
2. 收集所有 `!errors.Is(err, context.Canceled)` 的错误，按分支声明顺序选索引最小的。
3. 没有非取消错误时，如果启动错误（例如预算不足）非空，返回启动错误；预算错误不能被取消错误覆盖。
4. 仍然没有错误时，返回声明顺序第一个非 nil 错误，避免把失败的运行当成功。
5. 完全没有错误时返回 nil。错误优先于暂停。

判定“取消错误”只看 `errors.Is(err, context.Canceled)`，不看“分支返回时 context 是否已取消”。后者会把取消后返回的真实失败也排除掉，导致声明顺序靠前的根因被跳过。代价是：工具把取消包装成不兼容 `%w` 的自定义错误时，会被当成真实失败。这个限制要写进注释。

**为什么：**完成顺序不稳定；按声明顺序选根因才能让测试和调用方看到确定的结果。取消产生的错误不能覆盖真正的失败。

### D14 `Plan` 的 `FailFast` 只在重试耗尽或遇到不可重试错误时取消

节点第一次失败但还有重试次数时，不取消其他节点；重试成功后计划继续。重试耗尽或错误不可重试（本批是 `TaskBudgetError`）时，标记失败、设置一次 `failFastErr`、调用 `cancelNodes`。后续失败不覆盖根因；被取消的节点标记 `canceled`，不重试。

**为什么：**一次会成功的瞬时失败不该打断其他节点；根因被后续失败覆盖会让排查变难。

### D15 同线程准入挂在 `Agent` 上，进程内有效

`threadGate` 按线程 ID 维护忙标记和 FIFO 等待队列。`Agent` 上有一个零值可用的 gate。它不共享给其他 `Agent`，也不跨进程。持久 claim 仍然是跨进程写入的唯一正确性来源。

**为什么：**两个 Agent 共享同一个 Store 时，进程内 gate 不共享；跨进程更不共享。gate 只能优化“本进程内第二个请求怎么办”，不能替代 `BeginRun`/`Claim`。

### D16 入口默认值不同

| 入口 | 默认策略 | 理由 |
| --- | --- | --- |
| `Session.Send` | `AdmissionQueue` | 会话入口，同一会话的两个 Send 排队比直接失败更符合调用方预期。 |
| `Agent.Run` / `Agent.Stream` | `AdmissionFail` | 显式执行入口，调用方通常希望立刻知道线程忙。 |
| `Agent.Resume` | `AdmissionFail` | 恢复是明确的继续操作；忙时先让调用方处理，不能拿着 claim 排队。 |

`WithAdmission` 只覆盖 `Run`/`Stream`/`Session.Send`；本批 `Resume` 不支持按次覆盖。`MaxWaiters <= 0` 归一化为 16。

**为什么：**默认值不能写死在 gate 里，否则 Session 和 Agent 没法不同。`Resume` 的变参是 approvals，不适合再加 `ResumeOption`。

### D17 重入在创建句柄/获取 gate 之前检查

`threadScope` 只记录线程 ID，装在 `RunContext.Context` 里。`streamConfigured` 用调用方 context 检查；`Resume` 用调用方 context 检查。`prepare` 不检查。重入返回 `ErrAdmissionReentrant`，不同 Agent 但同线程的重入也会被检测到。

**为什么：**如果先创建句柄再检查，运行自己的作用域会让检查永远命中。重入排队会自己等自己，必须快速失败。

### D18 排队不持有 claim，FIFO 直接转交名额

`Resume` 和普通运行都在 `BeginRun`/`Claim` 之前拿 gate。等待期间不持有持久 claim。释放时如果队列里有等待者，`busy` 保持 true，名额直接转交给队首；等待者取消且已经被授予名额时，也要把名额转交给下一个。

**为什么：**先 claim 再排队会让 claim 在等待期间过期，其他进程可能抢走；不转交名额会丢掉释放。

### D19 本批不新增准入事件

`RunQueued`/`RunAdmitted` 留到以后和运行登记表一起做。`RunStarted` 仍在 `drive` 开始时发布；排队中的运行会看到 `RunStarted` 但暂时没有模型/工具事件。

**为什么：**新增事件要同步 wire、反序列化、测试和 UI；本批已经有 Task 事件和 BranchCanceled，先控制事件面。

## 4. 内部契约

### 4.1 Task 类型和预算

```go
type TaskKind string

const (
	TaskKindChat     TaskKind = "chat"
	TaskKindWorkflow TaskKind = "workflow"
	TaskKindPlan     TaskKind = "plan"
)

type TaskInfo struct {
	ID       string
	ParentID string
	ThreadID string
	TurnID   string
	Kind     TaskKind
	Depth    int
}

type TaskBudget struct {
	MaxDepth int // 0 → 8
	MaxUnits int // 0 → 64
	MaxTurns int // 0 → 不限制
}

func WithTaskBudget(b TaskBudget) Option

type TaskBudgetError struct {
	Dimension string // "depth" | "units" | "turns"
	Limit     int
	Used      int
	ParentID  string
	Kind      TaskKind
}

func (e *TaskBudgetError) Error() string
func (e *TaskBudgetError) Unwrap() error // ErrTaskBudgetExceeded

var ErrTaskBudgetExceeded = errors.New("agent: task budget exceeded")
```

默认值：

```go
func defaultTaskBudget() TaskBudget { return TaskBudget{MaxDepth: 8, MaxUnits: 64} }
```

### 4.2 RunContext 和 Run

```go
type RunContext struct {
	// ...既有字段

	task   *taskRuntime
	info   TaskInfo
	taskMu sync.Mutex
}

func (rc *RunContext) taskInfo() TaskInfo
func (rc *RunContext) setTaskTurn(turnID string)
func (rc *RunContext) ensureTask() *taskRuntime
func (rc *RunContext) admitChild(kind TaskKind) (TaskInfo, error)
func (rc *RunContext) consumeTurn() error

func (rc *RunContext) subRun(input core.Message, kind TaskKind) (*RunContext, error)
func (rc *RunContext) subRunWithContext(ctx context.Context, input core.Message, kind TaskKind) (*RunContext, error)
func (rc *RunContext) forBranch(branch string, kind TaskKind) (*RunContext, error)
func (rc *RunContext) forBranchWithContext(ctx context.Context, branch string, kind TaskKind) (*RunContext, error)

func (r *Run) Task() TaskInfo
func (a *Agent) taskKind() TaskKind
```

`deeper` 复制 `task` 指针和 `info` 副本，不增加 Depth。`subRun*`/`forBranch*` 调用 `admitChild`，把子 `TaskInfo` 写入新环境；共享线程的派生补父 ThreadID/TurnID。

### 4.3 Task 事件和 BranchCanceled

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

type BranchCanceled struct {
	Name   string
	Reason error
}
```

`TaskDone.Status` 的判定顺序：

| 顺序 | 条件 | Status |
| --- | --- | --- |
| 1 | `Control.Kind == Interrupt` | `paused` |
| 2 | `Err == nil` | `done` |
| 3 | `errors.Is(Err, context.Canceled)` | `canceled` |
| 4 | 其他 `Err != nil` | `failed` |

分支被兄弟取消、工具因此返回 context 错误时记 `canceled`；工具在取消后返回自己的非 context 错误时记 `failed`。判定只看错误本身，不看分支 context 是否已取消，避免把取消后返回的真实失败标成 `canceled`。

### 4.4 工具和执行选项

```go
func WithToolConcurrency(n int) Option // 默认 8；0 不限制；负数在 New 拒绝
```

### 4.5 失败策略和 Parallel 选项

```go
type FailurePolicy int

const (
	CancelSiblings FailurePolicy = iota
	WaitForAll
)

type ParallelOptions struct {
	StateConflict  StateConflictPolicy
	Failure        FailurePolicy
	MaxConcurrency int // <= 0 不限制
}
```

### 4.6 同线程准入

```go
type AdmissionPolicy int

const (
	AdmissionFail AdmissionPolicy = iota
	AdmissionQueue
)

type AdmissionOptions struct {
	Policy     AdmissionPolicy
	MaxWaiters int // <= 0 → 16
}

func WithAdmission(o AdmissionOptions) RunOption

var ErrAdmissionQueueFull = errors.New("agent: same-thread admission queue is full")
var ErrAdmissionReentrant = errors.New("agent: reentrant run on the same thread")
```

## 5. 交付顺序和任务卡

| 阶段 | 任务卡 | 设计章节 | 依赖 | 说明 |
| --- | --- | --- | --- | --- |
| A | TASK-RH-01 | §5.1、§5.2 | 无 | Task 类型、根任务、配置 |
| A | TASK-RH-02 | §5.3–§5.5 | RH-01 | 派生、AsTool、carrier |
| A | TASK-RH-03 | §5.6 | RH-02 | Task 事件和事件 JSON |
| B | TASK-RH-04 | §6 | RH-01 可独立 | 并行工具上限 |
| C | TASK-RH-05 | §7 | RH-02 | 深度/子任务/模型调用预算 |
| D | TASK-RH-06 | §8.1–§8.4、§8.6–§8.7 | RH-02、RH-03、RH-05 | Parallel 失败取消 |
| D | TASK-RH-07 | §8.5 | RH-05、RH-06 | Plan 失败取消 |
| E | TASK-RH-08 | §9.3、§9.4 | RH-01 | gate 数据结构和单元测试 |
| E | TASK-RH-09 | §9.2、§9.5.1、§9.7 | RH-08 | 普通运行接入 |
| E | TASK-RH-10 | §9.5.2、§9.6 | RH-09 | Resume 接入 |
| F | TASK-RH-11 | §11.3、§12 | RH-03–RH-10 | README、注释、示例 |
| F | TASK-RH-12 | §14 | 全部 | 全量门禁和反向验证记录 |

顺序说明：

- §5 先做，因为 §7 的预算计数、§8 的取消边界、§9 的 Task 事件都需要任务身份。没有 Task，预算只能挂在单个循环上，取消也没有统一的子任务边界。
- §6 在 §7 之前实现，但两者互不依赖。工具上限改动最局部，适合先验证“名额、事件顺序、超时放弃”。
- §8 在 §7 之后，因为 `Plan` 要把 `TaskBudgetError` 当作不可重试错误。
- §9 放最后，因为它改变 `Session.Send` 的默认行为，需要单独的文档和回归。
- 任务卡编号不等于设计章节编号；对应关系以本表为准。

## 6. 任务卡

### TASK-RH-01 建立 Task 身份与配置

- 依赖：无。
- 对应设计：§5.1、§5.2。
- 先写失败测试（`agent/task_identity_test.go`、`agent/task_internal_test.go`）：
  - `TestRunTaskRootIdentity`：`Stream` 返回的句柄在驱动前 `Task()` 有 `task_` ID、ThreadID、Kind=chat、Depth=0、ParentID 为空；`Wait` 之后 TurnID 非空，并等于检查点 head 的 TurnID。
  - `TestTaskKindForWorkflowAndPlan`：`Parallel` 根运行的 Kind 是 workflow；`NewPlan` 根运行的 Kind 是 plan。
  - `TestTaskBudgetDefaults`：`WithTaskBudget(TaskBudget{})` 得到 8/64/0；负数在 `New` 返回错误。
  - `TestTaskBudgetDoesNotAffectRunWithoutExplicitOption`：默认配置下普通运行仍能完成。
- 文件：
  - 新增 `agent/task.go`
  - 新增 `agent/task_identity_test.go`
  - 新增 `agent/task_internal_test.go`
  - [agent/agent.go](../../agent/agent.go)
  - [agent/options.go](../../agent/options.go)
  - [agent/runtime.go](../../agent/runtime.go)
  - [agent/run.go](../../agent/run.go)
  - [agent/run_prepare.go](../../agent/run_prepare.go)
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/plan.go](../../agent/plan.go)
- 实施：
  1. 在 `agent/task.go` 定义 `TaskKind`、常量、`TaskInfo`、`TaskBudget`、`taskRuntime`、默认预算、`normalizeTaskBudget`、`TaskBudgetError`、`ErrTaskBudgetExceeded`。本卡先只定义字段和类型，不接预算检查。
  2. `config` 增加 `taskKind TaskKind`、`taskBudget TaskBudget`。`New` 的默认配置填 `TaskKindChat` 和 `defaultTaskBudget()`；校验 `taskBudget` 三个字段不能为负。
  3. `WithTaskBudget(b TaskBudget) Option`：保存原值，由 `New` 归一化。工作流不走 `New`，`wrapWorkflowWithStore` 填 `TaskKindWorkflow` 和 `defaultTaskBudget()`。
  4. `NewPlan` 在 `wrapWorkflowWithStore` 返回后把 `a.cfg.taskKind` 改为 `TaskKindPlan`。`Agent.taskKind()` 返回配置值，空值按 `TaskKindChat`。
  5. `RunContext` 增加 `task *taskRuntime`、`info TaskInfo`、`taskMu sync.Mutex`。实现 `taskInfo()`、`setTaskTurn()`、`ensureTask()`。`ensureTask` 在 `taskMu` 下检查并创建：`task == nil` 时按默认预算建根任务，根 ID 用 `core.NewID("task")`；`info` 为空时补上根 ID、Kind、Depth=0，已有 ThreadID/TurnID 保留。
  6. `newRunHandleWithID` 创建根 `taskRuntime`，`info` 填 ID、ThreadID、Kind、Depth=0，TurnID 留空。`Run` 增加 `Task()`，读 `r.rc.taskInfo()`。
  7. `prepare` 在解析出 `turnID`、调用 `setLineageStart` 之后调用 `r.rc.setTaskTurn(turnID)`（[agent/run_prepare.go:93](../../agent/run_prepare.go#L93)）。TurnID 只填空值，不覆盖已有值。
  8. `deeper`、`subRun`、`forBranch` 暂时只复制 `task` 和 `info`，不创建子任务；下一卡再接 `admitChild`。
- 验证：
  ```bash
  go test ./agent -run 'TestRunTaskRootIdentity|TestTaskKindForWorkflowAndPlan|TestTaskBudgetDefaults|TestTaskBudgetDoesNotAffectRunWithoutExplicitOption' -count=1
  go test ./agent -count=1
  go vet ./agent
  ```
- 完成标准：根 Task 的 ID、ThreadID、Kind、Depth、TurnID 规则稳定；默认预算不影响现有运行；`Run.Task()` 在 `-race` 下无数据竞争。
- 提交：`feat(agent): add runtime task identity`

### TASK-RH-02 派生环境登记子 Task，接入 AsTool

- 依赖：RH-01。
- 对应设计：§5.3、§5.4、§5.5。
- 先写失败测试（`agent/task_derivation_test.go`、`agent/task_internal_test.go`）：
  - `TestSubRunDerivesChildTask`：`subRun` 的子 Task 有新的 ID、ParentID 等于父 ID、Depth 加 1、ThreadID/TurnID 继承父、`task` 指针相同。
  - `TestForBranchDerivesChildTask`：同上，额外断言 `branch` 标签保留。
  - `TestTransferKeepsTaskIdentity`：`deeper` 的 TaskID、ParentID、Depth 与父相同。
  - `TestSubRunWithoutTaskCreatesRoot`：手工构造的 `RunContext` 没有 `task` 时，派生仍能工作，并生成一个根 Task。
  - `TestWithTaskParentCarrier`：`resolveRunConfig` 应用 `withTaskParent` 后，`childTask` 和 `taskTree` 字段正确；`callOne` 包装的 context 能通过 `agentRunContext()` 取回父 `RunContext`。
  - `TestAsToolWithParentAdmitsChild`（内部测试）：父运行里调用 `AsTool`，用模型中间件捕获子运行的 `RunContext`，断言子 TaskInfo 的 ParentID 指向父；ThreadID 是子运行自己的线程，不是父线程。
- 文件：
  - 新增 `agent/task_derivation_test.go`
  - 修改 `agent/task_internal_test.go`
  - [agent/runtime.go](../../agent/runtime.go)
  - [agent/agent.go](../../agent/agent.go)
  - [agent/subagent.go](../../agent/subagent.go)
  - [agent/exectools.go](../../agent/exectools.go)
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/plan.go](../../agent/plan.go)
  - 现有测试调用点：[agent/task_loop_internal_test.go](../../agent/task_loop_internal_test.go)、[agent/state_isolation_test.go](../../agent/state_isolation_test.go)、[agent/stepview_drift_test.go](../../agent/stepview_drift_test.go)、[agent/preempt_internal_test.go](../../agent/preempt_internal_test.go)
- 实施：
  1. `admitChild(kind)`：先 `ensureTask()`，读父 `info`；生成新的 `task_` ID，ParentID 为父 ID，Kind 为传入值，Depth 为父 Depth+1。ThreadID/TurnID 留空。本卡不做预算检查，但后续卡会在这里加。
  2. `subRun`/`subRunWithContext`：构造隔离 State；调用 `admitChild`；把子 info 写入新环境，并填父 ThreadID/TurnID；共享同一个 `task` 指针。`subRun` 等价于 `subRunWithContext(rc.Context, ...)`。
  3. `forBranch`/`forBranchWithContext`：克隆 State；调用 `admitChild`；填子 info 和父 ThreadID/TurnID；共享 `task`。`forBranch` 等价于传入 `rc.Context`。
  4. `deeper`：复制 `task` 和 `info`，不调用 `admitChild`。`transferDepth` 继续加 1。
  5. `RunContext` 增加私有载体：
     ```go
     type runContextCarrier interface{ agentRunContext() *RunContext }
     type runContextCarrierCtx struct {
         context.Context
         rc *RunContext
     }
     func (c runContextCarrierCtx) agentRunContext() *RunContext { return c.rc }
     func runContextOf(ctx context.Context) *RunContext {
         if c, ok := ctx.(runContextCarrier); ok {
             return c.agentRunContext()
         }
         return nil
     }
     ```
     `callOne` 构造 `tool.Context` 时，在 `keepToolUpdates` 外面再包一层 `runContextCarrierCtx`。不要改变 `updaterContext` 的现有行为。`AsTool` 用 `runContextOf(tctx.Context)` 取父环境。
  6. `RunConfig` 增加未导出字段 `childTask *TaskInfo`、`taskTree *taskRuntime`。新增包内 `withTaskParent(parent *RunContext, child TaskInfo) RunOption`。
  7. `streamConfigured` 在 `newRunHandle` 之后，如果 `rc.childTask != nil`，调用 `run.rc.adoptChildTask(rc.taskTree, *rc.childTask, rc.ThreadID)`：替换根 Task 信息，ThreadID 用子运行自己的，TurnID 留空。
  8. `AsTool.Call`：先取 `parent := runContextOf(tctx.Context)`。没有父运行时，按普通 `child.Run` 执行，子运行自建根任务。有父运行时调用 `parent.admitChild(s.child.taskKind())`；失败返回 `tool.ErrorResult(err.Error())`；成功把 `withTaskParent(parent, childInfo)` 加进 `opts` 再调用 `child.Run`。
  9. 更新 `Parallel`、`execNode`、`p.plan`、`maybeReplan` 的 `subRun`/`forBranch` 调用，传入对应 Agent 的 `taskKind()`；`execNode` 用 `w.taskKind()`，planner/replanner 用各自 Agent 的 `taskKind()`。`maybeReplan` 的签名改成返回 `([]nodeSpec, error)`，本卡先把 `subRun` 的错误原样返回，调用方在 `run` 里返回 `runOutcome{Err: err}`。
  10. 更新第 2.1 节列出的现有测试调用点，传 `TaskKindChat` 并处理第二个返回值。
- 验证：
  ```bash
  go test ./agent -run 'TestSubRunDerivesChildTask|TestForBranchDerivesChildTask|TestTransferKeepsTaskIdentity|TestSubRunWithoutTaskCreatesRoot|TestWithTaskParentCarrier|TestAsToolWithParentAdmitsChild' -count=1
  go test ./agent -count=1
  go vet ./agent
  ```
- 完成标准：所有子环境都有 TaskInfo 和共享的 `taskRuntime`；AsTool 子运行能拿到父任务；没有父载体的直接调用仍能工作；现有派生测试签名更新后全绿。
- 提交：`feat(agent): derive child task identities`

### TASK-RH-03 发布 Task 生命周期事件

- 依赖：RH-02。
- 对应设计：§5.6、§11.2。
- 先写失败测试（`agent/task_events_test.go`、`core/eventjson_test.go`）：
  - `TestTaskEventsRoot`：事件顺序是 `RunStarted`、`TaskStarted`、模型/工具事件、`TaskDone`、`RunDone`；TaskStarted 的 TaskID 等于 `run.Task().ID`，TurnID 非空，Depth=0。
  - `TestTaskEventsAsToolChild`：父 Agent 和子 Agent 共享 bus；子运行的 TaskStarted ParentID 等于父 TaskID，ThreadID 是子运行自己的线程，Kind=chat；子运行只发一次 TaskStarted/TaskDone。
  - `TestTaskEventsParallelBranches`：Parallel 的两个分支各有一条 TaskStarted/TaskDone，ParentID 是根 Task，Depth=1；成功状态 `done`。
  - `TestTaskEventsPlanNodes`：计划节点各有 TaskStarted/TaskDone，ParentID 是计划根 Task；planner/replanner 不产生 Task 事件。
  - `TestTaskEventsNoDuplicateForTransferAndSequential`：transfer 和 Sequential 阶段不新增 Task 事件。
  - `TestTaskDoneStatus`：成功 `done`、失败 `failed`、暂停 `paused`、取消 `canceled`。
  - 把 `TaskStarted`、`TaskDone`、`BranchCanceled` 加入 `core/eventjson_test.go` 的往返列表。
- 文件：
  - 新增 `agent/task_events_test.go`
  - [core/eventjson_test.go](../../core/eventjson_test.go)
  - [core/event.go](../../core/event.go)
  - [core/eventjson.go](../../core/eventjson.go)
  - [agent/run.go](../../agent/run.go)
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/plan.go](../../agent/plan.go)
  - `agent/task.go`（修改）
- 实施：
  1. `core/event.go` 增加 `TaskStarted`、`TaskDone`、`BranchCanceled` 和对应的 `isEvent()`。`TaskDone.Status` 注释列全四种状态。
  2. `eventWire` 增加 `TaskID`、`ParentID`、`Kind`、`Depth`、`Name` 字段（`Err` 复用现有字段）。`MarshalEvent` 增加三个 case；`UnmarshalEvent` 增加三个 case。`BranchCanceled.Reason` 序列化到 `Err` 字符串。
  3. `Run.drive` 在 `prepare` 成功、`drainApprovalEvents` 之前发布根 `TaskStarted`；在终结分支之前发布 `TaskDone`。`TaskDone` 的状态由 `terminalOutcome` 的 err/paused 和 `context.Canceled` 判断。prepare 失败和 startErr 路径不发布 Task 事件。
  4. 子 AsTool 运行走同一个 `drive`，因为它有独立的 `Run` 句柄；不需要父运行额外发布。
  5. `Parallel` 每个分支启动前发布 `TaskStarted`，`runChild` 返回后发布 `TaskDone`；事件用 `branch.taskInfo()`。本卡还没有兄弟取消；父取消时工具通常返回 context 错误记 `canceled`，工具返回自己的错误则记 `failed`。
  6. `execNode` 在 `subRun` 成功后发布 `TaskStarted`，`runChild` 返回后发布 `TaskDone`；节点取消时的 `canceled` 状态由 RH-07 接上。planner/replanner 不发。
  7. `taskStatus(out runOutcome) string` 放在 `agent/task.go`，Run、Parallel、Plan 共用。判定顺序按 §4.3：中断 → `paused`；`Err == nil` → `done`；`errors.Is(err, context.Canceled)` → `canceled`；否则 `failed`。
- 验证：
  ```bash
  go test ./agent -run 'TestTaskEvents|TestTaskDoneStatus' -count=1
  go test ./core -run 'TestEventJSONRoundTrip' -count=1
  go test ./agent ./core -count=1
  go test -race ./agent -run 'TestTaskEvents' -count=1
  ```
- 完成标准：Task 事件在正确时机发布、能 JSON 往返、没有重复；现有事件顺序测试不因新增事件出现类型断言失败。
- 提交：`feat(agent): emit task lifecycle events`

### TASK-RH-04 并行工具上限

- 依赖：RH-01 可独立；建议在 RH-03 之后做。
- 对应设计：§6。
- 先写失败测试（`agent/tool_concurrency_test.go`）：
  - `TestToolConcurrencyCapsParallelSegment`：5 个阻塞工具，`WithToolConcurrency(2)`，用按调用释放的测试工具；前两个进入后只释放一个，第三个才开始；任意时刻峰值不超过 2。
  - `TestToolConcurrencyOneSerializesParallelSegment`：`n=1`，3 个调用逐个开始，`ToolStarted` 仍按调用顺序。
  - `TestToolConcurrencyZeroIsUnlimited`：`n=0`，3 个调用可以同时进入。
  - `TestToolConcurrencySerialSegmentUnaffected`：并行调用、串行工具、并行调用的批次，串行段独占，前后并行段峰值不超过 2。
  - `TestToolConcurrencyReleasesAfterTimeout`：`n=1`，第一个工具不返回且 `WithToolTimeout` 触发；第二个调用仍能拿到名额并完成。
  - `TestToolConcurrencyCanceledWaiter`：等待名额时取消运行，不产生多余的 `ToolStarted`，批次不永久阻塞。
  - `TestToolConcurrencyResultOrder`：结果仍按模型调用顺序排列。
- 文件：
  - 新增 `agent/tool_concurrency_test.go`
  - [agent/options.go](../../agent/options.go)
  - [agent/agent.go](../../agent/agent.go)
  - [agent/loop.go](../../agent/loop.go)
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/exectools.go](../../agent/exectools.go)
- 实施：
  1. 定义 `defaultToolConcurrency = 8`。`config` 增加 `toolConcurrency int`，`AgentLoop` 增加同名字段，`newLoop` 复制。
  2. `New` 默认配置填 8，校验负数。`wrapWorkflowWithStore` 填 8。`WithToolConcurrency(n)` 保存原值；0 表示不限制。
  3. 在 `execTools` 的并行段（[agent/exectools.go:168](../../agent/exectools.go#L168)）计算 `limit = l.toolConcurrency`；`limit <= 0 || limit > len(seg.indexes)` 时改为 `len(seg.indexes)`；创建一个容量为 `limit` 的 channel 并预填满。
  4. 启动循环每次先 `select` 取名额或响应 `rc.Context.Done()`。取到名额后再 `checkWriteAccess`；检查失败要把名额还回 channel 并停止分发。然后发布 `ToolStarted`，`wg.Add(1)`，启动 goroutine。
  5. goroutine 内 `defer wg.Done()`、`defer func(){ slots <- struct{}{} }()`，再调用 `run(i, c)`。名额必须在 `run` 返回后归还；`lc.stats.add` 已在 `run` 内完成，不要把它移到释放之后。
  6. `wg.Wait` 仍然在每个并行段末等待全部已启动调用。串行段完全不变。
  7. 更新 `WithToolExecution` 注释：并行段还受 `WithToolConcurrency` 限制。
- 验证：
  ```bash
  go test ./agent -run 'TestToolConcurrency' -count=1
  go test -race ./agent -run 'TestToolConcurrency|TestParallelBatchOverlaps|TestSequentialToolSerializesItselfOnly' -count=1
  go test ./agent -count=1
  ```
- 完成标准：默认上限 8 不改变现有两调用测试；上限、串行边界、超时归还、取消等待、结果顺序都有测试；`-race` 绿。
- 提交：`feat(agent): cap parallel tool calls`

### TASK-RH-05 任务树预算（深度、子任务数、模型调用数）

- 依赖：RH-02。
- 对应设计：§7。
- 先写失败测试（`agent/task_budget_test.go`、`agent/task_budget_internal_test.go`）：
  - `TestTaskBudgetDepth`：在内部测试里从根开始连续嵌套派生；深度 1..8 成功，深度 9 返回 `TaskBudgetError{Dimension:"depth"}`；`errors.Is(err, ErrTaskBudgetExceeded)` 为真。
  - `TestTaskBudgetUnitsCAS`：20 个 goroutine 并发 `admitChild`，MaxUnits=5，恰好 5 个成功，`units` 不超过 5。
  - `TestAsToolBudgetReturnsToolResult`：MaxUnits=1，父模型分两轮各调用一次 `AsTool`；第二轮收到 `tool.ErrorResult`，父运行继续并完成，模型看到预算文本。
  - `TestTaskBudgetTurns`：`TaskBudget{MaxTurns:2}`，模型每次都发起工具调用；第三次模型 attempt 返回预算错误，模型调用次数为 2。
  - `TestConsumeTurnCountsEveryAttempt`：内部单元测试连续调用 `consumeTurn`，第 MaxTurns+1 次返回错误；调用计数准确。
  - `TestPlanBudgetErrorNotRetried`：一个节点、`MaxRetries:2`、MaxUnits=1；第一次执行失败后重试时预算不足，worker 只被调用一次，计划返回预算错误。
  - `TestParallelBudgetStopsAdmission`：3 个分支、MaxUnits=2；运行返回预算错误；内部断言成功登记的 units 不超过 2。不要断言“没有一个分支跑过”，RH-06 会改变启动时机。
  - `TestResumeStartsNewTaskTree`：暂停后恢复，新根 TaskID 与暂停前不同，`turns/units` 从 0 开始。
  - `TestTaskBudgetVsRunBudget`：同时配置任务树 `MaxTurns` 和 `middleware.RunBudget`，两者各自生效；任务树错误是硬错误，middleware 超限仍走收尾。
- 文件：
  - 新增 `agent/task_budget_test.go`
  - 新增 `agent/task_budget_internal_test.go`
  - `agent/task.go`（修改）
  - [agent/runtime.go](../../agent/runtime.go)
  - [agent/loop.go](../../agent/loop.go)
  - [agent/subagent.go](../../agent/subagent.go)
  - [agent/plan.go](../../agent/plan.go)
  - [agent/options.go](../../agent/options.go)
- 实施：
  1. `TaskBudgetError.Error()` 写成可读的维度、上限、已用、父任务和 Kind；`Unwrap` 返回 `ErrTaskBudgetExceeded`。
  2. `admitChild` 增加检查：
     - 父 `Depth+1 > MaxDepth` → `Dimension:"depth"`，`Used = 父Depth+1`，`ParentID = 父ID`。
     - CAS `units`：读旧值，`old+1 > MaxUnits` → `Dimension:"units"`，`Used = old`，不做自增。
  3. `consumeTurn`：`MaxTurns == 0` 直接返回 nil；否则 CAS `turns`，`old+1 > MaxTurns` → `Dimension:"turns"`，`Used = old`。
  4. 在 `runStep` 的 attempt 循环顶部、`beginModelAttempt` 之前调用 `rc.consumeTurn()`（[agent/loop.go:395](../../agent/loop.go#L395) 附近）。失败时用 `l.fail(rc, step, history, lc.viewSnapshot(), err)` 返回，和同一函数里的其他失败路径一致。
  5. `AsTool` 在 RH-02 已经处理 `admitChild` 错误；确认预算错误文本会进入 `tool.ErrorResult`，父运行不失败。
  6. `Parallel` 当前先创建全部分支，`forBranch` 的预算错误会在启动任何 goroutine 之前返回；本卡保持这一点。RH-06 改成边启动边收结果后，预算错误变成启动期错误并取消已启动分支。
  7. `applyResult` 增加不可重试分支：`var be *TaskBudgetError; errors.As(d.err, &be)` 为真时直接标记 `failed`，不增加 Attempts；`FailFast` 下设置 `failFastErr`，`ContinueOnError` 下只标记失败并让依赖被 `skipBlocked` 跳过。本卡不做取消，RH-07 接上 `cancelNodes`。
  8. `maybeReplan` 中 `subRun` 的错误原样返回；如果 `runChild` 返回的错误是 `TaskBudgetError`，也返回给 `run`，不要像普通 replanner 失败那样静默忽略。其他 replanner 错误保持现有“发布 failed 后结束计划”的行为。
  9. `WithTaskBudget` 注释明确：预算属于整棵任务树；暂停恢复重新开始；和 `middleware.RunBudget` 的区别。
- 验证：
  ```bash
  go test ./agent -run 'TestTaskBudget|TestAsToolBudget|TestConsumeTurn|TestPlanBudgetErrorNotRetried|TestParallelBudget' -count=1
  go test -race ./agent -run 'TestTaskBudgetUnitsCAS|TestTaskBudgetTurns' -count=1
  go test ./agent ./middleware -count=1
  ```
- 完成标准：三个维度都能独立触发；CAS 不超限；AsTool 预算错误不杀父运行；计划预算错误不重试；暂停恢复新树；默认预算不破坏现有测试。
- 提交：`feat(agent): enforce task tree budgets`

### TASK-RH-06 Parallel 失败时取消兄弟分支

- 依赖：RH-02、RH-03、RH-05。
- 对应设计：§8.1–§8.4、§8.6–§8.7。
- 先写失败测试（`agent/parallel_failfast_test.go`）：
  - `TestParallelCancelSiblings`：分支 A 立即失败，分支 B 阻塞在监听 context 的工具；运行返回 A 的错误，B 的工具收到取消，B 有 `BranchCanceled` 和 `TaskDone{Status:"canceled"}`。
  - `TestParallelWaitForAll`：`Failure: WaitForAll`，A 失败后 B 仍完整跑完，再返回 A 的错误，不发 `BranchCanceled`。
  - `TestParallelRootErrorDeterministic`：让 B 先失败、A 在取消后返回自己的非 context 错误；最终错误仍是声明顺序靠前的 A，A 的 `TaskDone` 是 `failed` 而不是 `canceled`。
  - `TestParallelCanceledErrorDoesNotOverrideRoot`：B 不监听 context，A 失败后才释放 B，B 返回包装 `context.Canceled` 的错误；最终错误仍是 A，不是 B 的取消错误。
  - `TestParallelPauseDoesNotCancel`：一个分支暂停（复用 `phase1ApprovalGate`），另一个分支完成；运行返回暂停，不发 `BranchCanceled`。
  - `TestParallelMaxConcurrency`：4 个分支、`MaxConcurrency:2`，峰值不超过 2。
- 文件：
  - 新增 `agent/parallel_failfast_test.go`
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/runnable.go](../../agent/runnable.go)（如果 `branchResult` 放这里）
  - [core/event.go](../../core/event.go)
  - [core/eventjson.go](../../core/eventjson.go)
  - `agent/task.go`（修改）
- 实施：
  1. 定义 `FailurePolicy`、`CancelSiblings`、`WaitForAll`；`ParallelOptions` 增加 `Failure`、`MaxConcurrency`。`Parallel` 和 `ParallelWithOptions` 的零值都是 `CancelSiblings`。
  2. 新增 `branchResult{index int; out runOutcome}` 和 `isRealFailure(out) bool { return out.Err != nil }`。
  3. 重写 `parallelRunner.run`：
     - `baseline := cloneKV(rc.State.KV)`。
     - `branchCtx, cancelBranches := context.WithCancel(rc.Context)`，函数返回时 `defer cancelBranches()`。
     - `limit := p.opts.MaxConcurrency`；`<=0 || > len(subs)` 时改为 `len(subs)`。
     - `results := make(chan branchResult, len(subs))`；`children := make([]*RunContext, len(subs))`；`next, inflight := 0, 0`；`var collected []branchResult; var failure, launchErr error`；`settled := make([]bool, len(subs))` 记录哪些分支已经收过结果。
     - 外层循环：有名额、没有停止、`branchCtx.Err()==nil`、（`failure == nil || WaitForAll`）时，按声明顺序调用 `rc.forBranchWithContext(branchCtx, name, kind)`；失败设 `launchErr`、调用 `cancelBranches`、对已启动且 `!settled` 的兄弟发布 `BranchCanceled{Reason: launchErr}`，然后停止启动。成功则 `children[i]=child`、`next++`、`inflight++`，启动 goroutine 执行 `runChild` 并向 channel 发送 `branchResult{index:i, out:out}`。
     - 收结果：`inflight--`、`settled[r.index]=true`、追加 `collected`；如果 `failure == nil && isRealFailure(r.out)`，记录 `failure = r.out.Err`，`CancelSiblings` 下调用 `cancelBranches`，并对已启动且 `!settled` 的兄弟发布 `BranchCanceled{Reason: failure}`。两个触发点共用一个 `announceCanceled(reason)`，用布尔量保证每个分支只发一次。
     - `inflight == 0` 且不能再启动时退出。
     - `failure != nil || launchErr != nil || branchCtx.Err() != nil` 时返回 `runOutcome{Err: chooseRootError(rc.Context, collected, launchErr)}`，不 merge。
     - 否则把 `collected` 按 index 放回 `[]runOutcome`，调用 `p.merge`。
  4. `chooseRootError(parentCtx context.Context, collected []branchResult, fallback error) error`：
     - `parentCtx.Err() != nil` → 返回它。
     - 按 index 升序找第一个 `out.Err != nil` 且 `!errors.Is(out.Err, context.Canceled)` 的结果。
     - 没有非取消错误、`fallback != nil` 时返回 `fallback`；预算等启动错误不能被取消错误覆盖。
     - 否则返回 index 最小的非 nil 错误，保证有错误就不会被当成成功。
     - 完全没有错误时返回 nil。
     - 不看“分支返回时 context 是否已取消”；这个限制写进注释（见 §3 D13）。
  5. `merge` 只在无失败路径调用；现有“暂停/Stop/Escalate 按声明顺序返回”的行为只在无错误时保留。错误优先于暂停，写进注释和测试。
  6. `BranchCanceled` 加入 `core/event.go`、`isEvent`、`eventjson` 的两个 switch。每个被取消的已启动分支只发一次。
  7. 分支 `TaskDone` 的状态用 `taskStatus(out)`；工具返回 context 错误记 `canceled`，返回自己的非 context 错误记 `failed`。RH-03 在分支 goroutine 内发布；本卡把发布移到收集结果时，和 `BranchCanceled` 的顺序才可测。
- 验证：
  ```bash
  go test ./agent -run 'TestParallelCancelSiblings|TestParallelWaitForAll|TestParallelRootErrorDeterministic|TestParallelCanceledErrorDoesNotOverrideRoot|TestParallelPauseDoesNotCancel|TestParallelMaxConcurrency' -count=1
  go test ./agent -run 'TestTaskBudget|TestParallelBudget' -count=1
  go test -race ./agent -run 'TestParallel' -count=1
  go test ./agent ./core -count=1
  ```
- 完成标准：默认取消兄弟；`WaitForAll` 保留旧行为；根因确定；暂停不取消；不合并失败运行；已启动分支都等到退出；`-race` 绿。
- 提交：`feat(agent): cancel sibling branches on parallel failure`

### TASK-RH-07 Plan FailFast 取消在途节点

- 依赖：RH-05、RH-06。
- 对应设计：§8.5、§8.6、§8.7。
- 先写失败测试（`agent/plan_failfast_test.go`）：
  - `TestPlanFailFastCancelsInflight`：两个节点同时运行；A 重试耗尽后最终失败，B 监听 context 并收到取消；B 的 `PlanNodeDone{Status:"canceled"}`，最终错误是 A。
  - `TestPlanFailFastRetryDoesNotCancel`：A 第一次失败但还有重试次数，B 不被取消；A 重试成功后计划继续。
  - `TestPlanFailFastRootErrorNotOverwritten`：两个节点先后失败，最终错误是第一个最终失败节点，不被后一个覆盖。
  - `TestPlanContinueOnErrorUnaffected`：`ContinueOnError` 下失败节点不取消独立分支，`canceled` 状态不出现。
  - `TestPlanParentCancelReturnsContextError`：父 context 取消时，计划返回 `context.Canceled`，不把部分结果当成功。
  - `TestPlanCanceledNodeSkipsDependents`：`skipBlocked` 把 `canceled` 当作失败，依赖节点标记 `skipped`。
- 文件：
  - 新增 `agent/plan_failfast_test.go`
  - [agent/plan.go](../../agent/plan.go)
  - `agent/task.go`（修改）
  - [core/event.go](../../core/event.go)
  - [core/eventjson.go](../../core/eventjson.go)
- 实施：
  1. `planRunner.run` 增加节点 context：
     ```go
     nodeCtx := rc.Context
     var cancelNodes context.CancelFunc
     if p.cfg.policy == FailFast {
         nodeCtx, cancelNodes = context.WithCancel(rc.Context)
         defer cancelNodes()
     }
     ```
  2. `execNode` 增加第一个参数 `ctx context.Context`，用 `rc.subRunWithContext(ctx, core.UserText(input), w.taskKind())`。planner/replanner 仍用 `rc.subRun`（或 `rc.Context`），因为它们不参与节点取消。
  3. 调度器启动节点时把 `nodeCtx` 传给 `execNode`。
  4. 重写 `applyResult` 的决策顺序：
     - 成功：`done`，写输出，不变。
     - 失败且计划已经停止（`*failFastErr != nil`）或 `nodeCtx.Err() != nil`：不重试；`nodeCtx.Err()!=nil` 或 `errors.Is(d.err, context.Canceled)` 时标记 `canceled` 并发布 `PlanNodeDone{Status:"canceled"}`；否则标记 `failed`、发布 failed，但不覆盖 `*failFastErr`。
     - 失败且 `TaskBudgetError`：不重试，进入最终失败处理。
     - 失败且还有重试次数：`Attempts++`、`pending`、发布 `retry`。
     - 最终失败：标记 `failed`，发布 failed；`FailFast` 且 `*failFastErr == nil` 时设置 `fmt.Errorf("plan %q: node %q failed: %w", ...)` 并调用 `cancelNodes()`；已经设置时只记录当前节点，不覆盖。
  5. `saveErr` 分支收集在途结果时也传 `nodeCtx`；不要因为保存失败而取消节点（保持现有“收完再返回保存错误”的行为）。
  6. 主循环每轮开头如果 `rc.Context.Err() != nil`，不再启动新节点、不再 replan；收完在途结果后返回该错误，父取消优先于 `saveErr`。主循环结束后、返回成功结果之前再做一次检查，被取消节点不能留下“计划成功”的假象。
  7. `skipBlocked` 的依赖失败状态集合加入 `"canceled"`。
  8. `PlanNodeDone` 的状态注释加 `"canceled"`（[core/event.go:126](../../core/event.go#L126)）。
  9. 节点 TaskDone 的状态由 `execNode` 里的 `taskStatus(out)` 决定：返回 context 错误记 `canceled`，返回自己的非 context 错误记 `failed`。`PlanNodeDone` 的 `canceled` 仍由 `nodeCtx.Err()!=nil` 决定；两者口径不同时以各自的信息源为准，测试要固定这一点。
- 验证：
  ```bash
  go test ./agent -run 'TestPlanFailFast|TestPlanContinueOnError|TestPlanParentCancel|TestPlanCanceledNode' -count=1
  go test -race ./agent -run 'TestPlan' -count=1
  go test ./agent ./core -count=1
  ```
- 完成标准：FailFast 只在重试耗尽/不可重试时取消；根因不被覆盖；取消节点状态和依赖跳过正确；父取消不被吞掉；`ContinueOnError` 不变。
- 提交：`feat(agent): cancel in-flight plan nodes on fail-fast`

### TASK-RH-08 同线程准入 gate 的数据结构和单元测试

- 依赖：RH-01 可独立。
- 对应设计：§9.3、§9.4。
- 先写失败测试（`agent/admission_internal_test.go`，`package agent`）：
  - `TestThreadGateFIFO`：A 持有名额，B、C 依次排队；释放 A 后 B 先拿到，B 释放后 C 拿到。
  - `TestThreadGateHandoff`：释放时队列非空，`busy` 保持 true，名额直接转交；新来的运行不能插队。
  - `TestThreadGateWaiterCancelBeforeGrant`：B 在队列中取消，C 成为下一个，B 不被授予。
  - `TestThreadGateWaiterCancelAfterGrant`：B 已被授予但调用方 context 先取消，名额转交给 C。
  - `TestThreadGateQueueFull`：`MaxWaiters=1`，第二个等待者返回 `ErrAdmissionQueueFull`。
  - `TestThreadGateDifferentThreads`：不同线程互不阻塞。
  - `TestThreadGateReleaseIdempotent`：重复调用释放不会多转交名额。
- 文件：
  - 新增 `agent/admission.go`
  - 新增 `agent/admission_internal_test.go`
  - [agent/options.go](../../agent/options.go)（如果类型放这里）
- 实施：
  1. 定义 `AdmissionPolicy`、`AdmissionFail`、`AdmissionQueue`、`AdmissionOptions`、`WithAdmission`、`ErrAdmissionQueueFull`、`ErrAdmissionReentrant`。
  2. `WithAdmission` 在 `RunConfig` 上写 `admission` 和 `admissionSet`。`MaxWaiters <= 0` 归一化为 16；未知 policy 在 `streamConfigured`/`Resume` 校验。
  3. 实现 `threadGate`：
     ```go
     type threadGate struct {
         mu      sync.Mutex
         busy    map[string]bool
         waiters map[string][]*gateWaiter
     }
     type gateWaiter struct {
         ready   chan struct{}
         granted bool
     }
     ```
     零值可用；`busy`/`waiters` 在第一次 `acquire` 时惰性初始化。
  4. `acquire(ctx, threadID, policy, maxWaiters)`：
     - 线程空闲且没有等待者，或 policy 是 `AdmissionFail`：立即置忙，返回释放函数。
     - 线程空闲但队列有等待者且 policy 是 `AdmissionQueue`：进入队列，保证 FIFO。
     - 忙 + `AdmissionFail`：返回包装 `checkpoint.ErrClaimConflict` 的错误。
     - 忙 + `AdmissionQueue`：队列长度达到 `maxWaiters` 返回 `ErrAdmissionQueueFull`，否则入队等待。
     - 等待期间 context 取消：未授予则移除；已授予则释放名额并返回 `ctx.Err()`。
  5. `release(threadID)`：队列里有等待者就转交给队首并保持 busy；没有等待者才清空 busy。释放函数用 `sync.Once` 包装。
  6. 不接任何 Run/Agent 代码；本卡只测 gate 本身。
- 验证：
  ```bash
  go test -race ./agent -run 'TestThreadGate' -count=1
  go test ./agent -count=1
  ```
- 完成标准：FIFO、直接转交、取消前/后、队列满、不同线程、幂等释放都稳定；测试在 `-race` 下通过。
- 提交：`feat(agent): add same-thread admission gate`

### TASK-RH-09 普通运行接入同线程准入

- 依赖：RH-08。
- 对应设计：§9.2、§9.5.1、§9.7。
- 先写失败测试（`agent/admission_run_test.go`）：
  - `TestSessionSendQueuesSameThread`：同一个 Session 连续两次 `Send`，第一个模型被阻塞；第二个进入等待；释放第一个后两个都成功，第二次看到第一次提交后的历史。
  - `TestAgentStreamFailsWhenBusy`：第一个运行阻塞时，第二个 `a.Stream(...).Wait()` 返回包装了 `checkpoint.ErrClaimConflict` 的错误，第二个模型没有被调用。
  - `TestSessionSendAdmissionFailOverride`：`Session.Send(..., WithAdmission(AdmissionOptions{Policy: AdmissionFail}))` 在忙时立即失败。
  - `TestAdmissionReentrant`：工具里调用同一 Agent、同一线程的 `Run`，返回 `ErrAdmissionReentrant`，不挂死；不同线程的调用可以正常完成。
  - `TestAdmissionWaiterCanceled`：排队中的第二个运行 context 取消，不执行、不产生检查点，名额转交给下一个。
  - `TestAdmissionDifferentThreadsDoNotBlock`：两个不同线程同时运行互不阻塞。
  - `TestAdmissionReleasedOnPause`：第一个运行暂停后 gate 释放；第二个运行可以开始，随后 `BeginRun` 返回 `ErrResumeRequired`，不是 gate 忙。
- 文件：
  - 新增 `agent/admission_run_test.go`
  - [agent/agent.go](../../agent/agent.go)
  - [agent/options.go](../../agent/options.go)
  - [agent/run.go](../../agent/run.go)
  - [agent/run_prepare.go](../../agent/run_prepare.go)
  - [agent/session.go](../../agent/session.go)
  - `agent/admission.go`（新增）
- 实施：
  1. `Agent` 增加 `gate threadGate`（值字段，零值可用）。这样内部测试直接构造 `&Agent{...}` 也不会 nil panic。
  2. `RunConfig` 增加 `admission AdmissionOptions`、`admissionSet bool`。`resolveRunConfig` 默认 `AdmissionFail` + `MaxWaiters:16`；`WithAdmission` 覆盖并置位。
  3. `Session.Send` 在 `resolveRunConfig` 之后、`streamConfigured` 之前，如果 `admissionSet` 为 false，把 Policy 改成 `AdmissionQueue`。`Agent.Run`/`Stream` 保持 `AdmissionFail`。
  4. `Run` 增加 `admission AdmissionOptions`、`gateRelease func()`（`lifecycleMu` 保护）。`streamConfigured` 把 `rc.admission` 写到 `run.admission`。
  5. `newRunHandleWithID` 在 `context.WithCancel(ctx)` 之后，用 `context.WithValue` 把 `threadScope{threadID}` 装进运行 context。`threadScopeKey` 只存线程 ID。
  6. `streamConfigured` 在创建句柄之前调用 `checkReentrant(ctx, rc.ThreadID)`。失败时仍然返回一个 `Run` 句柄，把错误放进 `run.startErr`，走现有失败路径。校验 `AdmissionPolicy` 合法。
  7. `prepare` 在 context 检查之后、`preparation == nil` 判断之前调用 `acquireAdmission()`；已由 Resume 预置的释放函数直接复用。获取失败返回错误。用命名返回和 defer 在 prepare 失败时释放。
  8. `drive` 的 goroutine 开头 `defer r.releaseAdmission()`。所有出口（startErr、prepare 失败、正常、失败、暂停）都会释放。
  9. `Run.Cancel` 在 `!driveStarted` 时同时释放 gate（和现有 claim 释放放在一起），避免 Resume 句柄未驱动时泄漏。释放操作幂等。
  10. 忙错误格式：`fmt.Errorf("agent: thread %q is busy: %w", threadID, checkpoint.ErrClaimConflict)`。重入错误包装 `ErrAdmissionReentrant`。
- 验证：
  ```bash
  go test ./agent -run 'TestSessionSendQueuesSameThread|TestAgentStreamFailsWhenBusy|TestSessionSendAdmissionFailOverride|TestAdmissionReentrant|TestAdmissionWaiterCanceled|TestAdmissionDifferentThreadsDoNotBlock|TestAdmissionReleasedOnPause' -count=1
  go test -race ./agent -run 'TestAdmission|TestSessionSend' -count=1
  go test ./agent -count=1
  ```
- 完成标准：Session 默认排队；Agent 默认快速失败且仍满足 `errors.Is(err, checkpoint.ErrClaimConflict)`；重入不挂死；暂停释放 gate；现有同线程测试通过。
- 提交：`feat(agent): admit ordinary runs by thread`

### TASK-RH-10 Resume 接入同线程准入

- 依赖：RH-09。
- 对应设计：§9.5.2、§9.6、§9.9。
- 先写失败测试（`agent/admission_resume_test.go`）：
  - `TestResumeFailsFastWhenBusy`：一个普通运行正持有线程；`Resume` 返回包装 `ErrClaimConflict` 的错误；没有进入队列，也没有拿到 claim。
  - `TestResumeReleasesGateOnPreflightError`：对没有暂停的线程调用 `Resume`，返回校验错误；随后同线程的普通运行可以正常开始（gate 没有泄漏）。
  - `TestResumeCancelBeforeDriveReleasesGate`：暂停后 `Resume` 返回句柄，不 `Wait` 直接 `Cancel`；随后普通运行不再得到 gate 忙错误（会得到 `ErrResumeRequired`，因为暂停仍在）。
  - `TestResumeInFlightAdmission`：已有 in-flight 恢复测试继续通过；恢复期间同线程第二个运行快速失败。
  - `TestCrossAgentSameThreadUsesClaim`：两个 Agent 共享同一个 Store，第一个运行活跃时第二个 Agent 的运行返回 `checkpoint.ErrClaimConflict`，证明 gate 不是跨 Agent 的。
  - 现有 `checkpoint` 多进程测试不变，作为跨进程保证。
- 文件：
  - 新增 `agent/admission_resume_test.go`
  - [agent/hitl.go](../../agent/hitl.go)
  - [agent/run.go](../../agent/run.go)
  - `agent/admission.go`（新增）
- 实施：
  1. `Resume` 在 `CheckThreadID` 之后、`store.Latest` 之前调用 `checkReentrant(ctx, threadID)`；然后 `acquire(ctx, threadID, AdmissionFail, 16)`。失败直接返回。`cp.InFlight != nil` 分支把释放函数传给 `resumeInFlight`，不要直接 return。
  2. 拿到 gate 后，任何错误路径（Latest 错误、cp 为空、validatePause、preflightResumeTools、durable.Claim、Decisions、approvals、cloneState、文件快照）都要释放 gate。用一个局部 `releaseGate` 和成功标志控制，成功时把 `releaseGate` 交给 Run。
  3. `resumeInFlight` 增加 `releaseGate func()` 参数（避开现有 claim 的局部 `release`）；它不再自己获取 gate，错误路径释放，成功路径把释放函数挂到 Run 上。
  4. 正常 Resume 路径构造 `run` 后调用 `run.setGateRelease(releaseGate)`；in-flight 路径同样。
  5. `Run.Cancel` 在 `!driveStarted` 时释放 gate；`drive` 的 defer 覆盖已驱动路径。释放幂等。
  6. 不在 `Resume` 里发布准入事件；`RunStarted` 仍然由 `drive` 在真正驱动时发布。
- 验证：
  ```bash
  go test ./agent -run 'TestResumeFailsFastWhenBusy|TestResumeReleasesGateOnPreflightError|TestResumeCancelBeforeDriveReleasesGate|TestResumeInFlightAdmission|TestCrossAgentSameThreadUsesClaim' -count=1
  go test -race ./agent -run 'TestResume|TestAdmission' -count=1
  go test ./agent ./checkpoint -count=1
  ```
- 完成标准：Resume 忙时在 Claim 之前快速失败；所有错误路径释放 gate；未驱动句柄 Cancel 释放 gate；跨 Agent/跨进程仍由 claim 兜底；现有暂停恢复测试全绿。
- 提交：`feat(agent): admit resumed runs by thread`

### TASK-RH-11 文档、注释和离线示例

- 依赖：RH-03–RH-10。
- 对应设计：§11.3、§12。
- 文件：
  - [README.md](../../README.md)
  - [agent/options.go](../../agent/options.go)
  - [agent/workflow.go](../../agent/workflow.go)
  - [agent/runtime.go](../../agent/runtime.go)
  - [core/event.go](../../core/event.go)
  - [docs/analysis/tool-call-concurrency-design.md](../analysis/tool-call-concurrency-design.md)
  - 新增 `examples/runtime-hardening/main.go`
- 实施：
  1. README 同线程段落（[README.md:293](../../README.md#L293)）改为：普通 `Agent.Run/Stream` 仍快速失败；`Session.Send` 默认 FIFO 排队；`Resume` 固定快速失败；持久 claim 仍是跨进程正确性来源。补充 `WithAdmission` 的覆盖方式。
  2. README 增加一段任务树预算说明：默认深度 8、子任务 64、模型调用不限；暂停恢复重新计数；和 `middleware.RunBudget` 的区别；工作流根只使用默认预算。
  3. `WithToolExecution` 注释说明并行段受 `WithToolConcurrency` 限制（[agent/options.go:115](../../agent/options.go#L115)）。
  4. `Parallel` 注释说明默认取消兄弟分支，`WaitForAll` 恢复旧行为（[agent/workflow.go:34](../../agent/workflow.go#L34)）。
  5. `RunContext.deeper/subRun/forBranch` 注释说明 Task 深度和预算（[agent/runtime.go:85](../../agent/runtime.go#L85)、[agent/runtime.go:105](../../agent/runtime.go#L105)、[agent/runtime.go:128](../../agent/runtime.go#L128)）。
  6. `PlanNodeDone` 注释加 `"canceled"`（[core/event.go:126](../../core/event.go#L126)）。
  7. `docs/analysis/tool-call-concurrency-design.md` 加一段“并行段上限”说明，指向本计划，不重写分段设计。
  8. 新增离线示例 `examples/runtime-hardening/main.go`：
     - 使用 mock LLM 和 channel，不访问网络。
     - 演示根 Task 和 AsTool 子 Task 的事件、任务树预算耗尽后的工具错误。
     - 演示 `Parallel` 一个分支失败后另一个分支收到 `BranchCanceled`。
     - 演示同一个 `Session` 两次 `Send` 的 FIFO 执行。
     - 所有并发等待用 channel 或 `context`，不用固定 `sleep` 制造顺序。
- 验证：
  ```bash
  go run ./examples/runtime-hardening
  go test ./examples/... -count=1
  go vet ./examples/...
  ```
- 完成标准：README 和注释与实现一致；示例可重复运行、输出稳定；没有把未实现的可选事件写进文档。
- 提交：`docs: document runtime task and admission behavior`

### TASK-RH-12 全量门禁和反向验证记录

- 依赖：RH-01–RH-11。
- 对应设计：§14。
- 本卡不改生产代码；只补验收记录和必要的测试缺口。
- 实施：
  1. 按第 7 节验收矩阵逐项跑测试，把命令、结果、日期写进“执行记录”。
  2. 按第 8 节反向验证逐项做一次：只改一个真实逻辑点，跑对应测试，确认失败，用 `Edit` 还原。不要用 `git checkout --` 还原，避免连带退回同一文件的后续改动。
  3. 检查 Windows 下的换行：`gofmt -l` 只应报告本次新增/修改且本来就有 CRLF 的文件；新增文件用 LF。不要为了消除既有 CRLF 噪声重排整仓。
  4. 如果全量测试暴露既有事件消费者假设“只有已知事件”，只补类型过滤，不削弱新事件的断言。
- 验证：
  ```bash
  go build ./...
  go vet ./...
  go test ./... -count=1
  go test -race ./agent ./core ./middleware ./checkpoint -count=1
  go run ./examples/runtime-hardening
  ```
- 完成标准：全量构建、vet、测试、race 绿；反向验证每项都有失败证据和还原记录；没有未处理的预算/取消/准入回归。
- 提交：不新增提交；如果补了测试或验收记录，单独提交 `test(agent): cover runtime concurrency hardening`

## 7. 验收矩阵

| 编号 | 场景 | 证据 | 门禁 |
| --- | --- | --- | --- |
| A1 | 根 Task 的身份、TurnID、Kind、Depth | `TestRunTaskRootIdentity`、`TestTaskKindForWorkflowAndPlan` | `go test ./agent` |
| A2 | 子 Task 的父 ID、深度、线程/Turn 归属 | `TestSubRunDerivesChildTask`、`TestForBranchDerivesChildTask`、`TestAsToolWithParentAdmitsChild` | `go test ./agent` |
| A3 | Task 事件和 JSON 往返 | `TestTaskEvents*`、`TestEventJSONRoundTrip` | `go test ./agent ./core` |
| A4 | 工具并行上限、串行边界、超时归还、取消等待 | `TestToolConcurrency*` | `go test -race ./agent` |
| A5 | 深度、子任务数、模型调用数预算 | `TestTaskBudget*`、`TestAsToolBudgetReturnsToolResult` | `go test -race ./agent` |
| A6 | Parallel 取消兄弟、WaitForAll、根因、暂停 | `TestParallel*` | `go test -race ./agent` |
| A7 | Plan FailFast 取消、重试、根因、ContinueOnError | `TestPlan*` | `go test -race ./agent` |
| A8 | gate FIFO、转交、取消、队列满 | `TestThreadGate*` | `go test -race ./agent` |
| A9 | Session 排队、Agent 快速失败、重入、Resume 准入 | `TestSessionSendQueuesSameThread`、`TestAgentStreamFailsWhenBusy`、`TestAdmissionReentrant`、`TestResume*` | `go test -race ./agent` |
| A10 | 旧行为可恢复 | `WithToolConcurrency(0)`、`WaitForAll`、`WithAdmission(AdmissionFail)`、大预算 | 手工 + 测试 |
| A11 | 全仓回归 | `go build ./...`、`go vet ./...`、`go test ./...` | 全绿 |
| A12 | 竞态和泄漏 | `go test -race ./agent ./core ./middleware ./checkpoint` | 全绿 |

## 8. 反向验证清单

每项只改一处真实逻辑，跑对应测试看到预期失败，然后用 `Edit` 还原并重跑确认恢复。不要把“编译失败”当成反向验证通过；不要用 `git checkout --` 还原。

| 编号 | 篡改点 | 期望失败 |
| --- | --- | --- |
| R1 | 删除 `admitChild` 的 `MaxDepth` 检查 | `TestTaskBudgetDepth` 失败 |
| R2 | 把 `units` 的 CAS 改成普通自增并去掉上限 | `TestTaskBudgetUnitsCAS` 超过上限 |
| R3 | 删除 `consumeTurn` 的上限判断 | `TestTaskBudgetTurns` 不返回预算错误 |
| R4 | 把 `AsTool` 的预算错误改成成功返回 | `TestAsToolBudgetReturnsToolResult` 看不到错误结果 |
| R5 | 删除工具名额的 `defer` 归还 | `TestToolConcurrencyReleasesAfterTimeout` 挂起或超时 |
| R6 | 把 `ToolStarted` 移到取名额之前 | `TestToolConcurrencyCapsParallelSegment` 的事件顺序断言失败 |
| R7 | 删除 `cancelBranches()` 调用 | `TestParallelCancelSiblings` 在超时内不返回 |
| R8 | 把 `chooseRootError` 改成按完成顺序选错误 | `TestParallelRootErrorDeterministic` 失败 |
| R9 | 把 `failFastErr` 改回每次覆盖 | `TestPlanFailFastRootErrorNotOverwritten` 失败 |
| R10 | 删除 `cancelNodes()` 调用 | `TestPlanFailFastCancelsInflight` 在超时内不返回 |
| R11 | 删除 gate 释放时的名额转交 | `TestThreadGateFIFO` 挂起或失败 |
| R12 | 删除重入检查 | `TestAdmissionReentrant` 超时 |
| R13 | 删除 prepare 失败路径的 gate 释放 | `TestResumeReleasesGateOnPreflightError` 得到忙错误 |

## 9. 风险与回滚

| 风险 | 影响 | 处理 |
| --- | --- | --- |
| 默认工具并发从“无上限”变成 8 | 高峰批次变慢 | 文档给出 `WithToolConcurrency(0)`；示例和测试覆盖 |
| `Parallel` 默认取消兄弟 | 失败时其他分支提前结束 | `WaitForAll` 恢复旧行为；注释和 README 写清 |
| `Plan` FailFast 取消在途节点 | 节点状态出现 `canceled`，部分工作不再完成 | `ContinueOnError` 或自行收尾；事件注释更新 |
| 默认任务预算限制深度 8/子任务 64 | 大树或大计划会在默认值上失败 | `WithTaskBudget` 调大；文档说明 0 的含义 |
| `Session.Send` 默认排队 | 并发 Send 不再立即报错 | `WithAdmission(AdmissionFail)` 恢复；测试固定 FIFO |
| gate 是进程内的 | 跨进程/多 Agent 仍可能冲突 | 保留 claim 作为唯一正确性来源；测试覆盖跨 Agent |
| 取消是协作式的 | 不监听 context 的工具仍会跑完 | 文档说明；`Parallel` 会等待已启动分支 |
| 新增 Task 事件 | 事件消费者可能不认识 | 更新 `eventjson`；README 说明；消费者应忽略未知类型 |
| 根 Task 在 prepare 后才发 TaskStarted | prepare 失败没有 Task 事件 | 文档说明；失败仍由 `RunFailed` 表达 |
| Windows CRLF 和 `gofmt -l` 噪声 | 误报既有文件 | 只格式化本次文件；新文件用 LF |

回滚顺序：先回滚 RH-11/RH-12 的文档和验收记录，再按 RH-10 → RH-09 → RH-08 回滚准入，然后 RH-07 → RH-06 回滚失败取消，再 RH-05 回滚预算，最后 RH-04 → RH-03 → RH-02 → RH-01。RH-02 的 `subRun`/`forBranch` 签名变化影响面最大，回滚时要一起恢复所有调用点。没有 feature flag；旧行为通过公开选项恢复。

## 10. 明确不做的范围

- 不做 Task 持久化、Task 列表查询、Task 替换。
- 不做跨进程准入队列、分布式信号量。
- 不做跨任务树的全局工具并发名额。
- 不做 token 级任务树预算、成本预算。
- 不给 `Parallel`/`Loop`/`Sequential`/`NewPipeline` 增加预算配置入口。
- 不为 planner/replanner 发 Task 事件。
- 不新增 `RunQueued`/`RunAdmitted` 事件。
- 不改变 checkpoint 格式、revision、fencing token、claim 结构。
- 不把 `maxTransferDepth` 合并进 `TaskBudget.MaxDepth`。
- 不保证取消会回滚外部副作用或已提交的 checkpoint。
- 不把 `Run` 重构成 Codex 的 Task 执行模型。

## 11. 执行记录

| 任务卡 | 状态 | 提交 | 验证证据 | 备注 |
| --- | --- | --- | --- | --- |
| TASK-RH-01 | 已完成 | `e39d372` | `go test ./agent`；`TestRunTaskRootIdentity`、`TestTaskKindForWorkflowAndPlan` 通过 | — |
| TASK-RH-02 | 已完成 | `8700a77` | `go test ./agent`；`TestSubRunDerivesChildTask`、`TestForBranchDerivesChildTask`、`TestAsToolWithParentAdmitsChild` 通过 | — |
| TASK-RH-03 | 已完成 | `0b35b95` | `go test ./agent ./core`；`TestTaskEvents*`、`TestEventJSONRoundTrip` 通过 | — |
| TASK-RH-04 | 已完成 | `6df1f7c` | `go test -race ./agent`；`TestToolConcurrency*` 通过 | — |
| TASK-RH-05 | 已完成 | `0c6fef3` | `go test -race ./agent`；`TestTaskBudget*`、`TestAsToolBudgetReturnsToolResult` 通过 | — |
| TASK-RH-06 | 已完成 | `8538c3e` | `go test -race ./agent`；`TestParallel*` 通过 | — |
| TASK-RH-07 | 已完成 | `4dfdb67` | `go test -race ./agent`；`TestPlan*` 通过 | — |
| TASK-RH-08 | 已完成 | `98afc45` | `go test -race ./agent`；`TestThreadGate*` 通过 | — |
| TASK-RH-09 | 已完成 | `18018df` | `go test -race ./agent`；Session 排队、Agent 快速失败、重入用例通过 | — |
| TASK-RH-10 | 已完成 | `f26a16a` | `go test -race ./agent`；`TestResume*` 通过 | — |
| TASK-RH-11 | 已完成 | `5e7b603` | `go run ./examples/runtime-hardening`；`go test ./examples/... -count=1`；`go vet ./examples/...` | — |
| TASK-RH-12 | 已完成 | 不新增提交 | 见下方 RH-12 验收记录 | 只补记录，未改生产代码 |

### RH-12 验收记录（2026-10-22）

**全量门禁**

- `go build ./...`：通过。
- `go vet ./...`：通过。
- `go test ./... -count=1`：通过，34 个包全部 `ok`。
- `go test -race ./agent ./core ./middleware ./checkpoint -count=1`：通过，无 `DATA RACE`。
- `go run ./examples/runtime-hardening`：通过；连续 5 次运行（含 3 次 `-race`）输出 sha256 完全一致。
- 全量测试没有暴露“事件消费者只认已知事件”的问题，不需要补类型过滤。

**验收矩阵**

- A1：`go test ./agent -run '^(TestRunTaskRootIdentity|TestTaskKindForWorkflowAndPlan)$' -count=1 -v` → 2 个用例通过。
- A2：`go test ./agent -run '^(TestSubRunDerivesChildTask|TestForBranchDerivesChildTask|TestAsToolWithParentAdmitsChild)$' -count=1 -v` → 3 个用例通过。
- A3：`go test ./agent ./core -run '^(TestTaskEvents.*|TestEventJSONRoundTrip)$' -count=1 -v` → 6 个用例通过。
- A4：`go test -race ./agent -run '^TestToolConcurrency' -count=1 -v` → 8 个用例通过。
- A5：`go test -race ./agent -run '^(TestTaskBudget.*|TestAsToolBudgetReturnsToolResult)$' -count=1 -v` → 7 个用例通过。
- A6：`go test -race ./agent -run '^TestParallel' -count=1 -v` → 14 个用例通过。
- A7：`go test -race ./agent -run '^TestPlan' -count=1 -v` → 19 个用例通过。
- A8：`go test -race ./agent -run '^TestThreadGate' -count=1 -v` → 7 个用例通过。
- A9：`go test -race ./agent -run '^(TestSessionSendQueuesSameThread|TestAgentStreamFailsWhenBusy|TestAdmissionReentrant|TestResume.*)$' -count=1 -v` → 40 个用例通过。
- A10：`go test -race ./agent -run '^(TestToolConcurrencyZeroIsUnlimited|TestParallelWaitForAll|TestSessionSendAdmissionFailOverride|TestTaskBudgetDoesNotAffectRunWithoutExplicitOption)$' -count=1 -v` → 4 个用例通过；旧行为分别由 `WithToolConcurrency(0)`、`WaitForAll`、`AdmissionFail` 覆盖。
- A11：全量构建、vet、测试通过，见上。
- A12：四个包 `-race` 通过，无数据竞争，见上。

**反向验证**

每项只改一处真实逻辑，跑对应用例确认失败，再用 `Edit` 还原并重跑确认通过；没有使用 `git checkout --`。

| 编号 | 篡改点 | 观察到的失败 |
| --- | --- | --- |
| R1 | `admitChild` 的 MaxDepth 检查改为恒假 | `TestTaskBudgetDepth`：`depth 9 was admitted under the default depth limit 8` |
| R2 | units 的 CAS 加上限改为普通自增 | `TestTaskBudgetUnitsCAS`：`admitted children = 20, want 5` |
| R3 | `consumeTurn` 的上限判断改为恒假 | `TestTaskBudgetTurns`：`run error = agent: loop exceeded MaxTurns, want *agent.TaskBudgetError` |
| R4 | AsTool 预算拒绝改为成功结果 | `TestAsToolBudgetReturnsToolResult`：`parent answer = "parent saw: ok", want the budget error text` |
| R5 | 删除工具名额的 defer 归还 | `TestToolConcurrencyReleasesAfterTimeout` 20 秒超时 |
| R6 | `ToolStarted` 移到取名额之前 | `TestToolConcurrencyCapsParallelSegment`：连续 5 次 `ToolStarted before a release = [t1 t2 t3], want exactly [t1 t2]` |
| R7 | 删除分支失败后的 `cancelBranches()` | `TestParallelCancelSiblings`：`run did not settle within 5s` |
| R8 | `chooseRootError` 改为按完成顺序 | `TestParallelRootErrorDeterministic`：`run error = b-branch-failed, want the declaration-first branch A error` |
| R9 | “计划已停止”分支入口无条件写 `*failFastErr` | `TestPlanFailFastRootErrorNotOverwritten`：`plan error = b-node-failed, want node A` |
| R10 | 删除 `failPlan` 的 `cancelNodes()` | `TestPlanFailFastCancelsInflight`：`run did not settle within 5s` |
| R11 | 删除 `threadGate.release` 的名额转交 | `TestThreadGateFIFO`：`waiter was not granted` |
| R12 | 删除 `streamConfigured` 的 `checkReentrant` | `TestAdmissionReentrant`：内层运行返回忙错误，`want ErrAdmissionReentrant` |
| R13 | 删除 `resumeInFlight` 预检失败的 `releaseGate()` | `TestResumeReleasesGateOnPreflightError`：`run after a failed in-flight Resume = ... busy ..., want ErrResumeRequired` |

**与卡面的两处差异**

- R9：只删 `failPlan` 里的 `*failFastErr != nil` 守卫不会让用例失败，因为 `applyResult` 已经把“计划已停止”后的结果分流，不再调用 `failPlan`。真正能覆盖根因的位置是该分支入口，篡改这里才复现失败。
- R13：单点删除 `prepare` 失败路径的 defer 不会让 `TestResumeReleasesGateOnPreflightError` 失败——`drive` 的 `defer r.releaseAdmission()` 是兜底，两个守卫互相代偿。该用例实际覆盖的是 Resume 预检失败时的释放，篡改 `resumeInFlight` 的 `releaseGate()` 才复现失败；`prepare` 的释放路径已由 RH-09 的准入用例和全量测试覆盖，本卡不另加用例。

**换行检查**

- 本次涉及的 Go 文件 `gofmt -l` 无输出；新增 `examples/runtime-hardening/main.go` 为 LF（CRLF 计数 0）。
- 没有为消除既有 CRLF 噪声重排任何文件。
