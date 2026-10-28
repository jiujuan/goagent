# P0-2 外层任务循环实施计划

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文只覆盖 P0-2；P0-1（StepView）不在本计划内。

**目标：**按照 [StepView 与任务循环设计草稿](../analysis/stepview-and-task-loop-design-draft.md) §5，把「模型给出终稿」从「运行结束」改成「一次回合结束」：终稿之后如果还有输入，运行进入下一回合；没有输入才关闭输入队列并结束。与此同时，给 `Steer` 增加可查询的投递票据，让每条临时消息要么被投递到某一步，要么得到明确的未投递原因。

**架构：**`AgentLoop.run` 拆成 `prepareRun`、`runTask`、`runStep` 三层；步数上限在最外层固定一次。steering 队列增加关闭状态、票据和「取走或关闭」原子操作；终稿分支调用 `settleTurn` 判断是否继续。只有顶层 LLM 运行续跑，工作流内部的循环保持原有轮次语义。运行的每条结束路径都关闭对应队列，票据不会悬挂。

**技术栈：**Go 1.25（本机 go1.26.4）、标准库、现有 mock LLM、checkpoint Memory/File、HITL 暂停恢复。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-21。
- 基线：`85fbb98`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./agent ./checkpoint ./eval -count=1` 确认基线。
- 设计依据：[stepview-and-task-loop-design-draft.md](../analysis/stepview-and-task-loop-design-draft.md) §5（P0-2：外层任务循环）。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：P0-2。P0-1 的 StepView、视图冻结、检查点工具摘要不在本计划。

当前工作区有未跟踪的分析文档、`docs/adr/ADR-0034-*`、`docs/tasks/plan-step-view.md` 和 `.scratch/` 目录。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。本文和设计草稿的提交由用户另行安排；任务卡里的提交只包含该卡列出的代码、测试、示例和 README。

## 1. 现有能力与实施边界

### 1.1 现在一次运行是怎么结束的

1. `AgentLoop.run` 是单层步骤循环：从 `startStep` 开始，最多跑 `l.maxTurns` 步（[agent/loop.go:115](../../agent/loop.go#L115)、[agent/loop.go:176](../../agent/loop.go#L176)）。
2. 每步开头先把 steering 队列取空并追加到历史（[agent/loop.go:181](../../agent/loop.go#L181)），再读持久收件箱并登记待确认 ID（[agent/loop.go:185](../../agent/loop.go#L185)）。
3. 模型给出没有工具调用的回答时，循环把终稿写进历史、提交检查点、发布 `TurnDone`，然后直接返回终稿（[agent/loop.go:264](../../agent/loop.go#L264) 到 [agent/loop.go:274](../../agent/loop.go#L274)）。这里的检查点固定 `releaseClaim=true`（[agent/loop.go:269](../../agent/loop.go#L269)）。
4. 运行结束时，`Run.drive` 释放占用并根据结果发布 `RunDone`、`RunFailed` 或 `Interrupted`（[agent/run.go:228](../../agent/run.go#L228) 到 [agent/run.go:282](../../agent/run.go#L282)）。
5. `Run.Steer` 只把消息塞进队列，没有返回值（[agent/run.go:144](../../agent/run.go#L144)）；队列本身只有切片，没有关闭状态（[agent/runtime.go:121](../../agent/runtime.go#L121) 到 [agent/runtime.go:142](../../agent/runtime.go#L142)）。
6. 暂停、停止、升级、转移都由循环返回对应的控制指令；运行结束后队列里的消息没有任何人结算。
7. 工作流内部的 LLM 循环与父运行共用同一个 `RunContext`：`Sequential` 和 `Loop` 直接传 `rc`（[agent/workflow.go:92](../../agent/workflow.go#L92)、[agent/workflow.go:116](../../agent/workflow.go#L116)）；转移、并行分支和 DAG 节点各自派生新上下文（[agent/transfer.go:47](../../agent/transfer.go#L47)、[agent/workflow.go:149](../../agent/workflow.go#L149)、[agent/plan.go:393](../../agent/plan.go#L393)、[agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454)）。

### 1.2 本计划要解决的问题

- **终稿即结束。**模型回答完之后到达的追加消息，只能等下一次运行；如果调用方没有再次运行，消息就留在队列里，随运行上下文一起消失。
- **投递不可查询。**`Steer` 没有返回值。调用方不知道消息进了哪一步，也不知道它是否被投递。
- **结束后仍然接受消息。**运行已经结束，队列仍然开放；新的 `Steer` 照常写入，但不会再有人读取，属于静默丢失。
- **占用提前释放。**终稿分支固定释放占用，即使下一回合马上要继续，也会出现线程被第二个写入者拿走的窗口。
- **回合与运行混在一起。**`TurnDone` 现在是运行结束的信号；一次运行内出现多个回合后，这个判断不再成立。

### 1.3 可观察行为变化

| 场景 | 现在 | 本计划之后 | 原因 |
| --- | --- | --- | --- |
| 终稿模型调用期间追加消息 | 消息留在队列，运行结束后丢失 | 运行进入下一回合，消息进入下一步的模型上下文 | 终稿只结束回合 |
| 终稿之后、运行关闭之前追加 | 被接受，随后丢失 | 要么进入下一回合，要么返回 `ErrRunClosed` | 关闭与检查在同一把锁里 |
| 运行结束后追加 | 被接受，随后丢失 | 返回 `ErrRunClosed` | 队列已关闭 |
| 暂停前追加、尚未投递 | 留在旧队列，恢复后不可见 | 票据报告 `ErrRunInterrupted`；恢复使用新队列 | 运行结束即结算 |
| 停止、升级、转移时追加 | 留在队列 | 票据报告 `ErrRunStopped` | 同上 |
| 取消时追加 | 留在队列 | 票据报告 `ErrRunStopped` | 取消同步关闭队列 |
| 续跑期间占用 | 不存在续跑 | 占用保持到整个运行结束 | 检查点不释放占用 |
| 一次运行内的 `TurnDone` | 最多一次 | 可能多次；`RunDone` 只出现一次 | 回合与运行分离 |
| `Session.Send` 的耗时 | 等待一次回答 | 等待整个运行，包括中间的所有回合 | `Send` 本来就等 `Run.Wait`（[agent/session.go:47](../../agent/session.go#L47)） |
| 工作流内部的终稿 | 返回终稿 | 不变 | 只有顶层 LLM 运行续跑 |

这些变化是设计目标，但属于可观察行为。提交说明、示例输出和 README 要写清楚；`TurnDone` 的消费者必须改用 `RunDone` 判断运行结束。

### 1.4 不做什么

- 不实现 P0-1：不冻结 StepView，不改工具广告、参数校验、执行解析和检查点摘要。
- 不新增事件类型。`TurnDone` 的语义仍是「一步结束」，只是允许重复。
- 不把临时消息做成持久消息。需要跨进程、跨暂停的输入仍用持久收件箱。
- 不改变工作流的轮次语义。`Loop` 的每一轮、`Sequential` 的每一段仍然共享父运行上下文和队列。
- 不改变 HITL 恢复协议、占用协议、检查点格式和终态记录。
- 不提供票据取消、队列查看、消息优先级或跨进程 steering。

## 2. 已核实的事实

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `AgentLoop.run` 是单层循环，终稿直接返回 | [agent/loop.go:115](../../agent/loop.go#L115)、[agent/loop.go:264](../../agent/loop.go#L264) | 拆成 `prepareRun`/`runTask`/`runStep` |
| 步数上限每次循环用 `startStep + l.maxTurns` | [agent/loop.go:176](../../agent/loop.go#L176) | 续跑时必须在最外层固定一次 |
| 每步开头 drain steering，再读收件箱 | [agent/loop.go:181](../../agent/loop.go#L181)、[agent/loop.go:185](../../agent/loop.go#L185) | 票据在 drain 时结算 |
| 终稿检查点固定释放占用 | [agent/loop.go:269](../../agent/loop.go#L269) | 改为 `releaseClaim=!continuing` |
| 预算耗尽时用最后一步失败返回 | [agent/loop.go:345](../../agent/loop.go#L345) | 终稿分支要先判断预算 |
| 检查点是按位置传参的私有函数 | [agent/loop.go:376](../../agent/loop.go#L376) | 本计划不改签名，避免和 P0-1 冲突 |
| `Run.Steer` 无返回值，只转发给运行上下文 | [agent/run.go:144](../../agent/run.go#L144) | 改成两返回值 |
| `RunContext.Steer` 只调用 `push` | [agent/runtime.go:116](../../agent/runtime.go#L116) | 同上 |
| steering 队列只有切片和互斥锁 | [agent/runtime.go:121](../../agent/runtime.go#L121) | 移到独立文件并加关闭状态 |
| `Run.drive` 是所有结束路径的汇合点 | [agent/run.go:228](../../agent/run.go#L228) | 兜底关闭队列 |
| `startErr` 在 `drive` 里直接失败 | [agent/run.go:235](../../agent/run.go#L235)、[agent/agent.go:161](../../agent/agent.go#L161) | 失败路径也要关闭队列 |
| `prepare` 失败时直接失败 | [agent/run.go:242](../../agent/run.go#L242)、[agent/run_prepare.go:23](../../agent/run_prepare.go#L23) | 同上 |
| 取消在驱动前同步释放占用 | [agent/run.go:148](../../agent/run.go#L148) | 取消同时同步关闭队列 |
| 运行上下文在句柄创建时构造 | [agent/agent.go:231](../../agent/agent.go#L231)、[agent/agent.go:234](../../agent/agent.go#L234) | 在这里设置 `topLevel` |
| 恢复也走 `newRunHandle` | [agent/hitl.go:150](../../agent/hitl.go#L150) | 恢复的运行同样能拿到正确的 `topLevel` |
| `deeper`、`subRun`、`forBranch` 各自新建队列 | [agent/runtime.go:55](../../agent/runtime.go#L55)、[agent/runtime.go:74](../../agent/runtime.go#L74)、[agent/runtime.go:96](../../agent/runtime.go#L96) | 子上下文默认不是顶层 |
| 转移在父循环返回后运行子代理 | [agent/transfer.go:33](../../agent/transfer.go#L33)、[agent/transfer.go:47](../../agent/transfer.go#L47) | 父队列应在转移前关闭 |
| `Sequential`/`Loop` 把同一个 `rc` 传给成员 | [agent/workflow.go:92](../../agent/workflow.go#L92)、[agent/workflow.go:116](../../agent/workflow.go#L116) | 这两处不能关闭队列 |
| 并行分支使用 `forBranch` 并直接 run | [agent/workflow.go:149](../../agent/workflow.go#L149)、[agent/workflow.go:161](../../agent/workflow.go#L161) | 用 `runChild` 关闭分支队列 |
| DAG 节点、规划器、重规划器使用 `subRun` | [agent/plan.go:393](../../agent/plan.go#L393)、[agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454) | 用 `runChild` 关闭子队列 |
| 收件箱确认 ID 暂存在运行状态里，随检查点提交后清除 | [agent/durable_runtime.go:98](../../agent/durable_runtime.go#L98)、[agent/durable_runtime.go:400](../../agent/durable_runtime.go#L400) | 终稿判断要过滤这些 ID |
| 提交请求由运行状态组装 | [agent/durable_runtime.go:380](../../agent/durable_runtime.go#L380) | 检查点释放标志的入口 |
| 有写保护时提交强制不释放占用 | [agent/write_guard.go:123](../../agent/write_guard.go#L123)、[agent/write_guard.go:147](../../agent/write_guard.go#L147) | 占用连续性测试不能用带协调器的存储 |
| 运行结束时统一释放占用 | [agent/durable_runtime.go:432](../../agent/durable_runtime.go#L432) | 续跑不会让占用悬挂 |
| `Memory.Inbox` 只返回未确认的消息 | [checkpoint/durable_memory.go:87](../../checkpoint/durable_memory.go#L87) | 本步已确认但未提交的消息会再次出现 |
| `InboxMessage` 带稳定 ID | [checkpoint/durable.go:60](../../checkpoint/durable.go#L60) | 过滤待确认消息 |
| `Durable` 接口包含 Claim/ReleaseClaim/Commit | [checkpoint/durable.go:15](../../checkpoint/durable.go#L15) | 只用 Durable 的测试存储可以观察释放标志 |
| `ThreadCoordinator.BeginRun` 是独立能力 | [checkpoint/coordinator.go:36](../../checkpoint/coordinator.go#L36) | 写保护只在这种情况下安装 |
| 运行上下文只有通过协调器才安装写保护 | [agent/run_prepare.go:85](../../agent/run_prepare.go#L85) | 同上 |
| `RunDone` 表示运行成功结束 | [core/event.go:24](../../core/event.go#L24) | 唯一的运行结束信号 |
| `TurnDone` 目前只注释为「一步结束」 | [core/event.go:40](../../core/event.go#L40) | 更新注释说明可重复 |
| `Loop` 的 Gate 依赖共享队列跨轮传递评审意见 | [eval/loop.go:60](../../eval/loop.go#L60)、[eval/loop_test.go:53](../../eval/loop_test.go#L53) | 嵌套循环不能续跑、不能中途关队列 |
| 现有 Steer 调用点都是语句调用 | [agent/failure_test.go:126](../../agent/failure_test.go#L126)、[agent/phase1_interrupt_test.go:587](../../agent/phase1_interrupt_test.go#L587)、[middleware/argguard.go:148](../../middleware/argguard.go#L148)、[middleware/loopguard.go:160](../../middleware/loopguard.go#L160)、[middleware/runbudget.go:230](../../middleware/runbudget.go#L230)、[examples/context-window/main.go:159](../../examples/context-window/main.go#L159)、[examples/agent-tutorial/main.go:279](../../examples/agent-tutorial/main.go#L279) | 两返回值源码兼容 |
| 现有测试辅助可直接复用 | [agent/phase1_interrupt_test.go:20](../../agent/phase1_interrupt_test.go#L20)、[agent/phase1_interrupt_test.go:64](../../agent/phase1_interrupt_test.go#L64)、[agent/failure_test.go:23](../../agent/failure_test.go#L23) | 构造暂停、工具和脚本模型 |
| mock 模型同步调用 responder，可安全阻塞 | [llm/mock/mock.go:35](../../llm/mock/mock.go#L35) | 用通道制造终稿窗口 |

## 3. 锁定的设计决定

### D1 只有顶层 LLM 运行续跑

`RunContext` 增加不导出的 `topLevel bool`，在 `newRunHandleWithID` 里设置为 `a.loop != nil`（[agent/agent.go:231](../../agent/agent.go#L231)）。`deeper`、`subRun`、`forBranch` 都不复制这个字段，默认 `false`。

**为什么：**`Sequential` 和 `Loop` 把同一个 `rc` 交给成员（[agent/workflow.go:92](../../agent/workflow.go#L92)、[agent/workflow.go:116](../../agent/workflow.go#L116)），`eval.Gate` 在 `AfterModel` 里 `Steer` 评审意见，靠下一轮循环取走（[eval/loop.go:60](../../eval/loop.go#L60)）。如果内层 LLM 循环在终稿时续跑，它会自己消费评审意见，`Loop` 的轮次语义被破坏，`TestGateStopsAtMaxRounds`（[eval/loop_test.go:53](../../eval/loop_test.go#L53)）可能直接失败。

**影响：**顶层 LLM Agent 的一次运行可以包含多个回合；工作流、DAG 节点、转移代理、并行分支、`AsTool` 子代理内部的循环保持「终稿即返回」。`AsTool` 走的是完整的 `child.Run`，它自己的运行是顶层，但它没有对外暴露 `Run` 句柄，外部无法追加消息，因此不受影响。

### D2 队列有明确的关闭状态，关闭原因先到先得

队列增加 `closed bool` 和 `err error`。`push` 在关闭后返回 `nil, closedErr`；`close` 幂等，保留第一次关闭的原因。

**为什么：**取消、循环返回、运行驱动三处都会关闭同一个队列。如果后到的关闭覆盖先到的原因，票据会报告错误的原因。

**影响：**取消与正常结束同时发生时，票据可能得到 `ErrRunStopped` 或 `ErrRunClosed`，以先发生者为准。测试只对可控时序断言。

### D3 票据的投递时点

- 步开头 drain：消息追加到本步历史，票据结算为 `Delivered{Step: step}`，不等本步检查点。
- 终稿判断 drain：消息追加到历史，随终稿的检查点一起提交；检查点成功后票据结算为 `Delivered{Step: step+1}`；检查点失败则结算为未投递 `ErrRunClosed`。
- 终稿判断预算耗尽：已经 drain 的消息结算为未投递 `ErrMaxTurnsExceeded`，不追加到历史。

**为什么：**设计草稿 §5.6 规定「已投递」只表示进入某一步的模型上下文，不表示检查点已经提交。步开头符合这一点；终稿边界没有「下一步」的检查点可用，只能以终稿自己的检查点作为消息进入历史的提交点。先提交再结算，可以避免检查点失败后票据却报告已投递。

**影响：**票据的 `Step` 始终是「消息真正会出现在模型请求里的那一步」；终稿边界是 `step+1`。

### D4 终稿判断的顺序，以及有追加时不读收件箱

`settleTurn` 的顺序固定为：

```text
1. 如果没有下一步预算：drain 临时消息并按 ErrMaxTurnsExceeded 结算；关闭队列；结束
2. drain 临时消息；非空则追加到历史，返回继续
3. 读持久收件箱；过滤本步已登记的确认 ID；还有新消息则返回继续（不追加、不确认）
4. 在同一把锁里 drainOrClose：
   - 取到消息：追加到历史，返回继续
   - 关闭成功：返回结束
```

**为什么：**设计草稿 §5.4 在临时消息非空时仍然读收件箱。本计划改为跳过。收件箱读取可能很慢；如果在读取期间运行失败或检查点写不进去，已经 drain 的临时消息就失去了队列位置，也无法结算。跳过读取后，这些消息在下一步开头照常投递，持久收件箱里的消息也留到下一步处理，顺序和结果都更简单。

**影响：**临时消息优先于收件箱被处理；两者都非空时，本回合只由临时消息触发续跑，收件箱消息在下一回合开头投递。

### D5 终稿判断必须过滤本步已经登记的收件箱确认

新增 `RunContext.pendingInboxAcks() map[string]struct{}`，返回当前运行已经登记但尚未随检查点提交的消息 ID（数据来自 [agent/durable_runtime.go:98](../../agent/durable_runtime.go#L98) 的 `ackMessageIDs`）。`settleTurn` 读到收件箱后，跳过这些 ID。

**为什么：**这是对设计草稿 §5.4 的必要修正。现在的流程是：本步开头读收件箱、把消息追加到历史、把 ID 登记为待确认；确认要等本步检查点提交后才从收件箱消失（[agent/durable_runtime.go:400](../../agent/durable_runtime.go#L400)）。终稿判断发生在检查点之前，此时 `Inbox` 仍会返回这条消息。不过滤的话，运行会误以为「又有新输入」，多跑一回合，并把同一条消息再追加一次。

**影响：**`TestConsumedInboxDoesNotContinue` 是这条决定的回归测试。过滤只针对「本运行已经追加过」的 ID，不会漏掉真正的新消息。

### D6 预算在最外层固定一次

`prepareRun` 计算 `maxStep = startStep + l.maxTurns`，`taskState` 携带它。`settleTurn` 用 `step+1 >= maxStep` 判断还有没有下一步预算。预算耗尽时：

- 不进入下一回合；
- 终稿正常提交并返回；
- 已经 drain 的临时消息结算为未投递 `ErrMaxTurnsExceeded`；
- 不读、不确认持久收件箱；
- 关闭输入队列。

**为什么：**设计草稿 §5.5 要求最后一步的终稿仍然有效，而不是把回答丢掉换成 `ErrMaxTurnsExceeded`。续跑不能重置预算，否则不断追加消息会让运行无限延长。

**影响：**`runTask` 循环退出后的 `ErrMaxTurnsExceeded` 只用于「最后一步是工具步，没有产生终稿」的情况，和现在的行为一致。

### D7 续跑期间保持占用

终稿分支的检查点改为 `releaseClaim=!continuing`；继续时传 `false`，结束或失败时传 `true`。占用最终仍由 `releaseDurableClaim` 在运行结束时释放（[agent/durable_runtime.go:432](../../agent/durable_runtime.go#L432)）。

**为什么：**继续跑时如果释放占用，另一个运行可以在本运行写历史的同时拿到同一线程。设计草稿 §5.3 明确要求继续时 `releaseClaim=false`。

**影响：**对于实现 `ThreadCoordinator` 的存储，写保护本来就把提交请求的释放标志强制设为 `false`（[agent/write_guard.go:147](../../agent/write_guard.go#L147)），占用会一直保持到运行结束；检查点标志只在「只用 Durable 能力、没有写保护」的存储上产生差别。测试必须用后一种存储才能验证这条决定，不能用 `Memory` 直接验证。

### D8 关闭位置固定为五处

| 位置 | 关闭对象 | 原因 |
| --- | --- | --- |
| `settleTurn` 的结束分支 | 顶层运行的队列 | `ErrRunClosed` |
| `AgentLoop.run` 的顶层收尾 | 顶层运行的队列（兜底） | 运行结果对应的原因 |
| `runChild` | 派生出来的子上下文队列 | 子运行结果对应的原因 |
| `Run.drive` | 根运行上下文队列（兜底） | 运行结果对应的原因 |
| `Run.Cancel` | 根运行上下文队列（同步） | `ErrRunStopped` |

所有关闭都幂等，重复调用保留第一次的原因。

**为什么：**如果只在 `drive` 关闭，转移期间父队列仍然开放，父循环返回后追加的消息会被接受，却永远不会被投递；如果只在循环里关闭，工作流根运行和 `startErr`/`prepare` 失败路径会漏掉。`runChild` 覆盖转移、并行分支和 DAG 节点；`Sequential`/`Loop` 共享父上下文，不能在其中关闭。

### D9 关闭原因映射

新增 `runOutcome.inputCloseReason() error`：

| 运行结果 | 原因 |
| --- | --- |
| `Err != nil` | `ErrRunClosed` |
| `Control.Kind == core.Interrupt` | `ErrRunInterrupted` |
| `Control.Kind` 为 `Stop`、`Escalate`、`Transfer` | `ErrRunStopped` |
| 其他（正常终稿、预算结束） | `ErrRunClosed` |

`startErr` 和 `prepare` 失败直接用 `ErrRunClosed`；`Cancel` 直接用 `ErrRunStopped`。

**影响：**暂停的运行里残留票据报告 `ErrRunInterrupted`，停止/升级/转移报告 `ErrRunStopped`，正常结束和失败报告 `ErrRunClosed`。暂停前已经投递的消息不受影响。

### D10 嵌套上下文与子运行的收口

新增：

```go
func runChild(rc *RunContext, r Runnable) runOutcome
```

它运行 `r.run(rc)`，返回前关闭 `rc` 的队列，并返回原结果。用在：

- 转移：`return runChild(rc.deeper(), target.runnable)`（替换 [agent/transfer.go:47](../../agent/transfer.go#L47) 的直接调用）；
- 并行分支：分支 goroutine 里用 `runChild(branches[i], p.subs[i].runnable)`（替换 [agent/workflow.go:161](../../agent/workflow.go#L161)）；
- DAG 节点、规划器、重规划器：三处 `rc.subRun(...)` 的调用（[agent/plan.go:393](../../agent/plan.go#L393)、[agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454)）。

`Sequential`/`Loop` 的成员调用保持原样，不关闭共享队列。

**为什么：**每个派生上下文都有自己的队列；这些队列没有外部调用方，但中间件可能持有 `LoopContext` 并调用 `Steer`。不关闭的话，票据永远不结算。共享父上下文的两个工作流必须跨轮保留队列，不能在这里关闭。

### D11 不新增事件，`TurnDone` 允许重复

不新增 `TurnStarted`/`TurnDone` 之外的事件。`core.TurnDone` 的注释改成「一次回合结束，同一运行可以出现多次」；`core.RunDone` 的注释强调它是唯一的成功运行结束信号。`TurnStarted` 在续跑时会再次出现。

**为什么：**设计草稿 §5.8 明确要求事件消费者用 `RunDone` 判断运行结束。仓库内没有把 `TurnDone` 当结束信号的消费者；`core/eventjson.go` 只做序列化，不受影响。

### D12 检查点函数签名本计划不改

`AgentLoop.checkpoint` 仍然使用现有的位置参数 `(rc, step, pending, pause, releaseClaim, extra...)`（[agent/loop.go:376](../../agent/loop.go#L376)）。P0-1 计划会把它改成 `commitOptions`；两份计划谁先落地，另一份在 rebase 时适配。本计划只改终稿分支传的 `releaseClaim` 值。

**为什么：**避免 P0-1 和 P0-2 互相等待。P0-1 与 P0-2 都会改 `loop.go` 的终稿分支，冲突不可避免，但可以限制在一个函数签名上。

### D13 票据 API 只做投递结算

`SteerTicket` 只有 `Done`、`Outcome`、`Wait` 三个方法；不提供取消、不提供消息 ID、不提供队列长度。`Outcome` 不阻塞：未结算时返回零值；要阻塞等待用 `Wait`。票据只结算一次，结算后关闭 `done`。

**为什么：**设计草稿 §5.6 只要求「已投递/未投递 + 步骤 + 原因」。取消和队列查看会引入新的生命周期问题，本计划不做。

**影响：**调用方如果只关心「消息是否被接受」，可以只看 `Steer` 的 error；关心投递结果时再等票据。

## 4. 公共契约

### 4.1 签名

```go
// Run.Steer 把消息放进当前运行的输入队列。
// 队列开放时返回票据；运行已结束或不再接收输入时返回 nil 和对应错误。
func (r *Run) Steer(msg core.Message) (*SteerTicket, error)

// RunContext.Steer 与 Run.Steer 相同；中间件通过 LoopContext 调用的是同一个方法。
func (rc *RunContext) Steer(msg core.Message) (*SteerTicket, error)
```

现有调用点都是语句调用，源码不需要修改。把方法赋给函数变量或在自定义接口里声明 `Steer(core.Message)` 的代码会编译失败；仓库内没有这种用法。

### 4.2 票据

```go
type SteerTicket struct {
    // 不导出字段
}

// Done 在票据结算后关闭。
func (t *SteerTicket) Done() <-chan struct{}

// Outcome 返回当前结算结果；尚未结算时返回零值。
func (t *SteerTicket) Outcome() SteerOutcome

// Wait 等待结算；ctx 先结束时返回 ctx.Err()。
func (t *SteerTicket) Wait(ctx context.Context) (SteerOutcome, error)

type SteerOutcome struct {
    Delivered bool
    Step      int   // Delivered 为 true 时有效，表示消息进入的步号
    Err       error // Delivered 为 false 时说明原因
}
```

### 4.3 错误

```go
var (
    // ErrRunClosed：运行已结束或输入队列已关闭，消息没有被接受。
    ErrRunClosed = errors.New("agent: run is not accepting steering messages")

    // ErrRunInterrupted：运行暂停，残留的临时消息没有投递。
    ErrRunInterrupted = errors.New("agent: run paused; steering message was not delivered")

    // ErrRunStopped：运行被停止、升级、转移或取消，残留的临时消息没有投递。
    ErrRunStopped = errors.New("agent: run stopped; steering message was not delivered")
)
```

预算用尽时票据的 `Err` 复用已有的 `ErrMaxTurnsExceeded`（[agent/loop.go:18](../../agent/loop.go#L18)）。

### 4.4 结算规则

| 情况 | `Steer` 的返回 | 票据 `SteerOutcome` | 消息是否进入历史 |
| --- | --- | --- | --- |
| 队列开放，下一步开头取走 | 票据，nil | `Delivered=true, Step=下一步` | 是 |
| 终稿边界取走，检查点成功 | 票据，nil | `Delivered=true, Step=step+1` | 是 |
| 终稿边界取走，检查点失败 | 票据，nil | `Err=ErrRunClosed` | 内存中有，未提交 |
| 终稿后预算耗尽 | 票据，nil | `Err=ErrMaxTurnsExceeded` | 否 |
| 运行正常结束或失败时仍在队列 | 运行结束前：票据，nil；结束后：nil，`ErrRunClosed` | `Err=ErrRunClosed` | 否 |
| 暂停时仍在队列 | 暂停后：nil，`ErrRunInterrupted` | `Err=ErrRunInterrupted` | 否 |
| 停止/升级/转移时仍在队列 | 结束后：nil，`ErrRunStopped` | `Err=ErrRunStopped` | 否 |
| 取消时仍在队列 | 取消后：nil，`ErrRunStopped` | `Err=ErrRunStopped` | 否 |

约定：

- 一张票据只结算一次，结算后 `Done` 关闭；`Wait` 不会永远等待。
- 运行结束后，队列里没有未结算的条目。
- 「已投递」只保证消息进入了对应步骤的模型上下文，不保证该步之后一定会成功。
- 临时消息一旦投递，就成为普通历史消息，会随该步检查点落盘；这不是新增的持久化通道。

### 4.5 事件

不新增事件类型。文档注释调整为：

- `TurnStarted`：一步开始；续跑时同一次运行可以出现多次。
- `TurnDone`：一次回合结束；同一次运行可以出现多次，不代表运行结束。
- `RunDone`：运行成功结束的唯一信号。

## 5. 交付顺序与依赖

```text
TL-01 队列、票据与关闭状态
  └─ TL-02 循环拆成任务层和步骤层（纯重构）
       └─ TL-03 终稿后的输入判断与续跑
            └─ TL-04 暂停、停止、转移的关闭原因与收口
                 └─ TL-05 示例、README 与全量验收
```

- TL-01 先落地，后续改动都建立在票据和关闭状态上。
- TL-02 是纯重构，现有测试就是门禁；它让 TL-03 的改动集中在一个分支。
- TL-03 引入行为变化，必须先有 TL-01/TL-02。
- TL-04 补齐暂停、停止、转移的原因映射和测试；这些路径在 TL-01 里先以 `ErrRunClosed` 兜底。
- TL-05 只写示例和文档，最后跑全量门禁。

每张卡独立提交，提交信息使用仓库现有风格，不带任务编号。

## 6. 任务卡

### TASK-TL-01：队列、票据与关闭状态

**目标：**引入可查询的投递票据和队列关闭状态；`Steer` 返回票据；步开头取走的消息结算为已投递；运行结束、取消和派生子运行结束时关闭队列并结算残留票据。行为上不引入续跑。

**先写测试：**

- [ ] `agent/steering_internal_test.go`（`package agent`）：
  - [ ] `TestSteeringQueuePushDrainSettlesTickets`：同一把锁内按顺序取走；票据还没有结算（结算由循环完成）。
  - [ ] `TestSteeringQueueCloseSettlesRemainingOnce`：关闭后残留条目全部结算为未投递；重复关闭保留第一次原因。
  - [ ] `TestSteeringQueueDrainOrCloseIsAtomic`：空队列变为关闭；非空队列取走条目且保持开放。
  - [ ] `TestSteerTicketWaitHonorsContext`：未结算的票据在 ctx 取消时返回 `ctx.Err()`。
- [ ] `agent/steering_test.go`（`package agent_test`）：
  - [ ] `TestSteerTicketDeliveredAtStep`：第一步调用一个阻塞工具；工具阻塞期间 `Steer`；放行后第二步取走消息。断言票据 `Delivered=true, Step=1`，且模型在第二步看到了消息。
  - [ ] `TestSteerAfterRunClosed`：运行结束后调用 `Steer`，返回 `nil, ErrRunClosed`。
  - [ ] `TestSteerTicketSettledOnRunFailure`：模型在下一步取走消息前失败；票据为未投递 `ErrRunClosed`。
  - [ ] `TestCancelSettlesSteeringTicket`：模型阻塞时 `Steer` 再 `Cancel`；不释放模型也能在超时内等到票据 `ErrRunStopped`。随后放行模型，等运行结束。
- [ ] `agent/steering_internal_test.go` 另加：
  - [ ] `TestDriveStartErrClosesSteeringQueue`：构造一个带 `startErr` 的运行，先 `Steer`，再 `drive`/`Wait`；票据结算为 `ErrRunClosed`。
  - [ ] `TestRunChildClosesChildQueue`：派生上下文的队列里有票据；`runChild` 返回后票据已结算。

测试辅助建议放在同一个测试文件里：一个带 `started`/`release` 通道的模型 responder，以及一个阻塞工具。所有阻塞点都在 `t.Cleanup` 里放行，避免测试失败时留下 goroutine。

**实现：**

- [ ] 新建 `agent/steering.go`，把 `steeringQueue` 从 `agent/runtime.go:121` 移过来，并加入：
  - `entries []steerEntry`、`closed bool`、`err error`；
  - `push(msg) (*SteerTicket, error)`；
  - `drain() []steerEntry`；
  - `drainOrClose(err) ([]steerEntry, bool)`；
  - `close(err)`，幂等，保留第一次原因；
  - `SteerTicket`、`SteerOutcome` 和三个错误值；
  - `settleDelivered(entries, step)`、`settleUndelivered(entries, err)` 两个包内辅助函数。
- [ ] `agent/runtime.go` 删除旧的队列类型；`RunContext.Steer` 改为返回 `(*SteerTicket, error)`。
- [ ] `agent/runtime.go` 的 `RunContext` 增加 `topLevel bool`；`agent/agent.go:234` 构造时设置 `topLevel: a.loop != nil`。`deeper`/`subRun`/`forBranch` 不复制。
- [ ] `agent/loop.go:181` 的步开头改为取 `[]steerEntry`，把 `.msg` 追加到历史，取完后调用 `settleDelivered(entries, step)`。
- [ ] `agent/run.go:144` 的 `Run.Steer` 改为透传两个返回值；`Cancel` 在取消上下文后同步 `r.rc.steering.close(ErrRunStopped)`。
- [ ] `agent/run.go` 的 `drive`：`startErr` 分支、`prepare` 失败分支和 `runnable.run` 返回后都调用 `r.rc.steering.close(ErrRunClosed)`。TL-04 会把后两处改成结果对应的原因。
- [ ] `agent/runnable.go` 增加 `runChild(rc, r)`，返回前关闭 `rc.steering`（本卡用 `ErrRunClosed`）。
- [ ] 用 `runChild` 替换转移、并行分支和 DAG 三处直接调用（[agent/transfer.go:47](../../agent/transfer.go#L47)、[agent/workflow.go:161](../../agent/workflow.go#L161)、[agent/plan.go:393](../../agent/plan.go#L393)、[agent/plan.go:409](../../agent/plan.go#L409)、[agent/plan.go:454](../../agent/plan.go#L454)）。
- [ ] 更新所有需要传入新参数的内部调用；现有 `Steer` 语句调用不改。

**验证：**

```bash
go test ./agent -run 'TestSteer|TestSteering|TestCancelSettles|TestRunChild|TestDriveStartErr' -count=1
go test ./agent ./checkpoint ./eval -count=1
go test -race ./agent -run 'TestSteer|TestSteering|TestCancelSettles|TestRunChild' -count=1
```

**完成标准：**

- 队列开放时 `Steer` 返回票据；关闭后返回 `nil` 和具体原因。
- 步开头取走的消息票据结算为 `Delivered`，步号正确。
- 运行失败、正常结束、取消、子运行结束时，残留票据全部结算，没有悬挂。
- `Sequential`/`Loop` 的行为没有变化，`eval` 测试保持通过。

**提交说明：**`feat(agent): give steering messages delivery tickets`

### TASK-TL-02：把循环拆成任务层和步骤层

**目标：**纯重构。把 `AgentLoop.run` 拆成 `prepareRun`、`runTask`、`runStep`，为 TL-03 的续跑准备结构。本卡不改变任何行为、事件顺序、步号、检查点内容或错误。

**先写测试：**

- [ ] 不新增用例。现有 `agent`、`eval` 的测试就是门禁；重构前后逐项保持一致。
- [ ] 记录重构前的基线：`go test ./agent ./eval -count=1` 全绿，并保存输出到临时日志，便于对比失败用例名。

**实现：**

- [ ] 新增 `taskState`：

```go
type taskState struct {
    step      int
    maxStep   int
    history   []core.Message
    system    string
    usePrompt bool
}
```

- [ ] `prepareRun(rc) (taskState, runOutcome, bool)`：搬移现有 `run` 的前半段：
  - 复制 `rc.State.Messages`；
  - 处理 `rc.resumed` 批次，提前返回暂停、失败或控制指令；
  - 恢复批次结束后设置 `startStep = rb.step + 1`；
  - 渲染一次系统提示；
  - 计算 `maxStep = startStep + l.maxTurns`。
- [ ] `runTask(rc, st) runOutcome`：`for st.step < st.maxStep` 调用 `runStep`；`again=false` 时返回结果；循环退出时调用 `l.fail(rc, st.maxStep-1, st.history, ErrMaxTurnsExceeded)`。
- [ ] `runStep(rc, st) (runOutcome, taskState, bool)`：搬移现有循环体。
  - 步开头、准备阶段、模型调用、工具执行、检查点、`TurnDone` 的先后顺序不变。
  - 原来的 `continue` 改成返回 `taskState{step: step+1, maxStep: st.maxStep, history: history, system: st.system, usePrompt: st.usePrompt}` 和 `again=true`。
  - 所有 `return` 分支改成 `out, st, false`；`st.history` 在失败路径上使用当前 `history`。
  - 终稿分支暂时保持原样：`checkpoint(..., true)` 后返回终稿。
- [ ] `run` 变成：

```go
func (l *AgentLoop) run(rc *RunContext) runOutcome {
    st, out, ok := l.prepareRun(rc)
    if !ok {
        return out
    }
    return l.runTask(rc, st)
}
```

**验证：**

```bash
go test ./agent ./eval -count=1
go test -race ./agent -count=1
go vet ./agent
```

**完成标准：**

- 全量测试与重构前一致，没有新增失败或跳过。
- `git diff` 只包含 `agent/loop.go` 的结构调整，没有行为改动、没有新的错误值或事件。
- 现有 HITL、转移、工作流、检查点用例全部通过。

**提交说明：**`refactor(agent): split the run loop into task and step phases`

### TASK-TL-03：终稿后判断输入并续跑

**目标：**终稿分支调用 `settleTurn`：有临时消息或新的收件箱消息就继续下一回合；没有输入才关闭队列并返回。预算固定，续跑不释放占用。只有 `topLevel` 运行启用这套逻辑。

**先写测试：**

- [ ] `agent/task_loop_test.go`（`package agent_test`）：
  - [ ] `TestFinalSteeringStartsNextTurn`：模型第一次调用阻塞；阻塞期间 `Steer`；返回终稿后运行进入第二步，模型在第二步看到消息并给出第二个终稿。断言票据 `Delivered=true, Step=1`，最终结果是第二个终稿。
  - [ ] `TestFinalInboxStartsNextTurnOnce`：第一步模型阻塞期间，通过 `Durable.Deliver` 投递收件箱消息；终稿后继续；第二步历史里只有一份该消息；运行结束后收件箱为空。
  - [ ] `TestConsumedInboxDoesNotContinue`：运行开始前投递收件箱消息；第一步开头消费并登记确认；终稿后不得继续。断言只有一次 `TurnStarted`，模型只被调用一次，历史里只有一份消息。
  - [ ] `TestSettleTurnSkipsInboxWhenSteeringPending`：用计数包装的 `Durable`；终稿边界有临时消息时，运行进入下一回合。断言第二步模型调用时收件箱读取总次数为 2（第一步开头一次、第二步开头一次），说明终稿判断没有额外读取。
  - [ ] `TestSteerDuringInboxReadStartsNextTurn`：包装的 `Inbox` 在终稿判断时阻塞；阻塞期间 `Steer`；放行后读取返回空，`drainOrClose` 取到消息并继续；票据已投递。
  - [ ] `TestFinalTurnBudgetSettlesSteering`：`MaxTurns=1`；模型阻塞期间 `Steer`，同时投递一条收件箱消息；放行后返回终稿。断言结果是终稿、票据未投递 `ErrMaxTurnsExceeded`、随后 `Steer` 返回 `ErrRunClosed`、收件箱消息仍未确认。
  - [ ] `TestInboxReadFailureFailsAfterFinal`：第一步模型阻塞期间投递收件箱消息；终稿判断时 `Inbox` 返回错误。断言运行失败、错误被返回、终稿已写入检查点、收件箱消息未确认。
  - [ ] `TestContinuationKeepsClaim`：使用只实现 `Durable`、不实现 `ThreadCoordinator` 的包装存储；通过 HITL 暂停后 `Resume` 得到 claim；恢复后的模型第一次调用阻塞，期间 `Steer`，随后终稿续跑。断言终稿检查点的 `CommitRequest.ReleaseClaim` 为 `false`；运行结束后包装存储收到 `ReleaseClaim`。
  - [ ] `TestContinuationEventOrder`：收集事件；断言两次 `TurnStarted{0}`、`TurnStarted{1}`，两次 `TurnDone`，只有一次 `RunDone`，且 `RunDone` 在最后。
  - [ ] `TestContinuationRespectsTotalBudget`：`MaxTurns=3`；第一个终稿窗口追加的消息进入第二步，第二个终稿窗口追加的消息进入第三步，第三个终稿窗口再追加一次；断言总共三次 `TurnStarted`，第三次的票据为 `ErrMaxTurnsExceeded`，结果仍为第三次终稿。
  - [ ] `TestSessionSendWaitsForContinuation`：通过 `Session.Send` 触发运行；在第一次终稿窗口追加消息；`Send` 返回第二个终稿，而不是第一个。
- [ ] `agent/task_loop_internal_test.go`（`package agent`）：
  - [ ] `TestNestedLoopDoesNotConsumeSteering`：构造一个 `topLevel=false` 的循环上下文；终稿后队列保持开放，消息不被取走；由外部关闭后票据才结算。
- [ ] 回归门禁：`eval.TestGateStopsAtMaxRounds` 必须保持通过。

**实现：**

- [ ] `agent/durable_runtime.go` 增加：

```go
func (rc *RunContext) pendingInboxAcks() map[string]struct{}
```

读取 `ackMessageIDs` 的快照；空时返回 `nil`。

- [ ] `agent/runnable.go` 增加 `runOutcome.inputCloseReason()`。本卡先返回 `ErrRunClosed` 作为默认；TL-04 补齐暂停/停止/转移的映射。
- [ ] `agent/loop.go` 的 `run` 对顶层运行做兜底关闭：

```go
func (l *AgentLoop) run(rc *RunContext) (out runOutcome) {
    if rc.topLevel {
        defer func() { rc.steering.close(ErrRunClosed) }()
    }
    st, out, ok := l.prepareRun(rc)
    if !ok {
        return out
    }
    return l.runTask(rc, st)
}
```

- [ ] `agent/loop.go` 新增 `settleTurn(rc, st, history) (nextHistory []core.Message, entries []steerEntry, continuing bool, err error)`，按 D4 的顺序实现：
  - 预算判断在第一位（见 D6）；
  - `drain` 非空时追加消息并返回继续；
  - 读收件箱后先取 `pendingInboxAcks()`，跳过已登记 ID；有新的未登记消息就返回继续，不追加、不确认；
  - 最后调用 `drainOrClose(ErrRunClosed)`。
- [ ] 终稿分支改为：

```go
if len(calls) == 0 {
    if l.outputKey != "" {
        rc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: l.outputKey, Value: final.Text()})
    }

    if !rc.topLevel {
        // 工作流内部循环保持原有轮次语义。
        rc.State.Messages = history
        if err := l.checkpoint(rc, step, nil, nil, true); err != nil {
            return runOutcome{Err: err}, st, false
        }
        rc.publish(core.TurnDone{Step: step})
        return runOutcome{Result: core.Result{Message: final}}, st, false
    }

    nextHistory, entries, continuing, err := l.settleTurn(rc, st, history)
    if err != nil {
        return l.fail(rc, step, history, err), st, false
    }
    rc.State.Messages = nextHistory
    if err := l.checkpoint(rc, step, nil, nil, !continuing); err != nil {
        settleUndelivered(entries, ErrRunClosed)
        return runOutcome{Err: err}, st, false
    }
    if continuing {
        settleDelivered(entries, step+1)
    }
    rc.publish(core.TurnDone{Step: step})
    if continuing {
        return runOutcome{}, taskState{
            step: step + 1, maxStep: st.maxStep,
            history: nextHistory, system: st.system, usePrompt: st.usePrompt,
        }, true
    }
    return runOutcome{Result: core.Result{Message: final}}, st, false
}
```

- [ ] 保持其它步骤分支不变；`runTask` 的预算失败仍是 `maxStep-1`。
- [ ] 确认 `Sequential`/`Loop` 的成员运行 `topLevel=false`，不会进入 `settleTurn`。

**验证：**

```bash
go test ./agent -run 'TestFinal|TestConsumedInbox|TestSettleTurn|TestContinuation|TestInboxRead|TestSessionSend|TestNestedLoop' -count=1
go test ./agent ./eval -count=1
go test ./eval -run TestGate -count=1
go test -race ./agent -run 'TestFinal|TestContinuation|TestSettleTurn|TestConsumedInbox|TestContinuationKeepsClaim' -count=1
```

**完成标准：**

- 终稿后追加的临时消息进入下一回合，票据步号为下一步。
- 收件箱消息不会因为「本步已消费但未提交」而重复投递或多跑一回合。
- 预算耗尽时终稿正常返回，临时消息得到 `ErrMaxTurnsExceeded`，收件箱不被确认。
- 收件箱读取失败时运行失败，终稿已写入检查点，消息未确认。
- 续跑期间检查点不释放占用；运行结束后占用释放。
- `TurnDone` 可以重复，`RunDone` 只出现一次。
- 工作流内部的轮次语义不变。

**提交说明：**`feat(agent): continue a run when input arrives after the final answer`

### TASK-TL-04：暂停、停止、转移的关闭原因与收口

**目标：**把 TL-01 的默认关闭原因改为按运行结果映射；确认暂停、停止、升级、转移、取消、工作流根运行都在正确的时机以正确的原因关闭队列；更新事件注释。

**先写测试：**

- [ ] `agent/task_loop_internal_test.go`：
  - [ ] `TestInputCloseReasonTable`：表驱动覆盖 `Err`、`Interrupt`、`Stop`、`Escalate`、`Transfer`、正常结果。
  - [ ] `TestNestedContextsAreNotTopLevel`：`deeper`、`subRun`、`forBranch` 的 `topLevel` 都是 `false`，且都持有独立队列。
- [ ] `agent/task_loop_test.go`：
  - [ ] `TestPauseSettlesSteeringAsInterrupted`：模型调用期间 `Steer`，随后 `BeforeTool` 中断；票据为 `ErrRunInterrupted`；暂停后的 `Steer` 返回 `ErrRunInterrupted`；检查点历史里没有这条消息；恢复后的新运行可以正常 `Steer`。
  - [ ] `TestStopSettlesSteeringAsStopped`：`AfterModel` 返回 `Stop`；模型阻塞期间追加的消息票据为 `ErrRunStopped`。
  - [ ] `TestTransferSettlesParentSteering`：父模型返回 `Transfer`，目标代理阻塞；父运行的票据在目标运行期间就已结算为 `ErrRunStopped`；父运行的 `Steer` 返回 `ErrRunStopped`。
  - [ ] `TestWorkflowRootClosesSteering`：`Sequential` 的最后一个成员阻塞期间 `Steer`；工作流结束后票据为 `ErrRunClosed`，随后 `Steer` 返回 `ErrRunClosed`。
- [ ] 事件回归：确认 `TurnDone` 相关的现有测试和 `core/eventjson` 往返测试全部通过。

**实现：**

- [ ] 在 `agent/runnable.go` 实现 `inputCloseReason`：

```go
func (o runOutcome) inputCloseReason() error {
    switch {
    case o.Err != nil:
        return ErrRunClosed
    case o.Control.Kind == core.Interrupt:
        return ErrRunInterrupted
    case o.Control.Kind == core.Stop,
        o.Control.Kind == core.Escalate,
        o.Control.Kind == core.Transfer:
        return ErrRunStopped
    default:
        return ErrRunClosed
    }
}
```

- [ ] `agent/run.go` 的 `drive`：`runnable.run` 返回后用 `out.inputCloseReason()` 关闭；`startErr` 和 `prepare` 失败仍用 `ErrRunClosed`。
- [ ] `runChild` 改用 `out.inputCloseReason()`。
- [ ] `AgentLoop.run` 的顶层兜底改用 `out.inputCloseReason()`（命名返回值）。
- [ ] `Run.Cancel` 保持 `ErrRunStopped`。
- [ ] `core/event.go` 更新 `TurnDone` 和 `RunDone` 的注释，说明回合与运行的区别；不改变类型和 JSON 形状。
- [ ] 确认转移时序：`llmRunner` 先调用 `l.run`，循环的 defer 在返回前关闭父队列，之后才进入转移分支调用目标（[agent/transfer.go:33](../../agent/transfer.go#L33) 到 [agent/transfer.go:47](../../agent/transfer.go#L47)）。测试要在目标运行时检查父票据已经结算。
- [ ] 检查暂停恢复：旧运行队列关闭后，`Resume` 构造的新上下文使用新队列；`TestResumedSteeringDeliveryOrder` 保持通过（[agent/phase1_interrupt_test.go:768](../../agent/phase1_interrupt_test.go#L768)）。

**验证：**

```bash
go test ./agent -run 'TestPause|TestStop|TestTransfer|TestWorkflowRoot|TestInputCloseReason|TestNestedContext|TestResumedSteering' -count=1
go test ./agent ./core ./eval -count=1
go test -race ./agent -run 'TestPause|TestStop|TestTransfer|TestWorkflowRoot' -count=1
```

**完成标准：**

- 暂停的票据报告 `ErrRunInterrupted`，暂停后的 `Steer` 返回同一原因。
- 停止、升级、转移、取消的票据报告 `ErrRunStopped`。
- 正常结束和失败的票据报告 `ErrRunClosed`。
- 转移在目标运行开始前关闭父队列，不会接受新的父级追加。
- 工作流根运行结束后队列关闭；`Sequential`/`Loop` 跨轮共享队列的语义不变。
- 事件注释与实际语义一致。

**提交说明：**`feat(agent): settle steering when a run pauses or stops`

### TASK-TL-05：示例、README 与全量验收

**目标：**用一个离线示例把「终稿后追加 → 进入下一回合 → 票据结算 → 运行结束拒绝追加」完整跑一遍；更新 README；跑全量门禁并填写执行记录。

**先写测试/示例：**

- [ ] 新建 `examples/task-loop/main.go`，离线运行，不访问网络：
  - 模型第一次调用输出「初稿」；测试主协程在模型阻塞点调用 `run.Steer`，然后放行；
  - 模型第二次调用检查历史里有追加消息，输出「修订稿」；
  - 打印票据的 `Delivered` 和 `Step`；
  - 收集并打印两次 `TurnStarted`、两次 `TurnDone`、一次 `RunDone`；
  - 运行结束后再调用一次 `Steer`，打印 `ErrRunClosed`；
  - 用通道同步，不使用 `time.Sleep` 猜时序。
- [ ] 示例输出先实际运行，再写入执行记录；不得照格式编造。

**实现：**

- [ ] 示例代码顶部写清用途和运行命令；只使用公开 API。
- [ ] `README.md` 的「入门与核心循环」表格加一行：

```markdown
| `task-loop` | `go run ./examples/task-loop` | 终稿后的追加消息与投递票据：一次运行两个回合，`TurnDone` 可出现多次、`RunDone` 只出现一次。 |
```

  表格位置在 [README.md:186](../../README.md#L186) 附近。
- [ ] 在「从示例走向应用」之后、「会话快照与并发」附近加一小节（[README.md:267](../../README.md#L267)、[README.md:276](../../README.md#L276)），说明：
  - 临时追加用 `Run.Steer`，返回票据；
  - 票据只保证进入某一步的模型上下文；
  - 运行结束以 `RunDone` 为准，`TurnDone` 只表示回合；
  - 工作流内部循环不续跑；
  - 跨进程、跨暂停的输入用持久收件箱。
- [ ] 检查示例目录是否需要在其它文档中登记；只改 README 和 `docs/tasks` 内的执行记录，不改无关文档。

**验证：**

```bash
go run ./examples/task-loop
go test ./... -count=1
go vet ./...
go build ./...
gofmt -l agent/steering.go agent/loop.go agent/runnable.go agent/runtime.go agent/run.go agent/durable_runtime.go agent/task_loop_test.go agent/task_loop_internal_test.go agent/steering_test.go agent/steering_internal_test.go examples/task-loop/main.go
```

`gofmt -l` 在 Windows 上会对既有 CRLF 文件误报；只对本次新建或修改的文件判读结果。既有文件如果被 `gofmt -l` 标记但 diff 里没有换行变化，不要重排整份文件。

**完成标准：**

- 示例离线可运行，输出与执行记录一致。
- README 的表格和说明与实现一致，没有夸大「已投递」的含义。
- `go test ./...`、`go vet ./...`、`go build ./...` 全绿。
- 本文 §10 填写完整的执行记录和实际命令输出摘要。

**提交说明：**`docs(examples): demonstrate steering tickets across turns`

## 7. 验收矩阵

| 设计 §5.12 项 | 任务卡 | 测试 | 判据 |
| --- | --- | --- | --- |
| 1. 终稿期间追加 | TL-03 | `TestFinalSteeringStartsNextTurn` | 进入下一步；票据 `Delivered=true, Step=1` |
| 2. 终稿瞬间追加 | TL-03 | `TestSteerDuringInboxReadStartsNextTurn`、`TestSteeringQueueDrainOrCloseIsAtomic` | 只出现「进入下一轮」或「`ErrRunClosed`」；没有留在队列里的消息 |
| 3. 没有追加 | TL-01/TL-03 | `TestSteerAfterRunClosed`、`TestFinalSteeringStartsNextTurn` 的收尾断言 | 终稿后队列关闭；票据没有悬挂 |
| 4. 收件箱有待处理 | TL-03 | `TestFinalInboxStartsNextTurnOnce` | 继续；消息只出现一次；最终确认 |
| 5. 收件箱读取失败 | TL-03 | `TestInboxReadFailureFailsAfterFinal` | 运行失败；终稿写入检查点；消息未确认 |
| 6. 暂停 | TL-04 | `TestPauseSettlesSteeringAsInterrupted` | 票据 `ErrRunInterrupted`；消息不在检查点；恢复使用新队列 |
| 7. 停止/转移 | TL-04 | `TestStopSettlesSteeringAsStopped`、`TestTransferSettlesParentSteering` | 票据 `ErrRunStopped`；父队列在目标运行前关闭 |
| 8. 预算边界 | TL-03 | `TestFinalTurnBudgetSettlesSteering` | 返回终稿；票据 `ErrMaxTurnsExceeded`；收件箱未确认 |
| 9. 占用连续 | TL-03 | `TestContinuationKeepsClaim` | 续跑检查点 `ReleaseClaim=false`；结束才释放 |
| 10. 步数上限不被重置 | TL-03 | `TestContinuationRespectsTotalBudget` | 总步数不超过 `MaxTurns`；最后一步的追加得到 `ErrMaxTurnsExceeded` |
| 11. 事件顺序 | TL-03 | `TestContinuationEventOrder` | 多个 `TurnDone`；一个 `RunDone` 且最后出现 |
| 12. 兼容 | TL-02/TL-03 | `go test ./agent ./eval`，含 `TestGateStopsAtMaxRounds` | 现有语句调用、暂停恢复、失败检查点、Gate 轮次全部通过 |
| 已消费收件箱不重复 | TL-03 | `TestConsumedInboxDoesNotContinue` | 只有一次 `TurnStarted`；历史只有一份消息 |
| 有临时消息时不读收件箱 | TL-03 | `TestSettleTurnSkipsInboxWhenSteeringPending` | 收件箱读取次数不增加 |
| 票据不悬挂 | TL-01/TL-04 | 队列内部测试、取消、暂停、停止、转移、子运行测试 | 每个票据最终 `Done` 关闭且结果确定 |
| 工作流根运行关闭 | TL-04 | `TestWorkflowRootClosesSteering` | 结束后票据结算，后续 `Steer` 返回 `ErrRunClosed` |
| 会话等待整个运行 | TL-03 | `TestSessionSendWaitsForContinuation` | `Send` 返回第二个终稿 |

## 8. 风险、兼容性与注意事项

### 8.1 API 兼容性

- `Run.Steer` 和 `RunContext.Steer` 从无返回值变成两个返回值。Go 允许把有返回值的调用当语句使用，仓库内所有调用点都是语句调用（[agent/failure_test.go:126](../../agent/failure_test.go#L126)、[middleware/argguard.go:148](../../middleware/argguard.go#L148) 等），源码不需要修改。
- 风险在仓库外：把 `Steer` 赋给函数变量、或在自定义接口里声明 `Steer(core.Message)` 的代码会编译失败。提交说明要写清楚。
- `SteerTicket`、`SteerOutcome` 和三个错误是纯新增，不影响现有代码。

### 8.2 行为变化

- 运行结束后 `Steer` 从「静默接受」变成返回 `ErrRunClosed`。忽略返回值但仍监测错误的调用方会看到新错误；这是设计目标。
- 一次运行可能包含多个回合，`Session.Send` 和 `Run.Wait` 的耗时会变长，直到运行真正结束。
- `TurnDone` 可能重复。仓库内没有把它当结束信号的消费者；仓库外的适配器必须改用 `RunDone`。
- 终稿之后读取持久收件箱会给每个终稿增加一次存储读操作。存储慢时，终稿到 `RunDone` 的延迟增加。
- 收件箱读取失败会让一个已经产生终稿的运行失败。这是设计草稿 §8 的选择：宁可失败，也不静默丢消息。

### 8.3 占用与存储

- 对实现 `ThreadCoordinator` 的存储，写保护本来就会强制 `ReleaseClaim=false`（[agent/write_guard.go:147](../../agent/write_guard.go#L147)），检查点里的释放标志不产生差别。验证占用连续性必须使用只实现 `Durable` 的包装存储。
- 续跑不释放占用，占用到期由续租循环维持；运行结束时 `releaseDurableClaim` 统一释放（[agent/durable_runtime.go:432](../../agent/durable_runtime.go#L432)）。
- 预算耗尽和收件箱读取失败都不能留下未结算的票据；测试要显式断言 `Done` 已关闭。

### 8.4 测试注意事项

- 所有阻塞点必须用通道和 `t.Cleanup` 放行，不能用 `time.Sleep` 制造时序。mock 模型的 `Generate` 同步调用 responder（[llm/mock/mock.go:35](../../llm/mock/mock.go#L35)），阻塞 responder 是安全的。
- `TestCancelSettlesSteeringTicket` 只断言票据在取消后立即结算，不断言运行最终返回错误还是结果；取消后的模型可能仍然返回，运行可能正常收尾。
- `TestConsumedInboxDoesNotContinue` 是设计修正 D5 的回归用例；如果删掉 `pendingInboxAcks` 过滤，它必须失败。
- 收件箱错误测试需要包装存储，包装类型要同时满足运行使用的 `Store` 能力；只嵌入 `checkpoint.Durable` 即可隐藏 `ThreadCoordinator` 的 `BeginRun`。
- 竞争检测下不要并行运行同一个阻塞模型实例；每个用例单独构造模型、工具和存储。

### 8.5 与 P0-1 的关系

- 两份计划都会修改 `agent/loop.go` 的终稿分支和步循环。P0-1 计划把 `checkpoint` 改成 `commitOptions`；本计划保持现有签名，只改 `releaseClaim` 的值。谁先落地，另一份在 rebase 时适配。
- 本计划不依赖 StepView，也不读取 `LoopContext` 的视图字段。
- 两份计划的测试文件不重叠；P0-1 的 `step_view_test.go` 和本计划的 `steering_test.go`、`task_loop_test.go` 各自独立。

### 8.6 工作流兼容性

- `eval.Gate` 依赖共享队列跨轮传递评审意见。`topLevel=false` 让内层循环不续跑、不关闭队列，这是 `TestGateStopsAtMaxRounds` 继续通过的前提。
- `Sequential` 的每个成员共享同一个队列；如果某个成员是 LLM 循环，它在终稿后返回，队列保持开放，下一段继续使用。这是现有行为。
- `Parallel` 的每个分支有自己的队列；`runChild` 在分支结束时关闭。父运行无法向分支队列追加消息，票据只对分支内部可见。
- DAG 的节点和规划器使用 `subRun`；同样由 `runChild` 关闭。
- 转移：顶层父循环在返回 `Transfer` 时关闭自己的队列；目标代理使用新队列。父队列的残留票据报告 `ErrRunStopped`。

### 8.7 Windows 与格式

- 新文件使用 LF。既有文件多为 CRLF，`gofmt -l` 可能把既有文件全部标出来；只看本次 diff 是否引入格式问题，不要整份重排。
- 提交前逐文件 `git diff --check`，避免行尾空白。

## 9. 明确不做的事

- 不做 P0-1 的 StepView、视图冻结、工具摘要和恢复核对。
- 不新增 `RunClosed` 之类的事件；不改变事件 JSON 形状。
- 不把 steering 队列持久化；跨进程和跨暂停仍用 `Durable.Deliver`/`Inbox`。
- 不改变 `Session` 的公开方法；`Send` 的耗时变化是语义结果，不是新 API。
- 不做消息优先级、去重、取消、超时或队列容量限制。
- 不改变 HITL 恢复的审批合并、claim 协议和工具执行重放。
- 不让工作流内部循环、转移目标、并行分支或 DAG 节点续跑。
- 不改变 `MaxTurns` 的含义；续跑消耗同一个预算。
- 不修改 `checkpoint` 包的存储格式和接口。

## 10. 执行记录

实施时逐卡填写。没有实际运行的命令和输出不要填。

| 任务卡 | 状态 | 提交 | 验证命令 | 结果摘要 | 备注 |
| --- | --- | --- | --- | --- | --- |
| TASK-TL-01 | 已完成 | `9096656` | `go test ./agent -run 'TestSteer\|TestSteering\|TestCancelSettles\|TestRunChild\|TestDriveStartErr' -count=1`<br>`go test ./agent ./checkpoint ./eval -count=1`<br>`go test -race ./agent -run 'TestSteer\|TestSteering\|TestCancelSettles\|TestRunChild\|TestDriveStartErr' -count=1`<br>`go build ./...`<br>`go test ./middleware -count=1` | 定向用例通过；agent/checkpoint/eval 全量通过；-race 通过；全仓构建通过；middleware 通过 | 代码按 P0-1 已落地的 `commitOptions` 接口适配；`topLevel` 字段已加入，续跑判断留待 TL-03 |
| TASK-TL-02 | 已完成 | `b3b849c` | `go test ./agent ./eval -count=1`（重构前后各一次，前者存 `.scratch/tl02-baseline.log`）<br>`go test -race ./agent -count=1`<br>`go vet ./agent` | 重构前后均全绿，无新增失败；-race 与 vet 通过 | 纯重构，diff 仅 `agent/loop.go`；按 P0-1 已落地的 `commitOptions` 状态拆分 |
| TASK-TL-03 | 已完成 | `76b4385` | `go test ./agent -run 'TestFinal\|TestConsumedInbox\|TestSettleTurn\|TestContinuation\|TestInboxRead\|TestSessionSend\|TestNestedLoop' -count=1`<br>`go test ./agent -run TestSteerDuringInboxReadStartsNextTurn -count=1`（补跑，见备注）<br>`go test ./agent ./eval -count=1`<br>`go test ./eval -run TestGate -count=1`<br>`go test -race ./agent -run 'TestFinal\|TestContinuation\|TestSettleTurn\|TestConsumedInbox\|TestContinuationKeepsClaim' -count=1`<br>`go build ./...`<br>`go vet ./agent` | 定向用例通过；agent 13.215s、eval 0.169s 全量通过；Gate 通过；-race 1.239s 通过；构建与 vet 无输出 | Durable-only 存储的收尾检查点以 `ReleaseClaim=true` 原子释放占用（提交后 `d.claim` 清空），故 `TestContinuationKeepsClaim` 改为断言该字段为 `true`、且存储不再收到单独的 `ReleaseClaim` 调用；卡内正则 `TestInboxRead` 匹配不到 `TestSteerDuringInboxReadStartsNextTurn`，已单独补跑；`inputCloseReason` 本卡只返回默认 `ErrRunClosed`，TL-04 接线；`TestConsumedInboxDoesNotContinue`、`TestNestedLoopDoesNotConsumeSteering` 为回归守卫，实施前即通过 |
| TASK-TL-04 | 已完成 | `517f26a` | `go test ./agent -run 'TestPause\|TestStop\|TestTransfer\|TestWorkflowRoot\|TestInputCloseReason\|TestNestedContext\|TestResumedSteering' -count=1`<br>`go test ./agent ./core ./eval -count=1`<br>`go test -race ./agent -run 'TestPause\|TestStop\|TestTransfer\|TestWorkflowRoot' -count=1`<br>`go build ./...`<br>`go vet ./agent` | 定向用例通过；agent 13.379s、core 0.150s、eval 0.171s 全量通过；-race 1.355s 通过；构建与 vet 无输出 | 红阶段：关闭原因表 4 个映射子用例与暂停/停止/转移三例失败；反向检查把 `AgentLoop.run` 兜底关闭改回 `ErrRunClosed` 后三例同时失败，还原后通过；`TestWorkflowRootClosesSteering`、`TestNestedContextsAreNotTopLevel` 实施前即通过，属回归守卫；暂停用例恢复后的追加在驱动前登记，第一步开头投递（Step=1） |
| TASK-TL-05 | 已完成 | `be2680c` | `go run ./examples/task-loop`<br>`go test ./... -count=1`<br>`go vet ./...`<br>`go build ./...`<br>`gofmt -l examples/task-loop/main.go agent/steering.go agent/loop.go agent/runnable.go agent/runtime.go agent/run.go agent/durable_runtime.go agent/task_loop_test.go agent/task_loop_internal_test.go agent/steering_test.go agent/steering_internal_test.go` | 示例退出码 0；全仓 34 个含测试的包全部 ok；vet/build 无输出；gofmt 未列出任何文件 | 示例输出为：初始提问与追加消息之后依次“模型输出：初稿”“模型输出：修订稿：面向离线环境的产品介绍。”，票据 `Delivered=true Step=1`，TurnStarted 与 TurnDone 均为 `[0 1]`，RunDone 一次且最终结果为修订稿，运行结束后追加返回 `ErrRunClosed`；全量门禁首次失败于 runstore `TestRunFileDeadOwnerCannotBeReplaced`（Windows 上对已自行退出的子进程调用 TerminateProcess 报 Access is denied），已在 `89dd7a2` 单独修复夹具（先杀再关 stdin，只杀仍阻塞的子进程）后复跑全绿；README 在示例表新增 task-loop 行，并在“会话快照与并发”前新增“终稿后的追加与运行结束”小节 |
