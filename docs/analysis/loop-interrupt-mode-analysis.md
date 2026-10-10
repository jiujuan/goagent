# Loop 中断与恢复：现有契约及分阶段实施分析

- 审查日：2026-10-10。
- 仓库版本：`14b3b3ad1e33bd01b1edbdb26baafd64138ba613`，本次读取的 HEAD 与之相符。
- 交付范围：重写本文，并实现第一阶段“正确性修复”及其定向验收；第二阶段的崩溃一致性与并发恢复仍未实现。
- 阅读顺序：现有契约 → 已验证优势 → 问题与复现 → 两阶段方案 → 验收 → 外部对比。
- 仓库内链接相对本文所在目录；外部代码链接固定到审查提交，不使用浮动分支作为证据。

本文使用四类标记，避免把设计目标写成当前能力：

| 标记 | 含义 |
| --- | --- |
| **现有事实** | 已阅读当前版本代码，能够指出具体执行路径；不自动等于故障测试通过。 |
| **已验证** | 有无缓存测试覆盖并已在当前工作区执行；覆盖范围以测试实际行为为准。 |
| **静态发现待测试** | 代码已显示风险或缺口，下面给出复现场景，但尚未为该项新增用例或执行对应故障注入。 |
| **拟议契约 / 未验证** | 后续开发的目标或尚未检查的能力；不能用于承诺当前生产可靠性。 |

## 一、现有控制与恢复契约

### 1.1 一轮执行与信号生效位置

入口是 [agent/loop.go](../../agent/loop.go) 的 `AgentLoop.run`。
一轮通常执行 `PrepareTurn → CallModel → ExecuteTools → Checkpoint → ApplyDirectives`。
恢复的工具批次位于这套回合流程之前，不能把恢复简单理解为重新进入 `BeforeModel`。

| 位置 / API | 当前行为 | 需要保留的边界 |
| --- | --- | --- |
| `PrepareTurn` | `steering.drain()` 后把消息追加到局部 `history`，再压缩历史、执行 `BeforeModel`。 | drain 不是持久化确认；未消费队列不在快照中。 |
| `BeforeModel` | 返回 `Directive, error`；错误进入 `fail`。 | `Interrupt` 经 `terminalFromDirective` 直接返回，没有在此保存快照。 |
| `ModifyRequest` | 只返回 `error`，可修改出站请求。 | 它不是返回 `Directive` 的暂停钩子。 |
| `AfterModel` | 返回 `Directive, error`；此时模型最终消息已发布 `MessageDone`。 | `Interrupt` 发生在最终消息加入历史之前，当前直接返回且不保存。 |
| `BeforeTool` | loop 按调用顺序检查，全部通过后才调用 `execTools`。 | 在第 i 个调用中断时，前面的调用也尚未执行；当前却只保存 `calls[i:]`。 |
| `Stack.BeforeTool` | 对一个调用按注册顺序运行所有中间件，再 `Resolve`。 | 不会在一个中间件返回 `Interrupt` 时立即短路；返回 error 时才提前退出。 |
| `AfterTool` / `tool.Result.Control` | 工具结果产生后收集指令，整批返回后再合并生效。 | 串行模式也会执行后续调用；并行模式等待批次 join。这不是正在执行工具的抢占取消。 |
| `OnToolReject` | `ToolRejecter` 的无返回值观察钩子；批次结束后由 loop 调用。 | 不是直接控制流入口；此位置写入的 KV 可随该步保存。 |
| `checkpoint` | 保存 State 与可选 `PendingHITL`。 | 当前忽略 `Store.Save` 错误，不能从终态事件推导出保存成功。 |
| `Run.Cancel` | 取消运行上下文。 | 不等于生成可恢复 HITL 暂停；不保证外部工具副作用停止。 |

`AfterTool` 在并行批次中可能并发执行，中间件必须自行保证并发安全。
其 error 当前在 `execTools` 中被忽略；这是错误传播的另一处静态缺口，见第三、四部分。
本次方案不改变“工具指令批后生效”的调度语义。

### 1.2 指令排序不是安全策略排序

[core/directive.go](../../core/directive.go) 的 `Resolve` 选择最高优先级：

```text
Continue < Transfer < Escalate < Stop < Interrupt
```

同级取传入顺序中的第一项。
[agent/middleware.go](../../agent/middleware.go) 的 Before 钩子按注册顺序运行，After 钩子反向运行。
因此，同级指令的原因或目标可能随注册顺序、反向钩子顺序而改变。
不能据此声称编译器保证中断路径完整，也不能把 `Interrupt > Stop` 当作“人工审批高于安全拒绝”。

[middleware/permission.go](../../middleware/permission.go) 当前遇到 `AskTool` 返回 `Interrupt`，遇到 `DenyTool` 返回 `Stop`。
同一个 `Permission` 内部采用首个非 Allow 规则；不同中间件的指令再由 Stack 合并。
这两层排序都必须纳入审批安全设计。

### 1.3 错误恢复、审批恢复与 steering

[agent/loop.go](../../agent/loop.go) 的 `fail` 会先把 `seam(history)` 写回 State，再尝试保存。
`seam` 去掉尾部没有工具结果的 assistant 工具调用消息，让下次模型请求回到可重放边界。
如果失败前已 drain steering，它已经进入 `history`；正常返回的模型错误并不必然丢失这些消息。
这条保证仍依赖保存成功，不覆盖进程强杀或保存失败。

[agent/hitl.go](../../agent/hitl.go) 的 `Agent.Resume` 读取最新快照，克隆 State、恢复文件快照并创建新 Run。
`Run.Resume` 只是收集 `Decide` 的决定后转调 `Agent.Resume`，不是继续驱动原 Run。
必须迭代新 Run 的 `Iter()` 或调用 `Wait()` 才会开始执行；仅调用 `Events()` 只是订阅。

工具暂停的批准集合进入 `resumeBatch`，由 `runResumed` 在下一次模型请求前执行。
批准调用直接进入 `execTools`，当前不会再经过 `BeforeTool`。
拒绝或没有决定的调用不进 handler，而是生成错误 ToolResult。
`rejectionReason` 会保留人类理由，例如 `rejected: 改为归档`；不是只返回无解释的拒绝。

新 Run 的 [RunContext](../../agent/runtime.go) 有新 steering 队列。
暂停后向旧 Run 调用 `Steer`，不能指望这些消息自动迁移到新 Run。
向新 Run 注入 steering 会在它下次模型请求前消费，但已批准工具批次会先执行。
因此 steering 不能撤销已经提交的批准，也不是审批 gate。

### 1.4 存储、plan 与组合工作流

[checkpoint/checkpoint.go](../../checkpoint/checkpoint.go) 定义快照 State、Step、Pending 与文件快照交接字段。
[checkpoint/memory.go](../../checkpoint/memory.go) 是进程内存储，不提供跨进程恢复。
[checkpoint/file.go](../../checkpoint/file.go) 追加 JSONL，保存 blob 和文件索引，但没有调用 `Sync`。
`scan` 遇到坏 JSON 行直接返回错误；当前没有已承诺的断尾修复语义。
`Save` 在实际写成功前更新 known blob 缓存，失败后重试不能被假定为一定补齐 blob。

[agent/plan.go](../../agent/plan.go) 的 `planRunner.save` 同时忽略 `json.Marshal(st)` 与 `Store.Save` 的错误。
保存位置包括首次规划、重规划、节点结果收集后、awaiting 节点暂停及 whole-plan approval 暂停。
plan 通过 KV 保存节点状态和审批决定，这与 loop 的 PendingHITL 不是同一种恢复协议。

[agent/workflow.go](../../agent/workflow.go) 的 `sequentialRunner.run` 没有持久化子节点游标。
`loopRunner.run` 每次从第 0 次迭代开始；`Agent.Resume` 在 `a.loop == nil` 且存在工具 Pending 时还会合成未执行结果。
所以统一 `*Agent` / `Resume` API 不代表所有嵌套工作流都能从精确暂停位置继续。
嵌套 Sequential / Parallel / Loop 的精确恢复明确排除在第一、第二阶段保证之外，另立后续议题。

## 二、已验证的优势

### 2.1 控制流、错误边界与审批结果可以定位和回归

显式 `Directive`、集中 `AgentLoop.run` 和独立 `runResumed` 让控制点容易定位。
优势是可审查、可测试；当前仍有第三部分所列缺口，不能写成“所有中断点都可靠”。

| 已有证据 | 当前可以支持的结论 | 不支持的扩大解释 |
| --- | --- | --- |
| [core/directive_test.go](../../core/directive_test.go)：`TestResolvePrecedence`、`TestResolveTieKeepsFirst` | 指令优先级及同级首项规则有测试。 | 指令排序自动解决审批安全冲突。 |
| [agent/failure_test.go](../../agent/failure_test.go)：`TestModelFailureCheckpointsResumableSeam` | 模型正常返回错误时，已 drain 的 steering 会保存在 seam 中并供恢复使用。 | 硬崩溃、保存失败或未 drain 队列也有同等保证。 |
| 同文件：`TestFailureAfterAnsweredToolsKeepsWholeTurn`、`TestFailureWithUnansweredToolCallTrimsTurn` | 已回答工具轮次保留；未回答的尾部工具调用被裁剪。 | 外部副作用可回滚或必然只执行一次。 |
| [agent/resume_exec_test.go](../../agent/resume_exec_test.go)：`TestResumeApprovedCallGoesThroughAfterTool`、`TestResumeApprovedCallDirectiveEndsRun` | 恢复批准调用复用执行器、AfterTool 及结果控制流。 | 恢复前已经重新校验 BeforeTool 策略。 |
| 同文件：`TestResumeDeniedCallPublishesWithoutAfterTool` | 拒绝理由进入 ToolResult，发布工具事件，handler 与 AfterTool 不执行。 | 拒绝理由丢失，需要新增字段才能让模型看到。 |
| 同文件：`TestResumeKeepsOriginalCallOrder` | 恢复批次的结果按原调用顺序呈现。 | 任意位置暂停都已保留完整批次。 |

### 2.2 已有状态恢复基础，但必须说明测试边界

[agent/durable_test.go](../../agent/durable_test.go) 的 `TestDurableResumeAcrossInstances` 在同一测试进程中创建两个 File/Agent 实例。
它验证了**跨实例重建恢复**，没有真的杀死旧进程。
[checkpoint/file_test.go](../../checkpoint/file_test.go) 的 `TestFileCrossInstanceResume` 同样不能替代强杀或掉电测试。

[agent/filedurable_test.go](../../agent/filedurable_test.go) 的 `TestDurableHITLResumeRestoresFiles` 覆盖文件状态恢复。
这支持在特定文件后端与成功保存前提下恢复文件内容，不保证数据库写入、邮件发送或远端 API 的 exactly-once。

[middleware/loopguard.go](../../middleware/loopguard.go) 已把 interventions 计数放进 `State.KV`。
[middleware/loopguard_test.go](../../middleware/loopguard_test.go) 的 `TestLoopGuardAcrossProcessResume` 可作为恢复行为回归证据；不能仅凭测试名认定做过真实进程强杀。
不应把“把 interventions 移入 KV”列成尚未实现的改进。

[agent/plan_approval_test.go](../../agent/plan_approval_test.go) 的 `TestPlanPerNodeApproveResume`、`TestPlanPerNodeRejectCascades`，以及 [agent/plan_test.go](../../agent/plan_test.go) 的 `TestPlanFinalApprovalPauseResume` 覆盖特定 plan 审批场景。
它们不证明 plan 保存失败能被正确报告，也不证明通用工作流游标恢复已经实现。

## 三、已确认的问题与复现场景

以下执行路径已由静态阅读确认；除明确引用的现有测试外，故障复现仍为**静态发现待测试**。
“已确认的问题”指代码缺口明确，不代表本次已执行全部复现。

### 3.1 P0：暂停可能遗漏未执行调用

1. 模型生成 A、B、C 三个调用。
2. A 的 `BeforeTool` 返回 Continue，B 返回 Interrupt。
3. 此时 `execTools` 尚未调用，A、B、C 全都没有执行。
4. 当前保存 `calls[1:]`，恢复只处理 B、C，历史却含 A、B、C。
5. 结果可能缺少 A 的工具结果，或使后续 provider 拒绝消息序列。

证据：`AgentLoop.run` 的 BeforeTool 循环及 `pendingFrom(calls[i:])`。
第一阶段明确采用整批保守重新审批，不采用“前面已检查所以视为已执行”的解释。

### 3.2 P0：审批可绕过未运行或已变化的拒绝策略

1. 同批 A 配置 Ask，B 配置 Deny。
2. loop 检查 A 后立即暂停，B 尚未进入 `BeforeTool`。
3. B 已包含于 pending；调用方提交批准 B。
4. `runResumed` 直接调用 `execTools`，B 的 Deny 策略没有再运行，可能执行 B。

另一场景是暂停时允许某调用，恢复前策略变为 Deny，但旧批准仍直接执行。
即使修复完整 pending，这个安全问题仍然存在，甚至会影响更多未检查的调用。
必须同时修复 [agent/hitl.go](../../agent/hitl.go)、[agent/middleware.go](../../agent/middleware.go) 与 [middleware/permission.go](../../middleware/permission.go)。

同一个调用也可能同时得到 Ask 的 Interrupt 与另一个中间件的 Stop，最后由 Interrupt 胜出。
安全拒绝需要独立于普通 `Resolve` 的不可绕过检查，不能只交换中间件注册顺序。

### 3.3 P0：保存失败后仍呈现成功或已暂停

- loop 的 `checkpoint` 丢弃 `Store.Save` 错误，模型完成或 HITL 暂停仍可能发布成功终态。
- `fail` 只报告原始错误，保存失败隐藏，调用方可能误以为最新 seam 已可恢复。
- plan 的 `save` 同时忽略编码错误和保存错误，awaiting / whole-plan 暂停可能没有对应快照。
- `execTools` 忽略 `AfterTool` 返回的 error，执行器仍可能报告正常完成。

复现方法：注入第 N 次 Save 必失败的 Checkpointer，分别触发模型结束、工具暂停、模型失败和 plan 暂停。
AfterTool 另用固定 sentinel error，检查终态和 `errors.Is`。
当前 `planState` 仅含受支持的字符串、整数等类型，Marshal 错误路径难以自然触发；不要编造一个现有可触发的生产输入。
实现仍应显式检查该 error，必要时通过内部编码接缝验证。

### 3.4 P1：模型前后 Interrupt 没有明确恢复位置

`BeforeModel` 在 drain 后 Interrupt：没有新快照，恢复可能找不到检查点或退回旧历史。
`AfterModel` Interrupt：MessageDone 已发布，但新回复尚未加入历史，当前也没有暂停快照。
[core/event.go](../../core/event.go) 的 `Interrupted` 目前只有 Pending，没有 Reason 或 Phase，空 Pending 无法解释暂停阶段。
不能把这两条路径写成与工具审批同等完整的暂停契约。

### 3.5 P1/P2：持久化投递和重复执行仍有窗口

未 drain 的 steering、向旧 Run 新投递的消息、尚未持久化的决定都没有持久化 inbox 保证。
恢复批准批次执行外部副作用之后、结果 checkpoint 保存之前若进程终止，再次 Resume 可能重放旧 pending。
两个调用者同时读取最新暂停快照也可能分别执行同一批准调用，现有 Checkpointer 没有 claim / CAS 接口。
这些是第二阶段设计对象；普通文件快照不能消除远端副作用的不确定性。

File 还存在没有 Sync、半写尾部导致 scan 报错、known 缓存提前更新等存储窗口。
不能把“JSONL 追加”“有文件快照”或“同实例重试”直接等同于可靠提交协议。

## 四、分优先级的改进方案

### 4.1 第一阶段范围与交付顺序

**拟议契约：** 第一阶段解决正确性、可见错误、完整 pending、明确阶段语义及审批安全边界。
顺序为：保存错误传播 → 完整批次与 gate 修复 → 阶段暂停 → 现有 API 示例和回归。
不引入持久化 inbox，不宣称并发 Resume 安全，也不承诺外部工具 exactly-once。
下文的新类型、函数及测试名均为拟议名称，不是当前已有 API。

### 4.2 P0：保存与执行错误必须影响终态

实施文件与修改方式：

- [agent/loop.go](../../agent/loop.go)：让 `AgentLoop.checkpoint` 返回 error，检查普通回合、截断结果、BeforeTool 暂停、恢复批次及失败 seam 的全部调用点。
- 同文件 `fail`：用可被 `errors.Is` 检查的方式组合原始错误和保存错误，例如 `errors.Join`；不得吞掉任意一个。
- [agent/plan.go](../../agent/plan.go)：让 `planRunner.save` 返回 error；Marshal 成功后才写 `planStateKey`，再调用 Store.Save。
- `planRunner.run`：首次规划、重规划、每次节点结果收集后的保存、awaiting 暂停及 whole-plan approval 全部接收并传播错误。
- [agent/exectools.go](../../agent/exectools.go)：收集 AfterTool 错误，扩展内部批次返回值；保留所有已执行调用的结果，等批次结束后报告聚合错误。
- [agent/hitl.go](../../agent/hitl.go)：`runResumed` 传递批次错误；loop 先形成完整结果历史并尝试保存，再以失败结束，不能重放本次已知完成的调用。
- [agent/run.go](../../agent/run.go)：沿现有 `out.Err` 优先分支发布 RunFailed，核验不会同时发 RunDone / Interrupted。

保存失败时可以保留内存结果用于诊断，但不能承诺最新结果已持久化。
不得为“报告保存失败”再递归调用同一个保存 helper；一个提交点失败应直接进入终态。
`Store == nil` 对普通计算可保持无持久化模式；请求可恢复暂停时应返回明确错误，不能发布可恢复暂停承诺。

plan 处理一个节点结果时其他 worker 可能仍在运行。
保存失败后必须停止调度新节点，收拢已启动 worker，避免它们阻塞在当前无缓冲 results 通道上；最后返回失败。
不得为了尽快返回而遗留工作 goroutine，也不能声称已完成的外部副作用被撤销。
可用受控 worker 屏障验证收拢行为，不需要修改整个 plan 调度模型。

验收：保存失败没有成功暂停事件；原错与保存错均可识别；plan 所有保存分支覆盖；并发节点不泄漏。
AfterTool 出错不改变“整批执行后生效”，只使错误对调用方可见。

### 4.3 P0：整批保守重新审批，拒绝策略不可被批准覆盖

**确定的选择：** 在任何工具实际执行之前，只要某个调用需要审批，就保存并展示本轮完整调用批次。
A 已通过 preflight 也仍未执行，因此 A、B、C 都需要显式决定；未决定的调用继续按当前契约拒绝。
兼容性变化是审批范围扩大，客户端不能再假定 Pending 只包含触发点之后的调用。

实施文件与修改方式：

- [agent/loop.go](../../agent/loop.go)：BeforeTool 返回 Interrupt 后不立即截断调用检查；完成尚未执行批次的必要检查，再保存完整 `calls` 并生成一致的 Pending。
- [agent/middleware.go](../../agent/middleware.go)：增加独立的不可绕过工具策略检查能力，例如 `ToolPolicyChecker.CheckToolPolicy`，与普通 Directive 折叠分开。
- [middleware/permission.go](../../middleware/permission.go)：将 Deny 放入该检查，对全部规则检查硬拒绝；Ask 留在审批 gate。硬拒绝优先于 Ask，不能被首个 Ask 规则遮住。
- [agent/loopctx.go](../../agent/loopctx.go)：增加只读审批上下文，例如 `IsApproved(call)`；仅匹配本次快照中的调用 ID、工具名及参数内容，不能提供全局“跳过检查”开关。
- [agent/hitl.go](../../agent/hitl.go)：`runResumed` 在 execTools 之前重新做不可绕过策略检查及普通 BeforeTool 检查；不得仅凭 Allow 就执行。
- `permission.BeforeTool` 只对精确匹配的已批准调用免除 Ask；其他校验仍执行。未经改造的自定义 gate 不自动豁免。
- [agent/exectools.go](../../agent/exectools.go)：核对实际执行参数与受检参数一致；若参数准备或中间件改写改变了审批内容，拒绝本次执行并要求重新审批，不能沿用旧批准。

安全检查先于任何工具执行；首阶段采取保守的整批失败语义：任一硬策略拒绝，整批不执行，返回可识别的策略错误。
`Ask(A)+Deny(B)` 必须走此路径；本轮 handler 调用数为零，包括 A。
这是比现有“Deny 返回 Stop”更明确的终态契约变化，需要迁移说明和回归测试。
通用 `core.Resolve` 不改优先级；不可绕过策略在进入普通 Resolve 之前单独裁决。
遗留 BeforeTool 的 Stop / Escalate / Transfer 也不能在新 preflight 中被 Ask 覆盖后继续执行。

恢复时若自定义 gate 再次 Interrupt，整批仍不执行，重新保存完整 pending 并暂停；决定只对本次恢复有效，下次需重新提交。
不能为了消除重复弹窗而跳过整个 Stack；需要迁移的自定义 gate 应主动识别精确审批上下文。
批准后的参数变化应失败关闭；第一阶段不支持“批准时顺便改参数”的隐式契约。
`ArgumentPreparer` 的变换必须在策略实际检查的执行参数上体现；实现可抽取共享准备 helper，并在同批执行时复用已准备值，避免二次变换。
保存的 pending、审批展示参数和最终执行参数必须一致；需要同步更新历史中的对应调用表示，不能只改 ToolCalls 返回的副本。

未知或重复审批 ID 应报错，不能静默授权其他调用；未决定调用仍生成明确拒绝结果。
第一阶段批准仍不具备跨进程的一次性消费保证，策略配置与工具身份的分布式版本绑定由第二阶段补齐。

测试范围包括 [agent/resume_exec_test.go](../../agent/resume_exec_test.go)、[agent/middleware_test.go](../../agent/middleware_test.go) 以及 [middleware/middleware_test.go](../../middleware/middleware_test.go)。
验收重点是完整 Pending、原序结果、后置 Deny 不可执行、策略更新后旧批准失效，以及自定义 gate 的兼容路径。

### 4.4 P1：模型前后暂停明确采用安全重放

**确定的选择：** 第一阶段不保存并续跑半个模型调用；模型前后 Interrupt 都保存回到模型请求前的可重放历史。
新增轻量暂停元数据只用于说明恢复位置，不保存可直接执行的模型响应。

| 暂停位置 | 拟保存内容 | 恢复行为 |
| --- | --- | --- |
| BeforeModel | 已 drain / 压缩后的可重放 history、当前 State、phase / reason。 | 从 PrepareTurn 继续，已有消息不重新入队；重新运行 BeforeModel。 |
| AfterModel | 同一轮模型调用之前的 history、当前 State、phase / reason；丢弃该次新生成的回复。 | 重新请求模型，重新执行相应钩子；本轮未执行任何新工具。 |
| BeforeTool | 包含完整工具调用消息的 history、完整 Pending、phase / reason。 | 先执行第四部分定义的恢复 preflight，再处理批准 / 拒绝。 |
| 工具批后 Interrupt | 完整调用及结果历史、当前 State、phase / reason，无工具 Pending。 | 从下一次模型请求继续，不把已完成批次重新列入审批。 |

AfterModel 之前已经发布的 MessageDone 不能撤回；消费者必须以暂停及恢复事件识别该输出没有被提交为下一轮历史。
重放可能重复模型计费、生成不同文本，并重复调用钩子。
这里的“安全”仅指避免留下无结果工具调用、避免执行该次被丢弃回复的工具；不是 hook 外部副作用的回滚保证。
钩子需幂等或自行管理副作用；无条件 Interrupt 的钩子恢复后会再次暂停，不会被自动跳过。

实施文件与修改方式：

- [agent/loop.go](../../agent/loop.go)：模型前后 Interrupt 不能再只依赖 `terminalFromDirective`；调用有保存错误返回值的暂停 helper，并传递正确历史。
- [checkpoint/checkpoint.go](../../checkpoint/checkpoint.go)：新增可选 Pause 元数据，明确 phase、reason、恢复模式；模型重放模式与 tool pending 模式须校验一致。
- [checkpoint/file_test.go](../../checkpoint/file_test.go)、[checkpoint/memory_test.go](../../checkpoint/memory_test.go)：验证元数据往返及旧快照读取。
- [agent/hitl.go](../../agent/hitl.go)：读取并验证恢复模式；未知模式拒绝恢复，旧 Pending 快照仍走工具审批，旧无元数据快照保持原 seam 恢复行为。
- [core/event.go](../../core/event.go)：为 Interrupted 增加可选 Phase / Reason / 恢复模式信息，不能只更新 checkpoint。
- [core/eventjson.go](../../core/eventjson.go)、[core/eventjson_test.go](../../core/eventjson_test.go)：更新事件编码、解码及旧事件兼容测试。
- [agent/runnable.go](../../agent/runnable.go)、[agent/run.go](../../agent/run.go)：让 runOutcome 携带并发布元数据；保存成功后才产生 Interrupted。

字段应可选，旧快照缺字段不表示损坏；新版本未知阶段不能默默猜测并执行工具。
增加公开结构字段可能影响外部无字段名的 Go 结构体字面量，需记录源码兼容性变化。
工具批后控制仍批后生效，不扩展为串行早停或并行抢占功能。
验收分别统计模型调用数、工具调用数、history 消息数及事件顺序，不能只检查 `Resume` 没报错。

### 4.5 P1：用现有 API 写清拒绝与继续

以下片段使用**现有 API**，假设 `run` 已创建，`ctx` 可用，并已导入 `fmt`、`agent`、`core`。
它展示拒绝全部待审调用，再向新 Run 投递修正消息；不依赖上述拟议 API。

```go
var pending []core.ApprovalRequest
for ev, err := range run.Iter() {
    if err != nil {
        return err
    }
    if paused, ok := ev.(core.Interrupted); ok {
        pending = paused.Pending
    }
}
if len(pending) == 0 {
    return fmt.Errorf("没有工具待审批；应另行处理完成或非工具阶段暂停")
}
for _, call := range pending {
    run.Decide(agent.Reject(call.CallID, "不要删除，改为归档"))
}
continued, err := run.Resume(ctx)
if err != nil {
    return err
}
continued.Steer(core.UserText("请给出归档方案，等待下一次确认。"))
_, err = continued.Wait()
return err
```

拒绝理由本身已经通过 ToolResult 反馈模型；Steer 是额外上下文，不是修复“理由丢失”。
若换成 `Allow`，批准批次在新 steering 被模型读取之前执行，不得把提示词当作阻止执行的控制机制。
`Wait` 返回 nil error 也可能表示再次暂停；需要持续处理审批的 UI 应消费新 Run 的 `Iter` 并检查 Interrupted。
后续实现应同步修订 [examples/plan-approval/main.go](../../examples/plan-approval/main.go)，并增加独立 loop 审批示例；本次仅提供文档。

### 4.6 第二阶段：持久化投递、恢复竞争与结构化审批

**拟议契约：** 只有在第一阶段通过后，才引入持久化 inbox 与原子恢复协调。
不能只给 State 或 ToolResult 增加字段，就宣称投递可靠、审批幂等或并发安全。

下表以代码路径标出的文件均为拟新增文件；链接指向现有文件。
File / Memory 通过可选能力扩展，不新建一套与 Checkpointer 平行的存储实例。

| 子项 | 拟修改位置 | 具体实施方式 |
| --- | --- | --- |
| 可选能力与事务契约 | 拟新增 `checkpoint/durable.go`；保留 [checkpoint/checkpoint.go](../../checkpoint/checkpoint.go) 的基础接口 | 定义持久投递、原子消费提交、revision / claim 校验及决定去重的可选接口和请求 / 回执类型；一次消费提交包含 checkpoint history 与 inbox ack，失败均不推进。 |
| 内存功能后端 | 拟新增 `checkpoint/durable_memory.go`；扩展 [checkpoint/memory.go](../../checkpoint/memory.go) | 在现有 Memory 上实现可选接口，新增 inbox / claim / decision 状态与 checkpoint 共用同一实例和锁，供功能及故障模拟测试使用，不承诺跨进程持久化。 |
| 文件持久后端 | 拟新增 `checkpoint/durable_file.go`；扩展 [checkpoint/file.go](../../checkpoint/file.go) | 在现有 File 上实现同一可选接口；普通 Save 与 durable 提交共用该线程的日志、提交格式、锁和恢复扫描层，不另建独立 inbox 日志。 |
| 有确认的消息投递 | 拟新增 `agent/inbox.go`；接入 [agent/runtime.go](../../agent/runtime.go)、[agent/run.go](../../agent/run.go)、[agent/loop.go](../../agent/loop.go) | 封装稳定 message ID、持久化确认与待消费读取；loop 通过原子提交保存 history 和消费游标。现有无返回值 Steer 保留为易失接口。 |
| 恢复竞争 | 拟新增 `agent/resume_claim.go`；接入 [agent/hitl.go](../../agent/hitl.go)、[agent/agent.go](../../agent/agent.go) | 管理 claim 获取、续租、释放和失效；以 thread、checkpoint revision、pause ID 做 CAS，执行及提交路径携带 owner / fencing token。 |
| 执行幂等上下文 | [agent/exectools.go](../../agent/exectools.go)、[tool/tool.go](../../tool/tool.go) | 扩展现有 tool.Context 以传递可选幂等键及调用身份，由执行器注入；执行状态与结果通过 durable 提交层记录，工具上下文本身不另存一份账本。已确认完成可回读，结果未知时禁止自动重试非幂等副作用。 |
| 结构化审批 | 拟新增 `agent/approval_event.go`；接入 [agent/hitl.go](../../agent/hitl.go)、[core/event.go](../../core/event.go)、[core/eventjson.go](../../core/eventjson.go) | 新文件负责校验并组装审批事件；以 decision ID 绑定 pause ID、参数摘要、策略版本和决定。去重及冲突裁决交给 durable 提交层，事件类型与 JSON 编解码仍留在 core。 |
| 文件提交可靠性 | [checkpoint/file.go](../../checkpoint/file.go)、拟新增 `checkpoint/durable_file.go`；现有 [checkpoint/file_test.go](../../checkpoint/file_test.go)、[checkpoint/filesnapshot_test.go](../../checkpoint/filesnapshot_test.go) 与拟新增 `checkpoint/file_recovery_test.go` | 共享提交记录、完整性校验及同步策略，修正 blob 缓存更新时机，覆盖半写与失败重试；durable 入口不能绕过这些检查。 |

`checkpoint/file.go` 的 Save、scan 与拟新增 durable 方法应下沉到同一提交 / 重建路径。
同一线程的 checkpoint、inbox、ack、claim、decision 记录共享日志；history 与 ack 由一个有明确提交边界的事务一起可见。
两次各自成功的日志追加不构成这项原子保证；普通 Save 也必须参与 revision 与 claim 冲突校验。
File 的共享提交层需同时协调进程内和跨进程的线程锁；所有入口采用同一锁顺序，不能只给 durable 方法另加一把 mutex。
`checkpoint/durable_memory.go` 同样扩展原有 Memory 状态，不通过第二个 Memory 对象模拟事务。
已核实 tool.Context 在 [tool/tool.go](../../tool/tool.go) 中，目前仅嵌入 context.Context 并含 State / CallID；幂等上下文是拟新增能力，零值仍支持现有直接工具调用。

inbox 推荐按“可重投递、稳定 ID 去重、history 与 ack 原子提交”实现。
若后端不支持这些能力，新增 durable API 应明确拒绝或报告能力不足，不能退化为内存后仍返回持久化成功。
Memory 可以作为功能测试后端，但不是跨进程可靠性证据。

恢复 claim 必须在任何批准工具执行之前成功；单个 File 实例的 mutex 不足以协调独立实例或进程。
租约过期不代表旧 worker 已停止，fencing 必须进入提交校验；远端副作用仍需服务端幂等键或人工核对。
状态至少区分 pending、claimed、completed、unknown，不能把“未保存结果”一概视为“没有执行”。

File 的具体目标是：blob 和 checkpoint 完整提交并按契约同步后，才能确认保存成功并更新 known 缓存。
写入失败必须废弃或重建相应缓存，不能略过本次实际未写入的 blob。
尾部半写应在互斥修复下恢复到最后完整提交；中段坏数据必须报错，不能静默跳过任意坏 JSON。
格式需能识别截断的最后记录；旧 JSONL 保留读取路径，未知格式版本拒绝读取。
需分别测试追加失败、短写、Sync 失败、尾部截断和缺失 blob，不把 `Close` 等同于持久化屏障。

结构化审批事件优先独立于模型 ToolResult，保持人类拒绝文本的当前可读语义。
若确需扩展 `core.ToolResult`，实施范围必须包含 [core/message.go](../../core/message.go)、[core/messagejson.go](../../core/messagejson.go)、[core/message_test.go](../../core/message_test.go)。
还要明确 [llm/openaicompat/openaicompat.go](../../llm/openaicompat/openaicompat.go) 与 [llm/anthropic/anthropic.go](../../llm/anthropic/anthropic.go) 的映射：哪些是本地审计字段、哪些进入模型文本。
旧快照缺少新字段应保持原行为，JSON 往返及两个 adapter 的消息映射都要验证。
不能只增加 Go 字段而漏掉自定义 JSON 编解码，或直接向 provider 发送其协议不支持的字段。

第二阶段验收以第五部分表格中的崩溃窗口、竞争、去重及兼容性结果为准；嵌套工作流精确恢复仍需单独设计游标和子运行身份。

## 五、验收测试

### 5.1 已执行的基线与证据来源

本次基线验证记录，日期为 **2026-10-10**：

```text
go test -count=1 ./agent ./core ./middleware ./checkpoint/...
agent       1.323s  PASS
core        0.221s  PASS
middleware  0.422s  PASS
checkpoint  0.298s  PASS
```

第一阶段定向验收已在当前工作区无缓存执行并通过，其中包含内置 Skill Gate 的恢复集成路径：

```powershell
go test -count=1 -run '^Test(InterruptKeepsWholePendingBatch|AskThenDenyCannotBeApproved|DenyWinsOverAskAcrossRulesAndStack|ResumeRevalidatesChangedPolicy|ResumeApprovalIsCallScoped|ApprovalUsesExecutedArguments|CheckpointFailureChangesTerminal|FailurePreservesBothErrors|PlanSaveFailureAtEveryBoundary|AfterToolErrorSurvivesBatch|ModelPhaseInterruptReplaysSafely|AfterToolsInterruptResumesAfterBatch|PauseMetadataCompatibility|ResumedSteeringDeliveryOrder|GateOnLiveRunInterruptsOnceThenApproves)$' ./agent ./core ./middleware ./checkpoint/... ./workspace
```

同一工作区还已通过 `go test -count=1 ./...`，以及覆盖 Agent、检查点、工具、Skill Gate 和工作区的 `go test -race -count=1 ./agent ./core ./middleware ./checkpoint/... ./tool ./skills ./workspace`。
执行环境为 Windows / amd64，`CGO_ENABLED=1`，gcc 已在 PATH。
仓库当前没有 `.github` 目录，本文不声称已有相关 CI 配置或远端流水线结果。

### 5.2 第一阶段测试与通过标准

下表测试均已落地；测试名、位置与通过标准是当前第一阶段实现的验收契约。

| 测试 / 位置 | 输入或故障 | 通过标准 |
| --- | --- | --- |
| `TestInterruptKeepsWholePendingBatch`；`agent/phase1_interrupt_test.go` | A Continue、B Interrupt、C Continue | 暂停前零执行；Pending 恰为 A/B/C；决定后结果完整且原序排列。 |
| `TestAskThenDenyCannotBeApproved`；`agent/phase1_interrupt_test.go` | Ask(A)+Deny(B)，并用旧快照模拟曾暂停的批次 | 初始检查和恢复均不能执行 B；硬拒绝时整批 handler 为零。 |
| `TestDenyWinsOverAskAcrossRulesAndStack`；`agent/phase1_interrupt_test.go` | 同工具同规则集、不同中间件中的 Ask 与 Deny，交换顺序 | 硬拒绝不随注册顺序被 Interrupt 覆盖。 |
| `TestResumeRevalidatesChangedPolicy`；`agent/phase1_interrupt_test.go` | 暂停后将策略改为 Deny，再 Allow | 返回策略错误，handler 为零。 |
| `TestResumeApprovalIsCallScoped`；`agent/phase1_interrupt_test.go` | 未知 / 重复 ID、参数变化、未改造自定义 gate | 错误或重新暂停，不能靠通用批准跳过整栈。 |
| `TestGateOnLiveRunInterruptsOnceThenApproves`；`workspace/gate_run_test.go` | 已激活 skill 未声明 `write_file`，精确批准该调用后恢复 | 只暂停一次；该调用执行并写入工作区；后续模型响应完成。 |
| `TestApprovalUsesExecutedArguments`；`agent/phase1_interrupt_test.go` | ArgumentPreparer 或 BeforeTool 改写参数 | 展示、保存、受检和执行参数一致；变化后旧批准不可继续使用。 |
| `TestCheckpointFailureChangesTerminal`；`agent/phase1_failure_test.go` | 模型完成、截断调用、正常工具批次、暂停、恢复批次的 Save error | RunFailed 可见；无虚假 RunDone / Interrupted，暂停后不会执行工具。 |
| `TestFailurePreservesBothErrors`；`agent/phase1_failure_test.go` | 模型失败且失败 seam 的保存失败 | `errors.Is` 能识别两类错误，且只尝试一次失败保存。 |
| `TestPlanSaveFailureAtEveryBoundary`；`agent/plan_checkpoint_test.go` | 首次规划、重规划、节点结果、awaiting、最终审批分别失败 | 不继续调度，不虚报暂停；已有 worker 收拢后才返回。 |
| `TestAfterToolErrorSurvivesBatch`；`agent/phase1_failure_test.go` | 串行 / 并行 / 恢复批次 AfterTool 报错 | 批次完整收拢，结果历史保留，终态失败；不把批后语义改为提前抢占。 |
| `TestModelPhaseInterruptReplaysSafely`；`agent/phase1_interrupt_test.go` | BeforeModel / AfterModel 分别 Interrupt 一次 | 新快照含 steering；恢复后模型重放次数准确，无悬空工具调用，无重复消费。 |
| `TestAfterToolsInterruptResumesAfterBatch`；`agent/phase1_interrupt_test.go` | 工具批后 Interrupt | 所有该批结果先保存；恢复不重列已完成工具。 |
| `TestPauseMetadataCompatibility`；`agent/phase1_interrupt_test.go`、`core/eventjson_test.go`、`checkpoint/file_test.go`、`checkpoint/memory_test.go` | 新旧事件 / 快照、未知恢复模式 | 旧数据可读，新字段往返，未知模式不执行工具。 |
| `TestResumedSteeringDeliveryOrder`；`agent/phase1_interrupt_test.go` | 分别向旧、新 Run 投递消息 | 旧队列不继承；新消息在模型前可见；批准工具先于该模型请求执行。 |

保存失败矩阵已覆盖模型无工具结束、截断调用结果、正常工具批次、暂停、恢复批次和失败 seam；`planRunner.save` 现在返回并包装 JSON 编码或 Store.Save 错误。`planState` 的现有字段均可编码，因此未伪造不符合该结构的自然输入来测试不可达的 Marshal error。
既有拒绝理由、结果顺序、文件恢复及 LoopGuard KV 测试仍纳入下方相关包回归。恢复路径现在重新执行 `BeforeTool`，自定义 gate 只有通过 `LoopContext.IsApproved` 显式识别精确调用级批准时才会放行；这是有意的兼容性收紧。

### 5.3 第二阶段新增测试与通过标准

| 拟议测试 / 位置 | 故障窗口 | 通过标准 |
| --- | --- | --- |
| `TestInboxAckSurvivesProcessKill`；拟新增 `agent/inbox_test.go`（含子进程入口） | 持久化接收确认后、消费前强杀子进程 | 新进程能重新读取消息，稳定 ID 不变。 |
| `TestInboxHistoryAndAckCommitTogether`；拟新增 `checkpoint/durable_test.go`（Memory / File 共用契约用例） | history 写入与 ack 提交边界注入失败 | 同一消息最终只进入 history 一次，不会 ack 后丢失。 |
| `TestConcurrentResumeSingleClaim`；拟新增 `agent/resume_claim_test.go`（含子进程入口） | 两个独立进程同时 Resume 同一 revision | 只有一个持有效 claim 开始执行，另一方收到明确冲突。 |
| `TestExpiredOwnerCannotCommit`；拟新增 `checkpoint/durable_test.go`、`agent/resume_claim_test.go` | 租约到期后旧 worker 返回 | 旧 fencing token 无法覆盖新状态；不据此假定远端副作用停止。 |
| `TestToolOutcomeUnknownAfterCrash`；拟新增 `agent/resume_claim_test.go`（含子进程入口）；工具上下文传递另由拟新增 `tool/idempotency_test.go` 的同名测试验证 | 副作用完成、结果持久化前强杀 | 有服务端幂等键时可安全核对 / 重试；否则标 unknown，禁止自动重放。 |
| `TestFileTailRecoveryAndBlobRetry`；拟新增 `checkpoint/file_recovery_test.go` | 短写、Sync 失败、半写尾部、缓存已暂存但 blob 未成功写入 | 恢复最后完整提交，重试补齐 blob；中段损坏明确失败。 |
| `TestApprovalDecisionIdempotency`；拟新增 `agent/approval_event_test.go`、`checkpoint/durable_test.go` | 重复提交同 decision ID，或同 ID 不同内容 | 同内容回读原结果，冲突内容报错，不重复执行。 |
| `TestApprovalSchemaCompatibility`；现有 `core/eventjson_test.go`；若扩展 ToolResult，再覆盖现有 `core/message_test.go`、`llm/openaicompat/openaicompat_test.go`、`llm/anthropic/anthropic_test.go` 及拟新增 `checkpoint/durable_test.go` | 旧快照、结构化事件、自定义消息 JSON、两类 provider 映射 | 审计字段不丢失、不污染 provider 协议；旧拒绝文本仍可读。 |

真实进程强杀必须使用子进程、持久化目录和同步屏障控制故障位置，并由另一个进程读取结果。
同一测试进程重建 File/Agent 实例不算完成这项验收。
进程强杀也不能完全代替掉电测试；发布说明应列清实际覆盖的故障模型。

### 5.4 后续开发验证命令与交付门槛

以下命令是当前复验入口。第一阶段命令已在 5.1 所记录的工作区执行并通过；第二阶段命令仍是后续实现的验收要求。
正则中的名称对应 5.2 / 5.3；新增或重命名测试后应核对实际匹配到的用例，不能把“没有匹配测试”视为通过。

第一阶段定向验证（完整 Pending、安全 gate、错误传播与阶段暂停）：

```powershell
go test -count=1 -run '^Test(InterruptKeepsWholePendingBatch|AskThenDenyCannotBeApproved|DenyWinsOverAskAcrossRulesAndStack|ResumeRevalidatesChangedPolicy|ResumeApprovalIsCallScoped|ApprovalUsesExecutedArguments|CheckpointFailureChangesTerminal|FailurePreservesBothErrors|PlanSaveFailureAtEveryBoundary|AfterToolErrorSurvivesBatch|ModelPhaseInterruptReplaysSafely|AfterToolsInterruptResumesAfterBatch|PauseMetadataCompatibility|ResumedSteeringDeliveryOrder|GateOnLiveRunInterruptsOnceThenApproves)$' ./agent ./core ./middleware ./checkpoint/... ./workspace
```

第二阶段定向验证（包含由父测试启动并控制的子进程强杀、竞争恢复用例）：

```powershell
go test -count=1 -run '^Test(InboxAckSurvivesProcessKill|InboxHistoryAndAckCommitTogether|ConcurrentResumeSingleClaim|ExpiredOwnerCannotCommit|ToolOutcomeUnknownAfterCrash|FileTailRecoveryAndBlobRetry|ApprovalDecisionIdempotency|ApprovalSchemaCompatibility)$' ./agent ./core ./checkpoint/... ./tool
```

两个阶段共同回归与 race 检查；包含 tool.Context 所在的 tool 包：

```powershell
go test -count=1 ./...
go test -race -count=1 ./agent ./core ./middleware ./checkpoint/... ./tool ./skills ./workspace
```

第二阶段仅在修改 ToolResult 字段或 provider 映射时追加以下验证：

```powershell
go test -count=1 ./llm/openaicompat ./llm/anthropic
```

race 用于共享状态与钩子并发问题，不验证文件落盘或远端 exactly-once。
第二阶段定向测试必须真正启动独立进程并控制故障点；只选中测试名不能替代这项断言。
第一阶段完成的门槛是错误可见、完整 pending、安全拒绝不可绕过、暂停恢复位置可解释以及兼容性说明齐全。
第二阶段完成的门槛是逐项提交故障测试证据，并明确不支持的后端、工具和故障模型。

## 六、有版本依据的外部对比

### 6.1 对比对象与证据边界

本节比较 goagent 本次审查版本与 pi 的底层 agent 实现，不评价未审查的上层 session。
GitHub API 核查显示 `badlogic/pi-mono` 当前重定向到 `earendil-works/pi`。
固定提交为 [`eba849739511223c51a62bbd7e3f1c00f99fb1d0`](https://github.com/earendil-works/pi/commit/eba849739511223c51a62bbd7e3f1c00f99fb1d0)。
提交时间为 `2026-10-09T17:13:34Z`，即北京时间 **2026-10-10 01:13:34**。
该提交 [packages/agent/package.json](https://github.com/earendil-works/pi/blob/eba849739511223c51a62bbd7e3f1c00f99fb1d0/packages/agent/package.json) 的包版本为 **1.1.0**。
本次已读取 pinned raw 文件及提交 API；此处不是对未来版本或所有产品层的概括。

### 6.2 按实际源码比较

| 维度 | goagent：本次 HEAD | pi：固定提交 | 可以得到的结论 |
| --- | --- | --- | --- |
| 控制入口 | Middleware 返回 Directive，loop 明确解释阶段。 | 有显式 `beforeToolCall`、`afterToolCall` 钩子。 | 不能称 pi 只有隐式事件；应比较具体钩子语义。 |
| 消息队列 | RunContext 的进程内 steeringQueue，PrepareTurn drain。 | steering 与 followUp 都使用 agent.ts 内部的 PendingMessageQueue。 | 一队列与两队列主要表达不同调度意图，不代表持久性高低。 |
| 消费时机 | steering 在回合准备阶段消费。 | 启动及回合边界取 steering，正常准备停止时取 follow-up。 | follow-up 可以表达“本轮自然结束后再继续”的意图。 |
| 队列存储 | 内存 FIFO，进入 history 并保存后才成为快照内容。 | PendingMessageQueue 以数组存储，enqueue / drain / clear 修改内存。 | 所查队列自身没有持久化保证。 |
| 审批恢复 | 已有 PendingHITL、Resume 与拒绝结果；有本文确认的缺口。 | 本次未验证与 goagent 等价的持久化人工审批协议。 | 不做 HITL 可靠性排名，不根据钩子存在推断持久恢复能力。 |
| 上层 session | 本文只分析列出的 loop、plan、workflow 与 checkpoint。 | coding-agent session 的持久化未审查。 | 不能从底层队列推断 pi 上层 session 不持久化。 |

pi 的队列证据位于 [agent.ts](https://github.com/earendil-works/pi/blob/eba849739511223c51a62bbd7e3f1c00f99fb1d0/packages/agent/src/agent.ts)。
内部 `class PendingMessageQueue` 的 `messages` 是 `AgentMessage[]`；enqueue 追加，drain 按模式取出并切片，clear 清空数组。
本次没有发现该类自身保存到磁盘的实现；结论仅限该类与所读调用路径。

调度和工具钩子证据位于 [agent-loop.ts](https://github.com/earendil-works/pi/blob/eba849739511223c51a62bbd7e3f1c00f99fb1d0/packages/agent/src/agent-loop.ts)。
其中 `getSteeringMessages` 用于启动和回合边界；正常结束路径检查 `getFollowUpMessages`，显式结束分支另有语义。
`beforeToolCall` 与 `afterToolCall` 是显式执行钩子，不应被描述为只能事后监听事件。

可以借鉴的是把“立即干预下一回合”和“自然结束后继续”分成清晰的投递意图。
是否给 goagent 增加 follow-up 应另行评估，不是修复当前丢调用、错误吞没和审批绕过问题的前置条件。
本次优先级由仓库内可复现的正确性问题决定，不使用星级或未经验证的可靠性比较。
