# Background Run Context Ownership and Thread Terminal Status Implementation Plan

> 执行者说明：按 executing-plans 技能串行执行 BC-01 至 BC-06，先写对应失败测试，再做最小实现，检查通过后勾选。本文接口与测试均为待实施设计，不能描述为当前已有能力。使用符号名定位代码，不依赖旧稿行号。

- 日期：2026-10-12；状态：BC-01…BC-06 全部完成（见各卡执行记录），V1–V9 有对应通过证据，ADR-0033 实施结果已回填。
- 依据：[ADR-0033](../adr/ADR-0033-background-run-context-and-status.md) 的审核修订版 D1–D6、V1–V9。
- 工作目录：D:/codeproject/mygo/goagent。下文文件路径均相对此目录。
- 各卡列出的接口与测试均已实施；已落地范围以各卡执行记录为准，未落地项会在对应卡里明写。

## 一、目标与完成条件

提交后运行不随调用方 context 取消；Handle.Cancel 显式中止执行。终态只绑定具体 Run 及其成功提交边界，通过原子 revision 校验写入，完整保留文件快照。调用方可用 WaitRecorded 检查持久化结果，RunStatus 只查询存储事实。

不新增公开 detach、事件重放、跨进程 agent job、Running 登记表、整次运行硬期限或前台自动终态写入。允许新增 Run 的内部结束凭据与只读 Paused 方法、Handle.WaitRecorded；这是已决范围，不得留到实施中再猜。

完成条件：BC-01…BC-06 勾选，V1–V9 有对应测试与实际执行记录；前台既有历史条数断言不改；最终 build/vet/test/race 通过。检查失败先定位并修复，不以修改已有断言掩盖回归。新增文档/源码使用仓库格式，不重排无关文件。

## 二、锁定的接口与实现约束

### 1. context 与执行

    workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
    run := a.Stream(workCtx, input, opts...)

EnqueueAgent 入口检查 ctx.Err；实际入队仍使用 ctx。入口已取消必须失败；取消与入队并发时允许以 Queue.Enqueue 的返回值决定是否转交成功，不修改所有队列的 select 语义。Handle.Cancel 同时调用 run.Cancel 与 cancelWork；入队失败、闭包 defer 也释放 cancelWork。子 context 取消不会向父传播。

终态记录单独使用 context.WithTimeout(context.WithoutCancel(workCtx), 30*time.Second)，defer 释放。固定期限只约束记录 I/O 的合作式等待，不是执行预算，也不能强制中断忽略 context 的自定义 store。Pool 取消停止取新任务、等待在途任务；不自动停止后台 run。

### 2. 终态数据与身份

    type Completion struct {
        RunID  string
        Kind   string       // completed | failed，两个导出字符串常量
        Result *core.Result
        Err    string
    }
    func (a *Agent) RecordCompletion(ctx context.Context, run *Run) error
    func (r *Run) Paused() bool

JSON 标签遵循 ADR D4。RecordCompletion 不驱动或等待运行；验证所属 Agent、已结束、非暂停、有本次成功 commit 边界、store 支持 checkpoint.Durable。明确拒绝 nil、其他 Agent、未结束、暂停、无边界；基础 store 返回 checkpoint.ErrDurableUnavailable。错误不得伪装为成功或普通 Save。

在 RunContext.commitCheckpoint 成功时缓存 checkpoint ID 和 CommitReceipt.Revision；提交失败不更新。Run 在释放 claim 前冻结边界，在发布 done 前冻结实际 outcome/result/error。Run.Paused 在 Wait 返回后表达该 Run 的 outcome，提前查询为 false 且不能用于判断是否结束；读写同步，不能从空 Result、事件订阅或当前 Latest 推断暂停。结果须深拷贝，防止 Wait 返回对象被调用方修改后影响终态记录。

不把恢复时继承的 head 视为本次成功提交；无本次提交的启动/准备失败或自定义 Runnable 返回明确无边界错误，保留运行结果供调用方处理。本批不伪造首 checkpoint。

### 3. 原子记录与文件保留

- 从冻结 ID 调用 Load 并核对 ThreadID/ID/Revision；拒绝边界的 Pause/Pending；不在记录时调用 Latest 生成预期 revision。
- 克隆 State 和独立 FileSnapshot（包括 nil/空区别），Result.Message 用 core.CloneMessage。
- 每个 Run 固定终态 checkpoint ID，并缓存不可变的完整提交内容。同一 Run 并发记录要串行化构造/提交，重试使用同一 ID 和内容；构造失败不缓存半成品。存储已接受但回执丢失时可用同记录确认。
- ParentID/Step 取冻结边界；通过 Durable.Commit 的 ExpectedRevision 提交，不带旧 claim、inbox ack 或工具执行变更。活跃 claim / revision 冲突直接暴露；不得读取新 head 重试。
- 依赖现有同 ID 同内容提交的幂等语义。已记录 A 后即使 B 前进，重试 A 只能确认旧提交，不能追加或移动 head。
- 默认前台执行不调用 RecordCompletion；Fork 清除 Completion，保留文件。

### 4. 等待与查询

    func (h *Handle) Wait() (core.Result, error)
    func (h *Handle) WaitRecorded() (core.Result, error)
    func (a *Agent) RunStatus(ctx context.Context, threadID string) (RunStatus, error)

Wait 保持原行为，可启动惰性执行，只表示 run 结束。WaitRecorded 只等待 worker 的收尾，不自行启动；闭包内部 run.Wait 后依据 run.Paused 跳过暂停记录，否则调用 RecordCompletion；存储结果后关闭独立 done，返回 errors.Join(runErr, recordErr)。WaitRecorded 返回原结果与同一合并错误；记录失败不能只返回给当前不提供错误出口的 Pool。多个等待者看到一致数据，返回可变数据不能共享给调用方污染。

未消费任务的 WaitRecorded 会等待，即使 Handle.Cancel 已调用；例子与文档必须先启动 worker。暂停是正常收尾且无 Completion。completed 的 WaitRecorded 返回 nil 表示提交确认；failed 仍返回运行错误，额外记录错误保持 errors.Is/errors.As 可识别。RunStatus 读到的 head 可能已有更新，不能要求永远等于这个 Run。

RunStatus 结构严格按 ADR D5：ThreadID、Exists、Paused、Revision、Step、Completion、Latest。core.CheckThreadID 后读取 Latest；不取 claim、不写入、不续租；不存在与读取错误区分。Paused 表示暂停元数据，不承诺审批、claim 或实际 Resume 必然成功。旧 Completion 不证明当前无新运行。

### 5. 治理

RunBudget 通过钩子触发正常收尾，不是中断在途调用的 deadline；duration 超限不能断言预算错误 failed。LLM 工具任务示例同时配置 RunBudget 与非零 WithToolTimeout，说明 provider、工具批次、重试、middleware、自定义 Runnable 和 store 各自的边界；不写“最坏 maxTurns × 5 分钟”或“后台一定有限结束”。

## 三、任务依赖与文件范围

默认由一个执行者串行完成，不要求并行代理。

| 卡片 | 交付 | 依赖 | 对应验收 |
| --- | --- | --- | --- |
| BC-01 | 队列 context 归属与释放 | 无 | V1 |
| BC-02 | Completion 数据、深拷贝、Fork | 无 | V3 |
| BC-03 | Run 结束凭据、Paused、CAS 记录 | BC-02 | V4、V5、V6 |
| BC-04 | WaitRecorded 与队列收尾 | BC-01、BC-03 | V7 |
| BC-05 | RunStatus、治理与前台兼容 | BC-03、BC-04 | V2、V8 |
| BC-06 | 示例、文档、跨进程与全仓验收 | BC-01…BC-05 | V9 |

BC-01 与 BC-04 同改 queue/agent_bridge.go；BC-03 会修改 agent/run.go、agent/durable_runtime.go，不再声称 agent 执行层完全不变。开始实现前检查工作区，保护已有改动。

## 四、任务卡

### BC-01：队列执行 context 与释放

**状态：** 已完成（2026-10-12）。**文件：** 修改 queue/agent_bridge.go；新增 queue/agent_bridge_test.go。

- [x] 写 TestQueueCallerCancelDoesNotStopRun：先入队，再取消提交 ctx，再启动 worker，运行仍完成。
- [x] 按 context 契约实现入口 ctx.Err 检查、派生执行 ctx 和三条幂等释放路径；Handle.Cancel 同时取消两层，不改 Run.Cancel 既有 claim 释放行为。
- [x] TestQueueEnqueueFailsWithCanceledCaller 与 TestQueueEnqueueFailureReleasesContext：用失败 Queue stub 确认不执行且释放；用包内夹具观察派生 Done，不依赖不存在的 context introspection。
- [x] TestQueueHandleCancelStops：显式取消能结束执行，有限工具超时/abandon 路径遵守 ADR-0029。
- [x] TestQueueContextReleasedAfterSettlement：执行成功、失败和暂停后检查释放，重复 Cancel 无副作用。不以 runtime.NumGoroutine 单独证明 context 释放。
- [x] 更新 Handle/EnqueueAgent 注释：提交 ctx 不控制已入队执行，Pool 停止取任务不代表执行取消，未消费任务的资源仍由队列持有。
- [x] 运行 go test ./queue -run '^TestQueue' -count=1；go test -race ./queue -count=1；记录结果。

**验收：** V1；现有 queue 测试断言不变。

**执行记录（2026-10-12）：**

| 命令 | 结果 |
| --- | --- |
| `go test ./queue -run '^TestQueue' -count=1` | ok，6 个顶层用例（含暂停收尾子用例）全绿 |
| `go test -race ./queue -count=1` | ok（1.2s） |
| `go test -race ./queue -run 'TestMemBusConcurrent' -count=20` | ok（6.8s，补测后复跑） |
| `go build ./...`、`go vet ./...` | 无输出、退出 0 |
| `go test ./... -count=1` | 退出 0，31 个包 ok，无 FAIL |
| `go run ./examples/background` | 五个任务全部完成，输出与改动前一致 |

实现前的失败测试确认：把派生 ctx 改回 `context.WithCancel(ctx)`（即旧行为）后，TestQueueCallerCancelDoesNotStopRun 在"提交方取消：执行 context 不应结束"处失败。逐项变异检查（每次只删一处，其余全绿即视为无效测试）：删 `context.WithoutCancel` → 提交方取消用例失败；删闭包 `defer h.cancelWork()` → 正常/失败/暂停三条收尾用例失败；删 `Handle.Cancel` 内的 `h.cancelWork()` → 仅新增的未消费任务用例失败；删入口 `ctx.Err()` 检查 → TestQueueEnqueueFailsWithCanceledCaller 失败；删入队失败分支的释放 → TestQueueEnqueueFailureReleasesContext 失败。

**与卡的偏离（四条，均已核实）：**

1. 新增卡外用例 TestQueueHandleCancelReleasesUnstartedJob。Handle.Cancel 里那次 `cancelWork()` 在"任务已被 worker 消费"的路径上与闭包 defer 重复，删掉它其余用例仍全绿；只有任务未被取走（没有闭包会运行）才能证明这一层独立存在。变异检查结果见上表。
2. 实现把 EnqueueAgent 的主体拆到包内函数 `submitAgentRun`，导出函数只是它的薄包装：入队失败时不能向调用方返回 Handle，而包内测试需要那个 Handle 上的 `workDone` 才能检查释放。公开签名与契约未变。
3. 顺带修复 queue/bus.go 的既有数据竞争（不在本卡文件清单内）：`MemBus.Publish` 原先在锁外向订阅通道发送，与 `Subscribe` 的 cancel 关闭通道相竞争，HEAD `44e106b` 的 `go test -race ./queue` 即可复现 TestBridgeForwardsEvents 失败，直接挡住本卡的 -race 验收。修法是把非阻塞发送放回锁内，投递语义（丢事件、不阻塞发布方）不变，queue/bus_test.go 的既有断言未改。
4. 上一条修复起初没有自己的回归测试（`3a22bdb` 只改实现），已补 `queue/bus_test.go` 四个用例（提交 `0b4b8ae`）：并发发布 × 订阅取消交错、Bridge 停在源通道时被取消、缓冲区写满只丢弃不卡发布方、一个慢订阅者不能拖住另一个读取者。实测效力：把实现换回修复前的版本，第一个用例在不带 -race 时就 panic `send on closed channel`（bus.go 的锁外发送）；在当前实现下 `-race -count=20` 连续通过。既有 TestBridgeForwardsEvents 的断言与文件行尾（CRLF）未动。

### BC-02：Completion 与快照数据边界

**状态：** 已完成（2026-10-12）。**文件：** 修改 checkpoint/checkpoint.go、checkpoint/snapshot.go、checkpoint/branch.go；新增 checkpoint/completion_contract_test.go。

- [x] 增 Completion 可选字段、JSON 标签和 completed/failed 常量；先写 Memory/File 往返失败测试。
- [x] TestCompletionFrozenAcrossStores：覆盖输入与 Load/Latest/History 返回对象修改，含 Result.Message 内部可变数据，历史不受影响。
- [x] TestCompletionLegacyReadable：使用现有 JSONL 格式夹具覆盖不含 completion 的旧记录，加入新记录并重开可读。
- [x] TestCompletionForkClearsTerminal：从终态 Fork 清除 Completion，文件内容保持；覆盖 FileSnapshot nil 与非 nil 空映射。
- [x] freezeCheckpoint/cloneCheckpoint 深拷贝新增字段，传播 CloneMessage 错误；本卡不增加生产者。
- [x] 运行 go test ./checkpoint -count=1；go vet ./checkpoint；记录结果。

**验收：** V3，现有快照不可变性契约不退化。

**执行记录（2026-10-12）：**

实现形状：`checkpoint.Completion{RunID,Kind,Result,Err}` + `CompletionCompleted`/`CompletionFailed` 常量 + `Checkpoint.Completion *Completion \`json:"completion,omitempty"\``；`cloneCheckpoint` 经 `cloneCompletion` 深拷贝（结果走 `core.CloneMessage`，错误原样传播），`Fork` 置 `child.Completion = nil`。File 后端无需改格式：`checkpointRecord` 内嵌 `*Checkpoint`，字段随快照行自动写出与读回。

| 命令 | 结果 |
| --- | --- |
| 实现前先跑测试 | `go vet ./checkpoint` 报 `undefined: checkpoint.Completion`（红） |
| `go test ./checkpoint -count=1` | ok（1.5s，含既有快照一致性契约用例） |
| `go vet ./checkpoint`、`go vet ./...` | 退出 0 |
| `go test -race ./checkpoint ./agent ./queue -count=1` | 三个包 ok |
| `go build ./...`、`go test ./... -count=1` | 退出 0，无 FAIL |

测试：`TestCompletionRoundTripAcrossStores`（卡上"往返失败测试"的具名用例，memory/file 各一轮，file 走 reopen）、`TestCompletionJSONShape`（空终态不写 `completion` 键；有终态时 `run_id`/`kind`/`result` 齐备、空 `err` 不序列化）、`TestCompletionFrozenAcrossStores`、`TestCompletionLegacyReadable`、`TestCompletionForkClearsTerminal`（三种文件快照：nil / 非 nil 空 / 有内容）。

变异检查（每次只改一处实现，确认对应测试会失败）：`cloneCheckpoint` 不拷 Completion → Frozen 失败；`cloneCompletion` 改浅拷贝共享 Result 指针 → Frozen 报"工具参数未深拷贝"；`Fork` 不清 Completion → ForkClearsTerminal 三个子用例全失败；去掉 `omitempty` → JSONShape 与 LegacyReadable 都失败（旧记录夹具断言里也当场暴露多余键）。

**`065edb9` 复核发现的缺陷与修复：**

`checkpoint.Completion` 当时没有进入提交摘要 `commitRequestDigest`（只覆盖 ID/ThreadID/ParentID/Step/State/Pending/Pause/FileSnapshot），于是同 ID、仅终态内容不同的两次提交摘要相同，Memory 与 File 都把它当作幂等重试返回成功并保留先写入的终态——违反"只有同 ID 同内容才幂等成功"。修复：`commitDigestCheckpoint` 新增 `Completion *commitDigestCompletion` 字段（`omitempty`，位置在 Pause 与 FileSnapshot 之间），`makeCommitDigestCompletion` 折叠 RunID/Kind/Err 与经 `makeCommitDigestMessage` 转换的 Result；nil 终态时该键缺席，因此无终态记录的摘要与修复前逐字节一致，旧日志的幂等重试不受影响。

新增测试（`checkpoint/completion_contract_test.go`）：`TestCompletionDigestKeepsNilBaseline`（钉住无终态记录的修复前摘要基准值，并重开后同内容重试仍成功、不追加记录）、`TestCompletionDigestDistinguishesTerminal`（memory/file 各自独立实例：同内容重试成功；RunID/Kind/Err/结果文本/结果内部字节/结果 nil 化/终态整体 nil 七种变化都返回 `ErrCheckpointConflict`，冲突后存储内容与条数不变；再加"先 nil 后非 nil"反方向）。

| 命令（修复后） | 结果 |
| --- | --- |
| `go test ./checkpoint -run 'TestCompletionDigest' -count=1 -v` | 18 个子用例全 PASS |
| `go test ./checkpoint -count=1`、`go vet ./...`、`go build ./...` | 通过，退出 0 |
| `go test ./... -count=1` | 退出 0，无 FAIL |
| `go test -race ./checkpoint ./agent ./queue -count=1` | 三个包 ok |

摘要相关的变异检查：摘要重新不带 Completion → 七种变化子用例在 memory/file 全部报"应冲突，实际: <nil>"（正是复核复现的缺陷）；摘要结构去掉 `omitempty` → 基准值用例报"无终态快照的摘要变了"；只漏掉 `Err` → 仅"Err 不同"两个子用例失败。

**本卡的其余登记：**

1. `cloneCompletion` 的 CloneMessage 错误分支没有专门用例：`core.Part` 带非导出标记方法 `isPart()`，包外测试无法构造"不可克隆的 part"，而 `core` 的 6 个导出 part（Text/Thinking/Image/Video/ToolCall/ToolResult）都可克隆。代码仍按契约返回错误，与 `cloneToolExecution` 同一条 `core.CloneMessage` 路径；不为此在 `core` 开测试后门。

### BC-03：结束凭据与 RecordCompletion

**状态：** 已完成（2026-10-12）。**文件：** 修改 agent/run.go、agent/durable_runtime.go；新增 agent/terminal.go、agent/terminal_test.go、agent/terminal_cas_test.go、agent/terminal_files_test.go。复用 agent/write_guard.go 的 commit 回执，不改变其 claim 续租与释放规则。

- [x] 先写 TestRecordCompletionUsesRunIdentity：接口按锁定签名，完成/失败结果和 RunID 一致；Wait 返回结果被修改后不污染冻结终态。
- [x] 在成功 commit 后保存 ID/revision，drive 释放 claim 前冻结边界，发布终态事件之前冻结 outcome/result/error。补 Run.Paused；TestRunPausedOutcome 覆盖暂停、空结果正常完成、失败，不用结果空值区分。
- [x] TestRunTerminalBoundaryBeforeEvents：并发 Iter/Events 与 Wait，终态事件可见或 Wait 返回时凭据与 Paused 已完整；覆盖成功、暂停、准备失败与执行失败。
- [x] TestRecordCompletionRejectsInvalidRun：nil、其他 Agent、未结束、暂停均拒绝且历史不变。
- [x] TestRecordCompletionRequiresOwnBoundary：准备失败、首步失败、仅继承旧 head、失败提交不更新凭据；不覆盖前次 Completion，不伪造根记录。
- [x] TestRecordCompletionRequiresDurable：基础 Checkpointer stub 返回 ErrDurableUnavailable，不调用 Save；前台仍可使用该 stub。
- [x] 实现固定 ID 的不可变记录缓存和并发保护，Load 冻结边界、深拷贝 State/FileSnapshot，检查 Pause/Pending 与 identity，使用 ExpectedRevision Commit。
- [x] TestRecordCompletionStaleRunRejected：用通道闸门确定 A 结束→B 完成→A 记录，断言 ErrRevisionConflict 且 B head/历史不变；Memory/File 均覆盖。
- [x] TestRecordCompletionCASAfterLoad：阻塞 A 的 Commit，在其 Load 后允许 B 提交，恢复 A 后仍拒绝；不是仅在 Load 时检查 revision。
- [x] TestRecordCompletionClaimConflict：边界不变但其他 owner 持有活跃 claim，返回 ErrClaimRequired，不携旧 claim、不重读新 head。
- [x] TestRecordCompletionIdempotent：并发/重复调用只追加一次；模拟提交成功但回执失败再重试；A 已记录、B 前进后 A 重试不改变 B head。Memory/File 均覆盖。
- [x] TestRecordCompletionPreservesFiles：写文件→记录终态→重开 File→新运行或 Fork 恢复文件，覆盖空快照和历史副本修改；不能只检查旧 head。
- [x] 运行 go test ./agent -run 'TestRecordCompletion|TestRunPaused' -count=1；go test -race ./agent ./checkpoint -count=1；记录结果。

**验收：** V4、V5、V6；接口和并发规则无需留给后续卡片决定。

**执行记录（2026-10-12）：**

实现形状：

- `agent/durable_runtime.go`：`durableRunState` 增加 `boundaryID/boundaryRevision/boundarySet`；`commitCheckpoint` 只在 durable 分支成功返回后调用 `noteCommittedBoundary(cp.ID, receipt.Revision)`，失败提交不更新；非 durable 的 `Store.Save` 分支没有回执，因此永不记录边界。新增 `committedBoundary()`。
- `agent/run.go`：`Run` 增加 `terminalMu` 保护的冻结区（frozen/paused/result/err/边界）与 `recordMu` 保护的记录缓存；`freezeTerminal` 在冻结时就用 `core.CloneMessage` 深拷贝结果；`Paused()` 只读冻结结局。`drive()` 的尾部分支改成先 `committedBoundary()`（claim 仍持有）→ `releaseDurableClaim()` → `terminalOutcome(out, cleanupErr)` → `freezeTerminal(...)` → 才发布终态事件；startErr 与 prepare 失败走 `freezeFailure`。原三分支的发布/finish 顺序与错误合并语义（`errors.Join`）逐条保持。
- `agent/terminal.go`：导出错误 `ErrNoRun`/`ErrRunNotOwned`/`ErrRunNotFinished`/`ErrRunPaused`/`ErrNoCommittedBoundary`，纯函数 `terminalOutcome`，以及 `RecordCompletion`：校验身份与结局 → 断言 store 支持 `checkpoint.Durable` → 校验本次边界 → `recordMu` 下固定终态 checkpoint ID、`buildTerminalRecord`（Load 边界并核对 ThreadID/ID/Revision、拒绝 Pause/Pending、`core.CloneStateData` 与逐字节拷贝 FileSnapshot 并保留 nil/空映射区别）→ `Durable.Commit` 带 `ExpectedRevision=冻结 revision`，不带 claim、ack、工具执行；错误保留 `errors.Is`。

| 命令 | 结果 |
| --- | --- |
| `go test ./agent -run 'TestRecordCompletion\|TestRunPaused\|TestRunTerminalBoundary' -count=1 -v` | 12 个顶层/子用例结构全 PASS（memory/file 双后端） |
| `go test ./agent -count=1` | ok（2.0s，既有 agent 测试未退化） |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok（agent 5.3s） |
| `go build ./...`、`go vet ./...`、`go test ./... -count=1` | 退出 0，无 FAIL |
| `gofmt -l`（六个新增/改动文件） | 无输出 |

变异检查（每次只改一处实现，恢复后复跑绿）：失败提交也记录边界 → `RequiresOwnBoundary/首步提交失败` 报"边界不存在"；提交不带 `ExpectedRevision` → `StaleRunRejected`（memory+file）与 `CASAfterLoad`、`ClaimConflict` 全部失败；不缓存记录（每次新 ID） → 幂等三个子用例失败（回执丢失重试、B 前进后重试 A 都变成 revision 冲突）；冻结时不深拷贝结果 → `UsesRunIdentity` 报"调用方改写污染了冻结终态"；把 `freezeTerminal` 移到发布终态事件之后 → `TerminalBoundaryBeforeEvents/成功` 立刻报 `run has not finished`（观察方先看到事件）；去掉暂停拒绝 → `PausedOutcome/暂停` 拿到"边界是暂停点"的错误而非 `ErrRunPaused`；去掉归属检查 → `RejectsInvalidRun` 的他人运行被接受。

**与卡的偏离（四条）：**

1. 校验顺序：`RecordCompletion` 把"store 支持 Durable"放在"有本次提交边界"之前。按 D4 原文的顺序，只有基础 Checkpointer 的存储永远走 `Store.Save` 分支、永远没有边界，`ErrDurableUnavailable` 就不可达（第一轮实测确实返回的是 `ErrNoCommittedBoundary`，与卡上 `RequiresDurable` 的期望冲突）。调整后两项契约都可观测，其余顺序不变。
2. `TerminalBoundaryBeforeEvents` 的"执行失败"实测有自己的成功提交：`AgentLoop.fail`（`agent/loop.go` 的 `l.fail`）会写一个不含 Pause/Pending 的接缝。所以失败运行是可记录的（Kind=failed、Err 为文本、Result 为 nil），"无本次边界"的期望只放在"准备失败"子用例上。
3. 测试按关注点拆成三个文件（`terminal_test.go` 身份/结局/拒绝、`terminal_cas_test.go` 交错与幂等、`terminal_files_test.go` 文件保留），卡上只列了 `terminal_test.go`；当时未新增内部测试文件，凭据观察全部通过公开行为与测试用存储包装器完成（评审后的修复另加了一个包内测试文件，见下）。
4. `Run` 同时保留 `<-run.done` 与 `terminalFrozen` 两项检查。实测在当前实现下二者等价（冻结先于发布与 finish），但 done 是既有的"运行结束"公开信号，保留它使 `RecordCompletion` 的契约不依赖冻结的内部时序；去掉 done 检查的变异不会被现有用例抓到（`ErrRunNotFinished` 由 frozen 检查兜住），登记为冗余防护而非缺陷。

**评审后修复（P2，2026-10-12）：**

评审指出：`noteCommittedBoundary` 无条件覆盖，而并行分支共用同一份 `durableRunState`（`agent/runtime.go:96` 的 `forBranch` 直接复制 `durable` 指针），回执返回的顺序可以晚于存储提交的顺序——较早 revision 的回执最后返回时，边界从更高 revision 退回到它，运行虽然正常完成，`RecordCompletion` 却用自己冻结的旧 revision 做比较，被当前 head 拒绝。

- 实现改动只有一处：`agent/durable_runtime.go` 的 `noteCommittedBoundary` 在 `d.mu` 内按 revision 单调更新——尚无边界时接受，已有边界时只有更高的 revision 能替换，且 `boundaryID` 与 `boundaryRevision` 作为一对一起写。同 revision 视为不动（同一线程内一个 revision 只对应一条记录，重试拿到的就是同一对值）。`committedBoundary()`、`RecordCompletion`、`drive` 的冻结顺序都没改。
- 新增 `agent/terminal_boundary_internal_test.go`（包内测试，因为要直接构造共享 durable 状态的分支执行环境并读取 `run.rc`）：`TestNoteCommittedBoundaryIsMonotonic` 钉住推进规则（更低/相同 revision 不动，更高 revision 成对替换）；`TestLateCommitReceiptKeepsHighestBoundary` 用只实现 `checkpoint.Durable` 的存储包装把 revision 1 的回执压住，让较新提交在本 goroutine 写完边界后再放行；`TestParallelRunRecordsCompletionWithLateReceipt` 跑真正的 `Parallel` 工作流，用分支的 `core.TurnDone` 事件（在同一步提交完成之后发布，`agent/loop.go:272`）证明另一条分支的边界已写入，此时才放行迟到的回执，再断言终态记录成功。
- 可达性实测：存储只实现 `Durable`、不实现 `ThreadCoordinator` 时（线程协调是可选能力，`checkpoint/coordinator.go:34` 的注释写明 Durable 对第三方实现保持不变），`commitCheckpoint` 走 `durable.Commit` 直连分支，两次提交真正并发，顺序错乱可以稳定构造。存储是 `ThreadCoordinator` 时，`writeGuard.commit` 的 `opMu`（`agent/write_guard.go:120`）把提交本身串行化，但边界写在锁释放之后，仍留有一个调度窗口——这个窗口无法从存储外部确定性地打开，所以确定性覆盖放在前一种存储；后一种存储下"正常并行运行仍能记录终态"这一契约由同一个用例覆盖。

| 命令 | 结果 |
| --- | --- |
| `go test ./agent -run 'TestLateCommitReceiptKeepsHighestBoundary\|TestNoteCommittedBoundaryIsMonotonic\|TestParallelRunRecordsCompletionWithLateReceipt' -count=1 -v` | 三个 PASS |
| 同三条用例 `-count=20` | ok（0.18s，无抖动） |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok（agent 5.4s） |
| `go build ./...`、`go vet ./...`、`go test ./... -count=1` | 退出 0，31 个包 ok，无 FAIL |
| `gofmt -l`（本轮两个新增/改动文件） | 无输出 |

反向验证（每次只改一处实现，恢复后复跑）：

1. 去掉单调守卫（恢复无条件覆盖），`-count=3` 三个用例 3/3 全红：`迟到回执把边界退回成 cp-early/1(true)，期望留在 cp-late/2`、`更低的 revision 之后边界 = cp-low/3(true)，期望 cp-high/5`、`并行运行的终态记录失败: agent: record completion: commit at revision 1: checkpoint: expected revision does not match head`。第三条的报错就是评审复现到的那个冲突。
2. 只更新 revision、不更新 ID（破坏"成对更新"），三个用例同样全红：`较新提交后的边界 = /2(true)，期望 cp-late/2`、`更低的 revision 之后边界 = /5(true)，期望 cp-high/5`、`并行运行的终态记录失败: agent: record completion: load boundary : checkpoint: parallel-late-receipt/ not found`。

附带核实的一条既有行为，登记不修：并行分支的边界写在共享的 durable 状态里，所以 `RecordCompletion` 记录的是"本次运行提交过的最高 revision"，而不是某一条分支自己的最后一步——与 D4"运行自己的提交边界"一致。

### BC-04：队列持久化等待与错误出口

**状态：** 已完成（2026-10-12）。**文件：** 修改 queue/agent_bridge.go；新增 queue/agent_terminal_test.go。

- [x] 新增 WaitRecorded 的专用 done、结果与合并错误存储；不改变 Wait/Events 原有执行语义。
- [x] worker 运行结束后检查 Paused，暂停跳过 RecordCompletion；否则用独立 30 秒 context 记录。无论记录成功或失败，保存收尾结果、关闭 done 后返回合并错误；defer 释放两个派生 context。
- [x] TestQueueWaitRecordedWaitsForCommit：用 commit 闸门证明 Wait 已返回而 WaitRecorded 仍等待；放行后返回。测试用通道同步，不靠短 sleep 猜时序。
- [x] TestQueueRecordFailureVisible：注入写入错误，Wait 保留运行结果，WaitRecorded 保留结果并可 errors.Is 命中写入错误；运行失败加记录失败时两者均可识别。
- [x] TestQueueCancelStillRecords：执行 ctx 取消后，记录 ctx 不继承取消且能写 failed（必须选择已存在本次提交边界的夹具）；无边界取消另测为可见记录错误。
- [x] TestQueuePauseSkipsCompletion：暂停成功收尾、零新增终态记录、仍可按既有审批流程 Resume。
- [x] TestQueueRecordContextDeadline：store stub 检查独立 ctx 的约 30 秒 deadline、工作 ctx 已取消时仍可用，返回后 Done 关闭；无需真的等待 30 秒。
- [x] TestQueueWaitRecordedMultipleWaiters：多等待者结果一致且修改一份结果不污染其他等待者；预先启动 worker。另用闸门证明未消费时 WaitRecorded 不驱动运行，随后消费以结束测试。
- [x] 运行 go test ./queue -count=1；go test -race ./queue ./agent -count=1；记录结果。

**验收：** V7；不能以 Pool 忽略了 job 错误却测试通过来替代对外可见性验证。

**执行记录（2026-10-12）：**

实现形状（`queue/agent_bridge.go`）：

- `Handle` 新增 `owner *agent.Agent`（记录终态要用它，`RecordCompletion` 会拒绝别的 Agent 的 Run）、`settle chan struct{}` + `settleOnce` + `settleMu/settleRes/settleErr`。`Wait` 与 `Events` 一字未改，仍是运行级语义；`Wait` 的注释补上"成功不代表终态已落盘"。
- worker 闭包：`defer h.cancelWork()` → `h.run.Wait()`（只为驱动运行到结束）→ `res, runErr := h.run.FrozenOutcome()` → `err := errors.Join(runErr, h.commitTerminal(workCtx))` → `h.recordSettled(res, err)`（存结果、关 done）→ 再把同一个 `err` 返回给 Pool。收尾结果取自运行冻结的结局，不是 `Wait` 递出的活对象（评审后修复，见下）。
- `commitTerminal`：`h.run.Paused()` 为真直接返回 nil，不发起记录；否则 `context.WithTimeout(context.WithoutCancel(workCtx), terminalRecordTimeout)`（`const terminalRecordTimeout = 30 * time.Second`）+ `defer cancel()`，`workCtx` 的值保留、取消不继承。
- `WaitRecorded`：只 `<-h.settle`，绝不驱动运行；返回前对结果 `core.CloneMessage`，每个等待者拿到自己的副本。
- `recordSettled` 用 `settleOnce` 保护：重复投递同一 job 不会二次 close，也不会替换已上报的收尾结果。它存的就是 `FrozenOutcome` 给出的独立副本，不再另拷一次。

测试（`queue/agent_terminal_test.go`，包内测试，因为要观察 Handle 的收尾字段并自己消费 `recordQueue` 里的任务）。夹具 `settleStore` 包装 Memory 的线程协调能力，只截 Completion 非空的提交：可卡在闸门、可拒绝写入、并记下提交那一刻的 context、其 `Err()` 与剩余期限。六个用例覆盖 V7：闸门证明 Wait 已返回而 WaitRecorded 仍等；记录写入失败时 Wait 仍拿到结果、WaitRecorded 的错误同时可 `errors.Is` 命中注入错误并保留运行自身的文本；取消后仍写 failed 且归属正确的 RunID；无边界（取消先于消费）暴露 `agent.ErrNoCommittedBoundary`；暂停零终态提交、暂停元数据仍在、`Resume + Allow("c1")` 恢复后仍零终态；记录 context 独立、剩余期限接近 30 秒、收尾后被释放；三等待者内容一致互不污染、未消费时不驱动运行（模型调用数为 0）。

| 命令 | 结果 |
| --- | --- |
| `go test ./queue -run TestQueue -count=1 -v` | 全部 PASS，含 BC-01 既有六个用例未退化 |
| `go test ./queue -count=5` | ok（5.65s，两处 300ms 负向观察无抖动） |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok（queue 2.1s、agent 5.5s、checkpoint 3.7s） |
| `go build ./...`、`go vet ./...`、`go test ./... -count=1` | 退出 0，31 个包 ok，无 FAIL |
| `gofmt -l queue/agent_terminal_test.go` | 无输出。`queue/agent_bridge.go` 被标记是既有 CRLF 行尾所致（该文件工作区 196 行全为 CRLF，`git show HEAD` 的入库副本是 LF）；把改动内容转成 LF 再 `gofmt -l` 无输出，未重排行尾 |

反向验证（每次只改一处实现，恢复后复跑；输出为实测的第一条失败信息）：

1. 记录还没提交就先放开 WaitRecorded → `终态提交还卡在闸门上，WaitRecorded 就返回了: <nil>`。
2. 去掉 `Paused()` 跳过 → `暂停的收尾不应报错: agent: run is paused, not terminal`。
3. 记录 context 改为继承 `workCtx` → `记录 context 继承了执行的取消: context canceled`。仓库自带的 Memory 在提交时忽略 context，光看"是否写成功"抓不到这一条，所以断言直接读提交那一刻的 `ctx.Err()`（见下方偏离 2）。
4. 去掉 30 秒期限 → `记录 context 剩余期限 = 0s，期望接近 30s`。
5. `defer cancel()` 改成 `_ = cancel` → `收尾结束后记录 context 未释放`。
6. 合并错误只留运行错误 → `WaitRecorded 未暴露记录错误: <nil>`。
7. 合并错误只留记录错误 → `WaitRecorded 掩盖了运行自身的错误: ...`。
8. 去掉每个等待者的副本 → `等待者 1 的结果被改写污染`。
9. 把收尾结果的来源改回 `Wait` 递出的活对象 → 两条时序用例都报 `WaitRecorded 返回 "调用方改写"，期望 "原始结果"`（评审后修复新增，见下）。
10. 去掉 `FrozenOutcome` 内的副本 → `agent` 侧用例报 `已提交的终态被出口副本改写: "被拿到副本的一方改写"`。
11. `WaitRecorded` 内先调 `h.run.Wait()` → `WaitRecorded 驱动了运行，模型调用 1 次`。

**与卡的偏离（四条）：**

1. 收尾结果不走 `Wait` 的返回值，而是新增 `Run.FrozenOutcome()` 从运行冻结的结局取独立副本；`WaitRecorded` 再为每个等待者各拷一份。卡上只写了"多等待者互不污染"，但 `Wait` 递出的是运行自己的结果对象，调用方就地改写会让收尾结果与已落盘终态不一致（评审 P2，见下），所以隔离边界必须建在来源上。用例见反向验证 8~10。
2. "记录 context 不继承取消"不能用"记录是否写成功"来证明：Memory 的 `Commit` 忽略 context（`checkpoint/durable_memory.go:95` 的形参就是 `_`），继承取消也会写成功。测试改为在提交那一刻抓 `ctx.Err()` 与剩余期限并断言。这一条是补强，不是放宽。
3. 无边界场景独立成 `TestQueueRecordVisibleWithoutBoundary`，没有塞进取消用例：用 `recordQueue` 让任务先不被消费，测试自己 `h.Cancel()` 后再调用 `job.Run` 消费，这样 prepare 在已取消的执行 context 上失败、本次没有任何提交。同时断言闭包返回给 Pool 的也是同一个合并错误，呼应验收行"不能以 Pool 忽略 job 错误代替对外可见性"。
4. `Handle.owner` 是新增字段。`RecordCompletion` 是 Agent 的方法且拒绝别人的 Run，队列必须记住这个 Run 属于哪个 Agent；不新增该字段就只能让调用方自己保管 Agent，等于把已复现过的身份错配留给调用方。

**评审后修复（P2，2026-10-12）：**

评审指出：worker 从 `h.run.Wait()` 拿到的结果与 `Handle.Wait()` 递给调用方的是同一个消息对象，而收尾副本要到 `recordSettled` 才拷——这中间调用方就地改写 `Wait()` 的结果，就会污染随后保存的收尾结果，使它和已落盘的终态不一致。复现出两种时序：终态提交卡在闸门期间改写；worker 还没消费任务、调用方先自己 `Wait()` 驱动运行并改写、之后再消费。两种都得到 `WaitRecorded` 返回 `"调用方改写"` 而持久化终态仍是原文。

- 实现改动：新增 `agent.Run.FrozenOutcome()`（`agent/run.go`）——在 `terminalMu` 下返回 drive 冻结结局的独立副本 + 冻结错误，未结束时返回零值；队列闭包改成先 `h.run.Wait()` 驱动、再 `res, runErr := h.run.FrozenOutcome()`，收尾与合并错误都以这份冻结结局为准；`recordSettled` 不再另拷一次。这样"改写 Wait 返回值"根本碰不到收尾副本：drive 在发布终态事件之前就把结局冻结成另一份拷贝，`Wait` 递出的活对象再怎么被改，`FrozenOutcome` 拿到的仍是冻结内容。
- 队列侧不新增等待路径：`FrozenOutcome` 在 `Wait` 返回之后调用，此时 done 已关、结局必然已冻结（drive 的既有顺序保证，见 BC-03 偏离 4），不需要额外的"还没冻结怎么办"分支。
- 新增用例：`TestQueueWaitRecordedIsolatedFromWaitEdits`（两个子用例，各自复现评审的一种时序，断言收尾结果与已落盘终态文本都仍是原文）、`agent` 侧 `TestFrozenOutcomeIsDeepCopy`（拿到出口副本就地改写后，`RecordCompletion` 仍写冻结内容）。原有"改过 Wait 返回值再等记录"子用例保留，它测的是改写发生在收尾之后。

| 命令 | 结果 |
| --- | --- |
| 修复前 `go test ./queue -run TestQueueWaitRecordedIsolatedFromWaitEdits -count=1 -v` | 两个子用例都 FAIL：`WaitRecorded 返回 "调用方改写"，期望 "原始结果"`（评审复现确认） |
| 修复后同三条相关用例（含 `TestFrozenOutcomeIsDeepCopy`）`-count=2` | 全 PASS |
| `go test ./queue ./agent -count=3` | 两包 ok，无抖动 |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok |
| `go build ./...`、`go vet ./...`、`go test ./... -count=1` | 退出 0，31 包 ok，无 FAIL |
| 反向验证（对应上表第 9、10 条） | 两项改动各自被抓：来源改回活对象 → 两条时序用例红；去掉出口副本 → `TestFrozenOutcomeIsDeepCopy` 红 |

一处实测澄清：把"改写"与"拷贝"做成真正并发，`-race` 在本用例里报不出来（改写点在闸门放行之前、拷贝在放行之后，二者已被串行化）。评审提到的并发读写风险没有独立复现，但按来源修复后共享对象不再被读取，风险随之消失。

V7 通过（含上述修复）；V2、V8 留给 BC-05，V9 留给 BC-06。

### BC-05：状态查询、治理与兼容性

**状态：** 已完成（2026-10-12）。**文件：** 新增 agent/run_status.go、agent/run_status_test.go、queue/background_budget_test.go；复用已有 mock 与工具 timeout 夹具。

- [x] TestRunStatusStates：不存在、有步骤无终态、暂停、completed、failed；字段与 ADR D5 一致。对暂停另走实际 Resume 流程，不把 Paused 当作保证恢复成功。
- [x] TestRunStatusErrorsAndCopies：非法 ID、store 读取失败、深拷贝出口；确认查询不 Save/Commit/Claim/续租。
- [x] TestRunStatusOldCompletionIsSnapshot：新 run 开始但尚无新 checkpoint 时查询仍可看到旧终态，注释解释不能推断 Running=false。
- [x] TestBackgroundRunBudgetWrapsUp：注入时钟越过 MaxDuration，mock 返回正常收尾，WaitRecorded 后为 completed；不得期待预算错误 failed。
- [x] TestBackgroundToolTimeoutBoundsWait：有限 WithToolTimeout 触发取消/abandon，检查框架结束等待；不宣称工具 goroutine 已被杀死。
- [x] TestBackgroundBudgetDoesNotCancelInflightTool：用工具开始/释放通道和注入时钟验证 duration 不能打断未配工具超时的在途工具；测试必须主动释放工具后收尾。
- [x] TestBackgroundMaxTurnsBoundsModelLoop：只断言有限、会返回的模型调用受轮次上限约束，不推广到任意 Runnable 或阻塞 I/O。
- [x] TestNonDetachedHistoryCountUnchanged：不调用 RecordCompletion 的前台路径沿用已有历史条数基线；禁止为终态记录改写旧断言。
- [x] 运行 go test ./agent ./queue ./middleware -count=1；记录结果。

**验收：** V2、V8；治理测试证明实际边界，不制造硬期限承诺。

**执行记录（2026-10-12）：**

实现形状（`agent/run_status.go`）：

- `RunStatus` 结构体字段与 D5 一致（`ThreadID`/`Exists`/`Paused`/`Revision`/`Step`/`Completion`/`Latest`）；`(*Agent).RunStatus(ctx, threadID)` 只做两件事：先 `core.CheckThreadID`，再 `a.store.Latest`。store 错误原样返回且不填任何字段；`Latest == nil` 时 `Exists` 为假；`Paused = Completion == nil && (Pause != nil || Pending != nil)`；`Completion` 与 `Latest.Completion` 取自同一个对象。
- 没有新增写入路径，也没有新增深拷贝 API（见偏离 2）。文档里写明它不能回答"现在是否在跑"，以及 `Paused` 不是恢复成功的承诺。

测试：

- `agent/run_status_test.go`（外部测试包）：`statusCountingStore` 包装 Memory 的协调能力并统计 Save/Load/Latest/History/Commit/BeginRun/RenewClaim/ReleaseClaim 次数，同时可注入 Latest 错误。`TestRunStatusStates`（memory/file 双后端，含"两种暂停元数据"）、`TestRunStatusErrorsAndCopies`（四个子用例）、`TestRunStatusOldCompletionIsSnapshot`、`TestNonDetachedHistoryCountUnchanged`。
- `queue/background_budget_test.go`（包内测试）：`countingToolModel`（有工具就调用、工具被撤下就用文本回答，并数模型次数）、`heldTool`（阻塞直到放行，并记下确实走过等待分支）、`ignoresContextTool`（不看 context 自己睡 20 秒）、`fastClockAfter`（前 n 次读数不变、其后跳到一小时后）。四个用例都走真实后台路径（`EnqueueAgent` + `NewMemQueue` + `Pool` + `WaitRecorded` + `RunStatus`）。

| 命令 | 结果 |
| --- | --- |
| `go test ./agent -run 'TestRunStatus\|TestNonDetached' -count=1 -v` | 四个顶层用例 + 全部子用例 PASS（memory/file 双后端） |
| `go test ./queue -run TestBackground -count=1 -v` | 四个治理用例 PASS |
| `go test ./agent -run 'TestRunStatus\|TestNonDetached' -count=3`、`go test ./queue -run 'TestBackground\|TestQueue' -count=3` | ok（0.43s / 3.70s，两处 500ms 与 300ms 负向观察无抖动） |
| `go test ./middleware -count=1` | ok（预算实现未改动，只被用来构造用例） |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok（queue 2.7s、agent 5.6s、checkpoint 3.7s） |
| `go build ./...`、`go vet ./...`、`go test ./... -count=1` | 退出 0，31 包 ok，无 FAIL |
| `gofmt -l`（三个新增文件） | 无输出 |

反向验证（每次只改一处实现，恢复后复跑；引号为实测失败信息）：

`agent/run_status.go`：
1. `Paused` 只看 `Pause` → `只有待办工具 应读成暂停: {... Paused:false ...}`。
2. `Paused` 只看 `Pending` → `只有暂停描述 应读成暂停: {... Paused:false ...}`。
3. 去掉 `core.CheckThreadID` → `非法 ID "" 被接受`。
4. 吞掉 store 错误 → `错误 = <nil>，期望原样返回 存储不可用`。
5. 不填 `Completion` → `终态不符: <nil>`、`失败终态不符: <nil>`。
6. 查询里顺手 `Save` 一次 → `查询触发了 Save：0 -> 3`。
7. 空线程也当存在 → `未知线程读出了内容: {Exists:true ...}`。

治理机制（被测实现，不是测试自己）：
8. `middleware.RunBudget.ModifyRequest` 不再撤工具（永不进入收尾模式）→ `预算超限应正常收尾，实际: agent: loop exceeded MaxTurns`。
9. `agent/exectools.go` 忽略配置的工具超时（走无限等待分支）→ `框架仍在等待一个不看 context 的工具`。
10. `agent/loop.go` 轮次上限放宽 3 → `模型调用 6 次，期望等于上限 3`。
11. `middleware.RunBudget.BeforeTool` 去掉"这一轮请求确实带了工具"的门槛（预算越过后在工具执行前就拦停）→ `在途工具没有开始`。

**与卡的偏离（三条）：**

1. 暂停的两个元数据字段要各测一半。真实中断夹具（审批闸门）写下的快照同时带 `Pause` 与 `Pending`，所以只看其中任一半的实现都能通过，第一轮实测里"只看 Pause""只看 Pending"两个改动都是绿的。补了"两种暂停元数据"子用例：直接向 store 写两份各只含一个字段的快照。补完后两条都变红。
2. `RunStatus` 不额外做深拷贝，也不为此在 checkpoint 包开新的导出 API。理由：`checkpoint` 已有的两个 store 在读取时就交出副本（`checkpoint/memory.go:67` 的 `Latest` 走 `cloneCheckpoint`；`checkpoint/file.go:509` 每次从日志解码新对象），实测"改写出口再重读"两条后端都不污染历史；在 agent 再拷一次没有可观测差别，还会把"store 自己决定交出什么"这条边界藏起来。已在 `Latest` 字段注释里写明这层边界由 store 决定。登记不修。
3. 时钟注入按"第几次读数"跳变，`TestBackgroundBudgetDoesNotCancelInflightTool` 还要保证越过上限的时机：上限必须在模型已答应调用工具、`AfterModel` 记完账之后才跨过，否则 `ModifyRequest` 会先撤掉工具、那一轮根本不会发起工具调用。实现方式是让时钟依赖模型调用计数（`calls >= 1` 之后跳一小时），并在用例里先断言 `OnExceed` 报出的资源含 `duration`，再断言运行没结束——避免"其实没越界"的空转。同时 `heldTool` 记下自己确实走了等待分支，防止"工具立刻返回"让负向观察失去意义。

BC-05 未发现需要另卡修复的缺陷。V2、V8 通过；V9（跨进程与最终回归）留给 BC-06。

### BC-06：文档、示例与最终验收

**状态：** 待实施。**文件：** 新增 examples/background-status/main.go；更新 README.md、queue 包/Handle 注释、examples/background/main.go 的示例指引及本 ADR 的实施结果。跨进程测试放 agent/run_status_process_test.go，复用 checkpoint/coordinator_process_test.go 风格。

- [x] 示例配置 File store、RunBudget、非零 WithToolTimeout，启动 worker 后入队、取消提交 ctx、等待 WaitRecorded，再查 RunStatus；处理运行/记录错误，不把 Wait 当落盘确认。
- [x] 文档解释记录 CAS 冲突、无边界、基础 store 不支持、崩溃窗口；用户应如何检查 WaitRecorded 错误，不示范读取新 head 后盲目重写旧终态。
- [x] 解释 Pool 停止与 Handle.Cancel 区别，未消费任务的等待边界，30 秒记录期限与整次运行期限区别；保持不做列表。
- [x] 若 README 仍将 bus 描述为内置 Redis 后端，核对后将跨进程能力指向 queue/redis_pubsub.go 与 queue/redis_stream.go，不改无关实现。
- [x] TestRunStatusAcrossProcess：子进程完成后台任务并确认 WaitRecorded 后退出，父进程新开 File store 读取 completed 和文件；真正使用子进程，不以新 Agent 实例冒充跨进程。
- [x] 执行以下最终命令，记录实际输出与环境限制：

    go build ./...
    go vet ./...
    go test ./... -count=1
    go test -race ./queue ./agent ./checkpoint -count=1

- [x] 核对 V1–V9 每项已有对应通过证据；回填 ADR 实施结果。偏离已决契约时明确记录原因与追加决议，不默默改写验收目标。

**验收：** V9 及全部前置卡；不以“待运行”标记为通过。

**执行记录（2026-10-12）：**

交付物：

- `examples/background-status/main.go`（新增，离线 mock）：File store + `middleware.RunBudget{MaxDuration: 500ms, MaxTurns: 8}` + `agent.WithToolTimeout(3s)`；先 `go pool.Run(ctx)` 再 `EnqueueAgent`，入队成功后立刻取消提交 ctx，用 `WaitRecorded` 等落盘（错误按 `errors.Is` 分成 `ErrRevisionConflict`/`ErrClaimConflict`/`ErrStaleFencingToken`/`ErrDurableUnavailable`/`agent.ErrNoCommittedBoundary` 五类各给一句处置说明），再用 `Agent.RunStatus` 读 head，最后换一个新的 `checkpoint.NewFile(dir)` 实例 + 新 Agent 在同一线程跑一次 `Run` 读回 `report.txt`。注释与输出都写明 `Wait` 不是落盘确认、`MaxDuration` 是正常收尾不是硬期限、能打断不回应工具的是 `WithToolTimeout`。
- `agent/run_status_process_test.go`（新增）：`TestRunStatusAcrossProcess` 用 `os.Executable()` 重跑本测试二进制作为子进程（环境变量 `GOAGENT_RUN_STATUS_PROCESS_HELPER=1` 分支）。子进程走完整后台链路（`NewMemQueue` + `Pool` + `EnqueueAgent` + 取消提交 ctx + `WaitRecorded` 返回 nil + 自己那次 `RunStatus`），把 JobID/RunID/head ID/revision/回答/Kind/文件内容一行 JSON 交回；父进程等子进程退出后用全新 `checkpoint.NewFile(dir)` 与全新 Agent 读出同一条 completed 终态、同样的 `report.txt`，再在同线程新运行里通过 `peek` 工具把文件读回来，并断言新运行把 head 推前。
- 文档：README 新增“后台运行的终态与状态查询”一节（归属、两个终点、谁写终态及其拒绝、CAS/claim/fence 冲突与"不读新 head 改写旧终态"、崩溃窗口、三条时间线、RunStatus 的四条限制、保持不做清单）；修正“异步任务与事件总线”一行——`bus` 包只有进程内 pub/sub，跨进度事件走 `queue.NewBus(queue.WithRedis(url))`（`queue/redis_pubsub.go`），持久化队列在 `queue/redis_stream.go`；示例表加 `background-status` 一行；`examples/background/main.go` 顶部指向新示例；`queue` 包文档与 `Pool.Run` 注释补上“Pool 的 ctx 取消只停止取任务，不中止已在执行的运行；要用 Handle.Cancel”。

| 命令 | 结果 |
| --- | --- |
| `go run ./examples/background-status` | 退出 0；实测输出：`[budget] 资源 [duration] 越过上限，改为正常收尾` → `✅ 运行结束且终态记录已确认: 本季度报告已生成并归档。` → `🔎 RunStatus: exists=true paused=false revision=3 step=1 kind=completed run=run_…` → `快照里的文件: report.txt = "队列不再随提交方取消。"` → `♻️ 同线程新运行从终态快照恢复文件: 读回: 队列不再随提交方取消。` → `📌 新运行把 head 从 revision 3 推到 5，此刻 head 上还带着终态记录吗: false`（跑三次结构一致，只有 ID 与临时目录名变化） |
| `go run ./examples/background` | 退出 0（只改了注释，行为不变） |
| `go test ./agent -run '^TestRunStatusAcrossProcess$' -count=1 -v` | PASS（0.09s） |
| `go test ./agent -run '^TestRunStatusAcrossProcess$' -count=3`、`go test -race ./agent -run '^TestRunStatusAcrossProcess$' -count=2` | ok（0.35s / 3.50s），无抖动 |
| `go build ./...` | 无输出，退出 0 |
| `go vet ./...` | 无输出，退出 0 |
| `go test ./... -count=1` | 退出 0，31 个包 ok，0 个 FAIL，58 个包无测试文件 |
| `go test -race ./queue ./agent ./checkpoint -count=1` | 三包 ok（queue 2.66s、agent 6.73s、checkpoint 3.76s） |
| `gofmt -l`（两个新增文件） | 无输出。`queue/queue.go`、`queue/worker_pool.go`、`examples/background/main.go` 被标记是既有 CRLF 行尾（三个文件全部行均为 CRLF，`gofmt -d` 只有行尾差异，`queue/`、`agent/` 下多个未改动文件同样被标记），未重排行尾 |

环境限制：本轮全部验收为离线（`llm/mock`、临时目录里的 JSONL 文件存储、测试二进制自重跑）。`queue/redis_pubsub.go`、`queue/redis_stream.go` 与 `examples/redis` 没有自动化测试，最终门禁不能替它们背书。跨进程用例依赖 `os.Executable()` 重跑测试二进制（Windows 上实测可用），子进程 `-test.timeout=120s`。

反向验证（删代码变异，每次只改一处）：把 `queue/agent_bridge.go` 的 `commitTerminal` 改成无条件 `return nil`（不再写终态）后，`TestRunStatusAcrossProcess` 变红为 `子进程退出失败: exit status 1`，且失败信息里完整带出子进程自己的 `WaitRecorded 之后没有终态记录`——同时证明父进程确实读的是磁盘上由另一个进程写下的记录，也证明 stdout 采集能交出跨进程诊断。恢复 `queue/agent_bridge.go`（`git checkout --`，工作区干净）后复跑 ok。

V1–V9 对照（全部为本轮实测通过，非“待运行”）：

| 验收 | 证据 |
| --- | --- |
| V1 | `queue/agent_bridge_test.go`：`TestQueueCallerCancelDoesNotStopRun`、`TestQueueEnqueueFailsWithCanceledCaller`、`TestQueueEnqueueFailureReleasesContext`、`TestQueueHandleCancelStops`、`TestQueueHandleCancelReleasesUnstartedJob`、`TestQueueContextReleasedAfterSettlement` |
| V2 | `queue/background_budget_test.go`：`TestBackgroundRunBudgetWrapsUp`、`TestBackgroundToolTimeoutBoundsWait`、`TestBackgroundBudgetDoesNotCancelInflightTool`、`TestBackgroundMaxTurnsBoundsModelLoop` |
| V3 | `checkpoint/completion_contract_test.go`：`TestCompletionRoundTripAcrossStores`、`TestCompletionJSONShape`、`TestCompletionFrozenAcrossStores`、`TestCompletionLegacyReadable`、`TestCompletionForkClearsTerminal`、`TestCompletionDigestKeepsNilBaseline`、`TestCompletionDigestDistinguishesTerminal` |
| V4 | `agent/terminal_test.go`：`TestRecordCompletionUsesRunIdentity`、`TestRunPausedOutcome`、`TestRunTerminalBoundaryBeforeEvents`、`TestRecordCompletionRejectsInvalidRun`、`TestRecordCompletionRequiresOwnBoundary`、`TestRecordCompletionRequiresDurable`、`TestFrozenOutcomeIsDeepCopy`；`agent/terminal_boundary_internal_test.go`：`TestNoteCommittedBoundaryIsMonotonic` |
| V5 | `agent/terminal_cas_test.go`：`TestRecordCompletionStaleRunRejected`、`TestRecordCompletionCASAfterLoad`、`TestRecordCompletionClaimConflict`、`TestRecordCompletionIdempotent`；`agent/terminal_boundary_internal_test.go`：`TestLateCommitReceiptKeepsHighestBoundary`、`TestParallelRunRecordsCompletionWithLateReceipt` |
| V6 | `agent/terminal_files_test.go`：`TestRecordCompletionPreservesFiles`、`TestRecordCompletionPreservesEmptyFileView` |
| V7 | `queue/agent_terminal_test.go`：`TestQueueWaitRecordedWaitsForCommit`、`TestQueueRecordFailureVisible`、`TestQueueCancelStillRecords`、`TestQueueRecordVisibleWithoutBoundary`、`TestQueuePauseSkipsCompletion`、`TestQueueRecordContextDeadline`、`TestQueueWaitRecordedMultipleWaiters`、`TestQueueWaitRecordedIsolatedFromWaitEdits` |
| V8 | `agent/run_status_test.go`：`TestRunStatusStates`、`TestRunStatusErrorsAndCopies`、`TestRunStatusOldCompletionIsSnapshot`、`TestNonDetachedHistoryCountUnchanged` |
| V9 | `agent/run_status_process_test.go`：`TestRunStatusAcrossProcess`；四条最终门禁命令见上表 |

偏离与登记：

1. 跨进程测试的读法与卡面不同：父进程先等子进程退出，再一次性读完整段 stdout 并从中取 JSON 行；原设计"读一行就解码"会把 go test 写在 stdout 的失败诊断和交付行撞在一起，变异红时诊断信息拿不到（实测改后能完整带出）。不改验收目标，只改采集方式。
2. `examples/background-status` 里 RunBudget 的 500ms 一定被 800ms 的工具越过（靠 sleep 下限保证，不靠时钟注入），所以示例稳定演示“越过后正常收尾、终态是 completed”；工具上限设 3s，因此示例不演示 abandon——那条路径由 `queue/background_budget_test.go` 与 `examples/tool-timeout*` 负责，示例注释里指明了分工。
3. 保持不做清单未增删：公开 detach API、事件重放、跨进程 agent job、Running 登记表、整次运行硬期限、前台自动写终态，全部继续排除。

## 五、风险与控制

| 风险 | 控制 |
| --- | --- |
| A 的旧结果污染 B 的状态 | 冻结本次 commit 边界 + ExpectedRevision CAS + 交错测试 |
| 终态覆盖文件恢复入口 | 完整克隆 FileSnapshot + 重开 File/新运行/Fork 测试 |
| 记录失败无人知晓 | WaitRecorded 暴露合并错误；明确无人等待时无主动通知保证 |
| 暂停误记为 completed | Run 内部 outcome + Paused + RecordCompletion 双层拒绝 |
| 取消让终态也无法写入 | 独立有截止时间的记录 ctx，注入测试检查 |
| 完成后崩溃、首步失败没有终态 | 明确崩溃窗口/无边界错误，不把旧快照当本次结果 |
| 基础 store 被误认为有 CAS | ErrDurableUnavailable，不 Save 降级 |
| 把预算当作硬 deadline | 正常收尾与在途阻塞测试、有限工具超时示例 |
| 重复记录与结果被修改 | 固定 ID、冻结提交内容、并发保护、深拷贝出口 |
| 前台历史回归 | 前台不自动记录，保留已有断言 |

## 六、交付与本次文档验证

公开契约为 checkpoint.Completion、Agent.RecordCompletion(ctx, run)、Run.Paused、Run.FrozenOutcome（BC-04 复核后新增的只读出口）、Handle.WaitRecorded、Agent.RunStatus 和 RunStatus。这些契约均已实施，不再使用旧 threadID/res/err 签名或普通 Save 方案。

本次文档修订已对照 agent/run.go、agent/durable_runtime.go、agent/write_guard.go、checkpoint/durable.go、checkpoint/snapshot.go、checkpoint/file.go、queue/agent_bridge.go、queue/worker_pool.go、middleware/runbudget.go 与 agent/options.go 的相关符号。实施阶段（BC-01…BC-06，2026-10-12）逐卡运行了测试并回填执行记录，最终门禁为 `go build ./...`、`go vet ./...` 退出 0，`go test ./... -count=1` 31 包 ok、无 FAIL，`go test -race ./queue ./agent ./checkpoint -count=1` 三包 ok；用例数量以各卡记录为准，不沿用旧稿固定的“18 个用例”数字。
