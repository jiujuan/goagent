# Background Run Context Ownership and Thread Terminal Status Implementation Plan

> 执行者说明：按 executing-plans 技能串行执行 BC-01 至 BC-06，先写对应失败测试，再做最小实现，检查通过后勾选。本文接口与测试均为待实施设计，不能描述为当前已有能力。使用符号名定位代码，不依赖旧稿行号。

- 日期：2026-10-12；状态：BC-01、BC-02、BC-03 已完成（见各卡执行记录），BC-04 至 BC-06 未开工。
- 依据：[ADR-0033](../adr/ADR-0033-background-run-context-and-status.md) 的审核修订版 D1–D6、V1–V9。
- 工作目录：D:/codeproject/mygo/goagent。下文文件路径均相对此目录。
- 未开工的卡片所写接口与测试仍是设计，不能描述为当前已有能力；已落地范围以各卡执行记录为准。

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

**状态：** 待实施。**文件：** 修改 queue/agent_bridge.go；新增 queue/agent_terminal_test.go。

- [ ] 新增 WaitRecorded 的专用 done、结果与合并错误存储；不改变 Wait/Events 原有执行语义。
- [ ] worker 运行结束后检查 Paused，暂停跳过 RecordCompletion；否则用独立 30 秒 context 记录。无论记录成功或失败，保存收尾结果、关闭 done 后返回合并错误；defer 释放两个派生 context。
- [ ] TestQueueWaitRecordedWaitsForCommit：用 commit 闸门证明 Wait 已返回而 WaitRecorded 仍等待；放行后返回。测试用通道同步，不靠短 sleep 猜时序。
- [ ] TestQueueRecordFailureVisible：注入写入错误，Wait 保留运行结果，WaitRecorded 保留结果并可 errors.Is 命中写入错误；运行失败加记录失败时两者均可识别。
- [ ] TestQueueCancelStillRecords：执行 ctx 取消后，记录 ctx 不继承取消且能写 failed（必须选择已存在本次提交边界的夹具）；无边界取消另测为可见记录错误。
- [ ] TestQueuePauseSkipsCompletion：暂停成功收尾、零新增终态记录、仍可按既有审批流程 Resume。
- [ ] TestQueueRecordContextDeadline：store stub 检查独立 ctx 的约 30 秒 deadline、工作 ctx 已取消时仍可用，返回后 Done 关闭；无需真的等待 30 秒。
- [ ] TestQueueWaitRecordedMultipleWaiters：多等待者结果一致且修改一份结果不污染其他等待者；预先启动 worker。另用闸门证明未消费时 WaitRecorded 不驱动运行，随后消费以结束测试。
- [ ] 运行 go test ./queue -count=1；go test -race ./queue ./agent -count=1；记录结果。

**验收：** V7；不能以 Pool 忽略了 job 错误却测试通过来替代对外可见性验证。

### BC-05：状态查询、治理与兼容性

**状态：** 待实施。**文件：** 新增 agent/run_status.go、agent/run_status_test.go、queue/background_budget_test.go；复用已有 mock 与工具 timeout 夹具。

- [ ] TestRunStatusStates：不存在、有步骤无终态、暂停、completed、failed；字段与 ADR D5 一致。对暂停另走实际 Resume 流程，不把 Paused 当作保证恢复成功。
- [ ] TestRunStatusErrorsAndCopies：非法 ID、store 读取失败、深拷贝出口；确认查询不 Save/Commit/Claim/续租。
- [ ] TestRunStatusOldCompletionIsSnapshot：新 run 开始但尚无新 checkpoint 时查询仍可看到旧终态，注释解释不能推断 Running=false。
- [ ] TestBackgroundRunBudgetWrapsUp：注入时钟越过 MaxDuration，mock 返回正常收尾，WaitRecorded 后为 completed；不得期待预算错误 failed。
- [ ] TestBackgroundToolTimeoutBoundsWait：有限 WithToolTimeout 触发取消/abandon，检查框架结束等待；不宣称工具 goroutine 已被杀死。
- [ ] TestBackgroundBudgetDoesNotCancelInflightTool：用工具开始/释放通道和注入时钟验证 duration 不能打断未配工具超时的在途工具；测试必须主动释放工具后收尾。
- [ ] TestBackgroundMaxTurnsBoundsModelLoop：只断言有限、会返回的模型调用受轮次上限约束，不推广到任意 Runnable 或阻塞 I/O。
- [ ] TestNonDetachedHistoryCountUnchanged：不调用 RecordCompletion 的前台路径沿用已有历史条数基线；禁止为终态记录改写旧断言。
- [ ] 运行 go test ./agent ./queue ./middleware -count=1；记录结果。

**验收：** V2、V8；治理测试证明实际边界，不制造硬期限承诺。

### BC-06：文档、示例与最终验收

**状态：** 待实施。**文件：** 新增 examples/background-status/main.go；更新 README.md、queue 包/Handle 注释、examples/background/main.go 的示例指引及本 ADR 的实施结果。跨进程测试放 agent/run_status_process_test.go，复用 checkpoint/coordinator_process_test.go 风格。

- [ ] 示例配置 File store、RunBudget、非零 WithToolTimeout，启动 worker 后入队、取消提交 ctx、等待 WaitRecorded，再查 RunStatus；处理运行/记录错误，不把 Wait 当落盘确认。
- [ ] 文档解释记录 CAS 冲突、无边界、基础 store 不支持、崩溃窗口；用户应如何检查 WaitRecorded 错误，不示范读取新 head 后盲目重写旧终态。
- [ ] 解释 Pool 停止与 Handle.Cancel 区别，未消费任务的等待边界，30 秒记录期限与整次运行期限区别；保持不做列表。
- [ ] 若 README 仍将 bus 描述为内置 Redis 后端，核对后将跨进程能力指向 queue/redis_pubsub.go 与 queue/redis_stream.go，不改无关实现。
- [ ] TestRunStatusAcrossProcess：子进程完成后台任务并确认 WaitRecorded 后退出，父进程新开 File store 读取 completed 和文件；真正使用子进程，不以新 Agent 实例冒充跨进程。
- [ ] 执行以下最终命令，记录实际输出与环境限制：

    go build ./...
    go vet ./...
    go test ./... -count=1
    go test -race ./queue ./agent ./checkpoint -count=1

- [ ] 核对 V1–V9 每项已有对应通过证据；回填 ADR 实施结果。偏离已决契约时明确记录原因与追加决议，不默默改写验收目标。

**验收：** V9 及全部前置卡；不以“待运行”标记为通过。

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

公开契约为 checkpoint.Completion、Agent.RecordCompletion(ctx, run)、Run.Paused、Handle.WaitRecorded、Agent.RunStatus 和 RunStatus。代码实现后才可称为已提供；不再使用旧 threadID/res/err 签名或普通 Save 方案。

本次文档修订已对照 agent/run.go、agent/durable_runtime.go、agent/write_guard.go、checkpoint/durable.go、checkpoint/snapshot.go、checkpoint/file.go、queue/agent_bridge.go、queue/worker_pool.go、middleware/runbudget.go 与 agent/options.go 的相关符号。代码测试是未来实施验收，本次未运行；任务卡未勾选。最终测试数量由实际实现记录，不沿用旧稿固定的“18 个用例”数字。
