# Background Run Context Ownership and Thread Terminal Status Implementation Plan

> 执行者说明：按 executing-plans 技能串行执行 BC-01 至 BC-06，先写对应失败测试，再做最小实现，检查通过后勾选。本文接口与测试均为待实施设计，不能描述为当前已有能力。使用符号名定位代码，不依赖旧稿行号。

- 日期：2026-10-12；状态：BC-01、BC-02 已完成（见各卡执行记录），BC-03 至 BC-06 未开工。
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

**本卡的两点登记：**

1. `cloneCompletion` 的 CloneMessage 错误分支没有专门用例：`core.Part` 带非导出标记方法 `isPart()`，包外测试无法构造"不可克隆的 part"，而 `core` 的 6 个导出 part（Text/Thinking/Image/Video/ToolCall/ToolResult）都可克隆。代码仍按契约返回错误，与 `cloneToolExecution` 同一条 `core.CloneMessage` 路径；不为此在 `core` 开测试后门。
2. 交给 BC-03 决策：提交摘要 `commitRequestDigest` 覆盖 ID/ThreadID/ParentID/Step/State/Pending/Pause/FileSnapshot，不含 Completion（`checkpoint/coordinator.go`）。两个 ID 相同、仅终态内容不同的提交因此摘要一致，会被 store 当作幂等重复而返回成功并保留先写入的内容。ADR D4 只要求"同 ID 同内容"幂等成功，调用方缓存不可变记录也保证了重试内容一致，但如果希望"同 ID 不同终态"能报冲突，需要在 BC-03 把 Completion 纳入摘要。本卡未动摘要，避免与并发契约混在一起。

### BC-03：结束凭据与 RecordCompletion

**状态：** 待实施。**文件：** 修改 agent/run.go、agent/durable_runtime.go；新增 agent/terminal.go、agent/terminal_test.go；根据现有测试包约定放置必要内部凭据测试。复用 agent/write_guard.go 的 commit 回执，不改变其 claim 续租与释放规则。

- [ ] 先写 TestRecordCompletionUsesRunIdentity：接口按锁定签名，完成/失败结果和 RunID 一致；Wait 返回结果被修改后不污染冻结终态。
- [ ] 在成功 commit 后保存 ID/revision，drive 释放 claim 前冻结边界，发布终态事件之前冻结 outcome/result/error。补 Run.Paused；TestRunPausedOutcome 覆盖暂停、空结果正常完成、失败，不用结果空值区分。
- [ ] TestRunTerminalBoundaryBeforeEvents：并发 Iter/Events 与 Wait，终态事件可见或 Wait 返回时凭据与 Paused 已完整；覆盖成功、暂停、准备失败与执行失败。
- [ ] TestRecordCompletionRejectsInvalidRun：nil、其他 Agent、未结束、暂停均拒绝且历史不变。
- [ ] TestRecordCompletionRequiresOwnBoundary：准备失败、首步失败、仅继承旧 head、失败提交不更新凭据；不覆盖前次 Completion，不伪造根记录。
- [ ] TestRecordCompletionRequiresDurable：基础 Checkpointer stub 返回 ErrDurableUnavailable，不调用 Save；前台仍可使用该 stub。
- [ ] 实现固定 ID 的不可变记录缓存和并发保护，Load 冻结边界、深拷贝 State/FileSnapshot，检查 Pause/Pending 与 identity，使用 ExpectedRevision Commit。
- [ ] TestRecordCompletionStaleRunRejected：用通道闸门确定 A 结束→B 完成→A 记录，断言 ErrRevisionConflict 且 B head/历史不变；Memory/File 均覆盖。
- [ ] TestRecordCompletionCASAfterLoad：阻塞 A 的 Commit，在其 Load 后允许 B 提交，恢复 A 后仍拒绝；不是仅在 Load 时检查 revision。
- [ ] TestRecordCompletionClaimConflict：边界不变但其他 owner 持有活跃 claim，返回 ErrClaimRequired，不携旧 claim、不重读新 head。
- [ ] TestRecordCompletionIdempotent：并发/重复调用只追加一次；模拟提交成功但回执失败再重试；A 已记录、B 前进后 A 重试不改变 B head。Memory/File 均覆盖。
- [ ] TestRecordCompletionPreservesFiles：写文件→记录终态→重开 File→新运行或 Fork 恢复文件，覆盖空快照和历史副本修改；不能只检查旧 head。
- [ ] 运行 go test ./agent -run 'TestRecordCompletion|TestRunPaused' -count=1；go test -race ./agent ./checkpoint -count=1；记录结果。

**验收：** V4、V5、V6；接口和并发规则无需留给后续卡片决定。

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
