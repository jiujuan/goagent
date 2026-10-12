# ADR-0033: 后台运行的 context 归属与线程终态查询

- 状态: Accepted
- 日期: 2026-10-12
- 关联: ADR-0024（durable run 与恢复）、ADR-0026（状态文件外置）、ADR-0028（thread id 文件名安全）、ADR-0029（工具超时与 abandon）、ADR-0032（快照不可变性与线程写入协调）、`queue/{agent_bridge,queue,worker_pool,redis_stream}.go`、`agent/{agent,run,hitl,loop}.go`、`checkpoint/{checkpoint,snapshot,branch,durable,file,memory}.go`、`bus/bus.go`
- 动因: `docs/analysis/goagent-genkit-go-v2-comparison.md` 的 GA-03。本 ADR 是该项的**收缩版**：最初按 Genkit 的 `WithoutCancel` + detach 形态起草了公开 `WithDetached()` 选项与事件重放缓冲，经用户复核后判定范围过大，改为只修队列的 context 归属 + 补终态可查询性。
- 本版取代: `ADR-0033-detach-run-continuation.md`（同名编号、旧范围，已删除）

## 背景

以下五条全部是本轮实测结论，行号可复核。

1. **"断线就报废整跑"不是 agent 层的缺陷，而是队列层的 context 归属错误。** `queue.EnqueueAgent` 用**调用方传入的 ctx** 建立 run（`queue/agent_bridge.go:35-36`），任务闭包忽略 worker 交给它的 ctx 直接 `run.Wait()`（`:40-43`），而 `Pool.exec` 优先走 `job.Run`（`queue/worker_pool.go:63-65`）。后果：提交方取消 ctx，就会中止一个"已经交给 worker 的后台运行"——这与该类型自己的注释"The worker drives the run"（`queue/agent_bridge.go:11-14`）矛盾。
2. **靠 checkpoint + Resume 续跑这条路已经通了。** `Agent.Resume(ctx, threadID)`（`agent/hitl.go:65`）并不要求暂停点：`validatePause` 在 `cp.Pause == nil` 时直接返回 nil（`agent/hitl.go:177-179`）；失败路径会写一个可重放的接缝，其语义就是 `RecoveryReplayModel`——"从保存下来的模型请求边界重启"（`checkpoint/checkpoint.go:90-95`）。所以"客户端取消 → 重新发问 → 接上最后状态"今天就做得到，**不需要新的 detach 语义**。
3. **落盘的不是聊天事件，且没有默认目录。** 默认 checkpointer 是内存（`agent/agent.go:48`），文件 store 必须显式 `checkpoint.NewFile(dir)` 并由调用方给目录；`$HOME/.goagent` 只是配置文件搜索路径（`config/config.go:134`）与 workspace 放 rules/skills 的约定目录，**不存在 `~/.goagent/threads` 这种默认事件目录**。`bus` 包注释也写明它只管观察、durability 归 Checkpointer（`bus/bus.go:3`）。因此"重连后补回中段事件流"无法靠 checkpoint 实现，只能靠重新订阅或自行落盘。
4. **取消确实有两处不可逆损失，这是"重发即 Resume"救不回来的部分。** 取消正好卡在工具中途时，该工具属于"已开始但结果未原子提交"，恢复路径必须拒绝自动重放（`checkpoint/durable.go:49-52` 的 `ErrToolOutcomeUnknown`，由 `agent/exectools.go:350` 消费）；取消卡在模型流式返回中途时，那一轮的 tokens 已计费却要整轮重放（`RecoveryReplayModel` 定义）。两者都不是"再来一次就行"，而是**要么别中止、要么由人决定**。
5. **没有任何地方能回答"这个线程现在跑到哪了"。** checkpoint 只有 `Pending`/`Pause` 表达暂停（`checkpoint/checkpoint.go:34-42`），没有"已完成/已失败"记录，客户端无从判断该 `Resume`、该新起一次、还是该等。

## 决策

编号供执行卡引用。范围仍限于队列 context 归属与可查询终态，不引入公开 detach、事件重放或跨进程 agent job。2026-10-12 审核修订：以下契约取代原 D4 的普通 Save 方案，配套计划必须同步，不将公开接口和并发规则留到实施时决定。

### D1. 后台运行的 ctx 由队列拥有，不由提交方拥有


a.Stream 的执行 context 使用以下派生关系：

    workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
    run := a.Stream(workCtx, input, opts...)
    q.Enqueue(ctx, job)

- 调用方 ctx 仅控制入队；已入队任务不因提交方断连或 deadline 到期而停止。WithoutCancel 保留 context 值。
- EnqueueAgent 入口先检查 ctx.Err()，已取消时直接失败；入队过程中取消与队列接收并发发生时，以 Enqueue 的实际返回值为准，成功即转交后台执行，不能承诺取消一定胜出。
- 不新增 WithDetached()。队列仍只支持携带 Run 闭包的进程内 agent job。

### D2. 取消执行与终态持久化分别收尾

- Handle.Cancel() 同时调用 run.Cancel() 与 cancelWork()。run.Cancel() 只取消其子 context，不会向上取消 workCtx；不得把两者等同。
- 入队失败、Handle.Cancel()、任务闭包退出三条路径均幂等释放 cancelWork。闭包使用 defer 保证错误路径也释放。派生 context 不等于独立 goroutine，不宣称每个未取消 context 都永久泄漏；被队列或 Handle 引用时会保留相应资源。
- 执行取消不得取消终态记录操作。闭包在运行结束后，使用从 workCtx 经 WithoutCancel 派生的独立 context 记录终态，内部 WithTimeout 固定为 30 秒并 defer cancel；这是记录 I/O 的合作式截止时间，不是整次运行的期限，也不能强制打断忽略 context 的 store。
- Pool 的 context 取消会停止取新任务，但不会取消此类已开始运行；Pool 会等待在途任务。未被 worker 取走的任务不会自动写终态，丢弃 Handle 也不会取消队列持有的任务。文档必须说明这项生命周期边界。

### D3. 复用既有治理，但不承诺整次运行的硬期限

| 机制 | 实际保证与边界 |
| --- | --- |
| maxTurns（默认 16） | 限制模型循环轮数，不能中断当前阻塞调用，也不代表所有自定义 Runnable 的轮数上限 |
| middleware.RunBudget | 在钩子处检查预算并进入正常收尾模式；MaxDuration 不会取消正在运行的工具，超限通常得到 completed，不是预算错误 failed |
| provider HTTP timeout | 内置 provider 的默认单次请求超时为 5 分钟；可配置或替换，不是整次任务的期限 |
| agent.WithToolTimeout | 默认 0，表示无限制；配置有限值后结合 ADR-0029 的 abandon 宽限限制框架等待工具的时间，不能杀死忽略取消的工具 goroutine |

后台 LLM 工具任务的示例与使用要求必须同时展示 RunBudget 和非零 WithToolTimeout。删除“最坏 maxTurns × 5 分钟”与“挂预算即保证有限终止”的承诺：工具批次数、重试、自定义 provider/Runnable/middleware、文件与存储 I/O 等仍需各自治理。本 ADR 不新增整次运行期限；D2 的 30 秒只用于终态写入。取消后遗留的工具 goroutine 和外部副作用仍遵守 ADR-0029。

### D4. 终态绑定具体 Run 和其已提交边界，以 CAS 追加

Checkpoint 增加可选 Completion 字段（JSON completion,omitempty），旧记录读取为 nil：

    type Completion struct {
        RunID  string       `json:"run_id"`
        Kind   string       `json:"kind"`    // completed | failed，两个导出常量
        Result *core.Result `json:"result,omitempty"`
        Err    string       `json:"err,omitempty"`
    }

公开 API 固定为：

    func (a *Agent) RecordCompletion(ctx context.Context, run *Run) error

该方法不启动、不等待运行；仅接受 a 自己创建且已结束的 Run。nil、其他 Agent 的 Run、尚未结束、暂停状态均返回明确错误且不写入。以该 Run 内部冻结的结果、错误和 ID 生成 Completion，调用方不能另传 threadID/runID/res/err 拼装归属。完成与暂停必须由 drive 的实际 outcome 显式保存，不能猜测“空 Result + nil error”，也不能靠重新读取 Latest 判断本次运行是否暂停。

**结束凭据与写入算法：**

1. 在该运行每次成功的 durable commit 后，保存其 checkpoint ID 与 CommitReceipt.Revision；失败提交不得更新。drive 在释放 claim 之前冻结本次最后成功提交的边界，结束时一并冻结 outcome/result/error，且必须在发布任何终态事件之前完成，以免 Iter/capture 先关闭 done 使 Wait 提前返回。所有正常、暂停、准备失败和执行失败路径遵守同一顺序。不得在 Wait 后调用 Latest 来生成预期 revision，不得把运行开始时继承的旧 head 当成本次成功提交。没有本次成功提交（包括准备失败、首步提交失败、自定义 Runnable 未写 checkpoint）时，RecordCompletion 返回明确的“无可记录边界”错误，不伪造首条记录，也不改写前次运行的终态。
2. 仅支持 checkpoint.Durable.Commit 的原子 ExpectedRevision 能力；只有基础 Checkpointer 的自定义 store 返回 ErrDurableUnavailable，不回退为 Save。前台执行的既有基础 store 支持不变。
3. 用冻结的 checkpoint ID 调用 Load，核对 ThreadID、ID 与 Revision，并拒绝该边界含 Pause 或 Pending 的情况。克隆其 State 和 FileSnapshot；后者是独立字段，不能仅克隆 State.Files。读取失败或克隆失败直接返回，不写入。
4. 构造 append-only 记录：ParentID 指向冻结边界 ID、Step 沿用该边界、State/FileSnapshot 保留其完整快照、Pending/Pause 为 nil，Completion 取本次 Run 的冻结结果。为每个 Run 固定一个终态 checkpoint ID，并缓存深拷贝后的完整记录；构造与提交同步保护，同一 Run 的重复调用复用完全相同的内容，避免追加重复终态。
5. 释放 claim 后通过 Durable.Commit 提交，ExpectedRevision 为冻结边界的 revision，不携带旧 claim、不夹带 inbox ack 或工具执行变更。已有同 ID 同内容记录按 store 的幂等提交契约返回成功；否则活跃 owner 返回 ErrClaimRequired，head 已变化返回 ErrRevisionConflict。错误保留 errors.Is 可识别性，不改成读取新 head 后重试，不绕过 claim。

这样 A 结束、B 运行完成、A 才记录终态时，A 的 CAS 必须失败；若 A 已成功记录、随后 B 前进，A 的同记录重试只确认旧提交，不改变 B 的 head。进程在运行完成与终态提交之间退出仍可能没有终态，这是显式记录设计的崩溃窗口，不承诺两者原子完成。

freezeCheckpoint/cloneCheckpoint 必须深拷贝 Completion，Result.Message 使用 core.CloneMessage；FileSnapshot 保留 nil 与非 nil 空快照区别。Fork 清除 Completion，但保留原有文件快照复制语义。默认前台路径不自动调用 RecordCompletion，历史条数不变。

### D5. 等待与查询分别表达执行结果、记录结果和存储事实

队列提供两种等待：

    func (h *Handle) Wait() (core.Result, error)
    func (h *Handle) WaitRecorded() (core.Result, error)

- Wait 保持现有行为：等待底层 run，且可能触发惰性执行；成功不意味着终态已经落盘。Events 仍只观察运行事件，不暗示记录已成功。
- WaitRecorded 等待 worker 的收尾完成，不自行启动运行或绕过 worker 写记录。返回原运行结果与 errors.Join(runErr, recordErr)；暂停正常返回运行结果，不写 Completion。多个等待者得到同一冻结结果。调用方必须启动/保持 worker；任务未被消费时该方法会等待，不承诺 Cancel 会让未消费任务完成持久化。
- worker 闭包按 Run 内部保存的暂停 outcome 跳过 RecordCompletion，其余成功/失败均尝试记录。队列通过 Run 的只读 Paused() bool 方法获取该 outcome（仅 Wait 返回后有判定意义），不订阅事件来推断。RecordCompletion 自身仍独立拒绝暂停。
- 闭包先保存收尾结果并关闭专用 done，再返回同一个合并错误给 Pool。记录失败不能仅靠 job.Run 返回值上报：当前 MemQueue/Pool 没有此错误的对外通道，WaitRecorded 才是持有 Handle 的调用方检查持久化结果的入口。无人等待且无应用监控时不保证主动通知。
- 对 completed 运行，WaitRecorded 返回 nil 错误说明终态提交已确认；对 failed 运行仍返回运行错误，记录错误通过 errors.Is/errors.As 检查。随后 RunStatus 看到的是当时最新 head，可能已有更新的运行，不能保证永远仍为这次 Completion。

查询接口：

    func (a *Agent) RunStatus(ctx context.Context, threadID string) (RunStatus, error)
    type RunStatus struct {
        ThreadID   string
        Exists     bool
        Paused     bool
        Revision   uint64
        Step       int
        Completion *checkpoint.Completion
        Latest     *checkpoint.Checkpoint
    }

先做 core.CheckThreadID 校验，再读 Latest；store 错误原样返回。Latest 为 nil 表示不存在；Completion 非 nil 表示该快照记录了 completed/failed；否则 Pause/Pending 存在表示暂停元数据，否则表示没有终态记录。Paused 不是“必然可以成功 Resume”的承诺，实际恢复还受审批、claim、恢复类型和工具结果未知等约束。返回值遵守深拷贝边界。

不报告 Running；没有新快照时，旧 Completion 不能证明当前没有另一运行。查询只读、不取 claim、不续租；不将进程内事实伪装为跨进程状态。

### D6. 明确不做

公开 WithDetached()/WithRunReplayBuffer()、事件缓冲重放、Run.Dropped()、core 新事件类型、进程内 detached run 登记表、跨进程可序列化 agent job、终态历史列表与分页、前台终态自动写入、整次运行硬 deadline、进程崩溃后自动补写终态。

## 结果与取舍

- 后台执行不再随提交方 ctx 取消；显式 Handle.Cancel 停止执行，终态写入使用独立的有截止时间 context。
- 可跨进程读取已经提交的终态，并通过 CAS 避免旧结果污染新 head；保留完整文件快照。
- 新增 RecordCompletion、Run.Paused、Handle.WaitRecorded 与 RunStatus 契约；Run 的内部结束凭据增加实现工作，但前台不新增 checkpoint。
- 完成与记录不是原子操作：冲突、存储失败、无本次提交边界和进程退出都可能使终态缺失。需要确认的调用方使用 WaitRecorded，并处理错误；基础 Checkpointer 不具备此写入能力。
- 不提供整次运行硬期限、运行中查询或事件补送。需事件留档时可使用 bus 的 Lossless 订阅者自行落盘，并自行承担订阅者的背压与生命周期管理。

## 备选方案与否决理由

| 方案 | 否决理由 |
| --- | --- |
| agent 层公开 WithDetached | 当前需求可在队列 context 归属内解决，不扩展全框架运行选项 |
| 断线后总是重新 Resume | 工具结果未知及模型请求重放成本无法靠自动重试消除 |
| 所有前台终态自动写 Completion | 改变默认历史条数与存储行为，超出本批范围 |
| 释放 claim 后读取 Latest，再普通 Save | 无法绑定结束运行，旧结果可污染新 head；ParentID 不是 CAS |
| 让调用方分别传入 threadID/runID/result/revision | 可错配身份与结果；Run 已拥有结果和提交边界，应由它提供冻结凭据 |
| 改 Wait 为等待持久化 | 改变现有执行结果语义；使用明确的 WaitRecorded 暴露记录结果 |
| RunStatus 推断 Running | 当前存储模型不能可靠表达；旧终态也不代表没有新运行 |
| 跨进程 agent job、事件重放 | 分别需要独立的序列化/恢复和流订阅契约，继续排除 |

## 验证

编号供执行计划逐项映射，以下均为待实施验收，不是已通过结果。

1. V1 context：提交后取消仍完成；入口已取消失败；入队失败、显式 Cancel、闭包退出均释放派生 context；检查 Done，不以 goroutine 数量推导 context 数量。
2. V2 治理：RunBudget 超限正常收尾；有限 ToolTimeout 的超时/abandon；未配置工具超时时预算不能中断在途工具（用测试闸门释放，禁止泄漏测试进程）。
3. V3 数据：Memory/File 的 Completion 冻结、Result 深拷贝、旧 JSONL 兼容、Fork 清除终态；保留 nil/空 FileSnapshot 语义。
4. V4 身份：拒绝 nil、其他 Agent、未结束、暂停、无本次提交边界、基础 store；成功/失败 Run 的 RunID 与结果严格一致，失败提交不更新边界。
5. V5 并发：A 结束→B 完成→A 记录冲突；A Load 后→B 提交→A Commit 冲突；活跃 claim 冲突；同 Run 并发/重复记录不增加历史，B 前进后重试 A 不改变 head。对 Memory/File 均运行。
6. V6 文件：写文件→终态记录→重开 File store→读取/新运行/Fork，内容不变；覆盖空文件视图，且不能只检查历史旧记录。
7. V7 可见性：阻塞终态 Commit 时 Wait 可返回、WaitRecorded 不返回；注入写入失败验证运行结果保留、记录错误可识别；取消后仍尝试记录；暂停跳过；多个等待者结果一致；记录 ctx 有截止时间且释放。
8. V8 查询与兼容：不存在、无终态、暂停、completed、failed；非法 ID 和 store 错误；读取副本不可污染历史；前台历史条数不变。
9. V9 跨进程与回归：子进程等 WaitRecorded 后退出，父进程重开 File 读取终态与文件；go build ./...、go vet ./...、go test ./... -count=1、go test -race ./queue ./agent ./checkpoint -count=1。

## 实施结果

由 BC-06 回填实际命令、结果与偏离。2026-10-12：D1/D2 的队列 context 归属与释放已由 BC-01 落地（`queue/agent_bridge.go`，V1 由 `queue/agent_bridge_test.go` 覆盖并通过，含 `-race ./queue`）；D4 的数据形状由 BC-02 落地（`checkpoint.Completion` 与 `CompletionCompleted`/`CompletionFailed`、`Checkpoint.completion,omitempty` 的 Memory/File 往返、深拷贝与 Fork 清除，V3 由 `checkpoint/completion_contract_test.go` 覆盖并通过）。BC-02 复核发现 `Completion` 未进入提交摘要，同 ID 不同终态会被误判为幂等重试，已修复：摘要现在覆盖 RunID/Kind/Result/Err，且 nil 终态的摘要与修复前一致（旧日志重试不受影响），冲突与幂等由 `TestCompletionDigestDistinguishesTerminal`、`TestCompletionDigestKeepsNilBaseline` 覆盖。D4 的写入路径由 BC-03 落地：`RunContext` 在每次成功的 durable commit 后缓存 checkpoint ID 与回执 revision（并行分支共用这份状态，所以缓存按 revision 单调前进，ID 与 revision 成对更新，迟到的旧回执不会把边界拽回去），`drive` 在释放 claim 前冻结边界、在发布终态事件前冻结 outcome/result/error，新增 `Run.Paused` 与 `Agent.RecordCompletion`（Load 校验边界、深拷贝 State 与 FileSnapshot、固定终态 ID 与缓存、`ExpectedRevision` 追加，拒绝 nil/他人/未结束/暂停/无边界/基础 store）。V4、V5、V6 已由 `agent/terminal_test.go`、`agent/terminal_cas_test.go`、`agent/terminal_files_test.go` 覆盖并通过（memory/file 双后端，含 `-race ./agent ./checkpoint ./queue`）。BC-03 复核发现结束边界会被迟到的旧回执覆盖：并行分支共用同一个 `durableRunState`，而边界写在提交锁之外，正常完成的并行运行因此可能拿着较低 revision 记录终态并被自己的 CAS 拒绝。已修复为锁内按 revision 单调更新并成对替换 ID/revision；`agent/terminal_boundary_internal_test.go` 覆盖推进规则、机制层与真实 `Parallel` 工作流的迟到回执（V4、V5 复跑通过）。

实施期澄清三条（不改设计意图，但影响阅读 D4 的方式）：其一，Durable 能力检查必须在"有本次提交边界"之前——只有基础 Checkpointer 的存储走 `Store.Save` 分支、不产生回执，因此永远没有边界，按原顺序 `ErrDurableUnavailable` 不可达。其二，模型或工具故障的运行通常仍有自己的边界：`AgentLoop.fail` 会写一个不含 Pause/Pending 的可重放接缝，所以失败运行按 Kind=failed 记录；"无边界"实际出现在准备失败（BeginRun 被拒、线程 head 只是继承来的）和首步提交就失败的情况。其三，D4 第 1 条的"最后成功提交的边界"在并行分支下必须读成"本次运行提交过的最高 revision"：分支共用同一份 durable 状态，边界写在提交锁之外，回执到达顺序可以晚于提交顺序，所以更新是单调的——更低或相同的 revision 不动它，ID 与 revision 成对替换。

`Handle.WaitRecorded`、`Agent.RunStatus` 仍未实施，V2、V7–V9 尚未执行。
