# P1 会话与任务管理实施计划

> 执行方式：使用 executing-plans 技能逐任务实施。每张任务卡完成测试、复核后独立提交；除非用户明确授权，不启动并行子代理。SM-01…SM-14 已逐项实施并验收，勾选状态与证据见 §7。

**目标：**落实分析文档“第二批：补齐会话和任务管理接口”的 P1-A/B/C，提供 Session 门面、可重连任务记录和轻量分页查询。

**架构：**沿用 checkpoint 的不可变快照、Durable CAS 与线程 owner；agent 承担 Session 编排，独立 runstore 管理可变任务记录，queue 提供持久化任务协议。摘要索引可重建，不能替代 journal。

**技术栈：**Go 1.25，标准库、现有 checkpoint Memory/File、queue Registry/Redis Streams、mock LLM；本计划不引入数据库依赖。

- 状态：待实施。
- 日期：2026-10-12。
- 基线：b28018c（实施前执行 git status 和 git log 确认）。
- 设计依据：[ADR-0034](../adr/ADR-0034-session-task-management.md)。
- 分析输入：[goagent-genkit-session-snapshot-review.md](../analysis/goagent-genkit-session-snapshot-review.md)。
- 工作目录：D:/codeproject/mygo/goagent；下文所有代码路径均相对此根目录。
- 保留当前未跟踪分析文档；不得使用 git add . 混入其他工作。本文与 ADR 的提交须由用户另行安排。

## 1. 已有能力与实施边界

SC-01–12 已完成；随后 ADR-0033 已落实独立后台 context、RecordCompletion、WaitRecorded、RunStatus 与跨进程查询。writeGuard 已有独立续租。本计划扩展这些能力，不重新实现一套后台生命周期，不把 RunStatus 当 GetRun，不把可重连查询当自动恢复执行。

P1-A：SM-01–04、SM-11 的 Session.History、SM-14。
P1-B：SM-05–09、SM-12–14；续租配置在 SM-09，复用已有 guard。
P1-C：SM-10–13、SM-14。多租户 namespace 属条件项，明确延后；本批只支持上层可信选择独立 store 范围。

## 2. 交付顺序与依赖

```text
SM-01 → SM-02 → SM-03 → SM-04
SM-01 → SM-05 → SM-06 → SM-07 → SM-08
SM-02 + SM-07 → SM-09
SM-02 → SM-10 → SM-11
SM-06 + SM-08 + SM-11 → SM-12 → SM-13
SM-04 + SM-09 + SM-13 → SM-14
```

按编号顺序执行即可。SM-04 的 History 暂不公开，等 SM-11 能力具备再发布，避免对外提供只会返回未实现错误的方法。每项依赖完成后才能启动；不跨任务提交未完成占位函数。

| 任务 | 可独立审阅的交付 | 建议规模 |
| --- | --- | --- |
| SM-01 | 标识、谱系字段与兼容契约 | 0.5–1 天 |
| SM-02 | 所有提交路径的谱系与身份 | 1–2 天 |
| SM-03 | 冻结结构化结果 | 0.5–1 天 |
| SM-04 | Session 发送、读取与原子 Fork | 1 天 |
| SM-05 | RunRecord 协议和 Memory 状态机 | 1–2 天 |
| SM-06 | File RunStore、CAS 与恢复 | 1–2 天 |
| SM-07 | 运行身份注入与 worker 生命周期 | 1–2 天 |
| SM-08 | 持久任务协议、重连查询和取消 | 1–2 天 |
| SM-09 | 可配置续租与存活信号隔离 | 0.5–1 天 |
| SM-10 | Checkpoint 摘要和 Memory 查询 | 1 天 |
| SM-11 | File 摘要投影与分页 | 1–2 天 |
| SM-12 | 任务列表、显式修复和订阅 | 1–2 天 |
| SM-13 | 跨进程与故障契约 | 1 天 |
| SM-14 | 示例、文档与全量验收 | 0.5–1 天 |

规模是执行安排参考，不是时限承诺。每张卡内部按下面的“失败测试→最小实现→验证→提交”操作。

## 3. 公共契约清单

以下是待新增 API，不是当前已有接口。SM-01/05/10 实施时按此定稿并在包注释补齐错误语义。

### 3.1 Session 与结果

```go
// agent/result.go
// Ref=nil 表示该运行没有成功提交，不能使用继承 head 冒充。
type RunResult struct {
    ThreadID string
    RunID string
    TurnID string
    Result core.Result
    CheckpointRef *checkpoint.Ref
    Revision uint64
    Paused bool
}
```

Session 方法见 ADR D3。Session 持有私有 Agent 与 threadID，暴露 ThreadID() string；Send 的 RunOption 最终不能改变绑定线程。GetHead 没有记录返回 nil,nil。

### 3.2 任务存储

```go
// runstore/store.go；RunRecord 的完整字段见 ADR D4。
type Store interface {
    Create(context.Context, RunRecord) (RunRecord, error)
    Get(context.Context, string) (RunRecord, error)
    CompareAndSwap(context.Context, uint64, RunRecord) (RunRecord, error)
}
// expectedVersion=0 只用于 Create 的内部实现；CAS 不创建缺失记录。
// Version 由 store 递增，禁止调用方跳跃版本。
```

RunRecord 使用值字段和可选 Ref/Error/Result 指针，所有出口深拷贝。错误：ErrNotFound、ErrConflict、ErrInvalidTransition、ErrOwnershipLost。Cas 必须校验不可变身份、状态转换与终态冻结，不仅比较版本。

```go
// queue/run_client.go
func NewRunClient(store runstore.Store) *RunClient
func (c *RunClient) GetRun(ctx context.Context, runID string) (runstore.RunRecord, error)
func (c *RunClient) WaitRun(ctx context.Context, runID string) (runstore.RunRecord, error)
func (c *RunClient) CancelRun(ctx context.Context, runID string) (runstore.RunRecord, error)
```

GetRun 只读，WaitRun 只轮询，不调用 Run.Wait、不领取任务。业务 failed/cancelled/paused 通过返回记录表达；error 表示查询失败或等待 context 结束。CancelRun CAS 冲突时重读并有界重试（最多 8 次，检查 ctx），耗尽返回 ErrConflict。

### 3.3 摘要与列表

```go
// checkpoint/query.go
type MetadataReader interface {
    LoadMetadata(context.Context, Ref) (*CheckpointMetadata, error)
    HeadMetadata(context.Context, string) (*CheckpointMetadata, error)
}
type Query interface {
    ListCheckpoints(context.Context, ListOptions) (MetadataPage, error)
    ListSessions(context.Context, SessionListOptions) (SessionPage, error)
}
```

页面包含 Items 和 NextCursor；末页 NextCursor=""。具体字段、过滤与排序见 ADR D7。查询不支持则 ErrCapabilityUnavailable；非法 limit/filter/cursor 明确返回错误。负 limit 拒绝，0 使用默认，超过 200 拒绝。

Watcher 使用 Subscribe(ctx, filter) (<-chan Notice, error)，Notice 仅包含对象 ID 与版本。Checkpoint 与 Run 定义各自 filter/notice，避免跨包循环依赖。首个通知触发读取当前状态；非 durable 订阅，不承诺所有中间事件必达。

## 4. 任务卡

### TASK-SM-01：固定谱系元数据与旧记录兼容

**依赖：**无。**决策：**D2。

**文件：**修改 checkpoint/checkpoint.go、snapshot.go、coordinator.go、branch.go；新增 checkpoint/metadata_contract_test.go、checkpoint/testdata/legacy_digest_v1.jsonl。

- [x] 从当前版本生成并保存已 Commit 的旧格式 journal fixture，包含已有 RequestDigest；固定原请求数据，禁止测试运行时用新摘要生成“旧格式”。
- [x] 写 TestMetadataRoundTrip、TestMetadataCloneIsolation、TestLegacyDigestRetry、TestParentRefValidation。覆盖 nil、新字段、同 ID 异元数据、ParentID 冲突、旧跨线程来源未知。
- [x] 先运行测试观察缺失字段/API 导致失败；添加 Ref 与四个可选字段，更新 clone/freeze、摘要和 Fork。新 Fork 清 RunID/TurnID，创建时间重新分配且重试稳定。
- [x] 旧字段全部缺省时保留旧 digest 编码；新增 ParentRef 校验不拒绝历史上只有 ParentID 的记录。Load 不重写旧文件。
- [x] 执行 `go test ./checkpoint -run 'Test(Metadata|LegacyDigest|ParentRef)' -count=1`，预期全部通过。
- [x] 复核新字段全部进入幂等性约束；提交 `feat(checkpoint): add lineage and run metadata`。

**完成标准：**旧 journal 可读且原请求幂等重试成功；调用方修改 ParentRef 不污染存储；新冲突可识别。

### TASK-SM-02：贯通普通、计划、并行和恢复提交谱系

**依赖：**SM-01。**决策：**D2。

**文件：**修改 agent/runtime.go、durable_runtime.go、run_prepare.go、write_guard.go、hitl.go、terminal.go；按实际构造点检查 agent/loop.go、plan.go；新增 agent/lineage_test.go。

- [x] 写 TestLineageNormal/Plan/Parallel/Resume/Terminal。三次提交必须组成完整同线程链；暂停恢复新 RunID、同 TurnID；Completion 指向本次成功边界。
- [x] 写 TestLineageCommitFailureDoesNotAdvance：注入第二次提交失败，后续 parent 仍指向第一次成功提交；用 barrier 检验并行提交先后。
- [x] 在运行真正启动时初始化继承 head 和 TurnID；新增共享提交互斥区，锁内组装元数据并调用原提交路径，成功才推进 head。边界凭据只记录本次提交。
- [x] 明确锁顺序为谱系提交锁→现有 writeGuard 提交锁→store，续租不获取谱系锁，避免死锁；相同提交重试复用 ID/时间。
- [x] 基础 store 的 Save 也填谱系，但不新增跨实例一致性承诺。RecordCompletion 纳入新字段，不读新 head 重定位旧终态。
- [x] 执行 `go test ./agent ./checkpoint -run 'Test(Lineage|Terminal|Thread)' -count=1` 和 `go test -race ./agent ./checkpoint -count=1`。
- [x] 提交 `feat(agent): track checkpoint lineage across runs`。

**完成标准：**所有内置执行路径都有可定位谱系；基础第三方 store 仍可编译运行；失败提交不推进任何本地 head。

### TASK-SM-03：提供冻结结构化运行结果

**依赖：**SM-02。**决策：**D3。

**文件：**新增 agent/result.go、result_test.go；修改 agent/run.go、agent.go、terminal.go。

- [x] 写 TestRunDetailedBoundaryStable：A 结束后 B 推进 head，再读 A 结果仍是 A；TestRunDetailedNoBoundary：准备失败返回 Ref=nil；TestRunDetailedIsolation 修改嵌套消息不污染下一次读取。
- [x] 扩展现有冻结 outcome 结构，冻结身份、TurnID、成功边界和暂停标志；不要在外部拼装可变 Run.ID/ThreadID 作为真实性凭据。
- [x] 实现 RunDetailed，驱动现有 Run 并读取冻结副本，保留错误返回。Agent.Run 的字符串返回和 checkpoint 条数不变。
- [x] 执行 `go test ./agent -run 'Test(RunDetailed|FrozenOutcome|Terminal)' -count=1`。
- [x] 提交 `feat(agent): expose structured run results`。

**完成标准：**成功、暂停、准备失败、部分提交后失败均携带准确身份；没有新增自动 Completion。

### TASK-SM-04：实现 Session 门面与原子 Fork

**依赖：**SM-03。**决策：**D3。

**文件：**新增 agent/session.go、session_test.go；复用 agent/agent.go 的还原逻辑和 checkpoint/branch.go。

- [x] 写 TestSessionOpenNoWrite、TestSessionSendThreadBound、TestSessionForkFiles、TestSessionForkTargetConflict、TestSessionForkOwnership。
- [x] 实现 OpenSession、ThreadID、Send、GetHead、ForkFrom；来源 Load 后检查 ID/ThreadID。Fork 只 Durable.Commit(ExpectedRevision=0)，不能 Latest 检查后 Save。
- [x] 两个 Session 同时 Fork 到同目标时用 barrier 保证最多一个成功；Memory/File 双后端验证非 nil 空文件快照和外部文件注入规则。
- [x] 基础 Checkpointer 的 Send/GetHead 保持可用；ForkFrom 返回 ErrDurableUnavailable。History 等 SM-11 接入。
- [x] 执行 `go test ./agent -run TestSession -count=1` 与 `go test -race ./agent -run TestSession -count=1`。
- [x] 提交 `feat(agent): add session facade and atomic fork`。

**完成标准：**失败 Fork 没有返回成功门面、不覆盖目标线程；发送时 head 在实际启动时读取。

### TASK-SM-05：实现 RunRecord 状态机与 Memory RunStore

**依赖：**SM-01。**决策：**D4、D5。

**文件：**新建 runstore/record.go、store.go、memory.go、clone.go、contract_test.go。

- [x] 用表驱动 TestRunTransitions 穷举 ADR D4 允许/拒绝的状态边；包括 queued 取消、终态重复写、身份不可变、取消后完成先胜出。
- [x] 写 TestRunStoreCASConflict、TestRunStoreCopyIsolation、TestRunStoreCreateIdempotent、TestRunWorkerEpoch。并发写同版本只能一方成功。
- [x] 实现前述 Store 接口；Version 从 1 开始，UpdatedAt 由存储时钟产生；创建时间与身份保持不变。时钟作为包内测试注入点。
- [x] 状态/错误码用导出常量；通用 CAS 必须调用统一校验器，禁止任意调用方跳过状态机。心跳不得清除取消请求，worker mutation 必须验证 epoch。
- [x] 执行 `go test ./runstore -count=1` 与 `go test -race ./runstore -count=1`。
- [x] 提交 `feat(runstore): add run records and memory storage`。

**完成标准：**所有字段存在、出口隔离、状态机验证在存储边界生效；与 checkpoint.Revision 明确分离。

### TASK-SM-06：实现 File RunStore 和崩溃恢复

**依赖：**SM-05。**决策：**D5。

**文件：**新增 runstore/file.go、file_lock.go、file_test.go、file_process_test.go、testdata/；参考 checkpoint/file.go、coordinator_file.go 的现有锁与恢复规则。

- [x] 让 Memory 契约套件可传 factory，File 复用同套件；写 TestRunFileReopen、TestRunFileCASAcrossProcesses、TestRunFileTornTail、TestRunFileSyncFailure。
- [x] 使用独立 runs 目录，每 ID 校验安全文件名，版本化 JSONL 事件保存完整记录；CAS 在跨进程锁内重读最新有效状态，append+Sync 后才发布缓存。
- [x] 尾部半条记录可恢复，中部损坏/未知格式版本拒绝；重复同事件不递增版本。测试注入 write/Sync 错误及锁竞争，不吞错误。
- [x] 读取 context 在等待锁时可取消；两个独立实例不可依赖同一进程 mutex 保证一致性。
- [x] 执行 `go test ./runstore -count=1` 与 `go test -race ./runstore -count=1`。
- [x] 提交 `feat(runstore): persist run records with file CAS`。

**完成标准：**进程退出后身份、取消请求与结果可重新加载；失败写入不发布成功。

### TASK-SM-07：绑定稳定运行身份并记录 worker 生命周期

**依赖：**SM-03、SM-06。**决策：**D4–D6。

**文件：**新增 agent/managed_run.go、managed_run_test.go、queue/run_worker.go、run_worker_test.go；修改 agent/run.go、agent.go；复用 terminal.go。

- [x] 新增供 queue 使用的受校验 ManagedRunSpec{RunID,ThreadID,TurnID} 和 Agent.StreamManaged 入口，创建时冻结身份；不得修改既有已创建 Run 的导出 ID 来冒充同任务。
- [x] 测试 TestManagedRunIdentity、TestManagedRunPreparationFailure、TestManagedRunPaused、TestManagedRunCompletionRecordError。
- [x] worker 先 CAS queued→running，获得 WorkerEpoch，再调用 StreamManaged。重复 running/终态记录不得再启动模型；cancelled 排队记录直接结束。
- [x] 用 FrozenOutcome/结构化结果收尾；暂停跳过 RecordCompletion。执行结果、RecordError 与 RunStore 写错误分别保留，允许无边界 failed 记录。
- [x] 终态写采用独立有界记录 context，保留可信 values，默认 30 秒；runstore 失败必须向 worker/调用者返回，不用成功事件掩盖。
- [x] 执行 `go test ./agent ./queue ./runstore -run 'TestManagedRun' -count=1`。
- [x] 提交 `feat(queue): persist managed run lifecycle`。

**完成标准：**RunRecord、运行和 checkpoint 的 RunID 一致；旧 EnqueueAgent/RecordCompletion 的行为不变。

### TASK-SM-08：持久化任务提交、重连等待与取消

**依赖：**SM-07。**决策：**D5。

**文件：**新增 queue/managed_agent.go、run_client.go、managed_agent_test.go、run_client_test.go；修改 queue 包文档，复用 Registry。

- [x] 定义 PayloadVersion=1、AgentKey、RunID、ThreadID、TurnID、Input/Message；AgentResolver 为 `func(context.Context, string) (*agent.Agent, error)`，由应用绑定文件后端。
- [x] 实现 EnqueueManagedAgent：创建 queued 后发送 Type/Payload，返回 RunID 即使投递失败；定义 Submission{RunID,JobID}，error 单独返回。不能序列化 RunOption 闭包。
- [x] TestManagedDispatchFailure 区分明确失败与不确定结果；对无法分类的 Queue.Enqueue 错误默认 dispatch_unknown，不武断写 failed。提供带相同 Submission 的 RetryDispatch，只重新投递，不新建 Run。
- [x] 实现 RunClient 三方法；WaitRun 默认 250ms 至 2s 退避，ctx 退出；CancelRun CAS 写请求，queued 可直接 cancelled。worker 循环消费取消，不能仅调用本地 Handle.Cancel。
- [x] 测试 TestReconnectWaitDoesNotDrive、TestCancelBeforeStart、TestCancelRunning、TestCancelCompletionRace、TestDuplicateDeliveryNoSideEffects、TestUnknownPayloadVersion。
- [x] 执行 `go test ./queue ./agent ./runstore -count=1` 与 `go test -race ./queue ./runstore -count=1`。
- [x] 提交 `feat(queue): add reconnectable managed task client`。

**完成标准：**新客户端仅凭 store+RunID 可查询/取消；至少一次重复消息不会重新执行同 Run；旧闭包桥接仍可用。

### TASK-SM-09：配置 claim 租期并隔离任务心跳

**依赖：**SM-02、SM-07。**决策：**D6。

**文件：**修改 agent/options.go、run_prepare.go、hitl.go、write_guard.go；新增 agent/lease_config_test.go、queue/run_heartbeat.go、run_heartbeat_test.go。

- [x] 写 TestClaimLeaseNormalAndResume、TestLongCallRenewsClaim、TestRenewFailureCancels、TestHeartbeatDoesNotAdvanceHead、TestWorkerEpochLossStopsPublishing。
- [x] 加 WithClaimLease，普通准备/恢复/guard 都取配置，默认仍为 30 秒；不更改 store 全局 DefaultLease。
- [x] 任务心跳与取消检查由 worker 的独立循环承担；默认 heartbeat=5s、worker lease=30s、取消轮询=250ms，启动校验间隔和 lease。
- [x] 使用可控 clock/ticker/barrier 测试跨多个租期的模型/工具阻塞、取消与最终提交；禁止用真实 sleep 30 秒模拟。
- [x] claim 续租失败取消执行且拒绝旧 token；记录心跳失败不伪造 checkpoint owner；worker lease 丢失不得发布旧 epoch 终态。
- [x] 执行 `go test -race ./agent ./queue ./checkpoint ./runstore -count=1`。
- [x] 提交 `feat(agent): configure claims and managed run heartbeats`。

**完成标准：**两个租约职责清晰；结束/取消后循环退出；不引入第二套 checkpoint 续租器。

### TASK-SM-10：摘要协议、稳定 cursor 与 Memory 查询

**依赖：**SM-02。**决策：**D7。

**文件：**新增 checkpoint/query.go、query_memory.go、cursor.go、query_contract_test.go；修改 memory.go。

- [x] 定义前述 MetadataReader/Query、摘要、页面、过滤类型和错误常量；排序/未知旧时间/limit 严格按 ADR。
- [x] TestMetadataNoPayload 检查摘要无消息/KV/blob；TestQueryStablePages 在两页间插入和更新时间；TestQueryFilterBinding/Expired/InvalidCursor 覆盖无效 token。
- [x] 首屏生成不可变摘要投影，cursor 绑定过滤与代号；最多 128 代、15 分钟 TTL，包内可注入时钟；不得保留完整 Checkpoint 指针。
- [x] Memory 保存成功后更新摘要索引，失败提交不改变索引；ListSessions 只枚举已有 checkpoint 线程。
- [x] 执行 `go test ./checkpoint -run 'Test(Metadata|Query)' -count=1` 和 `go test -race ./checkpoint -count=1`。
- [x] 提交 `feat(checkpoint): add metadata queries and stable pagination`。

**完成标准：**并发新增不使固定投影分页重复/漏项；legacy 未知字段诚实返回；筛选和排序可重复。

### TASK-SM-11：File 可重建摘要投影与 Session.History

**依赖：**SM-04、SM-10。**决策：**D7。

**文件：**新增 checkpoint/query_file.go、metadata_index.go、metadata_index_test.go；修改 file.go、agent/session.go；补 session_test.go。

- [x] 写 TestFileMetadataRebuild、TestFileMetadataOtherProcessRefresh、TestFileMetadataIndexFailureAfterCommit、TestFileMetadataNoBlobDecode。
- [x] sidecar 保存摘要及 journal 有效偏移/长度/格式标识；在原线程文件锁内校验和更新，不读取半条未提交记录。原 journal 改变或截断时失效重建。
- [x] 旧库首次构建可逐行提取摘要，禁止文件 Restore 或 blob 解码；稳态查询读取 sidecar/追加尾，不能调用 History/Latest/Load。测试注入读取计数证明。
- [x] 索引写失败后 Commit 仍成功，后续查询修复；损坏 sidecar 可删除重建，但绝不删除 journal。重启失去分页代号按 ErrCursorExpired 返回。
- [x] 接入 Session.History；缺 Query 的第三方 store 返回 ErrCapabilityUnavailable。保持原 History 全量 API 不变。
- [x] 执行 `go test ./checkpoint ./agent -count=1`；记录 `BenchmarkMetadataPage` 与全量 History 在 1k/10k 条、含文件内容数据上的 ns/op、B/op、allocs/op，报告实测不写固定倍数承诺。
- [x] 提交 `feat(checkpoint): add rebuildable file metadata indexes`。

**完成标准：**跨实例提交可见；payload 大小不导致稳态摘要查询还原全量消息/文件；索引不是第二事实来源。

### TASK-SM-12：任务列表、显式终态修复与轮询订阅

**依赖：**SM-06、SM-08、SM-11。**决策：**D5、D7。

**文件：**新增 runstore/query.go、query_test.go、watch.go、watch_test.go、queue/run_reconcile.go、run_reconcile_test.go、checkpoint/watch.go、watch_test.go。

- [x] RunSummary 与 RunListOptions 按状态/线程/RunID/时间过滤，采用同样的快照投影分页规则；Memory/File 统一契约且列表不带 Result 正文。
- [x] ReconcileRun 明确依赖 checkpoint.Query + MetadataReader + Load；先按 RunID 找 Completion，再校验 ThreadID/RunID/Ref，读取对应结果并 CAS 修复。缺能力返回 ErrCapabilityUnavailable。
- [x] 没有匹配证据只报告 stale；不把最新线程 head 的其他 Run 终态写入当前记录。任务成功但 Completion 失败不靠猜测变成已持久化。
- [x] TestReconcileExactRun、TestReconcileMissingEvidence、TestReconcileCASRace 覆盖成功执行后写 RunRecord 前崩溃以及 B 已推进线程的情况。
- [x] Watch 轮询实现按 ID/version 去重，合并慢消费者通知；checkpoint 通知只针对新 revision，run 心跳不产生 checkpoint 通知；ctx 退出关闭通道且不泄漏 goroutine。
- [x] 执行 `go test ./runstore ./checkpoint ./queue -run 'Test(Query|Watch|Reconcile)' -count=1` 和相同包的 race 测试。
- [x] 提交 `feat(queue): query reconcile and watch managed runs`。

**完成标准：**列表状态跨页一致；订阅丢通知可重读恢复；GetRun 保持纯读，修复由显式入口负责。

### TASK-SM-13：跨进程、故障注入与兼容验收

**依赖：**SM-09、SM-12。**决策：**D10。

**文件：**新增 queue/managed_process_test.go、runstore/recovery_contract_test.go、checkpoint/query_process_test.go；扩展 agent/session_test.go。

- [x] 使用 os.Executable 启动独立生产者/worker/客户端测试角色，共享临时 File 目录；父进程保存完整子进程输出便于失败诊断。
- [x] 场景 A：worker 写 running，另一进程 CancelRun，worker 观察取消并记录终态，新客户端 WaitRun 返回；不用内存 Handle 传递取消。
- [x] 场景 B：Completion 成功后 kill worker、RunRecord 尚未终结，新客户端显式 ReconcileRun 修复；在工具调用后但结果未保存处退出，验证 unknown outcome 不重放。
- [x] 场景 C：重复投递同 RunID 模型调用计数为 1；queued 后未入队崩溃保留可诊断排队记录；stale 不自动重启任务。
- [x] 场景 D：分页时另一进程追加 checkpoint，当前投影无重复/漏项，新查询可见新增；sidecar 损坏与 Sync 失败不污染 head。
- [x] 场景 E：仅实现四方法的第三方 Checkpointer 可继续 Agent.Run；Session Fork/History、Managed 强能力缺失明确报错；旧 EnqueueAgent/RunStatus 用例不改预期。
- [x] Redis：用相同 Type/Payload 路径跑可选集成测试，测试 Redis 地址由环境注入；无环境时明确 skip，不能把 Memory/File 的通过记作 Redis 验证。
- [x] 执行 `go test -race ./agent ./checkpoint ./queue ./runstore -count=1`；重点进程用例重复 `-count=10` 验证竞争稳定性。
- [x] 提交 `test(session): verify reconnect and query failure boundaries`。

**完成标准：**故障点是可控制的 barrier，不靠碰运气时间延迟；所有持久化成功声明有磁盘重开证据。

### TASK-SM-14：示例、文档与最终验收归档

**依赖：**SM-13 及全部前项。**决策：**全部。

**文件：**新增 examples/session/main.go、examples/managed-run/main.go；修改 README.md、ADR-0034 与本计划；必要时给 ADR-0033 加后续引用，不抹去旧契约。

- [x] Session 示例演示 Open→Send→Fork→子分支 Send→摘要 History；Managed 示例提供提交/worker/查询/取消角色，重启客户端仍可连接相同 RunID。
- [x] 示例使用 mock 模型和 File 存储，可离线运行；README 给可复制命令、目录参数、AgentResolver 和外部文件重新绑定方式。
- [x] 文档明确 Wait/WaitRecorded/WaitRun 差别、paused 返回语义、RecordError、cursor 过期、unknown outcome、namespace 延后、查询不等于恢复调度。
- [x] 运行 `go build ./...`、`go vet ./...`、`go test ./... -count=1`、`go test -race ./agent ./checkpoint ./queue ./runstore -count=1`、`git diff --check`。
- [x] 记录每任务真实提交哈希、测试命令与结果；未跑 Redis 如实标记。全部通过后将 ADR 状态改 Accepted、逐项勾选本计划；不提前写“已完成”。
- [x] 提交 `docs(session): document managed tasks and query contracts`。

**完成标准：**新增 API 有可执行示例，全部验收证据可复查，未交付范围清楚。

## 5. 验收矩阵

| 验收项 | 主要任务 | 必须观察到的结果 |
| --- | --- | --- |
| 跨线程谱系、旧记录 | 01/02 | Ref 可定位；旧来源未知不误判；digest 重试兼容 |
| 运行身份与结果 | 02/03/07 | 每次执行独立 RunID；Resume 同 Turn；结果不随 head 改变 |
| Session Fork | 04 | 目标 CAS；源/子文件隔离；错误不留成功假象 |
| RunStore | 05/06 | CAS 单赢家、深拷贝、跨进程重开、错误可见 |
| 查询与取消重连 | 08/13 | 不持 Run 指针；取消请求跨进程消费；Wait 不驱动 |
| 租约/心跳 | 09 | 长调用可续租；旧 token 被拒；心跳不改 head |
| 元数据分页 | 10/11 | 无 payload 还原；稳定 cursor；跨实例刷新 |
| 任务列表/Watch | 12 | 固定状态投影、断线重读、慢消费者不阻塞提交 |
| 崩溃窗口 | 12/13 | 有证据才修复；stale 不伪装已终结 |
| 兼容性 | 13/14 | 旧接口和第三方基础 store 保持原能力 |

## 6. 执行约束与风险处置

1. 每项先运行新增定向失败测试，再实现并验证；提交前用 `git diff --cached --name-only` 确认仅包含该项文件。业务行为变更必须有回归测试，纯文档修订无需重复整个测试集。
2. 若执行时发现 ADR 编号或 API 名冲突，先修正文档引用并记录原因；不得另建重复的 RunStatus/续租实现绕过已有契约。
3. 重大风险集中在提交锁顺序、旧摘要编码、任务/快照双写、未知投递结果与 File 投影跨进程刷新；对应任务不得只测 happy path。
4. P1 完成不代表完整租户隔离、自动恢复调度、exactly-once 副作用或无限保留 cursor；这些需要后续独立设计。
5. 当前这轮只生成 ADR 与计划，不修改源代码、不执行实现、不提交仓库；实施验收命令是后续门禁，不是本轮测试结果。（该约束只适用于规划轮；SM-01…SM-14 的实施与门禁结果见 §7，本节其余四条在实施过程中继续有效。）

## 7. 验收归档（2026-10-15）

任务到提交的对应关系由 `git log --name-only` 的实际文件归属核对；完整说明与门禁数字见 ADR-0034 §11。

| 任务 | 提交 | 卡片要点 |
| --- | --- | --- |
| SM-01 | `3bb3514` | ParentRef/Run/Turn 元数据、旧 journal 摘要 digest 兼容（`checkpoint/testdata/legacy_digest_v1.jsonl`） |
| SM-02 | `7cf096d` `4f2839b` `f49d8d3` `7b9987a` | 普通/计划/并行/恢复四条提交路径谱系贯通；重试身份稳定 |
| SM-03 | `15d7a75` `3a6bc3b` | `RunResult` 冻结身份+结果+边界；句柄创建即定身份 |
| SM-04 | `9d11cc5` `bb8b44a` | `Session` 门面与 `ForkFrom` 目标线程 compare-and-swap；运行选项与外部分文件隔离 |
| SM-05 | `e97f2ce` `d544cc4` `954c424` `4f48a3b` `3016b83` | `RunRecord` 状态机、深拷贝出口、版本闸门与 worker fencing |
| SM-06 | `2514800` `8845af7` `4d6354f` | File journal + OS 锁跨进程 CAS、崩溃恢复、Sync 语义；同批加固 worker 终态发布 |
| SM-07 | `a6622fe` `62e15ea` | `StreamManaged` 派发身份；worker 领取/发布三段错误分报 |
| SM-08 | `b80595f` | `RunClient` 三方法、版本化 Type/Payload 提交与 `RetryDispatch` |
| SM-09 | `4f87031` | claim 租期配置与任务心跳隔离（两套租约互不相干） |
| SM-10 | `ce0aafb` | 摘要协议、不透明 cursor 与 Memory 查询 |
| SM-11 | `ea73b26` | File 可重建摘要投影、`Session.History` |
| SM-12 | `1246b2c` `19613b1` `1845476` `62ec2ad` `2e322d1` `caf2f9b` | 共享分页内核、任务列表、轮询订阅、`ReconcileRun` 显式修复、分页基准 |
| SM-13 | `3d034a7` | 跨进程取消/崩溃窗口/重复投递/分页并发/能力兼容/Redis 可选用例 |
| SM-14 | `8689c3b` `cd036a6` `accb67e` `85fbb98` | `examples/session`、`examples/managed-run`、README 与 ADR-0033 后续引用；另修两条测时序的跨进程断言，并把 `WaitRun` 超时的注释改成与行为一致 |

逐项状态：SM-01…SM-14 卡内条目全部勾选（92 项）。

门禁与未验证项：

- 实测命令与结果记在 ADR-0034 §11，本文件不重复数字。
- Redis：`queue.TestManagedRunThroughRedisQueue` 已写入但本轮无可用实例，两种环境（未给地址、地址指向关闭端口）都实测为 SKIP。因此该用例的 Redis 分支未经验证，Memory/File 的通过不计为 Redis 验证。
- 一处反向检查不可单独观测：投影文件里一行不是 JSON 时，删掉解析失败即丢弃的那道防护不会改变结果（随后的版本校验同样拒绝）。登记为冗余防护，不是测试缺口。
- 两条跨进程用例的断言原本在测时序（等待超时交回哪一条记录、重复投递的落败方读到什么状态），已改成只断言契约能保证的部分；改写后的重复跑数字与重新改错一遍的验证结果记在 ADR-0034 §11。
- 本文件与 ADR-0034 仍按卡面约定等用户另行安排入库，所以上表 SM-14 的四笔提交里不含这两份文档。
- 明确延后、本批不做：多租户与服务端鉴权、namespace capability、投递前崩溃的自动 outbox 调度、外部副作用 exactly-once、永久 cursor、文件形态持久队列、`ResumeOfRunID` 的自动填充。
