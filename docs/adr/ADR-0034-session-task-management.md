# ADR-0034：会话门面、可重连任务记录与轻量查询

- 状态：Accepted（SM-01…SM-14 已落地并验收；见 §11 实施与验收记录）
- 日期：2026-10-12
- 代码基线：b28018c；实施前重新核对工作区。
- 输入：[会话与快照分析](../analysis/goagent-genkit-session-snapshot-review.md) 第三节 P1-A/B/C。
- 前置：[ADR-0032](ADR-0032-session-snapshot-consistency.md)、[ADR-0033](ADR-0033-background-run-context-and-status.md)。
- 执行计划：[plan-session-task-management.md](../tasks/plan-session-task-management.md)。

## 1. 问题与现有能力

Checkpoint 描述某个已提交状态，Thread 是有独立 revision/head 的分支，Run 是一次执行，Turn 是一次用户输入及其后续审批恢复。四者不可混用。业务目前要手工组合 Load、Fork、Save、OnThread；后台 Handle 持有进程内 Run，不能凭任务 ID 重连；管理页查询需要读取完整消息及文件。

本设计依据当前源码，不把旧分析中的缺口全部当成尚未实现：

| 当前代码证据 | 已有能力 | 本批剩余工作 |
| --- | --- | --- |
| checkpoint/snapshot.go、branch.go | 深拷贝、文件冻结、Fork 清除暂停/终态 | 跨线程可定位 ParentRef、Run/Turn/时间元数据 |
| agent/durable_runtime.go | 成功提交才更新本次运行 boundary | 完整普通提交谱系；boundary 不能冒充继承 head |
| agent/write_guard.go、run_prepare.go | 独立 RenewClaim、失败取消与 fencing；使用 DefaultLease | 配置租期并覆盖长调用，不能再建第二个续租循环 |
| agent/terminal.go、checkpoint/checkpoint.go | 显式 RecordCompletion、CAS、冻结结果 | 保持旧契约，新增独立任务记录 |
| agent/run_status.go | 只读线程 head 与 Completion | 按 RunID 查询；head 不是活跃任务状态 |
| queue/agent_bridge.go | Wait/WaitRecorded、提交与执行 context 分离 | 重连、持久化取消、可序列化任务入口 |
| queue/queue.go、redis_stream.go | Type/Payload/Registry、至少一次投递 | Agent 专用 payload 与重复消费保护 |
| checkpoint/file.go | JSONL、跨进程锁、Sync、恢复 | 轻量投影、稳定 cursor、可重建索引 |

## 2. 决策 D1：保持底层接口，新增高层能力

保留 Checkpointer 的 Save/Load/Latest/History、Agent.Run、Stream、Resume、RunStatus、EnqueueAgent 和 Handle 现有行为与签名。新增能力用可选接口或新入口表达，不给第三方 store 强加方法。

Session 放在 agent 包，可复用 restoreCheckpoint、运行归属与冻结结果。任务持久化协议放在新 runstore 包，只依赖 core 和 checkpoint；队列负责驱动，不让 checkpoint 依赖 queue 或 agent。Memory/File 为首批实现，不新增数据库依赖。

原因：把现有接口整体替换会破坏已有自定义 store；把任务心跳写进 checkpoint 会放大历史并混淆执行进度与业务快照。

## 3. 决策 D2：谱系、运行身份与时间

Checkpoint 增加向后兼容的可选字段：

```go
type Ref struct {
    ThreadID string `json:"thread_id"`
    CheckpointID string `json:"checkpoint_id"`
}
// Checkpoint 新字段：
// ParentRef *Ref `json:"parent_ref,omitempty"`
// RunID string `json:"run_id,omitempty"`
// TurnID string `json:"turn_id,omitempty"`
// CreatedAt time.Time `json:"created_at,omitzero"`
```

ParentID 保留并与新 ParentRef.CheckpointID 同写。普通提交指向同线程上一条成功提交；新线程 Fork 首条指向来源线程的指定 checkpoint。ParentRef 一旦提供必须完整且与 ParentID 一致，否则拒绝。旧记录只有 ParentID 时不推断跨线程归属，也不全库搜索同名 ID；查询标记 LegacyParentUnresolved，由应用决定是否迁移。旧 RunID/TurnID/CreatedAt 缺失保留未知，不伪造历史。

RunContext 分别维护继承 head 和本次 committed boundary。取得 owner 后读取 head；提交锁内选择 parent、分配稳定 ID/时间并提交，成功才推进 head。并行分支共享这个提交序列，不能各自从旧 head 构造 sibling 后冒充线性历史。第三方仅基础 Save 的 store 只能提供其现有并发保证，Session 的强一致 Fork 要求 Durable 能力。

每次新输入分配 TurnID；Resume 分配新 RunID、沿用暂停 checkpoint 的 TurnID，并在 RunRecord 记录 ResumeOfRunID。旧暂停无 TurnID 时在首次恢复创建新值。Fork 是新分支起点，清除继承 RunID/TurnID，首次 Send 创建新 TurnID。CreatedAt 是记录第一次构造的 UTC 时间；提交重试复用原值。head 永远按线程 revision，不按时间；心跳不改变 head。

所有新字段纳入冻结/克隆与幂等摘要。摘要变化必须兼容旧 journal 的 RequestDigest：保留旧字段缺失时旧摘要的字节语义，以固定旧 journal fixture 检验同请求重试；不得仅靠重新计算新摘要拒绝旧记录。

## 4. 决策 D3：Session 与结构化结果

新增下列 API（精确类型由任务 SM-01 固化）：

```go
func (a *Agent) OpenSession(ctx context.Context, threadID string) (*Session, error)
func (s *Session) Send(ctx context.Context, input string, opts ...RunOption) (RunResult, error)
func (s *Session) ForkFrom(ctx context.Context, checkpointID, newThreadID string) (*Session, error)
func (s *Session) GetHead(ctx context.Context) (*checkpoint.Checkpoint, error)
func (s *Session) History(ctx context.Context, opts checkpoint.ListOptions) (checkpoint.MetadataPage, error)
func (a *Agent) RunDetailed(ctx context.Context, input string, opts ...RunOption) (RunResult, error)
```

RunResult 包含 ThreadID、RunID、TurnID、Result core.Result、CheckpointRef *checkpoint.Ref、Revision、Paused。Ref/revision 来自该 Run 的冻结成功边界，不能在结束后读 Latest；没有本次提交时 Ref=nil、Revision=0。Result 深拷贝；错误仍通过 error 返回，结果保留已知身份供诊断。

OpenSession 校验 ID、绑定 Agent，不写空 checkpoint，不长期缓存 head。Send 最终线程必须等于绑定线程，冲突的 OnThread 选项拒绝。正常执行延续既有惰性准备/owner 协议。GetHead 返回 nil 表示尚无快照。History 不支持轻量查询能力时返回 ErrCapabilityUnavailable，不悄悄全量 History。

ForkFrom 只接受本 Session 的来源 ID：Load 校验归属、Fork 冻结、Durable.Commit(ExpectedRevision=0) 创建新线程首条；目标非空或被 owner 占用时失败，绝不覆盖。来源读取后不可变；目标 CAS 足够，不同时持有两线程锁。失败不得返回一个看似已保存的 Session。外部 FileStore 仍需调用方注入，不能承诺可回滚外部目录。Fork 不复制任务记录、不重新执行工具。

原因：门面承担容易遗漏的一致性编排；保留简便 Run 返回让小型应用不承担新模型。

## 5. 决策 D4：持久化 RunRecord 和状态机

新增 runstore.Store，按 RunID 精确读写，Memory/File 均支持 CAS 与深拷贝。RunID 是全局生成的执行身份，JobID 单独记录，不混同 Handle.ID。记录至少包含：

- RunID、ThreadID、TurnID、JobID、ResumeOfRunID；Version（记录 CAS 版本，与 checkpoint revision 分开）。
- Status、FinishReason、Error{Code,Message,Retryable}、Result（冻结结果，可选）。
- LatestCheckpointRef、CheckpointRevision、CompletionRef（可空）。
- CreatedAt、UpdatedAt、WorkerID、HeartbeatAt、CancelRequestedAt。
- RecordError（checkpoint 终态记录失败）、WorkerEpoch、WorkerLeaseUntil（记录 worker 所有权）。

状态：queued → running → paused/succeeded/failed/cancelling；queued → cancelled；running → cancelling → cancelled；取消竞争时 cancelling 也可进入 succeeded/failed/paused，需保留取消请求时间。paused、succeeded、failed、cancelled 都结束本次 Run；Resume 新建记录而不是复活旧 Run。paused 的 WaitRun 返回暂停记录，不无限等待后续审批。排队取消必须先原子 CAS；worker 启动也做 CAS，所以最多一方取得启动权。

取消是持久化请求，不代表外部副作用被撤销。worker 在开始模型/工具前检查、运行时轮询并取消运行 context。若完成结局已经冻结，按冻结结局发布，不因迟到取消改写成功；若运行因请求取消结束，发布 cancelled。DeadlineExceeded 按 failed/deadline_exceeded 区分用户取消。unknown outcome 必须保留具体 code，不自动重试外部工具。

WorkerEpoch 只保护 RunRecord 的 worker 写入；checkpoint 仍由现有 claim/fencing 保护。RunRecord 心跳绝不能代替 RenewClaim。worker 超时仅使 Liveness=stale，不擅自宣布 failed/cancelled，也不自动重新执行旧 Run。重复投递已被领取或终结的 RunID 返回可识别的重复结果，不再次调用模型/工具；stale 记录进入人工恢复流程，显式 Resume 创建新 RunID。

## 6. 决策 D5：两个持久化边界及重连协议

runstore.File 使用独立 runs 子目录的版本化 append-only 事件 journal、每 Run 跨进程锁和 Sync 后发布；不得直接复用 checkpoint 私有锁文件名。创建同 ID 同内容幂等、异内容冲突；写入失败不更新内存缓存；读取校验版本、尾部损坏遵守 checkpoint 同类恢复规则。旧二进制不保证能读新 runstore 格式，不改写旧 checkpoint journal。

不宣称 RunRecord 与 checkpoint 跨文件事务。顺序是：保存 queued → 投递队列 → worker CAS running → 执行并保存快照 → 尝试现有 RecordCompletion → 保存冻结执行结局与记录结果。无本次边界的准备失败也可保存 failed RunRecord，但不得造 checkpoint。暂停不调用 RecordCompletion。

投递确定失败时记录 failed/enqueue_failed；不确定投递结果保持 queued + dispatch_unknown，调用方获得 RunID 和错误，可用相同 JobID/RunID 重新投递。Create 成功而入队前崩溃留下 queued，P1 不实现自动 outbox 调度。执行完成但任务终态写失败则状态可能仍 running；必须返回记录错误，不承诺已持久化成功。独立 ReconcileRun 只凭该 RunID 的已提交 Completion 修复终态；没有可靠证据保留 stale，不能根据线程最新 Completion 猜测。

任务 succeeded 表达执行成功，CompletionRef 与 RecordError 单独表达 checkpoint 收尾是否保存，延续 ADR-0033 两个终点。GetRun 纯读，不协调/续租/修复；修复是显式写操作。终态不可被普通 CAS 改写，允许以相同内容幂等重试。

新增 queue.RunClient（绑定 runstore.Store）：GetRun(ctx, runID)、WaitRun(ctx, runID)、CancelRun(ctx, runID)。WaitRun 有 context 退出，默认轮询 250ms、最大 2s 退避；不驱动执行。未知 ID 返回 ErrNotFound。CancelRun 对终结记录幂等返回原记录；仅持久化请求，不能承诺立即停止。新客户端无需 *agent.Run，即可连接同一 File 存储查询与取消。

新增 EnqueueManagedAgent 使用稳定 RunID + AgentKey + 输入/消息 + ThreadID + TurnID 的版本化 Type/Payload。worker 通过应用提供的 AgentResolver 取得配置；不序列化 Agent、闭包、context、凭证或 FileStore 句柄。外部文件后端由 resolver 恢复。旧 EnqueueAgent 的闭包路径保留。P1 不承诺 Redis exactly-once 或崩溃自动重调度。

## 7. 决策 D6：续租配置与任务心跳

增加 WithClaimLease(time.Duration)，默认保持 DefaultLease；校验正值并保证续租间隔大于零。普通运行、Resume、writeGuard 必须使用同一配置，继续复用现有 lease/2 续租循环和提交锁。lease 丢失取消执行且旧 fencing 无法提交。

任务心跳使用独立可配置间隔（默认 5 秒，worker lease 默认 30 秒，要求至少覆盖 3 次心跳），只 CAS 更新 RunRecord，发生所有权丢失停止任务执行。瞬时写失败保留错误，超过 worker lease 停止发布任务结局；旧 worker 不得以新版本覆盖新 owner。模型/工具不返回不影响续租和心跳；取消无法杀死忽略 context 的 goroutine，沿用 WithToolTimeout 和 RunBudget 的既有边界。

## 8. 决策 D7：摘要、分页、会话/任务列表与可选订阅

checkpoint 增加可选 MetadataReader、Query；runstore 增加可选 Query。公开投影不包含 Messages、KV、FileSnapshot 或工具参数：

- CheckpointMetadata：Ref、ParentRef、LegacyParentUnresolved、Revision、Step、RunID、TurnID、CreatedAt、Paused、CompletionKind。
- SessionSummary：ThreadID、Head metadata；仅枚举存在快照的线程，不把尚未提交的 Session 当持久化会话。
- RunSummary：身份、状态、时间、checkpoint 引用和错误码；不默认带 Result 正文。
- ListOptions：Limit（默认 50，最大 200）、Cursor、ThreadID、RunID、CreatedFrom/CreatedBefore、CompletionKind。运行列表另用 RunListOptions.Status，避免混淆 Run 状态与 checkpoint 终态。

时间区间为 [from,before)，旧时间未知记录无时间筛选时返回，开启时间筛选时排除。单线程时间线按 Revision 升序；跨线程 checkpoint 按 (CreatedAt,ThreadID,Revision,ID)，旧时间为零；会话列表按 ThreadID，任务列表按 (CreatedAt,RunID)。所有查询定义稳定 tie-breaker。

cursor 是版本化不透明 token，包含筛选指纹、快照投影代号与位置。首屏冻结摘要投影（包括 Run 的可变状态），续页读取同一代，新增/更新不产生重复漏项；投影保留 15 分钟，过期或服务重启失去代号返回 ErrCursorExpired，错误筛选/损坏 token 返回 ErrInvalidCursor。File 投影缓存可放在进程内，重启后允许明确过期，不假装永久 cursor。限制每 store 同时缓存 128 个投影，超限淘汰最旧并返回可识别过期，不无限积累。

File 建可重建摘要 sidecar，checkpoint journal 是事实来源。写 checkpoint 并 Sync 成功之后才能更新投影；sidecar 失败不得把已成功 Commit 改报失败。查询检查 journal 长度/提交边界，落后则修复；损坏或缺失重建；不能遗漏其他进程新提交。首次旧库重建允许扫描原 journal，但不得还原文件 blob；稳态分页不可调用 Load/Latest/History 或解码完整 State。跨线程首次枚举允许目录扫描，P2 再优化为全局目录索引。

CheckpointWatcher/RunWatcher 保留为可选接口，P1 提供轮询实现（Memory/File 都可用），不承诺持久事件日志。通知是重读提示，版本去重；取消关闭通道，慢消费者合并通知，断线重连先查询当前状态。WaitRun 的正确性不依赖 Watch，进程内 bus 不当跨进程订阅。

原因：只返回摘要但内部仍全量 History 不能解决内存和文件载荷成本；可重建投影避免双写索引成为第二事实来源。稳定分页优先采用有期限的快照投影，避免过早引入数据库 MVCC。

## 9. 范围、安全边界与备选方案

本批按单个可信 store 范围设计；不接收客户端随意提交 tenant 字符串并宣称隔离。多租户部署必须由上层认证后选择独立 store/目录，覆盖 checkpoint、runstore、审批和工具幂等记录。完整 namespace capability、服务端鉴权、共享目录租户迁移另立 ADR；这正是原分析“多租户部署再加入”的条件项，不计入本批完成承诺。

其他不做：业务 StateCodec/schema 迁移、delta、GC/压实、前端 patch、分布式事务、自动重放未知结果工具、自动恢复调度。

备选：

1. 只扩展 RunStatus：成本低，但线程 head 无法表达排队/活跃运行或查询旧 Run，拒绝。
2. 把任务状态/心跳写成 checkpoint：复用存储，但历史膨胀、心跳扰动 head，与不可变业务快照边界冲突，拒绝。
3. 新建全功能数据库与调度服务：事务和查询容易统一，但依赖与迁移超出当前框架范围，留待 adapter。
4. 本决策的可选接口 + 独立记录 + 可重建投影：保留兼容，代价是明确双写窗口、显式修复及 cursor 生命周期。

## 10. 验收原则

Memory/File 同套行为契约；File 用独立进程验证读写、CAS、取消和投影刷新。覆盖普通/plan/并行/Resume/Fork 谱系、旧 journal 幂等摘要、无边界错误、前台兼容、重复投递、崩溃窗口、取消竞争、续租丢失、分页并发与无 blob 读取。测试不使用真实 LLM，计时测试用可控 clock/barrier；实际跨进程测试用测试二进制子进程。

本 ADR 是拟实施设计，不表示上述新增接口已经存在或测试已经通过。验收任务及提交切片见配套计划。

## 11. 实施与验收记录（2026-10-15）

代码基线 `b28018c`（ADR-0033 的最终门禁）之后共 30 个提交，按任务归属如下（哈希由 `git log --name-only` 的实际文件归属核对，不是按提交顺序推断）：

| 任务 | 提交 |
| --- | --- |
| SM-01 谱系元数据与旧记录兼容 | `3bb3514` |
| SM-02 贯通提交谱系 | `7cf096d` `4f2839b` `f49d8d3` `7b9987a` |
| SM-03 冻结结构化运行结果 | `15d7a75` `3a6bc3b` |
| SM-04 Session 门面与原子 Fork | `9d11cc5` `bb8b44a` |
| SM-05 RunRecord 状态机与 Memory RunStore | `e97f2ce` `d544cc4` `954c424` `4f48a3b` `3016b83` |
| SM-06 File RunStore 与崩溃恢复 | `2514800` `8845af7` `4d6354f`（该提交同时加固了 worker 的终态发布） |
| SM-07 派发身份与 worker 生命周期 | `a6622fe` `62e15ea` |
| SM-08 任务提交、重连等待与取消 | `b80595f` |
| SM-09 claim 租期与任务心跳隔离 | `4f87031` |
| SM-10 摘要协议、稳定 cursor 与 Memory 查询 | `ce0aafb` |
| SM-11 File 可重建摘要投影与 Session.History | `ea73b26` |
| SM-12 任务列表、显式修复与轮询订阅 | `1246b2c` `19613b1` `1845476` `62ec2ad` `2e322d1` `caf2f9b` |
| SM-13 跨进程、故障注入与兼容验收 | `3d034a7` |
| SM-14 示例、文档与两条时序断言的改写 | `8689c3b` `cd036a6` `accb67e` `85fbb98` |

这四笔提交只包含示例、先前已入库的文档与队列包的注释；本 ADR 与执行计划按其卡面约定仍由用户另行安排入库，所以 README 与 ADR-0033 里指向本文件的链接在当前提交历史里对不上。

最终门禁（实测，2026-10-15，Windows，全部离线：mock 模型、临时目录、测试二进制自重跑子进程）：

- `go build ./...`、`go vet ./...`：无输出、退出 0。
- `go test ./... -count=1`：退出 0，无 FAIL 包。
- `go test -race ./agent ./checkpoint ./queue ./runstore -count=1`：四包全 ok（agent 18.2s、checkpoint 13.3s、queue 17.6s、runstore 3.6s）。
- 进程用例重复 `-race -count=10`：`queue` 五个受管进程用例 ok（141.5s）、`checkpoint` 三个分页/损坏/Sync 用例 ok（68.7s）、`runstore` 两个恢复契约用例 ok（2.1s）。
- `git diff --check`：干净。

SM-13 的故障注入靠子进程打印的 barrier 行（工具进到了哪一步、外部效果落没落、终态快照提交后停在发布之前）驱动，父进程看到行之后才 `Kill`；每条"已持久化"的判断都由新开的 `runstore.NewFile`/`checkpoint.NewFile` 句柄读回。故意改错代码的反向检查共试 9 处，7 处被新用例杀掉：取消轮询不看记录、无证据也写终态、归属栅栏放开、续页不读冻结投影、非排队记录可再领取、投影版本/线程校验去掉、投影行数/位置校验去掉。1 处补断言后才被杀（重复投递只验证"没重跑"，没验证落败方向队列确认成功——已补 `HandlerError` 为空）。1 处只删一道防护观察不到：投影文件里一行不是 JSON 时，随后的版本校验同样会拒绝它，属于冗余防护，登记为"不可单独观测"而非测试覆盖。

两条跨进程用例的断言在全量跑中被测出是在测时序而不是测契约，已改写。其一，等待一条没人领取的排队记录：超时点落在两次轮询之间时 `WaitRun` 交回最后读到的记录，落在读里面时 `runstore` 的文件锁入口把同一个 context 错误原样抛回、`WaitRun` 交回的是空记录，所以那里只认 `context.DeadlineExceeded`，"等待不推动运行"改由磁盘重读的记录状态与版本证明。其二，重复投递的落败方在领取前读到的是"已被别人领走"的记录，它是 running 还是 succeeded 取决于两条投递谁先落盘，因此只断言它不可能是排队态或取消态；"没重跑"仍由两方执行标记互斥、模型调用落盘文件只来自一个进程、记录归属赢家来钉，"队列确认成功"仍由 `HandlerError` 为空来钉。改写后实测：排队记录用例 `-count=30` ok（62.7s），重复投递用例 `-count=20` ok（2.4s）；把"非排队记录可再领取"重新改错一遍，重复投递用例第一次就红在 `HandlerError` 那行，报错文字带 `runstore: worker ownership lost`。

Redis：`TestManagedRunThroughRedisQueue` 用与生产一致的 Type/Payload 路径，地址由 `GOAGENT_REDIS_URL`（或 `REDIS_URL`）注入；本轮无可用实例，该用例在无环境变量与指向关闭端口两种情况下都明确 SKIP（实测输出 `--- SKIP`），因此本 ADR 不声称 Redis 后端已通过验证。`queue/redis_stream.go`、`queue/redis_pubsub.go` 与 `examples/redis` 同样不在本轮门禁的背书范围内。

未交付且明确留给后续设计：多租户与服务端鉴权、完整 namespace capability、投递前崩溃的自动 outbox 重投调度、外部副作用的 exactly-once、跨重启仍有效的永久 cursor、文件形态的持久队列，以及 `ResumeOfRunID` 的自动填充（当前由建记录的一方自行填写，存储只保证它不可变）。
