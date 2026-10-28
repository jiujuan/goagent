# goagent 与 Genkit Go：会话和快照升级分析

核对日期：2026-10-11（Asia/Shanghai）。

本地代码基线：02dbb6e430b7675e908cb534a63d4893b146f84e。Genkit Go 源码基线：09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b，从用户给出的 main/go 路径定位到当前 Go 代码最后一次提交后固定该版本读取。Genkit 对应实现位于 go/ai/exp、go/ai/exp/localstore 和 go/plugins/firebase/exp；它们是实验接口，不能直接当成已稳定的契约。

结论：优先修复快照隔离、文件分支及同线程写入一致性，再增加会话门面、任务状态查询和存储索引。保留 goagent 的逐步 checkpoint、原子提交、恢复 fencing 与工具执行记录。

本次仅分析，新增本文；未修改运行时代码。已有 docs/analysis/agent-genkit-analysis.md 保持原样。

## 1. 能力与语义对照

| 维度 | Genkit Go 实际行为 | goagent 现状 | 建议 |
| --- | --- | --- | --- |
| 会话身份 | SessionID 表示会话，SnapshotID 选择具体状态；指定两者会核验归属；无 store 时支持客户端携带状态。[G1][G2] | OnThread + ThreadID 延续会话，RunID 表示执行；Stream 自动读取 Latest。 | 先以 ThreadID 为唯一会话标识增加轻量 Session 门面，不急于引入另一套 ID。 |
| 快照粒度 | 普通执行在 turn 结束保存；detach 先写 pending 行，后台结束后原地终结该行。[G1][G3] | AgentLoop 在步骤完成、暂停和可恢复失败边界保存；Checkpointer 契约为 append-only。 | 保留步骤级持久化；面向用户另提供回合级视图。 |
| 分支 | WithSnapshotID 从指定节点继续，ParentID 连接新节点；同一会话可出现分支，Latest 按 CreatedAt 选择，不沿父链推导。[G2][G3] | 已有 Fork，建立新 ThreadID + ParentID；普通 loop/plan 写入不填 ParentID。 | 补齐父链与高层 Fork 操作，明确不同分支各自的 head。 |
| 隔离 | 内存 store 在保存及读取时深拷贝；恢复会话时也克隆状态。Session.Messages/Custom 并不承诺所有读取都是深拷贝，仍需遵守各方法契约。[G3][G4] | Memory 保存/返回对象指针；cloneState 仅复制外层切片、map。 | 明确不可变快照和读取结果所有权，统一 Memory/File 契约。 |
| 原子性与并发 | SaveSnapshot(fn) 表达单快照原子更新；Firestore 用事务；本地 File 明确不提供跨进程事务。[G2][G5][G6] | Durable.Commit 将 checkpoint、inbox ack、工具结果一起提交；Resume 有租约与 fencing；File 有跨进程锁和 Sync。 | 保留这些能力，将写入所有权保护扩展至普通 Run/Stream。 |
| 生命周期 | 快照含 Status、FinishReason、Error、CreatedAt、UpdatedAt、HeartbeatAt；Expired 是读取时推导。[G1][G2] | Checkpoint 有 Pending/Pause/Revision；终态主要在 Run 与事件中。 | 增加持久化 RunRecord，避免通过消息推断任务终态。 |
| 后台任务 | RunDetached/Task 提供可按 SnapshotID 重建的 Poll/Wait/Abort；依赖状态订阅能力。[G7] | queue.EnqueueAgent/Handle 已有后台执行，但 Handle 直接持有 *agent.Run。 | 增加可按 ID 重建的任务控制和持久化状态。 |
| 状态类型 | SessionState[T].Custom 是泛型业务状态；UpdateCustom 可生成客户端 patch。[G1][G2] | KV 为 map[string]any，业务字段和 __plan__ 等系统字段在同一命名空间。 | 增加可选类型化状态 codec、命名空间、schema 迁移。 |
| 长历史存储 | Firestore adapter 使用周期完整状态 + JSON Patch + 分片，另有 latest pointer 与 metadata-only 读取。[G6] | File 已对文件 blob 按内容去重，但各 checkpoint 仍携带 Messages/KV/Todos；读取与提交会扫描 journal。 | 先索引与轻量读取，再按测量结果引入增量与压实。 |

## 2. 第一批：修复已复现的一致性问题

### P0-A：快照需要真正不可变

证据：checkpoint/durable_memory.go:97 的 commitLocked 将输入 checkpoint 指针直接追加到 byThread；checkpoint/memory.go:38 的 Load、:50 的 Latest 返回内部指针，History 只复制指针切片。agent/loop.go:378 只浅拷贝 State；checkpoint/branch.go:25 和 agent/runtime.go:189 也只复制外层容器。

实测：Save 之后修改原对象的 KV，已保存的值随之变化；修改 Latest 的返回值后再次读取也能看到变化。Fork 后修改嵌套 KV 和 Message.Parts，父快照一起变化。

影响：时间旅行、审计、恢复和分支实验无法依赖历史节点保持原样；内存测试也难以代表文件持久化后的行为。

建议在存储边界实现 Freeze/Clone：保存时固化消息及其嵌套 Part、字节、业务状态、Pending/Pause 和文件快照，读取时返回独立值。受控消息类型可用专用 clone；业务状态走可配置 codec。对不可序列化值返回明确错误，不以 panic 作为业务状态的正常错误机制。不能直接对整个 State 做 JSON 往返后宣称完成，因为 Files 被 json:"-" 排除，而且 map[string]any 的数字会发生类型归一化。

Genkit 可借鉴点是 store 的隔离契约和恢复前克隆，而非照抄其所有 getter 或内部 jsonClone。[G2][G3][G4]

验收：对保存输入、Load/Latest/History 的返回值、fork 子状态分别做嵌套修改；父快照及历史节点均不变。Memory 和 File 运行同一套契约测试。

### P0-B：修复文件快照分支的完整性

已复现链路：File.Latest(source) → checkpoint.Fork(parent, target, id) → File.Save(child) → File.Latest(target)。父快照有 1 个文件，子快照有 0 个文件。

原因有两层：checkpoint/branch.go:12 只复制 State，未携带 FileSnapshot；checkpoint/file.go:735 的 buildCheckpointRecord 仅从 State.Files.(Snapshottable) 取文件，也不使用 FileSnapshot。因此只在 Fork 中补复制 FileSnapshot 仍不足以修复保存路径。

建议统一“从已保存 checkpoint 建立可运行状态”的逻辑：要么将 FileSnapshot 还原成独立 InState，再保存分支；要么让存储层接受明确的冻结文件快照。对可快照文件默认隔离分支；对 DirStore/远端外部文件保留已有外部管理契约，并让 Fork 显式选择共享或复制策略。不要破坏 subagent 有意共享 Files 的协作行为。

Pending/Pause 的继承也需要显式语义：ForkConversation 表示另起一条对话；ResumeExecution 表示继续暂停执行。不能不加区分地把父分支审批记录或工具执行结果移植到新分支。

验收：Memory/File、普通文件状态/空文件状态、跨进程读取后分支、分支写入不影响父状态，分别覆盖。外部目录的共享策略单独验证。

### P0-C：普通会话运行也要有写入所有权

已有能力：agent/hitl.go:86 在 Resume 时按 revision/pause 申请 Claim；Durable.Commit 验证 fencing。缺口是 agent/agent.go:130 的 Stream 先读取 Latest，普通运行并未申请相同的所有权，CommitRequest 也没有用于普通写入的 ExpectedRevision。

确定性复现不需要同时跑两个 goroutine：先执行 seed，再依次创建 runA = Stream("request-A") 和 runB = Stream("request-B")，此时两者都读到 seed；然后先 Wait(A)、再 Wait(B)。两个运行都成功，最新快照中的用户消息是 [seed, request-B]，缺少 request-A。A 仍存在于历史节点中，因此这是最新上下文遗漏，不是所有历史数据被物理删除。

建议默认每个 Thread 同时只有一个写入者；第二个请求明确冲突或进入既有 durable inbox。创建惰性 Run 句柄时不应提前冻结一个以后才执行的无保护 head；应在实际执行时按规定顺序获得所有权并加载状态。Commit 增加 expected head/revision，跨进程仍使用 fencing。明确需要并行探索时创建分支。

仅在最后 Commit 加 CAS 不够：模型或外部工具可能已经执行，仍需要执行前协调及已有工具幂等/unknown outcome 机制。Genkit 的单快照事务和 Session 内部 mutex 不能被解读为跨实例串行化整个会话，不能直接用来替换 goagent 的所有权机制。[G2][G5][G6]

验收：同线程两个惰性句柄、多个 Agent 实例、跨进程 writer、租约失效后的旧 writer、不同线程互不阻塞。

## 3. 第二批：补齐会话和任务管理接口

### P1-A：建立完整的快照谱系与会话门面

普通 AgentLoop 与 planRunner 创建 checkpoint 时未填 ParentID，实测三个正常 checkpoint 的 ParentID 均为空；不能仅凭已有 Fork 推断当前所有历史已经形成完整树。

建议 RunContext 维护当前 head，成功 Commit 后更新；每个新 checkpoint 保存 ParentRef{ThreadID, CheckpointID}、RunID、CreatedAt，以及适合面向用户聚合的 TurnID。跨线程 parent 需要可定位的引用，因为现有 Load 必须同时传 threadID 和 checkpointID。

高层增加 OpenSession、Send、ForkFrom、GetHead、History 等最小入口；这些是建议名称，不是现有 API。高层负责加载、校验归属、文件还原和保存，调用者不必手工拼 Load → Fork → Restore → Save → OnThread。保留 Agent.Run 的简便返回，同时增加携带 ThreadID/RunID/CheckpointID/Revision 的结构化结果。

Genkit 的 latest 依据“最新创建时间”，ParentID 仅表谱系；其老快照心跳或 finalize 不会使它越过更新的 sibling。[G2] goagent 可以继续按线程单调 Revision 确定 head，并给每个分支独立 head，避免照搬跨分支争夺 latest 的语义。

### P1-B：把后台任务从内存句柄升级为可重新连接的记录

goagent 已有 queue worker、后台 Handle、事件流与 Cancel。queue/agent_bridge.go:14 的 Handle 内含 *agent.Run，因此这个桥接接口本身不能仅凭持久化的任务 ID 在新进程重建；这不等于整个 queue 包没有 Redis 能力。

建议新增 RunRecord，关联 ThreadID、RunID 和最新 CheckpointRef，保存 running、paused、succeeded、failed、cancelling、cancelled 等状态，以及 FinishReason、结构化错误、创建/更新时间、worker、HeartbeatAt。Checkpoint 保持 append-only；可变任务状态、心跳和取消请求写到专门记录或独立 journal 事件，不反复重写历史状态。

提供 GetRun/WaitRun/CancelRun 与可选 Watch 能力；任务记录必须支持按 ID 重新加载。worker 消费持久化取消请求、提交终态；进程退出后新客户端仍能查询。Genkit 的 DetachedTask.Poll/Wait/Abort、metadata-only 查询和状态订阅是可直接借鉴的使用体验，但“可重新连接查询”不自动等于崩溃后会重新调度执行。[G1][G2][G7]

另一个需要核实并处理的长任务风险：DefaultLease 是 30 秒，现有恢复租约只在 Commit 时续期，agent 中未发现独立 Renew/Heartbeat；validateCommitClaim 对已过期 claim 返回 ErrStaleFencingToken。若恢复后的单次模型/工具调用跨过租约，后续提交会遇到过期保护。此项根据代码推导，本次未运行 30 秒端到端超时实验。应提供可配置租期、独立续租、续租失败后的取消和 fencing 校验；HeartbeatAt 只说明存活，不能替代租约所有权。

### P1-C：轻量查询、分页及可选能力接口

现有 Checkpointer 的 Save/Load/Latest/History 足以支撑执行，但不足以高效实现会话列表、任务列表和时间线页面。建议保留它的兼容性，增加 CheckpointMetadataReader、CheckpointQuery、CheckpointWatcher 等可选能力；返回 checkpoint 摘要和 cursor，按 Thread/Run/时间/终态过滤，避免读取全部消息和文件。Genkit 的 SnapshotMetadataReader 与 SnapshotSubscriber 是同类能力拆分。[G2]

多租户部署再加入可信 context 推导的 namespace/tenant 范围；作用到 checkpoint、head、任务记录、审批和工具幂等键。File 的文件名校验只防止不合法路径，不等于租户授权；Genkit Firestore 的 prefix 分区也是存储隔离机制，不能单独当成完整鉴权。[G6]

## 4. 第三批：状态演进与规模优化

### P1/P2：类型化业务状态与版本迁移

建议先给 KV 加一个可选的 StateCodec[T] / typed state 层，把业务状态与内部运行字段分开，而不是把全部 Agent、工具和中间件一次泛型化。codec 负责序列化、克隆、校验和 schema version 迁移；旧 API 继续工作。Genkit 的 SessionState[T].Custom 和 UpdateCustom 提供了可借鉴的接口形态。[G1][G2]

goagent 并非完全没有版本：checkpoint/file.go:81 已有 durableFormatVersion=1，负责 journal 格式兼容。缺少的是业务状态 schema 的版本和迁移策略；Revision 则用于提交顺序与并发控制，不能兼任 schema version。

若需要前端同步任务计划、进度和业务状态，再增加带基线 revision 的 StateChanged/patch 事件，并提供完整状态重同步。Genkit 的 CustomPatch 是客户端状态同步机制；Firestore 的 JSON Patch 是磁盘/数据库存储机制，两者需要分别设计。对外状态投影和流式输出投影保持一致，内部状态仍完整保留用于恢复。[G1][G2][G6]

### P1/P2：先索引，再做增量快照

checkpoint/file.go:448 的 Latest 调用 readAll；readAll 不仅读全部 checkpoint，还 rebuild 所有文件快照。withThread 在每次读写前 scanLocked 扫描完整 journal，并持有 File 实例级 mutex，导致同一实例内不同线程也会排队。文件 blob 已内容去重，这个优化应保留。

建议第一阶段提供 head 索引、checkpointID 到日志位置的索引、metadata-only 路径、文件内容按需还原，以及按 thread 分片的锁；索引作为可从 journal 重建的派生数据，崩溃后不得成为另一套真相。已有跨进程锁与提交 Sync 的保证不能因性能改造丢失。

第二阶段再考虑消息增量、业务状态 delta 与周期完整快照，并限制最长还原链。Genkit 的 Firestore 实现是“周期完整状态分片 + JSON Patch diff + latest pointer”，不是所有 Genkit store 都自动进行增量压缩。[G6] 应保留快照的外部语义，在存储内部完成 delta 还原，不把所有运行时 StateOp 强行变成事件溯源协议。

完整快照依然随会话状态变大，增量存储也不能让读取成本与状态大小无关。先测 100/1000/10000 checkpoint 下 Save、Latest、分页 History 的耗时、峰值内存、日志体积，再决定是否实施。

若需要多实例共享存储，可增加 SQLite（本机）或 PostgreSQL（服务端）adapter。新 adapter 必须同时满足 checkpoint、inbox ack、审批、工具执行与 fencing 的一致性，不能只实现 Save/Load 就宣称 durable 能力等价。

### P2：保留策略和空间回收

在完整谱系和任务记录建立后，提供会话列举、归档、分页、TTL/保留策略和 GC。回收需保留活动 head、可恢复 pause、仍引用的祖先/完整基线、文件 blob、工具执行记录与幂等窗口。共享 blob/delta 基线不能仅按“某快照太旧”删除。这些是基于 goagent 运维需求提出的扩展，并非已经确认 Genkit 的最小 SessionStore 全部提供。

## 5. 应保留的 goagent 能力

- Durable.Commit 将 checkpoint、消费确认、工具结果绑定到同一次提交。
- 恢复流程中的 revision、pause identity、Claim、fencing，审批记录的内容冲突检查。
- 对已经开始却未持久化结果的工具给出 unknown outcome，而不是默认再次执行；这仍不等于任意外部副作用的 exactly-once。
- File 的 journal 格式版本、Sync、截断尾修复、旧格式兼容与文件内容去重。
- 检查点写失败会影响执行结果的现有语义及测试。Genkit 当前 snapshotTurnEnd 写失败只记录日志并返回空 SnapshotID，是 best-effort；不适合替代 goagent 对可恢复执行的提交要求。[G3]

上述源代码入口：checkpoint/durable.go:14、checkpoint/durable_memory.go:210、checkpoint/file.go:196、agent/durable_runtime.go:112、agent/phase1_failure_test.go:78。Genkit 的本地 File 明确说明没有跨进程事务；因此不应把“接入 Genkit 风格 store”理解为自动获得更强持久性。[G5]

## 6. 建议实施顺序与验证记录

1. 第一组变更：快照冻结/深拷贝、文件 Fork 恢复、常规运行所有权、完整父引用；先覆盖本次复现场景。
2. 第二组变更：Session 门面与结构化结果、RunRecord/状态查询、可重新连接的任务句柄、独立续租。
3. 第三组变更：head/metadata 索引、分页、状态 codec/schema 迁移、存储 adapter 一致性套件。
4. 根据性能结果再做：delta/周期完整快照、压实与 GC、前端 patch 同步。

已执行 go test ./checkpoint ./agent，两个包均通过，命令退出码 0。本次没有执行 Genkit 测试、真实云数据库集成测试或性能基准，因此有关 Genkit 保证以其源码及接口契约为依据，性能收益是待验证的设计判断。

临时验证程序位于 C:/Users/xing/AppData/Local/Temp/goagent-genkit-session-review/probe.go；在本仓库目录执行 go run 该文件，使用 mock 模型，不调用真实模型服务。输出关键字段如下：

```json
{
  "file_fork": {"parent_files": 1, "child_files": 0},
  "normal_checkpoint_parent_ids": ["", "", ""],
  "overlapping_handles_latest_user_messages": ["seed", "request-B"],
  "parent_message_after_fork_mutation": "changed-by-child",
  "parent_nested_value_after_fork_mutation": {"value": "changed-by-child"},
  "saved_snapshot_changed_via_input": "changed-after-save",
  "saved_snapshot_changed_via_read": "changed-via-read"
}
```

现有测试通过与上述问题可以同时成立：当前测试未阻止这些契约缺口。本次复现针对当前代码，不代表线上已发生数据丢失。

## 7. Genkit 原始依据（固定提交）

所有源码由 GitHub 官方仓库直接获取；网页检索工具没有返回可用内容，因此没有把搜索摘要作为证据。完整源码副本保存在 C:/Users/xing/AppData/Local/Temp/goagent-genkit-session-review/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/。

[G1] go/ai/exp/gen.go：AgentInit 89–123；SessionSnapshot 346–393；SessionState 395–416；SnapshotStatus 418 起。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/gen.go

[G2] go/ai/exp/session.go：SnapshotReader 86–105；SnapshotMetadataReader 117 起；SnapshotWriter 150–189；SnapshotSubscriber 203–212；readSnapshot 277 起；Session 674 起；UpdateCustom 768 起。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/session.go

[G3] go/ai/exp/agent.go：snapshotTurnEnd 643–691；runHeartbeat 2052 起；resumeSessionFrom 2285 起。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/agent.go

[G4] go/ai/exp/localstore/inmemory.go：GetSnapshot 60–68；GetLatestSnapshot 87–98；SaveSnapshot 161–223。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/localstore/inmemory.go

[G5] go/ai/exp/localstore/file.go：63–69 明确写明文件写入原子，但不提供跨进程事务。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/localstore/file.go

[G6] go/plugins/firebase/exp/firestore_session_store.go：77–110 存储模型；604 起 SaveSnapshot/RunTransaction；700–751 checkpoint 与 delta 选择；758 起过大 patch 提升为完整 checkpoint。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/plugins/firebase/exp/firestore_session_store.go

[G7] go/ai/exp/task.go：27–48 DetachedTask；97–131 SnapshotID/Poll/Wait/Abort。

    https://github.com/genkit-ai/genkit/blob/09a9b1532c1d82a33ac1abfab20f316ff5dd6f2b/go/ai/exp/task.go
