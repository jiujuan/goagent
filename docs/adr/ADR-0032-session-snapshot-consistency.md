# ADR-0032: 会话快照不可变性与线程写入协调

- 状态: Accepted
- 日期: 2026-10-12
- 关联: ADR-0024（durable run）、ADR-0025（append-only checkpoint）、ADR-0026（State.Files 持久化）、`core/clone.go`、`checkpoint/{snapshot,branch,coordinator}.go`、`agent/{run_prepare,write_guard,durable_runtime}.go`

## 背景

会话与快照实现曾存在三个已经复现的一致性问题：

1. `Save` 输入、`Load`/`Latest`/`History` 返回值和 `Fork` 子对象仍与已提交快照共享可变引用，调用方后续修改可能反向污染历史。
2. `File.Load`/`Latest` 返回的 `FileSnapshot` 在 `Fork` 后重新保存时丢失，子分支文件数可能从 1 变成 0。
3. `Stream` 在创建惰性句柄时读取旧 head。多个句柄先创建、后依次执行时，后执行者仍从旧 head 启动，丢失前一个运行刚提交的上下文；真正并发的同线程 writer 也缺少统一的副作用前门禁。

这些问题不能通过修改旧 checkpoint 或简单复制顶层 struct 解决。历史查询、时间旅行和 Fork 都依赖 append-only 快照；普通运行、Resume、审批和工具账本又必须共享同一套 revision、claim 与 fencing 语义。

## 决策

### 1. 保留 append-only，在边界冻结和克隆

`Checkpointer.Save` 继续追加新 checkpoint，不原地修改旧记录。这样 `History`、按 ID `Load`、时间旅行和 Fork 仍能观察每个历史点。

不可变性在存储边界实现：

- 保存前冻结 `State`、`Pending`、`Pause` 和 `FileSnapshot`。
- `Load`、`Latest`、`History` 和 durable commit 的读取出口再次返回独立副本。
- `Fork` 从父 checkpoint 创建独立子快照，而不是复用父对象中的 map、slice、字节或文件快照。
- clone/freeze 失败时不发布部分 checkpoint，也不确认 inbox 或工具完成结果。

### 2. State 与 KV 的克隆契约

`core.CloneMessage` 深拷贝消息中的 `[]byte`、`json.RawMessage` 和嵌套工具结果。`core.CloneStateData` 深拷贝 `Messages`、`Todos`、`KV`，但保留活跃的 `State.Files` 句柄；文件内容由 checkpoint 冻结层单独处理。

`KV` 支持并保留具体 Go 类型和 nil 语义：

- 标量及命名标量。
- `interface` 中的受支持值。
- 字符串键 map、slice、array、pointer。
- 仅包含可复制导出字段的 struct。
- 白名单类型 `time.Time`、`json.RawMessage`、`json.Number`。

以下值被拒绝：

- 循环引用、非字符串键 map。
- `func`、`chan`、complex、unsafe pointer。
- `NaN`、正负无穷等非有限浮点值。
- 含私有字段且无法安全复制的 struct。
- 白名单之外实现自定义 JSON/Text marshal 或 unmarshal 接口的类型。

错误使用 `core.ErrStateNotCloneable` 包装，并携带 KV 路径和具体类型；调用方可用 `errors.Is(err, core.ErrStateNotCloneable)` 判断。这里没有实现类型化业务 codec 或 schema migration；需要这些能力的业务值必须先转换为受支持的数据形态。

### 3. 活跃文件共享与 checkpoint 文件冻结分离

运行期的 `core.CloneStateData` 保留 `State.Files` 句柄，使 subagent 和父运行仍可共享同一活跃工作区。已提交 checkpoint 则不能继续引用其中的可变内容：

- 当 `State.Files` 实现 `core.Snapshottable` 时，checkpoint 冻结层取得独立 `FileSnapshot`，并清除冻结快照里的活跃句柄。
- Memory 和 File checkpointer 都保存不可变的文件视图；所有读取出口再次克隆 `FileSnapshot` 及其字节。
- nil `FileSnapshot` 表示没有便携文件快照；非 nil 空 map 表示明确的空文件视图，不能回退到旧句柄。
- 显式 `agent.WithRunFiles` 始终覆盖 checkpoint 中的文件快照。

不实现 `core.Snapshottable` 的后端仍是外部托管：

- Memory 可在同进程内保留其活跃句柄，但不能提供内容时间旅行保证。
- File 不持久化该后端的内容。
- 跨进程 Resume 时调用方必须重新传入等价的 `WithRunFiles`。
- 真实目录或远端存储的内容不会随 checkpoint 时间旅行回滚。

### 4. Fork 是可能失败的纯快照构造

`Fork` 的签名由单返回值改为：

```go
func Fork(parent *Checkpoint, newThreadID, newID string) (*Checkpoint, error)
```

迁移调用：

```go
// 旧
child := checkpoint.Fork(parent, "child-thread", "child")

// 新
child, err := checkpoint.Fork(parent, "child-thread", "child")
if err != nil {
    return err
}
if err := store.Save(ctx, child); err != nil {
    return err
}
```

`Fork` 校验 nil parent、目标 ThreadID、目标线程不能与父线程相同、checkpoint ID 不能为空且不能与父 ID 相同。成功时深拷贝状态和文件快照，设置 `ParentID=parent.ID`、`Revision=0`，并清除 `Pending` 和 `Pause`。

Fork 本身不保存 child，也不复制 durable inbox、审批决策或工具执行账本。它用于从一个历史点创建不同线程的继续路径；继续一个暂停线程必须调用 `Agent.Resume`，不能用 Fork 绕过暂停语义。

### 5. BeginRun 原子返回 writer 所属的 head

`checkpoint.ThreadCoordinator` 是可选接口，Memory 和 File 实现它。`BeginRun` 必须在同一个内存锁或文件锁边界内原子完成：

1. 检查当前活跃 writer。
2. 检查 head 是否处于暂停状态。
3. 获取或续用租约和 fencing token。
4. 返回与该所有权判定处于同一时刻的 head。

Agent 不能先调用 `Latest` 再单独 claim，否则两步之间存在 TOCTOU 窗口。普通同线程运行冲突返回 `checkpoint.ErrClaimConflict`，暂停线程的普通运行返回 `checkpoint.ErrResumeRequired`；两者都必须发生在模型或工具调用之前。

每次提交通过 `ExpectedRevision` 对 checkpoint head 做 CAS。write guard 独立续租；续租失败会记录首个所有权错误并取消根 context，后续模型、工具分发和提交都必须经过门禁。guard 只由顶层 Run 启动和释放，Sequential、Parallel、Loop、Plan、AsTool、transfer 和 subagent 等嵌套执行共享同一个 durable 运行状态，不能提前释放 writer。

该设计不宣称 exactly-once。工具外部副作用已经发生、但结果尚未原子提交时，账本保留 unknown outcome；恢复路径不能默认重复执行该工具。

### 6. 惰性运行与 Prepared Resume

`Agent.Stream` 创建句柄时只校验 ThreadID 并冻结输入消息，不读取 store。第一次 `Wait` 或 `Iter` 驱动时才调用 `BeginRun`，加载当时的当前 head，并把本次输入追加到该状态。

`Agent.Resume` 不同：它在返回 prepared handle 前先读取暂停点并取得 durable claim，使审批决定绑定到明确的 revision/pause。真正 drive 前 write guard 必须先调用 `RenewClaim` 验证该 claim；如果 claim 已过期或被替换，返回 `checkpoint.ErrStaleFencingToken`，不得重新 claim，也不得执行已经批准的工具。prepared handle 在 drive 前调用 `Cancel` 会同步释放 claim。

### 7. 第三方 store 的兼容边界

`ThreadCoordinator` 是可选扩展，因此只实现既有 `checkpoint.Checkpointer` 或 `checkpoint.Durable` 的第三方 store 仍可编译并沿用旧路径。它们不会自动获得新增的跨实例普通 writer 串行保证。

需要强制该保证时，在构造 Agent 时启用：

```go
a, err := agent.New(
    agent.WithModel(model),
    agent.WithCheckpointer(store),
    agent.WithThreadCoordinationRequired(true),
)
```

如果 store 不实现 `checkpoint.ThreadCoordinator`，`agent.New` 返回可由 `errors.Is(err, checkpoint.ErrCoordinationUnavailable)` 识别的错误。包装 Memory/File 的 adapter 若要保留新保证，也必须显式转发 `BeginRun` 和 `RenewClaim`，仅嵌入或转发旧接口不够。

## 结果与取舍

- 已提交快照的数据和可快照文件不再受调用方后续修改影响。
- File 快照能跨 store 重开、Fork、重新保存和 Agent 恢复。
- 惰性句柄在真正启动时读取最新 head；真正重叠的同线程 writer 在副作用前冲突。
- Fork 的源码调用存在一次不兼容迁移，但错误现在可检查，非法分支不会以半有效对象继续。
- 第三方 store 保持源码兼容，代价是除非实现或强制检查 `ThreadCoordinator`，否则其并发保证仍取决于自身实现。
- 本 ADR 不引入业务 codec、schema migration、Session 门面、完整快照树、GC、自动排队或模型/工具自动重试。

## 验证

SC-01 至 SC-11 覆盖深拷贝、Memory/File 冻结、Fork、恢复、CAS、跨进程 claim、崩溃接管、续租和 unknown outcome。交付验收命令为：

```powershell
go test ./core ./checkpoint ./agent ./vfs ./middleware -count=1
go test -race ./core ./checkpoint ./agent ./vfs -count=1
go test ./... -count=1
go vet ./...
git diff --check
```
