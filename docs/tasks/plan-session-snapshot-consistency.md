# 会话与快照一致性第一批修复 Implementation Plan

> **执行者说明：**使用 executing-plans 技能，按本文件的依赖顺序逐任务实施；用复选框记录实际完成情况。默认由一个执行者串行完成，每张任务卡完成后先验证，再进入下一张。本文中的新接口、文件和测试名均是待实施设计，不能当作当前代码已经具备的能力。

**Goal：**修复已经复现的快照引用污染、文件快照分支丢失、普通同线程运行丢上下文三个问题，同时保持既有恢复、审批、工具结果与原子提交的语义。

**Architecture：**在 core/checkpoint 存储边界建立统一的深拷贝与文件冻结机制；在 checkpoint 增加可选的线程写入协调能力，复用现有 Claim、fencing 和 journal。Agent 在实际启动时取得写入权并加载最新状态，由顶层 Run 统一持有、续租和释放；内部工作流共享该所有权。

**Tech Stack：**Go（go.mod 声明 1.25.0）、testing、现有 Memory/File checkpointer、JSONL journal、现有 mock 模型；不引入新的第三方依赖。

- 日期：2026-10-11。
- 状态：待实施；本次仅编写计划。
- 代码基线：02dbb6e430b7675e908cb534a63d4893b146f84e。执行前重新读取工作区，行号仅帮助定位，函数名和实际代码优先。
- 输入分析：[goagent-genkit-session-snapshot-review.md](D:/codeproject/mygo/goagent/docs/analysis/goagent-genkit-session-snapshot-review.md:25)。
- 工作目录：D:/codeproject/mygo/goagent。

## 一、范围与完成定义

本计划只覆盖分析报告“第一批：修复已复现的一致性问题”的 P0-A、P0-B、P0-C。

| 编号 | 已复现的问题 | 最终应有的行为 |
| --- | --- | --- |
| P0-A | 保存输入、读取结果、Fork 子对象的修改会改变历史快照 | 已提交快照的数据和可快照文件不再被外部引用改写；嵌套容器、消息字节、Pending/Pause 均覆盖 |
| P0-B | File.Load/Latest → Fork → Save 后，文件数量从 1 变为 0 | 文件快照能跨 store 重开、Fork、再次保存和 Agent 恢复；子分支写入不改变父快照 |
| P0-C | 两个 Stream 句柄在同一旧 head 上创建，后执行者的最新上下文缺少前者输入 | 惰性句柄在启动时加载 head；同一线程真正并发执行时只有一个 writer，另一方在模型和工具执行前得到明确冲突 |

**不纳入本批：**完整 ParentRef/快照树、Session 门面、RunRecord、任务查询 API、业务 schema 迁移、增量快照、分页、索引、GC、新数据库 adapter、自动排队、自动重试模型/工具调用。续租纳入本批，是将现有 30 秒租约推广到普通运行后的必要组成部分。

**完成门槛：**SC-01 至 SC-12 全部勾选；三个原始场景有回归测试；Memory/File 的对应契约均通过；原有 durability、HITL、工作流和文件恢复回归通过；新增失败路径不会在失败提交中确认 inbox 或写入工具完成结果。

## 二、实施前锁定的设计决定

### D1. 快照数据与活跃文件句柄分别处理

- State.Messages、Todos、KV 属于快照数据，保存和返回时都必须独立。
- core.CloneStateData 只克隆这些数据，保留 Files 句柄；这样 subagent 的活跃文件协作不会被误改。checkpoint 的冻结层负责把 Snapshottable 文件转成独立的 FileSnapshot，并清除该快照中的活跃 Files 句柄。
- 非 Snapshottable 的 Files 继续遵守外部管理契约：Memory 可以保留外部句柄，File 不持久化其内容。不可变保证不包括真实目录或远端服务的外部内容；不得声称时间旅行会回滚它们。
- 显式 WithRunFiles 始终覆盖恢复出来的文件。没有覆盖值时，有 FileSnapshot 就创建独立 InState；恢复失败必须返回错误。
- nil FileSnapshot 表示没有携带文件快照；非 nil 空 map 表示明确的空文件视图。不得把一个明确空快照错误回退到旧的非空句柄。

### D2. 深拷贝不改变已有 Memory 数值类型

不能用对整份 State 的 JSON 往返代替深拷贝，否则 int 会变成 float64，map[string]string 也可能变形，而且 Files 不参与 JSON。

本批支持：标量与命名标量、接口内的上述值、字符串键 map、slice、array、指向这些数据的 pointer、仅含可复制导出字段的普通 struct，以及 json.RawMessage、json.Number、time.Time。保留具体 Go 容器类型、数值类型和 nil 语义。time.Time 按值复制，消息中的 []byte/json.RawMessage 必须另分配。

先验证 JSON 可序列化性以拒绝循环引用、非有限浮点值、函数、channel 等；再递归复制具体类型。非白名单的自定义 JSON 编解码类型、含无法安全复制的私有可变字段的对象返回 core.ErrStateNotCloneable，并包含 KV 路径和具体类型；不偷偷共享，不引入 panic。该收紧属于明确的兼容性变化，SC-12 必须记录；未来可扩展 codec，但本批不设计通用注册系统。

Memory/File 的共同契约是内容隔离与文件恢复；File 经 JSON 后的数字归一化仍是既有行为，不要求本批消除所有 Go 动态类型差异。

### D3. Fork 的错误需要成为显式返回值

本计划采用以下签名调整：

```go
func Fork(parent *Checkpoint, newThreadID, newID string) (*Checkpoint, error)
```

这是本批一项明确的源码不兼容变化，必须更新所有仓库内调用者并在 SC-12 给出迁移示例。原因是非法状态、nil parent 和非法目标标识均不能通过无错误返回值可靠报告；不得用返回 nil 而无 error 或新增 panic 来掩盖失败。

Fork 继续表示复制会话状态到新线程：保留 ParentID，Revision 重置为 0，Pending/Pause 不继承；不复制 durable inbox、审批或工具执行账本。继续暂停执行仍使用 Resume。Fork 本身不保存；Save 时再次冻结输入。

### D4. 普通运行使用新的可选能力，复用现有恢复租约

不向已有 Checkpointer/Durable 接口直接增加必实现方法。新增以下完整契约，Memory/File 都实现：

```go
// D:/codeproject/mygo/goagent/checkpoint/coordinator.go
package checkpoint

import (
    "context"
    "errors"
    "time"
)

var (
    ErrRevisionConflict = errors.New("checkpoint: expected revision does not match head")
    ErrResumeRequired = errors.New("checkpoint: paused thread requires resume")
    ErrCoordinationUnavailable = errors.New("checkpoint: thread coordination unavailable")
    ErrCheckpointConflict = errors.New("checkpoint: checkpoint ID conflicts with existing content")
)

type BeginRunRequest struct {
    ThreadID string
    Owner    string
    Lease    time.Duration
}

type BeginRunResult struct {
    Claim      Claim
    Checkpoint *Checkpoint // 拿到所有权的同一临界区内读取并克隆；新线程为 nil
}

type ThreadCoordinator interface {
    Durable
    BeginRun(context.Context, BeginRunRequest) (BeginRunResult, error)
    RenewClaim(context.Context, Claim, time.Duration) (Claim, error)
}
```

- BeginRun 原子完成“检查当前 writer → 检查暂停状态 → 取得租约 → 读取 head”。不得在 agent 中先 Latest 再无条件 claim。
- 新线程允许不存在 checkpoint：Claim.Revision=0，PauseID 使用保留值 thread-root；后续继续复用现有 Claim 字段和 fencing。普通非暂停 head 使用 EffectivePauseID(head)。PauseID 只是此次 claim 的绑定身份，不表示凭空创建了审批暂停。
- 有活跃 writer：返回已有 ErrClaimConflict，模型与工具调用次数必须为 0。
- head 有 Pending 或 Pause：普通 BeginRun 返回 ErrResumeRequired，必须走 Resume。
- RenewClaim 只续当前、未过期且身份匹配的 claim，不创建新 owner，不增加 fencing，不增加 checkpoint Revision，不刷新 checkpoint head。
- Claim.Revision 是取得所有权时的锚点，不随提交改变；agent 另存 headRevision，用于每次提交 CAS。

### D5. 提交 CAS 与幂等重试并存

在 CommitRequest 增加：

```go
ExpectedRevision *uint64
```

nil 保留旧调用者行为；非 nil 表示 head 必须匹配该值，指向 0 表示当前没有已提交 checkpoint。协调后的普通运行和 Resume 每次都传非 nil。

新 checkpoint 的处理顺序：验证并克隆输入及工具结果 → 校验 claim → 比较 expected head → 准备 journal/状态 → 原子提交 checkpoint、ack、工具结果及必要租约更新。任何验证失败都不改变上述状态。

同 ID 重试必须先确认提交内容等价，忽略 store 分配的 Revision；把 checkpoint 数据、文件内容、ack 集合、工具结果以及请求中的 owner/fencing 身份纳入等价性检查。ExpectedRevision、Lease 时长和 ReleaseClaim 不是新一轮可执行指令：重复请求只查询原提交结果，不能重新 ack、完成工具、续租或释放。

- 等价重试返回原 Revision，不重复推进 head；File 在第一次 Sync 失败后仍须再次 Sync 成功才可确认。
- 同 ID 不同内容返回 ErrCheckpointConflict。
- receipt 不得返回另一个 owner 的当前 Claim；至多返回请求者仍有效的同一 claim，否则返回 nil。
- 为判断重试等价性可在既有 recordCommit 增加可选 request_digest；旧记录没有 digest 时使用可重建内容进行比较，无法确认等价就返回 ErrCheckpointConflict，不能视作成功。保持现有 version=1 可读性，不引入新的 record kind。

### D6. 一次顶层 Run 持有一个 writer

- Stream 只创建句柄、复制输入消息和 RunConfig，不加载 checkpoint、不追加用户消息、不申请租约；第一次 Iter/Wait 才准备执行。
- BeginRun 成功后使用返回的 head 构造运行状态，再应用 WithRunFiles，最后追加输入一次。
- 同线程两个提前创建的句柄依次执行时，B 必须看到 A 的已提交消息；真正重叠时 B 明确冲突，不自动重试、重放或排队。
- Sequential/Parallel/Loop/Plan/subagent/transfer 共用根运行的协调状态；子任务不能重新申请或提前释放该 writer。存储提交串行化，模型/工具计算不因此全局串行。
- Commit/Renew/Release 使用同一个运行级操作锁，并另有短时状态锁；统一锁顺序，不能拿状态锁等待模型、工具、事件消费者或长时间 I/O。
- 子 loop 的 releaseClaim=true 不再直接释放协调后的根运行租约。FinishRun 执行完后，由顶层停止并等待续租退出、释放 claim，再发送唯一终态事件并唤醒 Wait。不能仅依靠终态事件发出后的 defer 清理，否则下一次 Run 可能误撞上旧 claim。
- 清理 I/O 使用独立、有限时长的 context；释放失败应作为终态错误的一部分报告，不能等待无限长或假称清理成功。

### D7. 租约续期与第三方兼容边界

- 续租周期为有效租期的 1/3；有效租期必须大于 0，定时器周期最小为 1 纳秒以避免零间隔 panic。普通执行默认使用已有 DefaultLease，测试使用可控时钟/短租期。
- 续租失败后保存首个错误并取消根执行 context；之后禁止新的模型/工具分发和新的提交。不得自动重新获取租约继续副作用。
- 已经执行且不理会 context 的外部工具无法被强制回滚；继续保留既有 unknown outcome 和幂等处理，不宣称 exactly-once。
- Resume 保留当前“调用时申请 claim、可同步返回冲突”的行为。实际 drive 前必须校验/续租一次；等待启动期间已过期就失败，不偷偷重新申请，也不执行已批准工具。Cancel 在 drive 前也要释放已有 claim。
- Memory/File 自动启用协调。只实现旧 Checkpointer/Durable 的第三方 store 保持原路径，文档明确其没有新增保证。
- 在 agent/options.go 增加 WithThreadCoordinationRequired(bool)，默认 false；true 且 store 不实现 ThreadCoordinator 时，Agent.New 返回 ErrCoordinationUnavailable。不要把进程内 mutex 冒充跨实例保证。包装 Memory/File 的测试或业务 wrapper 若要提供新能力，必须显式转发该接口。

## 三、任务总表和依赖

| ID | 交付物 | 依赖 | 问题映射 |
| --- | --- | --- | --- |
| SC-01 | 可保留 Go 数据类型的状态/消息深拷贝原语 | 无 | P0-A |
| SC-02 | Memory 保存与读取的不可变快照边界 | SC-01 | P0-A |
| SC-03 | Memory/File 共用文件冻结与保存语义 | SC-02 | P0-A、P0-B |
| SC-04 | 有错误返回的完整 Fork | SC-03 | P0-A、P0-B |
| SC-05 | Agent 恢复与内部数据分支使用安全拷贝 | SC-04 | P0-A、P0-B |
| SC-06 | Memory 的 BeginRun、续租、提交 CAS | SC-02 | P0-C |
| SC-07 | File 的持久化协调与 CAS | SC-03、SC-06 | P0-C |
| SC-08 | 运行级 writer guard 与续租生命周期 | SC-06、SC-07 | P0-C |
| SC-09 | 接入惰性启动、Resume、工作流和兼容选项 | SC-05、SC-08 | P0-C |
| SC-10 | 模型/工具分发前的取消与所有权门禁 | SC-09 | P0-C |
| SC-11 | 跨进程、失败注入和完整契约回归 | SC-10 | P0-A、P0-B、P0-C |
| SC-12 | 兼容性说明、使用示例和验收归档 | SC-11 | 交付 |

推荐按 SC-01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 10 → 11 → 12 串行执行。多个任务会修改 snapshot.go、durable_memory.go、file.go、durable_runtime.go，不直接并行编辑这些文件。

## 四、任务卡

### TASK-SC-01：实现消息与状态数据深拷贝

**依赖：**无。**交付：**独立的 core 拷贝原语及单元测试。

**文件：**新增 D:/codeproject/mygo/goagent/core/clone.go、D:/codeproject/mygo/goagent/core/clone_test.go；修订 D:/codeproject/mygo/goagent/core/state.go 的数据契约注释。

**新增签名：**

```go
var ErrStateNotCloneable = errors.New("core: state value cannot be cloned")
func CloneMessage(m Message) (Message, error)
func CloneStateData(s State) (State, error)
```

- [x] 写 TestSnapshotConsistencyCloneNestedState：KV 同时含 int、map[string]string、map[string]any、[]any、[]byte、json.RawMessage；克隆后逐层修改，原值不变，类型保持。
- [x] 写 TestSnapshotConsistencyCloneMessageParts：覆盖 Text、Thinking、Image.Data、Video.Data、ToolCall.Args、递归 ToolResult.Content，以及现有 Part JSON 接受的形式；未知/不支持的 Part 返回可定位错误。
- [x] 写 TestSnapshotConsistencyCloneInvalidState：func、chan、循环 map、非有限浮点数、不可复制的私有字段及非白名单自定义 JSON 类型；errors.Is 能识别 ErrStateNotCloneable。
- [x] 实现 D2 的验证与递归 clone。map/slice/pointer 每层重新分配，nil 保持 nil，struct 导出字段递归；明确处理 time.Time/json.Number/json.RawMessage。不得把 marshaled JSON 当作 Memory 的最终状态表示。
- [x] 验证 CloneStateData 保留 Files 句柄；文件冻结在 SC-03 完成。
- [x] 执行以下命令并记录先失败后通过的证据：

```powershell
go test ./core -run '^TestSnapshotConsistencyClone' -count=1
go test ./core -count=1
```

**实施记录（2026-10-11）：**

- RED：定向测试首次运行以 build failed 结束，缺少 CloneStateData、CloneMessage 和 ErrStateNotCloneable，确认测试能捕获未实现能力。
- GREEN：定向测试通过；完整 core 测试通过；额外执行全仓 go test ./... -count=1 与 go vet ./core，均通过。

**验收：**可复制类型无别名；非法类型有错误而无 panic；既有消息 JSON 行为不变。

### TASK-SC-02：固化 Memory 快照并隔离所有读取出口

**依赖：**SC-01。**交付：**Memory 的数据快照不再泄漏内部引用。

**状态：**已完成（2026-10-11）。

**文件：**新增 D:/codeproject/mygo/goagent/checkpoint/snapshot.go、D:/codeproject/mygo/goagent/checkpoint/snapshot_contract_test.go；修改 D:/codeproject/mygo/goagent/checkpoint/memory.go、D:/codeproject/mygo/goagent/checkpoint/durable_memory.go、D:/codeproject/mygo/goagent/checkpoint/file.go。file.go 仅适配共享工具结果深拷贝及错误传播。

**内部函数：**

```go
func freezeCheckpoint(cp *Checkpoint) (*Checkpoint, error)
func cloneCheckpoint(cp *Checkpoint) (*Checkpoint, error)
func cloneToolExecution(ex ToolExecution) (ToolExecution, error)
```

- [x] 先落下本卡末尾的完整红灯测试。它只使用当前 API，当前 Memory 必须失败。
- [x] freezeCheckpoint 使用 CloneStateData，另拷贝 Pending 的调用及 Args、Pause、FileSnapshot 内每个 []byte；SC-03 再加入活跃 Files 捕获规则。
- [x] commitLocked 在推进 revision、ack inbox、登记工具完成结果之前完成所有可能失败的验证和克隆。存入独立 checkpoint；保留现有 Save/Commit 向调用者回填 Revision 的行为。
- [x] Load/Latest/History 为每个返回节点提供独立 clone，History 不能只复制 []*Checkpoint 的外层 slice。
- [x] 在同一提交内保存的 ToolExecution.Result 使用 CloneMessage；修正现有 cloneExecution 的浅层结果引用，并调整其错误传播。失败的工具结果复制不能留下半个 checkpoint/ack。
- [x] 添加 TestSnapshotConsistencyMemoryReads、TestSnapshotConsistencyPendingPause、TestSnapshotConsistencyCommitCloneFailureAtomic、TestSnapshotConsistencyToolResultOwnership，分别覆盖读取出口、审批数据、原子失败和工具结果别名。
- [x] 验证后形成独立提交建议：fix(checkpoint): isolate in-memory snapshot data。只有任务全部通过才记录该任务完成。

```go
// D:/codeproject/mygo/goagent/checkpoint/snapshot_contract_test.go
package checkpoint_test

import (
    "context"
    "testing"

    "github.com/jiujuan/goagent/checkpoint"
    "github.com/jiujuan/goagent/core"
)

func TestSnapshotConsistencyMemorySave(t *testing.T) {
    ctx := context.Background()
    store := checkpoint.NewMemory()
    input := &checkpoint.Checkpoint{
        ID: "cp1", ThreadID: "thread1",
        State: core.State{KV: map[string]any{
            "nested": map[string]any{"value": "before"},
        }},
    }
    if err := store.Save(ctx, input); err != nil { t.Fatal(err) }
    input.State.KV["nested"].(map[string]any)["value"] = "changed"
    first, err := store.Latest(ctx, "thread1")
    if err != nil { t.Fatal(err) }
    if first == nil { t.Fatal("missing checkpoint") }
    if got := first.State.KV["nested"].(map[string]any)["value"]; got != "before" {
        t.Fatalf("saved snapshot changed through input: %v", got)
    }
    first.State.KV["nested"].(map[string]any)["value"] = "changed-via-read"
    second, err := store.Latest(ctx, "thread1")
    if err != nil { t.Fatal(err) }
    if second == nil { t.Fatal("missing checkpoint on second read") }
    if got := second.State.KV["nested"].(map[string]any)["value"]; got != "before" {
        t.Fatalf("saved snapshot changed through read: %v", got)
    }
}
```

```powershell
go test ./checkpoint -run '^TestSnapshotConsistency(Memory|Pending|CommitClone|ToolResult)' -count=1
go test ./checkpoint ./agent ./middleware -count=1
```

**实施记录（2026-10-11）：**

- RED：新增的五组定向契约测试全部失败，分别复现写入别名、读取别名、Pending/Pause 别名、克隆失败后仍提交、工具结果别名。
- GREEN：定向契约测试与 checkpoint/agent/middleware 回归测试通过；额外执行全仓 go test ./... -count=1 与 go vet ./checkpoint，均通过。
- 范围：Memory 保存现有 FileSnapshot 时会复制 map 与每个 []byte；State.Files 活跃句柄仍按 CloneStateData 约定保留，文件内容捕获留给 SC-03。

**验收：**写入后改 input、读取后改 output 都不能更改 store；错误不影响 revision、inbox、tool ledger；尚未接入文件捕获前不宣称 Files 已隔离。

### TASK-SC-03：统一文件快照捕获、保存和空值语义

**依赖：**SC-02。**交付：**Memory/File 都能保存冻结文件视图，FileSnapshot 重新保存不丢文件。

**状态：**已完成（2026-10-12）。

**文件：**修改 D:/codeproject/mygo/goagent/checkpoint/snapshot.go、D:/codeproject/mygo/goagent/checkpoint/file.go、D:/codeproject/mygo/goagent/checkpoint/checkpoint.go；新增 D:/codeproject/mygo/goagent/checkpoint/snapshot_files_contract_test.go；修订 D:/codeproject/mygo/goagent/vfs/instate.go 的持久化说明。

- [x] 写 TestSnapshotConsistencyFreezeFiles，保存 InState 后改写原 InState，再读取旧 checkpoint，旧内容保持。
- [x] 写 TestSnapshotConsistencyResaveFileSnapshot：File.Load 返回 State.Files=nil、FileSnapshot 非空，直接改 ID/ThreadID 后保存，重开 store 后仍可读取文件。先单独验证此路径，避免只修改 Fork 掩盖保存缺口。
- [x] 定义 freezeCheckpoint 的来源优先级：存在可快照的活跃 State.Files 时捕获它；否则采用传入的非 nil FileSnapshot；两者都无则不制造文件内容。可快照句柄捕获后从快照 State 中清除；对外部管理句柄按 D1 处理。
- [x] buildCheckpointRecord 以已冻结的 FileSnapshot 构建路径→hash 索引并写 blob，不再仅依赖 State.Files。保持 blob 内容去重、短写恢复、Sync 失败后的重试规则。
- [x] 保留空 map 与 nil 的语义：checkpointRecord.Files 区分缺失索引与显式空索引；若现有 omitempty 会抹平差异，修改该字段编码使新记录的空索引可往返。旧记录未携带索引继续按 nil 读取。
- [x] 添加 TestSnapshotConsistencyEmptyFiles、TestSnapshotConsistencyFileReadOwnership、TestSnapshotConsistencyExternalFiles；验证字节切片读取隔离以及外部文件不被伪装为已持久化。

```powershell
go test ./checkpoint -run '^TestSnapshotConsistency(FreezeFiles|ResaveFileSnapshot|EmptyFiles|FileReadOwnership|ExternalFiles)' -count=1
go test ./checkpoint ./vfs -count=1
```

**实施记录（2026-10-12）：**

- RED：定向测试复现三处缺口：Memory 保存后仍返回活跃 InState；FileSnapshot 改 ID/ThreadID 重存后文件为空；Memory/File 都把显式空文件视图读成 nil。
- GREEN：freezeCheckpoint 优先捕获活跃 Snapshottable，否则保留传入的 FileSnapshot；File 仅从冻结后的 FileSnapshot 构建 blob 索引；checkpointRecord.Files 不再使用 omitempty，rebuild 按 nil 判断是否缺失索引。
- 验证：定向契约测试、go test ./checkpoint ./vfs -count=1、go test ./agent ./middleware -count=1、go vet ./checkpoint ./vfs 与全仓 go test ./... -count=1 均通过。
- 语义：Snapshottable 文件保存后与活跃句柄隔离；Memory 继续保留非 Snapshottable 的外部句柄，File 不把外部句柄伪装成可移植快照；非 nil 空 map 可跨 File 重开往返。

**验收：**FileSnapshot→Save 可往返；修改原文件句柄或读取的字节不会污染旧快照；既有 blob 去重、legacy JSON、truncated tail 与 Sync 故障测试通过。

### TASK-SC-04：让 Fork 完整复制数据和冻结文件

**依赖：**SC-03。**交付：**Fork 的三个复现场景通过，错误可检查。

**状态：**已完成（2026-10-12）。

**文件：**修改 D:/codeproject/mygo/goagent/checkpoint/branch.go、D:/codeproject/mygo/goagent/checkpoint/memory_test.go；新增 D:/codeproject/mygo/goagent/checkpoint/fork_consistency_test.go。

- [x] 先按旧签名运行一个 File.Load→Fork→Save 红灯测试，确认当前父 1 文件/子 0 文件的结果；随后把测试迁移到 D3 的新签名。
- [x] Fork 验证 parent 非 nil、目标 ThreadID 合法且不同于源线程、newID 非空且不同于 parent.ID；错误不返回半成品、不修改 parent。
- [x] 通过 freezeCheckpoint 取得独立副本，设置新 ID/ThreadID/ParentID，Revision=0，Pending=nil，Pause=nil。文件数据保持为独立 FileSnapshot，交给 SC-03 的保存路径处理。
- [x] 添加 TestSnapshotConsistencyForkNested、TestSnapshotConsistencyForkFiles、TestSnapshotConsistencyForkInvalid、TestSnapshotConsistencyForkDropsPause；嵌套 KV、Message.Parts、Args/字节、FileSnapshot 修改均不得影响父节点。
- [x] rg 搜索所有调用者并一次更新；不能只改 memory_test.go。将旧签名迁移列入 SC-12。

```powershell
rg -n 'checkpoint\.Fork\(|\bFork\(' --glob '*.go' .
go test ./checkpoint -run '^TestSnapshotConsistencyFork' -count=1
go test ./checkpoint -count=1
```

**实施记录（2026-10-12）：**

- RED：旧签名下的 File.Load→Fork→Save 测试稳定复现 `forked files = map[]`；迁移测试到新签名后，所有调用点因返回值数量不匹配而按预期编译失败。
- GREEN：Fork 先验证 parent、目标线程和新 checkpoint ID，再通过 freezeCheckpoint 克隆完整数据及文件视图；子节点保留 Step/ParentID，重置 Revision，并清除 Pending/Pause。
- 覆盖：嵌套 KV、KV 字节、Message 图片字节、ToolCall Args、Todos、FileSnapshot 均与父节点隔离；活跃 Snapshottable 在 Fork 时冻结，File store 重开后父子各保留一个独立文件。
- 验证：全仓搜索仅发现 checkpoint/memory_test.go 这一处旧签名调用；定向 Fork 契约测试、go test ./checkpoint -count=1 与 go vet ./checkpoint 均通过。源码迁移说明仍由 SC-12 交付。

**验收：**父 1 文件/子 1 文件，子写入不影响父；分支不会继承父暂停审批；错误由返回值传播。

### TASK-SC-05：统一 Agent 恢复和内部数据分支的拷贝

**依赖：**SC-04。**交付：**外部 store 返回共享对象时，Agent 也不改写其历史状态。

**状态：**已完成（2026-10-12）。

**文件：**修改 D:/codeproject/mygo/goagent/agent/agent.go、D:/codeproject/mygo/goagent/agent/hitl.go、D:/codeproject/mygo/goagent/agent/runtime.go、D:/codeproject/mygo/goagent/agent/workflow.go；新增 D:/codeproject/mygo/goagent/agent/state_isolation_test.go。

- [x] 添加 TestSnapshotConsistencyAgentRestore，使用故意返回共享 checkpoint 指针的只读测试 store，验证 Agent 在独立运行状态上追加消息及修改嵌套数据。
- [x] 将 agent.cloneState 改为使用 core.CloneStateData 并传播 error；restore/Resume/forBranch 的所有调用点都处理错误。forBranch 返回 (*RunContext,error)，parallelRunner 在启动分支前完成所有分支的克隆验证，失败时不能只启动一半分支。
- [x] applyFileSnapshot 改为返回 error；恢复优先级遵守 D1。WithRunFiles 覆盖时不先还原一个随后会丢弃的 snapshot，也不因被覆盖的无效 snapshot 产生伪失败。
- [x] 添加 TestSnapshotConsistencyAgentForkFiles，完成保存源节点→重开 store→Fork→保存分支→Agent.OnThread 分支读取文件的闭环。
- [x] 添加 TestSnapshotConsistencyParallelNestedState 和 TestSnapshotConsistencySubagentFilesShared：内部 Parallel 的数据隔离仍成立，subRun/deeper 有意共享的活跃 Files 仍共享，不把运行时协作误变成文件隔离。

```powershell
go test ./agent -run '^TestSnapshotConsistency(Agent|ParallelNested|SubagentFiles)' -count=1
go test ./agent ./checkpoint ./middleware -count=1
```

**实施记录（2026-10-12）：**

- RED：定向测试稳定复现 Agent restore 污染共享 checkpoint、克隆失败未传播、非 nil 空文件快照未替换旧句柄，以及 Parallel 分支共享嵌套 KV 和克隆失败后仍启动分支。
- GREEN：cloneState 统一委托 core.CloneStateData；restore、Resume 和 forBranch 传播错误，Resume 在取得 Durable claim 后遇到克隆/还原失败会释放 claim；Parallel 在启动任何 goroutine 前完成全部分支克隆。
- 文件语义：显式 WithRunFiles 直接覆盖恢复文件并跳过 FileSnapshot 还原；没有覆盖时，任何非 nil FileSnapshot（含空 map）都生成新的 InState 并替换旧句柄；subRun/deeper 继续共享当前活跃 Files。
- 验证：定向测试、go test ./agent ./checkpoint ./middleware -count=1、go test ./... -count=1、定向 go test -race 与 go vet ./... 均通过。

**验收：**三个原始问题中的 P0-A/P0-B 完成；第三方 store 的共享读取不会污染运行历史；恢复错误可见，内部工作流原语义通过回归。

**检查点 A：**

- [x] SC-01～05 的新增用例和既有相关测试均通过。
- [x] 输入、读取、Fork、Agent restore 四处不再使用仅复制外层容器作为完整隔离措施。
- [x] File 的冻结快照路径可以独立于活跃文件句柄工作。

### TASK-SC-06：实现 Memory 线程协调、续租与 CAS

**依赖：**SC-02；按推荐顺序在 SC-05 后执行。**交付：**新的可选接口在 Memory 上有完整契约测试。

**状态：**已完成（2026-10-12）。

**文件：**新增 D:/codeproject/mygo/goagent/checkpoint/coordinator.go、D:/codeproject/mygo/goagent/checkpoint/coordinator_memory.go、D:/codeproject/mygo/goagent/checkpoint/coordinator_contract_test.go；修改 D:/codeproject/mygo/goagent/checkpoint/durable.go、D:/codeproject/mygo/goagent/checkpoint/durable_memory.go。

- [x] 按 D4/D5 定义接口、错误和 ExpectedRevision，增加编译期接口断言；旧 Durable 实现仍可编译。
- [x] 在 Memory.mu 内实现 BeginRun：先检查未过期 owner；检查暂停；克隆 head；在所有可失败步骤成功后登记 claim/fencing。相同 owner 的幂等 BeginRun 不重复推进 fencing；每次真正启动必须使用新 owner ID。
- [x] 在同一租约状态上实现 RenewClaim；过期、owner/pause/revision/fencing 不符返回 ErrStaleFencingToken，不能把过期 lease 复活。
- [x] 实现 ExpectedRevision 校验，新增重复请求摘要/等价性检查；摘要在发布提交结果之前算好，错误不修改状态；保存“提交请求摘要”与原 Revision 供后续幂等确认。
- [x] 按下表编写共享契约测试，先让 Memory 通过。测试中的短租期使用可注入时钟或显式控制到期，避免用大段 sleep 猜测调度。

| 测试名 | 必须断言 |
| --- | --- |
| TestSnapshotConsistencyBeginRunEmpty | 无历史也能取得 revision=0 的所有权；第二 owner 冲突 |
| TestSnapshotConsistencyBeginRunHead | 返回的 head 与 claim 绑定 revision 相同，返回对象可独立修改 |
| TestSnapshotConsistencyBeginRunPaused | Pending/Pause 阻止普通运行；已有 Resume Claim 仍可取得合法权限 |
| TestSnapshotConsistencyRenewClaim | 续期不推进 head/revision/fencing；旧 token 或过期 token 不能续期 |
| TestSnapshotConsistencyExpectedRevision | 不匹配时 checkpoint、inbox ack、工具结果均不改变 |
| TestSnapshotConsistencyCommitRetry | 等价请求只提交一次；不同内容同 ID 报冲突；不泄漏其他 owner 的 claim |
| TestSnapshotConsistencyStaleWriter | 新 owner 接管后旧 owner 的 Save/Commit/ClaimTool/Release 均不能更改新 owner 状态 |

```powershell
go test ./checkpoint -run '^TestSnapshotConsistency(BeginRun|RenewClaim|ExpectedRevision|CommitRetry|StaleWriter)' -count=1
go test ./checkpoint -count=1
```

**实施记录（2026-10-12）：**

- RED：共享契约测试首次运行因缺少 BeginRun 等协调 API 而编译失败，确认测试先于实现捕获能力缺口。
- GREEN：Memory 在同一互斥区内完成原子 BeginRun、可注入时钟续租、ExpectedRevision CAS、fencing 接管与提交幂等；同 ID 等价重试只返回原 Revision，不重复 ack、工具完成、续租或释放，也不泄漏其他 owner 的 claim。
- 摘要语义：覆盖冻结 checkpoint、文件、ack 集合、工具执行及 claim 身份，忽略 store Revision、ExpectedRevision、Lease 和 ReleaseClaim；工具执行按 InvocationID 的最终可见值规范化后排序。截断 ToolCall Args 作为原始字节参与摘要，不收紧既有可保存消息的兼容边界。
- 验证：定向契约测试、go test ./checkpoint -count=1、go test ./... -count=1、定向 go test -race、go vet ./... 与 git diff --check 均通过。
- 范围：提交摘要和协调状态当前仅由 Memory 保存；File journal 的跨实例持久化、重开恢复与 Sync 失败确认属于 SC-07，尚未标记完成。

**验收：**接口契约、空线程、CAS、续租、幂等和 stale owner 全部通过；现有恢复 claim 行为没有被新的普通 writer 绕过。

### TASK-SC-07：将相同协调契约落到 File journal

**依赖：**SC-03、SC-06。**交付：**多个 File 实例及进程使用同一租约和提交真相。

**状态：**已完成（2026-10-12）。

**文件：**新增 D:/codeproject/mygo/goagent/checkpoint/coordinator_file.go；修改 D:/codeproject/mygo/goagent/checkpoint/file.go、D:/codeproject/mygo/goagent/checkpoint/coordinator_contract_test.go、D:/codeproject/mygo/goagent/checkpoint/file_recovery_test.go。

- [x] 将 SC-06 的共享测试扩展到 File，每个测试使用独立 TempDir；不能只测一个 Memory 后假定 File 等价。
- [x] BeginRun/RenewClaim 必须经现有 withThread，在同一跨进程临界区内扫描、验证、追加 recordClaim 并 Sync；失败不能向调用者返回成功的 grant。
- [x] Commit 的 CAS 与 request_digest 检查也在 withThread 内完成，包含旧记录兼容路径。文件快照摘要基于冻结后的内容，不能因 map 顺序或 store 分配的 Revision 变化误判。
- [x] 复用 recordClaim/recordReleaseClaim/recordCommit 和既有 version，不新增与 journal 不一致的旁路锁表。重开 File 后恢复 fencing、claim、checkpoint head 和请求摘要。
- [x] 添加 TestSnapshotConsistencyFileTwoInstances、TestSnapshotConsistencyFileRestartClaim、TestSnapshotConsistencyFileClaimSyncFailure、TestSnapshotConsistencyFileCommitSyncRetry。通过现有 fileOps 注入写入和 Sync 失败；对已经写到 journal 但未获确认的结果，只允许安全的再次读取/确认，不能声称已物理回滚。

```powershell
go test ./checkpoint -run '^TestSnapshotConsistency' -count=1
go test ./checkpoint -run 'TestFile|TestExpiredOwner|TestInboxHistory' -count=1
```

**实施记录（2026-10-12）：**

- RED：将 SC-06 的七组契约同时运行在 Memory/File 后，File 因缺少 BeginRun、RenewClaim 与可控租约时钟而编译失败；四个 File 专项用例同时锁定跨实例竞争、重开恢复及 Sync 故障语义。
- GREEN：File 的 BeginRun/RenewClaim 在 withThread 临界区内完成扫描、校验、recordClaim 追加与 Sync；同目录的不同 File 实例共享 journal 中的 claim 和 fencing 真相。
- Commit：冻结 checkpoint 后计算请求摘要，在同一 withThread 内完成同 ID 摘要比对、ExpectedRevision CAS、追加和 Sync。request_digest 作为 version 1 commit 的可选字段持久化；旧 commit 会从 checkpoint、blob、ack、claim 与工具记录尝试重建，无法确认等价时保守返回 ErrCheckpointConflict。
- 故障确认：claim/commit 的首次 Sync 错误不会返回成功；若完整记录已写入，后续扫描恢复它。等价 Commit 重试只重新 Sync 并返回原 Revision，不重复追加 commit、ack 或工具完成记录。
- 验证：共享协调契约、File 专项故障用例、既有 File/ExpiredOwner/InboxHistory 测试、checkpoint 与全仓测试、定向 go test -race、go vet ./... 和 git diff --check 均通过。

**验收：**两实例竞争只有一个 owner；重启不重置 fencing；Sync 故障不产生虚假成功、不重复 ack/工具执行登记；既有尾部修复测试通过。

### TASK-SC-08：实现运行级 writer guard

**依赖：**SC-06、SC-07。**交付：**一个供普通 Run/Resume/内部工作流共用的所有权管理实现，本卡先单独测试，不改变 Agent 启动入口。

**状态：**已完成（2026-10-12）。

**文件：**新增 D:/codeproject/mygo/goagent/agent/write_guard.go、D:/codeproject/mygo/goagent/agent/write_guard_test.go；在 D:/codeproject/mygo/goagent/agent/durable_runtime.go 的 durableRunState 中增加 guard 引用。

**内部模型：**

```go
type writeGuard struct {
    store        checkpoint.ThreadCoordinator
    opMu         sync.Mutex // Commit、Renew、Release 的操作顺序
    mu           sync.Mutex // 字段的短时访问；不在该锁下等待 I/O
    claim        checkpoint.Claim
    headRevision uint64
    lease        time.Duration
    renewCancel  context.CancelFunc
    renewDone    chan struct{}
    firstErr     error
    started      bool
    stopped      bool
}

// 所有方法是 agent 包内部使用；具体字段私有。
// newWriteGuard 的 claim/headRevision 来自 BeginRun 或已验证的 Resume。
func newWriteGuard(store checkpoint.ThreadCoordinator, claim checkpoint.Claim, headRevision uint64, lease time.Duration) *writeGuard
func (g *writeGuard) start(ctx context.Context, cancel context.CancelFunc) error
func (g *writeGuard) commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error)
func (g *writeGuard) err() error
func (g *writeGuard) stopAndRelease(ctx context.Context) error
```

- [x] start 先做一次身份校验/续租，成功才启动后台 ticker；已经过期的 prepared Resume 失败而不重新申请。
- [x] commit 在操作锁内读取 headRevision，设置 ExpectedRevision/Claim，禁用子步骤直接 ReleaseClaim，成功后更新 headRevision 和自己的 claim；失败时不推进 headRevision。
- [x] 续租与提交、释放共享操作锁，避免续租结果用旧 ExpiresAt 覆盖新状态。后台续租失败只记录首个错误，取消根 context；之后 commit 立即返回该错误。
- [x] stopAndRelease 幂等；先停止 ticker 并等待退出，再在有限 context 内释放；即使根 context 已取消也能完成清理。不在持有操作锁时等待需要该锁才能退出的 goroutine。
- [x] 添加 TestSnapshotConsistencyGuardRenewal、TestSnapshotConsistencyGuardLostLease、TestSnapshotConsistencyGuardCommitSerial、TestSnapshotConsistencyGuardStop；用 fake coordinator、屏障 channel 和可控 ticker 验证顺序，无真实模型调用。
- [x] 测试 commit 与 renew 同时到达、stop 与 renew 同时到达、初次 renew 失败、释放失败、重复 stop、取消后的清理、后台 goroutine 退出。

```powershell
go test ./agent -run '^TestSnapshotConsistencyGuard' -count=1
go test -race ./agent -run '^TestSnapshotConsistencyGuard' -count=1
```

**实施记录（2026-10-12）：**

- RED：四组 guard 测试首次运行因缺少 newWriteGuard 和 writeGuardTicker 而编译失败；补充的取消场景还复现了初次续租因 context 取消失败后仍可提交的问题。
- GREEN：guard 在首次身份续租成功后按半个 lease 周期启动后台续租；Commit、RenewClaim、ReleaseClaim 通过同一操作锁串行，提交成功才推进 head revision 和 claim，所有初次续租失败及后台续租失败都会阻止后续提交。
- 停止顺序：stopAndRelease 先标记停止并取消续租，等待 goroutine 退出后使用独立的五秒清理 context 释放 claim；sync.Once 保证并发或重复停止只释放一次。
- 验证：定向 guard 测试、定向 go test -race、agent 与全仓测试、go vet ./... 和 git diff --check 均通过；本卡没有接入 Run、Resume 或工作流，生命周期接入保留给 SC-09。

**验收：**无重复续租/释放，无泄漏；提交串行推进 revision；失败后不再允许新提交；lease 的所有权与 head revision 始终分开。

### TASK-SC-09：接入惰性 Run、Resume 与工作流

**依赖：**SC-05、SC-08。**交付：**修复普通同线程运行的上下文遗漏，保留可解释的兼容路径。

**文件：**新增 D:/codeproject/mygo/goagent/agent/run_prepare.go、D:/codeproject/mygo/goagent/agent/thread_consistency_test.go；修改 D:/codeproject/mygo/goagent/agent/agent.go、D:/codeproject/mygo/goagent/agent/run.go、D:/codeproject/mygo/goagent/agent/hitl.go、D:/codeproject/mygo/goagent/agent/options.go、D:/codeproject/mygo/goagent/agent/durable_runtime.go。

本卡按下面三个小步骤顺序实施，不一次改完再定位失败：

**09-A：准备执行与普通运行。**

- [ ] 先加入本卡末尾的原始场景红灯测试；观察 request-A 丢失。
- [ ] Stream 保存输入和配置，由 run_prepare.go 在 drive 中完成“BeginRun→恢复→覆盖 Files→追加输入”。只实现旧 store 的兼容路径也推迟到 drive 才 Latest，以修复提前创建句柄的顺序执行问题。
- [ ] WithMessage 的消息/字节在创建句柄时复制，避免等待启动时调用者再改写输入。Steer、Deliver、动态工具等已有 run-scoped 队列保持在同一个 RunContext 上，不因替换状态而丢失。
- [ ] 添加 WithThreadCoordinationRequired(bool)，Agent.New 对缺少能力的 store 做明确检查；使用 fmt.Errorf 包装时保留 errors.Is。

**09-B：提交及顶层生命周期。**

- [ ] commitCheckpoint 有 guard 时走 guard.commit；ack/工具结果的采集与清理保持原子语义。多个子分支的 commit 不能从同一过期 headRevision 并行提交，也不能互相清掉未包含在本次提交中的 ack/result。
- [ ] 只在顶层 drive 启动/结束 guard。嵌套上下文继续共享 durableRunState；子步骤的终态 checkpoint 不释放根 writer。
- [ ] 将最终清理放到发布 RunDone/RunFailed/Interrupted 之前；FinishRun 仍只在现有规定的终态触发。清理失败加入最终错误，整个 Run 仍只发送一个终态事件。
- [ ] 加入 TestSnapshotConsistencyThreadOverlap：A 的 mock 在屏障处停住，B 的 Iter/Wait 得 ErrClaimConflict，B 的模型和工具计数均为 0；释放 A 后新 C 可继续。
- [ ] 加入 TestSnapshotConsistencyDifferentThreads：不同 Thread 的 mock 能同时进入屏障；不允许一个全局 agent mutex 串行所有会话。

**09-C：Resume 与嵌套回归。**

- [ ] Resume 保留现有同步 claim 和审批绑定；drive 时将该 claim 交给 guard，续租失败在 approved tool 之前退出。Cancel-before-drive 会释放 prepared claim，并保持与 drive 的竞争安全。
- [ ] BeginRun 拒绝 pending/pause，覆盖 TestSnapshotConsistencyPausedThreadRequiresResume。
- [ ] 添加 TestSnapshotConsistencyNestedOwnership，覆盖 Sequential、Parallel、Loop、Plan、AsTool/transfer。用后续阶段的屏障证明前一个子步骤结束后 root writer 仍被持有；整个 root 完成后立即可重新开始。
- [ ] 添加 TestSnapshotConsistencyLazyCancel、TestSnapshotConsistencyResumeExpiredBeforeStart、TestSnapshotConsistencyLegacyStore、TestSnapshotConsistencyRequireCoordinator。
- [ ] 更新确实需要模拟第一方能力的 store wrapper，使其转发新接口；保留明确只测旧 Checkpointer 的 wrapper，防止全部测试都意外绕过兼容路径。

```go
// D:/codeproject/mygo/goagent/agent/thread_consistency_test.go
package agent_test

import (
    "context"
    "testing"

    "github.com/jiujuan/goagent/agent"
    "github.com/jiujuan/goagent/checkpoint"
    "github.com/jiujuan/goagent/core"
    "github.com/jiujuan/goagent/llm"
    "github.com/jiujuan/goagent/llm/mock"
)

func TestSnapshotConsistencyLazyRunsUseCurrentHead(t *testing.T) {
    ctx := context.Background()
    store := checkpoint.NewMemory()
    a, err := agent.New(
        agent.WithCheckpointer(store),
        agent.WithModel(mock.New("test", func(*llm.Request) *llm.Response {
            return mock.Text("ok")
        })),
    )
    if err != nil { t.Fatal(err) }
    if _, err = a.Run(ctx, "seed", agent.OnThread("same")); err != nil { t.Fatal(err) }
    runA := a.Stream(ctx, "request-A", agent.OnThread("same"))
    runB := a.Stream(ctx, "request-B", agent.OnThread("same"))
    if _, err = runA.Wait(); err != nil { t.Fatal(err) }
    if _, err = runB.Wait(); err != nil { t.Fatal(err) }
    cp, err := store.Latest(ctx, "same")
    if err != nil { t.Fatal(err) }
    if cp == nil { t.Fatal("missing latest checkpoint") }
    var got []string
    for _, message := range cp.State.Messages {
        if message.Role == core.RoleUser { got = append(got, message.Text()) }
    }
    want := []string{"seed", "request-A", "request-B"}
    if len(got) != len(want) { t.Fatalf("user messages = %v, want %v", got, want) }
    for i := range want {
        if got[i] != want[i] { t.Fatalf("user messages = %v, want %v", got, want) }
    }
}
```

```powershell
go test ./agent -run '^TestSnapshotConsistency' -count=1
go test ./agent ./checkpoint ./middleware -count=1
go test -race ./agent ./checkpoint -run '^TestSnapshotConsistency' -count=1
```

**验收：**依次启动 A/B 可看到完整三条输入；真正重叠时拒绝第二 writer 且无模型/工具副作用；不同线程并发；暂停不被普通 Run 绕过；根生命周期和 legacy store 路径均通过。

### TASK-SC-10：在模型和工具分发前检查取消及所有权

**依赖：**SC-09。**交付：**续租失败的根 context 不会继续启动新的模型/工具调用；不依赖第三方模型或工具自觉检查 context。

**文件：**新增 D:/codeproject/mygo/goagent/agent/dispatch_guard.go、D:/codeproject/mygo/goagent/agent/dispatch_guard_test.go；修改 D:/codeproject/mygo/goagent/agent/loop.go、D:/codeproject/mygo/goagent/agent/exectools.go。

**内部签名：**

```go
func (rc *RunContext) checkWriteAccess() error
```

- [ ] checkWriteAccess 检查根 context、guard 首个错误和当前已知租约到期时间；没有 guard 的旧 store 仍检查 context。检查只持短时状态锁，不在模型/工具调用期间持有 guard 操作锁。
- [ ] 在 streamModel 调用 provider.Generate 前检查；失败直接返回该错误，不调用 provider。工具批次的每个实际分发入口以及 callOne 中真正调用 handler 前检查，覆盖串行、并行和 Resume approved batch。
- [ ] 工具已取得 ClaimTool 却在 handler 前发现失去所有权时，不执行 handler，也不伪造完成结果；沿用保守的 unknown outcome/未完成账本语义，不在旧 token 下修补结果。
- [ ] 验证租约失败发生在两个串行工具之间、模型返回与工具启动之间、并行批次尚未分发的调用之间，后续调用计数不再增加。已通过门禁并进入执行的调用视为 in-flight，不能承诺强制停止或回滚外部副作用。
- [ ] 添加 TestSnapshotConsistencyDispatchAfterLeaseLoss、TestSnapshotConsistencyDispatchAfterCancel、TestSnapshotConsistencyResumeDispatchGuard；使用故意忽略 context 的 mock/handler，证明拒绝由框架实施，错误通过 errors.Is 保留。

```powershell
go test ./agent -run '^TestSnapshotConsistency(Dispatch|ResumeDispatch)' -count=1
go test -race ./agent -run '^TestSnapshotConsistency(Dispatch|ResumeDispatch)' -count=1
go test ./agent -count=1
```

**验收：**已知租约失效、租约已到期或根 context 取消后，没有新的模型/工具分发；正在执行的工具继续遵守既有取消和 unknown outcome 契约。

**检查点 B：**

- [ ] SC-06～10 验证了同一个协议的 Memory、File、Agent 三层行为。
- [ ] 默认第一方 store 不需要额外选项即可得到线程协调保证。
- [ ] 独立续租不会因模型/工具执行超过 30 秒而自然停止，失去租约也不会被重新申请掩盖。
- [ ] Wait 返回后不会因旧运行尚未释放而产生无意义的 claim 冲突。

### TASK-SC-11：跨进程与故障注入验收

**依赖：**SC-10。**交付：**生产相关边界有可重复的证据，不能用“两实例”代替真正跨进程验证。

**文件：**新增 D:/codeproject/mygo/goagent/checkpoint/coordinator_process_test.go、D:/codeproject/mygo/goagent/agent/thread_recovery_test.go；扩展 D:/codeproject/mygo/goagent/checkpoint/snapshot_contract_test.go、D:/codeproject/mygo/goagent/checkpoint/file_recovery_test.go。

- [ ] TestSnapshotConsistencyFileCrossProcess：使用 os.Executable、exec.CommandContext 和测试 helper 子进程。父子通过 stdin/stdout 管道或明确屏障文件协调，不用任意 sleep 判断是否已拿锁；一个进程持有 claim 时另一个冲突。
- [ ] TestSnapshotConsistencyFileOwnerCrash：helper 已确认拿到 lease 后终止该 helper；确认租约到期才能接管，fencing 增加，旧 token 不能提交。测试只结束自己创建的子进程，所有数据位于 t.TempDir。
- [ ] TestSnapshotConsistencyResumeLongCall：受控 provider/tool 调用期间跨过多个短租期，guard 有续租记录，最终只执行一次并成功提交；另测续租失败后不会开始下一次调用。
- [ ] TestSnapshotConsistencyClaimedToolOnLeaseLoss：工具开始后租约失效，恢复不能默认重复调用；沿用现有 unknown outcome 行为，并断言调用计数。
- [ ] 对 clone 失败、CAS 冲突、Claim/Renew 写失败、短写、Sync 失败逐项检查 checkpoint head、inbox、工具执行记录、owner/fencing；持久化返回不确定的情况以重开 journal 读取结果为准。
- [ ] 为 Memory/File 运行相同的内容隔离、文件 Fork 和 CAS 测试；File 额外重开实例及子进程读回，不能只验证同一个对象。
- [ ] 运行分层检查；全仓已有失败单独记录基线和归属，不能通过关闭测试来达成验收，也不顺手改动不相关模块。

```powershell
go test ./core ./checkpoint ./agent ./vfs ./middleware -count=1
go test -race ./core ./checkpoint ./agent ./vfs -count=1
go test ./... -count=1
```

Windows 的 -race 需要可用的 cgo/C 编译工具链。若当前环境不支持，记录真实错误，并在支持的 CI/Linux 环境执行同一 race 检查；不得把“未运行”写为通过。

**验收：**跨进程所有权、崩溃接管、长调用续租和副作用不确定性均有测试；所有本次相关失败解决后才能完成本卡。

### TASK-SC-12：兼容性文档与交付归档

**依赖：**SC-11。**交付：**行为变化、迁移方式和验证结果可被下一位维护者查阅。

**文件：**新增 D:/codeproject/mygo/goagent/docs/adr/ADR-0032-session-snapshot-consistency.md（执行时若编号已被占用，使用实际下一个空号并更新本文引用）；更新 D:/codeproject/mygo/goagent/docs/adr/ADR-0026-state-files-durability.md、D:/codeproject/mygo/goagent/docs/tasks/plan-session-snapshot-consistency.md，以及当前介绍 Fork/Checkpointer 的实际文档段落。

- [ ] 记录为什么保留 append-only、为什么文件冻结与活跃文件共享分开、为什么 BeginRun 返回原子 head，以及顶层统一释放的约束。
- [ ] 给出 Fork 从一个返回值迁移到 (checkpoint,error) 的完整调用示例；明确不复制暂停/审批/工具账本。
- [ ] 列出 KV 支持类型和拒绝类型，明确新增 ErrStateNotCloneable；不要把类型化业务 codec 或 schema 迁移写成已实现。
- [ ] 更新 ADR-0026 中“Memory 共享活跃句柄即可正确持久化”的旧说明，记录 Memory 对可快照文件的行为变化，保留外部目录与 subagent 协作契约。
- [ ] 说明 WithThreadCoordinationRequired、普通运行冲突、暂停必须 Resume、惰性加载时点变化、Prepared Resume 过期以及旧 store 的保证边界。
- [ ] 回填本文件的任务状态、实际修改文件、测试命令/结果、相关提交和偏离设计的原因。只有真实通过的项才能勾选。

**验收：**文档与实际签名/行为一致；没有“将在以后补完”才能满足本批三个目标的尾项；提交、推送、PR 按执行会话的授权处理，本计划不要求直接合入主分支。

## 五、风险与控制

| 风险 | 控制方式 |
| --- | --- |
| JSON 深拷贝导致 Memory 中数字和具体 map 类型变化 | D2 保留类型；SC-01 类型断言与 middleware 回归 |
| 把运行时共享 Files 一并深拷贝，破坏 subagent | CloneStateData 保留句柄；checkpoint 单独冻结；SC-05 对共享行为回归 |
| 只复制 FileSnapshot，保存时仍被忽略 | SC-03 先独立测试 FileSnapshot→Save，再做 Fork |
| CAS 导致成功但未获确认的提交无法重试 | D5 区分等价重试与新提交；File 继续完成 Sync 确认 |
| 新租约使长模型调用普遍失败 | SC-08 独立续租；SC-11 多租期长调用测试 |
| 子 Agent 提前释放 root writer | guard 只由顶层拥有；SC-09 嵌套屏障测试 |
| 所有线程被一个 agent mutex 串行 | 按线程在 store 协调；不同 Thread 并发测试 |
| 续租失败后自动接管造成工具重复执行 | 取消根 context、禁止后续分发；保留 unknown outcome |
| 新接口让第三方 store/wrapper 编译失败 | 使用可选接口；严格模式显式报错；旧路径单独测试 |
| 改 Fork 签名遗漏外部迁移 | SC-04 全仓调用搜索；SC-12 记录明确的源码不兼容变更 |

## 六、最终交付清单

- [x] SC-01：状态/消息深拷贝。
- [x] SC-02：Memory 不可变快照。
- [x] SC-03：文件冻结与保存。
- [x] SC-04：完整 Fork。
- [x] SC-05：Agent 恢复及内部数据隔离。
- [x] SC-06：Memory 协调与 CAS。
- [x] SC-07：File 持久化协调。
- [x] SC-08：writer guard 与续租。
- [ ] SC-09：运行生命周期接入。
- [ ] SC-10：模型/工具分发门禁。
- [ ] SC-11：跨进程及故障回归。
- [ ] SC-12：文档迁移与验收归档。

执行者完成本批时，应能回答并用测试证明：保存后改 input 会不会改历史、读取后改 output 会不会改历史、Fork 是否带回文件、提前创建的两个句柄是否看见正确 head、真正并发是否在副作用前冲突、长调用是否持续持有租约、旧 writer 是否永远不能提交新 owner 的状态。

## 七、计划编写阶段的验证记录

2026-10-11：已将 SC-02、SC-09 的两段完整 Go 测试提取到工作区外的临时目录，分别使用 go test <绝对测试文件路径> -count=1 执行。两段均成功编译，并在当前代码上出现预期红灯：

- TestSnapshotConsistencyMemorySave：saved snapshot changed through input: changed。
- TestSnapshotConsistencyLazyRunsUseCurrentHead：user messages = [seed request-B], want [seed request-A request-B]。

这两次失败用于验证计划中的回归测试能捕获现有缺陷，不是实施完成后的验收结果。本文所有实施复选框仍保持未完成；本次没有修改 core/checkpoint/agent 的实现文件，也没有执行全仓回归。文件分支问题沿用输入分析中已记录的复现结果，SC-03/04 要将其固化为正式测试。
