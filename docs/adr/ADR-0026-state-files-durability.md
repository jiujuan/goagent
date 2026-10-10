# ADR-0026: State.Files 的持久化与快照契约修正

- 状态: Accepted（P0+P1 已实施：`core.Snapshottable/Restorable`、`vfs.InState.Snapshot/Restore`、`checkpoint/file.go` blob sidecar+索引+去重缓存、`Checkpoint.FileSnapshot` 内存手递、`agent` restore/Resume 三优先级还原；P2 未做）
- 日期: 2026-09-29
- 关联: ADR-0022(workspace 的 os.Root 文件工具)、ADR-0025(历史落盘，同属"别让每步重复膨胀")、`core/state.go`、`checkpoint/file.go`、`agent/{agent,hitl,runtime,subagent}.go`、`vfs/instate.go`

## 背景

`core.State` 里有一块工作区文件面 `Files`：

```go
// core/state.go:10-25
type State struct {
    Messages []Message
    Todos    []Todo
    Files    FileStore      `json:"-"`        // 故意不进 JSON
    KV       map[string]any
}
type FileStore interface {
    Read(path string) ([]byte, error)
    Write(path string, data []byte) error
    List(prefix string) ([]string, error)     // 注意：没有 Delete
}
```

它的设计意图（`vfs` 包注释、`State` 注释）是 deepagents 式的"大工具结果/中间产物外置"，随 run 的 State 一起被 checkpoint 捕获。但存在三处互相矛盾的事实：

1. **文档在说谎。** `vfs/instate.go:16-17` 写："files live in memory and travel with the run State (**so they are captured by checkpoints**)"。可 `Files` 是 `json:"-"`（`state.go:13`）。`File` checkpointer 整个 `Checkpoint` 走 `json.Marshal`（`checkpoint/file.go:44`），`Files` 在序列化时被丢弃，读回（`Latest`/`Load`）后 `cp.State.Files == nil`。

2. **只有"同进程 + 内存 checkpointer"侥幸能用。** `Memory.Save` 直接 append `*Checkpoint`（`memory.go:29`），`cloneState` 又按指针复制 `Files`（`checkpoint/branch.go:25-26`），所以单进程内 `Files` 因指针共享而存活。**这条侥幸掩盖了 1**，让人误以为契约成立。

3. **resume 路径把丢掉的 Files 一律重置成空。** 两个续跑入口遇到 `Files == nil` 都新建空 `InState`：
   - `Agent.Stream` → `restore`（`agent/agent.go:133-143`，cloneState 后 Files 仍 nil）→ `agent/agent.go:117-122` `else if state.Files == nil { state.Files = vfs.NewInState() }`。
   - `Agent.Resume` → `agent/hitl.go:72-75` 同样的 nil 兜底。

结论：**用 `File`（JSONL）checkpointer 做跨进程续跑（HITL 在另一进程批准、queue worker 崩溃重领——正是 ADR-0022/0024 强调的 durable 场景）时，暂停前写入工作区的所有文件在恢复后凭空消失，且没有任何报错。**

### 当前真实影响面（诚实评估）

全仓目前**没有第一方工具写 `State.Files`**：`tool/file`（`tool/file/file.go:1-14`）绑的是真实磁盘 `*os.Root`，文件本就落盘、天然跨重启存活，与此问题无关。`State.Files` 唯一的活跃消费者是 subagent 共享父 `Files`（`agent/subagent.go:50-51`）与 run 作用域注入的种子复制（`agent/runtime.go:71,182`）。

所以这不是"线上正在丢数据"的紧急故障，而是一个**契约与文档不一致的定时炸弹**：`WithRunFiles` 是公开 API（`agent/agent.go:98-99`），任何人传入自定义 `FileStore` 并依赖注释里"captured by checkpoints"的承诺，就会在 durable resume 时静默丢状态。

> 命名澄清：`memory.FileStore`（`memory/filestore.go:31`）是**向量库**的磁盘实现，与本 ADR 的 `core.FileStore`（虚拟文件面）同名但无关，勿混淆。

## 目标

1. 让"文件随 State 被 checkpoint 捕获"这句承诺在 `File` checkpointer 上**真正成立**：跨进程 resume 能还原 `Files` 内容。
2. **不重蹈 ADR-0025 的覆辙**：不能把整个文件映射内联进每一步的 checkpoint 行——那是 `O(步数 × 文件总量)` 的膨胀。
3. 无法序列化的后端（真实目录、远程库）保持可插拔，不被强加持久化义务。
4. 修正说谎的注释；把当前"内存 checkpointer 侥幸存活、File checkpointer 丢失"的分歧收敛成**一致且被测**的行为。

## 名词

- **快照能力（Snapshottable）**：`FileStore` 的可选能力，能把自身内容导出成 `map[string][]byte`。
- **还原能力（Restorable）**：可选能力，能从一个 `map[string][]byte` 重建自身内容。
- **内容寻址 blob**：以字节内容的哈希为 id 存储的文件块；同内容只存一份。
- **文件索引（FileIndex）**：某次 checkpoint 的逻辑视图 `路径 → blob 哈希`，体积小、随该步落盘。
- **外部托管（externally managed）**：后端不可快照时，持久化由调用方负责，resume 时用 `WithRunFiles` 重新注入。

## 决策概览

分三层，P0 是纯修正、P1 是真机制；核心取舍是把"如何序列化文件"与"谁来重建文件句柄"解耦，避免 `checkpoint` 包依赖 `vfs`。

### P0（必须，先做）：修正文档 + 钉住现状的回归测试

- 把 `vfs/instate.go:16-17` 那句"captured by checkpoints"改成诚实描述：默认后端在**同进程内存 checkpointer** 下随 State 存活；**`File`（JSONL）checkpointer 当前不序列化 `Files`**，跨进程 resume 需要一个可快照后端（引出 P1），否则由调用方 `WithRunFiles` 重新注入。
- `checkpoint/file.go:14-21` 的 Caveat 注释保留并收紧措辞（已提及 Files 不序列化，这里补一句"恢复路径见 ADR-0026"）。
- 加回归测试**锁定当前行为**（见测试锚点 T0），让 P1 的改动有对照、防"顺手改坏"。

### P1（推荐机制）：可选快照/还原能力 + 内容寻址 sidecar

**能力接口**（放 `core`，紧邻 `FileStore`，使 `checkpoint`/`agent` 都无需新增对 `vfs` 的依赖）：

```go
// core/state.go 追加
type Snapshottable interface{ Snapshot() map[string][]byte }
type Restorable   interface{ Restore(files map[string][]byte) error }
```

`vfs.InState` 实现二者（`Snapshot` 返回全量拷贝，`Restore` 替换内部 map）。不实现者即"外部托管"。

**`File` checkpointer 的存储格式扩展**（同一 `<thread>.jsonl`，两类行，向后兼容旧行）：

- checkpoint 行：现有 `Checkpoint` JSON，外加顶层 `"files": {"<path>":"<hash>", ...}`（文件索引，小）。
- blob 行：`{"blob":"<hash>","data":<base64>}`，仅在**该 hash 首次出现**时追加。Go 对 `map[string][]byte` 序列化天然 base64，二进制安全。

`Save` 时：若 `cp.State.Files` 实现 `Snapshottable`，取快照 → 对每个 `path→content`，算 `hash=sha256(content)`，若该 hash 不在"本线程已写集合"则追加 blob 行；把 `path→hash` 索引写进 checkpoint 行。已写 hash 集合用 `File` 实例内的 `map[thread]map[hash]bool` 缓存，进程内避免重复扫文件；**新进程**首次 Save 一个已有线程时先懒读该线程 blob 行播种缓存（一次性）。因 `FileStore` 无 `Delete`、`InState` 只增不改删，索引单调、去重安全。

`Load`/`Latest` 时：读回 checkpoint 行 → 收集 blob 行建 `hash→bytes` → 依索引组装 `map[path]bytes` → **不在此处造句柄**，而是塞进返回值 `Checkpoint` 的一个新字段 `FileSnapshot map[string][]byte`（`json:"-"`，仅内存传递）。

**agent 侧还原**（`vfs` 只在这里出现，保持分层）：`Stream`/`Resume` 拿到恢复的 `Checkpoint`（含 `FileSnapshot`）后：
1. 若显式 `WithRunFiles` → 用它（调用方最高优先）。
2. 否则若 `FileSnapshot != nil` → 建 `vfs.NewInState()`，`Restore(FileSnapshot)`。
3. 否则 → 现状 `NewInState()`。
`Memory` checkpointer 走指针共享已正确，可不动；但为一致性与被测，`restore` 逻辑统一按"有 FileSnapshot 用快照、否则指针"处理。

### P2（可选，视需要）：`InState` 落盘后端

真正给 `Files` 一个"内容即磁盘"的默认后端（`vfs` 加 `NewDirStore(dir)`，`Write` 直接写 `os.Root`），使默认配置天然持久、无需快照机制。**不做**的理由：把默认后端从内存换成磁盘会改变现有 subagent 共享语义与测试假设，风险大于 P1 的能力式增量；仅当 P1 的快照对超大产物仍显笨重时再引。列为后续，不纳入本次实施。

## 序列化接缝为何这样设计（避免 `checkpoint → vfs` 依赖）

`Checkpoint` 只携带"数据"（`FileSnapshot` 或索引+blob），不携带"如何变成 `FileStore`"。把 `map→InState` 的重建留在**已经 import vfs 的 agent 层**（`agent.go:121` 已用 `vfs.NewInState()`），`checkpoint` 包继续只依赖 `core`。这样：
- `checkpoint` 不需要知道 InState 长什么样；
- 任何 `Restorable` 后端都能被同一 agent 逻辑还原；
- 与既有 `Snapshottable`/可选能力风格（`SequentialTool`、`HistoryCompacter`）一致。

## 范围

### 做
- P0：改注释、加锁定现状的回归测试。
- P1：`core.Snapshottable/Restorable`；`InState` 实现；`File` checkpointer 的索引+blob 行读写与懒播种去重缓存；`Checkpoint.FileSnapshot`（`json:"-"`）；`agent` 三优先级还原。
- 与 ADR-0025/0024 一致的"跨进程 resume"测试补一个文件维度。

### 不做（理由）
- **不内联整份文件映射进每步 checkpoint**：`O(步数×文件)` 膨胀，正是 ADR-0025 刚消除的反模式。
- **不给 `FileStore` 加 `Delete`**：本问题的既有语义无删除；加 API 牵动 `tool/file`、subagent 共享，另案。
- **不让 `checkpoint` 依赖 `vfs`**：见上节。
- **不把默认后端换成磁盘（P2）**：改默认语义风险大，列为后续。
- **不动 `tool/file`/workspace**：那是真实磁盘 os.Root，与此虚拟文件面无关。

## 备选方案

- **A. 直接去掉 `state.go:13` 的 `json:"-"`，让 Files 内联进 State。** 否决：① 对不可序列化后端（os.Root/远程）`MarshalJSON` 直接失败，破坏可插拔契约；② 每步全量内联 = ADR-0025 刚根治的膨胀；③ `map[string][]byte` base64 内联使历史 checkpoint 行体积翻倍可读性变差。
- **B. 只改注释、声明 Files 本就是易失暂存、要持久用 `WithRunFiles` 自管。** 这是 P0 的下限（若 P1 判定过度设计可回退到此），但把负担推给每个调用方、且与包注释原承诺相悖，作为"最小正确"而非"最佳"。
- **C. 用 P2 的磁盘后端做默认。** 否决（本次）：改默认行为影响面广，宜作为独立增强。
- **D. 快照但每步全量存快照（不内容寻址去重）。** 比 A 好（不内联进 State、二进制隔离），但仍 `O(步数×文件)` 字节。否决，内容寻址去重才符合"每步增量"的框架取向。

## 测试锚点

- **T0（P0，锁定现状）**：两个 Agent 实例共用一个 `File` 目录（模拟跨进程），暂停前用 `WithRunFiles` 注入 InState 并 Write 一个文件，resume 后当前读到空 —— 断言"确实丢"，作为 P1 的对照基线。
- **T1（能力）**：`InState.Snapshot/Restore` 往返一致（含二进制、空 map、覆盖同名）。
- **T2（File 读写）**：Save 含文件的 checkpoint → 读回 `FileSnapshot` 等于快照；blob 去重（同内容不同路径 → 一行 blob、两条索引）；改一文件 → 只追加新 blob、旧 blob 不重写。
- **T3（端到端 durable resume）**：P1 后，T0 场景改为断言文件**不丢**：resume 后 `Files.Read(path)` 返回暂停前内容。
- **T4（优先级）**：resume 同时传 `WithRunFiles(custom)` → 用 custom，忽略快照。
- **T5（外部托管）**：传入一个不实现 `Snapshottable` 的 FileStore，Save 不报错、`FileSnapshot` 为 nil、resume 走 WithRunFiles 或空，文档路径成立。
- **T6（兼容旧行）**：读一个 P1 之前写出的、无 `files` 字段的旧 JSONL，`Latest`/`History` 正常返回、`FileSnapshot=nil`，不炸。
- 回归：全仓 `go test ./...` 绿；`checkpoint`/`agent`/`vfs` 加 `-race`。

## 实施切分（每批可编译可测）

1. `core`：`Snapshottable`/`Restorable` 接口 + `Checkpoint.FileSnapshot`（在 checkpoint 包）说明——或 P0 与 P1 接口分两 commit。
2. `vfs`：`InState` 实现 `Snapshot`/`Restore` + 诚实注释（P0 先行单独 commit）。
3. `checkpoint/file.go`：索引+blob 行格式、懒播种去重缓存、读回 `FileSnapshot`；T1/T2/T6。
4. `agent`：`Stream`/`Resume` 三优先级还原；T0→T3/T4/T5。

## 未决问题

- 超大单文件产物的 blob 行是否要设阈值分片或落 `FileStore` 外部目录（关联 P2）——先按"整 blob 追加"实现，尺寸问题留给真实压力数据。
- Fork/时间旅行下 blob 的线程归属：当前设计 blob 去重是"每线程文件"局部，跨线程分叉会各自重写所引用 blob（正确但可能重复）；是否需要线程树级共享 blob 池待评估。
