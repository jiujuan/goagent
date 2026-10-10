# ADR-0027: DirStore —— 以真实目录为内容的工作区文件后端

- 状态: Accepted（2026-09-30 三卡落地：`1e560ce` vfs、`7cdfbcf` workspace、`1654b8c` agent）
- 日期: 2026-09-29
- 关联: ADR-0026（P1 已落地的快照机制，本 ADR 是其 P2 条目）、ADR-0022（workspace 装配边界、`.goagent/<sub>` 目录约定、`os.Root` 约束）

## 背景

ADR-0026 已让 `State.Files` 可持久：`vfs.InState` 实现 `core.Snapshottable`，`File` checkpointer 把内容按内容哈希存进线程 JSONL 的 blob 行，读回时经 `Checkpoint.FileSnapshot` 交还 agent 层重建。该路线把文件当作**检查点数据**，代价是三处结构性上限：

1. **单条目上限 16MB**。扫描线程文件的 buffer 上限写死在 `checkpoint/file.go:157`（`sc.Buffer(..., 16*1024*1024)`）：任何一条 blob 超过它，整个线程文件此后读不回。
2. **每次 Save 全量复制**。`InState.Snapshot()`（`vfs/instate.go`）深拷贝全部文件字节；一个字节没变的旧产物每步都被重新复制一遍，成本随"文件总量 × 步数"增长——与 ADR-0025 刚消除的"历史每步重复发送"同形。
3. **产物在 JSONL 里不可直接取用**。运维/调试要看一眼 agent 写出的中间产物，必须先解析线程文件；产物无法被外部工具直接消费。

同时，ADR-0026 已经把"后端不可导出内容"这件事变成了一条**已定义的路径**而非漏洞：不实现 `Snapshottable` 的后端属于**外部托管**——检查点不搬它的字节，续跑时由调用方用 `agent.WithRunFiles` 重新注入（`core/state.go` 接口注释、`agent/agent.go` 的 `WithRunFiles` 文档均已写明）。本 ADR 就是沿这条已定义的路径，提供一个"内容本来就在磁盘上、因此不需要被快照"的后端。

## 名词

- **目录后端（DirStore）**：`vfs` 包新增的 `core.FileStore` 实现，其文件内容就是指定目录下的真实文件；句柄只保存 `*os.Root`。
- **产物目录（artifact dir）**：DirStore 绑定的根目录，位于 `<工作区根>/.goagent/files/<thread>`，沿用 `workspace` 既有的 `.goagent/<sub>` 约定（`workspace/workspace.go:36,162`）。
- **外部托管（externally managed）**：ADR-0026 定义的状态——后端内容由环境（磁盘、远程库）自行持久，检查点不参与其持久化，续跑需重新注入句柄。
- **原子替换**：写入先落临时文件再 `rename`，读者只会看到"旧的完整内容"或"新的完整内容"，不会看到半截文件。

## 决策概览

新增 `vfs.NewDirStore(dir string) (*DirStore, error)`，实现 `core.FileStore` 的 Read/Write/List，**不实现** `core.Snapshottable/Restorable`。除此之外不改任何既有机制：

| 关注点 | 归属 |
| --- | --- |
| 内容持久 | 文件系统本身（检查点不参与，符合外部托管语义） |
| 路径越界防护 | `os.Root`（与 `tool/file` 同一机制，`workspace/workspace.go:95`、`tool/file/file.go:100` 已用） |
| 目录选址 | `workspace` 装配层新增 `ws.RunFiles(threadID)`，钉在 `<root>/.goagent/files/<thread>` |
| 句柄注入 | 既有 `agent.WithRunFiles`（优先级高于检查点快照，`agent/agent.go` 已定义并有测试钉住） |
| 默认行为 | **不变**，仍为 `InState`（`agent/agent.go:117-122` 的兜底一字不改） |

## 设计细节

### 1. 类型与构造

```go
// vfs/dir.go
// DirStore keeps its contents in a real directory. Durability comes from the
// filesystem, so it is deliberately not core.Snapshottable: the checkpointer
// does not copy bytes it does not own. A process resuming the thread
// re-attaches the same directory with agent.WithRunFiles(vfs.NewDirStore(dir)).
type DirStore struct{ root *os.Root } // root 由 NewDirStore 内部 OpenRoot 得到

func NewDirStore(dir string) (*DirStore, error) // MkdirAll + os.OpenRoot
```

构造期校验沿用既有风格：目录不存在则创建，`os.OpenRoot` 失败即报错（对齐 `tool/file/file_test.go:39` 与 `workspace.go:95` 的用法）。

### 2. 路径规则（关键约束，明确写入文档）

模型可控路径与框架内部路径共用一个入口，规则取"严格"一侧：

- 允许：斜杠分隔的相对路径，如 `notes/summary.md`；写入时按需 `MkdirAll` 父目录（同 `tool/file` 的 `write_file`，`file.go:96-101`）。
- 拒绝并返回**工具错误**（不是 Go error 上抛，遵循"工具错误是数据"的既有立场）：绝对路径、盘符前缀、`..` 起始或越界、`.`（指名目录本身）。
- 实现：`vfs` 自带一个约 20 行的 `cleanPath`，语义与 `tool/file` 的 `relPath`/`filePath`（`tool/file/file.go:159-180`）一致。**不跨包复用未导出函数**，也不为此新建 `internal/` 包——两处各 20 行、且安全边界本就由 `os.Root` 兜底（`filePath` 自己的注释即说明"这些检查是为了报错可读，安全由 os.Root 保证"）。

拒绝清单必须与 `os.Root` 的拒绝行为一致，避免"校验放行但 OpenRoot 报错"的双重口径。

### 3. 写入原子性

`Write` = `root.writeFile("<name>.tmp-<callid>", data)` → `root.Rename(tmp, name)`。理由：DirStore 的读者不止本进程（P2 的存在意义之一就是外部可读），半截文件会被直接消费。`FileStore` 无 `Delete`（ADR-0026 明确不动该接口），故只承诺"覆盖写原子"。

### 4. 与检查点/续跑的交互（不新增机制，只声明结果）

- `File.Save` 遇到不实现 `Snapshottable` 的 `State.Files` 时**静默跳过**（已由 ADR-0026 实现并有 `TestFileExternalBackendNotPersisted` 钉住）。
- `agent.Stream`/`Resume` 的重建顺序（显式 `WithRunFiles` > 存活句柄 > 快照 > 新建空 `InState`）不变。用 DirStore 的调用方在续跑时必须自己传回同一目录的句柄；**没传**就会得到一个空 `InState`——这与今天的行为一致，不是新问题，但文档要显式提醒。
- 时间旅行/分支的语义变化（必须写进注释与文档）：检查点不再记录文件内容的历史版本，回滚 `State.Messages` 不会回滚磁盘上的产物；并行分支写同一路径会互相覆盖。这是"内容外置"换来的代价，不是缺陷。

### 5. workspace 接入

```go
// workspace.go
// RunFiles attaches a disk-backed artifact store for one thread, rooted at
// <root>/.goagent/files/<thread> — inside the workspace root, so the model's own
// file tools can reach what a run leaves behind.
func (w *Workspace) RunFiles(threadID string) (*vfs.DirStore, error)
```

`threadID` 的处理在本 ADR 落地时是"清洗成安全目录名"，**该结论已被 ADR-0028 取代**：目录名就是标识原样，不合格的标识（含 `/ \ : 空格 .`、空串、超过 `core.MaxThreadIDLen`、非 ASCII）由 `core.CheckThreadID` 直接拒绝，因为清洗是多对一映射（`a/b` 与 `a_b` 会共用一个产物目录），而且去掉清洗后 `..` 这类值会把目录指到别处。副作用：workspace 为调用这个判定新增了 `core` import（下面一段"不新增 `core`"仅就返回类型而言仍然成立）。

返回类型与初稿不同（落地时改）：初稿写 `core.FileStore`，实际返回具体类型 `*vfs.DirStore`。理由：返回接口要为一个类型名给 workspace 新增 `core` 依赖，返回具体类型只新增 `vfs` 一个 import；调用处把它传给 `agent.WithRunFiles`（收 `core.FileStore`）时自动满足接口；额外好处是"它不是可快照后端"这件事在调用点就看得见。附带结果：DirStore 自持 `*os.Root`，因此提供 `Close()`，句柄生命周期归使用方，且不随 `Workspace.Close()` 失效。

## 范围

### 做
- `vfs/dir.go`：`DirStore` + `cleanPath` + 原子写 + List 排序（与 `InState.List` 一致）。
- `workspace.Workspace.RunFiles(threadID)`。
- 测试（见锚点）；`agent.WithRunFiles` 文档补一句"DirStore 场景必须在续跑时重新注入"。

### 不做（理由）
- **不改默认后端**：默认 `InState` 的"每步随检查点版本化"是现有语义与测试的基础；改成磁盘会把"回滚不还原文件"变成默认行为（见设计细节 4），风险大于收益。
- **不给 DirStore 实现 `Snapshottable`**：那会把大文件重新塞回 JSONL，正是要规避的 16MB/全量复制问题。
- **不做产物 TTL/GC**：目录归调用方（按线程目录整体删除即可），框架不引入清理策略。
- **不做跨进程文件锁**：同线程并发写本身已是不推荐用法（分支并行写同路径在 InState 下同样会互相覆盖），文档声明即可。
- **不加 `Delete` 到 `FileStore`**：与 ADR-0026 同一立场，属独立 API 扩张。

## 备选方案

- **A. 让 DirStore 也实现 Snapshottable，只存 `path→磁盘路径`索引进检查点。** 否决：blob 索引语义被破坏（`FileSnapshot` 是 `path→bytes`，agent 层用它重建内存后端），且产物换目录/改名后索引即失效，把"外部可读"变成"外部可读但检查点会撒谎"。
- **B. 提高 16MB 上限 / 流式扫描 blob。** 否决：只解决痛点 1，痛点 2（全量复制）与 3（不可直接取用）仍在；且调大 buffer 会把内存峰值同步抬高，是打补丁不是解法。
- **C. 复用 `tool/file` 的校验函数（导出或移入 `internal/pathsafe`）。** 可选的整洁性改动，但为两个各 20 行的私有校验引入新的导出面/新包不划算；安全边界由 `os.Root` 保证这一事实已在 `tool/file` 注释中确认。否决本次，留作独立整理。
- **D. 不做 DirStore，需要大产物时由调用方自己用真实文件工具（`tool/file`）+ 把路径写进 `State.KV`。** 这是今天就能跑的替代方案，代价是没有 `FileStore` 抽象、子代理共享要靠 KV 约定。DirStore 的价值正是把这套约定收敛成 20 行代码 + 一个已有接口。

## 迁移与兼容性

- 纯加法：新类型 + 新方法。既有 `InState`/检查点/agent 行为零变化。
- 已在用 `WithRunFiles` 传自研后端的用户不受影响（外部托管语义 ADR-0026 已定义）。
- 用法示例（写入文档，不新增示例目录）：
  ```go
  files, err := ws.RunFiles(threadID)   // 或 vfs.NewDirStore(`<abs>`)
  out, err := a.Run(ctx, input, agent.OnThread(threadID), agent.WithRunFiles(files))
  // 续跑（同进程或新进程）：再传同一个 files 句柄/同一目录即可看到此前的产物
  ```

## 测试锚点

- 往返：Write→Read 二进制安全；List 前缀匹配且排序；`.`/绝对/`..`越界/空串均返回错误且不落盘。
- 原子替换：覆盖写后 `Read` 得到新内容；目录里不残留 `.tmp-*` 文件。
- 跨进程可见：`NewDirStore(dirA)` 写 → 另一 `NewDirStore(dirA)` 读得到（同目录即同事实）。
- 与检查点交互（回归 ADR-0026 已钉的语义）：把 DirStore 挂到 `State.Files` 后 `File.Save` → 线程文件无 blob/无 `files` 索引、`FileSnapshot` 为 nil、不报错。
- 端到端：agent 以 `WithRunFiles(DirStore)` 跑"写产物→答"；新 agent 实例 + 新句柄指向同目录续跑，工具能读到前一进程的产物文件内容。
- 反向端到端：续跑**不传**句柄时文件面为空，而磁盘上的产物仍在（外部托管的代价，写进 `agent.WithRunFiles` 文档）。
- `ws.RunFiles` 选址：产物落在 `<root>/.goagent/files/<thread>` 下，且不越过工作区根；不合格 threadID 被拒绝（原为"字符被清洗"，见 ADR-0028）；工作区 root 句柄（即模型 `read_file` 走的那条路径）读得到该产物。
- 回归：全仓 `go build/vet/test ./...` 与 `vfs/agent/workspace` 的 `-race`。

落地实测对应关系（三卡全绿）：第 1–3 条 = `vfs/dir_test.go` 的 8 个用例；第 4 条复用 ADR-0026 已有的 `checkpoint.TestFileExternalBackendNotPersisted`，加上 `workspace.TestRunFilesIsNotSnapshottable` 保证装配层交出的确实是该性质；第 5–6 条 = `agent.TestDurableDirStoreResumeAcrossInstances` / `TestDurableDirStoreForgetsHandleYieldsEmpty`；第 7 条 = `workspace` 的 5 个 `TestRunFiles*` 用例。

## 实施切分

1. `vfs/dir.go`：`DirStore` + `cleanPath` + 原子写 + 能力断言（`_ core.FileStore`，且**不**断言 Snapshottable）+ 单元测试。→ `1e560ce`
2. `workspace`：`RunFiles(threadID)` + threadID 清洗 + 选址测试。→ `7cdfbcf`（清洗部分已由 ADR-0028 的 `ac7b89a` 换成拒绝）
3. 端到端续跑测试 + `WithRunFiles` 文档补句 + 全仓验证。→ `1654b8c`

## 未决问题

- 产物目录是否应随线程删除而自动清理（框架目前无线程删除 API，留白）。
- 若日后要"大产物 + 文件版本可回滚"两者兼得，唯一干净路线是把 blob 从线程 JSONL 挪到独立 blob 目录并让检查点只存哈希——那是对 ADR-0026 P1 存储介质的替换，需单独 ADR。
- ~~**threadID 折叠**~~：**已修复，见 ADR-0028**。落地时这里的选择是"清洗保留、两处各自一份、不加消歧后缀"，随后实测发现同一条折叠会让两条线程写进**同一个 `.jsonl`** 并互相接错对话历史（比产物目录共用严重一档），因此清洗整体废除、改为 `core.CheckThreadID` 拒绝（checkpoint `6abfd2f`、workspace `ac7b89a`）。
- **盘符路径的跨平台差异**（落地时发现）：`C:\x` 在 Windows 被 `filepath.VolumeName` 判为绝对路径而拒绝；在 POSIX 下它是 root 内一个合法文件名，`os.Root` 不拒绝，于是被当作相对文件名接受。校验规则与 `tool/file` 保持一致（只为报错可读，安全边界由 `os.Root` 提供），不为平台差异加特例。**注**：这条说的是产物*内部路径*（`DirStore.cleanPath`），与线程标识无关 —— 标识由 ADR-0028 的 `core.CheckThreadID` 在两个平台上用同一条规则拒绝。
