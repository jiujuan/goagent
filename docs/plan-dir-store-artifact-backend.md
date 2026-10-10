# 实施方案：DirStore —— 磁盘目录型的工作区文件后端

- 范围：`vfs` 新增 `DirStore`；`workspace` 新增 `RunFiles`；`agent` 侧文档补句与端到端测试。对应 ADR-0027，即 ADR-0026 中列为后续的 P2 条目。
- 日期：2026-09-29
- 状态：三卡全部落地 —— DS-01 `1e560ce`、DS-02 `7cdfbcf`、DS-03 `1654b8c`；ADR-0027 已转 Accepted。落地时对卡片的修正集中在"实施期修正"与各卡内标注为"实现期新增/改为"的条目。
- 后续修订：本卡的"未解决问题 #6/#10"与两条风险项由 **ADR-0028（线程标识命名契约）** 处理 —— 目录名/文件名清洗（`workspace.safeThread`、`checkpoint/safeName`）一律删除，不合格标识在读写两端直接报错（`6abfd2f`、`ac7b89a`），检查点读取端补上归属过滤（`6eb6d3c`、`e5d2de6`）。文中相关条目已就地标注"已过时/已由 ADR-0028 取代"，原描述保留以留痕。

## 前置事实（已 grep 核实）

- `core.State.Files` 被 `json:"-"` 排除在 State 自身序列化之外（`core/state.go:13`）；`core.FileStore` 只有 Read/Write/List 三个方法，**没有删除方法**（`core/state.go:20-25`）。
- 可选能力 `core.Snapshottable`（`core/state.go:34-35`）与 `core.Restorable`（`core/state.go:41`）已存在；不实现 Snapshottable 的后端在接口注释里已被定义为"外部托管：检查点不搬它的字节，续跑由调用方重新注入"。
- `File` checkpointer 的保存逻辑用类型断言决定是否快照（`checkpoint/file.go:88`）：断言失败即静默跳过，不报错。该分支已有测试覆盖（`checkpoint/filesnapshot_test.go:162-167` 的 `notSnap` 后端 + 相应用例）。
- 检查点读回的内存手递字段是 `Checkpoint.FileSnapshot`（`checkpoint/checkpoint.go:36`，同样 `json:"-"`）；重建文件句柄发生在 agent 层（`agent/agent.go:159` 的 `applyFileSnapshot`），`checkpoint` 包不依赖 `vfs`。
- 续跑优先级已实现并有测试：显式 `WithRunFiles` > 检查点里仍存活的句柄 > 快照重建 > 新建空 `InState`（`agent/agent.go:105,125,127`、`agent/hitl.go:73,75`；用例见 `agent/filedurable_test.go` 的 `TestDurableRunFilesOverridePrecedence`）。
- 默认后端的两个已知上限仍在：每次保存全量深拷贝（`vfs/instate.go:72` 的 `Snapshot`）、线程文件单行 16MB（`checkpoint/file.go:157` 的 `sc.Buffer(..., 16*1024*1024)`，超过则整个线程文件读不回来）。
- `os.Root` 的可用方法（本机 go1.26.4，`go doc` 核实）：`Create/Open/OpenFile/ReadFile/WriteFile/Mkdir/MkdirAll/Rename/Stat/Lstat/Remove/RemoveAll/OpenRoot/FS` 等，**没有 Walk**；目录枚举需走 `root.FS()` 配 `io/fs`。既有同族用法见 `tool/file/file.go:67`（ReadFile）、`:96`（MkdirAll）、`:100`（WriteFile）、`:117`（`fs.ReadDir(root.FS(), name)`）。
- 路径校验的既有实现：`tool/file/file.go:164` 的 `filePath` 与 `:184` 的 `relPath`（拒绝绝对路径、盘符、`..` 越界），其注释明确"这些检查是为了报错可读，越界由 `os.Root` 拒绝"。两者均未导出。
- 目录名清洗函数 `safeName` 在 `checkpoint/file.go:271`，未导出；空串回退为 `"thread"`。**（已过时：ADR-0028 的 `6abfd2f` 删除了该函数，文件名改为标识原样 + `core.CheckThreadID` 拒绝不合格值。）**
- workspace 的目录约定：常量 `userDirName = ".goagent"`（`workspace/workspace.go:36`）、`projectDir` 即 `<root>/.goagent/<sub>`（`:160-167`）、工作区句柄 `os.OpenRoot(root)`（`:95`）。**workspace 当前不 import `core` 与 `vfs`**（`:19-31` 的 import 块可查）。**（已过时：DS-02 起 import `vfs`；ADR-0028 的 `ac7b89a` 起再 import `core`，因为 `RunFiles` 要用 `core.CheckThreadID` 拒绝不合格标识。）**
- 全仓 `State.Files` 的写入方**只有本系列新增的测试工具**（`agent/filedurable_test.go:24`）；运行期没有任何第一方机制往里写。相关共享逻辑仅为传递句柄：`agent/runtime.go:71`、`agent/subagent.go:50-51`。

## 总设计立场（三个 TASK 共用）

1. **纯加法，不改任何既有机制。** 默认后端仍是 `InState`，检查点仍走 ADR-0026 的 blob 路线；DirStore 只是新增一个可选后端，并把"外部托管"这条已有路径用起来。
2. **不实现 `Snapshottable`/`Restorable`。** 这是设计前提而非偷懒：内容已经在磁盘上，再复制进线程 JSONL 就会同时保留 16MB 单行限制与全量拷贝成本。用一条测试把这个"故意不实现"记录下来，避免日后被误加。
3. **返回具体类型 `*vfs.DirStore`，不返回 `core.FileStore` 接口。** 与 ADR-0027 初稿的签名不同，理由：返回接口会为了一个类型名给 workspace 新增 `core` 依赖；而返回具体类型只新增 `vfs` 一个 import，且调用处传给 `agent.WithRunFiles` 时自动满足接口。附带好处是"它不是可快照后端"这件事在调用点可见。**（ADR-0028 后 workspace 确实新增了 `core` 依赖（`RunFiles` 校验标识），但那是为了 `CheckThreadID`，不是为了类型名；本条"返回具体类型"的结论不变。）**
4. **越界防护只依赖 `os.Root`，本地校验只为报错可读。** 与 `tool/file` 同一标准；校验规则必须与 `os.Root` 的拒绝集合一致，避免出现"本地放行、Root 报错"两套口径。
5. **原子替换用"临时文件 + Rename"**，因为 DirStore 的读者不只本进程（外部可直接查看产物是该方案的存在意义之一），不能让人读到半截文件。
6. 失败一律以 error 返回给调用方（工具层会把它变成回喂模型的数据），不 panic、不上抛致命错误。

---

## TASK-DS-01：`vfs.DirStore` 主体

### 改动

新增 `vfs/dir.go`：

- `type DirStore struct{ root *os.Root }`，句柄由 `NewDirStore` 自己创建。
- `NewDirStore(dir string) (*DirStore, error)`：`os.MkdirAll(dir)` → `os.OpenRoot(dir)`；任一步失败返回包装后的 error（错误前缀 `vfs:`，与 `instate.go` 现有风格一致）。
- `Read(p string) ([]byte, error)`：`cleanPath` → `root.ReadFile`；`os.IsNotExist(err)` 时返回 `vfs: <p> not found`，与 `InState.Read` 的错误语义对齐（`vfs/instate.go:29-39`）。
- `Write(p string, data []byte) error`：`cleanPath` → 父目录 `root.MkdirAll(parent, 0o755)`（parent 不为 `.` 时）→ `root.WriteFile(name+".tmp-"+core.NewID("w"), data, 0o644)` → `root.Rename(tmp, name)`。临时名带唯一后缀，避免同一路径并发写相互覆盖中间文件。
- `List(prefix string) ([]string, error)`：`fs.WalkDir(d.root.FS(), ".")` 收集所有常规文件（跳过目录、跳过含 `.tmp-` 的中间文件），按 `strings.HasPrefix` 过滤，`sort.Strings` 排序返回（与 `InState.List`，`vfs/instate.go:56-66`，行为一致：前缀为空即全量）。实现期补充两点：非空前缀也先过 `cleanPath`（否则 `List("../x")` 会静默返回空而不报错，与本卡"越界一律 error"的验收不符）；`"."` 与 `""` 同样表示全量。
- `Close() error`（实现期新增，卡里没有）：DirStore 自持一个 `*os.Root`，句柄由 `NewDirStore` 创建、使用方负责释放，不给 Close 就是泄漏。它不改变"检查点不搬字节"的立场，也不参与 `core.FileStore` 契约。
- `.tmp-` 这个标记由包内常量 `tempInfix` 同时供 `Write` 造临时名和 `List` 隐藏中间文件使用，避免两处字符串各写一遍。
- 本地 `cleanPath(p string) (string, error)`：语义照 `tool/file/file.go:164,184`（`filepath.ToSlash` → 拒绝空串、`/` 起始、`filepath.VolumeName` 非空、`path.Clean` 后等于 `.` 或以 `../` 起始）。
- 编译期断言 `var _ core.FileStore = (*DirStore)(nil)`；**不加** `core.Snapshottable`/`core.Restorable` 断言。
- 类型注释必须写清三件事：内容持久由文件系统负责、检查点不会保存它、续跑必须重新注入同一目录句柄、文件不随检查点回滚。

### 验收

`go test ./vfs/`：

- `TestDirStoreRoundTrip`：写含子目录路径（`notes/a.md`）与二进制内容（`{0,1,2,255}`）→ 读回逐字节相等；`List("")` 能列出该路径。
- `TestDirStoreRejectsPaths`：表驱动，输入 `""`、`"   "`、`"."`、`"/"`、`"/abs"`、`".."`、`"../x"`、`"a/../../x"`、`"./.."` → 全部返回 error，且断言目录内文件数不变（递归 `filepath.WalkDir` 计数，证明未落盘）。盘符用例（`C:\x`、`C:/x`）**只在 Windows 断言**：POSIX 下 `filepath.VolumeName` 对盘符返回空串，`C:\x` 是 root 内一个合法的文件名，`os.Root` 不会拒绝它——初稿"非 Windows 平台也应被挡下"的预设有误，实现期已更正。`"."` 走自己的错误文案（"names the store root itself"），与越界（"escapes the store root"）分开，因为使用方犯的错不同（照 `tool/file/file.go:169-171` 的既有分法）。
- `TestDirStoreAtomicOverwrite`：连续两次写同一路径 → 内容为第二次；断言目录下不残留任何 `.tmp-` 文件。
- `TestDirStoreSharedAcrossHandles`：`NewDirStore(dir)` 写 → 第二个 `NewDirStore(dir)` 读到同样内容（跨句柄、跨进程等价证明）。
- `TestDirStoreIsNotSnapshottable`：`if _, ok := any(d).(core.Snapshottable); ok { t.Fatal(...) }` —— 把"故意不实现"这条设计前提记录在测试里。
- `TestDirStoreListSkipsTempAndSorts`：手动写入一个 `x.tmp-w1` 文件，断言 `List` 不返回它，且连列三次顺序一致。
- `TestDirStoreListPrefixSemantics`（实现期新增）：`List("")` 与 `List(".")` 同为全量，`List("notes/")` 只给该子树，`List("../notes")` 返回 error。
- `TestDirStoreClose`（实现期新增）：`Close` 后 `Read` 返回 error（不 panic），磁盘上的文件内容完好。

---

## TASK-DS-02：`workspace.RunFiles` 接入

### 改动

`workspace/workspace.go` 新增方法（放在 `Tools()`（`:200`）附近，与其"装配层"角色并列）。下面这段是 DS-02 当时的写法，其中 `safeThread(...)` 已被 ADR-0028 换成"不合格标识直接报错"（当前实现见 `workspace/workspace.go` 的 `RunFiles`）：

```go
// RunFiles attaches a disk-backed artifact store for one thread, rooted at
// <root>/.goagent/files/<thread>: outside the source tree, inside the workspace.
// The store is externally managed — checkpoints do not copy its bytes, so a
// process resuming the thread must pass an equivalent handle again via
// agent.WithRunFiles. Files here are not rolled back by time travel.
func (w *Workspace) RunFiles(threadID string) (*vfs.DirStore, error) {
    return vfs.NewDirStore(filepath.Join(w.root, userDirName, "files", safeThread(threadID)))
}
```

- 新增 import `github.com/jiujuan/goagent/vfs`（本条原稿附注"不新增 `core`"）。**（ADR-0028 后：`RunFiles` 也 import `core` 以调用 `CheckThreadID`；返回具体类型、不返回 `core.FileStore` 的立场不变。）**
- 本地 `safeThread(threadID string) string`：把非 `[A-Za-z0-9_-]` 字符换成 `_`，空串回退 `"thread"`（语义与 `checkpoint/file.go:271` 的 `safeName` 相同，**不复用、不导出**；重复问题登记在"未解决的问题 #10"，折叠后果登记在"#6"）。**（已由 ADR-0028 取代：`ac7b89a` 删除 `safeThread`，目录名即标识，不合格标识由 `core.CheckThreadID` 拒绝。）**
- 依赖方向可接受的理由写进方法注释：workspace 本来就是装配层，其包注释（`:1-16`）已声明"每个部件都来自既有包且可单独使用"。

### 验收

`go test ./workspace/`：

- `TestRunFilesLocation`：`ws.RunFiles("t1")` 写 `notes/a.md` → 断言 `<root>/.goagent/files/t1/notes/a.md` 存在，且 root 内除 `.goagent` 子树外没有多出一个文件（递归列举时跳过该子树，前置准备一个 `keep.txt` 作对照）。实现期在同一条用例里加了一项断言：`ws.FS().ReadFile(".goagent/files/t1/notes/a.md")` 读得到同一内容——这正是 `RunFiles` 注释里"产物在模型文件工具的 root 之内"那句的说法是否有依据的判据（`tool/file` 的 `read_file` 就走 `root.ReadFile`）。
- `TestRunFilesSanitizesThread`：threadID 传 `"a/b/../c"`、`x\y`、`"weird id:1"`、`""`、`"   "` → 每个都落成一层的目录名（不含 `/`、`\`、`:`，不是 `.`/`..`），产物确实位于 `<root>/.goagent/files/<清洗名>/` 之下。**（已由 ADR-0028 反转：现在这条叫 `TestRunFilesRejectsThread`，断言这些 id 全部报错且一个目录都不建。）**
- `TestRunFilesDistinctThreadsIsolated`：两个 threadID 各写同名文件 → 互不可见。
- `TestRunFilesIsNotSnapshottable`：装配层交出的句柄既不是 `core.Snapshottable` 也不是 `core.Restorable`，防止日后把产物塞进检查点行。
- `TestRunFilesHandleOutlivesWorkspace`（实现期新增）：`RunFiles` 取的句柄在 `ws.Close()` 之后仍能读写——两处 `os.Root` 相互独立，这是 `RunFiles` 注释承诺过的生命周期。
- 用例名不带 `Workspace` 前缀，与该包既有测试（`TestNewRootsAtDir`、`TestToolsAreBoundToRoot`）的体例一致；卡初稿写的是 `TestWorkspaceRunFiles*`。

---

## TASK-DS-03：端到端续跑 + 文档口径

### 改动

- `agent/filedurable_test.go` 新增两个用例（无生产代码改动，除下一项注释）：
  - `TestDurableDirStoreResumeAcrossInstances`：实例 1 以 `agent.WithRunFiles(dirStore)` 跑"写产物 → 答复"；实例 2（新 `Agent`、新 `File` checkpointer 指向同一目录、新 `DirStore` 句柄指向同一产物目录）续跑，工具读到实例 1 的产物内容。实现期在同一条用例里加了一句 `os.ReadFile(filepath.Join(artifacts, "note.txt"))`：产物本来就是普通文件，不必从检查点解包也能拿到——这正是本方案存在的理由。
  - `TestDurableDirStoreForgetsHandleYieldsEmpty`（反向用例，把未解决问题 #1 变成可执行记录）：实例 2 **不传** `WithRunFiles` 时，读到"文件不存在"。测试注释明确写出：这是外部托管语义的已知后果，框架无法区分"忘记注入"与"本来就没有"。实现期加了 `os.Stat` 断言：报告"读不到"的同时，磁盘上的产物仍在——空的是文件面，不是目录。
  - 两个用例共用本文件内的 `stashModel()` / `processOneStashes()` / `processTwoPeeks()` 三个小助手（卡初稿没写；正向与反向只差"传不传句柄"，共用可避免两份 40 行的 agent 装配）。
- `agent/agent.go` 的 `WithRunFiles` 文档注释补句（实现期扩写为：点名 `vfs.DirStore` 为"非可快照后端"的具体例子、说明"只传检查点不会恢复文件"、时间旅行不回滚这些文件、并指向本包的 `TestDurableDirStore*` 用例）。
- 与检查点交互那条测试锚点（"挂 DirStore → 线程文件无 blob、`FileSnapshot` 为 nil、不报错"）**不在本卡重复实现**：它属于 `checkpoint` 层，已由 ADR-0026 的 `checkpoint.TestFileExternalBackendNotPersisted` 钉住；本卡只需 `workspace.TestRunFilesIsNotSnapshottable` 保证装配层交出的句柄确实落在该分支上。

### 验收

- `go test ./agent/ -run Durable` 全绿：命中 7 条 = 既有的 4 条（`filedurable_test.go` 的 ADR-0026 三条 + `durable_test.go` 的 `TestDurableResumeAcrossInstances`）+ 本卡新增 2 条 + 按子串命中的 ADR-0025 用例 `TestLoopHistoryCompacterIsDurable`，证明未回归。
- `go build ./... && go vet ./... && go test ./...` 全仓绿；`go test -race ./vfs/... ./workspace/... ./agent/...` 绿。
- 手工检查：三卡合计涉及包只有 `agent`、`vfs`、`workspace`（`git diff --name-only` 逐文件核对），`checkpoint/`、`core/` 零改动 —— 用于证明"纯加法"立场成立。
- 提交切分照卡执行；DS-03 的改动只有测试与注释，`feat(agent)` 这个 type 沿用卡片点名的写法（不是 `test(agent)`），提交正文首句即声明"No behavior change"。

---

## 执行顺序与提交切分

1. TASK-DS-01 → 2. TASK-DS-02 → 3. TASK-DS-03（严格依赖顺序，DS-02 调 DS-01 构造函数）。
2. 三个 commit：
   - `feat(vfs): add a disk-backed DirStore file backend`
   - `feat(workspace): attach per-thread artifact files under .goagent/files`
   - `feat(agent): cover and document disk-backed artifact reattachment`
3. 每个 commit 前跑对应包的 `go test`，第三个 commit 后对 HEAD 跑全仓 `build/vet/test` 与相关包 `-race`（沿用 ADR-0026 的逐批校验做法）。

---

## 未解决的问题（明确列出，不含糊）

### 甲类：本方案接受并保留的限制

1. **续跑忘记注入句柄时，文件面静默变为空。** 框架无法区分"这个线程本来没有产物"与"调用方忘了传目录句柄"：前者与后者的可观察结果完全相同。本方案只做到"文档写明 + 反向测试记录"，没有做提示机制。要真正解决需要一个额外事实来源（例如在检查点或 `State.KV` 里记"本线程使用外部托管后端 + 目录标识"），那是独立设计，需另行 ADR。
2. **产物不随检查点版本化。** 时间旅行、分支、`Fork` 回滚 `State.Messages` 时不会回滚磁盘文件；并行分支写同一路径会互相覆盖。选 DirStore 就是把"文件跟着检查点走"换成"文件跟着目录走"，代价不可消除。
3. **无法删除产物。** `core.FileStore` 没有删除接口（前置事实第 1 条），`DirStore` 与 `InState` 都只能覆盖写；产物目录单调增长。补删除属于对 `FileStore` 的 API 扩张，本方案明确不做（ADR-0026 同一立场）。
4. **无大小上限、无配额、无清理。** 写满磁盘由操作系统负责；线程废弃后目录残留（当前框架没有"删除线程"的概念）。`List` 走 `fs.WalkDir`，产物极多时该调用是 O(文件数) 的目录遍历，未做分页/缓存。
5. **同线程跨进程并发写未定义。** 无文件锁。Windows 上 `os.Rename` 覆盖被其他进程打开的目标文件可能失败（共享模式限制），Linux/macOS 通常成功——即同一份代码在两平台行为不完全一致，属于已知平台差异，只在注释里说明，不做重试或降级。
6. **线程名折叠会让只差非法字符的 id 共用一个产物目录**（实现期发现）：`safeThread("a/b")` 与 `safeThread("a_b")` 同为 `a_b`，于是两条线程看得见、也能覆盖对方的产物。检查点给自己的文件命名用的是同一条规则（`checkpoint/file.go:271`），所以这不是 DirStore 新引入的。处置：写进 `safeThread` 注释；不加消歧后缀——那要么只改一处造成两处选址规则分叉，要么同时改检查点命名，属独立决定。**后续已开 ADR-0028**：实测表明同一条折叠会让两条线程写进**同一个 `.jsonl`** 且 `Latest` 互相读到对方的对话历史（`checkpoint/file.go:212-235` 的 `readAll` 不校验记录归属），严重度高于产物目录共用，故从"登记不修"升级为待落地修复。**已修复**：ADR-0028 用"严格校验 + 报错拒绝"替掉清洗（`6abfd2f` 删 `safeName`、`ac7b89a` 删 `safeThread`），并补上读取侧的归属过滤（`6eb6d3c`）；折叠这一整类问题不再存在。

### 乙类：需要独立设计才能解决

7. **默认后端（`InState`）的两个上限仍在**：每步全量深拷贝（`vfs/instate.go:72`）与单行 16MB（`checkpoint/file.go:157`）。改用 DirStore 可以绕开，但如果一个使用方**同时**要"超大产物"和"文件随检查点回滚"，本方案给不出答案。唯一干净路线是把 blob 从线程 JSONL 移到独立 blob 目录、检查点只存哈希 —— 那是替换 ADR-0026 的存储介质，必须另写 ADR（已记入 0027 未决问题）。
8. **运行期没有任何机制会往 `State.Files` 写东西**（前置事实最后一条：唯一写入方是本轮新增的测试工具）。因此 0026 与 0027 都在完善"文件面的持久化能力"，但**文件面本身仍是空的**。要让它产生实际价值，还缺一个上游决策：是否实现"大工具结果自动外置到 `State.Files`、模型上下文里只留摘要与路径"这套 deepagents 式 offload 机制（涉及 `agent/execTools` 阶段的截断策略、`prompt` 侧如何告知模型、以及子代理共享语义）。这是独立特性，不属于本实施方案范围。
9. **`List` 与工具层的可见性不统一**：`tool/file` 暴露的是 `os.Root` 上的四个模型可见工具（`read_file`/`write_file`/`list_dir`/`glob`，`workspace.Tools()`），而 `State.Files` 目前**没有对应的模型可见工具**。选址在 root 内让模型可以用 `read_file`/`write_file` 直接够到产物目录（DS-02 已断言），但 `DirStore` 自己的 `Read/Write/List` 没有模型入口，二者语义也不同（`read_file` 拒绝二进制、有读取上限）。是否补一组 `vfs` 型模型工具（`stash`/`fetch_artifact`）属独立决策。

### 丙类：整洁性遗留

10. **三份近似重复的小工具**：路径校验在 `tool/file/file.go:164,184` 与本方案 `vfs/dir.go` 各一份；目录名清洗在 `checkpoint/file.go:271` 与本方案 `workspace.safeThread` 各一份。本方案选择不抽 `internal/pathsafe`（理由见 ADR-0027 备选 C：为两处约 20 行的私有校验扩大导出面不划算）。留给后续一次独立整理。**（部分已由 ADR-0028 解决：两份目录名清洗都不存在了，判定统一到 `core.CheckThreadID`；`tool/file` 与 `vfs/dir.go` 的*路径*校验重复仍在。）**
11. **16MB 这个数值的可发现性差**：它藏在 `checkpoint/file.go:157` 的 `sc.Buffer` 参数里，超限后的表现是"线程文件解析失败"而不是"某条产物太大"。本方案不改它，但至少应在 DS-03 之后，把该限制写进 `checkpoint` 包注释（与 DirStore 的适用场景并列说明）。

---

## 明确不做

- 不改默认后端（`agent/agent.go:117-127` 的兜底顺序一字不动）。
- 不让 `DirStore` 实现 `Snapshottable`（否则等于把大产物重新写进线程 JSONL，第 7 条限制会以更糟的形式回来）。
- 不做产物 TTL/GC、不做跨进程文件锁、不给 `FileStore` 加 `Delete`。
- 不实现大工具结果自动外置（乙类第 8 条），不新增 `vfs` 的模型可见工具（第 9 条）。
- 不动 `checkpoint/` 与 `core/` 的任何行为（只做可能的注释补充）。**（已由 ADR-0028 突破：那两张卡各自的行为都改了——`checkpoint` 增加标识校验与归属过滤，`core` 新增 `CheckThreadID`。本方案自身交付的 `vfs/` 未受影响。）**

## 风险

| 风险 | 概率/影响 | 处置 |
| --- | --- | --- |
| 使用方以为"挂了 DirStore 就自动持久"，续跑没传句柄，产物面为空且无提示 | 中 / 中 | 未解决问题 #1 的第一条缓解：`WithRunFiles` 注释 + `TestDurableDirStoreForgetsHandleYieldsEmpty` 把该行为写成测试事实；真正解决需另行设计 |
| Windows 上原子替换失败（目标被另一进程持有）导致写工具报错 | 低 / 中 | 在 `DirStore.Write` 注释中说明平台差异；不重试、不降级为直接覆盖（会破坏"读者不会看到半截文件"的立场） |
| `fs.WalkDir` 在产物极多时变慢，被高频调用 `List` | 低 / 低 | `DirStore.List` 目前没有模型可见入口（未解决问题第 9 条），暴露面小；记录为待观察项，不预先优化 |
| 只差非法字符的两个 threadID 折叠进同一产物目录，互相看见/覆盖 | 低 / 中 | 已消除：ADR-0028 不再清洗目录名，`RunFiles` 与检查点都用 `core.CheckThreadID` 拒绝不合格标识（`6abfd2f`/`ac7b89a`），`TestRunFilesRejectsThread` 与 `TestFileRejectsUnsafeThreadIDs` 固定该行为 |
| 误给 `DirStore` 加上 `Snapshottable`，使大产物进入线程 JSONL 并触发 16MB 读不回 | 低 / 高 | `TestDirStoreIsNotSnapshottable` 直接断言该能力不存在，回归即失败 |
| `RunFiles` 返回具体类型使日后想换后端需改调用点 | 低 / 低 | 调用点只是传给 `agent.WithRunFiles`（收接口），换后端只改这一行；workspace 现已 import `core`（ADR-0028 的标识校验），但返回类型仍是具体类型 |
