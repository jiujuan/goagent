# ADR-0028: 线程标识必须是文件名安全串（严格校验，撤销清洗）

- 状态: Accepted（四卡全部落地：TI-01 `6eb6d3c` + `e5d2de6`、TI-02 `6abfd2f`、TI-03 `ac7b89a`、TI-04 文档收尾见"实施切分"第 4 条）
- 日期: 2026-09-30
- 关联: ADR-0026（`State.Files` 快照）、ADR-0027（`vfs.DirStore` 与 `workspace.RunFiles`；本 ADR 处理其执行卡登记的"threadID 折叠"一项，并取代其中"两处各自清洗、不抽公共包"的结论）

## 背景

线程标识完全由调用方给定（`agent/agent.go:92` 的 `OnThread`，缺省值 `core.NewID("thread")` 在 `agent/agent.go:122`）。两个存储位置把它**先清洗再当文件/目录名**（下面两条是本 ADR 决策前的代码状态，两个清洗函数已随 TI-02/TI-03 删除）：

- 检查点：`checkpoint/file.go:55-56` 用 `safeName(threadID)+".jsonl"`（`safeName` 见 `:271-285`，把非 `[A-Za-z0-9_-]` 的字符逐个换成 `_`，空串换成 `"thread"`）。
- 产物目录：`workspace/workspace.go:215` 的 `RunFiles` 用 `safeThread`（`:230-245`，同规则）拼 `<root>/.goagent/files/<name>`。

清洗是多对一映射，于是不同标识会共用同一个文件名。实测（真 `checkpoint.File`，两条标识只相差一个 `/`）：

```
checkpoint file: tenant_a.jsonl          ← 只有一个文件
Latest("tenant/a") -> cp=c2 cpThread="tenant_a" firstMsg="conversation B"
History("tenant_a") len: 2               ← 两条线程的记录混在同一文件里
```

根因有两条，缺一不可：①命名映射不是一一对应；②读取侧不校验归属 —— `File.Load`/`Latest`/`History`（`checkpoint/file.go:238-263`）全部经由 `readAll`（`:212-235`），而 `readAll` 把该文件里的记录不加区分地返回。因此 `agent/agent.go:143` 与 `agent/hitl.go:65` 的续跑会静默接上另一条线程的对话历史。

结论：**本 ADR 用"严格校验 + 拒绝"取代"清洗 + 消歧后缀"，并保留读取侧归属校验。** 这是**破坏性更新**，不迁移老数据。

## 名词

- **文件名安全串**：整体由 `[A-Za-z0-9_-]` 组成、长度在上下限内的标识。
- **归属校验**：读取时只承认 `Checkpoint.ThreadID` 等于所查标识的记录。
- **折叠**：两个不同标识得到同一个文件名（本 ADR 要消除的对象）。

## 决策概览

| 项 | 决策 |
| --- | --- |
| 规则 | 标识必须整体匹配 `[A-Za-z0-9_-]`，长度 1..64；否则**报错**，框架不再替换任何字符 |
| 缺省值 | 不传 `OnThread` 时用 `core.NewID("thread")`（本身合规）；不再有"空串当作 thread"的兜底 |
| 读取侧 | `readAll` 增加归属校验，三个读方法统一生效（先行落地，独立可上线） |
| 校验落点 | agent 层早报错（友好）+ `checkpoint.File` 的 `path()` 单点拒绝（`Save` 与 `Load`/`Latest`/`History` 都经过它）+ `workspace.RunFiles` 拒绝。**读写一律拒绝**：标识不再被改写，非法 id 会解析出目录外的路径，所以读侧也不能放过 |
| 规则归属 | 判定函数放 **`core`**（`core.CheckThreadID(id string) error`，与生成缺省标识的 `core.NewID` 同处 `core/id.go`）；**不**新建 `internal/threadid`，**不**新增 `agent.CheckThreadID` |
| 非 ASCII | 拒绝（取舍与否决理由见"备选 D"） |
| 消歧后缀 | **撤销**（本 ADR 上一版方案，理由见"备选 A"） |
| 老数据 | 不迁移、不提供接续工具，接受非法标识线程的数据失联 |

## 设计细节

### 1. 规则放 `core`，不新建 `internal/threadid`

线程标识**已经是 `core` 的词汇**：`core.RunStarted` 就带 `ThreadID` 字段（`core/event.go:19`、`core/eventjson.go:21/43/103`），而缺省标识由 `core.NewID("thread")` 生成（`core/id.go:11-15`、消费点 `agent/agent.go:122`）。也就是说"生成"和"消费事件"都在 core 一侧，只有"判定"散落在 `checkpoint` 与 `workspace` 各写一份。所以本 ADR 把判定补进同一处：

```go
// core/id.go
const MaxThreadIDLen = 64

// CheckThreadID reports whether id can be used as a thread's file and directory
// name verbatim: only [A-Za-z0-9_-], 1..MaxThreadIDLen bytes.
func CheckThreadID(id string) error
```

选择 `core` 而不是 `internal/threadid` 的三条理由：① 少一个包和一层转调（若规则藏在 `internal`，为了外部使用方（HTTP handler、队列生产侧）仍能自行判定，还得在 `agent` 或 `core` 再导出一个公开谓词，等于同一件事写两处）；② `checkpoint`/`agent`/`bus`/`eval` 本来就依赖 `core`，没有新增依赖边；③ 标识规则是对使用方的**契约**，放进 `core` 才是它应有的可见度，藏在 `internal` 会让契约只存在于文档里。

代价如实记录：`core` 是公开 API，这个函数一旦导出，字符集与长度就成了对外承诺（要改就得走新的 ADR）；另外 `workspace` 会因此新增一个 `core` import —— 这与 ADR-0027 执行卡里"workspace 当前不 import core"那条事实相反，但当时的拒绝针对的是"为一个类型名引入依赖"，这里是"为契约本身引入依赖"，两者不同，此点在第 5 节记为修订。

字符集选 `[A-Za-z0-9_-]` 的理由：它正是今天 `safeName`/`safeThread` 里"保持不变"的那个集合。用同一集合做**准入判定**后，任何通过的标识经过（已不再存在的）清洗也是恒等变换，标识 ↔ 文件名成为一一对应，折叠在结构上不可能发生；`.`、`..`、`/`、`\`、`:`、空格、控制字符因为不在集合内而被一并挡下，目录逃逸与"点名工作目录"这两类问题同时消失。

长度上限 64：集合内全是 ASCII 单字节，64 字节名对任何目标文件系统的 255 限制都很宽裕，且给前缀留余地 —— 缺省的 `thread_<16位hex>` 是 23 字符，`thread-` + 一个 UUID 是 43 字符。下限 1 排除空串。

### 2. 为什么取消"空串换成 thread"的兜底

兜底本身是一次多对一映射：所有以空串入场的线程都会共用 `thread.jsonl`。既然已经要求"标识必须由调用方给出且合规"，空串就该是错误，而不是一个匿名公共线程。缺省标识由 `RunConfig` 生成，不靠命名层补。

### 3. 落点与错误时机（逐路径）

| 路径 | 位置 | 行为 |
| --- | --- | --- |
| `Run` / `Stream` / `queue.EnqueueAgent` | `agent/agent.go:139`（restore 决策处，先判定再读检查点） | `Stream` 没有 error 返回值（`OnThread` 是 `func(string) RunOption`，`:92`），所以错误照既有机制挂在 run 上（`run.startErr`，`agent/agent.go:153`），由 `Run`/`Wait` 返回 |
| `Agent.Resume` | `agent/hitl.go:64` | 直接返回 `(nil, error)` |
| 队列 | `queue/agent_bridge.go:35-36` 内部调 `a.Stream` | 入队仍会成功，错误出现在消费侧的 run 结果里；要"入队即失败"请用 `core.CheckThreadID` |
| 直接用 checkpointer | `checkpoint/file.go:87` `Save`（经 `:127` 的 `f.path(cp.ThreadID)`） | 返回 error，拒绝写入 `cp.ThreadID` 非法的检查点 —— agent 层之外的不可绕过保证 |
| 直接用 checkpointer 读 | `checkpoint/file.go` 的 `path()`（`Load`/`Latest`/`History` 经 `readAll`→`scan` 到它） | **也拒绝**，返回 error（见下面的"实现期发现"） |
| 产物目录 | `workspace/workspace.go:215` `RunFiles` | 返回 `(*vfs.DirStore, error)`，非法标识直接报错且不创建任何目录 |

错误文案要能自解释：指明"只允许 `[A-Za-z0-9_-]`，长度 1..64"，并给出被拒绝的标识（不打印整段请求）。

### 4. 读取侧归属校验保留的理由

`readAll` 一处过滤（`:212-235`）即可覆盖三个读方法，它兜住两件校验兜不住的事：① 改造前已经混写在一起的遗留 `.jsonl`；② 第三方 `Checkpointer` 实现，或将来新出现的按线程派生的存储路径。`Save` 一直用 `cp.ThreadID` 定位文件，正常单线程使用下过滤不会改变任何行为。

### 5. 被本 ADR 收编的两个旧结论

- ADR-0027 执行卡"未解决问题 #6"（threadID 折叠）：由本 ADR 修复。
- ADR-0027 备选 C"两处各写一份私有校验，不抽公共包"：**被取代**。现在需要的不是同一份字符串处理，而是同一份**准入判定**，两处必须逐字一致，因此判定上收到 `core.CheckThreadID`（见第 1 节），`checkpoint.safeName` 与 `workspace.safeThread` 两个清洗函数删除，调用点直接使用标识本身。
- ADR-0027 执行卡"前置事实"里"`workspace` 当前不 import `core`"：**事实会变**（TI-03 之后 workspace 依赖 `core`）。当时刻意回避的是"为一个类型名引入依赖"（那条仍成立，`RunFiles` 依然返回 `*vfs.DirStore` 而不是 `core.FileStore`）；本次引入依赖是为了调用契约判定函数，性质不同，记录在此以免后来者误以为违背了既有立场。

### 6. 实现期发现：清洗曾在无意中挡住了路径穿越

`File.path()` 原先是 `filepath.Join(dir, safeName(threadID)+".jsonl")`，而 `safeName` 把 `.` 也换成 `_`，所以 `"../../escape"` 变成 `"________escape.jsonl"`，永远留在 store 目录内。这一保护不是它的设计目的（函数注释只说是"filesystem-safe base name"），但去掉清洗、把标识当名字用之后，它就是唯一剩下的防线——因此判定**必须同时覆盖读写两侧**，只拒绝写入会留下一个能任意指定读取路径的入口。冒烟已验证：`Save` 与 `Latest` 对 `"../../escape"` 都返回 `core: thread id ... contains '.'`，store 的上级目录没有多出任何文件。

同一缺陷在 workspace 侧也存在，且 TI-03 实测到具体后果：去掉 `safeThread` 而不加校验时，`RunFiles("..")` 会拼出 `<root>/.goagent` 并成功返回一个绑定在**配置目录本身**上的产物存储（`.`、`x\y`、空串同样通过）；加了校验后这些标识在创建目录之前就被拒绝。也就是说 `os.Root` 只保证"操作不出 store 根"，store 根落在哪里仍由命名决定 —— 校验不能被它替代。

## 范围

### 做

- `core/id.go`：`MaxThreadIDLen` + `CheckThreadID` + 表驱动测试。
- `checkpoint`：`readAll` 归属校验；`Save` 拒绝非法 `cp.ThreadID`；删除 `safeName`，文件名直接用标识。
- `agent`：`Stream`/`Resume` 早报错（调用 `core.CheckThreadID`）。
- `workspace`：`RunFiles` 校验并拒绝，删除 `safeThread`；改写既有那条断言"非法 id 也能建目录"的测试。
- 文档：`OnThread`、`WithRunFiles`、`RunFiles`、`checkpoint` 与 `workspace` 包注释统一写清"标识会被当作文件名/目录名使用"。

### 不做（理由）

- **不迁移、不提供接续工具**：这是破坏性更新，见"迁移与兼容性"第 2 条给出的手工做法。
- **不做"发现混写文件即报错"的门禁**：把静默问题变成运行期新失败，收益不抵风险；校验后新写已不可能折叠。
- **不做线程生命周期 API**（创建/列出/删除、占用检查）：与折叠无因果关系，见"备选 F"。
- **不做队列"入队即失败"**：错误时机改动涉及 `queue` 的公开契约，交给外层用 `CheckThreadID` 自助。
- **不改 `OnThread` 的签名**、不给 `FileStore` 加 `Delete`、不处理 POSIX 盘符差异（ADR-0027 立场不变）。

## 备选方案

- **A. 保留清洗，只在清洗改变原值时追加短哈希。** 本 ADR 上一版的选择，否决：调用方永远看不到"你的标识不合格"这个事实，折叠只是被哈希掩盖；而且两处清洗规则必须逐字一致才安全，留下的复杂度比直接判定更多。
- **B. 只做读取侧归属校验。** 最小改动、零迁移、当天可绿。否决为终态：两条线程仍共用同一个 `.jsonl` 与同一个产物目录，追加写互相影响、按目录清理与排错彼此牵扯，折叠的根还在。
- **C. 采用：严格字符集校验 + 拒绝 + 读取侧归属校验。** 采纳（用户拍板，并接受破坏性变更与不迁移老数据）。
- **D. 允许 Unicode 字母/数字，只禁路径语法字符。** 能保住中文等非 ASCII 线程名。否决：选择严格 ASCII 集合才有"标识 = 文件名"的一一对应，放宽字符类就要重新讨论映射是否单射。**代价如实记录**：今天用非 ASCII 标识的调用方改后会直接报错。附带事实是这类标识今天的下场更糟 —— `safeName` 把每个非 ASCII 字符换成 `_`，`会话一` 与 `会话二` 折叠成同一个名字并互相串读历史，所以拒绝是改进而非倒退。
- **E. 让 `OnThread` 当场返回 error。** 实现上做不到：`OnThread` 返回 `RunOption`（`agent/agent.go:92`），没有 error 通道；改签名对全体使用方的破坏面比"错误延后到 run 启动"更大。
- **F. 维护线程注册表以保证"唯一"。** 否决：线程标识**必须可复用**才能续跑（`examples/agent-tutorial/main.go:255-265` 两次 `Run` 传同一个 `OnThread(thread)`，第二问才引用得到第一问）；"每个线程一个不重复的新值"恰恰会取消这个特性。真正需要的"唯一"是"标识与线程一一对应"，那是字符集校验给的性质。
- **H. 新建 `internal/threadid` 承载判定。** 上一版本 ADR 的选择，否决：`core` 已经带着这个概念（`core.RunStarted.ThreadID`，`core/event.go:19`）也带着生成缺省标识的 `core.NewID`，判定放在别处反而分裂；更实际的问题是 `internal/*` 不能被模块外的使用方导入，为了让 HTTP/队列那一层仍能自行判定就必须在 `agent` 或 `core` 再导出一个公开谓词，同一件事写两处。少一个包、少一层转调，直接把契约放 `core` 更省。
- **G. 对所有标识一律加哈希后缀。** 否决：会把 `h1`、`thread_<hex>` 这类本就合规的名字改掉，使全部现存检查点目录失联，只省掉一个 if。

## 迁移与兼容性

1. **仓库内既有标识全部合规，测试与示例不受影响**（已逐个核对字面量：`artifacts`、`h1`、`hitl-demo`、`mw`、`mw2`、`notes`、`notes2`、`s2`、`t1`、`chat-1`、`job-1`、`settle-A1001`、`fmt.Sprintf("job-%d", i)`，以及缺省 `thread_<hex>`）。
2. **非法标识从"静默折叠"变成"报错"**：`Run`/`Wait` 与 `Resume` 返回错误、`RunFiles` 返回错误、`File` 的 `Save`/`Load`/`Latest`/`History` 对非法标识返回错误。覆盖含 `/ \ : 空格 .` 的标识、空串、超过 64 字节的标识、以及非 ASCII 标识。
3. **老数据的下场**（用户决定：不做旧名兼容，一律拒绝）：
   - 标识本来就合规的线程：文件名不变，继续读得到；归属过滤让它们只看见自己的记录。
   - 标识非法的线程：老文件在磁盘上叫清洗后的名字，而新规则下**读写都拒绝该标识**，所以其历史既读不到也接不上，即使有人改用合规标识也拿不到（名字不同）。数据仍在盘上，可人工查看文件内容。
   - 曾与他人折叠在同一文件的：合规那条因归属过滤恢复正常；非法那条同上。
   **不提供迁移工具或命令**：把老记录的 `thread_id` 改写成合规标识需要改写检查点内容，与 `Checkpointer.Save` 的 append-only 契约（`checkpoint/checkpoint.go:47`）冲突，本 ADR 明确不做。
4. **`File.path()` 是读写的单点闸口**：`Save` 直接经它，`Load`/`Latest`/`History` 经 `readAll`→`scan` 到它（TI-02 实现），因此"绕过 agent 直接用 checkpointer"也拿不到非法标识的数据 —— 这也是第 6 节那条穿越问题的防线所在。
5. **必须同步修改的既有测试**（两条都已随对应 TASK 改完）：① TI-03 把 `workspace/runfiles_test.go` 的 `TestRunFilesSanitizesThread`（原断言 `"a/b/../c"`、`x\y`、`"weird id:1"`、`""`、`"   "` 都能成功建目录）改写成 `TestRunFilesRejectsThread`：八个非法标识（含 `..`、`.`、`会话一`）都报错、`.goagent` 一个都不建、`a_b` 合法而 `a/b` 被拒；② TI-02 把 `TestFileFoldedIdsKeepSeparateHistories` 改写成 `TestFileRejectsUnsafeThreadIDs`（写入与读取都拒绝、非法标识不留文件、合规标识文件名逐字等于标识）。
6. 公开 API 变化只有新增：`core.CheckThreadID` 与 `core.MaxThreadIDLen`（`core/id.go`）；`core`/`vfs` 既有契约不动，`agent`/`checkpoint`/`workspace` 的方法签名不变（只是多了返回 error 的情况）。不新建 `internal` 包。

## 测试锚点

- `core`：合规表（`h1`、`notes`、`thread_<16hex>`、`thread-`+UUID、64 字符边界）与不合规表（`""`、`" "`、`"."`、`".."`、`"/abs"`、`"a/b"`、`x\y`、`"id:1"`、`"会话一"`、65 字符）；同一标识两次判定结果一致；错误文案含字符集与 `MaxThreadIDLen`。
- `checkpoint`：`Save` 非法 `cp.ThreadID` → 返回 error 且目录里不新增文件；两个今天会折叠的标识 → 其一被拒，另一个的文件名逐字等于标识；手工构造"同一 `.jsonl` 内混两种 `ThreadID`"的遗留文件 → 各读各的、`Load(非法但历史的 checkpointID)` 不再命中他人记录；既有 `durable_test.go`/`filesnapshot_test.go` 全绿（无碰撞时行为与改前逐字一致）。
- `agent`：`OnThread("tenant/a")` → `Run` 返回错误且错误文案含字符集与长度约束；`Resume("bad/id")` → error；错误来自 `core.CheckThreadID`（agent 只做转达，不再自定一套文案）。
- `workspace`：`RunFiles` 对非法标识返回 error 且 `<root>/.goagent` 下不新增任何目录；合规标识下目录名逐字等于标识（不再有 `_` 替换的痕迹）。
- 端到端：两条只相差非法字符的线程各自续跑，历史互不串（补 `agent/filedurable_test.go` 用例）。
- 回归：全仓 `go build/vet/test ./...`，`core`/`checkpoint`/`workspace`/`agent` 的 `-race`。

## 实施切分

1. **TASK-TI-01**：`checkpoint/file.go` 的 `readAll` 归属校验 + 遗留混写文件测试。可独立上线，先落。→ 已落地 `6eb6d3c` + 补测 `e5d2de6`（`checkpoint/threadisolation_test.go` 四条用例：混写文件各读各的、`Save` 产生的折叠文件两条标识互不接错、单一线程行为逐字不变、被过滤掉的记录不再因自身索引缺失 blob 而拖垮他人的读取。四条中前三条与最后一条都做过反向验证：把过滤改回旧行为时它们分别失败，断言有效）。
2. **TASK-TI-02**：`core/id.go` 增加 `MaxThreadIDLen` 与 `CheckThreadID`（含表驱动测试）；`checkpoint.File.Save` 写侧拒绝、删除 `safeName`；`agent.Stream`/`Agent.Resume` 早报错。→ 已落地 `6abfd2f`，与卡片的三点差别：① 拒绝点做成 `File.path()`（读写都经它，见第 4/6 节），不只 `Save`；② `agent` 侧同时给 `OnThread` 补了契约文档（原属 TI-04，因同一提交里就出现了新报错而提前）；③ TI-01 的折叠用例改写为拒绝用例。
3. **TASK-TI-03**：`workspace.RunFiles` 调用 `core.CheckThreadID` 并拒绝非法标识、删除 `safeThread`；反转 `runfiles_test.go:77-107` 那条用例；补 agent 端到端用例。→ 已落地 `ac7b89a`（`safeThread` 删除、`workspace` 因此新增 `core` import 并去掉已无用的 `strings`；拒绝发生在建目录之前，故非法标识不留任何目录；agent 侧的补测是给 `TestUnsafeThreadIDFailsTheRun` 挂上 `File` checkpointer 并断言拒绝后 store 目录为空 —— 该性质在 TI-02 只有"模型没被调用"，现在连"没写检查点"也钉住了）。
4. **TASK-TI-04**：文档收尾 —— `OnThread`/`WithRunFiles`/`RunFiles`/`checkpoint`/`workspace` 包注释写明标识即文件名；把 ADR-0027 执行卡"未解决问题 #6"与 `workspace.safeThread` 注释里的"折叠不修"改指向本 ADR 结论。→ 已落地 `d107b62`（提交信息 `docs: record the thread id naming contract`）：① `checkpoint/checkpoint.go` 包注释新增"标识会被持久化后端当作名字"一段并指向 `core.CheckThreadID`；② `checkpoint/file.go` 的 `File` 类型注释写明一线程一文件、文件名逐字等于标识、非法标识在 `Save`/`Load`/`Latest`/`History` 都被拒绝；③ `workspace/workspace.go` 包注释补 `RunFiles` 一段（选址来源 + 标识兼作目录名），并把 `vfs` 加进"来自既有包的部件"清单；④ `agent/agent.go` 的 `WithRunFiles` 注释补"后端要与它所挂载的线程对应，两半都由同一个标识定位"；⑤ `docs/adr/ADR-0027-dir-store.md` 五处（§5 清洗→拒绝、测试锚点、实施切分、未决问题的折叠与 POSIX 两条）与 `docs/plan-dir-store-artifact-backend.md` 十二处（前言"后续修订"一条总述 + 前置事实 `safeName`/workspace import 两条、总立场第 3 条、DS-02 代码块引导句与 `safeThread`/`TestRunFilesSanitizesThread` 三条、未解决问题 #6/#10 两条、"明确不做"的 checkpoint/core 条、风险表两行）标注本 ADR 结论；原描述一律就地加粗标注而非删除，以便后来者看到当时的判断依据。收尾时另发现并修掉一处 TI-02 留下的缺陷：`agent/agent.go` 的 `OnThread` 注释被**重复了两遍**（旧的三行版本没有删掉，改写后的版本接在它后面），已在 `e1ff77e`（`docs(agent): drop the duplicated OnThread comment left by ADR-0028`）删除；随后对 ADR 全系列改过的 Go 文件做了"相邻重复注释行"扫描，无第二例（`gofmt`/`go vet` 抓不到这类重复，故单列）。

顺序：TI-01 → TI-02 → TI-03 → TI-04（TI-01 与 TI-02 改同一文件，串行以免互踩）。
提交切分（实际落地）：`fix(checkpoint): only read a thread's own checkpoints`（`6eb6d3c`）、`test(checkpoint): cover that a foreign record cannot break a thread's read`（`e5d2de6`）、`feat(core,checkpoint,agent): require file-name-safe thread ids`（`6abfd2f`）、`fix(workspace): reject unsafe thread ids in RunFiles`（`ac7b89a`）、`docs: record the thread id naming contract`（`d107b62`）。

## 未决问题

- 长度上限 64 是否需要做成可配置（或放宽到 128）：取决于是否有"给标识加业务前缀"的外部习惯，本 ADR 先定 64 并在错误文案里写明。
- 队列路径的错误时机是否值得改到入队即失败：涉及 `queue` 的公开契约，留给后续需求。
- 是否需要公开的线程生命周期 API（创建/列出/删除线程）来替代"标识只是一个自由字符串"的模型；ADR-0027 未决问题第 4 条（无线程删除概念、产物目录不清理）会一并被它解决。
- 遗留 `.jsonl` 的离线改写工具：本 ADR 明确不进库，若确有大量此类数据再单独讨论。
