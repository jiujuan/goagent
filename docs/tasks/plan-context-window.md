# 实施方案：上下文窗口的三种裁剪策略（ADR-0031）

- 范围：`middleware/window.go`（新建）、`middleware/compaction.go`（仅提私有 helper）、`core/event.go`、`core/eventjson.go`、`examples/context-window`、`examples/context-window-relevance`
- 日期：2026-10-08
- 进度：**CW-01 `b2d6222`、CW-02 `13404bb`、CW-03 `b71ebbc`、CW-04 `e3893c4`、CW-05 `c98beb9` + `fd8f7a8` + `14b3b3a` 全部完成（均未推送）**。本卡收尾；各 TASK 的验收记在本卡对应小节的完成记录里。
- 设计文档：`docs/adr/ADR-0031-context-window-strategies.md`
- 前置依赖：无。ADR-0025（Compaction 的 Counter/Persist/校准）与 ADR-0023/0024/0030 都已落地，本卡只复用它们的既有挂载点，不改循环。

## 前置事实（逐条 grep 核实）

**挂载点与循环阶段**

- `agent.Middleware` 六钩子：`agent/middleware.go:22-29`；可选能力 `HistoryCompacter`：`:134-145`，接口体 `CompactHistory(lc, history []core.Message) []core.Message`。
- `agent.BaseMiddleware` 提供六个空实现：`agent/middleware.go:149-167`。
- `Stack.ModifyRequest` 正序遍历：`agent/middleware.go:187-194`；`Stack.CompactHistory` 正序折叠、非实现者跳过：`:257-264`；`Stack.AfterModel` **逆序**：`:266-277`。
- 循环里的一step时序：`lc := &LoopContext{...}`（`agent/loop.go:154`）→ `history = l.mw.CompactHistory(lc, history)`（`:165`）→ `lc.History = history`（`:166`）→ `req := &llm.Request{...}`（`:178`）→ `lc.Request = req`（`:180`）→ `l.mw.ModifyRequest(lc, req)`（`:181`）→ `l.mw.AfterModel(lc, finalResp)`（`:197`）→ `rc.State.Messages = history` + `l.checkpoint(rc, step, nil)`（`:259-260`）。
  - 结论一：`CompactHistory` 阶段 `lc.Request` 仍为 nil。
  - 结论二：`lc.Request` 与 `req` 同一指针，`ModifyRequest` 改 `req.Messages` 后 `AfterModel` 里读到的是改写后的最终消息集。
- 中间件栈随 loop 构造一次、跨 run 共享：`agent/loop.go:45`（`newLoop`）、`:66`（`mw: NewStack(c.middleware...)`）。因此策略性状态不放实例字段，与 ArgGuard/LoopGuard 同一立场。

**Compaction 里可直接复用的件**

- `TokenCounter` 公开类型：`middleware/compaction.go:17`。
- `estimateTokens`：`:247-270`；`textTokens`：`:272-285`；`isWideRune`：`:287-293`（同包私有，新文件可直接调用）。
- 校准读写：`loadCalib`（`:205-218`）、`loadCalibDefault`（`:220-225`）、`saveCalib`（`:227-229`）、`clamp`（`:231`）。`saveCalib` 整块覆盖 `KV["_compaction"] = map[string]any{"calib": v}` —— 同键第二个写者会擦掉前者，所以 Window 必须用 `loadCalibAt`/`saveCalibAt` 形式的带键版本（CW-01 做）。
- 校准算法：`middleware/compaction.go:158-173`（比值、限幅 0.5~2.0、`0.7*prev+0.3*ratio` 的 EMA）。既有覆盖：`middleware/compaction_internal_test.go:65`（`TestCompactionCalibration`）、`:108`（`TestCompactionCalibrationIgnoresMissingUsage`）。
- `safeCut`：`:146-152`，只在连续尾段边界上回退，非连续选择用它不够。
- 双模式互斥的既有断言：`middleware/compaction_test.go:27`（`TestCompactionMutualExclusion`）、`:52`（persist 真的缩小 `State.Messages`）、`:107`（request 模式不动 State）。
- 工厂不加 `New` 前缀的先例：`middleware/compaction.go:56`、`middleware/circuit.go`、`middleware/rag.go:28`。

**配对完整性的事实（本卡最大风险来源）**

- `ToolCall{ID,Name,Args}` 与 `ToolResult{CallID,Name,Content,IsError}`：`core/message.go:64-77`；`Message{Role,Parts}`：`:23-26`；助手侧取调用的辅助方法 `Message.ToolCalls()`：`:112`。
- provider 原样搬运 CallID、不校验配对：`llm/openaicompat/openaicompat.go:178-187`（转 `role:"tool"`+`tool_call_id`）、`llm/anthropic/anthropic.go:173-185`（转 `tool_result`+`tool_use_id`）。
- 循环侧的相邻保证：assistant 入历史 `agent/loop.go:203`；其全部结果打包成**一条** `RoleTool` 消息紧随入历史 `:256`；max_tokens 截断分支同样成条追加 `:222`。
- **悬挂调用是合法状态，不能补合成结果**：HITL 的 Interrupt 分支只落 assistant 调用消息 + `PendingHITL`，不带结果（`agent/loop.go:236-244`，那里的注释 "Persist history (incl. the assistant tool-call message) plus the still-pending calls"）；恢复时由 `runResumed` 追加结果（`agent/loop.go:119-128`、`agent/hitl.go:116`）。
- 这条推翻了需求分析阶段的口头结论（"缺结果补 `IsError` 占位"），更正记在 ADR-0031 §3 修复段与备选 E。

**打分可用的件**

- `lastUserText(msgs []core.Message) string`：`middleware/rag.go:66-73`，同包可直接调用。
- 关键词重叠打分的既有实现思路：`middleware/rag.go:85-110`（小写折叠 + `strings.Fields` + 命中计数 + 稳定排序）。
- sha256 + 规范化 JSON 取短哈希的本包先例：`middleware/loopguard.go:341-346`；同法另见 `memory/memory.go:80`。
- `embeddings.Embedder` 接口（只有 `Embed(ctx, texts) ([][]float32, error)`）：`embeddings/embeddings.go:11-15`；离线实现 `embeddings/mock`：`New()`/`NewDim(dim)`/`Embed`，见 `embeddings/mock/*.go:20-44`。
- cosine 相似度的既有实现在 `memory` 包但是私有：`memory/inmemory.go:172`（`rank` 在 `:153-167`）。middleware 包**不引** memory/embeddings 依赖，CW-05 的示例自带约 10 行 cosine。
- `Memory` 状态写入的容错先例：JSONL 往返后数值只能是 `float64`，`core/state.go:10-15,75-93`；本卡的缓存不进 KV，因此不涉及。

**事件**

- 密封联合：`core/event.go:12` 的 `Event interface{ isEvent() }`，`HistoryCompacted` 在 `:105-114`，marker 串在 `:132-149`。
- `core/eventjson.go`：`eventWire` 在 `:18-38`（**已有** `Dropped`/`Kept`/`EstTok`/`Step`，**没有** `Strategy`）、`MarshalEvent` `:42-98`（`history_compacted` 在 `:90-91`）、`UnmarshalEvent` `:101-150`（`"history_compacted"` 在 `:143-144`）。漏编解码不报编译错，运行时在 `default` 分支报 "cannot marshal event of type"。
- 防护事件往返用例的位置：`core/stuckevent_test.go`，现有四条 —— `:5` StuckDetected、`:21` BudgetExceeded、`:37` HistoryCompacted、`:53` ArgRejected。**不是** `core/eventjson_test.go` 的那张通用表。

**行尾（本仓既有现象，新文件一律 LF）**

- 待改文件全为 LF：`core/event.go`、`core/eventjson.go`、`middleware/compaction.go`、`core/stuckevent_test.go`。
- `middleware/middleware.go` 是 CRLF，本卡不碰它（理由见"明确不做"）。

## 总设计立场（五个 TASK 共用）

1. **裁剪不截断**。窗口策略只决定"哪些消息单元整组带上、哪些不带"，永不改写任何一条消息的内容。单条工具输出的体积控制属于工具层（`tool/file/file.go:71-79`、`tool/web/web.go:160,199`、循环侧 `truncatedResults`）。
2. **单元不可分优先于任何预算**。最新单元超预算时整单元保留（宁可不裁也不给出残缺请求），见 ADR-0031 §1 边界情形。
3. **循环不认识策略**。策略是中间件里的接口实现，循环侧零改动 —— 挂载点用现成的 `HistoryCompacter` 与 `ModifyRequest`。
4. **校准系数各存各的键**。Window 用 `_window`，Compaction 保持 `_compaction`；两者同装时互不覆盖。
5. **每个 TASK 结束时仓库可编译、`go test ./...` 全仓绿**，且新用例覆盖到本 TASK 引入的每条分支。

---

## TASK-CW-01：消息单元分组与配对修复（纯 middleware 内部）— 已完成 `b2d6222`

### 改动

新增 `middleware/window.go` 的第一段内容（本 TASK 只放分组与修复，不放策略与中间件本体，编译产物暂时只有被测试引用的私有函数）：

```go
// unit is an indivisible slice of the message history for eviction purposes.
type unit struct{ start, end int } // [start,end) over the original slice

// groupUnits splits msgs into units: an assistant message carrying tool calls
// binds with the immediately following tool messages whose results belong to
// those calls; everything else is its own unit. See ADR-0031 §3 for the loop
// invariant this relies on (agent/loop.go:203,256).
func groupUnits(msgs []core.Message) []unit

// keptMessages flattens the chosen units back into a message slice.
func keptMessages(msgs []core.Message, keep []unit) []core.Message

// repairPairing drops tool results whose call is not present in the kept
// messages. It deliberately does NOT synthesize a result for an assistant call
// that has none: a pending HITL interrupt checkpoints exactly that shape
// (agent/loop.go:236-244), and a synthetic result would make the resume path
// see an already-answered call.
func repairPairing(msgs []core.Message) []core.Message
```

- `groupUnits` 的并入条件：后随消息 `Role==RoleTool` 且**至少一个** `ToolResult.CallID` 在该单元的 CallID 集合里。第一条不满足即停止并入（不跨过它继续找）。
- CallID 集合用 `map[string]bool`，每个单元构造一次；无调用的 assistant 直接单独成单元。
- `repairPairing` 先收集保留集里所有 `ToolCall.ID`，再逐条 `RoleTool` 消息过滤 `Content` 里的孤儿 `ToolResult`；过滤后为空则整条丢弃。**其它角色消息一律原样保留**。
- 顺带把校准读写提成带键版本（放 `middleware/compaction.go`，因为 helper 原本就在那儿）：

```go
func loadCalibAt(lc *agent.LoopContext, key string) (float64, bool)   // 原 loadCalib 泛化
func loadCalibDefaultAt(lc *agent.LoopContext, key string) float64
func saveCalibAt(lc *agent.LoopContext, key string, v float64)
const kvCompaction = "_compaction"  // 保留，compaction 侧调用改传该常量
```

  原 `loadCalib`/`loadCalibDefault`/`saveCalib` 删除或改成薄封装（取薄封装：`compaction.go` 的调用点不动，测试文件 `compaction_internal_test.go:65,108` 因此继续有效）。

### 验收

- `go build ./... && go vet ./...` = 0；`go test ./...` 全仓绿（含 Compaction 既有校准两条用例仍绿 —— 它们就是这次提参数的安全网）。
- 新增 `middleware/window_internal_test.go`（LF）：
  - `TestGroupUnits`：表驱动，六个形状 —— 纯 user/assistant 交替、assistant+单结果、**assistant 两个调用且结果分装两条 tool 消息**、assistant 带调用但无结果（悬挂）、开头就是孤儿结果、user 插在中间打断相邻性。断言每个单元的 `[start,end)`。
  - `TestRepairPairingDropsOrphanResults`：手工构造一条"结果没有对应调用"的输入，断言该 `ToolResult` 被删；结果为空的那条 `RoleTool` 消息整条消失。
  - `TestRepairPairingKeepsPendingCallUntouched`：悬挂 assistant 调用输入下修复段是 no-op —— 这条专门守住"不补合成结果"（本卡更正过的那处）。
  - `TestKeptMessagesNeverSplitsAUnit`：任取分组结果的子集拼接，断言输出里每条 `ToolResult` 的 CallID 都能在同一个输出里找到对应 `ToolCall`。
- `gofmt -l` 对 `middleware/window.go`、`middleware/window_internal_test.go`、`middleware/compaction.go` 无输出。

---

### TASK-CW-01 完成记录（2026-10-08）

与卡里写法不同的六处，第二条是卡的验收项本身有问题：

1. **表驱动从六个形状加到八个 case**，并给每个 case 追加一条"单元必须正好平铺输入"的断言（相邻不重叠、总覆盖等于消息数）。卡只要求断言 `[start,end)`。加它的当场理由：并入循环的边界写错时（例如 `end` 少进一位）单点断言看不出来，而平铺断言能。空历史那条是顺手补的，成本为零。
2. **卡里 `TestKeptMessagesNeverSplitsAUnit` 的断言写法会空转**。卡要求"断言输出里每条 `ToolResult` 的 CallID 都能在同一个输出里找到对应 `ToolCall`"——但按单元选择**不可能**造出孤儿，所以对一份良构历史跑任何子集，这条断言修不修 `repairPairing` 都成立。实现拆成三段，每段都能被对应变异打到：
   - 修复**前**就无孤儿（这才是单元原子性的断言）；
   - `repairPairing` 对良构选择是 no-op（条数不变）；
   - 另备一份"本来就含孤儿"的历史（孤儿自成单元），断言修复前恰有 1 条孤儿、修复后只剩那条 user 消息 —— 证明修复段不是装饰。
3. **新增一条卡里没有的别名断言**：`keptMessages` 的返回不得与输入共享底层数组（`&sel[0] == &msgs[first]` 即失败）。它钉的是函数注释里"fresh slice is deliberate"那句；变异 M3（改成返回 `msgs[u.start:u.end]`）实测让它变红。
4. **校准 helper 取薄封装**：`loadCalib`/`loadCalibDefault`/`saveCalib` 保留为三行转调，`compaction.go` 的调用点一字未改，因此 `middleware/compaction_internal_test.go:65,108` 两条既有用例仍然有效并实测通过 —— 卡里"取薄封装"的预判成立。
5. **`window.go` 本 TASK 只有单元规则与文件头注释**，没有公开 API。风险表最后一条（未使用私有函数会不会被 `go vet` 拦）实测**不拦**：`go vet ./...` = 0。
6. **`repairPairing` 对非 `RoleTool` 消息一律原样保留**，包括带悬挂调用的 assistant。这条是 §3 那个更正的实现形状，由 `TestRepairPairingKeepsPendingCallUntouched` 守住（变异 M4 实测能打到它，见下表）。

变异验证四次（每次改坏一处、跑对应用例、还原；最后 `identical: True` 确认源文件字节级还原）：

| 改动 | 结果 |
| --- | --- |
| M1 `groupUnits` 的并入条件只看 `Role == RoleTool`、不查结果归属 | `TestGroupUnits/absorb on at least one match, stop at a message reporting none` 失败：`units = 2 [[0 3] [3 4]] over [attu], want 3 [[0 2] [2 3] [3 4]]` —— 一条不属于本次调用的工具消息被并了进来 |
| M2 `repairPairing` 的孤儿过滤条件恒假（等于不删） | `TestRepairPairingDropsOrphanResults` 失败 |
| M3 `keptMessages` 返回输入底层数组的窗口 | `TestKeptMessagesNeverSplitsAUnit` 失败：别名断言命中 |
| M4 `repairPairing` 给带调用的消息补一条合成结果（本卡明确否决的行为） | `TestRepairPairingKeepsPendingCallUntouched` 失败：2 条变 3 条 |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` = 31 个包 ok、无 FAIL；`go test -race -count=1 ./middleware/ ./agent/` = 0。
- `gofmt -l` 对三个涉及文件无输出。新文件为 LF（`git show HEAD:middleware/window.go` 的入库内容亦为 LF；checkout 时的 "LF will be replaced by CRLF" 警告是本仓 `core.autocrlf` 的既有现象，非本次引入）。
- 新用例 4 个函数：`TestGroupUnits`（8 子例 + 平铺断言）、`TestRepairPairingDropsOrphanResults`（含"不得改动输入"断言）、`TestRepairPairingKeepsPendingCallUntouched`、`TestKeptMessagesNeverSplitsAUnit`（8 个子集 + 孤儿历史一段）。
- 既有的两条 Compaction 校准用例与三条 `Compaction` 行为用例全绿 —— 它们是本次 helper 提取的安全网。
- 提交 `b2d6222 refactor(middleware): split the message history into indivisible eviction units`，未推送。三个文件：`middleware/window.go`（新，133 行）、`middleware/window_internal_test.go`（新，236 行）、`middleware/compaction.go`（+17/-6）。

---

## TASK-CW-02：`core.WindowTrimmed` 事件 — 已完成 `13404bb`

### 改动

**`core/event.go`**（接在 `HistoryCompacted` 之后，`:105-114` 的体例）

```go
// WindowTrimmed is emitted by the Window middleware each time it evicts message
// units from what the step would otherwise send. Strategy names the policy
// ("recent_n" | "sliding_window" | "importance_weighted"), Dropped/Kept count
// messages (not units) after pairing repair, EstTokens is the calibrated
// estimate of the history *before* eviction, and Step is the loop step. Unlike
// HistoryCompacted — which fires only when a durable summarization rewrote the
// history — this fires per actual eviction, so a request-mode window emits one
// per step it trimmed.
type WindowTrimmed struct {
    Strategy  string
    Dropped   int
    Kept      int
    EstTokens int
    Step      int
}

func (WindowTrimmed) isEvent() {}   // 加进 :132-149 的 marker 串
```

**`core/eventjson.go`**

- `eventWire` 新增 `Strategy string \`json:"strategy,omitempty"\``（放在 `Class`/`Count` 之后，`:37-38` 旁）。
- `MarshalEvent` 加 `case WindowTrimmed: w.Type, w.Strategy, w.Dropped, w.Kept, w.EstTok, w.Step = "window_trimmed", e.Strategy, e.Dropped, e.Kept, e.EstTokens, e.Step`（放 `:90-91` 的 `history_compacted` 旁）。
- `UnmarshalEvent` 加 `case "window_trimmed": return WindowTrimmed{...}, nil`（放 `:143-144` 旁）。

### 验收

- `core/stuckevent_test.go` 加 `TestWindowTrimmedEventRoundTrip`（照 `:37` 的 `TestHistoryCompactedEventRoundTrip` 写法，第五条同类用例）。
- 变异验证：临时删掉 `MarshalEvent` 的 case → 断言报 `core: cannot marshal event of type core.WindowTrimmed`；删掉 `UnmarshalEvent` 的 case → 报 `core: unknown event type "window_trimmed"`；把 `e.Strategy` 换成空串 → 往返后 Strategy 丢失。三次都要红。
- `go test ./core/ ./bus/ ./queue/` 绿（`bus`/`queue` 是泛型转发，确认没有第二处需要登记事件类型：`grep -rn "history_compacted" --include=*.go` 应只命中 `core/eventjson.go` 与其测试）。
- 全仓 `go build ./... && go vet ./... && go test ./...` 绿。

---

### TASK-CW-02 完成记录（2026-10-08）

与卡里写法不同的四处：

1. **插入位置让既有行号下移**。按 ADR-0031 §6 把 `WindowTrimmed` 放在 `HistoryCompacted` 之后（两个上下文事件相邻，比堆在联合末尾好读），于是卡"前置事实"里引的旧行号按实测更正：`ArgRejected` 现在在 `core/event.go:139`（卡引 `:123`）、marker 串在 `:148-166`（卡引 `:132-149`）、`eventWire` 到 `:40`（新字段 `Strategy` 在 `:39`）、`MarshalEvent` 在 `:43`（卡引 `:42`）、`UnmarshalEvent` 在 `:104`（卡引 `:101`）、新事件的 marshal case 在 `:93-94`、unmarshal case 在 `:147-148`。
2. **变异验证做了五次而不是卡里说的三次**，其中两条是卡里没有的：
   - 卡的三条**照跑**（删 marshal case、删 unmarshal case、`e.Strategy` 换空串）。
   - **新增"字段映射写反"**（`e.Kept` 与 `e.EstTokens` 互换）：编解码里最典型也最难看出的一类错误，卡的三条都抓不到它。
   - **新增"类型串写错"**（marshal 写 `"window_trim"`，unmarshal 仍按 `"window_trimmed"`）：抓两侧不同步。实测红，报错 `core: unknown event type "window_trim"`，走 `UnmarshalEvent` 的 `default` 分支（该分支返回 error 而非 panic）。
3. **往返用例的取值刻意全部互不相同**（`Dropped:12, Kept:5, EstTokens:9400, Step:6` + 非空 `Strategy`），否则"字段写反"这条变异不会被发现。既有的四条同类用例（`TestHistoryCompactedEventRoundTrip` 用 8/6/8100/4）已是这个取法，照做。这条不是多余的谨慎：正是它让新增的第 4 条变异转红。
4. **没有为 `Step` 的 `omitempty` 加子用例**。AG-05 已为 `arg_rejected` 记下"step 为 0 时该字段在 JSON 里不存在，消费侧按缺省处理"这条读法，而 `Step` 是所有事件共用的同一个 `eventWire` 字段，每条事件再钉一遍是重复。CW-05 的示例会把这条读法打在输出里（那里才是它的用处所在）。

另外确认了卡"前置事实"的一条判断：事件类型串只需在 `core/eventjson.go` 登记。`grep -rn "history_compacted" --include=*.go` 只命中该文件与其测试，`bus`/`queue` 是泛型转发，`go test ./core/ ./bus/ ./queue/` 全绿即为证。

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test -count=1 ./...` = 31 包 ok、无 FAIL；`go test -count=1 ./core/ ./bus/ ./queue/` = 0；`go test -race -count=1 ./core/` = 0。
- `gofmt -l` 对 `core/event.go`、`core/eventjson.go`、`core/stuckevent_test.go` 无输出（三个文件本就是 LF，改完仍是）。
- 新用例 1 条：`TestWindowTrimmedEventRoundTrip`，与既有四条并列（第五条防护事件往返用例）。
- 变异五次全部转红，每次做完都做了字节级还原确认（`identical: True`）：

| 改动 | 结果 |
| --- | --- |
| 删 `MarshalEvent` 的 `case WindowTrimmed` | `core: cannot marshal event of type core.WindowTrimmed` |
| 删 `UnmarshalEvent` 的 `case "window_trimmed"` | `core: unknown event type "window_trimmed"` |
| marshal 把 `e.Kept` 与 `e.EstTokens` 写反 | `round trip = ...{Kept:9400, EstTokens:5}…, want ...{Kept:5, EstTokens:9400}…` |
| marshal 的类型串写成 `"window_trim"` | `core: unknown event type "window_trim"`（`UnmarshalEvent` 的 default 分支返回 error，不是 panic） |
| marshal 把 `e.Strategy` 换成空串 | `round trip = ...{Strategy:""}…, want ...{Strategy:"importance_weighted"}…` |

- 提交 `13404bb feat(core): WindowTrimmed event for context-window eviction`，未推送。三个文件 `+38/-0`：`core/event.go`（+17，类型 + 文档 + marker）、`core/eventjson.go`（+5，字段 + 两条 case）、`core/stuckevent_test.go`（+16）。

---

## TASK-CW-03：`Window` 骨架 + RecentN + SlidingWindow — 已完成 `b71ebbc`

### 改动

在 `middleware/window.go` 补齐中间件本体与前两个策略。

```go
// WindowOptions configures Window.
type WindowOptions struct {
    Strategy WindowStrategy      // 必填；RecentN / SlidingWindow / ImportanceWeighted
    Counter  TokenCounter        // nil 用 estimateTokens
    Persist  bool                // 语义与 CompactionOptions.Persist 相同
}

// Window bounds one request's message history by *evicting* whole message units
// — as opposed to Compaction, which *summarizes* them. Two mount points, two
// modes, mutually exclusive like Compaction's (compaction.go:92-118)...
func Window(o WindowOptions) agent.Middleware

type window struct {
    agent.BaseMiddleware
    strategy WindowStrategy
    counter  TokenCounter
    persist  bool
}

const kvWindow = "_window"
```

- `Select` 的 `count` 闭包每步构造一次：`factor := loadCalibDefaultAt(lc, kvWindow)`，闭包内 `int(float64(w.counter(m)) * factor)`。整步只读一次 KV。
- `ModifyRequest`：`if w.persist { return nil }`，否则 `if out := w.apply(lc, req.Messages); len(out) != len(req.Messages) { req.Messages = out; 发事件 }`。
- `CompactHistory`：`if !w.persist { return history }`，同样走 `apply`，裁了才发事件并返回新切片。
- `apply(lc, msgs)` 内部三件事：`groupUnits` → `strategy.Select(lc, count, msgs)` → `repairPairing`；返回 `(out []core.Message, estTokens int)`，`estTokens` 是裁剪前整段的 `count(msgs)`。
- `AfterModel`：与 `middleware/compaction.go:158-173` 同构，写 `saveCalibAt(lc, kvWindow, ratio)`。注释要写明两点事实：`lc.Request` 是同一指针、`Stack.AfterModel` 逆序，所以"校准值针对最终发出的那份消息集"（见前置事实）。
- **返回新切片，绝不原地改** `req.Messages` 背后的数组：`agent/compacthistory_test.go:30-42` 的 `trimCompacter` 就是这个体例（注释写明"copying them into a fresh slice so the returned history never aliases dropped backing memory"）。照抄该做法，用 `keptMessages` 构造新切片。

```go
func RecentN(keep int) WindowStrategy        // 按单元条数保留末尾 keep 个；keep<=0 时取 1
func SlidingWindow(budgetTokens int) WindowStrategy  // 从末尾整单元累加，超预算即停；budget<=0 时取 4000
```

- 两者都实现同一个 `Select`；`RecentN` 完全不调 `count`（所以 `Counter` 对它无效，注释写明）。
- 最新单元单独超预算 → 整单元保留、`Dropped=0`（§1 边界情形），要有一条用例专门钉住。
- 文档注释里固定三策略语义差异（ADR-0031 §1 那张表），并点名"sliding window 在别处常指最近 N 条，本包不是"。

**事件字段**：`Strategy` 串由各策略自报，接口加一个方法还是构造时传？取接口第二方法：

```go
type WindowStrategy interface {
    Select(lc *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message
    Name() string   // "recent_n" | "sliding_window" | "importance_weighted"，进事件
}
```

（比在每个工厂外面再挂一张 name 表省事，也让自定义策略自带标识。）

### 验收

- `middleware/window_test.go`（包外，走真实 loop，体例参照 `middleware/compaction_test.go`）：
  - `TestWindowRecentNByUnitNotMessage`：尾部是"assistant+结果"单元时 `keep=1` 留 2 条消息；断言历史里不存在孤儿结果。
  - `TestWindowSlidingWindowKeepsTailWithinBudget`：给一串固定消息与一个刚好卡在两个单元之间的预算，断言保留集与预算一致。
  - `TestWindowSlidingWindowOversizedNewestUnit`：最新单元单独超预算 → 一条不裁、仍发事件或不发（取"不发"：没裁就没事实可报）——**这条要写成显式断言，别留含糊**。
  - `TestWindowMutualExclusion`：`Persist:true` 时 `ModifyRequest` 不动请求、`Persist:false` 时 `CompactHistory` 原样返回（照 `compaction_test.go:27` 的写法）。
  - `TestWindowPersistShrinksState`：持久模式跑完断言 `State.Messages` 条数变小（照 `compaction_test.go:52`）。
  - `TestWindowRequestModeLeavesStateFull`：请求模式跑完 `State.Messages` 仍全量（照 `compaction_test.go:107`）。
  - `TestWindowEventPublished`：`Stream(...).Iter()` 收到 `WindowTrimmed`，`Strategy`/`Dropped`/`Kept` 与断言的历史形状一致。
  - `TestWindowCalibrationIsolatedFromCompaction`：`Window` + `Compaction` 同装跑几步，断言 `_window` 与 `_compaction` 两个键都在且互不覆盖（这条防的是 `saveCalib` 整块覆盖那个坑）。
  - `TestWindowResultPassesPairingCheck`：对任意裁剪结果跑一遍"每条 ToolResult 的 CallID 都能找到对应 ToolCall"的全局断言。
- `middleware/window_internal_test.go` 增补：`count` 闭包在无校准记录时因子为 1、有记录时按乘。
- `go test -race -count=1 ./middleware/ ./agent/` 绿；全仓 `go vet ./... && go test ./...` 绿；`gofmt -l` 对新文件无输出。

---

### TASK-CW-03 完成记录（2026-10-08）

与卡里写法不同的八处，第 2、8 两条是实施中发现的问题，其余是实现细节：

1. **`apply` 返回三个值** `(out, est, trimmed)`，卡写的是两个。多出的 `trimmed` 既决定要不要发事件、也决定要不要把新切片交回循环，同时是"跳过修复段"的开关。让调用方比较长度来推断会读错一种情形：长度相等但内容不同（策略换了消息）不该被读成"没裁"。
2. **修复段只在这一步真的裁掉了东西时才跑**（卡正文没有这一步，ADR-0031 §3 已补记）。策略决定一条不丢时原样交回输入切片，不去清理历史里本来就存在的孤儿结果 —— Window 不是历史清理器，一个没做任何决定的步骤静默改写 `State.Messages` 是另一个特性。代价写进了 ADR：那份坏历史仍会照原样发出并被服务端拒，但它在本中间件安装之前就是这样发的。由 `TestWindowApplySkipsRepairWhenNothingEvicted`（内部）与 `TestWindowResultPassesPairingCheck` 的第三个子例（外部）两头钉住。
3. **`Strategy` 为 nil 时整体降级成 no-op**，卡正文只写"必填"。取的是 `middleware.RAG` 对 nil Retriever 的既有体例（`middleware/rag.go:46-48`），并测了四件事：`ModifyRequest` 不动请求、`CompactHistory` 原样返回、`AfterModel` 不写校准、都不报错。
4. **`SlidingWindow` 的兜底预算取 4000**，写成具名常量 `defaultWindowBudget` 并在注释里给理由：窗口既然常当"发出前最后一道硬上限"，就该比 Compaction 的 8000 历史阈值更紧。卡写"取 4000"但没给理由，ADR 也没这条数。
5. **`publishTrimmed` 需要"裁剪前那份输入"**才能算 `Dropped`。卡的伪码是"改写 `req.Messages` 之后再比长度"，那时入参已经拿不到了，所以实现先留一份 `in := req.Messages`（切片是值，赋值不共享后续改写）。
6. **"没裁就不该发事件"这条只能直接调钩子中才断言得出来**：真实 loop 里"零事件"与"事件没被订阅到"不可区分。实现用 `bus.New()` + `Subscribe(topic, bus.Lossy)` + `cancel()`（`cancel` 关闭通道，`bus/bus.go:78-103`）收干通道计数，卡里没有给这一手。
7. **helper 名字冲突**：`drain` 已被 `middleware/fallback_test.go:52` 占用，改用 `winDrain`；`collect`（`middleware_test.go:169`）与 `numOfTest` 复用既有，未重复定义。包内私有的 `newLC` 也复用 `compaction_internal_test.go:61`。
8. **校准隔离那条第一版是空转的**，写法是"两个键都非 nil 且各自带 `calib`"——若两者共用 `_compaction`，值恰好相等时这条断言看不出来。改成**让两个因子在数值上必须不同**：给 Compaction 注入常量 `Counter=100`、给 Window 注入 `Counter=200`、模型统一上报 `Usage.InputTokens=150`，于是期望值精确落在 `_compaction=1.5`、`_window=0.75`；共用一个键的话必有一个数被对方改掉。常量 Counter 顺带让断言与 `estimateTokens` 的 rune 算术脱钩。另有一条内部用例从正面守同一件事：`TestWindowApplyCalibratesCount` 里把 `_compaction` 写成 0.5，Window 的估算仍是 60。

变异验证六次（每次改坏一处、跑对应用例、还原；六次都做了字节级还原确认）：

| 改动 | 结果 |
| --- | --- |
| `ModifyRequest` 的守卫写成 `if !w.persist`（两种模式装反） | 红：`persist mode must leave the request untouched, got 1 messages` |
| `AfterModel` 写进 `kvCompaction`（两中间件共键） | 红：`both calibration keys should be checkpointed, got _window=<nil> _compaction=map[calib:0.975]` |
| 去掉 `if !trimmed { return nil }`（没裁也发事件） | 红：`a step that evicted nothing must publish nothing, saw 1 events` |
| `RecentN` 保留末尾 keep **条消息**而不是 keep 个单元 | 红：`kept 0 messages, want 2 (the last unit is a call and its result)` —— 调用被切走、结果成了孤儿又被修复段删掉，正是这条设计要防的形状 |
| 去掉"最新单元无条件保留" | 红：`the oversized newest unit must be kept whole, got 0 messages`（发空请求） |
| `len(kept) >= len(msgs)` 改成 `>`（没裁也跑修复段） | 红：`a strategy that evicts nothing must not report a trim` |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test -count=1 ./...` = 31 包 ok、无 FAIL；`go test -race -count=1 ./middleware/ ./agent/` = 0。
- `gofmt -l` 对 `middleware/window.go`、`middleware/window_test.go`、`middleware/window_internal_test.go` 无输出（三个文件均 LF；`git show` 的入库内容也是 LF，checkout 时的 CRLF 警告属本仓 `core.autocrlf` 既有现象）。
- 包外用例 9 组：`RecentN` 按单元保留（`keep=1` 得 2 条且无孤儿）、`SlidingWindow` 预算内累加与"预算够就不动"、超预算最新单元整单元保留（含"零事件"断言与 `EstTokens=40` 的裁剪前口径）、双模式互斥、裁剪结果配对完好（三子例：两种裁剪 + 一种不裁）、真实 loop 的 persist 缩小状态、request 保留全量、事件字段与顺序、校准隔离。
- 包内新增 3 组：`count` 闭包的校准读写（含 `_compaction` 不串味）、没裁时跳过修复段、策略默认值与 nil 策略的四件事。
- 真实 loop 两条形状实测：4 次工具调用下 persist 把 `State.Messages` 压到 ≤5，request 模式仍留 ≥9 条且存储的历史里没有孤儿。
- 提交 `b71ebbc feat(middleware): Window bounds one request by evicting message units`，未推送。三个文件 `+820/-6`：`middleware/window.go`（389 行，含 CW-01 的单元部分）、`middleware/window_test.go`（456 行，新）、`middleware/window_internal_test.go`（+102）。

---

## TASK-CW-04：ImportanceWeighted + 默认启发式打分器 — 已完成 `e3893c4`

### 改动

```go
type MessageScorer interface {
    // ScoreContent scores each message's relevance to ref in [0,1], in the given
    // order. It must not depend on position — the strategy adds the recency term
    // itself, and callers may cache results by message content hash.
    ScoreContent(ctx context.Context, ref string, msgs []core.Message) ([]float64, error)
}

type ImportanceOptions struct {
    BudgetTokens int           // 校准 token 预算
    TauUnits     int           // recency 衰减常数，单位=单元（默认 12）
    Scorer       MessageScorer // nil 用内置启发式
    PinFirst     bool          // 固定保留第一条 user 消息（默认 true）
}

func ImportanceWeighted(o ImportanceOptions) WindowStrategy
```

- 内置打分器 `heuristicScorer`（同文件私有）：`role 先验 + 与 ref 的小写关键词重叠`，重叠归一到 [0,1]（命中数 / max(1, 查询词数)）。角色先验 user 1.0 / assistant 0.7 / tool 0.35，常量表 + 注释说明"工具输出体积占多数且信息衰减最快"。分词借用 `middleware/rag.go:85-110` 的思路（`strings.ToLower` + `strings.Fields`）。
- `Select` 流程：
  1. `ref := lastUserText(msgs)`（`middleware/rag.go:66`），空则跳过内容项。
  2. 按单元聚合分数：`score(unit) = Σ content(msg) + 0.5 * exp(-距离/TauUnits)`，距离按单元序号从末尾数；单元内取总和（不是均值），因为"一个调用+它的结果"作为一个整体才有意义。
  3. 贪心 key 用 **`score(unit) / max(1, count(unit))`** 降序；预算内整单元选中。
  4. `PinFirst` 时第一条含 user 消息所在单元无条件先占预算（先扣它，再贪心）。
  5. 选中集按原下标升序 → `keptMessages` → 交回 `apply` 走 `repairPairing`。
- `Scorer` 返回 error 时整步不动（best-effort，与 `middleware/compaction.go:134-137` 摘要失败的处理一致），并保证不打事件。
- 系数 0.5 与固定角色先验**不提供配置**：注释写理由，调节面是 `Scorer`（ADR-0031 §7）。
- **缓存不在本 TASK 实现**：内置打分器纯本地算术。远端打分器的哈希缓存属于示例/用户侧，CW-05 的示例里自带（约 20 行，键取 `middleware/loopguard.go:341-346` 那套 sha256+hex[:16]），并注明"性能缓存，不进 `State.KV`"。

### 验收

- `middleware/window_internal_test.go` 增补：
  - `TestImportanceGreedyUsesValuePerToken`：一条与 ref 高度相关但巨大的 tool 单元，与三条小但关键的 user/assistant 单元并存，预算只够一边。断言选中的是那三条（这条钉住"性价比贪心"而不是"分数贪心"）。
  - `TestImportancePinFirstUser`：预算极小时 `PinFirst:true` 保住首条 user，`PinFirst:false` 时不保。
  - `TestImportanceScorerErrorLeavesHistoryUnchanged`：注入一个必返错的 scorer，断言历史一条没少、没有事件发出。
  - `TestImportanceRecencyBreaksTies`：内容分相同、位置不同的两条，保留离末尾近的那条。
- `middleware/window_test.go` 增补：端到端一例 `ImportanceWeighted`（真实 loop + 多次工具调用 + 中途一条关键决定），断言该决定那条消息在最终请求里仍在，而末尾若干条大工具输出被裁。
- 变异验证（每次改坏一处、跑相关用例、还原）：把性价比改成绝对分数；把单元分从总和改成均值；`PinFirst` 强制置 false；把 recency 项去掉。四次都应至少让一条用例变红。
- 全仓 `go vet ./... && go test ./...` 绿；`go test -race -count=1 ./middleware/` 绿。

---

### TASK-CW-04 完成记录（2026-10-08）

与卡里写法不同的八处。前三条是实施中发现的真问题（第 3、4 条让用例一开始空转），其余是实现细节：

1. **`PinFirst bool` 改成 `NoPinFirst bool`**。Go 的零值是 false，用 `PinFirst` 就分不开"没设置"与"显式关掉"，而卡与 ADR 定的默认是"钉住首条 user"。取仓库既有体例：`RunBudgetOptions.NoWrapUpLastTurn`（`middleware/runbudget.go:55-57`）同样是"默认开、写成 opt-out"。
2. **默认打分器必须自己读工具结果的内容**。`Message.Text()` 只拼顶层 `Text` part（`core/message.go:101-109`），而 `RoleTool` 消息的内容在 `ToolResult.Content` 里；不额外处理的话所有工具输出永远拿 0 相关性，排序只剩角色先验，"旧的相关工具输出被保留"这条卖点根本成立不了。`windowText` 的理由与 `estimateTokens` 走 `ToolResult.Content` 同源（`middleware/compaction.go:259-265`）。
3. **性价比那条用例第一版是空转的**：大单元 92 token 加最新单元 9 = 101 > 预算 100，无论按绝对分数还是按每 token 分数都装不下，两种实现给出同一个结果，M1 变异第一次跑是绿的。把预算改成 120（大单元先手装得下、之后只剩两条小单元；按性价比则六条小单元全留、大单元出局）才有区分力。实测差异：错误实现 `kept [bxxx]`，正确实现 `[xxxxxxx]`。
4. **卡的第四条变异（单元分改均值）当时没有任何用例会被它打红**——卡里给的用例只覆盖"性价比排序""钉首条""recency"。补 `TestImportanceUnitScoreIsSummed`：`TauUnits` 设成 10000 让 recency 对所有单元相同，只剩"求和还是求均值"这一个变量。
5. **补了 `TestImportanceSkipsUnfitUnitsInsteadOfStopping`**。卡把"装不下就跳过而不是停"写进了实现要点却没有相应用例；M5（`continue`→`break`）由它抓住（`kept [d]` vs `[bd]`）。
6. **打分器错误那条用例的桩件形状会影响结论**。`stubScorer` 原本是"返回 error 时给 nil 切片"，于是把生产的错误判断改坏（忽略 err）也会被长度检查顺手兜住，M7 第一次跑不出来。改成专用 `errScorer`：返回**长度合法的结果 + error**，M7 才转红。顺带删掉 `stubScorer.err` 字段。
7. **卡里"把 recency 项去掉"这条变异自身编译不过**（`math` 变成未使用导入）。改成"recency 项变成常数"（`math.Exp(0)`）：抓的是同一件事（位置不再参与区分），且能编译。实测红：`kept [ad], want [cd]`——没了位置项，并列时按原顺序取，留下的是最旧那条。
8. **分词必须处理中日韩**。`strings.Fields` 对没有空格的中文整条算一个 token，中文问答的重叠度会恒为 0。`tokenSet` 按 `embeddings/mock` 的 `tokenize`（`embeddings/mock/mock.go:65-88`）同一思路：宽字符按单字成词、其余按字母数字成词，宽字符判定直接复用本包的 `isWideRune`（`middleware/compaction.go:287-293`）。由 `TestHeuristicScorerReadsToolResultsAndCJK` 的中文段覆盖。

系数按 ADR-0031 §7 定为常量，不提供配置：recency 权重 0.5、内容与角色的配比 0.65/0.35、角色先验 user 1.0 / assistant 0.7 / tool 0.35、`TauUnits` 默认 12。唯一的调节面是 `MessageScorer`。

变异验证七次（每次改坏一处、跑对应用例、还原；全部做了字节级还原确认）：

| 改动 | 结果 |
| --- | --- |
| M1 按绝对分数而不是每 token 分数排序 | 红：`kept [bxxx]: the oversized unit was taken first, so ranking was by score, not score per token` |
| M2 单元分改成求均值 | 红：`kept 2 messages [bd], want 3 (unit A plus the newest unit)` |
| M3 钉住首条 user 的逻辑强制失效 | 红：`with pinning kept [bc], want [qc]` |
| M4 recency 项变成常数 | 红：`kept [ad], want [cd]: recency did not break the tie` |
| M5 装不下即停止（`continue`→`break`） | 红：`kept [d], want [bd]` |
| M6 打分看不到工具结果内容 | 红：`a tool result matching the query scored 0.1225, an unmatched assistant 0.245` |
| M7 忽略打分器返回的错误 | 红：`a scorer error must return the input history` |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test -count=1 ./...` = 31 包 ok、无 FAIL；`go test -race -count=1 ./middleware/ ./agent/` = 0。
- `gofmt -l` 对三个涉及文件无输出（均 LF）。
- 包内新增 7 组：性价比排序、求和 vs 求均值、跳过而非停止、钉首条（开与关两向）、recency 破并列、打分器错误（直调钩子 + 经中间件且不发消息，用 `errScorer`）、默认值与 `Name()` 与空历史与结果长度不符的降级；另有一组专测默认打分器能读工具结果与中文。
- 包外新增 1 组端到端：4 次 400 字工具输出 + 中段一条"决定"，断言决定仍在最终请求里、发出的大结果数 1 < 存下的 4、请求无孤儿、`State.Messages` 仍大于被裁的请求（request 模式保全量）。
- 文件规模：`middleware/window.go` 669 行、`middleware/window_test.go` 566 行、`middleware/window_internal_test.go` 603 行。本次提交 `+657/-2`。
- 提交 `e3893c4 feat(middleware): importance-weighted window strategy with a pluggable scorer`，未推送。
- 端到端用例的第一版脚本有个真错误：中段那条"决定"发的是不带工具调用的纯文本，而这个循环里"无调用的助手消息"就代表回合结束，于是 run 在第 2 次模型调用后终止（实测 `model calls=2`）。改成"文本 + 一次便宜的工具调用"才既留下决定又不结束，这一点写进了用例注释。

---

## TASK-CW-05：组合顺序用例 + 两个离线示例 — 已完成 `c98beb9` + `fd8f7a8` + `14b3b3a`

### 改动

- `middleware/window_test.go` 增两条顺序用例（`Stack.ModifyRequest` 正序，`agent/middleware.go:187-194`）：
  - `TestWindowAfterCompactionCapsTheRequest`：注册顺序 `Compaction(request) → Window`，断言最终 `req.Messages` 的规模受 Window 预算约束（硬上限保证要把 Window 放最后）。
  - `TestWindowBeforeCompactionSummarizesTheKeptSet`：顺序反过来，断言摘要确实先执行、且 Window 随后仍可能继续裁 —— 说明两个顺序的行为差异，不判对错。
- 新增 `examples/context-window/main.go`（`package main` + `llm/mock`）：
  - 一份 12 次模型调用的脚本：多次工具调用（含一条 4000 字级的巨大工具输出）+ 第 5 步一条"决定用方案 B"的 user 消息 + 末尾正常收束。
  - 跑五遍：不装、`RecentN(4)`、`SlidingWindow(900)`、`ImportanceWeighted{BudgetTokens:900}`、`Compaction+Window`。每遍打印：最终请求的消息条数与估算 token、`WindowTrimmed` 事件行、以及"那条决定是否在最终发送的 messages 里"（用 `mock.Model` 收到的 `req.Messages` 直接判定，`llm/mock.New(name, resp)` 的回调里存下来）。
  - 打印一律在主流程（示例既有约束：模型跑在另一个 goroutine，回调里 `Println` 会与事件消费交错）。
- 新增 `examples/context-window-relevance/main.go`（`llm/mock` + `embeddings/mock`）：
  - 同一份脚本两遍：`ImportanceWeighted` 用内置启发式 vs 用示例自带的 `embeddingScorer`（实现 `MessageScorer`，内部拿 `embeddings/mock.New()` 编 `ref` 与各条消息、算 cosine，带条数上限的 sha256 内容哈希缓存）。**（设计期描述：最终实现就是"键取消息内容哈希"，靠"问题一换整张表作废"来保证正确，不是在键里拼 ref；另外打印的是打分/送去编码/命中三个按消息计的计数，参考文本另计。以完成记录第 7、8 条为准。）**
  - 打印：两遍各自的保留集差异（哪条旧消息因为与当前问题相关而被留下）、打分器被真正调用几次、缓存命中几次。
  - 示例文件头注释写明"这是注入远端打分器时的通用形状：缓存在打分器自己手里，不进 `State.KV`"。
- README、`middleware/middleware.go` 包注释：不动（理由见"明确不做"）。

### 验收

- `go build ./... && go vet ./... && go test ./...` 全绿（含两个新 example 包）。
- `go run ./examples/context-window` 退出码 0；连跑三次输出逐字节相同（`diff` 比对）。
- `go run ./examples/context-window-relevance` 同上；无网络、无密钥。
- 输出可见：`RecentN` 那遍把那条关键决定裁掉了（这正是它不该用于长任务的证据）、`ImportanceWeighted` 那遍留下它、`SlidingWindow` 那遍最终估算 token 落在预算内、`Compaction+Window` 那遍既有摘要笔记又有裁剪事件。
- 第二示例输出可见：默认启发式与 embedding 打分器保留集不同、缓存命中数 > 0 且打分调用次数明显小于"消息数 × 步数"。

---

### TASK-CW-05 完成记录（2026-10-08）

落地为两个提交：`c98beb9`（顺序用例，`test(middleware)`）与 `fd8f7a8`（两个示例，`docs(examples)`）。与卡里写法不同的九处，第 1、2、5 条是实施中发现的真问题（第 1 条推翻了卡的前提，第 5 条让示例一度空转），其余是形态与口径：

1. **卡的前提在这个用例的形状下不成立**：两种注册顺序"最终规模不同"看不出来。用例里 `RecentN(1)`、历史 10 条等长消息，两种顺序最终都是 1 条（`:510`、`:528` 都断 `!= 1`），真实差别是**谁的活白跑了**——`Compaction → Window` 会把刚写下的摘要 note 再被窗口裁掉一次；`Window → Compaction` 则摘要器一次都没被调用（`calls == 0`）。这只是这套桩件暴露出的差异面，不是"顺序永远不影响规模"的一般结论：`Compaction` 的 `MaxTokens` 是触发阈值而不是上限，想要确定上限仍然得让 `Window` 后跑（ADR-0031 §8 的顺序规则照旧）。
2. **两条用例合并成一条 `TestWindowOrderWithCompaction` 的两个子例**：两者断的是同一件事的两面，拆开就要各写一遍桩件模型与断言。另外直接用 `agent.NewStack(compactionMW, windowMW)` 跑 `Stack.ModifyRequest`，不经真实 loop——要钉的就是两个中间件在同一个 `*llm.Request` 指针上的折叠顺序，套上 loop 只会引入无关时序。
3. **示例一的"决定"必须由中间件注入，且必须带一次便宜工具调用**。脚本本身发不出"跑到第 5 步再插一条 user 消息"；用 `steerer.BeforeModel` 在 `lc.Step == 5` 时 `lc.Steer` 才行（`&steerer{}`，指针接收者才满足接口）。第一版注入的是纯文本，回合当场结束（实测模型只调了 2 次），改成"文本 + 一次 `fetch` 调用"才继续跑到第 10 次。
4. **示例一第 5 遍的 Compaction 用 `Persist: true`**：request 模式的改写不发 `HistoryCompacted`，那一遍就没东西可打印；persist 恰好也是 ADR-0031 §8 的推荐搭配。这一遍的"决定"打印为"不在"，原因是它已被持久摘要吸收进 note（不再是原文），不是被丢掉——示例末尾的读法说明了这一点。
5. **示例二第一版是空转的**：`fetch` 每条返回同样的 `strings.Repeat("y", 500)`，两遍的保留集在"按内容去重"的比对里必然相同，`裁剪事件` 也看不出位置差。改成每条带编号、长度随 n 变化（内容两两不同）后才有区分力。
6. **示例二还差点把窗口跑没了**：把内容改成变动长度后总量落到预算以下，`裁剪事件 0 次`、两遍全留——示例退化成"什么都不裁"。把单次输出下限提到 900 字节（八轮合计远超 `budget = 700`）才恢复裁剪，实测 5 次。
7. **打分器的计数口径有三处会骗人**，都改了：`embedded` 曾把参考文本一起计入，于是"命中 = scored - embedded"少 1；"命中"最初是推算而不是观测，改成 `s.hits++` 直接数；`msgText(m)` 在取文本与拼批量时各调一次，改成一次取好。现在打分、送去编码、命中三个数各自独立累加（实测 81 = 10 + 71）：相加相等这条能抓住"某条路径少计了一个数"（第一版的错就是这么暴露的），但不证明命中与编码各自的方向没错。
8. **缓存的正确性靠"换问题就整个作废"**：缓存存的是"与当前问题的相似度"，而键只有消息内容哈希（`examples/context-window-relevance/main.go:204-207`），所以参考问题一换必须清空整张表，不能增量淘汰。本例全程只有一个问题，这条路径没被走到，属于形状示范；`cap = 512` 的整体丢弃同样未触发（实际 10 项）。
9. **`whereIs` 的打印范围限定成"末次输入"**：它只看最后一次发给模型的 messages，回答不了"中间某步有没有被裁过"——那一问由上一行的裁剪次数回答。

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test -count=1 ./...` = 31 包 ok、无 FAIL。工作树干净，两个新示例目录已随 `fd8f7a8` 入库。
- `gofmt -l` 对 `middleware/window_test.go` 与两个示例无输出（三个文件均 LF；`middleware/` 下其余被标记的文件是本卡之前就有的 CRLF，未动）。
- 两个示例各连跑三次，`cmp` 逐字节相同。
- 示例一五遍：`RecentN` 那遍"决定"**不在**末次输入（正是它不该用于长任务的证据）；`ImportanceWeighted` 那遍**在**；`SlidingWindow` 那遍末次输入 8 条、巨大工具输出 0 条；第 5 遍同时打印 `HistoryCompacted`（step 3 摘要掉 3 条、留下 4 条）与 `WindowTrimmed`（2 条）。
- 示例二：裁剪事件 5 次、末次 7 条消息 10 类内容；打分 81 条 / 送去编码 10 条 / 命中缓存 71 条 / 编码参考文本 1 条 / 当前缓存 10 项（上限 512，打印里注明"本例未触顶"，免得读者以为丢弃路径跑过）；逐项比对打印"有分歧：第 1 遍独有 1 类，第 2 遍独有 1 类"（第 1 遍留 `fetch 第 6 次`，第 2 遍留 `fetch 第 3 次`，两遍都留末尾那条——最新单元无条件保留）。
- "打分调用次数明显小于消息数 × 步数"这条：实测消息数 × 步数约 9 × 9 = 81 恰是打分的条数（打分是本地算术，每条都算），真正省下的是**编码**：81 条里只送编 10 条。卡的原文把两者混写成"打分调用次数"，验收按编码次数读。

---

## 执行顺序与提交切分

1. CW-01 → `refactor(middleware): split the message history into indivisible eviction units`
2. CW-02 → `feat(core): WindowTrimmed event for context-window eviction`
3. CW-03 → `feat(middleware): Window bounds one request by evicting message units`
4. CW-04 → `feat(middleware): importance-weighted window strategy with a pluggable scorer`
5. CW-05 → `docs(examples): offline demos for the three context-window strategies`

提交信息用英文（与仓库历史一致，Conventional Commits，不加 trailer）。每 TASK 单独提交，中间态 `go build ./...` 必须通过。`git push` 等确认后再做。

CW-01 与 CW-02 之间没有依赖，可并行；CW-03 依赖两者（`Select` 要发事件）。CW-04 依赖 CW-03。CW-05 依赖全部。

## 明确不做

- 不给 `CompactionOptions` 加 `Strategy` 字段（ADR-0031 备选 B，已否决）。
- 不改 `agent` 包任何文件：不新增钩子、不改阶段时序、不给 `core.Message` 加 ID/时间戳（备选 F，已否决）。
- 不做单元内部截断（单条消息超长时切开内容）：属于工具层与循环层的既有职责。
- 不给 `ImportanceOptions` 加权重配置表；系数与角色先验固定并写注释。
- 不做"被裁消息写进 `memory` 供后续检索"的补偿通路：需要跨包引入 store 与写入时机决策，另开 ADR。
- 不实现 embedding 打分器进 `middleware` 包（保持该包不引 `embeddings`/`memory` 依赖），示例自带实现。
- 不修 `queue.MemBus` 的既有竞态（ADR-0030 期间登记，与本卡无关，勿顺手改）。
- README 与 `middleware/middleware.go` 包注释不新增行：后者那份清单本就只列 Tracing/RateLimit/Compaction/RAG 五条（`middleware/middleware.go:1-15`），LoopGuard/RunBudget/ArgGuard/ToolTimeout 都没登记，单为本特性加一行会造成新的不一致；整体补齐另开文档任务。

## 风险

| 风险 | 现状判断 | 处置 |
| --- | --- | --- |
| 非连续选择破配对，服务端 400 | provider 无校验（`llm/openaicompat/openaicompat.go:178-187`、`llm/anthropic/anthropic.go:173-185`）；`safeCut` 只管连续边界 | 已修（设计层）：单元不可分 + 修复段 + 全局配对断言（CW-01 的 `TestKeptMessagesNeverSplitsAUnit`、CW-03 的 `TestWindowResultPassesPairingCheck`） |
| 修复段给悬挂调用补合成结果，破坏 HITL 恢复 | 悬挂是合法落盘状态（`agent/loop.go:236-244`） | 已修：设计明确"只删孤儿不补结果"，并由 `TestRepairPairingKeepsPendingCallUntouched` 钉住。这一条更正了需求分析阶段的口头结论 |
| 裁剪后首条消息变成 assistant（`RecentN`/`SlidingWindow` 的 persist 模式尤其容易固定成这个样子），provider 对消息序列的额外要求未逐一验证 | 本卡只核实了"孤儿结果会被拒"这一条；首条角色是否受约束未验证，属未证实项 | 登记不修：`ImportanceWeighted` 默认钉住首条 user（`NoPinFirst` 才关）天然回避该形态；`RecentN`/`SlidingWindow` 先不加同名选项，等真撞到具体 provider 报错再加（届时也是十行内的改动）。示例全程 `llm/mock`，不会暴露这个问题，故在文档注释里点名提醒 |
| `_window` 与 `_compaction` 两个校准系数长期分叉，读者以为它们同源 | 两者独立采样、都针对"最终发出的那份消息集"，因此应当接近但不必相同 | 已修：CW-03 的 `TestWindowCalibrationIsolatedFromCompaction` 断言两个键都在且互不覆盖；两处注释写明同指针与逆序两个事实 |
| request 模式每步一条 `WindowTrimmed`，事件流变吵 | 与 `HistoryCompacted` 只在持久改写时发不同，这是每步真实发生的裁剪 | 已修（有意为之）：ADR-0031 §6 写明理由，读者按 `Step` 区分。若实践证明确实过吵，加"仅当形状变化才发"的去重要在 `State.KV` 存一份上一次签名 —— 那是新的状态写，等有需要再做 |
| `ImportanceWeighted` 的内容分依赖 `ref`（最后一条 user 消息），中途换话题时旧话题的关键消息会被大批裁掉 | `lastUserText` 的取法与 `middleware/rag.go:49-51` 相同，是仓库既有的相关性口径 | 登记不修：这是"以当前问题为准"这一定义的必然结果；`PinFirst` 至少保住任务定义。要更稳需要话题/意图跟踪，超出本卡范围 |
| 注入远端打分器时逐步全量重算，成本失控 | 内置启发式无此问题；远端调用是用户自己选的 | 已修：接口注释明确要求实现侧按内容哈希缓存（CW-05 示例给了完整实现，并把"打分多少条、其中送去编码多少条、命中缓存多少条"三个数打在输出里，实测 81 / 10 / 71）；不把缓存塞进 `State.KV` 的取舍写进 ADR-0031 §7 |
| 卡把顺序差异的前提写成"Window 先跑最终规模可能更大"，在该用例的形状下看不出来 | 两种注册顺序最终都是 1 条消息（`middleware/window_test.go:510`、`:528` 都断 `!= 1`），差异体现在"谁的活白跑"：Compaction 先跑会被窗口裁掉刚写的 note，Window 先跑则摘要器一次都没调（`calls == 0`）。这不推翻 ADR-0031 §8 的顺序规则——`Compaction.MaxTokens` 是触发阈值不是上限 | 已修：完成记录按实测改写这条并标明适用范围，用例名与断言按实测写（`c98beb9`）；卡的原文保留作为出处 |
| 示例里两条内容全同的裁剪决定会被"按内容去重"的比对吞掉，读起来像"两遍相同" | 第一版 `fetch` 每条返回同样的 500 个 `y`，两遍保留集在去重比对里必然相同；把内容改成变动长度后又因总量落到预算以下而一步都不裁（`裁剪事件 0 次`） | 已修：`fetch` 带编号、长度随 n 变化且单次下限 900 字节（八轮合计远超 `budget = 700`），实测裁剪 5 次、两遍各有一类独有内容 |
| 单元分组依赖"结果紧随调用消息"这一循环不变量；若将来把工具结果改成多条消息或插入别的消息，分组会退化 | 当前由 `agent/loop.go:203,222,256` 三处保证 | 已修：不变量的出处写进 `groupUnits` 注释并引这三个位置；CW-01 的表驱动里放了"user 插在中间打断相邻性"这一形状，退化行为是分裂成新单元（不报错、不丢消息），有断言 |
| 一次 `go vet` 可能报未使用的私有函数（CW-01 只交付私有函数与测试） | CW-01 的 `groupUnits`/`repairPairing`/`keptMessages` 在本 TASK 只被测试引用 | 已实测不修：`go vet ./...` 对未使用的私有函数无输出（Go 只对未使用的局部变量与 import 报错）。CW-01 落地时 `go build`/`go vet`/全仓 `go test` 均为 0 |
