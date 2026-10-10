# ADR-0031: 上下文窗口的三种裁剪策略（Window）

- 状态: Accepted（CW-01…CW-05 全部实施完毕，五个特性提交 `b2d6222`、`13404bb`、`b71ebbc`、`e3893c4`、`c98beb9` + `fd8f7a8` + `14b3b3a`，均未推送。依次：`middleware/window.go` 的 `unit`/`groupUnits`/`keptMessages`/`repairPairing` 与 `middleware/compaction.go` 的带键校准 helper；`core.WindowTrimmed` + `eventjson` 的 `Strategy` 字段与两条 case；`Window` 中间件（双模式 + 校准 + 事件）与 `RecentN`/`SlidingWindow`；`ImportanceWeighted` + `MessageScorer` + 内置启发式打分器；顺序用例与两个离线示例。实施期与本文的差异（`PinFirst` 改名 `NoPinFirst`、打分器必须读工具结果内容、中文分词、§12 两条示例的实测形态、以及"注册顺序不改变最终规模"这一处前提修正）记在 `docs/tasks/plan-context-window.md` 各 TASK 的"完成记录"里）
- 日期: 2026-10-08
- 关联: ADR-0025（Compaction 历史改写与校准后的 token 计数）、ADR-0024（RunBudget）
- 执行卡: `docs/tasks/plan-context-window.md`
- 需求方已拍板的三项形态（2026-10-08，不再重议）：独立中间件而非并入 Compaction；重要性打分默认纯启发式、打分器可注入；`Persist` 默认 false。

## 背景

仓库里管上下文尺寸的中间件只有一个：`middleware.Compaction`。它做一件事 —— 历史的校准估算超过 `MaxTokens` 时，把除最近 `KeepRecent` 条之外的全部消息交给摘要模型，换成一条笔记（`middleware/compaction.go:40-49` 的文档注释、`:123-140` 的实现）。三条缺口都在"只能这么切"这一点上：

1. **选择法只有一种**：按时间远近切一刀。没有"只留最近 N 条、不摘要"（不想为省窗口再花一次模型调用），没有"按 token 预算给一个确定上限"（一次体积巨大的工具输出就能让窗口失控），也没有"按内容重要性挑"（第 30 步的一条关键决定，按时间法会被当作旧消息摘要掉）。
2. **窗口能力与"能不能再调一次模型"绑死**：`Model` 为空时 `compact` 直接返回不压缩（`middleware/compaction.go:124`）；摘要调用失败时静默放行、历史原样不动（`:134-137`）。两种情况下窗口完全没有防护，而调用模型失败是常事。
3. **计量单位只有一个数**：`MaxTokens`（`:26`，默认 8000）。"消息条数"这个单位没有入口。

相邻的两个机制都不覆盖这里：`middleware.RunBudget` 管的是**一个 run 累计**花掉多少 token/钱（`middleware/runbudget.go:38-40`），与单次请求规模无关；`memory/workingmem` 把关键事实放进 `State.KV`，其包注释明确说这样就不受压缩影响（`memory/workingmem/workingmem.go:1-9`），那是"别让重要事实只在消息里"的对策，不是窗口策略。

## 名词

- **裁剪（eviction）**：把消息从本次请求或本次历史里去掉，不生成任何替代文本。与"压缩/摘要"相对 —— 后者有损但保留语义，前者无损但丢内容。
- **消息单元（unit）**：裁剪的最小决定单位，定义见第 3 节。
- **预算（budget）**：以校准后的估算 token 计的保留上限。

## 决策概览

| 层 | 加什么 |
| --- | --- |
| middleware | 新文件 `middleware/window.go`：`WindowStrategy` 接口、`Window` 中间件、`RecentN` / `SlidingWindow` / `ImportanceWeighted` 三个策略工厂、默认启发式打分器 |
| core | 一个事件 `WindowTrimmed` + `core/eventjson.go` 一个新字段与两条 case |
| agent | **不改**。复用既有的 `HistoryCompacter`（`agent/middleware.go:134-145`）与 `ModifyRequest`（`:22-29`）两个挂载点 |
| compaction.go | 只把校准读写提成带键名参数的包内私有 helper，行为不变 |

三条立场：

1. **独立中间件**，不与 Compaction 共用配置与实现（第 9 节备选 B 给了否决理由）。
2. **不新增循环钩子**。窗口策略需要的两个时机（改历史、改请求）Compaction 已经各自有一个，且都带测试。
3. **裁剪永不切断一个消息单元**，这是三种策略共同的、优先级最高的正确性约束（第 3 节）。

---

## 1. 三种策略的语义切分

这三个词在业界的通常含义互相重叠（"sliding window"在很多框架里就是指"最近 N 条"）。本 ADR 把它们固定成下表，实现时把这张表写进 `window.go` 的包注释和每个工厂的文档注释，避免后来者按外部语义理解。

| | `RecentN` | `SlidingWindow` | `ImportanceWeighted` |
| --- | --- | --- | --- |
| 保留依据 | 位置 | token 预算 | 分数 |
| 触发计量 | **单元条数** > keep | 校准 token > 预算 | 校准 token > 预算 |
| 保留段形状 | 连续尾部 | 连续尾部 | 非连续 |
| 调用模型 | 否 | 否 | 否（注入远端打分器时才是） |
| 需要 token 计数 | 否 | 是 | 是 |
| 典型用途 | 便宜的兜底、演示 | 给请求规模一个确定上限 | 长任务里保住关键决定、丢掉过时的大输出 |

`RecentN` 与 `SlidingWindow` 的差别必须是**计量单位**（单元条数 vs token），否则两者等价；`SlidingWindow` 存在的理由正是"一条巨型工具输出就能顶穿窗口"，此时按条数保留给不出可预测的规模。

**边界情形**（三策略共用一条规则）：最新的单个单元本身就超过预算时，**整单元保留**，不做部分截断、不产生空请求。理由：窗口策略的职责是"少带点东西"，不是"改写带上的东西"——单条工具输出的截断属于工具层，仓库里已有先例（`tool/file/file.go:71-79` 的读取上限、`tool/web/web.go:160,199` 的输出 rune 上限、循环侧的 `truncatedResults`）。把裁剪做成截断会造出第二套截断机制。该情形可以从事件里读出来：`EstTokens` 仍大于预算而 `Dropped` 为 0。

## 2. 挂载点与两种模式

`Window` 同时实现 `agent.Middleware` 与 `agent.HistoryCompacter`，模式由 `Persist` 决定，两条路互斥 —— 与 Compaction 的 `ModifyRequest`/`CompactHistory` 互斥体例一致（`middleware/compaction.go:92-118`，互斥断言在 `middleware/compaction_test.go:27`）。

- `Persist=false`（默认）：在 `ModifyRequest` 里改写 `req.Messages`，`State.Messages` 保持全量，每步重算。
- `Persist=true`：在 `CompactHistory` 里返回新的历史，循环把它当本步工作历史（`agent/loop.go:165-166`），并在本步末随 `rc.State.Messages = history` + `l.checkpoint` 落盘（`agent/loop.go:259-260`）。

两条已从代码核实的时间约束，实现时必须遵守：

1. **`CompactHistory` 阶段读不到请求**。`lc.Request` 在 `agent/loop.go:180` 才被赋值，而 `CompactHistory` 在 `:165` 调用。所以持久模式的策略不能依赖 `lc.Request`（Compaction 的持久模式同样不依赖，它的摘要只用历史，`middleware/compaction.go:179-201`）。
2. **`AfterModel` 是逆序跑，且 `lc.Request` 与 `req` 是同一个指针**。`Stack.AfterModel` 倒序遍历（`agent/middleware.go:266-277`），`Stack.ModifyRequest` 正序（`:187-194`）。两个裁剪中间件同装时，先注册的先改 `req.Messages`，两个的 `AfterModel` 都从同一个 `lc.Request.Messages` 取估算值 —— 也就是说校准值针对的是**最终真正发出去的那份消息集**，这正是想要的语义（估算器要修正的是"发出去多少东西"），但不是"各测各的"。这句话要写进代码注释。

## 3. 消息单元：本设计的正确性地基

先列核实到的事实。

- `ToolResult` 用 `CallID` 与某条 assistant 消息里的 `ToolCall.ID` 关联（`core/message.go:64-77`），`Message` 本身不带任何配对信息。
- **provider 侧不做配对校验，原样搬运**：`llm/openaicompat/openaicompat.go:178-187` 把每条 `ToolResult` 直接转成 `role:"tool"` + `tool_call_id`；`llm/anthropic/anthropic.go:173-185` 转成 `tool_result` block 并带上 `tool_use_id`。因此一条没有对应调用的结果会到服务端才被拒。
- **循环保证配对相邻**：assistant 消息入历史在 `agent/loop.go:203`，它的**全部**结果打包成**一条** `RoleTool` 消息紧随入历史在 `:256`；max_tokens 截断路径同样按调用数补齐结果（`:222`）。
- 既有的 `safeCut`（`middleware/compaction.go:146-152`）只把切点回退到 user/assistant 边界，从而保证**连续尾段**不会以孤儿结果开头。它对非连续选择无能为力 —— 这正是 `ImportanceWeighted` 的形态。

**单元规则**（`groupUnits(msgs []core.Message) []unit`）：

1. 顺序扫描。遇到一条含 ≥1 个 `ToolCall` 的 assistant 消息，开一个单元，记下它的 CallID 集合。
2. 向后并入紧随的 `RoleTool` 消息，条件是该消息**至少有一个** `ToolResult.CallID` 落在集合里。
3. 遇到第一条不满足条件的消息（含不匹配的 `RoleTool`）就停止并入；那条消息由后续单元或修复段处理。
4. 其它消息（user、无调用的 assistant、找不到归属的 tool）各自成一个单元。
5. **所有策略只在单元粒度上决定去留**，从不越过单元边界。

**修复段**（保留集定下之后跑一次，只做一件事）：

- 删除孤儿：保留集里 `CallID` 在"被保留的 assistant 调用集合"中不存在的 `ToolResult` part；某条 `RoleTool` 消息的结果被删空时整条删掉。
- **不给被丢掉的调用补合成结果。** 这一条与需求分析阶段（本会话前一轮）的口头结论相反，是读了循环的 HITL 路径之后改的：人工门径中断时，循环会把"assistant 带 ToolCall、结果尚未产生"的状态**原样落盘**（`agent/loop.go:236-244`，那里的注释写明 "Persist history (incl. the assistant tool-call message) plus the still-pending calls"），恢复时由 `runResumed` 把批准后的结果接上去（`agent/loop.go:119-128`、`agent/hitl.go:116`）。也就是说悬挂是合法状态；给它补一条错误结果，会让恢复路径读到"已经答复过"的假象。单元不可分已经保证**本中间件自己不会造出悬挂**，所以修复只需要处理"输入历史里本来就存在的孤儿"。
- **修复段只在这一步真的裁掉了东西时才跑**（实施期定形）。策略决定一条不丢时，中间件原样交回输入切片，也不清理历史里本来就存在的孤儿结果 —— Window 不是历史清理器，一个"没做任何决定"的步骤静默改写 `State.Messages` 属于另一个特性。代价要说明白：那份坏历史会照原样发给服务端并被拒；但在装这个中间件之前它就已经是这样发出去的了，顺手"修好"反而让用户看不出问题出在别处。用例 `TestWindowResultPassesPairingCheck` 的第三个子例钉的就是这条。

## 4. 类型与配置

```go
// middleware/window.go

// WindowStrategy decides which message units survive the window. count is the
// run's calibrated token measure (never nil; the middleware folds its
// estimator with the calibration factor learned from real Usage). Returning
// msgs unchanged means "no eviction this step". Implementations may only drop
// whole units — the middleware repairs pairing afterwards, but a strategy that
// splits a unit would drop tool results whose call is gone (providers reject an
// orphan tool result; see ADR-0031 §3).
type WindowStrategy interface {
    Select(lc *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message
}

type WindowOptions struct {
    Strategy WindowStrategy
    // Counter estimates tokens for a message slice. nil uses the built-in
    // rune-aware estimator (estimateTokens, compaction.go:247).
    Counter TokenCounter
    // Persist has the same meaning as in CompactionOptions: rewrite the working
    // history via HistoryCompacter instead of only the outgoing request.
    Persist bool
}

func Window(o WindowOptions) agent.Middleware   // 工厂不加 New 前缀，同 compaction.go:56

func RecentN(keep int) WindowStrategy
func SlidingWindow(budgetTokens int) WindowStrategy

type ImportanceOptions struct {
    BudgetTokens int          // 校准 token 预算
    TauUnits     int          // recency 衰减常数，单位是"单元"（默认 12）
    Scorer       MessageScorer // nil 用内置启发式打分器
    PinFirst     bool          // 固定保留第一条 user 消息（任务定义），默认 true
}

func ImportanceWeighted(o ImportanceOptions) WindowStrategy
```

预算数值由各策略自己保管（`RecentN(keep)`、`SlidingWindow(budget)`、`ImportanceWeighted{BudgetTokens}`），`WindowOptions` 不再放一个"有时生效有时忽略"的 `BudgetTokens` 字段 —— 与 `CompactionOptions` 把 `MaxTokens`/`KeepRecent` 放在自己配置里的做法同构，也少一处"这个字段对我的策略有没有用"的疑问。

`TokenCounter`（`middleware/compaction.go:17`，已有公开类型）复用，不新增类型。中间件每步构造一次闭包：

```go
factor := loadCalibAt(lc, kvWindow)                 // 每步一次 KV 读
count := func(m []core.Message) int { return int(float64(w.counter(m)) * factor) }
```

策略按单元调用 `count(unit)`，累加即预算比对；因为校准是乘性系数，"逐单元求和再乘"与"整体乘"数值等价，不引入新的偏差。

## 5. token 计量与校准

- 估算沿用 `estimateTokens`（`middleware/compaction.go:247-270`，同包私有，可直接调用），它按 rune 区分宽字符与 ASCII 并带每条消息 4 的框架开销，中文不会低估。
- 校准沿用"用响应真实 `Usage.InputTokens` 对估算求比值、限幅 0.5~2.0、EMA 系数 0.7/0.3、结果写 `State.KV`"这套算法（`middleware/compaction.go:158-173`、`205-229`）。
- **但必须写自己的键**：`saveCalib` 是整块覆盖 `KV["_compaction"] = map[string]any{"calib": v}`（`:227-229`），复用同键会让两个中间件互擦。做法是把 `loadCalib/saveCalib` 提成 `loadCalibAt(lc, key)` / `saveCalibAt(lc, key, v)`，Compaction 传 `_compaction`，Window 传 `_window`。这两个函数是包内私有，无公开 API 变化；既有的两条校准用例是这一步的安全网（`middleware/compaction_internal_test.go:65`、`:108`）。
- 预算只衡量**消息**，system prompt 与工具 schema 的开销由校准系数吸收 —— 与 Compaction 同一立场（`middleware/compaction.go:23-26`）。
- `RecentN` 完全不读计数与校准，所以 `Counter` 对它无效；但它仍然实现 `AfterModel` 吗？**不**。校准由 `Window.AfterModel` 提供，只在策略需要计数时才有意义。实现上统一提供 `AfterModel`（闭包成本一次估算），这样换策略不用换装配代码。

## 6. 事件 `core.WindowTrimmed`

```go
// core/event.go（放在 HistoryCompacted 之后，:105-114 的体例）
type WindowTrimmed struct {
    Strategy  string // "recent_n" | "sliding_window" | "importance_weighted"
    Dropped   int    // 被裁掉的消息条数（不是单元数）
    Kept      int    // 保留下来的消息条数（修复段之后）
    EstTokens int    // 裁剪前整段历史的校准估算
    Step      int
}
```

- `core/eventjson.go` 的 `eventWire`（`:18-38`）已有 `Dropped` / `Kept` / `EstTok` / `Step`，**只需新增 `Strategy string \`json:"strategy,omitempty"\``** 一个字段。
- `MarshalEvent` 加一条 case 放在 `history_compacted`（`:90-91`）旁，`UnmarshalEvent` 加一条 case 放在 `"history_compacted"`（`:143-144`）旁，类型串 `"window_trimmed"`。密封联合的 marker 也要补（`core/event.go:132-149` 那一串）。漏掉编解码不会编译失败，会在 `default` 分支运行时报 "cannot marshal event of type"。
- **发布时机**：两种模式都在"真的裁了"的那一刻发布一条。这与 Compaction 只在持久模式发布（`middleware/compaction.go:114-116`）不同，理由是事实性的：持久摘要罕见地发生、请求模式摘要每步重算，而 `WindowTrimmed` 描述的是"这一步实际发出去的东西被裁成了什么样"，request 模式下每步都真的发生了一次裁剪，逐步一条是准确的。读者按 `Step` 区分。

## 7. Importance-Weighted 的打分

打分接口只负责**内容相关性**，位置与角色之外的东西不进接口：

```go
// middleware/window.go
type MessageScorer interface {
    // ScoreContent scores each message's relevance to ref in [0,1], in the
    // given order. It must not depend on position: the strategy adds the recency
    // term itself, and implementations cache results by message content hash.
    ScoreContent(ctx context.Context, ref string, msgs []core.Message) ([]float64, error)
}
```

- `ref` 取 `lastUserText(req.Messages)` —— 同包已有的函数（`middleware/rag.go:66-73`），直接调用。
- 默认打分器零依赖、零网络：`role 先验 + 查询词重叠`。角色先验取 user 1.0 / assistant 0.7 / tool 0.35 —— 工具输出的体量占绝对多数而信息衰减最快，是默认该先丢的那类；重叠计数借用 `middleware.NewInMemory(...).Retrieve` 的打分思路（`middleware/rag.go:85-110`），大小写折叠、`strings.Fields` 分词。
  - 实施期补的两条必要修正（原设计会让默认打分器形同虚设）：① **必须读 `ToolResult.Content`** —— `Message.Text()` 只拼顶层 `Text` part（`core/message.go:101-109`），照原文实现的话所有工具输出永远 0 相关性、只剩角色先验在排序，与"按内容挑"这个卖点自相矛盾；取法与 `estimateTokens` 走 `ToolResult.Content` 同源。② **分词不能用 `strings.Fields`** —— 中文没有空格，整条问句会算一个 token，重叠度恒为 0。改为宽字符按单字成词、其余按字母数字成词，与 `embeddings/mock` 的 `tokenize`（`embeddings/mock/mock.go:65-88`）同一思路，宽字符判定复用本包的 `isWideRune`。
  - 配置字段 `PinFirst` 改名 **`NoPinFirst`**（opt-out 体例，同 `RunBudgetOptions.NoWrapUpLastTurn`）：本文写的是"默认 true"，而 Go 的零值是 false，`PinFirst` 无法区分"没设置"与"显式关掉"。
- 策略侧再加两项：**recency** `exp(-距离/TauUnits)`，距离按单元序号从末尾数（`core.Message` 没有时间戳，下标是唯一可用的位置信息）；**PinFirst** 把第一条 user 消息置为必留（任务定义被丢掉之后模型会重新开始一个别的任务，这是不可接受的）。
- 综合分：`score = content + 0.5*recency`。系数固定、注释里写理由，不提供 `Weights` 配置 —— 唯一需要的调节面是 `Scorer`，再加一组权重就是为假设需求做设计。
- 装箱：按 `score / max(1, count(unit))` **降序**贪心（性价比，不是绝对分数）。按绝对分数贪心会让一条"与当前问题相关但体积巨大"的工具输出吃掉整个预算，把若干条小而关键的问答全挤掉 —— 这条是必须钉住的行为，测试里有专门一例。选中后按原下标升序输出，再过修复段。
- 修复段之后消息数可能比贪心结果少（删孤儿），因此实际 token 只会更少，不会超预算。

**缓存**：默认打分器是纯本地算术，不需要缓存。注入的远端打分器（embedding）需要，缓存归打分器实现自己所有，键用**消息内容哈希**，先例是本包已有的两处：`middleware/loopguard.go:341-346`（sha256 + 规范化 JSON，取 16 位十六进制）与 `memory/memory.go:80`。缓存**不进 `State.KV`**：它是性能缓存不是账本，丢了只多算一次，进 KV 会让每步 checkpoint 都多带一份 map（`checkpoint/file.go:131` 每步全量追加写）。要有条数上限，满了整体丢弃，不做逐出策略。

## 8. 与 Compaction / RunBudget 的组合

- **顺序规则**：`Stack.ModifyRequest` 按注册顺序执行（`agent/middleware.go:187-194`）。想要硬上限保证，`Window` 必须注册在 `Compaction` **之后**（后跑的才能对最终结果负责）；想"先摘要再裁剪"就是现在的顺序。两个顺序都要有测试。
- **推荐搭配**：`Compaction{Persist:true}` + `Window(request 模式)`。摘要负责保留语义并缩小持久状态，窗口负责在每次发请求前兜住确定上限，且窗口不动 `State`，被它裁掉的东西仍能从本步的完整历史与早期 checkpoint 里回看。这条写进 `Window` 的文档注释与示例。
- **两个都 persist**：`Stack.CompactHistory` 按注册顺序折叠（`agent/middleware.go:257-264`），摘要先手会把历史变短、窗口通常就不再触发；窗口先手会把可摘要的旧消息直接丢掉、摘要的收益下降。能用但不推荐，注释里说明。
- **KV 命名空间**：`_window` 与 `_compaction` 互不读写。
- 与 `RunBudget` 无交集：一个管单次请求规模，一个管 run 累计成本（`middleware/runbudget.go:38-40`）。

## 9. 备选方案

- **A. 独立中间件 + `WindowStrategy` 接口**（取）。一个关注点一个中间件文件是仓库现状：`loopguard.go` / `runbudget.go` / `argguard.go` / `circuit.go` / `tooltimeout.go` 都是 `XxxOptions` + `Xxx(opts) agent.Middleware` + 内嵌 `agent.BaseMiddleware`（`agent/middleware.go:149-167`）。
- **B. 给 `CompactionOptions` 加 `Strategy` 字段**（否）。Compaction 的语义核心是"调模型摘要"，`Model` 是必填（`middleware/compaction.go:124` 以 `c.model == nil` 表示不压缩），摘要失败是 best-effort 放行（`:134-137`）。窗口策略不调模型、必然成功，塞进去后 `Model` 变成可空、`Persist` 与 `MaxTokens` 的语义随策略变化，三处既有测试（`middleware/compaction_test.go:27,52,107`）要重新解释，而换来的是少一个装配调用点。不值。
- **C. 新建 `contextmgr` 包**（否）。要跨包复用就得导出 `estimateTokens`、校准读写、`safeCut`，公共 API 面变大；同包私有调用即可，成本为零。
- **D. 采用 `docs/analysis/arch-optimization-code.md:468-478` 那份 `CompactionStrategy` 提案**（`ShouldCompact(totalMessages, estimatedTokens) bool` + `Compact(oldMessages, keepRecent)`，否）。两点：触发计量恰是三策略唯一的分歧点，收在中间件里判更清楚且少一份状态；而 `Compact(oldMessages, keepRecent)` 的签名预设了"旧消息 + 保留最近 N"这一种时间轴形态，非连续选择根本写不进去。
- **E. 裁剪时给被丢的调用补一条 `IsError` 占位结果**（否，理由见第 3 节修复段：会破坏 HITL 的合法悬挂状态）。
- **F. 用消息 ID 或时间戳做跨步骤的稳定识别**（否）。`core.Message` 只有 `Role` 与 `Parts`（`core/message.go:23-26`），加字段会波及所有 provider 转换与已落盘的 checkpoint JSONL。内容哈希够用，且本包已有两处同法先例。
- **G. 默认 `Persist=true`**（否，已由需求方确认取 request 为默认）。纯裁剪不保留语义，默认应当可回看。补充一条已核实的事实：被裁消息确实仍留在更早的 checkpoint 行里（`checkpoint/file.go:131` 追加写、`checkpoint.Checkpoint.State` 每步全量快照，`History()` 可列），但那是排障与 time-travel 路径，不是模型能自动检索的路径 —— 所以它不构成把 persist 设为默认的理由。

## 10. 成本

每步一次单元分组（O(消息数)）加一次整体估算；request 模式每步都跑，这与 Compaction 的 request 模式同量级（`middleware/compaction.go:92-100`）。`RecentN` 不做任何 token 计算。`ImportanceWeighted` 的默认打分是 O(消息数 × 查询词数) 的关键词重叠，与 `middleware/rag.go:85-110` 既有实现同量级。只有注入远端打分器时才有网络成本，由第 7 节的哈希缓存摊薄。

## 11. 测试锚点

- 单元分组：正常配对、一条 assistant 带两个调用且结果分装两条消息、悬挂调用（无结果）、开头就是孤儿结果、user 夹在中间打断相邻性。
- 修复段：删孤儿后不留没有结果的 `tool` 消息；**悬挂调用不被补合成结果**（这条断言直接防第 3 节那个改动）；保留集本来就合法时修复段是 no-op。注意"子集里没有孤儿"这一断言对**良构**历史是恒真的（按单元选择不可能造出孤儿），要另备一份本来就含孤儿的历史才能证明修复段在做事。`keptMessages` 的返回不得与输入共享底层数组（防后来者把连续段优化成切片窗口）。
- `RecentN`：按单元条数而不是消息条数保留 —— 尾部是一个"assistant+结果"单元时，`keep=1` 留 2 条消息而不是 1 条。
- `SlidingWindow`：预算内从尾部整单元累加；最新单元单独超预算时整单元保留且 `Dropped=0`；结果首条永不是 `tool`。
- `ImportanceWeighted`：性价比贪心那条（巨型相关输出 vs 多条小而关键）；`PinFirst` 生效与关掉两种；`Scorer` 返错时整步不动（best-effort，与 Compaction 摘要失败的处理一致）。
- 双模式互斥与持久性：`Window` 的两条路径各一例，持久模式断言 `State.Messages` 真的变小（体例参照 `middleware/compaction_test.go:52` 与 `agent/compacthistory_test.go:70`）。
- 校准隔离：`Window` 与 `Compaction` 同装、同跑若干步，断言 `_window` 与 `_compaction` 两个键各自独立、值都合理（防第 5 节那个整块覆盖）。
- provider 侧安全：把裁剪结果过 `llm/mock` 之外的最严格检查 —— 加一条断言"没有 `ToolResult` 的 CallID 落在保留集的 `ToolCall` 之外"，直接在修复段的输出上跑，不依赖真实服务端。
- 事件：`MarshalEvent`/`UnmarshalEvent` 对 `WindowTrimmed` 的往返（放 `core/stuckevent_test.go`，那里已有四条同类防护事件的往返用例，`:5,21,37,53`）。

## 12. 可运行示例（两个，均离线）

| 视角 | 命令 | 教什么 |
| --- | --- | --- |
| 三策略并排 | `go run ./examples/context-window` | 同一份长历史（十次工具调用 + 一条中途注入的关键决定）跑五遍：不装、`RecentN`、`SlidingWindow`、`ImportanceWeighted`，每遍打印模型最后一次实际收到的消息（条数、字符数、巨大工具输出条数）与 `WindowTrimmed`/`HistoryCompacted` 事件，并打印那条决定在不在输入里；第五遍装 `Compaction(persist) + Window(request)`，即本文 §8 的推荐搭配。实测：`RecentN` 那遍把决定裁掉了，`ImportanceWeighted` 那遍留着 |
| 相关性打分 | `go run ./examples/context-window-relevance` | 给 `ImportanceWeighted` 注入一个基于 `embeddings.Embedder`（`embeddings/mock`，离线）的打分器，与默认启发式在同一份脚本上对照，最后把两遍的保留集逐项比一遍（实测各有一类独有内容：预算边际上留下的是哪条工具输出）。同时打印打分器侧的三个计数（打分条数 / 送去编码条数 / 命中缓存条数）与参考文本编码条数，用来读"缓存在打分器自己手里"能省下多少远端调用 |

全程 `llm/mock` + `embeddings/mock`，无网络、无密钥，秒级完成；打印一律放在主流程（示例的既有约束：模型跑在另一个 goroutine，回调里打印会与事件消费交错）。

README 与 `middleware/middleware.go` 的包注释清单都不新增行：那份包注释列的还是 Tracing/RateLimit/Compaction/RAG 五条（`middleware/middleware.go:1-15`），LoopGuard、RunBudget、ArgGuard、ToolTimeout 都没登记，为本特性单加一行只会让不一致更显眼。整体补齐另开文档任务，与本 ADR-0030 §10 末尾的同一条理由一致。
