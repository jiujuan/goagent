# ADR-0025: Compaction 历史改写与校准后的 token 计数

- 状态: Accepted（已实施：`core.HistoryCompacted`、`agent.HistoryCompacter`+`Stack.CompactHistory`+loop Phase1 调用点、`middleware/compaction.go` 的 Counter/Persist/校准/配对切点）
- 日期: 2026-09-29
- 关联: ADR-0023(LoopGuard)、ADR-0024(RunBudget)、`middleware/compaction.go`、`agent/loop.go`

## 背景

现有 `middleware.Compaction`（`middleware/compaction.go:29-60`）只做请求级改写：每次模型调用前，若消息历史超过阈值，就把除最近 `KeepRecent` 条以外的旧消息摘要成一条说明并替换进**请求**（`ModifyRequest`）。这有三个直接后果：

1. **压缩结果不落盘**。loop 每步从 `history` 局部变量重建请求（`agent/loop.go:145`），`State.Messages` 每步照原样全量保存（`agent/loop.go:222`）并被 checkpoint 序列化。历史越长：
   - 每一步的模型请求都在重复发送全部旧消息（压缩只影响单次请求，下一步又从头来）；
   - checkpoint 文件每步追加"完整历史"的快照（`checkpoint/file.go:40-57` 每步一行 JSON），单线程文件按步数×历史长度增长。
2. **摘要被反复重算**。超过阈值之后，每一步都会对同一段旧消息重新调用摘要模型——旧前缀从未被真正移除，这个模型调用成本每个可避免的步骤都要付一次。
3. **token 估计过于粗糙且对中文严重失真**。`estimateTokens` 按 `len(m.Text())/4` 计（`compaction.go:86-92`）：
   - `len()` 是 UTF-8 **字节数**，中文 3 字节/字 → 估计约 0.75 token/字，而主流分词器对中文实际约 1～1.5 token/字，低估可达近一倍；
   - 完全不统计 ToolCall 参数、Thinking、非 Text 部件；
   - 不统计系统提示词和工具模式（阈值名义上是"请求大小"，实际只看到消息正文）。

同时 `llm.Model` 接口没有计数方法（`llm/model.go:17-24`），provider 也不都提供计数接口；但**每次响应的 `Usage.InputTokens` 就是上一次请求的真实大小**（`llm/model.go:54-55`）——这是免费的精确校准源。

## 目标

1. 压缩可以**改写会话历史本身**并随 checkpoint 落盘：摘要只算一次、历史真正变短、旧消息不再每步重复发送。
2. token 计数升级为** rune 感知 + 用真实 Usage 校准**的估计；同时留一个可注入精确计数器的口子（不新增重依赖）。
3. 完全向后兼容：默认行为不变（仍然只改请求），通过配置项开启历史改写。

## 名词（先约定，后文只用这些词）

- **历史改写（persist）**：把压缩结果写回 `State.Messages`，使压缩对后续步骤与 checkpoint 生效。
- **请求改写（现有行为）**：只改单次模型请求，不动 `State.Messages`。
- **估计器（counter）**：给定消息/请求算出近似 token 数的函数。
- **校准系数**：`真实 InputTokens ÷ 同一次请求的估计值`，滚动更新，用于修正估计器系统性偏差。
- **配对约束**：assistant 消息携带的每个 tool call，其 tool 结果消息紧随其后；压缩切点不得让"结果被保留而发起它的消息被删掉"，否则 provider 请求非法。

## 决策概览

三块改动，按依赖顺序：

### 1. agent：新增可选能力 HistoryCompacter（唯一的 loop 侧改动）

```go
// agent/middleware.go
// HistoryCompacter 是可选中间件能力：实现它的中间件可以在每个步骤开始时
// 改写会话历史本身。返回值替换循环内的 history，随后照常进入请求构造与
// 步骤末的 checkpoint——因此改写天然落盘。
type HistoryCompacter interface {
    CompactHistory(lc *LoopContext, history []core.Message) []core.Message
}
```

- `Stack` 增加折叠方法：按注册顺序把 history 依次交给每个实现者（无人实现时原样返回，与 `Stack.ModelContext` 同一扩展先例，`agent/middleware.go:46-55`）。
- loop 调用点在每步开始、drain steering 之后（`agent/loop.go` Phase 1 内），一行：
  `history = l.mw.CompactHistory(lc, history)`，并同步 `lc.History`。
- 不新增 Directive 种类、不动终止条件；`ErrMaxTurnsExceeded` 等语义不变。

### 2. middleware：Compaction 增加历史改写模式

`CompactionOptions` 增加两个字段（零值 = 现状）：

```go
type CompactionOptions struct {
    Model      llm.Model
    MaxTokens  int          // 阈值：估计超过它才压缩（语义不变，见第 3 节计数升级）
    KeepRecent int          // 保留最近多少条（不变，默认 6）
    // Counter 是消息计数函数；nil 用内置估计器（第 3 节）。
    // 想接精确分词器（如 tiktoken）的使用者在这里注入自己的实现。
    Counter func(msgs []core.Message) int
    // Persist 为 true 时启用历史改写：压缩结果写回消息历史并随 checkpoint 持久，
    // 每段旧消息只摘要一次；同时 ModifyRequest 不再改请求（两条路径互斥）。
    Persist bool
}
```

历史改写模式的处理顺序（`CompactHistory`）：

1. `counter(history) <= MaxTokens` → 原样返回。
2. 计算切点 `cut = len(history) - KeepRecent`，然后**向前调整满足配对约束**：循环回退直到 `history[cut]` 是 user 或 assistant 消息（tool 结果不能成为保留段开头）。`cut == 0` 时放弃压缩（只有摘要没有正文没有意义）。
3. 用 `Model` 摘要 `history[:cut]`（沿用现有摘要提示词；失败则原样返回，best-effort 立场不变）。
4. 返回 `[摘要说明消息] + history[cut:]`。摘要说明沿用现有前缀 `[earlier conversation summary]`（`compaction.go:55-57`），使 RunBudget/LoopGuard 等对历史文本的处理方式不变。

**原始记录不丢**：checkpoint 树每步都存了改写前的完整状态（`checkpoint/file.go` 是追加式 JSONL，`History`/`Load` 可按 ID 取回任意旧快照），历史改写只影响"从现在起"的工作历史，审计与时间旅行仍然可用。文档必须写明：`FinishRun`/memx 固化看到的是改写后的历史——这是"摘要进入长期记忆"的预期行为而非缺陷。

观测：新增事件 `core.HistoryCompacted{Dropped int; Kept int; EstTokens int; Step int}`（sealed union 加变体 + `eventjson.go` 编解码，同 StuckDetected/BudgetExceeded 先例）。

### 3. middleware：估计器升级 + Usage 校准

内置估计器（`Counter == nil` 时）：

```go
// 对每条消息：
//   基础开销 4 token/条（各主流消息模板的角色分隔经验值，允许通过校准修正残差）
//   Text / Thinking 部件：CJK 文字按 1 token/字；其余按 rune 数 / 4（向上取整）
//   ToolCall：名称 rune/4 + 参数 JSON 字节/4 + 4
//   ToolResult：按 Text 规则统计内容，另加 4
// 系统提示词与工具模式：现有请求改写路径里可以统计；历史改写路径只有消息，
// 因此阈值在两种模式下统一按"消息部分"计算，文档写明（现行为也是如此）。
```

（rune 级 CJK 判别：`unicode.Is(unicode.Han, r)` 加上日文假名/韩文区间——实现时用一个 `isWide` 辅助函数收口，常数 1 token/字是偏保守的起点，靠校准收敛。）

**校准**（两种模式都启用，无额外开关；`Counter` 非 nil 时同样校准——校准的是"这个计数器的偏差"，仍然成立）：

- `Compaction` 增加 `AfterModel`：取本次 `resp.Usage.InputTokens`（真实值）与 `counter(lc.Request.Messages)`（同一次请求的估计值，注意请求改写模式下这是压缩后的消息集——正是实际发送的内容，校准依然对得上）。
- `ratio = clamp(真实/估计, 0.5, 2.0)`；指数滚动 `calib = 0.7*calib + 0.3*ratio`（首次直接取 ratio），存 `State.KV["_compaction"]`。估计值判阈值时乘 `calib`。
- 效果：第一次调用后即获得 provider 口径的精度，之后每步继续修正。无 Usage 的模型（Usage==nil）不动系数。
- 持久化：系数随 checkpoint 走，resume 后校准不丢——与 ADR-0023/0024 的状态立场一致。

## 范围

### 做
- `agent`：`HistoryCompacter` 接口 + `Stack.CompactHistory` + loop 一行调用点。
- `middleware/compaction.go`：`Counter`/`Persist` 两个新配置、配对约束切点、新估计器、Usage 校准（KV 存储）。
- `core`：`HistoryCompacted` 事件 + 编解码。
- 测试全覆盖（见下）+ 把 `examples/middleware` 的注释更新为提到持久模式可选（不新增强制示例；若需要再补独立离线示例）。

### 不做（理由）
- **不引入 BPE 分词依赖**（如 tiktoken-go）：编码文件首次使用需联网拉取、体积大，且各 provider 词表不同，"本地精确"是伪精确——真实精度靠 Usage 校准解决，真要接就在 `Counter` 注入。
- **不做增量摘要（rolling summary）**：现方案已保证每段旧消息只摘要一次；"对已有摘要再摘要"的链式质量问题是另一层优化，不混入本次。
- **不删除 checkpoint 旧快照**：配对约束保证请求合法，旧全量快照保留成本由追加式 JSONL 天然承担，删除会破坏时间旅行。
- **不改 `MaxTokens` 字段名**：叫 threshold 更准确，但改名破坏兼容；注释写清语义即可。

## 备选方案

- **A. 压缩直接写 `State.Messages`，不加新接口**：loop 每步末尾会用局部 `history` 覆盖 `State.Messages`（`agent/loop.go:177,222`），中间件在现有任何钩子里改写 State 都会在下一步被冲掉——必须经过 `history` 这个变量本身，因此需要 loop 侧的显式接缝。否决（不成立）。
- **B. 在 loop 里内建压缩阶段**：违背"控制类能力全部 middleware 化"的既有立场，同 ADR-0023/0024 的备选 B。否决。
- **C. 每步都重算摘要但缓存结果**（按前缀哈希缓存摘要文本）：不解决历史增长与重复发送问题，只是省模型调用。否决。
- **D. 只用固定分词表、不做 Usage 校准**：与 provider 真实计费口径必然存在偏差，且中文失真问题依旧。否决。

## 迁移与兼容性

- 默认 `Persist=false`：现网行为唯一的变化是估计器变准（中文场景阈值更早触发）与校准生效——这是缺陷修正；需要完全旧行为的用户可注入旧 `Counter`（`len/4` 一行函数）关闭两者影响。
- `HistoryCompacter` 是新增可选接口，第三方中间件不受影响；未实现者 loop 行为逐字节不变。

## 测试锚点

- 估计器表驱动：纯 ASCII / 纯中文 / 中英混合 / 带 ToolCall 参数 / nil Counter 默认值；校准乘数与 clamp 边界。
- 配对约束：构造 `[user, assistant(calls), tool..., assistant(calls), tool...]`，KeepRecent 落在 tool 消息中间 → 断言切点回退到 assistant 边界，保留段无孤儿 tool 结果；cut==0 → 不压缩。
- 历史改写落盘：文件 checkpoint 跑一个必然触发压缩的对话 → 断言最终 `State.Messages` 缩短、摘要说明消息存在；旧快照（`store.History`）仍含压缩前全历史；压缩后步骤的请求明显变短（摘要模型调用次数 = 压缩次数，stub 计数断言"不逐步重复摘要"）。
- 校准：mock 回假 `Usage.InputTokens`（如估计 100 真实 200）→ 断言 KV 系数≈2，后续阈值判断按乘数生效；Usage==nil 不动系数。
- 互斥：`Persist=true` 时 `ModifyRequest` 不改请求。
- 事件：`HistoryCompacted` round-trip + 触发时机（改写那次出现）。
- 回归：`examples/middleware` 既有用例、agent/durable 全量测试跑绿。

## 实施切分

1. `core`：`HistoryCompacted` 事件 + 编解码 + roundtrip 测试。
2. `agent`：`HistoryCompacter` + `Stack.CompactHistory` + loop 调用点（独立小 commit，向后兼容）。
3. `middleware/compaction.go`：估计器 + 校准 + `Counter`/`Persist` + 配对约束切点。
4. 测试（上表全覆盖）+ `go test ./...` 全绿。
