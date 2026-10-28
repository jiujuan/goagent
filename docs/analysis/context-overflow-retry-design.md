# 上下文超限恢复设计（P0-4）：压缩一次，重试一次

日期：2026-10-22
状态：设计提议，未实施
范围：只讨论 P0-4「上下文超限：压缩一次，重试一次」。工具并发调度属于 P0-3，见 [工具调用粒度并发调度设计](tool-call-concurrency-design.md)，本文不改动它。本文只做设计说明，不落代码；文中新增的导出类型、事件和选项都属于实施阶段的内容。
基线：`2f18017`（P0-1 StepView、P0-2 任务循环、P0-3 工具分段并发均已在该提交上）
关联：[Codex core 运行时对 goagent 的优化建议](codex-core-runtime-optimization-proposal.md)、[StepView 与外层任务循环设计草稿](stepview-and-task-loop-design-draft.md)

## 1. 结论

把「上下文超限」从一次普通失败变成一次**同步骤内的受控恢复**：

1. **先识别，再恢复。** 新增 `llm.ContextLengthError`，携带提供方、HTTP 状态、提供方报告的上限（`Limit`）与本次请求大小（`Requested`，未知时为 0）。只有 400/413 且响应体带有明确超限签名时才升级成这个类型；其余 400 仍是普通 `*llm.StatusError`。
2. **恢复能力挂在中间件上。** 新增可选能力 `agent.ContextOverflowRecoverer`：`RecoverContext` 收到工作历史和类型化错误，返回更短的历史。`Compaction` 和 `Window` 分别实现它，用比平时更低的目标强制压缩/淘汰。
3. **循环默认只重试一次，且只重试「还没说话」的调用。** 只要本次调用已经发出过任何 `MessageDelta` 或 `MessageDone`，就不重试，避免重复输出；取消、截止时间、普通 4xx 都不重试。
4. **重试留在原步骤。** 不发布额外的 `TurnStarted`/`TurnDone`，不消耗 `MaxTurns`，不重新执行 `BeforeModel`、`ModifyRequest` 或 `checkReplayDrift`，工具视图与设置摘要保持不变。
5. **请求从冻结视图重建。** 重试请求的 `System`、`Tools`、`Options` 原样取自 `StepView`，只有 `Messages` 换成恢复后的历史。这样 RAG 不会重复拼接系统块，RunBudget 的收尾指令不会被追加两次，`AddTool` 注入的工具也不会丢失，也不需要再跑一遍请求改写。
6. **先落盘，再重试。** 恢复后的历史先写入 `State.Messages` 并提交一个**中途检查点**（`Release:false`，步骤号不变），然后发布 `core.ContextOverflowRetried`，最后才发起第二次模型调用。进程在重试前崩溃，恢复的是压缩后的历史，而不是再次发送那条超限请求。
7. **第二次仍超限就返回类型化错误。** 不再压缩、不再重试；`errors.As` 能取到 `*llm.ContextLengthError`，检查点错误用 `errors.Join` 保留。如果第二次换成别的错误，返回第二次的错误。
8. **默认一次，可关闭。** 新增 `agent.WithContextOverflowRetry(n)`：默认 1 次额外重试，0 关闭，允许大于 1 但按「额外轮次」解释。没有安装任何恢复者时，循环行为与今天一致（不重试、不写中途检查点）；错误类型和 `RetryModel` 的跳过是全局变化。

一句话概括状态机：**模型调用 → 类型化超限且无输出 → 找一个恢复者把历史真正改短 → 基于冻结视图重建请求 → 写中途检查点 → 发事件 → 同一模型、同一步骤、同一工具视图再问一次 → 仍失败则类型化返回。**

## 2. 目标与非目标

### 2.1 目标

1. 让「上下文超限」在 `llm` 层可识别，且不依赖字符串匹配调用方代码；提供方响应体里的上限与请求量尽量结构化。
2. 在**当前步骤内**用一次真实的压缩换一次重试，而不是让整次运行直接失败。
3. 压缩必须真正缩短历史；返回原历史、等长历史或更长历史都不算恢复。
4. 恢复结果在重试前持久化，崩溃恢复后不会重放超限请求。
5. 重试请求与第一次请求在「模型看到的工具和设置」上完全一致，避免请求改写副作用叠加。
6. 全过程可观察：恢复者发布自己的 `HistoryCompacted`/`WindowTrimmed`，循环发布 `ContextOverflowRetried`。
7. 默认行为可预测：默认 1 次恢复轮次、可配 0 关闭；第二次失败返回第二次的错误；没有恢复者时不重试。
8. `RetryModel` 不再把上下文超限当成可重试的瞬时错误，避免在循环恢复之前先白白重试多次。
9. 压缩失败、检查点失败、冻结失败都不能掩盖最初的超限错误，必须能同时看到两个错误。

### 2.2 非目标

- 不改变工具并发调度、工具超时、工具审批或暂停恢复的语义（P0-3 的范围）。
- 不在 `agent` 层重新实现 token 估算或维护「各模型上下文窗口」配置表。`Limit` 只来自提供方响应体，未知就是 0。
- 不切割单条消息、单个工具调用或单个工具结果。如果整个历史只有一个单元且它自身就超过上限，恢复无法得到更短历史，返回类型化错误；如果还有其他单元，恢复会先丢弃/摘要其他单元再重试一次，第二次仍可能失败。
- 不自动切换模型、不自动缩短输出上限、不做「换一个窗口更大的模型」故障转移。`FallbackModel` 的默认判定不会把超限当失败；需要换模型的用户可以自定义 `ShouldFallback`。
- 不改变 `State`/`Checkpoint` 的 JSON 结构，不新增存储字段，不改 `StepSnapshot` 的内容。
- 不改变 `MaxTurns`、`RunBudget` 的计数方式；一次恢复不额外消耗一个 turn。
- 不重试「已经开始输出之后」的错误，也不尝试修补半条回复。
- 不把 `Compaction`/`Window` 的 `Persist` 默认值改掉；恢复路径对持久化的覆盖只发生在这一次超限恢复上。
- 不为上下文超限新增重试退避或抖动。摘要路径已经包含一次模型调用，重试本身也是一次新的提供方调用，立即重试是预期行为。
- 不修改 `Agnes` 图片/视频接口的错误构造；它不是聊天模型，走不到这条路径。

## 3. 现状

### 3.1 今天上下文管理发生在哪里

一个步骤开始后，循环先让实现了 `HistoryCompacter` 的中间件改写工作历史，再构造请求：

1. `history = l.mw.CompactHistory(lc, history)`：[agent/loop.go:281](../../agent/loop.go#L281)，折叠逻辑在 [agent/middleware.go:272](../../agent/middleware.go#L272)，能力接口在 [agent/middleware.go:158](../../agent/middleware.go#L158)。
2. 构造 `llm.Request`，再按顺序执行 `BeforeModel`、`ModifyRequest`、`freezeStepView`、`checkReplayDrift`：[agent/loop.go:303](../../agent/loop.go#L303)、[agent/loop.go:306](../../agent/loop.go#L306)、[agent/loop.go:309](../../agent/loop.go#L309)、[agent/loop.go:314](../../agent/loop.go#L314)。
3. 发起模型调用并发布增量/最终事件：[agent/loop.go:324](../../agent/loop.go#L324)、[agent/loop.go:479](../../agent/loop.go#L479)。

`Compaction` 有两种模式：[middleware/compaction.go:20](../../middleware/compaction.go#L20)、[middleware/compaction.go:40](../../middleware/compaction.go#L40)。

- **请求模式（默认）**：只在 `ModifyRequest` 里改写本次请求，`State.Messages` 不动：[middleware/compaction.go:92](../../middleware/compaction.go#L92)。
- **持久模式（`Persist`）**：在 `CompactHistory` 里改写工作历史，随步骤检查点落盘：[middleware/compaction.go:105](../../middleware/compaction.go#L105)。
- 两种模式都用 `State.KV` 里的滚动校准因子，把真实 `Usage.InputTokens` 与估算值的比例折算进阈值：[middleware/compaction.go:158](../../middleware/compaction.go#L158)、[middleware/compaction.go:175](../../middleware/compaction.go#L175)。

`Window` 的结构相同：[middleware/window.go:154](../../middleware/window.go#L154)、[middleware/window.go:207](../../middleware/window.go#L207)、[middleware/window.go:224](../../middleware/window.go#L224)。它用单元分组（[middleware/window.go:43](../../middleware/window.go#L43)）和配对修复（[middleware/window.go:117](../../middleware/window.go#L117)）保证不拆开工具调用与结果。

关键事实：**两种模式都只在「模型调用之前」运行**。模型调用一旦因为上下文超限失败，今天没有任何代码会再次调用压缩，也没有代码会把压缩结果写进检查点后重试。

### 3.2 今天的错误分类与重试

`StatusError` 是唯一的 HTTP 状态错误类型：[llm/error.go:14](../../llm/error.go#L14)。

- `Error()` 固定为 `<provider>: status <code>: <body>`：[llm/error.go:20](../../llm/error.go#L20)。
- `Retryable()` 只认 408、429、5xx：[llm/error.go:28](../../llm/error.go#L28)。
- `IsRetryable` 对取消/截止时间返回 false，对 `*StatusError` 交给 `Retryable()`，其余错误一律 true：[llm/error.go:37](../../llm/error.go#L37)。

两个真实提供方对所有非 200 都构造裸 `StatusError`：

- OpenAI 兼容：[llm/openaicompat/openaicompat.go:89](../../llm/openaicompat/openaicompat.go#L89)。
- Anthropic：[llm/anthropic/anthropic.go:89](../../llm/anthropic/anthropic.go#L89)。

于是上下文超限（通常是 400）被归为「调用方错误」，既不能重试，也无法和参数错误区分。

重试目前有两个层面：

- `RetryModel` 是模型装饰器，重试一切在**产生任何响应之前**失败的错误，包括 400：[middleware/retry.go:46](../../middleware/retry.go#L46)、[middleware/retry.go:64](../../middleware/retry.go#L64)。
- 循环本身不重试；`streamModel` 遇到错误只调用一次 `OnError` 观察钩子就返回：[agent/loop.go:486](../../agent/loop.go#L486)。

`FallbackModel` 默认用 `llm.IsRetryable` 判断是否切换：[middleware/fallback.go:38](../../middleware/fallback.go#L38)；`CircuitBreaker` 默认也用它判断失败：[middleware/circuit.go:85](../../middleware/circuit.go#L85)。因此给 400 增加「超限」识别后，这两个默认行为都不需要改，但 `RetryModel` 必须显式跳过。

### 3.3 中途失败时今天的落盘方式

`streamModel` 出错后，`runStep` 走 `fail`：[agent/loop.go:325](../../agent/loop.go#L325)、[agent/loop.go:567](../../agent/loop.go#L567)。`fail` 会把 `State.Messages` 设成 `seam(history)` 并写一个 `Release:true` 的检查点：[agent/loop.go:572](../../agent/loop.go#L572)、[agent/loop.go:573](../../agent/loop.go#L573)。这是失败恢复的「最后可重放点」，不是「压缩后的历史」——今天没有中间态可写。

### 3.4 提案说法与本仓库事实核对

| 提案说法 | 仓库事实 | 结论 |
|---|---|---|
| 压缩只在每步开头发生（提案引用 `agent/loop.go:202`） | 实际在 [agent/loop.go:281](../../agent/loop.go#L281)；提案的行号已随 P0-1/P0-2 前移 | 行为描述成立，锚点需更新 |
| 错误分类只有 `StatusError` 和「是否可重试」 | [llm/error.go:14](../../llm/error.go#L14)、[llm/error.go:37](../../llm/error.go#L37) | 成立 |
| 上下文超限通常是 400，400 被判为不可重试 | [llm/error.go:28](../../llm/error.go#L28)；提供方对所有非 200 造裸 `StatusError` | 成立 |
| 「本步还没有执行过工具、没有副作用」 | 超限发生在 `streamModel`（[agent/loop.go:324](../../agent/loop.go#L324)），工具的 `CheckToolPolicy`/`BeforeTool`/`execTools` 在其后（[agent/loop.go:418](../../agent/loop.go#L418)、[agent/loop.go:442](../../agent/loop.go#L442)） | 本步成立；但更早步骤的工具副作用已经存在，恢复不能撤销它们 |
| 可以复用同一条压缩路径 | `Compaction`/`Window` 已有摘要、单元分组、配对修复和校准 | 成立；但请求模式默认不落盘，恢复路径必须自己把结果交给循环持久化 |
| 基于 `StepView` 重建请求 | `StepView` 已能提供 `System`/`Options`/`Messages`/`Tools`：[agent/stepview.go:122](../../agent/stepview.go#L122)、[agent/stepview.go:131](../../agent/stepview.go#L131)、[agent/stepview.go:140](../../agent/stepview.go#L140)、[agent/stepview.go:148](../../agent/stepview.go#L148) | 能力成立，但循环目前没有这样用过 |
| 压缩结果写进检查点并发事件 | 现有 `HistoryCompacted`/`WindowTrimmed` 只记录压缩者自己的动作 | 需要新增「重试」事件；中途检查点是新动作 |
| 第二次失败返回类型化错误 | `fail` 会把错误原样放进 `RunFailed` | 类型化错误可以透传；跨进程序列化只保留字符串 |

### 3.5 现有测试为什么证明不了新行为

- `llm/error_test.go` 只覆盖状态码可重试性和固定错误串，没有任何「响应体 → 超限类型」用例。
- `RetryModel` 的测试只覆盖网络型失败：[middleware/middleware_test.go:93](../../middleware/middleware_test.go#L93)、[middleware/middleware_test.go:110](../../middleware/middleware_test.go#L110)。
- 压缩/窗口测试只直接调用 `ModifyRequest`/`CompactHistory`，没有经过循环的模型调用失败路径：[middleware/compaction_test.go:27](../../middleware/compaction_test.go#L27)、[middleware/window_test.go:77](../../middleware/window_test.go#L77)、[middleware/window_test.go:209](../../middleware/window_test.go#L209)。
- `agent/failure_test.go` 有可复用的脚本模型，能按调用次序注入错误并记录每次请求的 `Messages`：[agent/failure_test.go:23](../../agent/failure_test.go#L23)、[agent/failure_test.go:46](../../agent/failure_test.go#L46)。P0-4 的集成测试应沿用它，而不是新建一套假模型。

## 4. 设计原则

1. **错误先分类，恢复后行动。** 只有类型化超限才进入恢复；字符串永远不参与循环决策。
2. **没输出才重试。** 已经发布过任何增量或最终消息，就视为这次调用已经「说过话」，重试会重复输出。
3. **重试留在原步骤。** 不重开 turn、不重置工具视图、不重新执行准备阶段；恢复是 `CallModel` 阶段内部的一次重入。
4. **请求只重建，不重写。** `BeforeModel`/`ModifyRequest` 只跑一次；重试沿用冻结视图里的 `System`/`Tools`/`Options`，只替换 `Messages`。
5. **先落盘再重试。** 恢复后的历史必须在第二次调用前成为最新检查点；事件在这之后发布，语义是「已经压缩并提交，马上重试」。
6. **一个恢复者生效。** 多个恢复者按注册顺序询问，第一个返回严格更短历史的生效，避免同一轮里摘要加淘汰叠在一起。
7. **恢复失败不掩盖原错误。** 没有恢复者、历史没变短、摘要失败、检查点失败，都返回最初的超限错误；后两类用 `errors.Join` 带上第二个错误。
8. **默认一次，显式关闭。** 默认 1 次额外重试；`0` 关闭；不引入隐式递归。
9. **不改存储。** 只改 `State.Messages` 的内容，不新增字段、不改 `StepSnapshot`、不改检查点 JSON。

## 5. 核心设计

### 5.1 术语

| 术语 | 含义 |
|---|---|
| 超限错误 | `*llm.ContextLengthError`，由提供方响应分类得到 |
| 恢复者 | 实现 `agent.ContextOverflowRecoverer` 的中间件 |
| 恢复轮次 | 本步骤内已经发生的「压缩 + 重试」次数；`WithContextOverflowRetry(n)` 是上限 |
| 冻结视图 | `StepView`，本步请求定稿后不可变的工具/设置/消息快照 |
| 中途检查点 | 在原步骤号上额外写的一次检查点，状态是压缩后的历史，`Release:false` |
| 严格更短 | 恢复后的消息条数严格少于输入；等长或更长都视为「没有恢复」 |

### 5.2 错误类型与分类

新增类型放在 `llm` 包，和 `StatusError` 同文件：

```go
// ContextLengthError is a provider-reported context-window overflow.
// It embeds the original status error so existing classification still works,
// and adds the two numbers the provider disclosed (0 means unknown).
type ContextLengthError struct {
    *StatusError
    Limit     int // provider-reported maximum context tokens
    Requested int // provider-reported request size in tokens
}

func (e *ContextLengthError) Unwrap() error { return e.StatusError }

// Retryable is always false: the same request will fail identically.
func (e *ContextLengthError) Retryable() bool { return false }
```

要点：

- **不重写 `Error()`。** 直接继承 `StatusError.Error()`，日志里仍是 `<provider>: status <code>: <body>`，和分类前完全一致。
- **必须实现 `Unwrap`。** Go 的嵌入只提升方法，不会让 `errors.As(err, &se)` 自动找到匿名嵌入的 `*StatusError`；显式 `Unwrap` 让旧的 `errors.As`/`errors.Is` 继续工作。
- **覆盖 `Retryable()`。** 即便某个网关用 429/5xx 表达超限，直接调用该方法的重试器也会得到 false。

新增分类函数。名字保留 `NewStatusError`，因为它构造的仍是状态错误；只有能识别的超限才升级为子类型：

```go
// NewStatusError classifies one non-2xx provider response. It returns a
// *ContextLengthError when the status and body identify a context overflow,
// otherwise a plain *StatusError.
func NewStatusError(provider string, code int, body string) error {
    se := &StatusError{Provider: provider, Code: code, Body: body}
    if code != 400 && code != 413 {
        return se
    }
    limit, requested, matched := parseContextOverflow(body)
    if !matched {
        return se
    }
    return &ContextLengthError{StatusError: se, Limit: limit, Requested: requested}
}

func IsContextLength(err error) bool {
    var cle *ContextLengthError
    return errors.As(err, &cle)
}
```

`parseContextOverflow` 只做**尽力而为**的解析：命中签名但没有数字时，仍返回匹配（`Limit`/`Requested` 为 0），因为「知道是超限」比「知道具体数字」更重要。

只对 400/413 分类。401/403/404 即使响应体里偶然出现 `context` 字样，也不会被误判；500 同样不进入这个分支。`Agnes` 的图片/视频接口不经过这里。

`IsRetryable` 增加一条前置判断：

```go
func IsRetryable(err error) bool {
    if err == nil {
        return false
    }
    if IsContextLength(err) {
        return false
    }
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
        return false
    }
    var se *StatusError
    if errors.As(err, &se) {
        return se.Retryable()
    }
    return true
}
```

两个聊天提供方把裸构造换成分类器：

```go
yield(nil, llm.NewStatusError("openaicompat", resp.StatusCode, string(data)))
yield(nil, llm.NewStatusError("anthropic", resp.StatusCode, string(data)))
```

#### 5.2.1 响应体签名

解析器按「标记 + 数字模式」两级工作。标记决定是不是超限，数字模式只负责填 `Limit`/`Requested`：

| 来源 | 状态 | 判据标记（大小写不敏感） | 数字模式 |
|---|---|---|---|
| OpenAI | 400 | `context_length_exceeded`、`maximum context length` | `maximum context length is <N> tokens` → `Limit`；`resulted in <M> tokens` 或 `requested <M> tokens` → `Requested` |
| Anthropic | 400 | `prompt is too long`、`exceed context limit` | `prompt is too long: <M> tokens > <N> maximum` → `Requested=M, Limit=N`；`exceed context limit: <A> + <B> > <N>` → `Requested=A+B, Limit=N` |
| 通用兼容层 | 400/413 | `too many tokens`、`input is too long`、`context window exceeded`、`request too large` | 能解析到数字就填，否则留 0 |

这是设计表，不是实测日志；实现时每条标记都要有表驱动用例，不要求整段 JSON 完全一致。响应体之外的字段（例如提供方私有 header）不读取。

#### 5.2.2 兼容性

- 原来的 `errors.As(err, &se)` 仍能拿到 `*StatusError`，因为 `Unwrap` 返回嵌入值；`errors.As(err, &cle)` 能拿到 `*ContextLengthError`。
- 直接做 `err.(*StatusError)` 类型断言（不带 `errors.As`）的调用方会在超限时失败。这种写法本来就无法穿透任何包装，不在兼容承诺内；文档需要在 `StatusError` 注释里点明「新代码用 `errors.As`」。
- `Error()` 字符串不变，日志/指标上的字符串匹配不受影响。
- 新增 `Retryable()==false` 只影响「此前会被 429/5xx 判为可重试」的超限响应；400 超限本来也是 false。

### 5.3 恢复能力接口

放在 `agent/middleware.go`，与 `HistoryCompacter` 并列：

```go
// ContextOverflowRecoverer is an optional middleware capability: shrink the
// working history after the provider rejected a model call for exceeding its
// context window. The returned history must be provider-valid (no orphan tool
// result), non-empty, and strictly shorter than the input; ok=false means
// "I cannot help".
type ContextOverflowRecoverer interface {
    RecoverContext(lc *LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool)
}
```

`Stack` 按注册顺序折叠，**第一个严格更短的结果生效**：

```go
func (s *Stack) RecoverContext(lc *LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    for _, m := range s.mws {
        r, ok := m.(ContextOverflowRecoverer)
        if !ok {
            continue
        }
        out, changed := r.RecoverContext(lc, history, overflow)
        if changed && len(out) > 0 && len(out) < len(history) {
            return out, true
        }
    }
    return history, false
}
```

设计取舍：

- **严格更短由 `Stack` 统一兜底**，不信任单个恢复者的 `changed`。恢复者可以返回 `(history, true)` 表示「我改了但没变短」，`Stack` 会忽略它并继续问下一个。空历史也不算恢复：返回 nil 或空切片会被拒绝，避免重试时丢掉整段任务上下文。
- **第一个生效**而不是依次串行压缩：多个恢复者串行会让同一轮里叠加摘要和淘汰，成本和延迟不可控，而且第二个恢复者的输入已经不是模型刚才看到的历史。注册顺序就是优先级；建议把 `Compaction` 放在 `Window` 前面，优先保留「摘要」而不是「丢弃」。
- **不校验消息序列合法性**：`agent` 不能反向导入 `middleware`（会形成循环），`repairPairing` 在 `middleware` 包里。恢复者负责返回模型可接受的历史；内置实现复用现有的 `safeCut`/`groupUnits`/`repairPairing`。
- **请求模式也实现恢复**：`Compaction`/`Window` 即使配置为请求模式，也实现 `RecoverContext`。恢复结果由循环持久化，这是对 `Persist:false` 的一次性覆盖，理由见 5.9。

### 5.4 Compaction 的恢复算法

正常压缩是「超过阈值才压缩，保留 `KeepRecent` 条」。恢复路径不满足于这个策略：请求已经超限，说明正常路径要么没触发、要么触发后仍然太大。因此恢复路径**强制压缩，并把目标降到当前估计的一半**。

目标计算：

```go
// before is the calibrated estimate of the working history.
// Limit may be 0 (provider did not disclose it).
func compactionRecoveryTarget(before, limit int) int {
    if limit > 0 {
        return min(limit*3/4, before/2)
    }
    return before / 2
}
```

- 已知 `Limit` 时取「上限的 3/4」和「当前估计的一半」的较小值。3/4 给系统提示和工具 schema 留出余量；再与 half 取小，保证恢复一定会让估算值下降。
- 未知 `Limit` 时直接减半。这时无法保证重试一定能过，只能保证「比刚才小」；第二次失败由循环返回类型化错误。
- `before <= 1` 时没有恢复空间，返回 `false`。

裁剪点的选择：

1. 从 `keep = len(history)-1` 开始递减，用现有的 `safeCut`（[middleware/compaction.go:146](../../middleware/compaction.go#L146)）找到第一个合法边界，使被保留后缀的校准估算不超过 `target/2`。
2. 找不到（例如最近一条消息本身就巨大）就退到 `safeCut(history, 1)`：只保留最后一条，把其余全部摘要。仍不合法（`cut <= 0`）则放弃。
3. 对前缀调用现有的 `summarize`（[middleware/compaction.go:179](../../middleware/compaction.go#L179)），生成一条 `RoleUser` 的摘要消息，拼成 `[summary] + 后缀`。
4. 计算恢复后的校准估算，只有**严格小于**恢复前才返回成功。

裁剪点本身可以写成一个纯辅助函数，便于单测：

```go
func forceCut(c *compaction, lc *agent.LoopContext, msgs []core.Message, target int) int {
    for keep := len(msgs) - 1; keep >= 1; keep-- {
        cut := safeCut(msgs, keep)
        if cut > 0 && c.calibratedEstimate(lc, msgs[cut:]) <= target/2 {
            return cut
        }
    }
    return safeCut(msgs, 1) // 最激进但仍合法的边界；0 表示无法恢复
}
```

```go
func (c *compaction) RecoverContext(lc *agent.LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    if c.model == nil || len(history) <= 1 {
        return history, false
    }
    before := c.calibratedEstimate(lc, history)
    target := compactionRecoveryTarget(before, overflow.Limit)
    if target <= 0 || target >= before {
        return history, false
    }
    cut := forceCut(c, lc, history, target)
    if cut <= 0 {
        return history, false
    }
    summary, err := c.summarize(lc.Context, history[:cut])
    if err != nil || summary == "" {
        return history, false
    }
    note := core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: summaryMark + summary}}}
    out := append([]core.Message{note}, history[cut:]...)
    if len(out) >= len(history) || c.calibratedEstimate(lc, out) >= before {
        return history, false
    }
    publishCompacted(lc, before, cut, len(out)-1)
    return out, true
}
```

长度检查必须在发布事件之前：`cut=1` 时 `[摘要] + 后缀` 与输入等长，`Stack` 会按「严格更短」丢弃它；如果恢复者已经发布了 `HistoryCompacted`，订阅方就会看到一次没有生效的压缩。

事件沿用 `HistoryCompacted`：`Dropped=cut`、`Kept=len(out)-1`、`EstTokens=before`、`Step=lc.Step`。这与现有持久模式发布的事件字段完全一致（[middleware/compaction.go:115](../../middleware/compaction.go#L115)、[core/event.go:126](../../core/event.go#L126)），订阅方不需要新字段就能看到「压缩了多少」。实现时把 115 行的内联发布抽成私有 `publishCompacted(lc, before, cut, kept)`，正常持久压缩和恢复路径共用，避免两处字段口径漂移；请求模式的恢复也会发布它（正常的请求模式压缩不会）。

注意：恢复路径**不再检查 `MaxTokens` 阈值**，也不使用原来的 `KeepRecent` 条数。阈值和条数都是正常路径的守门员；请求已经超限，继续守门没有意义。恢复根据 `target` 重新选择要保留的后缀，因此可能比 `KeepRecent` 保留得更少。

**与请求模式的先后关系。** 恢复的输入是工作历史 `history`，不是第一次请求里经过 `ModifyRequest` 改写后的消息。如果 `Compaction`/`Window` 配置为请求模式并在第一次调用前已经压缩过，工作历史会比实际发出的请求长；恢复仍按工作历史减半，因此重试请求只保证比工作历史短，不保证比刚才失败的请求短。最坏情况下重试仍会超限，第二次失败返回类型化错误。希望恢复基于「模型实际看到的消息」的部署应使用持久模式（正常步骤已经把历史改短），或实现自己的恢复者，从 `lc.Request.Messages` 出发压缩；循环会把返回值持久化，两种方式都能工作。

**摘要调用自身的窗口。** `summarize` 会把整个前缀拼成一条用户消息（[middleware/compaction.go:179](../../middleware/compaction.go#L179)）。恢复目标把保留后缀压到 `target/2` 以内，而 `target` 最多是原估计的一半，因此前缀可能仍占原估计的四分之三以上；如果这次溢出是因为整段历史远超模型窗口，摘要请求本身也可能超限。这种情况下 `summarize` 失败，`RecoverContext` 返回 false。若同时安装了 `Window`，`Stack` 会继续询问它并改用尾部淘汰，因此推荐两者一起安装（`Compaction` 在前、`Window` 兜底）；只安装 `Compaction` 的部署在极端超限时仍可能无法恢复，这是 best-effort 的边界。

### 5.5 Window 的恢复算法

窗口的恢复比摘要更保守：不做摘要（没有额外模型调用），只做一次更紧的尾部淘汰。

```go
func (w *window) RecoverContext(lc *agent.LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    if w.strategy == nil || len(history) <= 1 {
        return history, false
    }
    before := w.estimate(lc, history)
    target := windowRecoveryTarget(before, overflow.Limit) // min(before/2, limit/2)
    if target <= 0 || target >= before {
        return history, false
    }
    out := keepTailWithin(lc, w, history, target)
    out = repairPairing(out)
    after := w.estimate(lc, out)
    if len(out) == 0 || len(out) >= len(history) || after >= before {
        return history, false
    }
    w.publishTrimmedAs(lc, "overflow_tail", history, out, before)
    return out, true
}

// estimate is the one calibrated measure both apply and recovery use; apply's
// count closure delegates here so the two cannot drift apart.
func (w *window) estimate(lc *agent.LoopContext, msgs []core.Message) int {
    return int(float64(w.counter(msgs)) * loadCalibDefaultAt(lc, kvWindow))
}

// keepTailWithin keeps the newest unit unconditionally, then walks backwards
// adding whole units while they fit target. Returning msgs means "nothing to
// drop"; the caller still compares lengths and refuses an unchanged result.
func keepTailWithin(lc *agent.LoopContext, w *window, msgs []core.Message, target int) []core.Message {
    units := groupUnits(msgs)
    used, start := 0, len(units)
    for i := len(units) - 1; i >= 0; i-- {
        size := w.estimate(lc, msgs[units[i].start:units[i].end])
        if i != len(units)-1 && used+size > target {
            break
        }
        start = i
        used += size
    }
    if start <= 0 {
        return msgs
    }
    return keptMessages(msgs, units[start:])
}
```

规则：

- **整单元淘汰。** 复用 `groupUnits`（[middleware/window.go:43](../../middleware/window.go#L43)）和 `keptMessages`（[middleware/window.go:94](../../middleware/window.go#L94)）；最新的一个单元无条件保留，和 `SlidingWindow` 的规则一致（[middleware/window.go:353](../../middleware/window.go#L353)）。如果最新单元自己就超过目标，它之前的单元仍会被丢掉；如果它已经是全部历史，就没有更短的结果可返回，恢复放弃。
- **目标更紧。** `target = min(before/2, limit/2)`（`Limit>0` 时）；未知上限时 `before/2`：

  ```go
  func windowRecoveryTarget(before, limit int) int {
      if limit > 0 {
          return min(limit/2, before/2)
      }
      return before / 2
  }
  ```

- **不调用配置的策略。** `RecentN`/`SlidingWindow`/`ImportanceWeighted` 各自有自己的预算或条数，恢复需要的是一个统一的紧急目标。策略仍然在正常步骤里生效；恢复只借用它的单元和配对规则。事件里的 `Strategy` 用新值 `"overflow_tail"`，与三个正常策略区分。
- **配对修复后再比较。** `repairPairing` 可能把开头孤立的结果单元去掉，导致实际保留更少；因此用修复后的结果计算 `after` 和 `len(out)`。
- **空结果不算恢复。** 如果保留下来的消息全是孤立工具结果，`repairPairing` 会把它们全部删掉；返回空历史会让下一次调用失去全部对话上下文，因此 `len(out)==0` 直接放弃，不发布事件。
- **事件复用现有发布逻辑。** 现有 `publishTrimmed` 在 256 行把 `Strategy` 写死为 `w.strategy.Name()`（[middleware/window.go:256](../../middleware/window.go#L256)）；恢复需要一个不同的取值，因此把发布体抽成 `publishTrimmedAs(lc, strategy, in, out, est)`，`publishTrimmed` 用自己的策略名调用它，恢复用 `"overflow_tail"` 调用。`Dropped`/`Kept` 仍按消息数、在配对修复后统计。

`WindowTrimmed` 的注释需要同步：`Strategy` 从三个值扩展为四个，新增 `"overflow_tail"`（[core/event.go:141](../../core/event.go#L141)、[middleware/window.go:256](../../middleware/window.go#L256)）。这是字符串取值扩展，旧的订阅方不受影响。

### 5.6 Agent 选项

```go
// WithContextOverflowRetry bounds how many recovery rounds one step may run
// after a typed context-length overflow. The default is 1; 0 disables recovery.
// Values above 1 allow that many compression rounds in the same step; each round
// must shrink the history further. Negative values are treated as 0.
func WithContextOverflowRetry(n int) Option
```

- 默认值在 `agent.New` 的 `config` 字面量里种下：`contextOverflowRetry: 1`（[agent/agent.go:38](../../agent/agent.go#L38)）。不能用「零值即默认」，因为 0 是合法的「关闭」。
- `newLoop` 把 `c.contextOverflowRetry` 复制到 `AgentLoop`（[agent/loop.go:50](../../agent/loop.go#L50)）。
- 计数单位是**每个步骤**，不是每次运行：第 3 步超限可以恢复一次，第 7 步再超限还可以恢复一次。`MaxTurns` 仍然是步骤上限，恢复不改变它。
- `n=1` 表示「第一次调用 + 最多一次重试」，总共最多两次提供方聊天调用（不含 `Compaction` 恢复时额外的一次摘要调用）；`n=2` 表示最多三次。默认只允许一次，避免恢复轮次无限递归。
- `n=0` 时仍然会分类错误（调用方可以用 `errors.As` 识别），但不会调用恢复者，也不会写中途检查点。

### 5.7 循环重试状态机

重试发生的位置在第一次 `streamModel` 返回错误之后、`AfterModel` 之前（[agent/loop.go:324](../../agent/loop.go#L324)）。`streamModel` 需要告诉调用方「这次调用有没有发布过任何响应」，因此签名增加一个布尔值：

```go
func (l *AgentLoop) streamModel(...) (final *llm.Response, ok bool, emitted bool, err error)
```

- 第二个返回值 `ok` 是现有语义：出错为 false，成功为 true（[agent/loop.go:479](../../agent/loop.go#L479)）。
- 新增的 `emitted` 在发布任何 `Partial` 或最终 `MessageDone` 时置 true（[agent/loop.go:489](../../agent/loop.go#L489)、[agent/loop.go:493](../../agent/loop.go#L493)）。
- 出错返回时 `ok=false`，`emitted` 保留真实值。
- 成功时 `ok=true`，`err=nil`；正常提供方至少发布一次 `MessageDone`，因此 `emitted` 为 true。模型一个响应都不给属于违反现有约定，循环仍按空回复成功处理（与今天一致）。

循环内的状态机。片段位于 `runStep` 的 `view` 与 `history` 作用域内，省略了成功后的 `final := finalResp.Message` 与正常步骤收尾：

```go
attempts := 0
for {
    finalResp, _, emitted, err := l.streamModel(genCtx, rc, lc, req)
    if err == nil {
        break
    }
    // 1) 类型化超限是进入恢复的前提；其余错误原样失败。
    cle, isOverflow := asContextLength(err)
    // 2) 上下文已经取消/超时、已经输出过、或预算用尽，都不重试，
    //    也不该花一次摘要调用。
    if isOverflow && genCtx.Err() == nil && !emitted && attempts < l.contextOverflowRetry {
        before := len(history)
        recovered, changed := l.mw.RecoverContext(lc, history, cle)
        if changed {
            // 3) 基于旧视图重建请求，只替换 Messages
            retryReq := &llm.Request{
                System:   view.System(),
                Messages: recovered,
                Tools:    view.Tools().Schemas(),
                Options:  view.Options(),
            }
            // 4) 先冻结新视图，验证工具表仍然一致
            retryView, ferr := l.freezeStepView(lc, retryReq)
            if ferr != nil {
                return l.fail(rc, step, history, lc.viewSnapshot(), errors.Join(err, ferr)), st, false
            }
            // 5) 更新工作状态，提交中途检查点
            history = recovered
            lc.History = history
            lc.Request = retryReq
            rc.State.Messages = history
            if cerr := l.checkpoint(rc, commitOptions{Step: step, Snapshot: retryView.snapshot()}); cerr != nil {
                // 中途检查点失败也不重试；交给 fail 再尝试一次 Release:true
                // 的收尾检查点，并把两个错误都保留下来。
                return l.fail(rc, step, history, retryView.snapshot(), errors.Join(err, cerr)), st, false
            }
            // 6) 新视图生效，发事件，换上下文，重试
            view = retryView
            lc.view = view
            attempts++
            rc.publish(core.ContextOverflowRetried{
                Step: step, Attempt: attempts,
                Model: view.Model(), Provider: cle.Provider,
                Limit: cle.Limit, Requested: cle.Requested,
                MessagesBefore: before,
                MessagesAfter:  len(history),
            })
            genCtx = l.mw.ModelContext(lc, rc.Context)
            continue
        }
    }
    return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
}
```

第二个返回值 `ok` 在错误路径上恒为 false，片段里只用 `emitted` 和 `err`；成功路径在 `err == nil` 时直接跳出。`asContextLength` 是对 `errors.As` 的一层薄封装，便于避免每个调用点重复声明 `var cle *llm.ContextLengthError`。

判定顺序是刻意的：

1. **先看是不是类型化超限，且调用上下文仍有效**（最便宜，且决定后续是否值得做）。`genCtx` 已经取消或超时时，即使错误对象里同时带超限也不恢复。
2. **再看 `emitted`**：已经输出过就不允许重试，哪怕错误类型匹配、恢复者可用。
3. **再看重试预算**：已经用尽时不做无用的摘要。
4. **最后才调用恢复者**：摘要可能是一次真实的模型调用，不能在注定不重试的路径上花这笔钱。

恢复后的顺序也是刻意的：

1. **重建请求**：用旧视图的 `System`/`Tools`/`Options`，只把 `Messages` 换成恢复后的历史。
2. **重新冻结视图**：验证恢复后的请求仍然只广告同一批工具、schema 仍与实现一致。冻结失败说明运行期工具表发生了变化，此时不重试，返回 `errors.Join(超限, 冻结错误)`。
3. **写中途检查点**：`Release:false`，步骤号不变，快照用新视图。检查点失败就不重试，转交 `fail`；`fail` 会再尝试一次 `Release:true` 的收尾检查点，并把两个错误用 `errors.Join` 合并。没有发布 `ContextOverflowRetried`，因为重试并没有发生。
4. **发布事件、换 `ModelContext`、重试**：`ModelContext` 重新推导一次是刻意的——它是为「一次提供方调用」注入 span 的钩子（[agent/middleware.go:215](../../agent/middleware.go#L215)），第二次调用应该有自己的一次；`BeforeModel`/`ModifyRequest` 则不重跑。

重试时刻意**不**做的事：

- 不发布 `TurnStarted`/`TurnDone`。它们由 `runStep` 的边界发布（[agent/loop.go:254](../../agent/loop.go#L254)、[agent/loop.go:457](../../agent/loop.go#L457)），恢复是 `CallModel` 阶段内部的重入。
- 不调用 `checkReplayDrift`。它只用于「恢复运行后的第一次冻结」，而且会在第一次冻结时消费掉基线（[agent/stepview.go:437](../../agent/stepview.go#L437)）；第二次冻结再调用是空操作，但显式跳过可以避免未来语义变化时误触。
- 不重新 drain steering/inbox，不重跑 `BeforeModel`，不重新渲染系统提示，不重新 `Apply` 模型选项。
- 不执行任何工具。第一次调用失败时，本步的 `CheckToolPolicy`/`BeforeTool`/`execTools` 都还没到（[agent/loop.go:418](../../agent/loop.go#L418)、[agent/loop.go:442](../../agent/loop.go#L442)）。

第二次失败：

- 直接把第二次的 `*ContextLengthError` 交给 `fail`；不保留第一次的错误对象。调用方看到的是导致最终失败的那次请求的上限/请求量，更有诊断价值。
- 如果第二次失败的不是超限错误（例如压缩后的历史触发了别的 4xx），返回第二次的错误；恢复已经发生，真正失败的是新请求。
- `fail` 会把当前历史（如果恢复过，就是压缩后的历史）设为 `seam(history)` 并写 `Release:true` 的检查点（[agent/loop.go:572](../../agent/loop.go#L572)、[agent/loop.go:573](../../agent/loop.go#L573)）。因此「压缩过、但第二次仍失败」的运行，线程头部的状态也是压缩后的。

### 5.8 为什么从 StepView 重建，而不是重跑 ModifyRequest

在同一个 `llm.Request` 上重跑 `ModifyRequest` 不是幂等的。仓库里至少有三处证据：

1. **RAG** 把检索块追加到 `req.System`：[middleware/rag.go:45](../../middleware/rag.go#L45)、[middleware/rag.go:61](../../middleware/rag.go#L61)。重跑会把同一段检索内容拼接两次。
2. **RunBudget 收尾模式** 把 `budgetInstr` 追加到 `req.System` 并清空工具：[middleware/runbudget.go:181](../../middleware/runbudget.go#L181)、[middleware/runbudget.go:182](../../middleware/runbudget.go#L182)。重跑会重复追加指令，而且如果清空逻辑被改变，工具广告可能不一致。
3. **`LoopContext.AddTool`** 在视图冻结前会同时写运行期注入表和当前请求：[agent/loopctx.go:64](../../agent/loopctx.go#L64)。它不是「只改请求」的纯函数：重跑 `ModifyRequest` 会再次执行注入逻辑；如果该逻辑依赖外部状态或条件，重试请求的工具表就可能与第一次不同，`freezeStepView` 也会因此失败。

因此重试请求的三个「设置面」直接取自冻结视图：

```go
retryReq := &llm.Request{
    System:   view.System(),          // 已经包含 RAG/收尾指令等所有改写
    Messages: recovered,              // 唯一变化的部分
    Tools:    view.Tools().Schemas(), // 已经包含 AddTool 注入
    Options:  view.Options(),         // 已经 Apply 过 modelOpts + ModifyRequest 的改动
}
```

`view.Options()` 返回 `Stop` 的副本，`Metadata` 仍是共享只读 map（[agent/stepview.go:131](../../agent/stepview.go#L131)、[llm/options.go:6](../../llm/options.go#L6)）；这与 `StepView` 的既有约定一致（[agent/stepview.go:61](../../agent/stepview.go#L61)），重试不修改它。

代价与边界：**请求级、会改写消息的中间件不会在重试时重跑**。例如一个自定义中间件在 `ModifyRequest` 里对消息做脱敏，恢复后的历史来自工作历史，不会自动再脱敏一次。这是有意的取舍——重跑无法区分「幂等改写」和「追加型改写」，而追加型改写会造成重复内容。需要这类改写参与恢复的用户，应实现自己的 `ContextOverflowRecoverer`，在返回历史前完成改写。

### 5.9 中途检查点与恢复语义

恢复后的写入是「同一步骤的第二个检查点」：

```go
l.checkpoint(rc, commitOptions{
    Step:     step,           // 不变
    Snapshot: retryView.snapshot(),
    Release:  false,          // 运行还在继续，保留租约
})
```

`checkpoint` 的提交路径在 [agent/loop.go:514](../../agent/loop.go#L514)、[agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339)。`Release:false` 是有意的：运行没有结束，durable 运行继续持有线程租约；步骤结束时的检查点仍按原来的指令决定 `Release`（[agent/loop.go:454](../../agent/loop.go#L454)）。

崩溃窗口：

| 崩溃位置 | 线程最新检查点 | 恢复后的行为 |
|---|---|---|
| 恢复者返回前 | 上一成功步骤 | 重放本步，重新发原请求；如果再次超限，再走一次恢复 |
| 恢复后、检查点提交前 | 上一成功步骤 | 同上；不会出现「内存已压缩、磁盘没压缩」的中间态 |
| 检查点提交后、重试调用前 | 本步的中途检查点（压缩后历史，无 `Pause`） | `Resume` 直接以压缩后的 `State.Messages` 为起点，重新问一次模型 |
| 重试调用中 | 中途检查点 | 重试的输出没有落盘；`Resume` 从压缩历史重新问 |
| 重试成功、步骤检查点前 | 中途检查点 | 重试的回复没有落盘；`Resume` 重新问。这是「重放模型调用」的既有语义，不是新问题 |
| 重试成功、步骤检查点后 | 步骤检查点（完整历史） | 正常继续 |

`Resume` 读取 `Latest` 后把状态复制出来（[agent/hitl.go:79](../../agent/hitl.go#L79)、[agent/hitl.go:128](../../agent/hitl.go#L128)）。中途检查点没有 `Pending`，所以不会走暂停恢复分支；`validatePause` 对 `cp.Pause == nil` 直接放行（[agent/hitl.go:210](../../agent/hitl.go#L210)），`cp.Snapshot` 会在快照策略未关闭时成为下次运行的 replay 基线（[agent/hitl.go:189](../../agent/hitl.go#L189)）。由于 `StepSnapshot` 只记录工具/设置摘要，不记录消息（[checkpoint/checkpoint.go:36](../../checkpoint/checkpoint.go#L36)），压缩历史不会触发 `StepViewDrifted`；下次冻结的视图只要工具和设置没变，摘要就一致。

另外，非 pending 的 `Resume` 本身从新运行的 step 0 重新计数（[agent/loop.go:115](../../agent/loop.go#L115)），所以中途检查点之后的恢复运行不会接着使用原来的步骤号；这是既有的 Resume 语义，P0-4 没有改变它。replay 漂移比较只看工具/设置摘要，不看步骤号。

一个需要接受的副作用：中途检查点会让外部观察者（例如 `Session.Head` 或运行状态查询）看到「同一步骤号的第二个快照」。这是崩溃恢复的代价，也是 P0-4 的核心要求（压缩必须落盘）。检查点 ID 每次新生成（[agent/loop.go:522](../../agent/loop.go#L522)），不存在同 ID 覆盖。

### 5.10 事件

新增事件，放在 `HistoryCompacted`/`WindowTrimmed` 之后：

```go
// ContextOverflowRetried is emitted after the loop has compressed the working
// history for a provider-reported context overflow, committed it to a
// checkpoint, and is about to issue the retry. Attempt is 1-based and counts
// recovery rounds within this step; MessagesBefore/After are message counts,
// not tokens.
type ContextOverflowRetried struct {
    Step           int
    Attempt        int
    Model          string
    Provider       string
    Limit          int
    Requested      int
    MessagesBefore int
    MessagesAfter  int
}
```

- 只在**真正发起重试**时发布：恢复者没返回更短历史、检查点失败、冻结失败都不发。
- 发布顺序是「中途检查点提交成功 → `ContextOverflowRetried` → 第二次模型调用」。订阅方收到事件时，`store.Latest` 已经是压缩后的历史；测试应利用这一点断言「先落盘、后重试」。
- `Provider`/`Limit`/`Requested` 来自 `*ContextLengthError`；`Model` 来自冻结视图的 `Options.Model`（可能为空，取决于用户是否显式设置）。这些字段未知时为 0/空串。
- 每个恢复轮次一条。`WithContextOverflowRetry(2)` 且连续两次超限时，会看到 `Attempt=1` 和 `Attempt=2` 两条。
- 恢复者自己的 `HistoryCompacted`/`WindowTrimmed` 先于本事件发布，因为它们发生在 `RecoverContext` 内部。
- 恢复者的压缩事件只表示「内存里已经改短」，不表示「已经落盘」：它在 `RecoverContext` 返回前发布，早于中途检查点。要判断落盘，看随后的 `ContextOverflowRetried`，它只在检查点提交成功后发布。

JSON 编码：`eventWire` 增加字段（[core/eventjson.go:18](../../core/eventjson.go#L18)），编解码分支加在现有事件之后（[core/eventjson.go:104](../../core/eventjson.go#L104)、[core/eventjson.go:168](../../core/eventjson.go#L168)）：

```go
Attempt        int    `json:"attempt,omitempty"`
Model          string `json:"model,omitempty"`
Provider       string `json:"provider,omitempty"`
Limit          int    `json:"limit,omitempty"`
Requested      int    `json:"requested,omitempty"`
MessagesBefore int    `json:"messages_before,omitempty"`
MessagesAfter  int    `json:"messages_after,omitempty"`
```

事件类型字符串用 `context_overflow_retried`。标记方法加到 `core/event.go` 的 marker 区（[core/event.go:180](../../core/event.go#L180)、[core/event.go:202](../../core/event.go#L202)），并在 `core/eventjson_test.go` 的往返表里加一项（[core/eventjson_test.go:8](../../core/eventjson_test.go#L8)）。

跨版本注意：旧的 `UnmarshalEvent` 遇到未知类型会返回 `unknown event type`（[core/eventjson.go:184](../../core/eventjson.go#L184)）。任何新增事件都有这个边界；使用 Redis/跨进程事件总线的部署需要同步升级消费者。事件字段本身用 `omitempty`，不影响旧事件的编码。

### 5.11 一次完整时序

```text
TurnStarted{step}
  [PrepareTurn]
  CompactHistory            // 正常路径，可能没有改动
  BeforeModel
  build req → ModifyRequest → freezeStepView → ModelContext
  model call #1
    provider: 400 context_length_exceeded
    OnError(err)
    no MessageDelta / MessageDone
  ← ContextLengthError
  RecoverContext(history)
    Compaction/Window 强制压缩
    publish HistoryCompacted / WindowTrimmed
  rebuild req from view (System/Tools/Options) + recovered Messages
  freezeStepView(retryReq)          // 同一步骤，工具/设置摘要不变
  State.Messages = recovered
  checkpoint{step, snapshot=retryView, Release:false}
  publish ContextOverflowRetried{Attempt:1, ...}
  ModelContext (新 span)
  model call #2
    MessageDone
  AfterModel                // 只对成功的第二次调用执行
  append final
  [ExecuteTools ...]        // 正常继续
  checkpoint{step, Release:...}
  TurnDone{step}
```

### 5.12 与模型装饰器的关系

- **`RetryModel`**：必须在退避前检查 `llm.IsContextLength`，命中就直接把错误交给调用方（[middleware/retry.go:64](../../middleware/retry.go#L64)）。否则它会在循环恢复之前把同一条超限请求重试到自己的 `MaxAttempts`，既浪费调用，又让循环只看到最后一次。其他 4xx 的既有行为（仍然重试）不在本次修改范围。
- **`FallbackModel`**：默认 `ShouldFallback=llm.IsRetryable`，超限现在为 false，因此不会自动换模型。想要「换一个窗口更大的模型」的用户可以传入自定义 `ShouldFallback`，显式检查 `llm.IsContextLength`；这是用户选择，不是默认行为。注意它发生在循环恢复之前：一旦 fallback 成功，循环根本看不到超限，也就不会压缩。
- **`CircuitBreaker`**：默认 `IsFailure=llm.IsRetryable`，超限不计为失败，不会打开熔断。自定义 `IsFailure` 如果仍按字符串或 `StatusError` 判定，需要自行检查 `ContextLengthError`。
- **`OnError`**：两次失败的调用各触发一次，和今天「每次模型错误观察一次」的语义一致。它仍然不能否决重试（返回值被忽略，[agent/loop.go:486](../../agent/loop.go#L486)）。
- **`AfterModel`**：只对成功的调用执行。第一次失败不校准；第二次成功时 `lc.Request` 是重试请求，校准因子会学习「压缩后的请求」的真实大小（[middleware/compaction.go:158](../../middleware/compaction.go#L158)、[middleware/window.go:278](../../middleware/window.go#L278)）。

### 5.13 取消、流式与写保护

- **取消/截止时间**不会进入恢复：它们本身不是 `ContextLengthError`，`IsRetryable` 也已经是 false；即使错误对象同时带超限和取消，循环里的 `genCtx.Err()` 检查也会拒绝恢复。
- **已经流式输出**不会重试：`emitted` 覆盖 `Partial` 和最终 `MessageDone` 两种情况。半条回复不修补，交给 `fail`/`Resume` 的既有语义。
- **写保护/租约**：`streamModel` 每次调用都先 `checkWriteAccess`（[agent/loop.go:480](../../agent/loop.go#L480)）。第二次调用会重新检查；如果租约在恢复期间过期，返回写保护错误，不重试。
- **工具**：恢复路径不接触工具，不启动 goroutine，不取消任何工具；本步工具尚未开始。
- **压缩本身**：`Compaction.summarize` 用 `lc.Context` 调用摘要模型（[middleware/compaction.go:179](../../middleware/compaction.go#L179)）。如果该上下文已经取消或摘要失败，恢复者返回 false，循环返回原超限错误；不会重试。

### 5.14 提供方差异

| 提供方 | 超限状态 | 识别方式 | 备注 |
|---|---|---|---|
| OpenAI 兼容 | 400 | 响应体 `context_length_exceeded` / `maximum context length` | 数字通常齐全，`Limit`/`Requested` 可填 |
| Anthropic | 400 | `prompt is too long` / `exceed context limit` | 数字通常是「请求量 > 上限」逆序 |
| 自建/代理 | 400 或 413 | 通用标记 | 只命中标记、没有数字时仍算超限，但恢复目标只能减半 |
| 其他非 200 | 401/403/404/5xx | 不进入分类 | 保持普通 `StatusError` |
| Agnes 图片/视频 | 非聊天接口 | 不经过 | 现有 `fmt.Errorf` 保持不变 |

`Limit` 为 0 不代表「没有上限」，只代表「提供方没有在响应体里告诉我们」。恢复算法把它当成未知处理，不做猜测。

## 6. 不变量

1. **类型保真**：`errors.As(err, &cle)` 在提供方包装、`RetryModel`、`errors.Join` 之后仍能取到 `*ContextLengthError`；`errors.As(err, &se)` 仍能取到 `*StatusError`。
2. **无输出才重试**：本次调用只要发布过任何 `MessageDelta` 或 `MessageDone`，就不重试；`MessageDelta` 在一次步骤内的序列不会因为重试而重复。
3. **同步骤**：恢复不改变 `st.step`，不发布额外的 `TurnStarted`/`TurnDone`，不消耗 `MaxTurns`、`RunBudget` 的 turn 计数。
4. **设置一致**：重试请求的 `System`、`Tools`、`Options` 与第一次请求逐字段一致；唯一的差异是 `Messages`。
5. **严格变短**：恢复后的消息条数必须严格小于输入；恢复者返回等长/更长时循环不重试。
6. **先落盘**：第二次模型调用前，`State.Messages` 已经是恢复后的历史，且最新检查点已包含它。
7. **租约不丢**：中途检查点 `Release:false`；运行结束/失败时的检查点仍按原有规则决定 `Release`。
8. **工具零执行**：失败调用所在步骤的 `CheckToolPolicy`、`BeforeTool`、`execTools` 均未运行；恢复不启动任何工具。
9. **状态面不变**：恢复只改 `State.Messages` 的内容；`State.KV`、`Todos`、`Files` 不被压缩逻辑改写。
10. **事件真实**：`ContextOverflowRetried` 只在检查点提交成功且即将重试时发布；失败路径不发布伪造的成功事件。
11. **错误不丢失**：检查点/冻结失败用 `errors.Join` 保留超限错误；第二次失败返回第二次的超限错误。
12. **无恢复者即旧循环行为**：没有实现 `ContextOverflowRecoverer` 的中间件时，循环的模型调用次数、检查点次数与今天一致；`RetryModel` 的额外重试除外（见不变量 13），错误类型仍是全局变化。
13. **`RetryModel` 不碰超限**：无论配置多少次尝试，它遇到 `ContextLengthError` 只调用一次底层模型。
14. **存储不变**：不新增 `State`/`Checkpoint` 字段；`StepSnapshot` 不记录消息，恢复后的历史不会触发摘要漂移。

## 7. 兼容性与影响

### 7.1 行为变化

1. **错误类型变化**：400/413 且命中超限签名的响应，从 `*StatusError` 变成 `*ContextLengthError`。`Error()` 字符串不变；`errors.As` 到 `*StatusError` 仍可用（通过 `Unwrap`）；直接类型断言会失败。
2. **`IsRetryable` 变化**：超限即使带 429/5xx 也返回 false。400 超限本来就是 false，实际变化只影响非 400 编码的超限。
3. **`RetryModel` 变化**：不再重试超限。之前它会把 400 超限重试到 `MaxAttempts`；现在只调用一次。
4. **循环调用次数**：安装恢复者且发生超限时，默认只多一次重试调用；若恢复者是 `Compaction`，还可能额外调用一次摘要模型。成本和延迟增加，换来运行不中断。
5. **请求模式压缩**：`Compaction`/`Window` 在 `Persist:false` 下平时不写 `State`；超限恢复时循环会把恢复结果写入状态。这是对 `Persist` 的一次性覆盖，必须写进两者的文档注释。
6. **检查点数量**：发生恢复时，同一步骤会多一个检查点。依赖「每个步骤恰好一个检查点」的外部工具需要知道这个变化；按 `Latest` 读取的恢复路径不受影响。
7. **事件流**：恢复时多两类事件（恢复者的压缩事件 + `ContextOverflowRetried`）。只按事件数量做断言的下游需要更新。

没有恢复者、没有超限时，以上 4–7 都不发生。

### 7.2 API 与存储

新增导出面：

- `llm.ContextLengthError`、`llm.NewStatusError`、`llm.IsContextLength`。
- `agent.ContextOverflowRecoverer`、`(*agent.Stack).RecoverContext`。
- `agent.WithContextOverflowRetry`。
- `core.ContextOverflowRetried`（以及事件标记）。

不变：

- `llm.Model`、`llm.Request`、`llm.Response`、`agent.Middleware`、`HistoryCompacter` 的签名。
- `State`/`Checkpoint`/`StepSnapshot` 的 JSON 结构。
- `RetryOptions`/`FallbackOptions`/`CircuitOptions` 的字段。
- `HistoryCompacted`/`WindowTrimmed` 的字段（`WindowTrimmed.Strategy` 只多一个取值）。

### 7.3 需要同步修改的注释和文档

- `agent/loop.go:486` 的注释「retry middleware consulted; real retry lands later」需要改成「循环在这里决定是否做上下文恢复重试」。
- `middleware/retry.go` 的 `RetryModel` 注释需要说明「上下文超限不重试」。
- `llm/error.go` 的 `StatusError` 注释需要指向 `ContextLengthError` 和 `errors.As` 用法。
- `middleware/compaction.go` 的 `Persist` 注释需要说明「超限恢复路径由循环持久化」。
- `middleware/window.go` 的 `Persist` 注释同上；`WindowTrimmed` 的 `Strategy` 注释加 `"overflow_tail"`。
- `core/event.go` 的 `HistoryCompacted` 注释需要说明「持久模式的常规压缩」和「超限恢复（含请求模式）」都会发布它；`WindowTrimmed` 注释加恢复来源 `"overflow_tail"`。
- 优化建议文档 P0-4 的 `agent/loop.go:202` 锚点应更新为 `281`，并注明本设计文档。
- P0-3 设计文档不需要改；两篇文档的边界是「工具调度」与「模型调用恢复」。

### 7.4 恢复与漂移

- 中途检查点没有 `Pending`/`Pause`，`Resume` 按普通检查点继续（[agent/hitl.go:189](../../agent/hitl.go#L189)）。
- `Resume` 会在快照策略开启时把 `cp.Snapshot` 设为 replay 基线；恢复只改消息，工具/设置摘要不变，因此不会产生 `StepViewDrifted`。
- 如果恢复后用户在 resumed run 里换了模型或改了工具，第一次冻结仍会按既有策略报告漂移；这与压缩无关。
- 暂停恢复的批次（`Pending`）不会发生模型调用，因此不会触发超限；批次执行完后的下一个模型调用如果超限，走本文的同步骤恢复，逻辑一致。

## 8. 备选方案

### 8.1 在 Agent 里内置一个压缩器，而不是中间件能力

- 需要把 `Compaction`/`Window` 的配置、摘要模型、校准因子复制到 `agent`，或者让 `agent` 依赖 `middleware`（会形成循环）。
- 用户已有的 `Compaction` 配置无法复用，等于两套上下文管理。
- 否决原因：现有 `HistoryCompacter` 已经把「改写工作历史」的入口放在中间件上，恢复只是同一入口的第二个触发点。

### 8.2 只重试、不压缩

- 同一条超大请求会以完全相同的方式再次失败，纯属浪费一次调用。
- 否决。

### 8.3 在同一个请求上重跑 `ModifyRequest` 后重试

- RAG 会重复拼接检索块（[middleware/rag.go:61](../../middleware/rag.go#L61)），RunBudget 会重复追加收尾指令（[middleware/runbudget.go:182](../../middleware/runbudget.go#L182)），`AddTool` 会再次执行注入逻辑、可能改变运行期工具表。
- 无法判断哪些 `ModifyRequest` 是幂等的。
- 否决原因：`StepView` 的存在就是为了固定「模型实际看到的东西」；重试只应改变历史。

### 8.4 把恢复做成模型装饰器（类似 `RetryModel`）

- 模型装饰器看不到 `State`、`StepView`、检查点，也发布不了业务事件。
- 它无法把压缩结果持久化，崩溃后会重放超限请求。
- 否决原因：恢复需要运行状态，必须是循环能力。

### 8.5 就地截断消息 / 砍掉工具结果

- 会留下没有对应调用的工具结果，提供方会直接拒绝；或者让模型看到被截断的 JSON/文本，产生更糟的幻觉。
- 否决原因：工具调用与结果的配对是硬约束，单元级淘汰和摘要已经处理了这一点。

### 8.6 自动缩短 `MaxTokens` 输出上限

- 输出上限不是输入超限的根因；把输出调小不能保证请求能过，反而可能触发 `StopMaxTokens` 截断。
- 否决原因：解决的是另一个问题。

### 8.7 自动 fallback 到窗口更大的模型

- 会改变模型身份、成本和输出风格；`StepView` 的 `Model` 摘要也会变化。
- 用户可以用自定义 `ShouldFallback` 显式选择，不应成为默认。
- 暂不实现。

### 8.8 多次压缩直到通过

- 需要循环层知道 token 数、维护目标，并且每次压缩都可能调用摘要模型；容易递归，成本不可控。
- 默认一次、可配多次已经覆盖大多数场景；再复杂的策略应由恢复者自己实现。
- 否决为默认行为。

## 9. 测试与验收

### 9.1 `llm` 分类单元测试

文件：`llm/error_test.go`（扩展）。

1. 表驱动：OpenAI 400 超限 → `*ContextLengthError`，`Limit`/`Requested` 正确；Anthropic 400 → 同上；通用 413 → 匹配；普通 400（参数错误）→ 普通 `*StatusError`。
2. 401/403/404/500 即使正文含 `context length` 也不分类。
3. `errors.As(err, &cle)` 与 `errors.As(err, &se)` 同时成立；`err.Error()` 与 `StatusError.Error()` 完全一致。
4. `Retryable()==false`；`IsRetryable(cle)==false`，包括手动构造 `Code:429` 的超限错误。
5. 命中标记但没有数字 → `Limit=0`、`Requested=0`，仍是超限类型。
6. 已有的 `TestStatusErrorRetryable`/`TestStatusErrorMessageStable`/`TestIsRetryable` 不回归（[llm/error_test.go:12](../../llm/error_test.go#L12)）。

### 9.2 `RetryModel` 测试

文件：`middleware/middleware_test.go`（扩展）。

1. 底层模型第一次返回 `*ContextLengthError`：`RetryModel` 只调用一次，错误原样返回（`errors.As` 可取）。
2. 普通网络错误仍然按 `MaxAttempts` 重试；既有用例 `TestRetryModelRecovers`/`TestRetryModelGivesUp` 保持（[middleware/middleware_test.go:93](../../middleware/middleware_test.go#L93)）。
3. 已开始流式输出后的超限错误不重试（与既有 `yielded` 规则一致）。

### 9.3 恢复者单元测试

`middleware` 包内测试，复用 `newLC`（[middleware/compaction_internal_test.go:61](../../middleware/compaction_internal_test.go#L61)）和窗口夹具（[middleware/window_internal_test.go:17](../../middleware/window_internal_test.go#L17)、[middleware/window_internal_test.go:27](../../middleware/window_internal_test.go#L27)、[middleware/window_internal_test.go:35](../../middleware/window_internal_test.go#L35)）。

1. **Compaction 强制压缩**：小于正常阈值、但 `Limit` 比估算小的历史，正常 `compact` 不会动，`RecoverContext` 会摘要并返回更短历史；`HistoryCompacted.Dropped/Kept/EstTokens` 正确。
2. **Compaction 摘要失败**：摘要模型返回错误/空串 → `(history,false)`，不发布事件。
3. **Compaction 无法再短**：只有一条消息，或 `safeCut` 找不到合法边界（例如整个历史只有一个保留单元）→ `(history,false)`。最近消息自身超限但前面还有可摘要内容时，恢复仍会返回 `[摘要, 最近消息]`；历史变短但第二次调用可能仍超限，这正是 9.4 第 3 条覆盖的路径。
4. **Compaction 请求模式也恢复**：`Persist:false` 时 `RecoverContext` 仍生效；正常 `CompactHistory` 仍不动。
5. **Window 强制尾部淘汰**：`RecentN`/`SlidingWindow`/`ImportanceWeighted` 三种配置下，恢复都使用 `overflow_tail` 目标；结果不含孤立工具结果（用 `winAsst`/`winTool` 构造）。
6. **Window 最新单元超限**：只有最新单元且自身超限 → 返回 false，不返回「空历史」。
7. **Stack 第一个生效**：注册两个恢复者，第一个返回等长/更长，第二个返回更短 → 第二个生效；第一个返回更短 → 第二个不被调用。
8. **严格变短兜底**：恢复者返回等长历史或空历史但 `changed=true` → `Stack.RecoverContext` 返回 false。

### 9.4 循环集成测试

文件：`agent` 包，复用 `scriptModel`（[agent/failure_test.go:23](../../agent/failure_test.go#L23)）和它的 `history()`/`calls()`（[agent/failure_test.go:46](../../agent/failure_test.go#L46)）。假模型按调用次数返回「第一次超限、第二次成功」。

1. **基本恢复**：第一次返回 `ContextLengthError`，第二次返回文本。断言：`calls=2`；第二次请求的 `Messages` 更短；`System`/`Tools`/`Options` 与第一次逐字段一致；结果正确；只有一对 `TurnStarted`/`TurnDone`。
2. **先落盘再重试**：在假模型的第二次调用里读取 `store.Latest`，断言 `State.Messages` 已经是压缩后的历史，且 `cp.Step == 第一次的 step`、`cp.Pause == nil`。
3. **第二次仍超限**：两次都返回超限。断言：`calls=2`；`RunFailed` 的错误 `errors.As` 到 `*ContextLengthError`；没有第三次调用；线程头部是压缩后的历史。
4. **没有恢复者**：不安装 `Compaction`/`Window`，第一次超限。断言：`calls=1`；错误类型可取；恢复行为不发生。
5. **恢复者没变短**：自定义一个总是返回原历史的 `ContextOverflowRecoverer`。断言：`calls=1`，不写中途检查点。
6. **已经流式输出**：第一次调用先发一个 `Partial` 再返回超限。断言：`calls=1`；`MessageDelta` 只出现一次；不重试。`scriptModel` 目前每次调用只 `yield` 一个响应或一个错误（[agent/failure_test.go:31](../../agent/failure_test.go#L31)）；这一条需要一个能先 `yield` 一个 `Partial` 再 `yield` 错误的模型，在测试文件里加一个小变体，不要改 `scriptModel` 的既有用例。
7. **关闭恢复**：`WithContextOverflowRetry(0)`。断言：`calls=1`，错误类型仍可取。
8. **多次恢复**：`WithContextOverflowRetry(2)`，前两次超限、第三次成功。断言：`calls=3`，两条 `ContextOverflowRetried`（`Attempt=1,2`），每次历史都更短。
9. **RAG 不重复**：自定义中间件在 `ModifyRequest` 里给 `System` 追加一个唯一标记。断言：第二次请求的 `System` 中标记只出现一次。
10. **RunBudget 收尾保持**：配置 `RunBudget` 使当前步骤进入 wrap-up。断言：第二次请求的 `Tools` 仍为空，`System` 中的收尾指令只出现一次。
11. **冻结失败**：用一个自定义恢复者，在 `RecoverContext` 里对已有工具名调用 `lc.AddTool`，注入同名的不同 schema，并返回更短历史。第一次视图冻结后 `lc.view != nil`，这次调用只改运行期工具表；第二次 `freezeStepView` 用旧视图的 schema 去比对，会因不匹配而失败。断言：不重试，错误是 `errors.Join`，`errors.As` 仍能取到超限。
12. **检查点失败**：用会失败的 store 包装。断言：不重试；错误同时包含超限和检查点错误；没有 `ContextOverflowRetried`。
13. **取消**：第一次调用返回 `context.Canceled`。断言：不进入恢复，`calls=1`。
14. **恢复不碰工具**：第一次超限、第二次成功且返回一个工具调用。断言：`ToolStarted` 只出现在第二次调用之后，`BeforeTool` 只被调用一次。
15. **第二次失败换了类型**：第一次超限、第二次返回普通 400。断言：`calls=2`；`RunFailed` 的错误是第二次的 `*StatusError`，不是超限类型；线程头部仍是压缩后的历史。

### 9.5 事件测试

1. `core/eventjson_test.go` 的往返表加入 `ContextOverflowRetried`，覆盖所有字段非零和 `Limit/Requested` 为 0 两种情况（[core/eventjson_test.go:8](../../core/eventjson_test.go#L8)）。
2. `UnmarshalEvent` 对 `context_overflow_retried` 的字段逐项还原。
3. 事件顺序测试：收集一个恢复运行的全部事件，断言 `HistoryCompacted`（或 `WindowTrimmed`）在 `ContextOverflowRetried` 之前，且 `ContextOverflowRetried` 之后才出现第二次 `MessageDone`。
4. 事件字段：`Attempt` 从 1 开始；`MessagesBefore > MessagesAfter`；`Provider`/`Limit`/`Requested` 与注入错误一致。

### 9.6 反向验证

以下每条都要在实现阶段实际做一遍，确认测试会失败：

1. 去掉 `emitted` 判断 → 第 9.4 节第 6 条应出现重复的 `MessageDelta`。
2. 跳过中途检查点 → 第 9.4 节第 2 条读到的 `store.Latest` 仍是旧历史。
3. 在同一个请求上重跑 `ModifyRequest` 而不是从视图重建 → 第 9.4 节第 9 条 RAG 标记出现两次。
4. 不检查「严格更短」→ 第 9.4 节第 5 条仍会重试。
5. 让 `RetryModel` 继续重试超限 → 第 9.2 节第 1 条 `calls` 大于 1。
6. 把中途检查点的 `Release` 改成 true → durable 运行应在重试前丢失租约（用可观察 claim 的假 store 断言）。
7. 在第二次失败后继续压缩 → 第 9.4 节第 3 条出现第三次调用。
8. 恢复后不更新 `lc.Request` → `AfterModel` 的校准读到旧请求（用记录 `lc.Request` 的中间件断言）。

### 9.7 验收门禁

```bash
go build ./...
go vet ./...
go test ./llm/... ./core/... ./middleware/... ./agent/...
go test -race ./llm/... ./core/... ./middleware/... ./agent/...
```

另外：

- `gofmt -l` 不新增文件（Windows 上既有 CRLF 文件可能被列出，只检查本次改动文件）。
- 事件往返测试通过。
- 至少一个集成测试实际读取 `store.Latest`，证明「先落盘、后重试」，而不是只断言内存里的 `State.Messages`。
- 至少一个测试覆盖「第二次仍失败」和「已经流式输出」两条拒绝重试的路径。

## 10. 实施拆分

建议按四个可独立编译/测试的提交落地。每一步都能单独跑绿，避免把错误类型、事件、循环和两个恢复者混在一个大提交里。

### 第 1 步：错误类型与分类

- `llm/error.go`：新增 `ContextLengthError`、`Unwrap`、`Retryable`、`NewStatusError`、`parseContextOverflow`、`IsContextLength`；`IsRetryable` 增加前置判断。
- `llm/openaicompat/openaicompat.go`、`llm/anthropic/anthropic.go`：改用 `llm.NewStatusError`。
- `middleware/retry.go`：退避前跳过 `llm.IsContextLength`。
- 测试：`llm/error_test.go`、`middleware/middleware_test.go`。
- 产物：类型化错误可用；重试装饰器不再空转；不改循环。

### 第 2 步：事件

- `core/event.go`：新增 `ContextOverflowRetried` 与 marker。
- `core/eventjson.go`：wire 字段、marshal/unmarshal 分支。
- 测试：`core/eventjson_test.go` 往返。
- 产物：事件可编码、可解码；还没有发布者。

### 第 3 步：恢复能力与循环

- `agent/middleware.go`：`ContextOverflowRecoverer` 接口、`Stack.RecoverContext`。
- `agent/options.go`：`WithContextOverflowRetry`。
- `agent/agent.go`/`agent/loop.go`：`config`/`AgentLoop` 字段、默认值、`streamModel` 的 `emitted` 返回值、循环恢复状态机。
- 测试：`agent` 包的 9.4 全部用例（恢复者用测试桩，先不接真实 Compaction/Window）。
- 产物：循环能力可用；生产环境的压缩器还没实现恢复方法。

### 第 4 步：Compaction/Window 恢复与文档

- `middleware/compaction.go`：`RecoverContext`、`forceCut`；把 115 行的内联发布抽成 `publishCompacted` 并让恢复路径共用；更新 `Persist` 注释。
- `middleware/window.go`：`RecoverContext`、`keepTailWithin`、`window.estimate`（让 `apply` 的 `count` 闭包委托给它，避免两条估算路径漂移）；把 256 行的发布体抽成 `publishTrimmedAs`（`publishTrimmed` 委托给它）；更新 `Strategy` 注释。
- 测试：9.3 全部用例；补充一个真实 `Compaction` + 循环的端到端用例。
- 文档：更新 7.3 列出的注释和提案锚点。
- 最终门禁：9.7。

如果第三步和第四步需要合并才能跑绿（因为 `Stack.RecoverContext` 只有测试桩），保持前两步独立，第三、四步可以合成一个提交；不建议把第一步拆分，因为错误类型和 `RetryModel` 的配合是同一个行为修正。

## 11. 风险与待定问题

### 11.1 风险

1. **响应体解析脆弱**：不同网关/版本的措辞不同，可能漏判。漏判的后果是「不恢复、直接失败」，不会误压缩；安全方向是对的。新的提供方应通过 `NewStatusError` 的签名表扩展，而不是在循环里加字符串判断。
2. **摘要模型本身失败**：压缩路径要再调一次模型；该调用失败、超时、返回空串，或摘要输入本身就超过摘要模型的窗口时，恢复放弃，运行仍以超限错误失败。这是 best-effort 的固有限制。
3. **摘要有损**：被摘要的历史细节可能丢失，模型后续回答质量下降。正常步骤仍按 `KeepRecent` 保留最近消息；恢复路径更激进，可能保留得更少。`HistoryCompacted` 事件给出丢弃条数，必要时用户可调小阈值/调大保留数。
4. **成本和延迟**：默认多一次重试调用；摘要恢复还会多一次摘要模型调用，且可能更慢。`WithContextOverflowRetry(0)` 可以关闭。
5. **请求模式被持久化**：平时 `Persist:false` 的用户可能惊讶于「超限后状态变短了」。这是为了崩溃安全；文档和 `HistoryCompacted` 事件必须明确。
6. **多个恢复者的顺序**：第一个生效意味着 `Window` 排在 `Compaction` 前面时会优先丢弃而不是摘要；5.3 已建议把 `Compaction` 放在前面。用户自定义恢复者时，注册顺序就是优先级。
7. **检查点失败后的内存状态**：恢复时先改 `rc.State.Messages` 再写检查点；如果写失败，`fail` 会再写一次 `Release:true`。如果两次都失败，线程仍停在旧检查点，内存状态与磁盘不一致——这是现有 `fail` 已有的行为，不是 P0-4 新增的。
8. **估计器与上限不一致**：`Limit` 是提供方按自己的 tokenizer 计算的，而恢复用的是本地估算 × 校准因子；目标取 3/4 或 1/2 是保守近似，不保证一次成功。
9. **单条消息过大**：无法切割消息，恢复可能直接放弃；这类请求需要业务层把大工具结果分页/落盘，而不是靠上下文恢复。
10. **跨进程事件**：新事件类型对旧消费者是未知类型；混合版本部署需要同步升级消费者。
11. **`ModelContext` 重跑**：如果某个 `ModelContexter` 有非幂等副作用（例如按步骤号缓存 span），第二次调用会得到不同的上下文。该接口的文档定位是「为一次提供方调用注入 span」（[agent/middleware.go:211](../../agent/middleware.go#L211)），重跑是符合语义的；实现时应在测试里覆盖 tracing 只产生两个 span、不产生重复父链。
12. **请求模式的预压缩**：如果第一次请求已被请求模式压缩，恢复仍基于工作历史，重试请求可能比刚失败的请求大，第二次调用仍可能超限。见 5.4 的边界说明；持久模式或自定义恢复者可以避免。

### 11.2 待定问题

1. **`ContextLengthError` 是否要带「解析来源」**（`code` / `message` / `header`）？v1 不带；如果后续需要区分「提供方明确说超限」和「我们猜的」，可以加一个未导出的标记或导出字段。
2. **未知 `Limit` 时是否要用校准估算反推上限**？v1 不用；减半更简单，也不会把估算误差当成真实上限。
3. **`Requested` 缺失时是否填入本地估算**？v1 不填，保持「提供方报告值」的语义；事件里的 0 表示未知。
4. **恢复预算按步骤还是按运行**？v1 按步骤；按运行需要把计数放进 `State`/`RunContext`，并定义跨 resume 的语义，复杂度更高。
5. **是否让 `FallbackModel` 默认把超限当失败**？v1 不；自动换模型会改变模型身份，应该由用户显式选择。
6. **是否提供内置的「只保留最近 N 条」恢复者**？`Window` 已经能配置 `RecentN`，不需要第三个实现。
7. **`ContextOverflowRetried` 是否要带压缩者名字**？v1 不带；前一条 `HistoryCompacted`/`WindowTrimmed` 已经给出了来源。若后续要简化订阅方，可加 `Recoverer string`。
8. **摘要调用是否计入 `RunBudget` 的 token 预算**？v1 不计；`RunBudget` 的 `AfterModel` 只在成功调用后记账，摘要走的是 `Compaction` 自己的模型调用，不会经过循环的 `AfterModel`。这与现有正常压缩的行为一致。

## 12. 代码锚点

以下行号均在 `2f18017` 上核对。

| 位置 | 作用 |
|---|---|
| [agent/loop.go:247](../../agent/loop.go#L247) | `runStep` 的五个阶段 |
| [agent/loop.go:253](../../agent/loop.go#L253) | 每步构造 `LoopContext` |
| [agent/loop.go:254](../../agent/loop.go#L254) | 每步唯一的 `TurnStarted` |
| [agent/loop.go:281](../../agent/loop.go#L281) | 步骤开头的 `CompactHistory` |
| [agent/loop.go:303](../../agent/loop.go#L303) | 构造 `llm.Request` |
| [agent/loop.go:306](../../agent/loop.go#L306) | `ModifyRequest` |
| [agent/loop.go:309](../../agent/loop.go#L309) | `freezeStepView` |
| [agent/loop.go:314](../../agent/loop.go#L314) | `checkReplayDrift` |
| [agent/loop.go:322](../../agent/loop.go#L322) | `ModelContext` |
| [agent/loop.go:324](../../agent/loop.go#L324) | 第一次模型调用 |
| [agent/loop.go:418](../../agent/loop.go#L418) | 工具策略检查 |
| [agent/loop.go:442](../../agent/loop.go#L442) | 工具批次执行 |
| [agent/loop.go:454](../../agent/loop.go#L454) | 步骤结束检查点 |
| [agent/loop.go:457](../../agent/loop.go#L457) | 每步唯一的 `TurnDone` |
| [agent/loop.go:479](../../agent/loop.go#L479) | `streamModel` |
| [agent/loop.go:486](../../agent/loop.go#L486) | 模型错误的 `OnError` 观察钩子（P0-4 在此之后插入恢复） |
| [agent/loop.go:514](../../agent/loop.go#L514) | `checkpoint` |
| [agent/loop.go:567](../../agent/loop.go#L567) | `fail` |
| [agent/loop.go:584](../../agent/loop.go#L584) | `seam` |
| [agent/middleware.go:158](../../agent/middleware.go#L158) | `HistoryCompacter` 能力 |
| [agent/middleware.go:185](../../agent/middleware.go#L185) | `Stack` |
| [agent/middleware.go:215](../../agent/middleware.go#L215) | `ModelContext` 折叠 |
| [agent/middleware.go:272](../../agent/middleware.go#L272) | `CompactHistory` 折叠 |
| [agent/middleware.go:332](../../agent/middleware.go#L332) | `OnError` 折叠 |
| [agent/stepview.go:65](../../agent/stepview.go#L65) | `StepView` 字段 |
| [agent/stepview.go:122](../../agent/stepview.go#L122) | `System()` |
| [agent/stepview.go:131](../../agent/stepview.go#L131) | `Options()` |
| [agent/stepview.go:140](../../agent/stepview.go#L140) | `Messages()` |
| [agent/stepview.go:148](../../agent/stepview.go#L148) | `Tools()` |
| [agent/stepview.go:168](../../agent/stepview.go#L168) | `snapshot()` |
| [agent/stepview.go:201](../../agent/stepview.go#L201) | `ToolView.Schemas()` |
| [agent/stepview.go:388](../../agent/stepview.go#L388) | `freezeStepView` |
| [agent/stepview.go:437](../../agent/stepview.go#L437) | `checkReplayDrift` |
| [agent/loopctx.go:64](../../agent/loopctx.go#L64) | `AddTool` 对当前请求的影响 |
| [agent/options.go:93](../../agent/options.go#L93) | `WithMaxTurns`（新选项的邻居） |
| [agent/agent.go:38](../../agent/agent.go#L38) | `config` 默认值种入点 |
| [agent/run.go:274](../../agent/run.go#L274) | `RunFailed` 发布 |
| [agent/hitl.go:79](../../agent/hitl.go#L79) | `Resume` |
| [agent/hitl.go:189](../../agent/hitl.go#L189) | 无 pending 时的 replay 基线 |
| [agent/hitl.go:210](../../agent/hitl.go#L210) | `validatePause` |
| [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339) | `commitCheckpoint` |
| [llm/error.go:14](../../llm/error.go#L14) | `StatusError` |
| [llm/error.go:20](../../llm/error.go#L20) | 固定错误串 |
| [llm/error.go:28](../../llm/error.go#L28) | `Retryable` |
| [llm/error.go:37](../../llm/error.go#L37) | `IsRetryable` |
| [llm/openaicompat/openaicompat.go:89](../../llm/openaicompat/openaicompat.go#L89) | OpenAI 兼容错误构造 |
| [llm/anthropic/anthropic.go:89](../../llm/anthropic/anthropic.go#L89) | Anthropic 错误构造 |
| [llm/model.go:27](../../llm/model.go#L27) | `Request` |
| [llm/model.go:43](../../llm/model.go#L43) | `Response` |
| [llm/options.go:6](../../llm/options.go#L6) | `Options` |
| [middleware/retry.go:46](../../middleware/retry.go#L46) | `RetryModel.Generate` |
| [middleware/retry.go:64](../../middleware/retry.go#L64) | 只按 `yielded`/次数停止 |
| [middleware/fallback.go:38](../../middleware/fallback.go#L38) | 默认 fallback 判定 |
| [middleware/fallback.go:69](../../middleware/fallback.go#L69) | 已输出不切换 |
| [middleware/circuit.go:85](../../middleware/circuit.go#L85) | 默认失败判定 |
| [middleware/compaction.go:20](../../middleware/compaction.go#L20) | `CompactionOptions` |
| [middleware/compaction.go:92](../../middleware/compaction.go#L92) | 请求模式改写 |
| [middleware/compaction.go:105](../../middleware/compaction.go#L105) | 持久模式改写 |
| [middleware/compaction.go:115](../../middleware/compaction.go#L115) | `HistoryCompacted` 发布 |
| [middleware/compaction.go:123](../../middleware/compaction.go#L123) | `compact` |
| [middleware/compaction.go:146](../../middleware/compaction.go#L146) | `safeCut` |
| [middleware/compaction.go:158](../../middleware/compaction.go#L158) | `AfterModel` 校准 |
| [middleware/compaction.go:175](../../middleware/compaction.go#L175) | `calibratedEstimate` |
| [middleware/compaction.go:179](../../middleware/compaction.go#L179) | `summarize` |
| [middleware/compaction.go:258](../../middleware/compaction.go#L258) | `estimateTokens` |
| [middleware/window.go:43](../../middleware/window.go#L43) | `groupUnits` |
| [middleware/window.go:94](../../middleware/window.go#L94) | `keptMessages` |
| [middleware/window.go:117](../../middleware/window.go#L117) | `repairPairing` |
| [middleware/window.go:154](../../middleware/window.go#L154) | `WindowOptions` |
| [middleware/window.go:207](../../middleware/window.go#L207) | 请求模式改写 |
| [middleware/window.go:224](../../middleware/window.go#L224) | 持久模式改写 |
| [middleware/window.go:242](../../middleware/window.go#L242) | `apply` |
| [middleware/window.go:256](../../middleware/window.go#L256) | `publishTrimmed` |
| [middleware/window.go:310](../../middleware/window.go#L310) | `WindowStrategy` |
| [middleware/rag.go:45](../../middleware/rag.go#L45) | RAG 的 `ModifyRequest` |
| [middleware/rag.go:61](../../middleware/rag.go#L61) | 追加系统块（重跑会重复） |
| [middleware/runbudget.go:176](../../middleware/runbudget.go#L176) | RunBudget 的 `ModifyRequest` |
| [middleware/runbudget.go:181](../../middleware/runbudget.go#L181) | 清空工具 |
| [middleware/runbudget.go:182](../../middleware/runbudget.go#L182) | 追加收尾指令 |
| [core/event.go:12](../../core/event.go#L12) | `Event` 并集 |
| [core/event.go:126](../../core/event.go#L126) | `HistoryCompacted` 类型 |
| [core/event.go:141](../../core/event.go#L141) | `WindowTrimmed` |
| [core/event.go:180](../../core/event.go#L180) | 事件 marker 区 |
| [core/eventjson.go:18](../../core/eventjson.go#L18) | `eventWire` |
| [core/eventjson.go:104](../../core/eventjson.go#L104) | 现有压缩事件的编码 |
| [core/eventjson.go:121](../../core/eventjson.go#L121) | `UnmarshalEvent` |
| [core/eventjson.go:168](../../core/eventjson.go#L168) | 现有压缩事件的解码分支 |
| [core/eventjson_test.go:8](../../core/eventjson_test.go#L8) | 事件往返测试 |
| [core/state.go:12](../../core/state.go#L12) | `State` |
| [core/state.go:16](../../core/state.go#L16) | `KV` |
| [checkpoint/checkpoint.go:36](../../checkpoint/checkpoint.go#L36) | `StepSnapshot` |
| [checkpoint/checkpoint.go:45](../../checkpoint/checkpoint.go#L45) | `Checkpoint` |
| [agent/failure_test.go:23](../../agent/failure_test.go#L23) | 可注入错误的 `scriptModel` |
| [agent/failure_test.go:46](../../agent/failure_test.go#L46) | `scriptModel.history()` |
| [middleware/compaction_internal_test.go:61](../../middleware/compaction_internal_test.go#L61) | 测试用 `newLC` |
| [middleware/window_internal_test.go:17](../../middleware/window_internal_test.go#L17) | 工具消息夹具 |
| [middleware/middleware_test.go:93](../../middleware/middleware_test.go#L93) | 既有 `RetryModel` 用例 |
| [docs/analysis/codex-core-runtime-optimization-proposal.md:113](codex-core-runtime-optimization-proposal.md#L113) | P0-4 原始建议 |
