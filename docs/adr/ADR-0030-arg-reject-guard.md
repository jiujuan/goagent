# ADR-0030: ArgGuard —— 参数校验拒绝的计数与升级

- 状态: Accepted（**四个 TASK 全部实施**：AG-01 `51df6fc`——`RejectClass`/`ToolRejection`/`ToolRejecter` + `Stack.ToolReject` + 循环侧标注与两处调用点；AG-02 `adc064c`——`core.ArgRejected` 变体 + `eventjson` 编解码 + 往返与到达订阅者两条用例；AG-03 `7c4d40f` + `41e37ca`——`middleware.ArgGuard`（按工具名计数、一次告警、到阈值标记、BeforeTool 只作用一次、干预预算、KV 账本）+ 包内外 15 组用例；AG-04 `e622dab`——`examples/arg-guard`，同一段脚本两遍对照 LoopGuard 不动作与 ArgGuard 走完"计数—告警—暂停—预算耗尽后终止"的阶梯；AG-05 `19d3895` + `551ad9f`——再补两个视角示例（写工具的人、读账本的人），三个示例的分工见 §10。均未推送。实施期与本文的差异记在 `docs/tasks/plan-arg-reject-guard.md` 的五节"完成记录"）
- 日期: 2026-10-02
- 关联: ADR-0023(LoopGuard)、ADR-0029(工具执行超时，提供 `RejectTimedOut` 分类)
- 执行卡: `docs/tasks/plan-arg-reject-guard.md`

## 背景

循环会在真正执行 handler 之前拒绝一次调用，共有四类原因，全在 `callOne` 里（`agent/exectools.go:89-117`）：

| 拒绝原因 | 代码位置 | 回喂给模型的文本 |
| --- | --- | --- |
| 工具名不存在 | `:92-98` | `unknown tool: <name>` |
| `ArgumentPreparer` 修参失败 | `:100-106` | `invalid arguments: <err>` |
| JSON Schema 校验失败 | `:107-109`（`tool.Validate`，`tool/validate.go:21-35`） | `invalid arguments: <err>` |
| handler 返回 Go error | `:112-114` | `<err>` |

四类都只变成一条 `IsError` 的 `core.ToolResult`（`errResult`，`agent/exectools.go:119-124`）写进历史，**没有任何计数、没有次数上限**。设计意图本身是对的 —— "工具错误是数据，不是 Go 错误"（`sandbox/sandbox.go:57-60` 写明了同一立场），让模型自我修正。问题是修正失败时无人拦停。

现有的相关计数器都不覆盖这个面：

- `middleware.LoopGuard` 的 `error_streak` 规则要求连续错误的**工具名 + 错误首行完全相同**（`middleware/loopguard.go:307`：`key := tr.Name + "\x00" + firstLine(resultText(tr))`）。模型每次改一点参数，校验错误文本就跟着变（缺 `q` → 缺 `limit` → `q` 类型不对），连击计数不断归零，永远命不中阈值。
- `LoopGuard` 的 `repeat_call` 规则要求**调用签名相同**（`middleware/loopguard.go:340-346`），参数一变签名就变。
- `middleware.CircuitBreaker` 与 `RetryModel`、`FallbackModel` 只包裹 `llm.Model`（`middleware/circuit.go:74`、`middleware/retry.go:27`、`middleware/fallback.go`），没有工具版本。

所以真实场景是：模型对一个复杂 schema 的工具连续发出十几次都不合格，直到用满 `MaxTurns`（`agent/loop.go:19,145`）抛 `ErrMaxTurnsExceeded`（`:260`），既没有结果，也多花了 token。

缺的第二件事是观测：`obs/otel` 已经按 `error` 属性给每次工具调用计数（`obs/otel/otel.go:117-119,265-282`），但那是原始指标，进程内没有"某工具被拒绝了几次"的可用数值，`State.KV` 里也没有。

## 名词

- **拒绝(rejection)**：handler 没有被执行的那类结果，即上表四种，外加 ADR-0029 的超时。
- **参数面拒绝**：因调用本身不合格而被拒（工具名不存在、修参失败、schema 校验失败）。handler 错误与超时不属于默认统计范围。

## 决策概览

| 层 | 加什么 |
| --- | --- |
| agent 扩展点 | 可选能力 `ToolRejecter` + 公开类型 `RejectClass` / `ToolRejection`（`agent/middleware.go`） |
| agent 执行 | `callOne` 给每条结果标注拒绝类别；`execTools` 收集并在**批次结束后串行**交给循环，循环逐个调用钩子 |
| core | 新增事件 `core.ArgRejected{Tool, Class, Count, Step}`（密封联合变体 + `eventjson` 编解码） |
| middleware | `middleware.ArgGuard`：按工具名计数、警告、升级 |

立场：**分类由循环做（只有 `callOne` 知道失败原因），策略由中间件做（循环不含策略）**，与 `ModelContexter`/`HistoryCompacter` 的"循环只提供可选接口、策略放在中间件"一致（`agent/middleware.go:46-68`）。

## 1. 分类与扩展点

```go
// agent/middleware.go
type RejectClass int

const (
    RejectUnknownTool   RejectClass = iota // 名字查不到
    RejectPrepareFailed                     // PrepareArguments 返回错误
    RejectSchemaInvalid                     // tool.Validate 返回错误
    RejectHandlerError                      // handler 返回 Go error
    RejectTimedOut                          // ADR-0029 的放弃等待合成
)

func (k RejectClass) String() string // "unknown_tool" | "prepare_failed" | "schema_invalid" | "handler_error" | "timed_out"

type ToolRejection struct {
    Call   core.ToolCall
    Class  RejectClass
    Detail string // 与回喂给模型的文本一致
}

type ToolRejecter interface {
    OnToolReject(lc *LoopContext, r ToolRejection)
}
```

`Stack.ToolReject(lc, rejects []ToolRejection)` 逐条 rejection 外层、逐中间件内层，按注册顺序遍历实现了该接口的中间件；没有任何实现时是 no-op，行为与今天完全相同。

**五类中 `RejectTimedOut` 的标注点在 `awaitAbandoned`，不在 `callOne`**（ADR-0029 的放弃等待发生在那里）。同一句合成超时文案既进 `ToolResult` 也进 `ToolRejection.Detail`，两者由同一个函数产出，不会分叉。run 本身被取消那一支**不产生拒绝记录**：类别表里没有"运行被取消"，而且此时已经没有下一步能让升级生效。

与"工具自己报告失败"的区别：handler 正常返回但 `Result.IsError = true`（`tool.ErrorResult`，`tool/tool.go:102-104`）**不是拒绝**，不计入；`eval` 的 ToolGuard 在 AfterTool 里把结果翻成 `IsError`（`eval/loop.go:79-96`）同样不计入。只有 handler 未被执行（或执行未完成）才算。另外两处 handler 未执行的情形同样不计，理由已写进这两处的代码注释：人工门径的拒绝（`deniedResult`，那是人的决定，不是调用不合格）与 max_tokens 截断整批（`truncatedResults`，问题出在回复被截断，不在那条调用）。

## 2. 钩子调用时机：批次结束后，串行

拒绝发生在 `execTools` 的工作协程里（并发默认见 `agent/options.go:92-95`），但钩子**不在协程内调用**。`callOne` 把 `ToolRejection` 随结果一起返回，`execTools` 收集成本批次的列表，循环在批次结束后统一调用：

```go
results, dirs, rejects := l.execTools(rc, lc, calls)   // agent/loop.go:254
l.mw.ToolReject(lc, rejects)                            // 新增，紧随其后
history = append(history, core.Message{Role: core.RoleTool, Parts: results})
```

位置选在 `execTools` 返回之后、`rc.State.Messages = history` 与 `l.checkpoint`（`agent/loop.go:259-260`）之前 —— 中间件在这里写 `State.KV` 会随本步 checkpoint 落盘，因此计数跨 HITL 暂停和进程重启仍然保留。

这样安排的理由，写进代码注释：

1. 避免与 `execTools` 的 `stateMu` 写并发（`agent/exectools.go:24-32`）；中间件可以直接用 `lc.State.Apply`，不需要自己加锁。
2. 不给 `ToolRejecter` 施加 `AfterTool` 那条协程安全约束（`agent/middleware.go:20-21`）。
3. 升级动作本来就作用在**下一步**（见第 4 节），晚一个批次通知没有语义损失。

`runResumed`（`agent/hitl.go:143`）同样调用 `execTools`，恢复路径需要一并调用，否则 HITL 放行批次里的拒绝不会被计数。实施时的形状：那个批次的 `LoopContext` 由 loop 构造后传入 `runResumed(rb, lc)`（`agent/loop.go:119-124`），钩子随即在同一个 `lc` 上调用 —— 阶段顺序仍集中在 loop，且 `hitl.go` 不必再复制一份 `LoopContext` 字面量。位置同样在批次收束之后、本步快照写出之前。

## 3. 计数状态：按工具名，存 State.KV

`middleware/ArgGuard` 的计数记录分三个 `State.KV` 键存放（实施时定形，命名沿用 LoopGuard 的 `_loopguard.*` 风格）：

```go
"_argguard.tools": map[string]any{        // 按工具名
    "<tool name>": map[string]any{"count": int, "warned": bool},
},
"_argguard.actions":       map[string]string, // 工具名 -> "interrupt" | "stop"，待下一步生效的标记
"_argguard.interventions": int,               // 已用干预预算
```

- 写入一律用 `map[string]any` / `map[string]string` / `int` 这类纯结构，**不放本包的 struct**：字段名小写的 struct 经 `json.Marshal` 会得到 `{}`，恢复后计数静默归零（`middleware/argguard_test.go` 的 `TestArgLedgerSurvivesJSON` 专防这一条）。

- **键粒度取工具名**（决策点，已与需求方确认）。语义是"这个工具连续被拒了 N 次，不论参数怎么变"，正对背景里那条"错误文本一变就不满足 LoopGuard 连击条件"的缺口。按调用签名计会退回同一个问题。
- 数值一律 `float64`，读取时容忍 `int`/`int64`/`float64` 三种形态 —— JSONL checkpoint 往返后只能是 `float64`（`core.State.KV map[string]any`，`core/state.go:10-15`；`Apply(OpSetKV)` `:75-93`；LoopGuard 的同类容错见 `middleware/loopguard.go:374-387`）。
- 中间件实例随 Agent 构造、跨 run 共享（`agent/loop.go:63`），所以不放内存态；从 `State.KV` 读，恢复后行为一致 —— 与 LoopGuard、RunBudget 同一立场。

## 4. 分级处置

```go
middleware.ArgGuard(middleware.ArgGuardOptions{
    WarnThreshold:      3,        // 单工具被拒达到此数：steering 警告一次
    EscalateThreshold:  6,        // 达到此数：升级
    OnEscalate:         middleware.ArgGuardInterrupt, // 或 ArgGuardStop
    Count               []agent.RejectClass,          // 默认三类参数面拒绝；可加 HandlerError / TimedOut
    MaxInterventions:   3,        // 干预预算；超出后新写下的标记转为 Stop
    OnDetect:           func(tool string, count int, class agent.RejectClass),
})
```

零值取默认，工厂不加 `New` 前缀（同 `middleware/compaction.go:56`、`middleware/circuit.go:74`）。

`OnToolReject` 里做两件事：

1. 该工具计数 +1，发布 `core.ArgRejected{Tool, Class: k.String(), Count, Step}`，调用 `OnDetect`。
2. 到 `WarnThreshold` 且 `warned` 为假 → `lc.Steer` 一条 `[arg-guard]` 消息（`RunContext.Steer` 是 goroutine 安全的，`agent/runtime.go:105`；下一步开头 drain 进历史，`agent/loop.go:150-152`）：工具名、被拒次数、最近一次 `Detail`，以及**从本次请求的工具广告里取该工具的 `required` 字段列表**。取法：`lc.Request.Tools`（`agent/loop.go:170-172` 已赋 `[]llm.ToolSchema`，字段为 `Name`/`Description`/`Parameters json.RawMessage`，`llm/model.go:70-74`），解析 `Parameters` 的 `required` 数组。AfterModel/OnToolReject 阶段 `lc.Request` 仍在，无需新 plumbing。
3. 到 `EscalateThreshold` → 写 `actions` 标记并计入干预预算；实际动作在 `BeforeTool` 执行，与 LoopGuard 的写法完全一致（`middleware/loopguard.go:171-181`）：
   - `ArgGuardInterrupt`（默认）→ `core.Directive{Kind: core.Interrupt}`，走既有 `PendingHITL` 快照与 `Run.Decide/Resume`（`agent/loop.go:227-233`、`agent/hitl.go`）。
   - `ArgGuardStop` → 直接结束，保留最后一条 assistant 消息为结果（`agent/loop.go:234-240`）。
   - 超过 `MaxInterventions` 后**新写下的标记本身变成 stop**，而不是"下一步任何调用一律 Stop"：预算耗尽时最后那一次升级仍是较温和的一种，再往上才转为终止；没有标记的调用任何情况下都不受影响——本中间件只拦它数过的那个工具（已与需求方确认；本节早先写的"一律 Stop"会连带打断与该工具无关的正常调用）。
   - 标记**只作用一次**：`BeforeTool` 命中后即从 KV 删除。不清除会出现"模型已经改对参数，仍被上一步的旧标记拦住"，而中断/终止本身已经结束了这一步，不需要留痕。

警告前缀常量 `WarnMarkerArg = "[arg-guard]"`（与 `WarnMarker`（`middleware/loopguard.go:99`）、`WarnMarkerBudget`（`middleware/runbudget.go:97`）同一命名法）。告警正文取英文，与 `warningMessage` 一致。

## 5. 与 LoopGuard 的边界

两者会在同一批上各自命中，**不去重**，理由：

- 判据不同：ArgGuard 按工具名数参数面拒绝；LoopGuard 按调用签名数复读、按"工具名+错误首行"数连击（`middleware/loopguard.go:208-230,292-319`）。
- 升级点相同，多个指令的合并结果是安全的：`BeforeTool` 的多方指令由 `core.Resolve` 按优先级取最高，`Interrupt > Stop > Escalate > Transfer > Continue`，同优先级按中间件注册顺序取前者（`core/directive.go:13-31,51-62`；合并实现 `agent/middleware.go:157-167`）。
- KV 命名空间分离：`_argguard` 与 `_loopguard.actions` / `_loopguard.interventions`（`middleware/loopguard.go:101-102`）互不读写。

## 6. core 改动

只加一个事件变体（决策点，已与需求方确认：与 `StuckDetected`、`BudgetExceeded`、`HistoryCompacted` 对齐，防护中间件的动作一律有事件）：

```go
// core/event.go
type ArgRejected struct {
    Tool  string
    Class string // RejectClass 的字符串形式
    Count int    // 该工具在本次 run 内累计被拒次数（含本次）
    Step  int
}
func (ArgRejected) isEvent() {}
```

`core/eventjson.go` 的 `eventWire` 需新增 `Tool string` / `Count int` / `Class string` 三个 `omitempty` 字段（实施后字段表 `:18-39`，此前无同名字段），并在 `MarshalEvent` 与 `UnmarshalEvent` 两个 switch 各加一条 `case`（`:92-93`、`:145-146`），类型为 `"arg_rejected"`。密封联合要求 marker 与编解码同时补齐，否则 `default` 分支会报 "cannot marshal event of type"。

往返用例与 `StuckDetected`、`BudgetExceeded`、`HistoryCompacted` 放在一起（`core/stuckevent_test.go`），而不是加进 `core/eventjson_test.go` 的那张通用表 —— 后者只覆盖联合建立之初的变体，三个防护事件都各自成一条用例。发布侧的断言在 `agent/reject_test.go`：一个测试内的 `ToolRejecter` 把拒绝逐条发成 `ArgRejected`，Stream 订阅者按步收到 `Count` 递增的三条，并与 `State.KV` 的计数对齐（AG-03 的 `ArgGuard` 会替换那个测试发布者，这条断言本身与谁发布无关）。

## 7. 备选方案

- **A. 给 `core.ToolResult` 加错误类别字段**：AfterTool、otel、eval 都能直接看到分类，但这是公共类型改动，会影响所有构造 `ToolResult` 的位置和已落盘的 checkpoint JSONL 兼容性。否决 —— 分类只在拒绝发生的位置上有意义，不必长期随结果传递。
- **B. 中间件在 AfterTool 解析 `errResult` 的文本前缀**（`"invalid arguments: "`，`agent/exectools.go:103,108`）：零循环改动，但把行为绑在文案上，措辞一改就静默失效，且 `unknown tool` / handler 错误无从区分。否决。
- **C. 并入 LoopGuard 作第三条规则**：配置面继续变大（`LoopGuardOptions` 已有 8 个字段，`middleware/loopguard.go:29-53`），并把"参数不合格"与"调用重复"两个不同关注点耦合在一个组件里；两者的豁免名单、阈值语义也不同。否决。
- **D. 按调用签名计数**：正是要修的问题的反面 —— 参数略变即不计数。否决（已与需求方确认按工具名）。
- **E. 循环内直接实现计数与升级**：最省代码，但把策略硬编码进循环，违背 `agent/middleware.go:10-18` 的立场，也无法按 agent 关闭。否决。

## 8. 成本

`OnToolReject` 每次一次 KV map 读写；`schema_invalid` 的分类本身就是 `tool.Validate` 已算过的结果，不重复校验。警告文案里的 `required` 解析是一次 O(schema 大小) 的 JSON 解析，仅在到阈值那一次发生。

## 9. 测试锚点

- 分类正确性：四类拒绝各一例（不存在的名字、`PrepareArguments` 返错、缺 required 字段、handler 返 `error`），断言 `RejectClass` 与 `Detail` 文本一致；handler 正常返回但 `IsError=true` 时**不**触发钩子。
- 计数按工具名：同一工具三种不同的错误参数 → `count=3` 并触发警告；错误文本每次不同也照计（这是背景缺口的反向验证）。
- 调用时机：并发批次里两条拒绝 → `go test -race` 通过，`State.KV` 两项都计上，且不需要中间件自己加锁。
- 分级处置：第 3 次出现 `[arg-guard]` 警告且不重复；第 6 次写标记 → 下一步 `BeforeTool` 返回 `Interrupt` → 断言 `PendingHITL` 落盘 → `Decide(true)/Resume` 放行 → 超 `MaxInterventions` 断言 Stop。
- durable：Interrupt 后跨进程 `Resume`，确认 `_argguard` 的 `float64` 读回与计数继续（复用 `agent/durable_test.go`、`middleware/loopguard_test.go` 的机制）。
- 事件：`core.MarshalEvent/UnmarshalEvent` 对 `ArgRejected` 的往返；`Stream` 观察者能按顺序收到多条 `ArgRejected`。
- 警告文案含 `required` 字段：给一个 required 为 `["q","limit"]` 的工具，断言警告串里出现两个字段名。
- 与 LoopGuard 共存：两个中间件同时装、同一批同时命中，断言合并后的指令仍是 `Interrupt`，且各自 KV 命名空间互不污染。

## 10. 可运行示例（三个视角）

三个示例都是 `package main` + `llm/mock`，无网络、无密钥，可重复执行（打印全部由主流程做，模型响应函数不落笔；`State.KV` 摘要按键名排序）；实测时长见各行。

| 视角 | 命令 | 教什么 |
| --- | --- | --- |
| 阶梯与配置 | `go run ./examples/arg-guard`（约 0.7s，`e622dab`） | 同一段"每次参数都不太对"的脚本跑两遍：只装 LoopGuard 时两条判据都认不出，一步不拦直到 `MaxTurns` 用满报失败（本 ADR 背景里那道缺口）；装 ArgGuard 则数到告警、数到标记、暂停两次、预算花完后第三次升级直接终止，末行打印 `_argguard.*` 三个键 |
| 写工具的人 | `go run ./examples/arg-guard-tool-author`（约 0.4s，`19d3895`） | 被记几次很大程度取决于工具接住了多少：同一份 schema 两段跑，裸写法四次拒绝换来一条告警，加 `PrepareArguments` 把别名字段与字符串数字接住后只剩一次（还是 preparer 自己承认修不了的那次），并且它写的那句缺什么会原样回给模型；第四段用 `Count` 的正反两跑说明 handler 的 500 默认不计，计了就会训斥参数全对的调用 |
| 读账本的人 | `go run ./examples/arg-guard-ops`（约 0.4s，`551ad9f`） | 事件流、形状、存活三样：`ArgRejected` marshal 成日志行（`step` 为 0 时被 omitempty 省掉，消费侧按缺省处理）；同一个计数键从内存读回 `int`、从 JSONL 读回 `float64`（只断言 `int` 会读成 0），带私有字段的 struct 进 KV marshal 出来是 `{}`；换 store、换 agent、换中间件实例但同一线程时计数照旧续涨且告警不重发，而人的否决既不计数也不动预算 |

- 被拦下的永远是**下一条**调用：标记写在被拒那一步，`BeforeTool` 只在兑现它的那一次清掉。所以人工决定要批的那条可能参数完全正确，而一段 run 结束时 `_argguard.actions` 里也可能留着一次没兑现的动作（ops 示例末段可见）。
- `core.Stop` 收尾仍然只发 `RunDone`（`agent/run.go:121`），干预预算也是在写标记那一刻扣掉的，与随后放行还是否决无关——这两个判断在示例里都只能靠最终消息文本与账本读出。
