# ADR-0024: RunBudget —— per-run 成本/时长上限与 maxTurns 优雅收尾

- 状态: Proposed
- 日期: 2026-09-29
- 关联: ADR-0023(LoopGuard，同类中间件)、`agent/loop.go`(阶段机)、`core/usage.go`

## 背景

长任务的资源防线目前全部是"步数"这一个维度：

1. **Usage 只上报不强制**。Provider 在最终响应上带 `core.Usage{InputTokens, OutputTokens}`（`core/usage.go:5-8`、`llm/model.go:55`），loop 把它交给 AfterModel（`agent/loop.go:164`）并发 `MessageDone`（`agent/loop.go:266`）——没有任何组件累计或设限。
2. **无 per-run 时长上限**。只有 `sandbox` 的命令级超时（`sandbox/process.go:63-81`）与 `ratelimit` 的 RPS 整形；run 本身可以跑任意久。
3. **撞 maxTurns 是硬报错**。`for step < l.maxTurns`（`agent/loop.go:125`）耗尽后返回 `ErrMaxTurnsExceeded`（`agent/loop.go:233`）→ `RunFailed`（`agent/run.go:112-115`）。此前 16 轮的劳动只留下一个 error，没有可用的收尾产物；queue 消费者（`queue/`）拿到的是失败而非结果。
4. **无 cost 维度**。`Usage` 不带价格；`eval.TokenBudget`（`eval/rule.go:125-133`）是跑完后的打分器，不是运行中护栏；`examples/agent-tutorial` 里的 `budgetMW` 只是"改控制流"的教学演示（`examples/agent-tutorial/main.go:171-180`）。

目标：**预算接近上限时先提醒模型收尾；预算耗尽或只剩最后一轮时，禁用工具、强制模型用一条最终答复结束 run，而不是抛错。**

## 名词

- **收尾模式（wrap-up）**：后续模型请求 `req.Tools` 置空，并注入"请直接产出最终答复"的指令。loop 的自然结束条件（回复不含 tool call，`agent/loop.go:172-181`）随之触发，run 以真实结果正常收尾（`RunDone`）。
- **预警提醒（warn）**：某资源达到上限的 `WarnRatio`（默认 80%）时，向对话注入一条提醒消息，请模型开始收敛，每种资源只提醒一次。
- **收尾标志**：`State.KV["_budget"]["wrapup"]`，为 true 时进入收尾模式。
- **资源（resource）**：`input_tokens | output_tokens | total_tokens | cost | duration | turns`。
- **run 终身累计**：计数与耗时无内存态，全部存于 `State.KV`（随 checkpoint JSON 持久，resume/跨进程续跑后继续累计，与 ADR-0023 同一立场）。

## 决策概览

新增 **`middleware.RunBudget`**（工厂命名遵循中间件工厂惯例），重写四个钩子，零改 loop 语义；唯一的核心侧改动是给 `LoopContext` 加一个只读字段 `MaxTurns`（见"maxTurns 优雅收尾"）：

| 钩子 | 职责 |
| --- | --- |
| `AfterModel` | 用量记录：累计 `resp.Usage`（仅非 Partial 的最终响应，`llm/model.go:54-55` 由 loop 传入，`agent/loop.go:164`），折算 cost，写 `State.KV["_budget"]`；越过预警线 → 注入一次性预警提醒 |
| `BeforeModel` | 检查预算：超硬上限 → 置收尾标志；时长预算在此检查（每步串行点，粒度=一步） |
| `ModifyRequest` | 执行收尾模式：收尾标志为真时 `req.Tools = nil` 并追加收尾指令。Compaction 同钩子改 Messages（`middleware/compaction.go:46`），两者互不影响 |
| `BeforeTool` | 收尾模式保障：已置收尾标志而模型仍发出 tool call → 返回 `Stop` 指令（该答复仍作为结果，`agent/loop.go:209-214` → `RunDone`，不是 `RunFailed`） |

观测：新增事件 `core.BudgetExceeded{Resource string; Step int}`（同 StuckDetected 的做法，sealed union 加变体 + `eventjson.go` 编解码）+ `OnExceed func(report)` 回调；完整 `BudgetReport` 存于 `State.KV["_budget"]`，RunFinisher/memx 固化时可直接取用。

## KV 记录结构

```go
// State.KV["_budget"] = map[string]any{
//   "in": 累计输入 tokens,  "out": 累计输出 tokens,
//   "cost": 累计成本(USD),  "turns": 累计模型调用次数,
//   "started": RFC3339Nano 起始时间(仅计时用 ISO 字符串),
//   "warned": 已预警过的资源名(每种资源只预警一次),
//   "wrapup": 已进入收尾模式 bool,
// }
```

JSON round-trip 后数值变 `float64`——读取用与 `middleware/loopguard.go` 的 `interventions()` 相同的容错策略（int/int64/float64）。

**时长语义**：`started` 首见即写（幂等），跨 resume 连续 → **HITL 等待时间计入 wall-clock**。这是保守选择：无人值守 queue 消费正是预算护栏的主场景，需要"这一单总共花了多久"。交互场景若不想让暂停期计入，文档给出按 wave 自理的方案（每 wave 重新 Stream），不再为此增加配置项。

## 配置面

```go
middleware.RunBudget(middleware.RunBudgetOptions{
    MaxInputTokens, MaxOutputTokens, MaxTotalTokens int
    MaxCostUSD   float64            // 需配合 Price 才有意义
    Price        middleware.Price   // 每百万 token 美元价 {InputPerMTok, OutputPerMTok}
    MaxDuration  time.Duration
    MaxTurns     int                // run 终身模型调用数（跨 resume 累计，与 loop 的 per-wave maxTurns 相互独立）
    WarnRatio    float64            // 默认 0.8：达到 cap 的 80% 时预警一次
    Now          func() time.Time   // 测试时钟，同 CircuitOptions.Now 惯例(middleware/circuit.go:58-59)
    OnExceed     func(r middleware.BudgetReport)
})
```

- 全零值 = 不设上限但照常记录用量（纯观测用法，report 进 KV + 事件照常发）。
- **Price 是扁平单表**：多模型/fallback 场景价格不同时成本计算会不准——文档明说，`Price` 留 `func(modelName string)` 扩展余地但不第一版实现（`llm.Model.Name()` 在 `agent.LoopContext` 里拿不到，注入 model 名需要构造期参数，`RunBudget(model, opts)` 工厂显式接收即可——见备选方案 C）。
- 成本折算公式：`cost = in/1e6*Price.Input + out/1e6*Price.Output`。**已知的不精确来源**：流式中途失败的调用，provider 侧已计费但 Usage 未到（`agent/loop.go:256-259` 错误路径直接返回）——记录的是"上报的用量"，不是真实账单，文档必须写明。

## maxTurns 优雅收尾（唯一的 loop 侧改动）

loop 的 `maxTurns` 是 per-wave 步数上限，middleware 看不到它（`agent/loop.go:39,67` 均为私有）。加一个只读字段：

```go
// agent/loopctx.go
type LoopContext struct {
    *RunContext
    Step     int
    MaxTurns int   // 新增：本 loop 的步数上限，供预算/收尾类中间件观察
    Request  *llm.Request
    History  []core.Message
}
```

构造点只有一处（`agent/loop.go:126`），补 `MaxTurns: l.maxTurns` 一行。纯加法，不改任何控制流。

`RunBudgetOptions.NoWrapUpLastTurn bool`（默认启用，只要配置了任一预算）：在 `Step == MaxTurns-1`（本轮就是最后一轮）时，本轮的 `ModifyRequest` 直接进入收尾模式——最后一轮不再执行任何工具，模型被迫以纯文本收束，`ErrMaxTurnsExceeded` 只在模型无视"无工具"约束时发生（最终报错的保障仍在，只是从"预期会发生"降级为"仅在模型不配合时发生"）。step `MaxTurns-2` 及之前的工具照常执行。

实现细节：最后一轮判定放在 ModifyRequest/BeforeTool（`Step == MaxTurns-1` 即时判断），不写 KV；预算越线产生的收尾标志由 AfterModel/BeforeModel 写入 KV["wrapup"]。两者消费同一条置空工具表 + Stop 保障路径。不新增 directive 类型，不复用 Interrupt。

## 与其他模块的关系

- **LoopGuard**（ADR-0023）：KV 键互不冲突（`_loopguard.*` vs `_budget.*`），在同一中间件栈中任意顺序注册均可。LoopGuard 的 Stop 与 RunBudget 的收尾模式互不影响（各自独立触发）。
- **circuit/fallback/retry**：模型层装饰器，与本 run 层中间件互不交叉；fallback 换模型导致价格不准的问题见"配置面"。
- **HITL**：进入收尾模式后 `BeforeTool` 的 Stop 能否被 permission 的 Interrupt 压过，由 `core.Resolve` 优先级定（Stop < Interrupt，permission 在外层栈时仍可拦）——文档给出注册顺序建议：RunBudget 放在 Permission 内侧。
- **queue/worker pool**：预算记录随 checkpoint 落 JSONL，worker 崩溃重领任务后可继续累计（同 ADR-0023 的续跑立场）；但 wall-clock 的 `started` 在长排队后会占用过多时间预算，文档提示长排队场景应在新投递时重置 thread 或仅用 token 预算。

## 范围

### 做
- 上述六类资源的用量记录、预警提醒、三段式收尾执行（ModifyRequest 置空工具表 / 注入收尾指令 / BeforeTool Stop 保障）。
- `LoopContext.MaxTurns` 加法字段 + `WrapUpLastTurn`。
- `core.BudgetExceeded` 事件 + roundtrip 编解码。
- 离线示例 `examples/run-budget`（mock 模型可注入假 Usage，`llm/mock` 目前不回传 Usage——需在 responder 里手工填 `resp.Usage`，示例与测试共用该手法）。

### 不做（理由）
- **不做美元账单级精度**：只认 provider 上报的 Usage，理由见上。
- **不做跨 run/thread 的全局配额**（租户级总额）：那是 queue/外部网关的职责，run 级护栏不越层。
- **不做"预算耗尽转 HITL 请示"**：收尾模式已经是无人值守的正解；要人批的形态用 LoopGuard 的 Interrupt 或 permission 规则表达，不给 RunBudget 增加第二条控制路径。
- **不动 `ErrMaxTurnsExceeded`**：保留为最终错误保障，只把常见路径变成优雅收尾。

## 备选方案

- **A. loop 内建预算阶段**（CheckBudget phase）：违背"控制类能力全部 middleware 化"的既有立场（`agent/middleware.go:10-18`），同 ADR-0023 备选 B，否决。
- **B. 用 llm.Model 装饰器记录用量**：看得到每次调用的 Usage，但摸不到 Steer/ModifyRequest/State.KV，无法预警也无法进入收尾模式，且 resume 后无状态可存。否决。
- **C. 工厂签名收 `llm.Model` 取模型名定价格**：更精确的多模型计价，但把 RunBudget 和具体 model 绑定，fallback 装饰链下仍拿不准实际计费的模型。第一版扁平价 + 文档，留扩展口。
- **D. maxTurns 收尾直接改 loop 报错前多调一次模型**：控制流分裂（loop 里出现"报错前特判"），且拿不到"禁用工具"之外的请求整形机会。否决，用 LoopContext.MaxTurns + 收尾模式复用自然终止更干净。

## 测试锚点

- 记录纯函数：累计/折算/JSON 形态容错（float64、map[string]any）。
- 预警只发一次；每资源独立 warned 位。
- 收尾链路：持续发 tool call 的 mock（但只要 `req.Tools` 为空就交纯文本）→ 断言 run 以 RunDone 收尾、最终消息为收尾文本、`req.Tools` 在某步后恒空。
- 收尾模式保障：mock 在无工具请求下仍回 tool call → BeforeTool Stop → RunDone（非 RunFailed）。
- maxTurns 收尾：`agent.WithMaxTurns(3)` + WrapUpLastTurn，断言不再出现 `ErrMaxTurnsExceeded`。
- 时长：注入 `Now` 假时钟逐步推进。
- 持久化：JSONL 文件 checkpoint 跨 agent 实例 resume，累计值继续累加不重置（复用 loopguard 的跨进程测试手法）。
- 全零值配置 = 纯观测：无预警注入、工具表不被置空、KV 有完整记录。

## 实施切分

1. `core`：`BudgetExceeded` 事件 + 编解码 + roundtrip 测试。
2. `agent`：`LoopContext.MaxTurns` 字段 + 构造点补一行（独立小 commit，向后兼容）。
3. `middleware/runbudget.go`：记录/预警/收尾三段 + OnExceed。
4. 测试（上表全覆盖）+ `examples/run-budget` 离线演示。
