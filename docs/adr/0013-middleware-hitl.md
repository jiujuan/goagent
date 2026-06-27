# ADR 0013：Human-in-the-Loop 作为模型装饰器（工具调用的人工批准 / 拒绝 / 改参）

状态：已接受（沿用 [ADR 0011](0011-middleware-decorator.md) 的中间件机制）

## 背景

很多 agent 场景里，模型决定调用的工具是**不可逆或高风险**的——删文件、发邮件、转账、执行
shell。需要在工具真正执行前让人类介入：批准、拒绝（并把原因反馈给模型）、或修改参数后再放行。

约束来自 turn 引擎的结构（[agent/agent.go](../../agent/agent.go)）：中间件是**模型装饰器**
`func(next llm.Model) llm.Model`，只能包裹 `model.Generate`；而工具是在 `Generate` 返回**之后**
由 `LLMAgent.execTools` 执行的。引擎用 final assistant 消息里的 `ToolCalls()` 决定执行哪些工具，
并据此构建 tool 结果消息——**每个 tool_use 必须有配对的 tool_result**，否则下一轮请求非法。

## 决策

把 HITL 实现为一个中间件 `middleware.HumanInTheLoop`，拦截**带工具调用的 final 响应**，在交给
引擎前按人类裁决改写这条 assistant 消息：

```go
type Decision struct { Approve bool; EditedArgs json.RawMessage; Reason string }
type Approver func(ctx, core.ToolCall) (Decision, error)

func HumanInTheLoop(HITLOptions{ Gate func(core.ToolCall) bool; Approver Approver }) Middleware
func RequireApprovalFor(names ...string) func(core.ToolCall) bool // 只 gate 指定工具
func ConsoleApprover(in, out) Approver                            // CLI 用的内建审批器
```

裁决语义与表示：

- **批准** → 保留该 `ToolCall`，原样交引擎执行。
- **改参批准** → 替换 `ToolCall.Args` 后交引擎执行。
- **拒绝** → 从 assistant 消息里**移除**该 `ToolCall`（引擎根本看不到，绝不产生孤儿 tool_use）。
  拒绝反馈分两种表示：
  - **混合**（同一轮里还有被批准的调用）：把放行的调用交引擎执行，拒绝信息作为一条 user note
    **暂存**，在**下一次**模型调用前注入 `req.Messages`（steering 式 drain-once）。
  - **全部被拒**（本轮无调用幸存）：引擎遇到零工具调用会直接结束 turn、模型无从反应——因此中间件
    **自身续跑**：把原 assistant 消息 + 配对的合成 `RoleTool` 拒绝结果追加进本地工作副本，重新调用
    模型，直到它产出"放行的调用"或"纯文本答复"，引擎只会收到这个最终结果。

`Gate` 为空时对所有调用要审批；`Approver` 必填。审批点尊重 `ctx`（取消即 `FailStream`，对齐
Retry 的取消处理）。

## 理由

- **唯一可行的拦截点**：中间件够不到工具执行层，但够得到那条触发执行的 assistant 消息；改写它就能
  精确控制"哪些工具执行、用什么参数执行"，无需改动 turn 引擎，契合 ADR 0011 的"一种机制统管全部"。
- **wire 始终合法**：被拒调用在引擎看到之前就被删除，不留孤儿 tool_use；全拒绝分支用配对的合成
  tool_result 喂回模型，仍是合法的 tool_use/tool_result 对。
- **拒绝可反馈、可改道**：模型能得知"被人工拒绝及原因"，从而选择别的路径，而非静默失败。

## 后果

- HITL 注入的反馈（user note / 合成 tool_result）只进入**当前 run 的模型上下文**，不作为独立事件
  持久化到 session——与 steering 的取舍一致（[ADR 0011](0011-middleware-decorator.md) 末节）。
  会话里记录的是模型改道后的最终结果，而非被拒的中间往返。
- 全拒绝分支的内层续跑由**人在回路**自然兜底（人不会无限拒绝）；若换成自动化的"恒拒绝"审批器又
  搭配"恒重试"的模型，理论上可能在单次 `Generate` 内打转——这超出引擎 `MaxSteps` 的约束范围，
  使用自动化审批器时需自行保证会收敛。
- 装饰器顺序：HITL 应在 Retry **之外**（重试不应重放人工审批），通常也在 Compaction 之外。

## 备选方案

- **在引擎加 `BeforeToolCall` 钩子**：语义最干净（可直接为被拒调用注入 tool_result），但那是新的
  Config 字段、不是中间件，破坏"能力即中间件"的统一抽象；故不采用。
- **把 HITL 做成工具包装器**：只能改单个工具、无法跨工具统一策略，也拿不到"同一轮多调用"的整体视图。
