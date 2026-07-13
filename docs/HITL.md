# Human-in-the-Loop 中间件

## Context

goagent 的 `middleware` 把所有横切能力统一为**模型装饰器** `func(next llm.Model) llm.Model`（见 [ADR 0011](docs/adr/0011-middleware-decorator.md)）：装饰器拦截 `Generate` 调用，改写请求（compaction/RAG/steering）或控制调用（retry/ratelimit）。目前缺少一个 **Human-in-the-Loop（HITL）** 能力：在模型决定调用工具后、turn 引擎真正执行工具前，让人类介入**批准 / 拒绝 / 修改参数**。这对"危险操作需人工确认"（删文件、发邮件、转账、执行 shell）是刚需。

关键约束（来自代码走读）：
- `Middleware` 只能包裹 `model.Generate`，**无法直接拦截工具执行**——工具由 `agent.LLMAgent.execTools` 在 `Generate` 返回之后调用（[agent/agent.go:182-188](agent/agent.go)）。
- 因此 HITL 唯一的可拦截点是**带工具调用的 final 响应**：引擎用 `final.ToolCalls()`（[core/message.go:101](core/message.go)）决定执行哪些工具，并据此构建 tool 结果消息。中间件改写这条 assistant 消息，就能精确控制"哪些工具会被执行、用什么参数执行"，且不破坏 tool_use/tool_result 配对（被删的 call 引擎根本看不到，不会产生孤儿）。
- 不改动 turn 引擎，纯中间件实现，符合 ADR 0011 的"一种机制统管全部"。

## 设计：`middleware.HumanInTheLoop`

新增 `middleware/hitl.go`。核心是一个装饰器，拦截 `Generate` 的响应流：透传 partial（保留流式渲染），捕获 final；若 final 含需审批的工具调用，则**阻塞**调用 `Approver` 征询人类，按决定改写 assistant 消息后再交给引擎。

### 公开 API

```go
// Decision 是人类对一次工具调用的裁决。
type Decision struct {
    Approve    bool            // 是否放行执行
    EditedArgs json.RawMessage // Approve 时若非 nil，用它替换调用参数
    Reason     string          // 拒绝原因（反馈给模型）
}
func Approve() Decision
func ApproveWithArgs(args json.RawMessage) Decision
func Deny(reason string) Decision

// Approver 对每个需审批的工具调用阻塞征询。必须尊重 ctx（取消时尽快返回）；
// 返回 error 则整个调用失败（走 FailStream）。
type Approver func(ctx context.Context, call core.ToolCall) (Decision, error)

type HITLOptions struct {
    Gate     func(core.ToolCall) bool // 哪些调用需审批；默认全部
    Approver Approver                 // 必填
}

func HumanInTheLoop(opts HITLOptions) Middleware

// 便捷：只对指定名字的工具要审批（最常见的"仅危险工具"场景）。
func RequireApprovalFor(names ...string) func(core.ToolCall) bool

// 便捷：从 io.Reader/io.Writer 构造一个命令行 Approver（用于 CLI / 示例 / 测试）。
func ConsoleApprover(in io.Reader, out io.Writer) Approver
```

### 拦截与裁决逻辑（`HumanInTheLoop` 返回的装饰器）

用 `Wrap`（[middleware/middleware.go:36](middleware/middleware.go)）构造，`gen` 内部：

1. **内层循环**消费 `next.Generate(ctx, work)`（`work` 是 `req` 的本地副本，避免改动引擎的 history slice）：
   - `resp.Partial` → 直接 `yield` 透传；
   - 非 partial → 记为 `final`，**先不 yield**。
   - 中途 `err` → `yield(nil, err)` 并返回。
2. 取 `final.Message.ToolCalls()`，按 `Gate` 过滤出 `gated`。`gated` 为空 → `yield(final)` 返回（无审批，零开销路径）。
3. 对每个 `gated` 调用顺序征询 `Approver`（每次先查 `ctx.Err()`，取消则 `FailStream(ctx.Err())`）：
   - **Approve** → 保留该 `ToolCall`（若 `EditedArgs` 非 nil 则替换 `Args`）；
   - **Deny** → 从 assistant 消息中**移除**该 `ToolCall`，把 `(name, reason)` 收进 `denials`。
   - 未 gate 的 call 原样保留。
4. 重建 `final.Message.Parts` → `rewritten`。设 `remaining = rewritten.ToolCalls()`。
   - `denials` 非空时构造一条**用户角色反馈消息** `denialNote`：`"[系统] 以下工具调用被人工拒绝，请勿重试：tool(reason); ..."`（沿用 compaction/steering 用 user 角色注入合成消息的先例）。
   - **若 `remaining` 非空**（有放行/改参的调用要执行）：`yield(rewritten)` 交引擎执行；把 `denialNote` **暂存**，在**下次** `Generate` 时追加到 `req.Messages`（steering 式 drain-once），让模型在拿到工具结果的同时看到拒绝反馈。返回。
   - **若 `remaining` 为空**（全部被拒）：引擎遇到零工具调用会直接结束 turn、模型无从反应——因此中间件**自身续跑**：把 `final.Message`（原始、含被拒 call）+ 一条合成的 `RoleTool` 拒绝结果消息（每个被拒 call 一个 `ToolResult{IsError:true, Content: reason}`，CallID 配对）追加到 `work.Messages`，`continue` 回到第 1 步重新调用模型。模型据此改走别的路径，引擎最终只会收到"放行的工具调用"或"纯文本答复"。

> 全拒绝分支用合成 `ToolResult` 配对原 `ToolCall`（wire 合法）；混合分支用 user note。两者都只活在中间件本地 `work` 副本里，**不作为独立事件持久化到 session**——与 steering 的取舍一致（[middleware/steering.go:17-19](middleware/steering.go)），ADR 中会写明此后果。

### 与现有约定的一致性
- 取消处理对齐 retry（[middleware/retry.go](middleware/retry.go)）：阻塞点尊重 `ctx`，取消即 `FailStream`。
- 选项默认值风格对齐 retry/compaction：`Gate` 缺省为"全部 gate"，`Approver` 缺省 panic 提示必填（或返回构造期校验）。
- Go 1.23 习惯用法（`for range`、`min` 等，来自 modern-go）。

## 改动文件
- **新增** `middleware/hitl.go` — 上述实现。
- **新增** `middleware/hitl_test.go` — 见验证。
- **新增** `docs/adr/0013-middleware-hitl.md` — 记录"HITL 作为模型装饰器"的决策、拒绝反馈两种表示、不持久化的后果（沿用 ADR 0011/0007 的中文 ADR 体例）。
- **新增** `examples/hitl/main.go` — 一个危险工具（如 `delete_file`）+ `RequireApprovalFor("delete_file")` + `ConsoleApprover`，演示批准/拒绝/改参三条路径（参照 [examples/workflow/main.go](examples/workflow/main.go) 的写法与中文注释风格）。
- **更新** `README.md` 的中间件清单，列入 HumanInTheLoop（紧随 Steering）。

## 验证

`middleware/hitl_test.go`（`package middleware_test`，复用 `mock` + `runner` + `agent`，仿 [middleware/steering_test.go](middleware/steering_test.go)）：

1. **Approve 放行**：mock 第一轮 `mock.CallTool` 调危险工具，Approver 恒 `Approve`；断言工具真的执行（工具体置标志位 / 返回值进入下一轮），最终答复正常。
2. **Deny 全拒绝→模型改道**：Approver 恒 `Deny("not allowed")`；mock 看到拒绝（`work` 中出现拒绝结果/note）后改返回纯文本；断言危险工具**未执行**、最终文本来自改道分支、引擎未收到孤儿 tool_use。
3. **Edit 改参**：Approver 返回 `ApproveWithArgs`；断言工具收到的是**改后**参数。
4. **混合**：两个并发工具调用，一个 Approve 一个 Deny；断言放行的执行、被拒的没执行、且下一轮请求里出现 denialNote。
5. **ctx 取消**：Approver 阻塞期间取消 ctx；断言 `Generate` 以 ctx 错误结束、工具未执行。
6. **零开销路径**：`Gate` 返回 false 时，行为与无中间件完全一致（透传）。

命令：
```
cd D:/codeproject/mygo/goagent
go test ./middleware/...      # 单测
go vet ./...
go run ./examples/hitl        # 手动体验 CLI 批准流
```
