# ADR-0029: 工具执行超时 —— agent 级默认 + ToolContexter 可选接口 + 取消并放弃等待

- 状态: Accepted（**已全部实施**：TO-01 `edfde88`：`WithToolTimeout` + `ToolContexter` + `Stack.ToolContext` + `keepToolUpdates`；TO-02 `b9e751b`：放弃等待 + `abandonGrace` + 超时/取消两种合成结果；TO-03 前置 `8348a3e` + `1f6b6a3`：`middleware.ToolTimeout`；TO-04 `3d4aee5`：`examples/tool-timeout`；TO-05 `b9d71a2` + `be482a9`：写工具的人 / 读事件流的人两个补充示例（见第 10 节）。TO-01…TO-04 已在 `origin/main`；TO-05 两个 commit 未推送）
- 日期: 2026-10-02
- 关联: ADR-0023(LoopGuard)、ADR-0024(RunBudget)、ADR-0025(Compaction persist)、ADR-0030(参数拒绝计数，依赖本 ADR 的超时分类)
- 执行卡: `docs/tasks/plan-tool-execution-timeout.md`

## 背景

工具循环对被调用的工具**没有任何时间约束**。`agent/exectools.go:89-117` 的 `callOne` 把 run 的上下文原样交给 handler：

```go
tctx := &tool.Context{Context: rc, State: rc.State, CallID: c.ID}   // exectools.go:110
res, err := t.Call(tctx, raw)                                        // exectools.go:111
```

`rc` 就是 `*RunContext`（内嵌 `context.Context`，`agent/runtime.go:22-23`），因此工具收到的取消信号只有"整个 run 被取消"这一种。`agent/` 包内没有 `context.WithTimeout` / `context.WithDeadline` 的任何调用点（全仓 grep 确认）。

后果落在两处，都会让 run 无限期停住：

1. `execTools` 的并发分支 `wg.Wait()`（`agent/exectools.go:71-80`）与串行分支的顺序循环（`:63-69`）都不带 deadline。一个不返回的工具就阻塞整批，后面的步骤不再执行。
2. `tool.Tool` 契约（`tool/tool.go:18-23`）里没有任何与时间有关的字段，`agent` 的 `config`（`agent/options.go:14-38`）与 `With*` 选项（`:43-111`）也没有。调用方只能在 `Agent.Run(ctx, ...)`（`agent/agent.go:122`）之前给一个总的 ctx，粒度是整个 run。

已经自带超时的只有个别工具，且各自独立、不是循环的能力：

| 位置 | 机制 |
| --- | --- |
| `tool/mcp/adapter.go:55`、`tool/mcp/mcp.go:38-41` | `mcp.WithTimeout` 约束握手与每次远程调用 |
| `tool/web/web.go:68,77` | `http.Client{Timeout: 15s}` |
| `sandbox/sandbox.go:42-43`、`sandbox/process/process.go:63-65` | `Policy.Timeout` 走 `exec.CommandContext` 杀进程，`Outcome.TimedOut` 标记（`sandbox/sandbox.go:68-69`）；`tool/exec.RunCommand`（`tool/exec/exec.go:31`）经沙箱间接受益 |
| `middleware/runbudget.go:48,265` | `MaxDuration` 约束整轮挂钟，但只在 `BeforeModel`/`AfterModel` 检查（`:159-170`），无法打断正在执行的工具 |

## 名词

- **每次调用超时(per-call timeout)**：一次工具调用允许占用的最长 wall-clock 时间。
- **协作式取消(cooperative cancellation)**：工具在自己的执行里检查传入 context 的 `Done`，收到取消后尽快返回。Go 无法强制终止一个已在运行的函数。
- **放弃等待(abandon the wait)**：到点后循环不再等这个工具返回，改为合成一条错误结果继续走；工具协程留在后台，其迟到产出全部丢弃。

## 决策概览

三件事，落在三个不同层，互不越界：

| 层 | 加什么 | 作用 |
| --- | --- | --- |
| agent 配置 | `WithToolTimeout(d time.Duration)` | 本 agent 所有工具调用的默认上限，0 表示不设（默认值，保持现有行为不变） |
| agent 扩展点 | 可选能力 `ToolContexter`（`agent/middleware.go`，与 `ModelContexter` 写法一致） | 让中间件按工具名派生/收紧本次调用的 context；循环只提供可选接口，不含策略 |
| agent 执行 | `execTools`/`callOne` 改为"取消 context + 放弃等待" | 到点必然结束本批的等待，写一条合成错误结果，迟到协程的结果被丢弃 |

立场与仓库既有设计一致：**循环提供可选接口，策略放在选项与中间件**。`ModelContexter`（`agent/middleware.go:46-55`）和 `HistoryCompacter`（`:57-68`）已经确立了"可选接口 + Stack 按注册顺序依次调用 + 未实现即 no-op"这套做法。

## 1. agent 级默认超时

```go
// agent/options.go
func WithToolTimeout(d time.Duration) Option { return func(c *config) { c.toolTimeout = d } }
```

- `config.toolTimeout time.Duration`（`agent/options.go:14-38` 内）→ `newLoop` 写入 `AgentLoop.toolTimeout`（`agent/loop.go:43-70`）。
- 默认 0 = 关闭。理由：现有部署里长时间工具（部署等待、子 agent 作为工具 `agent/subagent.go:23`）是刻意用法，默认开启会改变既有行为；本仓库新增防护中间件一律 opt-in（ADR-0023/0024/0025 同此）。
- 语义是**每次调用**，不是每批：并发批次里每个调用各自计时；串行模式（`WithToolExecution(ToolSequential)`，`agent/options.go:113-121`）下一批 N 个慢工具最多消耗 N×d。整轮时间上限由 `RunBudget.MaxDuration` 负责，两层正交。

## 2. ToolContexter 扩展点

```go
// agent/middleware.go（紧跟 ModelContexter 之后）
type ToolContexter interface {
    ToolContext(lc *LoopContext, ctx context.Context, call *core.ToolCall) (context.Context, context.CancelFunc)
}
```

`Stack.ToolContext(lc, ctx, call)` 按注册顺序依次派生子 context，未实现该接口的中间件跳过 —— 与 `Stack.ModelContext`（`agent/middleware.go:123-130`）写法逐字一致。

**签名带 cancel（`8348a3e` 修正，写卡时没料到）**：中间件派生的往往是 `context.WithTimeout`，而一个定时器会挂在父 context 上，直到被 cancel 或父结束——这里的父就是 run 的 context。若能力接口只回传 context，每笔调用都会留下一个没人收的定时器，一次长 run 攒几百个。所以接口回传 `(context, CancelFunc)`：`Stack.ToolContext` 按注册顺序倒序调用各家交回的 cancel（回 nil 表示无可释放），`toolCallCtx` 再把它与 agent 默认那一层的 cancel 串成一个，`execTools` 每笔调用 defer 一次。这也是 `middleware.ToolTimeout` 的落点——它把 cancel 交回循环，自己不持有。

调用点在 `callOne` 构造 `tool.Context` 之前（`agent/exectools.go:110`）。**它会在批次的工作协程里被并发调用**，实现必须协程安全；这与 `AfterTool` 已经写明的并发约束相同（`agent/middleware.go:20-21`）。

组合顺序（固定，写进注释）：

```go
ctx, mwCancel := l.mw.ToolContext(lc, rc.Context, &c)   // 中间件先派生（span、按工具的更紧上限）
if l.toolTimeout <= 0 {
    return ctx, mwCancel
}
callCtx, deadlineCancel := context.WithTimeout(ctx, l.toolTimeout)   // agent 默认最后套上
return callCtx, func() { deadlineCancel(); mwCancel() }              // 倒序释放
```

由此得出一条必须写进文档的限制：**这个接口只能收紧、不能放宽**。Go 的 context 只能携带更早的 deadline；若 agent 默认 60s、中间件想给某工具 300s，做不到 —— 那种需求应改为 `WithToolTimeout(0)` 关掉默认，再用中间件给出每个工具的上限。备选方案一节记录了为什么不为此引入"每工具映射"。

工具侧无需改动：`tool.Context` 内嵌 `context.Context`（`tool/tool.go:43-47`），已尊重 ctx 的工具（`tool/exec/exec.go:31`、`tool/mcp/adapter.go:55`）自然收到取消。

**派生 context 必须保住 `tool.Updater`**（TO-01 实现期发现）。`Context.Update` 是在工具拿到的那个 context 上查找 `tool.Updater` 能力的（`tool/tool.go:65-72`），而 `*RunContext` 正是该能力的实现（`agent/exectools.go` 的 `UpdateTool`）。一旦循环把 rc 包进 `WithTimeout` 或中间件返回的派生 context，能力查找就落空，长任务上报的 `core.ToolUpdate` 会被静默丢弃。因此 `callOne` 构造 `tool.Context` 时用 `keepToolUpdates` 把 run 的 Update 能力重新挂到派生 context 上（`updaterContext`，转发 `UpdateTool`）；context 本身就是 rc（没设超时、也没有中间件改写过）时不包一层，保持原状。这条约束对 TO-02 之后的所有用法都成立：任何给工具换 context 的中间件都要经过同一处。

## 3. 取消并放弃等待

现状是共享切片写入：`execTools` 预分配 `results`/`dirs`（`agent/exectools.go:22-23`），每个调用在工作协程里写 `results[i]`、`dirs[i]`，并用 `stateMu` 串行化 `rc.State.Apply`（`:24-31`）。放弃等待会让"协程在循环已经前进之后继续写"成为真实可能，因此必须改掉这种写法。

改为每调用一个带缓冲的通道：

```go
type toolOutcome struct {
    tr      core.ToolResult
    control *core.Directive
    ops     []core.StateOp
}

ch := make(chan toolOutcome, 1)          // 容量 1：被放弃的协程写入后即可退出，永不阻塞
go func() { ch <- l.callOne(rc, callCtx, c) }()

start := time.Now()
var out toolOutcome
select {
case out = <-ch:
case <-callCtx.Done():
    out = awaitAbandoned(ch, callCtx, c, start)   // 宽限窗口内仍收真实结果，否则合成
}
```

要点：

- **真实结果优先，靠一个有界的宽限窗口**：`Done` 之后循环再等 `abandonGrace`（常量 2ms）才改口。盯着取消信号的工具，它的错误几乎总在同一个瞬间之后立刻到达，那条错误比循环的措辞更有信息量。实测记一笔：先写成非阻塞排空一次，连跑 5 次全是循环抢先、handler 的真实错误被丢掉；把窗口设为 0 之后 `TestToolTimeoutCancelsCooperativeTool` 稳定失败，因此改成带定时器的等待。代价只落在真正卡死的调用上：每调用至多多等 2ms。
- **合成结果沿用既有结构**：`core.ToolResult{CallID, Name, IsError: true, Content: []core.Part{core.Text{...}}}`，与 `errResult`（`agent/exectools.go:119-124`）、`truncatedResults`（`agent/loop.go:359-373`）一致。文案给出实测耗时与下一步建议，风格对齐 `sandbox` 的 `[timed out after %s]`（`tool/exec/exec.go:56`）。
- **AfterTool 只对合成结果调用一次**：`run(i, c)` 的收尾（AfterTool、`results[i]`、`core.ToolDone` 发布，`agent/exectools.go:39-47`）在 select 之后统一执行一次。因此被放弃的工具迟到返回时，其结果、`Result.Control`、`Result.State` 三项全部丢弃，AfterTool 也不再触发。
- **可观测性因此保持完整**：`obs/otel` 的 AfterTool 按 CallID 结束 span 并记 `error=true`（`obs/otel/otel.go:265-282`）。已知不精确之处：`agent.tool.duration` 直方图记下的是超时上限值，不是工具真实耗时（真实耗时没有上限）。
- **run 被取消与超时共用一条 Done 分支**，按 `callCtx.Err()` 区分文案：`context.DeadlineExceeded` → 超时；`context.Canceled` → "run 已取消"。两种都不引入新控制流：循环照常追加结果，下一次模型调用因 provider 侧 ctx 取消而由既有 `l.fail`（`agent/loop.go:322-331`）结束。
- **协程泄漏有界**：`cancel` 在 `run` 返回时执行（defer），被放弃的协程立即看到 ctx 结束；它的 ctx 派生自 `rc.Context`，run 结束/`Run.Cancel` 时同样收到取消。不检查 ctx 的工具仍会跑到自己完成 —— 这是 Go 语言本身的限制，本 ADR 只保证**循环**不再等待，不保证**副作用**被中止。工具若有写外部资源的副作用，需要自己尊重 ctx；这条限制写进 `WithToolTimeout` 的文档注释。
- **迟到的 `core.ToolUpdate`**：`RunContext.UpdateTool` 的注释已经声明"未 join 的工作协程可能在 ToolDone 之后再发 ToolUpdate"（`agent/exectools.go:126-135`）。放弃等待会显著提高这种情况的概率，观察者需按 CallID 自行丢弃；本 ADR 不加新机制。
- **恢复路径自动继承**：`runResumed`（`agent/hitl.go:142-165`）内部同样调用 `l.execTools`（`:151`），HITL 批准放行的一批同样受超时约束，无需额外分支。

`callOne` 的签名相应变化：新增 `callCtx context.Context` 参数，`tool.Context` 用它而不是 `rc`（其余解析与校验顺序不变，`agent/exectools.go:89-117`）。

## 4. middleware.ToolTimeout（可选，收紧用）

`ToolContexter` 的第一个实现：

```go
middleware.ToolTimeout(middleware.ToolTimeoutOptions{
    Default   time.Duration            // 批次内所有工具
    PerTool   map[string]time.Duration // 按工具名，优先于 Default
    Exempt    []string                 // 不设限（等价于跳过该调用）
})
```

`ToolContext` 交出派生的 context 与它的 cancel；判定这笔调用超时并改口的是循环（见第 3 节），中间件只设时限。

零值即全部不设限。它给"不同工具不同上限"提供了不触碰 agent 配置的入口，也给出与 `WithToolTimeout` 的组合方式：两者都在，效果取更早的那个 deadline。中间件工厂不加 `New` 前缀，符合仓库例外约定（`middleware/compaction.go:56`、`middleware/circuit.go:74`）。

## 5. 与既有防护中间件的关系

- **LoopGuard**：合成结果进入历史，`error_streak` 规则（`middleware/loopguard.go:292-319`）会在连续 3 条相同超时文案时命中并升级 —— 这是有意的组合：反复超时正是卡死形态之一。豁免名单同样适用。
- **RunBudget**：`MaxDuration` 管整轮挂钟并在超限时进入 wrap-up；本 ADR 管单次调用。两者不重叠也不互相削弱。
- **Compaction**：无关。
- **ADR-0030**：超时被记为一类拒绝（`RejectTimedOut`）供参数拒绝计数器使用，但本 ADR 本身不引入分类；执行卡按 0029 → 0030 顺序排。

## 6. core 改动

**无**。超时以既有 `core.ToolResult{IsError: true}` + `core.ToolDone` 表达（同 `truncatedResults` 的做法，`agent/loop.go:359-373`），checkpoint、事件 JSON 编解码、`Stream`/`Wait` 消费者全部不动。

## 7. 备选方案

- **A. 工具自声明 `Timeout()` 能力**（对标 `SequentialTool`，`tool/tool.go:86-94`）：配置随工具走，但无法按 run/agent 覆盖，也无法在不动工具源码的情况下给第三方工具加限制。否决。
- **B. 只做 `WithToolTimeout`，不加 `ToolContexter` 接口**：改动最小，但没有每工具上限的表达入口，且 `obs/otel` 这类需要把 per-call span 放进工具 ctx 的观察者仍无处接线（它现在的 `BeforeTool` 只存 span，不改 ctx，`obs/otel/otel.go:250-263`）。否决。
- **C. 纯协作式取消（只传 ctx，不放弃等待）**：实现最省，但不检查 ctx 的工具仍会把整批乃至整个 run 卡死 —— 也就是背景第 1 条主症状没有解决。否决；这是本 ADR 选择"放弃等待"的唯一理由，代价（迟到结果丢弃、协程泄漏、耗时指标失真）在文档与测试中显式承担。
- **D. 把超时做成循环的固定阶段/内建默认**：违背"循环阶段稳定、策略表达为中间件"的既有立场（`agent/middleware.go:10-18`），且会静默改变既有 agent 的行为。否决。

## 8. 成本

每个调用多一次 `context.WithTimeout` 与一次 select；无模型调用，无额外内存驻留。合成文案 O(1)。

## 9. 测试锚点

- 不检查 ctx 而阻塞的工具：设 50ms 上限，断言批次结束等待、历史里是超时错误结果、`ToolDone` 恰好一次、run 能继续到最终答复。
- 尊重 ctx 的工具：断言它返回的错误被优先采用，而不是合成文案。
- 快工具：断言无任何行为变化（`WithToolTimeout(0)` 与不设两种都覆盖）。
- 串行批次：一个阻塞工具不拖住同批后续调用。
- 并发批次：`go test -race`。
- run 取消 vs 超时：两种 `callCtx.Err()` 的文案分支。
- 恢复路径：`PendingHITL` 批准后放行的慢工具被超时（复用 `agent/resume_exec_test.go`、`agent/durable_test.go` 的机制）。
- 被放弃协程的 `Result.State` 与 `Control` 确实不生效（工具在超时后才返回带 ops 的结果，断言 KV 未被改写）。
- `middleware.ToolTimeout` 与 `WithToolTimeout` 同时存在时取更早 deadline；`Exempt` 命中时不限时。

## 10. 可运行示例（三个视角）

三个示例都是 `package main` + `llm/mock`，无网络、无密钥，可重复执行；实测时长见各行。

| 视角 | 命令 | 教什么 |
| --- | --- | --- |
| 配置与循环行为 | `go run ./examples/tool-timeout`（约 7.0s，`3d4aee5`） | 上限设在哪（agent 默认 / `middleware.ToolTimeout` 的 Default、PerTool、Exempt）；到点后循环怎么做；被限住的工具还能不能上报进度 |
| 写工具的人 | `go run ./examples/tool-timeout-author`（约 3.0s，`b9d71a2`） | 每步副作用之前看一次 context；被中断时回报"做到哪了"而不是只交出 `context.Canceled`；做不到就把副作用设计成幂等——同批两种写法对照，并打印被放弃的调用在 run 结束前后各写了多少行 |
| 读事件流的人 | `go run ./examples/tool-timeout-consumer`（约 7.9s，`be482a9`） | 按 CallID 记账，一条调用只在 `ToolDone` 结算一次；`ToolDone` 之后仍到达的 `ToolUpdate` 要丢弃（场景 B 同批留一个上限更宽的调用，让 run 活得比被放弃的那条更久，迟到上报才真的可见）；`IsError` 为真不显示成完成态；超时 ≠ 已取消/已回滚 |

- consumer 的 `classify` 是全示例唯一读文案的地方：`core.ToolResult` 没有"这句话出自谁"的字段，循环代答的两句措辞目前只能靠文本识别。循环内部另有机器可读的分类（ADR-0030 §1 的 `RejectClass`，含 `timed_out`），但 ADR-0030 明确否决了把它加到 `core.ToolResult` 上（其备选方案 A），所以事件流消费者要区分"工具自报"与"循环代答"仍只能读文案；要改也只改 `classify` 这一处。

