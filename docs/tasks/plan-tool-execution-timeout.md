# 实施方案：工具执行超时（ADR-0029）

- 范围：`agent/options.go`、`agent/loop.go`、`agent/middleware.go`、`agent/exectools.go`、`middleware/`、`examples/`
- 进度：**全部完成** — TO-01 `edfde88`、TO-02 `b9e751b`、TO-03 `8348a3e`+`1f6b6a3`、TO-04 `3d4aee5`（四个已在 `origin/main`）、TO-05 `b9d71a2`+`be482a9`（未推送）
- 日期：2026-10-02
- 设计文档：`docs/adr/ADR-0029-tool-execution-timeout.md`
- 前置事实（逐条 grep 核实）：
  - `callOne` 把 run 的上下文原样交给 handler：`tctx := &tool.Context{Context: rc, State: rc.State, CallID: c.ID}`（`agent/exectools.go:110`），`t.Call(tctx, raw)`（`:111`）。`agent/` 包内没有任何 `context.WithTimeout` / `context.WithDeadline`（全仓 grep 无该包命中）。
  - `execTools` 的并发分支用 `wg.Wait()` 收束（`agent/exectools.go:71-80`），串行分支顺序循环（`:63-69`），两处都没有 deadline。
  - 结果与指令写在共享切片上：`results := make([]core.Part, len(calls))`、`dirs := make(...)`（`:22-23`），State 改写由 `stateMu` 串行（`:24-32`），批次收尾是 AfterTool → `results[i]`/`dirs[i]` → `core.ToolDone`（`:39-47`）。
  - `RunContext` 内嵌 `context.Context`（`agent/runtime.go:22-23`），`tool.Context` 也内嵌它（`tool/tool.go:43-47`）→ 工具已能收到取消信号，只是今天只有"整个 run 取消"这一个来源。
  - 已有的可选接口按注册顺序依次调用的先例：`ModelContexter`（`agent/middleware.go:46-55`）+ `Stack.ModelContext`（`:123-130`），`HistoryCompacter`（`:57-68`）+ `Stack.CompactHistory`（`:136-143`）。
  - 不执行 handler 却仍要给出结果的先例（写法相同）：`truncatedResults`（`agent/loop.go:359-373`）与 `errResult`（`agent/exectools.go:119-124`）。
  - `AgentLoop` 每 run 构造一次、中间件栈随之建好（`agent/loop.go:43-70`，`mw: NewStack(...)` 在 `:63`）；`config` 与 `With*` 在 `agent/options.go:14-38`、`:43-111`。
  - HITL 恢复批次同样经过 `execTools`：`runResumed` → `l.execTools(rc, lc, approved)`（`agent/hitl.go:151`）。
  - `obs/otel` 在 AfterTool 按 CallID 结束工具 span 并记 `error` 属性与计数/耗时（`obs/otel/otel.go:265-282`，instruments 定义 `:117-119`）。
  - 沙箱的超时文案样式：`[timed out after %s]`，用实测 `out.Duration`（`tool/exec/exec.go:56`）。
  - `runResumed` 自己构造 `LoopContext`（`agent/hitl.go:143`）→ 新参数从 `lc` 取即可，不需要额外 plumbing。
  - Go 版本 `go 1.25.0`（`go.mod:3`）。

## 总设计立场（四个 TASK 共用）

1. **默认关闭**。`WithToolTimeout` 零值 = 不设限；不装新中间件时，除下面明确列出的行为变化之外，现有 agent 行为完全不变。
2. **循环只提供可选接口，策略放在选项与中间件**。`ToolContexter` 与 `ModelContexter` 写法逐字一致：未实现即跳过，合并不产生任何副作用。
3. **零 `core` 改动**。超时以既有 `core.ToolResult{IsError: true}` + `core.ToolDone` 表达，checkpoint 与事件 JSON 不动。
4. **超时文案用实测耗时**，不引用配置值。谁设的 deadline 都能报出同一个数（agent 默认与中间件同时存在时生效的是更早的那个）。
5. 每个 TASK 结束时仓库可编译、全测可跑；提交切分见文末。

---

## TASK-TO-01：选项 + ToolContexter 可选接口 + 协作式取消

### 改动

**`agent/options.go`**

- `config` 增字段 `toolTimeout time.Duration`（放在 `toolExec` 之后，`:24` 附近）。
- 新增：

```go
// WithToolTimeout bounds how long a single tool call may run (default 0 = no bound).
// It is per call, not per batch: a sequential batch of N slow tools can take N*d.
// Two things happen when it fires: the context handed to the tool is cancelled, and
// (TO-02) the loop stops waiting for it. A tool that ignores its context is not
// interrupted mid-execution — Go cannot kill a running function — so its external
// side effects continue even though the run has moved on. Total wall-clock for a
// whole run is RunBudget.MaxDuration's job, not this one's.
func WithToolTimeout(d time.Duration) Option { return func(c *config) { c.toolTimeout = d } }
```

**`agent/loop.go`**

- `AgentLoop` 增字段 `toolTimeout time.Duration`（`:26-41`），`newLoop` 里 `toolTimeout: c.toolTimeout`（`:54-69`）。

**`agent/middleware.go`**

- `ModelContexter` 之后新增能力接口，文档注释写明**并发调用**约束（与 `:20-21` 的 AfterTool 同语气）：

```go
// ToolContexter is an optional middleware capability. If a middleware
// implements it, the loop calls ToolContext to derive the context.Context
// handed to one tool invocation, so a middleware can attach a span or give a
// particular tool a tighter deadline than the agent's default. The core holds
// no timeout policy of its own; it only offers this seam. Middleware that does
// not implement it is skipped in the fold (Stack.ToolContext), so behaviour is
// unchanged.
//
// ToolContext is called once per tool call, from the batch's worker goroutine,
// so an implementation must be goroutine-safe.
type ToolContexter interface {
    ToolContext(lc *LoopContext, ctx context.Context, call *core.ToolCall) context.Context
}
```

- `Stack.ToolContext(lc *LoopContext, ctx context.Context, call *core.ToolCall) context.Context`，逐字仿 `Stack.ModelContext`（`:123-130`）：按注册顺序把上一个的结果 threaded 给下一个，无人实现时原样返回 `ctx`。

**`agent/exectools.go`**

- 新增私有 helper：

```go
// toolCallCtx derives the context one tool call runs under: middleware first
// (a span, or a per-tool bound), the agent's default deadline last. Only
// tightening is expressible — a context can carry an earlier deadline, never a
// later one, so widening past WithToolTimeout means setting it to 0 and
// configuring middleware.ToolTimeout instead.
func (l *AgentLoop) toolCallCtx(lc *LoopContext, c *core.ToolCall) (context.Context, context.CancelFunc)
```

 实现：`ctx := l.mw.ToolContext(lc, lc.RunContext.Context, c)`；`if l.toolTimeout > 0 { ctx, cancel = context.WithTimeout(ctx, l.toolTimeout); return ctx, cancel }`；否则 `return ctx, func() {}`（返回 no-op cancel，调用方一律 defer，无分支）。

- `callOne` 签名改为 `func (l *AgentLoop) callOne(lc *LoopContext, callCtx context.Context, c core.ToolCall) (...)`：函数体内 `rc.dynamic` → `lc.dynamic`、`rc.State` → `lc.State`（`LoopContext` 内嵌 `*RunContext`，`agent/loopctx.go:13-20`），`tctx` 用 `callCtx`：`&tool.Context{Context: keepToolUpdates(callCtx, lc.RunContext), State: lc.State, CallID: c.ID}`。
- **实现期补充（写卡时未预见）**：`Context.Update` 靠工具拿到的那个 context 查找 `tool.Updater`（`tool/tool.go:65-72`），`*RunContext` 就是该能力的实现（`UpdateTool`，`agent/exectools.go`）。派生出带 deadline 的 context 会让查找落空、`core.ToolUpdate` 全被丢掉，因此新增 `updaterContext`（转发 `UpdateTool`）与 `keepToolUpdates`（context 已具备能力时不包一层）。透传点放在 `callOne` 构造 `tool.Context` 处，`toolCallCtx` 只管派生与超时。
- `execTools` 的工作函数 `run(i, c)` 里：`callCtx, cancel := l.toolCallCtx(lc, c); defer cancel()`，再 `l.callOne(lc, callCtx, c)`。**本 TASK 不改动等待方式**，串行/并发分支保持原样。

### 验收

- `go build ./... && go vet ./...`；`gofmt -l` 会列出既有文件（本卡要改的 `agent/exectools.go`、`agent/options.go` 就在其中，行尾是 CRLF），这是既有现象，不要为了格式化通过而重排既有文件；新增文件保持与相邻文件一致的写法即可。
- 新增 `agent/tooltimeout_test.go`：
  - `TestToolTimeoutCancelsCooperativeTool`：工具 handler 内 `<-ctx.Done()` 后返回 `ctx.Err()`；`WithToolTimeout(30*time.Millisecond)` + `mock` 模型先调用工具、再收到错误结果后给出无工具答复。断言最终答复正常返回、历史里的结果文本含该错误、handler 确实在 30ms 量级被叫醒（用耗时上界断言，不用 `time.Sleep` 猜）。
  - `TestToolTimeoutZeroLeavesRunUntouched`：不设选项时行为与既有测试一致（用一个立即返回的工具，断言结果文本没有超时字样）。
  - `TestStackToolContextFoldsInOrder`：两个 `ToolContexter`（往 ctx 里塞不同的值）依次生效，未实现该接口的中间件被跳过。可直接测 `NewStack(...)`，不必起循环。
  - `TestToolCallCtxTakesEarlierDeadline`：两个方向各一例（中间件 200ms + agent 默认 5s；agent 200ms + 中间件 5s），由工具自己读 `Deadline()` 上报剩余时长，断言生效的是更早的那个（< 1s）。
  - 执行期实际多写的两条（均已在实现上做过"去掉这行代码用例必须失败"的反向验证）：
    - `TestToolUpdateSurvivesDerivedDeadline`：设了超时、且中间件改写过 context 之后，长任务的 `tctx.Update` 仍能产出 `core.ToolUpdate`（覆盖 `keepToolUpdates`）。
    - `TestToolTimeoutReachesInjectedTool`：由中间件用 `LoopContext.AddTool` 注入的工具同样收到超时取消。
  - 执行期加固：两个会让 handler 一直阻塞的用例，run 的 context 额外套 5s 上限。否则一旦超时能力回归失效，测试进程会挂死而不是失败。
- `go test ./agent/ ./middleware/ ./...` 全绿。

---

## TASK-TO-02：放弃等待（到点必然结束本批的等待）— 已完成 `b9e751b`

### 改动

**`agent/exectools.go`**

- 新增私有类型与合成结果：

```go
type toolOutcome struct {
    tr      core.ToolResult
    control *core.Directive
    ops     []core.StateOp
}

// timeoutResult words an abandoned call for the model. The tool may still be
// running: its late result, its requested Control and its State ops are
// discarded (see execTools' wait). duration is measured, not the configured
// bound, so middleware and agent-level limits report the same way.
func timeoutResult(c core.ToolCall, d time.Duration) core.ToolResult

// cancelledResult words the same abandonment when the run itself was cancelled.
func cancelledResult(c core.ToolCall) core.ToolResult
```

 文案与 `errResult` 结构相同（一个 `core.Text` part、`IsError: true`），措辞需给出下一步动作，风格对齐 `sandbox` 的 `[timed out after %s]`（`tool/exec/exec.go:56`）。

- `callOne` 返回值改为 `toolOutcome`（其余逻辑不动）。
- `run(i, c)` 改为"结果送进带缓冲通道 + 带 deadline 的等待"：

```go
ch := make(chan toolOutcome, 1) // capacity 1: an abandoned worker writes and exits, never blocks
go func() { ch <- l.callOne(lc, callCtx, c) }()
start := time.Now()

var out toolOutcome
select {
case out = <-ch:
case <-callCtx.Done():
    out = awaitAbandoned(ch, callCtx, c, start)
}

// awaitAbandoned 在 abandonGrace（2ms）内继续听通道的结果，超时/取消各用一种措辞：
//   errors.Is(callCtx.Err(), context.DeadlineExceeded) -> timeoutResult（带实测耗时）
//   否则                                              -> cancelledResult
```

- `out.tr` / `out.control` / `out.ops` 之后按原顺序处理：ops 加锁 `rc.State.Apply` → AfterTool → `results[i]`/`dirs[i]` → `core.ToolDone`（即现有 `:26-48` 的尾部，一次都不多）。**放弃之后**迟到协程只往通道里写一次，通道无人再读，因此它的 ops、Control、AfterTool 副作用自然全部不生效——在这一段加注释说明这是刻意的。
- `defer cancel()` 保持在 `run` 这一层：返回即取消，被放弃的协程能立刻收到 ctx 结束。
- 串行分支（`:63-69`）复用同一个 `run`，因此同样受超时约束；一个被放弃的调用不影响同批后续调用。

**新增文件头的并发说明**：`UpdateTool` 的注释已经允许"ToolDone 之后才到的 ToolUpdate"（`agent/exectools.go:126-135`）。本改动会显著提高其出现概率，补一句：观察者按 CallID 丢弃迟到的 `core.ToolUpdate` 即可，循环不做拦截。

### 验收

- `go test -race ./agent/`（必须带 `-race`）。
- 用例落在新文件 `agent/tooltimeout_abandon_test.go`（`tooltimeout_test.go` 已有 6 个用例、含共用 helper，另开一文件避免膨胀；`gateOnce`/`pauseOn` 复用 `agent/resume_exec_test.go` 的）：
  - `TestTimeoutUnblocksBatchThatIgnoresContext`：工具 handler 用 `<-time.After(30*time.Second)`（完全不检查 ctx），`WithToolTimeout(50*time.Millisecond)`。断言：run 在秒级返回而非 30s；历史里是超时错误结果；`ToolDone` 恰一次；循环继续走到下一个 step。**这是本 TASK 的核心断言：没有 TO-02 时该用例会挂住整个测试进程**。
  - `TestAbandonedToolCannotMutateStateAfterTimeout`：handler 睡过超时后返回带 `Result.State`（`OpSetKV`）与 `Result.Control`（`core.Stop`）的结果。断言 `State.KV` 里那个键不存在、run 没有因该 Control 提前结束。
  - 卡里列的 `TestRealErrorWinsOverTimeoutText` 没有单独新建：TO-01 已有的 `TestToolTimeoutCancelsCooperativeTool` 正是同一场景，给它加了「不得出现合成超时文案」的反向断言，并因此暴露出非阻塞排空抢不过的问题（见上条宽限窗口）。
  - `TestRunCancelWordsCancellationNotTimeout`：取消 run 的 ctx（或用 `Run.Cancel`），断言文案走 `cancelledResult` 分支。
  - `TestSequentialBatchSlowToolDoesNotBlockRest`：`WithToolExecution(ToolSequential)` + 第一个工具阻塞、第二个立即返回，断言第二个仍执行且顺序与模型调用顺序一致。
  - `TestResumedHitlBatchHonoursTimeout`：HITL 批准后放行的慢工具被超时（复用 `agent/resume_exec_test.go` 的构造方式）。
  - `TestTimeoutResultReachesOtelAfterTool`：用一个记录 AfterTool 入参的测试中间件，断言超时批次里 AfterTool 恰好一次、`IsError` 为真（保住 `obs/otel` 的 span 结束与计数，`obs/otel/otel.go:265-282`）。
- 全仓 `go build ./... && go vet ./... && go test ./...` 跑绿。
- 实测过的变异（每处改完即还原）：把等待改回阻塞读通道 → `TestTimeoutUnblocksBatchThatIgnoresContext`
  与 `TestAbandonedToolCannotMutateStateAfterTimeout` 双双失败；超时/取消判据写反 → 三条措辞相关用例失败；
  `abandonGrace` 设为 0 → `TestToolTimeoutCancelsCooperativeTool` 失败。
- 用例里被放弃的 handler 仍在睡 20s，测试不等它：进程退出即回收。若将来把这类用例改成 `t.Parallel()`
  或加 `goleak` 断言，需要先把 sleep 缩短到百毫秒量级。

---

## TASK-TO-03：`middleware.ToolTimeout`（按工具收紧）— 已完成 `8348a3e` + `1f6b6a3`

### 改动

**新增 `middleware/tooltimeout.go`**，工厂函数不加 `New` 前缀（`middleware/compaction.go:56`、`middleware/circuit.go:74` 同此）：

```go
type ToolTimeoutOptions struct {
    // Default bounds every tool call that has no PerTool entry. Zero = no bound.
    Default time.Duration
    // PerTool overrides Default by tool name. A zero or negative entry means
    // "no bound for this tool" and wins over Default.
    PerTool map[string]time.Duration
    // Exempt names a tool that never gets a bound. Same effect as a zero
    // PerTool entry, offered for readability.
    Exempt []string
}

func ToolTimeout(o ToolTimeoutOptions) agent.Middleware
```

- 实现 `agent.ToolContexter`：按 `call.Name` 选定时长，有界限就 `context.WithTimeout(ctx, d)` 并把 cancel 交回循环；不限时（Exempt、PerTool 显式 0、Default 非正）时**原样返回进来的 context 且 cancel 为 nil**，这样未受限的调用拿到的 context 与没装这个中间件时完全一致，`keepToolUpdates` 也不会多做包装。选项构造后只读、不留 per-run 状态，协程安全天然成立。`bound(name)` 在 `ok=false` 时返回 0 而不是配置里的负值，免得调用方漏判 `ok` 时把负时限塞进 context。
- 文档注释写明与 `WithToolTimeout` 的组合：这个接口只能收紧、不能放宽；agent 默认 60s 时想给某工具 300s，需 `WithToolTimeout(0)` 关掉默认。
- `middleware/tooltimeout_internal_test.go`：选定时长的纯函数单测（表驱动：命中 PerTool、落 Default、Exempt、PerTool 显式 0）。

### 验收

- 前置改动（同一能力接口的签名修正，单独提交 `8348a3e`）：`ToolContexter.ToolContext` 回传 `(context.Context, context.CancelFunc)`；`Stack.ToolContext` 倒序回收；`toolCallCtx` 串接两层 cancel；`agent/tooltimeout_test.go` 的测试中间件随之改造，并在 `TestStackToolContextFoldsInOrder` 里加"回收顺序 = 倒序、nil cancel 被跳过"的断言。
- `middleware/tooltimeout_internal_test.go`：`bound()` 表驱动 7 档（未配置、落 Default、PerTool 覆盖、PerTool 显式 0、未列出工具仍走 Default、Exempt 压过 PerTool、负数 Default）；另两例断言 cancel 真能释放派生 context、以及不限时时原样透传。
- `middleware/tooltimeout_test.go`（`package middleware_test`，走真实循环。卡里原写放 `agent` 包，但中间件的循环级测试本包已有先例 `middleware/loopguard_test.go`，就近放）：
  - `TestToolTimeoutTightensAgentDefault`：agent 默认 5s + 本中间件给该工具 40ms，工具完全不查 ctx；断言 run 在 2s 内返回且带超时文案（生效的是更早的 deadline）。
  - `TestToolTimeoutBoundsWithoutAgentDefault`：只装中间件（不设 `WithToolTimeout`）同样限得住。
  - `TestToolTimeoutUnboundedCallKeepsContext`：4 档子用例，由工具自报"我的 context 上有没有 deadline"覆盖 Exempt / PerTool 0 / 未列出走 Default / 列出的用自己的界限。
- 实测过的变异（每处改完即还原，还原后复跑全绿）：`ToolContext` 交回 nil cancel → `TestToolTimeoutCancelReleasesTimer` 失败；`bound()` 忽略 PerTool → 表驱动两档失败且 `TestToolTimeoutTightensAgentDefault` 用掉 5s 而失败；忽略 Exempt → 两档失败；`Stack.ToolContext` 不回收 cancel → 顺序断言失败。
- 全仓 `go build ./... && go vet ./... && go test ./...` 绿；`go test -race ./agent/ ./middleware/` 绿。

---

## TASK-TO-04：离线示例 — 已完成 `3d4aee5`

### 改动

新增 `examples/tool-timeout/main.go`（`package main`，只用 `llm/mock`，无网络无密钥），三段工具 + 五组演示：

| 工具 | 行为 |
| --- | --- |
| `polite` | `select` 自己的 context，被取消时回报 `stopped early: <ctx.Err()>`；没人限时它跑 1.2s 完成 |
| `hang` | `time.Sleep(2s)`，完全不看 context |
| `progress` | 先 `tctx.Update` 一条，再每 200ms 上报一条，同时盯着 ctx |

演示分组（每组打印"模型看到的那句话 + 实耗"）：

1. 不设任何上限：`polite` 1.2s、`hang` 2.0s —— 循环只能一直等。
2. `WithToolTimeout(400ms)` + `polite`：0.4s 结束，模型看到的是**工具自己的**取消报告（不是循环的措辞，这就是 2ms 宽限窗口的效果）。
3. `WithToolTimeout(400ms)` + `hang`：0.4s 结束，循环替工具回答，完整一句另外单印一行（表格里只截断显示）。
4. `middleware.ToolTimeout` 三档：只装中间件也能限住；agent 默认 5s + 中间件 `Default 3s` + `PerTool{"hang":300ms}` 时生效的是最早那个；`Exempt` 里的 `polite` 回到无上限（1.2s 自然完成）。
5. 上限 500ms 的 `progress`：`core.ToolUpdate` 仍逐条到达（演示 `keepToolUpdates` 的价值）。

与卡里原写的两处不同：卡建议「不设超时的那一组先 sleep 到可观察时长再打印以保持输出确定」——不需要，改成把工具自身的耗时压到 1.2s/2s，既保留"没有上限就会等这么久"的观感，全段又只要约 7s；`slow_but_polite` 更名 `polite`。另加卡里没列的第 5 组（进度上报）。

- 未提交任何可执行产物（`.gitignore:2` 忽略 `*.exe`）。
- README 未动，理由见"明确不做"。

### 验收

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` = 0（31 包）；`go test -race ./agent/ ./middleware/` = 0；`gofmt -l examples/tool-timeout/main.go` 无输出。
- `go run ./examples/tool-timeout` 退出码 0，实测 `real 0m7.025s`。
- 输出确定性：连跑两次并归一化耗时数字（`[0-9]+ms`、`[0-9]+\.[0-9]s`）后逐行 diff 为空 —— 唯一差异是实测毫秒（303ms vs 302ms），文案与行为一致。
- 提交：`3d4aee5`。

---

## TASK-TO-05：补充两个视角的可运行示例 — 已完成 `b9d71a2` + `be482a9`（2026-10-03）

TO-04 的示例讲的是"上限设在哪、循环到点后怎么做"。用户追加要求：再写两个详细示例说明**怎么用好**工具超时，因此补两个视角，各一个可执行目录。

### 改动

- `examples/tool-timeout-author/main.go`（`b9d71a2`）——**写工具的人**：同一个 300ms 上限下对照 `fetch_rows`（每步副作用前查 ctx，被中断时回报"写完 3 行后中断（第 4 行未落盘）"）与 `legacy_batch`（不查 ctx，run 结束时只写了 2 行、再等 1s 变 6 行）。落点是三条约定：切小副作用、自报进度、做不到就把副作用做成幂等。
- `examples/tool-timeout-consumer/main.go`（`be482a9`）——**读事件流的人**：一个按 CallID 记账的 `tracker`，三个场景分别产出"工具自报中断 / 超时，结果未知 / 运行已取消"三种结算，并打印每种情形丢弃的迟到 `ToolUpdate` 条数。落点是五条约定（见文件末尾）。
- 两处实现期才确定的要点：
  1. 迟到上报**必须让 run 活得比被放弃的调用更久才看得见**——单调用的批次里 run 随即结束、订阅已关闭，观察者在流上根本收不到迟到的 `ToolUpdate`。场景 B 因此把 `legacy_batch` 与一个上限更宽（`middleware.ToolTimeout` 的 `PerTool{job_watcher:1.5s}`）的 `job_watcher` 放进同一批次，实测丢弃 4 条（450/600/750/900ms 四次上报，ToolDone 在 403ms）。这一步同时演示了"按工具给不同上限"的真实用法。
  2. 区分"工具自报"与"循环代答"**目前只能读文案**：`core.ToolResult` 只有 `IsError` + 文本。示例把判断集中在一个 `classify` 里并注明这是措辞依赖；ADR-0030 的 `RejectClass` 含 `timed_out`，但其备选方案 A 明确否决了把分类放进 `core.ToolResult`，所以事件流消费者不会因此免掉文案匹配——ADR-0029 第 10 节已按这个口径写。
- 未动库代码：两个示例只读既有公开 API（`agent.WithToolTimeout`、`middleware.ToolTimeout`、`a.Stream`、`core.ToolStarted/ToolUpdate/ToolDone`）。

### 验收

- `gofmt -l` 对两个新文件无输出；`go vet ./...` = 0；`go test ./...` = 0；`go build ./...` = 0。
- `go run ./examples/tool-timeout-author` 退出码 0，实测 `real 0m3.918s`（含编译）；连跑两次逐行一致。
- `go run ./examples/tool-timeout-consumer` 退出码 0，实测 `real 0m7.928s`（含编译）；三场景的分类与计数稳定（A 自报 2 行、B 丢弃 4 条、C 取消措辞）。
- ADR-0029 新增第 10 节，索引三个示例（视角、命令、实测时长、对应 commit）。
- README 仍未动，理由与 TO-04 相同（见"明确不做"）。

---

## 执行顺序与提交切分

五个 TASK 已全部落地，提交顺序与卡里规划一致（中间多插一个接口修正）：

1. `edfde88` — TASK-TO-01 `feat(agent): ToolContexter seam and a per-agent tool timeout bound`
2. `b9e751b` — TASK-TO-02 `feat(agent): stop waiting on a tool call that outlives its bound`
3. `8348a3e` — TASK-TO-03 前置：`ToolContexter` 回传 cancel（做 TO-03 时才发现原签名会让每笔调用留下一个无人回收的定时器）
4. `1f6b6a3` — TASK-TO-03 `feat(middleware): per-tool timeout on the ToolContexter seam`
5. `3d4aee5` — TASK-TO-04 `docs(examples): offline demo for tool execution timeouts`
6. `b9d71a2` — TASK-TO-05 `docs(examples): show the tool author's side of execution timeouts`
7. `be482a9` — TASK-TO-05 `docs(examples): read tool timeouts from the event stream`

1–5 已由用户推到 `origin/main`；6–7 未推送（本仓库由用户自行 push）。

提交信息用英文（与仓库历史一致，Conventional Commits，不加 trailer）。每个 TASK 单独提交，中间态必须 `go build ./...` 通过。`git push` 等确认后再做。

## 明确不做

- 不给 `tool.Tool` 增加 `Timeout()` 能力接口（ADR-0029 备选 A，已否决）。
- 不动 `core.ToolResult` / `core.Event`（超时沿用既有类型）。
- 不强杀 goroutine、不承诺副作用中止：Go 做不到，文档与 `WithToolTimeout` 注释里写清这条边界。
- 不做每工具时长映射放进 agent 配置（要按工具区分就用 `middleware.ToolTimeout`，避免配置面重复）。
- 不改 `RunBudget.MaxDuration` 的检查时机（仍在模型调用边界）。
- README 不新增示例行：现有 README 的「示例目录详解」并未收录 `examples/loop-guard`、`examples/resilience` 之外的防护中间件示例（`grep -n "loop-guard\|run-budget\|LoopGuard\|RunBudget" README.md` 无命中），本次单独补一行会造成新的不一致。若要补齐 README 与 `docs/adr` 索引，另开一个文档任务。

## 风险

| 风险 | 现状判断 | 处置 |
| --- | --- | --- |
| 被放弃的工具继续跑并写外部资源，run 已按"失败"继续 | Go 语言限制：无法终止运行中的函数 | 已承担并写进 `WithToolTimeout` 与 ADR 第 3 节的文档；要求工具有副作用时自行尊重 ctx（登记不修） |
| `agent.tool.duration` 指标记成上限值而非真实耗时 | `obs/otel` 的耗时在 AfterTool 结算（`obs/otel/otel.go:280`），超时时真实耗时未知 | 已承担：在 ADR 第 3 节列为已知不精确，与 RunBudget 已有的"中途失败的调用不计 Usage"同类措辞（登记不修） |
| 迟到 `ToolUpdate` 变多，前端渲染可能出现"已完成后又冒进度" | 该情况早已被 `UpdateTool` 注释允许（`agent/exectools.go:126-135`） | 本方案修：补一句注释 + TO-02 用例断言 CallID 语义；拦截留给观察者（部分修复，循环侧不处理），观察者侧的写法见 `examples/tool-timeout-consumer`（TO-05） |
| select 分支下"真实错误优先"依赖内层非阻塞读的时序 | 已用显式内层 `select` + 容量 1 的通道消除随机性 | 本方案修：TO-02 的 `TestRealErrorWinsOverTimeoutText` 覆盖 |
| 串行批次总耗时变成 N×d，用户以为有整轮上限 | 语义就是 per-call | 本方案修：选项文档注释明写，整轮上限指向 `RunBudget.MaxDuration`（已修） |
| 派生 context 后 `Context.Update` 找不到 `tool.Updater`，长任务的 `ToolUpdate` 静默丢失 | 写卡时未预见；`tool/tool.go:65-72` 在工具拿到的 context 上做能力查找，包一层 deadline 就查不到 | 已修：`updaterContext` + `keepToolUpdates`（`agent/exectools.go`），并用 `TestToolUpdateSurvivesDerivedDeadline` 覆盖；删掉透传该行会导致该用例失败，已实测 |
| HITL 恢复批次漏掉超时约束 | `runResumed` 走同一 `execTools`（`agent/hitl.go:151`），自动继承 | 本方案修：TO-02 的 `TestResumedHitlBatchHonoursTimeout` 覆盖该用例（已修） |
