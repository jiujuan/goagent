# 工具调用粒度并发调度设计（P0-3）

日期：2026-10-22
状态：设计提议，未实施
范围：只讨论 P0-3「工具并发按调用粒度判断，不再整批串行」。本文不修改代码、公开 API 或存储格式。
关联：[Codex core 运行时对 goagent 的优化建议](codex-core-runtime-optimization-proposal.md)、[StepView 与外层任务循环设计草稿](stepview-and-task-loop-design-draft.md)

## 1. 结论

把现在的「批次里只要出现一个串行工具，整批按顺序逐个执行」改成「按调用切段，段内并发，段间等待」：

1. **连续的非串行调用合成一个并行段**，段内仍然为每个调用启动一个 goroutine，和今天的并行批次一样。
2. **每个串行调用单独成一个串行段**，它执行时，同一个批次里没有别的调用被启动。
3. **段按模型给出的调用顺序依次执行**。串行工具是分界，不是整批锁；它前后的连续调用各自成组，但两侧不会互相重叠。
4. **结果仍按模型原始调用顺序组装**；`ToolStarted` 仍按下标顺序发布；`ToolReject` 仍在整批结束后串行回调。
5. **全局强制串行保留**。`ToolExecMode == ToolSequential` 时，每个调用都成为单调用串行段，语义和今天完全一致。

这个改动只动 `execTools` 的调度部分，不改 `callOne`、状态锁、持久化、审批、超时和事件类型。设计上采用确定性的分段，而不是 Codex 那样的读写锁：Go 的互斥锁不保证等待顺序，锁的作用范围也要覆盖 handler、`AfterTool` 和持久化完成记录，容易写出依赖调度器的行为；显式分段可以直接对「串行调用执行时没有别的调用在跑」写断言。

原提案里有一句需要纠正：**状态写入不是 `checkWriteAccess` 保护的**。`checkWriteAccess` 只检查运行取消、写保护是否停止、租约是否过期；真正的并发写保护是 `execTools` 内部的 `stateMu`。本次改动保留 `stateMu` 不动，见 [agent/exectools.go:49](../../agent/exectools.go#L49) 和 [agent/exectools.go:106](../../agent/exectools.go#L106)。

## 2. 目标与非目标

### 2.1 目标

1. 一个串行工具只让它自己独占执行，不再连累同批次的其他调用。
2. 同一段内的非串行调用可以并发，恢复并行模式的吞吐。
3. 结果、`ToolStarted`、指令的组装顺序保持模型调用顺序，不让并发改变模型看到的历史。
4. 取消和租约失效仍然能阻止后续调用启动；已经启动的调用按现有语义完成或被放弃。
5. 保持全局 `ToolSequential`、审批、暂停恢复、`AfterTool`、`ToolReject`、检查点和超时的现有行为。
6. 调度计划是可单独测试的纯函数，不依赖 goroutine 的实际完成先后。

### 2.2 非目标

- 不实现工具之间的依赖图或拓扑调度。串行能力仍然只由 `SequentialTool` 声明。
- 不改变 `tool.Tool`、`SequentialTool`、`AsSequential` 的签名，也不新增公开配置项。
- 不改变单调用超时（`WithToolTimeout`）的语义，不试图杀死已经运行的工具。
- 不为并行段增加最大并发数限制；今天一个并行批次会启动多少个 goroutine，改动后同一段内仍然如此。
- 不改变 `State.Apply` 的锁模型，也不把 `checkWriteAccess` 扩展成状态互斥锁。
- 不改变审批时机：`CheckToolPolicy` 和 `BeforeTool` 仍在整批执行前完成。
- 不改变步骤数、`MaxTurns` 或 `RunBudget` 的消耗；工具之间的并发方式不影响步骤预算。
- 不改变跨步骤、跨运行的并发模型。

## 3. 现状

### 3.1 一次工具批次的执行链

一个步骤拿到模型的工具调用后，按下面的顺序处理：

1. 冻结步骤视图。请求定稿后，`freezeStepView` 把本步广播的工具、实现和串行标记固定下来：[agent/loop.go:309](../../agent/loop.go#L309)、[agent/stepview.go:388](../../agent/stepview.go#L388)。
2. 整批策略检查。`CheckToolPolicy` 在任何一个工具执行前检查整批：[agent/loop.go:418](../../agent/loop.go#L418)。
3. 逐个调用审批。`BeforeTool` 可以中断、停止或改写调用：[agent/loop.go:422](../../agent/loop.go#L422)。
4. 执行批次。`execTools` 负责调度、调用、状态写入、事件和结果组装：[agent/loop.go:442](../../agent/loop.go#L442)。
5. 批次结束后回调被拒绝的调用。`ToolReject` 在 `execTools` 返回后、检查点提交前串行执行：[agent/loop.go:443](../../agent/loop.go#L443)。
6. 折叠指令并提交检查点。工具或 `AfterTool` 产生的指令按调用顺序折叠：[agent/loop.go:453](../../agent/loop.go#L453)；然后发布 `TurnDone`：[agent/loop.go:457](../../agent/loop.go#L457)。

暂停恢复的批次走同一套执行器：`prepareRun` 用 `resumeStepView` 重建视图：[agent/loop.go:124](../../agent/loop.go#L124)，`runResumed` 只把批准的调用交给 `execTools`：[agent/hitl.go:329](../../agent/hitl.go#L329)。

### 3.2 当前的整批降级

`execTools` 在执行前先算一个布尔值：

```go
needSeq := l.toolExec == ToolSequential
if !needSeq {
    for _, c := range calls {
        if lc.view.Tools().Sequential(c.Name) {
            needSeq = true
            break
        }
    }
}
```

完整分支在 [agent/exectools.go:146-169](../../agent/exectools.go#L146)。只要 `needSeq` 为真，所有调用都走同一个循环，前一个 `run` 返回后才发布下一个 `ToolStarted` 并执行下一个调用。

这带来两个结果：

- **串行工具连累整批。** 模型一次发十个只读查询加一个必须串行的写操作，十个查询也要排队。
- **并行调用被误伤。** 一个批次里只要有一个 `tool.AsSequential`，原本互不相关的调用全部失去并发。

需要说明的是：这个判断本身只读取 `lc.view`，不是执行时临时读工具表。因此同一个批次内，某个工具在 handler 里动态注入工具，也不会改变本批已经决定的串行/并行方式。这个性质要保留。

### 3.3 现有执行器已经具备按调用并发的基础

`execTools` 的大部分代码已经把「一次调用」当作最小单位：

- `results`、`reported`、`dirs` 都按调用下标预分配，每个 worker 只写自己的下标：[agent/exectools.go:46-48](../../agent/exectools.go#L46)。
- `run(i, c)` 负责一次调用的完整生命周期：检查写权限、派生调用上下文、启动 handler goroutine、等待或超时放弃、应用状态、执行 `AfterTool`、记录持久化完成、保存结果、发布 `ToolDone`：[agent/exectools.go:65](../../agent/exectools.go#L65)。
- 并行批次里的状态写入通过 `stateMu` 串行化：[agent/exectools.go:106](../../agent/exectools.go#L106)。`core.State.Apply` 自身没有锁：[core/state.go:77](../../core/state.go#L77)。
- 同一批次里的 `AfterTool` 可以并发，因此中间件必须可并发调用：[agent/middleware.go:20](../../agent/middleware.go#L20)。
- 持久化完成记录用 `addCompletedTool` 自带的互斥锁：[agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139)。每次调用的 `ClaimTool` 本来就在并行模式下并发执行：[agent/exectools.go:353](../../agent/exectools.go#L353)。
- `ToolStarted`、`ToolUpdate`、`ToolDone` 都通过并发安全的总线发布：[bus/bus.go:109](../../bus/bus.go#L109)、[core/event.go:55](../../core/event.go#L55)。

因此本次不需要改 `run` 和 `callOne`，只需要把「哪些调用可以同时启动」从整批判断改成按段判断。

### 3.4 提案说法与本仓库事实核对

| 提案里的说法 | 当前代码事实 | 对设计的影响 |
|---|---|---|
| 「状态写入已经有 `checkWriteAccess` 保护」 | `checkWriteAccess` 只读 context、写保护状态和租约到期时间，返回取消、停止或租约错误：[agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13)。真正的并发写保护是 `execTools` 的 `stateMu`。 | 不改 `stateMu`；分段只决定哪些 handler 能重叠，不能替代状态锁。 |
| 「路由层按单个调用判断」 | 本仓库没有独立的路由层；判断点在 `execTools`，读取冻结视图的 `sequential` 表：[agent/stepview.go:240](../../agent/stepview.go#L240)。 | 调度计划必须只使用 `lc.view.Tools().Sequential`，不要重新读静态/注入表。 |
| 「串行工具会降级整批」 | 这是当前行为，也是 `SequentialTool` 和 `AsSequential` 注释写明的行为：[tool/tool.go:88-96](../../tool/tool.go#L88)、[agent/options.go:95](../../agent/options.go#L95)。 | 改动后必须同步修改这些注释，否则文档和代码互相矛盾。 |
| 「超时是每次调用」 | 正确：[agent/options.go:100](../../agent/options.go#L100)。 | 分段边界只能等待到「循环放弃并发布超时结果」为止，不能等待超时后仍在运行的 handler。 |

另外，`checkWriteAccess` 不是状态互斥锁，这一点也意味着：工具直接写 `tool.Context.State` 的代码本来就不在 `stateMu` 保护范围内。这不是本次引入的问题，分段也不会修复它；工具应通过 `Result.State` 声明状态变更。

### 3.5 现有测试为什么证明不了新行为

现有涉及串行批次的测试主要只有两个调用：

- `TestSequentialToolDowngradesWholeBatch`，调用序列是 `[plain, seq]`：[agent/seqexec_test.go:68](../../agent/seqexec_test.go#L68)。
- `TestInjectedSequentialToolDowngradesBatch`，调用序列是两个同名工具：[agent/dynamictool_test.go:272](../../agent/dynamictool_test.go#L272)。
- 同名替换的两个用例也都只有两个调用：[agent/stepview_test.go:531](../../agent/stepview_test.go#L531)、[agent/stepview_test.go:553](../../agent/stepview_test.go#L553)。

`[plain, seq]` 在整批串行（旧行为）和「plain 单独一组、seq 单独一段」（新行为）下最大并发都是 1，无法区分两者。要证明新行为，至少需要三个调用，并且让两个非串行调用位于串行工具的同一侧，例如 `[p1, p2, seq]`：旧行为下 `p1`、`p2` 不会重叠，新行为下它们必须重叠。

## 4. 设计原则

1. **确定性优先。** 计划只由调用列表、冻结视图和全局模式决定，不依赖 goroutine 的启动或完成顺序。
2. **一个判断来源。** 是否串行只来自 `lc.view.Tools().Sequential(name)`；和今天的判断源一致。
3. **最小改动。** 只替换 `execTools` 中决定串行/并行的分支；`run`、`callOne`、状态、持久化、事件全部复用。
4. **顺序稳定。** 结果数组、指令数组和 `ToolStarted` 的下标顺序不因并发改变；模型历史仍然按模型给出的顺序排列。
5. **边界写清楚。** 串行工具是「不与其他调用同时启动」的边界，不是「整批必须排队」的锁。
6. **先检查再启动。** 每一个新段、每一个新调用在启动前都再次检查写权限；已经启动的调用不强行终止。
7. **计划冻结。** 计划在第一个 handler 启动前一次算完；handler 内部动态注入工具不会影响本批次的边界。
8. **不扩大承诺。** 分段提供的是调度层的隔离，不能阻止被超时放弃的 handler 或工具自己启动的 goroutine 继续运行。

## 5. 核心设计

### 5.1 术语

- **并行段（parallel segment）**：一段连续的非串行调用。段内每个调用可以同时执行。
- **串行段（serial segment）**：只包含一个串行调用的段。该调用执行时，同批次没有其他调用被启动。
- **调度计划（dispatch plan）**：按模型调用顺序排列的若干段，覆盖每个调用恰好一次。
- **全局串行模式**：`WithToolExecution(ToolSequential)`。每个调用都成为单调用串行段。

这里的「段」是调度单位，不是模型消息单位，也不跨步骤。

### 5.2 计划类型

新增一个只在 `agent` 包内部使用的类型和构造函数：

```go
type toolCallSegment struct {
    serial  bool
    indexes []int
}

func buildToolCallSegments(calls []core.ToolCall, tools *ToolView, mode ToolExecMode) []toolCallSegment
```

- `indexes` 保存原调用切片中的下标，而不是复制 `core.ToolCall`。`results`、`reported`、`dirs` 本来就按原下标存放，直接用下标最省事，也避免参数 JSON 被复制多份。
- `serial` 为真时，`indexes` 长度必定为 1。把这个不变量写进单元测试。
- 函数是纯函数：同样的 `calls`、`tools`、`mode` 一定得到同样的计划。它不读运行状态，不启动 goroutine，也不发布事件。

不建议把类型命名为 `plan` / `dispatchPlan`，因为仓库里已经有一套 agent 的「计划模式」（`agent/plan_*.go`）。用 `toolCallSegment` 可以避免阅读时混淆。

### 5.3 分段算法

```go
func buildToolCallSegments(calls []core.ToolCall, tools *ToolView, mode ToolExecMode) []toolCallSegment {
    if len(calls) == 0 {
        return nil
    }
    if mode == ToolSequential {
        segments := make([]toolCallSegment, len(calls))
        for i := range calls {
            segments[i] = toolCallSegment{serial: true, indexes: []int{i}}
        }
        return segments
    }

    var segments []toolCallSegment
    var group []int
    flush := func() {
        if len(group) == 0 {
            return
        }
        segments = append(segments, toolCallSegment{indexes: group})
        group = nil
    }
    for i, c := range calls {
        if tools.Sequential(c.Name) {
            flush()
            segments = append(segments, toolCallSegment{serial: true, indexes: []int{i}})
            continue
        }
        group = append(group, i)
    }
    flush()
    return segments
}
```

实现上有两个细节：

- `flush` 把 `group` 放入计划后必须把 `group` 置空。否则后续 `append` 可能复用同一块底层数组，改写已经放进计划的 `indexes`。
- 全局串行模式不走通用循环，而是直接为每个调用生成一个串行段。这样执行器只有一种「串行段」路径，事件顺序和写权限检查也和旧的串行循环一致。

分类是保守的：只看冻结视图里的名字，不预先判断这个调用是否会被拒绝、是否会命中已完成的持久化结果。即使一个串行调用最终因为参数非法或已有完成记录而不执行 handler，它仍然形成一个边界。这不会影响正确性，只是少了一次本可以并发的机会。

同名调用按每个调用分别判断，不会合并。两个调用都指向同一个串行工具时，会得到两个相邻的串行段，仍然一次执行一个。

### 5.4 分段示例

| 模型调用顺序 | 产生的段 | 说明 |
|---|---|---|
| `[A, B]` | `[A, B]` | 一个并行段，A、B 可以重叠。 |
| `[A, B, S]` | `[A, B]`、`[S]` | A、B 重叠；S 等它们都结束后独自执行。 |
| `[S, A, B]` | `[S]`、`[A, B]` | S 先独自执行；A、B 之后重叠。 |
| `[A, S, B]` | `[A]`、`[S]`、`[B]` | A 与 B 分居 S 两侧，不会互相重叠。 |
| `[A, S1, B, S2, C]` | `[A]`、`[S1]`、`[B]`、`[S2]`、`[C]` | 每个串行调用都是独立边界。 |
| `[S1, S2]` | `[S1]`、`[S2]` | 两个串行调用也不合并，仍然一次一个。 |
| `[A, B]`，全局串行 | `[A]`、`[B]` | 与旧行为一致。 |
| `[]` | 空 | 没有调用，不需要计划。 |

这个模型有一个重要推论：**串行工具两侧的并行组不会互相重叠**。例如 `[A, S, B]` 中 A 和 B 不能同时运行，因为 S 必须独占，而段又必须按顺序执行。这不是额外限制，而是串行独占语义的直接结果。

### 5.5 执行器改动

`execTools` 把现在的 `needSeq` 分支（[agent/exectools.go:146-185](../../agent/exectools.go#L146)）替换成下面的结构：

```go
segments := buildToolCallSegments(calls, lc.view.Tools(), l.toolExec)
dispatchStopped := false
for _, seg := range segments {
    if dispatchStopped {
        break
    }

    if seg.serial {
        i := seg.indexes[0]
        if err := rc.checkWriteAccess(); err != nil {
            recordRunErr(err)
            break
        }
        rc.publish(core.ToolStarted{Call: calls[i]})
        run(i, calls[i])
        continue
    }

    var wg sync.WaitGroup
    for _, i := range seg.indexes {
        if err := rc.checkWriteAccess(); err != nil {
            recordRunErr(err)
            dispatchStopped = true
            break
        }
        rc.publish(core.ToolStarted{Call: calls[i]})
        wg.Add(1)
        go func(i int, c core.ToolCall) {
            defer wg.Done()
            run(i, c)
        }(i, calls[i])
    }
    wg.Wait()
}
return reportedToolResults(results, reported), dirs, rejects,
    errors.Join(append(afterErrs, runErrs...)...)
```

逐条说明：

- **进入函数时的检查不变。** [agent/exectools.go:43](../../agent/exectools.go#L43) 仍然在算计划前检查一次写权限。
- **串行段在循环 goroutine 上直接调用 `run`。** 和旧的串行分支一样，不额外启动一个 goroutine。
- **并行段为每个调用启动一个 goroutine，并在段末 `wg.Wait()`。** 这就是段与段之间的等待点。`AfterTool`、持久化完成记录和 `ToolDone` 都在 `run` 内，因此它们也包含在等待范围内。
- **写权限在启动每个调用前检查。** 并行段内某次检查失败时，记录错误、停止本段后续调用的启动，把 `dispatchStopped` 置真，等已经启动的调用结束后不再进入下一段。这与当前并行分支「检查失败就停止继续分发」的行为一致。
- **串行段不吞掉 handler 错误。** `run` 记录的 `runErr` 只加入最终错误，不会阻止后续段；这一点和旧串行循环一致：只有写权限检查失败才停止分发。
- **不新增锁。** 计划在单 goroutine 内构建和遍历；`results`、`reported`、`dirs` 每个下标只有一个 worker 写，底层切片长度固定，不需要额外同步。`rejects`、`afterErrs`、`runErrs` 继续使用现有互斥锁。
- **结果仍是原顺序。** `reportedToolResults` 按下标升序扫描：[agent/exectools.go:188](../../agent/exectools.go#L188)。并行段里谁先完成都不会影响模型看到的历史顺序。

不建议把 `run` 改成带段参数的函数，也不建议在 `run` 内部判断串行。`run` 的职责是「执行并结算一次调用」，串行/并行只属于调度层。

**并行段上限。** 分段只决定哪些调用可以重叠，不限制一个并行段里同时执行多少个调用。这个上限由另一个开关负责：`agent.WithToolConcurrency` 默认把一个并行段限制在 8 个在途调用，`0` 表示不限制；串行段不受它影响，仍然是「段内一次一个」。上限的动机、默认值、名额结算点和与工具超时的关系见 [运行时并发加固设计](runtime-concurrency-hardening-design.md) 的 §6，本文不重复分段设计。

### 5.6 一个批次的执行时序

```text
buildToolCallSegments
        │
        ├─ 并行段 [A, B]
        │    ├─ checkWriteAccess → ToolStarted(A) → goroutine run(A) ─┐
        │    ├─ checkWriteAccess → ToolStarted(B) → goroutine run(B) ─┤
        │    └─ wg.Wait() ←───────────────────────────────────────────┘
        │         （A、B 的 handler、AfterTool、完成记录、ToolDone 都结束后）
        │
        ├─ 串行段 [S]
        │    ├─ checkWriteAccess → ToolStarted(S) → run(S)
        │    └─ run 返回（S 的 handler、AfterTool、完成记录、ToolDone 都结束）
        │
        └─ 并行段 [C, D]
             ├─ checkWriteAccess → ToolStarted(C) → goroutine run(C) ─┐
             ├─ checkWriteAccess → ToolStarted(D) → goroutine run(D) ─┤
             └─ wg.Wait() ←───────────────────────────────────────────┘

返回：reportedToolResults（按 A、B、S、C、D 的原下标）
```

`run` 内部仍然是原来的顺序：写权限检查 → 派生调用上下文 → 启动 handler goroutine → 等待或超时放弃 → `stateMu` 下应用状态 → `AfterTool` → 记录持久化完成 → 写 `results[i]` → 发布 `ToolDone`。

### 5.7 事件顺序

| 事件 | 并行段 | 串行段 | 段与段之间 |
|---|---|---|---|
| `ToolStarted` | 按段内下标顺序发布；每个调用发布后立即启动自己的 goroutine | 发布后立即执行 | 后一段的 `ToolStarted` 一定在前一段的 `run` 全部返回后发布 |
| `ToolUpdate` | handler 运行期间随时可能发布 | 同左 | 被超时放弃的 handler 可能在后续段继续发布，见 5.10 |
| `ToolDone` | 按完成顺序发布；同一个并行段里，快的调用可能在慢的调用 `ToolStarted` 之前就完成 | 调用结束时发布 | 前一段的 `run` 全部返回后才进入下一段；串行段的 `ToolDone` 一定早于下一段的 `ToolStarted` |
| `ToolReject` | 整批结束后由循环串行回调 | 同左 | 同左 |
| `TurnDone` | 步骤检查点提交后发布一次 | 同左 | 同左 |

两个需要明确写进文档的观察：

1. **`ToolStarted` 仍然保持模型顺序，但不保证同一并行段的所有 `ToolStarted` 都早于该段第一个 `ToolDone`。** 当前并行分支也是这个行为：发布一个 `ToolStarted` 就启动一个 goroutine，快的调用可以很快完成。若要改成「先全部发布 Started，再统一启动」，会改变写权限检查与启动的对应关系，收益不大，本文不做。
2. **串行段的 `ToolDone` 一定早于下一段的 `ToolStarted`。** 这是分段带来的新保证，事件消费者可以依赖它判断某个串行调用已经结算。

### 5.8 结果、指令和拒绝

- `results[i]` 对应 `calls[i]`，`reportedToolResults` 过滤掉没有结果的调用后仍按模型顺序返回：[agent/exectools.go:188](../../agent/exectools.go#L188)。
- `dirs[i]` 对应 `calls[i]`；批次结束后 `core.Resolve(dirs...)` 按模型顺序折叠：[agent/loop.go:453](../../agent/loop.go#L453)。因此并行完成顺序不会改变指令优先级。
- `rejects` 在 `run` 内按完成顺序追加，带 `rejectMu`；`ToolReject` 在整批结束后串行回调：[agent/middleware.go:75](../../agent/middleware.go#L75)。分段不改变这个时机。
- `errors.Join` 仍把 `AfterTool` 错误和运行错误合并后返回。分段只是改变了错误出现的先后，不改变最终合并规则。

### 5.9 状态与持久化

**状态。** `stateMu` 继续保护每一次 `State.Apply`：[agent/exectools.go:106](../../agent/exectools.go#L106)。分段提供一个顺序保证：前一段所有调用的 `run` 都返回后，后一段才会开始，因此后一段的 handler 能看到前一段已经应用的状态。并行段内部没有跨调用的顺序保证，和今天的并行批次一样。

`AfterTool` 也在 `run` 内、在 `wg.Wait` 之前完成，因此它的耗时和错误都落在段边界内；它本身没有声明式的状态操作返回值。

**持久化。** 每次调用的 `ClaimTool`、完成记录和重放都在 `callOne`/`run` 内，按调用独立进行：[agent/exectools.go:353](../../agent/exectools.go#L353)、[agent/exectools.go:130](../../agent/exectools.go#L130)。分段不会改变调用身份、幂等键或完成账本；已经完成并被重放的调用仍然返回存储结果，`skipAfter` 仍然跳过 `AfterTool`。区别只在于：如果这个调用被分类为串行，它仍然会单独成段，即使它不会真正执行 handler。这是保守但安全的行为。

### 5.10 取消、租约和超时

**取消和租约。** 以下位置继续检查写权限：

- `execTools` 入口：[agent/exectools.go:43](../../agent/exectools.go#L43)
- 每个段、每个调用启动前：本次改动新增的调度检查
- `run` 内部：[agent/exectools.go:67](../../agent/exectools.go#L67)
- `callOne` 参数校验后、持久化声明后：[agent/exectools.go:340](../../agent/exectools.go#L340)、[agent/exectools.go:382](../../agent/exectools.go#L382)

因此，取消或租约失效发生在两个段之间时，后一段不会被启动；发生在并行段内部时，已经启动的调用会继续到自己的结算点，未启动的调用不再启动。这与当前「两个串行工具之间检查失败就停止」的行为一致，现有测试 [agent/dispatch_guard_test.go:19](../../agent/dispatch_guard_test.go#L19) 必须继续通过。

**超时。** `WithToolTimeout` 是每次调用的上限，不是每段的上限：[agent/options.go:100](../../agent/options.go#L100)。一个并行段里的调用超时后，`run` 最多再等一个 `abandonGrace`（2ms）宽限窗口，然后返回并发布超时结果，段才可能结束：[agent/exectools.go:203](../../agent/exectools.go#L203)。被放弃的 handler 可能仍在运行，因此：

> **串行段保证的是「调度层没有启动其他调用与它重叠」，不是「进程里绝对没有其他工具代码在跑」。**

这是 Go 无法强制终止 goroutine 的既有边界，ADR-0029 已经记录了同样的语义。分段不会修复它，也不能在文档里把它说成绝对隔离。

同理，被放弃的 handler 之后通过 `Context.Update` 发出的 `ToolUpdate` 仍然会进入事件流：[agent/exectools.go:506](../../agent/exectools.go#L506)。事件消费者仍应按 `CallID` 丢弃已经结算调用的迟到更新。

### 5.11 全局串行模式

`ToolExecMode == ToolSequential` 时，计划由每个调用一个串行段组成，执行器不会进入并行段分支。结果：

- 所有的调用仍然严格按模型顺序执行。
- 每个调用启动前都检查写权限。
- 不启动额外的调度 goroutine（handler 自己的 goroutine 仍然存在，和旧行为一样）。
- 事件顺序与旧串行循环一致。

因此 `WithToolExecution(ToolSequential)` 可以继续作为应急开关使用，行为不变。

### 5.12 审批和暂停恢复

审批发生在 `execTools` 之前，分段发生在审批之后：

- `CheckToolPolicy` 仍然看整批。
- `BeforeTool` 仍然按模型顺序逐个调用；中断时保存整个原始批次。它也可以改写调用本身，计划使用的是改写后的 `calls`，和 `callOne` 最终解析的对象一致。
- 恢复时 `runResumed` 先过滤出批准的调用，再交给 `execTools`：[agent/hitl.go:301](../../agent/hitl.go#L301)。
- 分段建立在**实际交给 `execTools` 的调用序列**上。被拒绝或未批准的调用不会形成边界，因为它们不会执行；批准后的调用如果因为中间有一个未批准调用而变成相邻，它们可以在同一段里并发。

`resumeStepView` 的串行标记来自恢复时当前静态表的实现：[agent/stepview.go:377](../../agent/stepview.go#L377)。如果暂停期间实现被替换（例如静态串行工具变成了非串行工具），恢复后的分段方式会跟着变。这是现有整批判断也存在的性质，不是本次新引入的；但由于分段对边界更敏感，建议在文档里把它列为已知边界，而不是假装快照能检测到它。`StepSnapshot` 目前记录广播名称、工具摘要、设置摘要和模型名，不记录串行标记。

## 6. 不变量

以下性质应当可以直接写成断言：

1. **覆盖。** 计划中的下标集合恰好等于 `0..len(calls)-1`，每个下标出现一次。
2. **顺序。** 段按原调用下标升序排列；每个段内部的下标也升序。
3. **分类。** 全局模式为 `ToolSequential` 时，每个段都是单调用串行段；否则，一个段是串行段当且仅当该调用在冻结视图里 `Sequential(name)` 为真。串行段的 `indexes` 长度恰好为 1。
4. **隔离。** 串行段执行时，执行器不会启动同批次的其他调用；串行段的 `run` 返回之前，下一段不会发布任何 `ToolStarted`。
5. **结果顺序。** 返回的结果和指令按原调用下标升序排列，与完成先后无关。
6. **结算。** 每个启动的调用要么写入结果并发布 `ToolDone`，要么记录运行错误；并行段在返回前 `wg.Wait()`，不会留下调度层的 goroutine。
7. **状态可见性。** 前一段的 `State.Apply` 在后一段任何 handler 启动前完成；并行段内不承诺跨调用的状态顺序。
8. **写权限。** 写权限检查失败后，不再启动新的调用；已经启动的调用按现有取消/超时语义结算。
9. **持久化。** 每个调用的 `ClaimTool` 和完成记录仍然只按调用身份执行一次；分段不改变幂等键或重放规则。
10. **全局模式。** `ToolSequential` 下没有任何两个调用重叠，顺序与旧实现一致。

注意第 4 条的措辞是「执行器不会启动」，不是「进程中没有其他代码」。被放弃的 handler、工具自己创建的 goroutine 不在调度器的控制范围内。

## 7. 兼容性与影响

### 7.1 行为变化

| 场景 | 改动前 | 改动后 | 影响 |
|---|---|---|---|
| 十个只读 + 一个串行写 | 十一个全部排队 | 十个只读按连续位置成组并发，写单独执行 | 这是本次要解决的问题 |
| 串行工具在两个并行调用之间 | 三个全部排队 | 前后两个并行调用各自成组，但两组不会互相重叠 | 串行工具只保护自己，不保护整批 |
| 多个相邻的串行工具 | 整批排队 | 仍然一次一个 | 无行为变化 |
| 全局 `ToolSequential` | 一次一个 | 一次一个 | 无行为变化 |
| 并行段里的 `AfterTool` | 本来就可并发 | 仍可并发，且并发范围可能变大 | 中间件必须已经是并发安全的 |
| 串行调用之后还有多个调用 | 所有调用逐个开始 | 串行段结束后，后续并行组可以同时开始 | 消费者不能再假设串行调用之后仍逐个开始 |

最重要的一条风险：**有些工具可能一直依赖「同批次里有串行工具，所以自己也被顺带串行化」这个偶然保护。** 例如两个只读工具共享一个非线程安全的全局缓存，它们本来就不应该并发；在默认并行模式下它们已经是并发的，本次改动只是让这种情况在更多批次里出现。上线前应审计：

- 声明了 `SequentialTool` 的工具的相邻工具；
- 共享全局变量、文件、数据库连接或其他非可重入资源的工具；
- `AfterTool` 中做非并发安全写入的中间件。

### 7.2 需要同步修改的注释和文档

| 位置 | 当前说法 | 建议 |
|---|---|---|
| [agent/exectools.go:18-24](../../agent/exectools.go#L18) | 「any tool ... which downgrades the whole batch」 | 改为「串行工具形成独立段，前后连续调用仍可并发」，并补充段边界的事件顺序 |
| [agent/exectools.go:146-149](../../agent/exectools.go#L146) | 「one such tool downgrades the whole batch」 | 随代码删除或改写 |
| [tool/tool.go:88-92](../../tool/tool.go#L88) | 「the whole batch executes one at a time」 | 改为「该调用单独执行；批次中其他连续调用可以在各自段内并发」 |
| [tool/tool.go:94-96](../../tool/tool.go#L94) | 「forcing its batches to run one at a time」 | 改为「把它放进独立的串行段」 |
| [agent/options.go:95-97](../../agent/options.go#L95) | 「downgrades its whole batch to serial」 | 改为「该工具单独成段，其他调用仍按段并发」 |
| [agent/stepview.go:75-76](../../agent/stepview.go#L75) | 「which implementations force a serial batch」 | 改为「require a serial segment」 |
| [agent/stepview.go:238-239](../../agent/stepview.go#L238) | 「forces its batch to run one call at a time」 | 改为「forces its own call to run alone」 |
| [agent/middleware.go:20-21](../../agent/middleware.go#L20) | 「across a parallel tool batch」 | 可改为「across a parallel segment」，语义更准确 |
| [docs/tasks/plan-step-view.md:403](../../docs/tasks/plan-step-view.md#L403)、[docs/tasks/plan-step-view.md:627](../../docs/tasks/plan-step-view.md#L627) | 历史任务记录里写着「整批降级」 | 不改写历史；在 P0-3 的实现提交里附一条说明，指向本文 |

### 7.3 API 和存储

- **没有公开 API 变化。** `tool.SequentialTool`、`tool.AsSequential`、`ToolExecMode`、`WithToolExecution` 的签名都不变。
- **没有存储格式变化。** `Checkpoint`、`StepSnapshot`、工具执行账本都不新增字段。
- **没有配置变化。** 不新增最大并发数、段超时或其他开关。
- **测试兼容。** 现有的两个调用用例在两种实现下都通过，所以它们可以保留；但相关测试名和注释要改，避免继续宣称「整批降级」。新的三调用用例才是区分新旧行为的关键。

### 7.4 恢复和漂移

暂停批次恢复时，`resumeStepView` 的 `sequential` 表来自恢复时的当前实现。若实现发生变化：

- 旧实现是串行，新实现不是：恢复后该调用可能和同段其他调用并发。
- 旧实现不是串行，新实现是：恢复后该调用单独成段。

`StepSnapshot` 不记录串行标记，因此这种漂移不会被现有的步骤视图漂移检查发现。当前整批实现也有同样的暴露面，但影响是「整批是否串行」；分段后影响是「哪些调用重叠」，更细，因此应在实现说明里写清楚。若要彻底检测，需要把串行标记纳入摘要，属于另一项改动，本文不做。

## 8. 备选方案

| 方案 | 做法 | 结论 |
|---|---|---|
| A. 保持整批串行 | 现状 | 不解决 P0-3；十个只读查询仍被一个写操作拖住。 |
| B. 按调用分段（本文） | 连续非串行调用成组；串行调用单独成段 | 采用。确定、可测试、改动局限在调度层。 |
| C. 读写锁 | 并行调用拿读锁，串行调用拿写锁 | 不采用。原因见下。 |
| D. 全并行，忽略串行声明 | 所有调用都直接并发 | 违反 `SequentialTool` 契约，破坏限流和非可重入资源保护。 |
| E. 每个工具一个信号量 | 按工具配置并发度 | 需要新 API 和配置，且不能表达「只与其他调用互斥」的语义；超出 P0-3。 |
| F. 相邻串行调用合并成一个串行段 | 段内循环执行多个串行调用 | 安全等价，但段的形状不再统一，结算和写权限检查要额外分支；本文选择「一个串行调用一个段」。 |

**为什么不采用读写锁。** Codex 在路由层和并行执行层用读/写锁表达「串行调用与所有调用互斥」，那是可行的实现方式，但移植到这里有几个问题：

1. **等待顺序不确定。** Go 的 `sync.RWMutex` 不保证等待者的获取顺序。模型调用顺序是明确的，用锁会让「先发布的调用先执行」变成调度器行为，难以写出稳定断言。
2. **锁的覆盖范围不清楚。** 要真正保护串行调用，锁必须覆盖 handler、`AfterTool`、持久化完成记录和事件发布；只在 handler 外面加锁会留下重叠窗口。`AfterTool` 还可能调用其他工具或阻塞，容易引入锁顺序问题。
3. **取消时不好收尾。** 分段可以在段之间直接检查写权限并停止后续段；读/写锁需要额外的「关闭后不再获取」逻辑，否则取消后仍可能有等待者在抢锁。
4. **测试不直观。** 分段计划是纯函数，可以直接断言段形状；锁只能通过并发时序间接观察，容易写出依赖 `sleep` 的脆弱用例。

因此本文选择显式分段：串行工具是计划里的一个边界，不参与锁竞争。

## 9. 测试与验收

### 9.1 计划构造函数单元测试

放在 `agent` 包内部测试（因为 `toolCallSegment` 不导出），直接构造 `ToolView{sequential: map[string]bool{...}}`，覆盖：

| 用例 | 输入 | 期望 |
|---|---|---|
| 空批次 | `[]`，并行模式 | 空计划 |
| 全并行 | `[A, B, C]` | 一个并行段 `[0,1,2]` |
| 单个串行 | `[S]` | 一个串行段 `[0]` |
| 串行在末尾 | `[A, B, S]` | `[0,1]`、串行 `[2]` |
| 串行在开头 | `[S, A, B]` | 串行 `[0]`、`[1,2]` |
| 串行在中间 | `[A, S, B]` | `[0]`、串行 `[1]`、`[2]` |
| 多个串行 | `[A, S1, B, S2, C]` | `[0]`、串行 `[1]`、`[2]`、串行 `[3]`、`[4]` |
| 相邻串行 | `[S1, S2]` | 串行 `[0]`、串行 `[1]` |
| 全局串行 | `[A, B]`，`ToolSequential` | 串行 `[0]`、串行 `[1]` |
| 未知名 | `[unknown, A]` | 一个并行段 `[0,1]`（未知名不构成边界，随后由 `callOne` 拒绝） |
| 覆盖性 | 任意组合 | 下标不重不漏，升序 |

后两个用例可以写成表驱动测试，避免为每种组合单独写函数。

### 9.2 行为集成测试

全部放在 `agent_test` 包，使用可控的 channel 和原子计数器，不依赖 `sleep` 来制造并发顺序。测试里的超时只作为「死锁时失败」的保护，不参与断言。

| 编号 | 调用序列 | 断言 |
|---|---|---|
| T1 | `[p1, p2, seq]` | p1、p2 都能启动并重叠（峰值 2）；seq 启动时没有 p1/p2 在运行；结果顺序是 p1、p2、seq |
| T2 | `[p1, p2, seq, p3, p4]` | 两个并行段的峰值都是 2；seq 独占；p1..p4 的结果顺序不变 |
| T3 | `[s1, s2]`（相邻串行） | 峰值 1；执行顺序是 s1、s2 |
| T4 | `[p1, p2]`，全局 `ToolSequential` | 峰值 1；执行顺序是 p1、p2；现有全局串行测试保持不变 |
| T5 | `[p1, injectedSeq, p2]`，注入同名实现 | 分段跟随冻结视图的实现：injectedSeq 单独成段；p1 和 p2 不会与它重叠 |
| T6 | `[p1, p2, seq]`，在 p1/p2 运行期间取消运行 | p1/p2 按取消语义结算；seq 的 handler 调用次数为 0 |
| T7 | `[p1, p2, seq]`，p2 在 handler 内触发租约错误 | 当前段先结算；seq 不启动；返回错误 `errors.Is(err, checkpoint.ErrStaleFencingToken)` |
| T8 | `[p1, p2, seq]`，p1 慢、p2 快 | p2 先完成时，结果仍按 p1、p2 的顺序进入历史；seq 在两者都结算后才启动 |
| T9 | `[p1, seq]`，p1 返回 `State` 操作，seq 读取 `tool.Context.State` | seq 能看到 p1 已经应用的状态 |
| T10 | 含拒绝调用的混合批次 | `ToolReject` 仍在整批结束后串行回调；KV 写入仍随本步检查点提交（现有 [agent/reject_test.go:156](../../agent/reject_test.go#L156) 必须通过） |
| T11 | 暂停恢复后的混合批次 | 批准调用按顺序分段；串行调用仍独占；未批准调用不形成边界 |
| T12 | `[p1, p2, seq, p3]`，记录事件 | 全局 `ToolStarted` 按 p1、p2、seq、p3 的顺序出现；seq 的 `ToolDone` 早于 p3 的 `ToolStarted` |

T1 的关键写法：

```go
// p1、p2 各自在启动时向 started 发送信号，然后等待 release。
// 测试先收两个 started（说明两者确实重叠），再关闭 release。
// 如果实现仍然整批串行，只能收到一个 started，测试用 context 超时失败，
// 而不是一直挂住。
```

`seq` 在 handler 里读取一个「当前运行的并行调用数」原子计数器，断言为 0。计数器必须在 p1/p2 返回前递减，且 `wg.Wait` 在 seq 启动前完成，因此这个断言是稳定的。

T6/T7 的取消和租约触发点要放在段中间，不能只在模型调用前触发；否则所有调用都会在入口检查时被挡住，测不到「段之间停止」的行为。T7 可以复用 [agent/dispatch_guard_test.go:19](../../agent/dispatch_guard_test.go#L19) 里的中间件捕获方式：在 p2 的 handler 内让写保护失败，等当前段结算后由下一段的检查阻止 seq。

### 9.3 现有测试的调整

- `TestSequentialToolDowngradesWholeBatch` 改名为类似 `TestSequentialToolSerializesItselfOnly`，并把调用序列改成 `[p1, p2, seq]`；`AsSequential` 的委托/能力断言可以保留在同一个测试的前半段。
- `TestInjectedSequentialToolDowngradesBatch` 至少扩展为三个调用，或者新增一个独立的混合批次用例；只有两个调用时新旧实现不可区分。
- `TestInjectedSequentialToolReplacingStaticDowngradesBatch` 改名为「同名注入替换后仍串行」，避免继续使用「降级整批」；它仍然测试分类来源，不负责证明新的分段行为。
- `TestInjectedParallelToolReplacingStaticSequentialAllowsParallel` 可以保留，名字仍然准确。
- `agent/seqexec_test.go` 里的 `overlap` 辅助结构可以复用，但新用例不要用 `time.Sleep` 控制顺序。
- 更新 `agent/exectools.go`、`tool/tool.go`、`agent/options.go`、`agent/stepview.go` 的注释（见 7.2）。

### 9.4 反向验证

实现完成后至少做两次反向检查，证明测试确实能区分新旧行为：

1. **临时改回整批串行。** 让 `execTools` 在任何串行调用出现时走旧的整批循环。T1 必须失败（峰值只能是 1），T3/T4 仍然通过。
2. **临时忽略串行标记，全部走并行。** T1 的峰值仍然通过，但 T3/T4 的串行隔离断言必须失败，T7 的后段停止断言也必须失败。
3. **把分段计划改成「串行调用与前后组合并」或「串行调用仍与同段并发」。** 对应的隔离测试必须失败。

每次反向检查只改一处真实逻辑，确认失败信息指向预期断言，然后还原。编译失败不算测试通过，必须看到具体断言失败。

### 9.5 验收门禁

- `go build ./...`
- `go vet ./...`
- `go test ./agent`（包含全部现有用例）
- `go test -race ./agent`（新增的并发路径必须过竞态检测）
- `gofmt` 检查只针对本次修改的文件；不要重排既有的 CRLF 文件。
- 手工核对一次事件序列：`[p1, p2, seq, p3]` 的 `ToolStarted` 顺序、串行段前后的 `ToolDone` 位置、`TurnDone` 只在最后出现一次。

## 10. 实施拆分

建议分三个可独立验证的提交：

### 第 1 步：计划和单元测试

- 新增 `toolCallSegment`、`buildToolCallSegments`（放在 `agent/exectools.go` 或新的 `agent/toolsegments.go`）。
- 新增内部单元测试覆盖 9.1 的表。
- 此步不接入执行器，现有行为不变。

完成标准：所有单元测试通过；计划覆盖性、顺序、串行段长度为 1 的断言都成立。

### 第 2 步：接入执行器

- 用计划替换 `execTools` 的 `needSeq` 分支。
- 保留 `run`、`callOne`、所有锁和返回结构不变。
- 新增 T1、T2、T3、T8 等核心行为测试；调整旧测试名和调用序列。
- 运行现有 `go test ./agent`，确认暂停恢复、失败检查点、动态工具、串行工具、预算、事件顺序都没有回归。

完成标准：T1 在旧实现下失败、在新实现下通过；现有测试全部通过；`-race` 通过。

### 第 3 步：边界、文档和回归

- 新增 T5、T6、T7、T9、T11 等边界测试。
- 更新 7.2 表中的注释和文档；在历史任务文档旁加一条指向本文的说明。
- 做 9.4 的反向验证并记录结果。

完成标准：取消、租约、注入、恢复、拒绝和状态可见性都有固定的回归测试；全仓 `go test ./...` 通过；`go vet` 和 `-race` 通过。

## 11. 风险与待定问题

### 11.1 风险

1. **依赖偶然串行化的工具。** 这是最主要的风险。两个本来共享非可重入资源的工具，之前可能因为旁边有一个串行工具而碰巧没有并发；改动后它们会在同一段里并发。需要在实现前审计所有 `AsSequential` 使用点及其邻居。
2. **`AfterTool` 阻塞会拖住整段。** `AfterTool` 在 `run` 内执行，并行段要等它结束才能进入下一段。它本来就没有单调用超时保护；如果某个中间件很慢，串行段会被推迟。这不是新的阻塞类型：旧串行路径同样要等 `AfterTool` 返回才执行下一个调用；分段只是把这种等待明确放在段边界上。
3. **被放弃的 handler 不受分段保护。** 超时只能让循环不再等待，不能停止 handler。串行段开始后，前一段被放弃的 handler 可能仍在运行。
4. **事件模式变化。** 旧行为里，含串行工具的批次所有调用逐个开始；新行为里，串行段之后的多个调用可以同时开始。按 `ToolStarted`/`ToolDone` 实际到达渲染的消费者不受影响；仍假设「含串行工具的批次一定逐个开始」的消费者需要调整。
5. **恢复时的串行标记漂移。** 见 7.4；快照不记录标记，无法通过现有漂移检查发现。
6. **保守分类带来的吞吐损失。** 已被持久化完成、最终不会执行 handler 的串行调用，仍然会阻塞一段。这是为了避免在执行前做额外的存储查询；如果将来成为瓶颈，可以在计划构建前批量读取完成状态，但那会扩大改动面。

### 11.2 待定问题

1. **相邻串行调用是否值得合并成一个串行段？** 当前选择不合并，保持「串行段只有一个调用」的简单不变量。合并可以减少段对象，但会让取消和结果映射多一层循环，收益很小。
2. **是否需要把串行标记写入 `StepSnapshot` 摘要？** 这能检测恢复漂移，但会改变检查点摘要，属于 P0-1 的延伸，不放本次。
3. **是否需要在事件里暴露段边界？** 目前不需要；`ToolStarted`/`ToolDone` 的顺序已经足够推导。若可观测性有需求，可以以后新增事件，不影响本次调度。
4. **是否为并行段增加最大并发数？** 这是独立的资源控制问题；当前行为是「一批启动所有调用」，本次保持。
5. **串行工具能否按调用参数决定是否独占？** 现在 `SequentialExecution()` 没有参数，无法表达「这个调用需要独占，那个不需要」。若将来需要，必须扩展能力接口，不能在调度层猜测。

## 12. 代码锚点

| 主题 | 位置 |
|---|---|
| 当前整批降级 | [agent/exectools.go:146-169](../../agent/exectools.go#L146) |
| 当前并行分支 | [agent/exectools.go:171-185](../../agent/exectools.go#L171) |
| 一次调用的执行 | [agent/exectools.go:65](../../agent/exectools.go#L65)、[agent/exectools.go:312](../../agent/exectools.go#L312) |
| 结果按序返回 | [agent/exectools.go:188](../../agent/exectools.go#L188) |
| 状态写入锁 | [agent/exectools.go:49](../../agent/exectools.go#L49)、[agent/exectools.go:106](../../agent/exectools.go#L106)、[core/state.go:77](../../core/state.go#L77) |
| 超时和放弃 | [agent/exectools.go:203](../../agent/exectools.go#L203)、[agent/exectools.go:233](../../agent/exectools.go#L233)、[agent/exectools.go:286](../../agent/exectools.go#L286) |
| 冻结视图和串行标记 | [agent/stepview.go:77](../../agent/stepview.go#L77)、[agent/stepview.go:240](../../agent/stepview.go#L240)、[agent/stepview.go:271](../../agent/stepview.go#L271)、[agent/stepview.go:475](../../agent/stepview.go#L475) |
| 恢复视图 | [agent/stepview.go:340](../../agent/stepview.go#L340) |
| 请求、审批、执行 | [agent/loop.go:303](../../agent/loop.go#L303)、[agent/loop.go:309](../../agent/loop.go#L309)、[agent/loop.go:418](../../agent/loop.go#L418)、[agent/loop.go:442](../../agent/loop.go#L442) |
| 拒绝回调时机 | [agent/loop.go:443](../../agent/loop.go#L443)、[agent/middleware.go:75](../../agent/middleware.go#L75) |
| 全局串行和单调用超时 | [agent/options.go:95](../../agent/options.go#L95)、[agent/options.go:100](../../agent/options.go#L100)、[agent/options.go:157](../../agent/options.go#L157) |
| 串行能力契约 | [tool/tool.go:88](../../tool/tool.go#L88) |
| 写权限检查的职责 | [agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13) |
| 事件和总线 | [core/event.go:55](../../core/event.go#L55)、[bus/bus.go:109](../../bus/bus.go#L109) |
| 持久化完成记录 | [agent/durable_runtime.go:139](../../agent/durable_runtime.go#L139)、[agent/exectools.go:353](../../agent/exectools.go#L353) |
| 现有相关测试 | [agent/seqexec_test.go:68](../../agent/seqexec_test.go#L68)、[agent/dynamictool_test.go:272](../../agent/dynamictool_test.go#L272)、[agent/stepview_test.go:531](../../agent/stepview_test.go#L531)、[agent/reject_test.go:156](../../agent/reject_test.go#L156)、[agent/dispatch_guard_test.go:19](../../agent/dispatch_guard_test.go#L19) |
