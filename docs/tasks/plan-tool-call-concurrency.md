# P0-3 工具调用分段并发实施计划

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文只覆盖 P0-3。

**目标：**按照 [工具调用并发设计稿](../analysis/tool-call-concurrency-design.md) §5，把「批次里出现一个串行工具就整批排队」改成「按模型调用顺序切成确定的分段」：连续的非串行调用组成一个并行段，每个串行调用单独成段，段按顺序执行。十个只读查询加一个写操作时，十个只读查询仍可以并发，写操作独自执行。

**架构：**新增包内纯函数 `buildToolCallSegments`，只读冻结视图里的串行标记。`execTools` 按计划遍历分段：串行段在循环 goroutine 上直接调用现有的 `run`，并行段为每个调用启动 goroutine，并在段末 `wg.Wait`。`run`、`callOne`、状态写入锁、持久化完成记录、结果顺序和事件发布都不变。全局 `ToolSequential` 改成「每个调用一个串行段」，行为与旧实现一致。

**技术栈：**Go 1.25（本机 go1.26.4）、标准库、现有 mock LLM、checkpoint Memory、bus。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-22。
- 基线：`be2680c`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./agent -count=1` 确认基线。
- 设计依据：[tool-call-concurrency-design.md](../analysis/tool-call-concurrency-design.md) §5–§11。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：P0-3 工具调用分段调度。P0-1 的步骤视图和 P0-2 的回合语义已经体现在当前基线中，不在本计划内。

当前工作区有未跟踪的分析文档、`.scratch/` 目录和几份未入库的计划文档。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。本文和设计稿的提交由用户另行安排；任务卡里的提交只包含该卡列出的代码和测试。

## 1. 现有能力与实施边界

### 1.1 现在一个工具批次怎么执行

1. 模型返回工具调用后，循环先调用 `CheckToolPolicy`，再逐个调用 `BeforeTool`；审批门通过后把整批调用交给 `execTools`（[agent/loop.go:418](../../agent/loop.go#L418)、[agent/loop.go:422](../../agent/loop.go#L422)、[agent/loop.go:442](../../agent/loop.go#L442)）。
2. `execTools` 要求当前步骤有冻结视图；进入函数时先检查一次写权限（[agent/exectools.go:39](../../agent/exectools.go#L39) 到 [agent/exectools.go:45](../../agent/exectools.go#L45)）。
3. 结果、是否已上报、指令三个切片按原调用下标预分配；状态写入锁 `stateMu` 也在此时创建（[agent/exectools.go:46](../../agent/exectools.go#L46) 到 [agent/exectools.go:49](../../agent/exectools.go#L49)）。
4. 一次调用的执行封装在 `run` 闭包里：检查写权限、派生调用上下文、启动 handler goroutine、等待或超时放弃、在 `stateMu` 下应用状态、执行 `AfterTool`、记录持久化完成、写入结果、发布 `ToolDone`（[agent/exectools.go:65](../../agent/exectools.go#L65) 到 [agent/exectools.go:144](../../agent/exectools.go#L144)）。
5. 旧的调度逻辑先扫描整批调用：只要有一个调用指向串行工具，整批改成逐个执行；否则整批并发（[agent/exectools.go:146](../../agent/exectools.go#L146) 到 [agent/exectools.go:185](../../agent/exectools.go#L185)）。
6. 返回时 `reportedToolResults` 按原下标升序过滤，模型看到的结果顺序始终是调用顺序（[agent/exectools.go:188](../../agent/exectools.go#L188)）。
7. `ToolReject` 在整批结束后由循环串行回调，随后写本步检查点并发布 `TurnDone`（[agent/loop.go:443](../../agent/loop.go#L443)、[agent/loop.go:453](../../agent/loop.go#L453)、[agent/loop.go:457](../../agent/loop.go#L457)）。
8. 暂停批次的恢复只把批准的调用交给 `execTools`，顺序仍是暂停批次里的原顺序（[agent/hitl.go:329](../../agent/hitl.go#L329)）；恢复视图的串行标记来自恢复时的当前实现（[agent/stepview.go:340](../../agent/stepview.go#L340)、[agent/stepview.go:377](../../agent/stepview.go#L377)）。
9. `ToolStarted` 在调用开始前发布，`ToolDone` 在调用结算后发布；Bus 本身并发安全（[core/event.go:55](../../core/event.go#L55)、[core/event.go:64](../../core/event.go#L64)、[bus/bus.go:109](../../bus/bus.go#L109)）。

### 1.2 本计划要解决的问题

- **一个串行调用拖住整批。** 十个只读查询和一个写操作同批时，十一个调用全部排队，只读查询之间也无法并发。
- **串行能力只能表达「整批」**。`SequentialTool` 的契约是「批次里有我，整批逐个执行」，无法表达「只有这个调用需要独占，其他调用可以并发」。
- **两个调用的旧测试区分不了新旧行为。** `[plain, seq]` 在整批串行和分段两种实现下最大并发都是 1，必须有三个及以上调用、并且串行调用两侧都有调用，才能观察差异。
- **事件模式会变化。** 旧行为里，含串行工具的批次所有调用逐个开始；新行为里，串行段之后的多个调用可以同时开始。依赖「含串行工具的批次一定逐个开始」的消费者需要调整。

### 1.3 可观察行为变化

| 场景 | 现在 | 本计划之后 | 原因 |
| --- | --- | --- | --- |
| 十个只读 + 一个写，写排最后 | 十一个全部排队 | 十个只读在同一并行段里并发，写单独成段 | 连续非串行调用组成并行段 |
| `[A, S, B, C]` | 四个逐个执行 | A 独自执行，S 独自执行，B、C 并发 | S 是边界，B、C 在同一个并行段 |
| `[A, S, B]` | 三个逐个执行 | A 独自执行，S 独自执行，B 独自执行 | S 两侧的并行组不能互相重叠，最大并发仍是 1 |
| 多个相邻串行调用 | 逐个执行 | 仍然逐个执行，每个调用一个串行段 | 相邻串行段不合并 |
| 全局 `ToolSequential` | 逐个执行 | 逐个执行，每个调用一个串行段 | 全局模式生成全串行计划 |
| 串行段之后的多个调用 | 逐个开始 | 可以同时开始 | 后续并行段统一启动 |
| 并行段里的 `AfterTool` | 本来就可并发 | 仍可并发，并发范围可能变大 | 并行段边界替代整批边界 |
| 被拒绝或从持久化记录重放的调用 | 参与整批判定 | 仍按名字参与分段，可能形成边界 | 分类是保守的，不预判调用是否执行 |

最后一条是刻意的：计划只看冻结视图里的名字，不先查这个调用会不会被拒绝、会不会命中已完成的持久化结果。正确性不受影响，只是少了一次本可以并发的机会。

### 1.4 不做什么

- 不采用读写锁。Go 的 `sync.RWMutex` 不保证等待顺序，锁的覆盖范围要包含 handler、`AfterTool`、持久化完成和事件发布，取消时也需要额外的关闭逻辑；分段计划是纯函数，可以直接断言形状。
- 不做按工具配置并发度的信号量，不新增最大并发数、段超时或其他开关。
- 不合并相邻的串行调用。每个串行调用单独一个段，保持「串行段长度必定为 1」的简单不变量。
- 不修改 `run`、`callOne`、超时放弃、状态写入锁、持久化完成记录和拒绝回调时机。
- 不修改检查点、`StepSnapshot`、工具执行账本的字段，也不把串行标记写进摘要。
- 不新增事件类型，不在事件里暴露段边界。
- 不改变 `tool.SequentialTool`、`tool.AsSequential`、`ToolExecMode`、`WithToolExecution` 的签名。
- 不按调用参数决定是否独占；`SequentialExecution()` 没有参数，调度层不猜测。

## 2. 已核实的事实

下表是实施前核对过的代码位置。执行时如果行号已经漂移，按函数名和代码内容定位，不要按行号硬改。

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `execTools` 要求冻结视图并在入口检查写权限 | [agent/exectools.go:39](../../agent/exectools.go#L39) | 保持不变 |
| 结果数组按原下标预分配 | [agent/exectools.go:46](../../agent/exectools.go#L46) | 分段只传下标，不复制调用 |
| 状态写入锁 `stateMu` | [agent/exectools.go:49](../../agent/exectools.go#L49) | 并行段的状态写入仍受保护 |
| `run` 闭包负责一次调用的全部结算 | [agent/exectools.go:65](../../agent/exectools.go#L65) | 串行段和并行段都复用它 |
| 状态操作在 `stateMu` 下应用 | [agent/exectools.go:105](../../agent/exectools.go#L105) 到 [agent/exectools.go:109](../../agent/exectools.go#L109) | T9 依赖这个顺序 |
| `AfterTool` 在 `run` 内执行 | [agent/exectools.go:117](../../agent/exectools.go#L117) | 段末等待会包含中间件 |
| 持久化完成记录在 `run` 内写入 | [agent/exectools.go:125](../../agent/exectools.go#L125) 到 [agent/exectools.go:131](../../agent/exectools.go#L131) | 分段不改变完成记录 |
| `ToolDone` 在 `run` 末尾发布 | [agent/exectools.go:143](../../agent/exectools.go#L143) | T12 的顺序断言 |
| 旧的整批串行分支 | [agent/exectools.go:146](../../agent/exectools.go#L146) 到 [agent/exectools.go:169](../../agent/exectools.go#L169) | 本计划替换它 |
| 旧的整批并行分支 | [agent/exectools.go:171](../../agent/exectools.go#L171) 到 [agent/exectools.go:185](../../agent/exectools.go#L185) | 本计划替换它 |
| 结果按原下标顺序返回 | [agent/exectools.go:188](../../agent/exectools.go#L188) | T8 依赖它 |
| 单调用超时和放弃 | [agent/exectools.go:203](../../agent/exectools.go#L203)、[agent/exectools.go:208](../../agent/exectools.go#L208)、[agent/exectools.go:286](../../agent/exectools.go#L286) | 不在本计划改动范围 |
| 一次调用的解析、claim 和 handler | [agent/exectools.go:312](../../agent/exectools.go#L312)、[agent/exectools.go:353](../../agent/exectools.go#L353) | 不在本计划改动范围 |
| 冻结视图的串行标记 | [agent/stepview.go:77](../../agent/stepview.go#L77)、[agent/stepview.go:240](../../agent/stepview.go#L240)、[agent/stepview.go:271](../../agent/stepview.go#L271)、[agent/stepview.go:475](../../agent/stepview.go#L475) | 分段的唯一分类来源 |
| 恢复视图按当前实现填串行标记 | [agent/stepview.go:340](../../agent/stepview.go#L340)、[agent/stepview.go:377](../../agent/stepview.go#L377) | T5、T11 和恢复漂移说明 |
| 请求构造和视图冻结 | [agent/loop.go:303](../../agent/loop.go#L303)、[agent/loop.go:309](../../agent/loop.go#L309) | 串行标记在调用前已经冻结 |
| 循环调用 `execTools` 和 `ToolReject` | [agent/loop.go:442](../../agent/loop.go#L442)、[agent/loop.go:443](../../agent/loop.go#L443) | 拒绝时机不变 |
| 指令折叠和 `TurnDone` | [agent/loop.go:453](../../agent/loop.go#L453)、[agent/loop.go:457](../../agent/loop.go#L457) | 结果和事件顺序不变 |
| `TurnDone` 每步发布一次，不是整次运行只发一次 | [agent/loop.go:364](../../agent/loop.go#L364)、[agent/loop.go:380](../../agent/loop.go#L380)、[agent/loop.go:404](../../agent/loop.go#L404)、[agent/loop.go:457](../../agent/loop.go#L457) | 修正设计稿 §9.5 的旧说法 |
| 恢复批次把批准调用交给 `execTools` | [agent/hitl.go:329](../../agent/hitl.go#L329) | 未批准调用不参与分段 |
| 全局执行模式的选项 | [agent/options.go:95](../../agent/options.go#L95) 到 [agent/options.go:98](../../agent/options.go#L98)、[agent/options.go:157](../../agent/options.go#L157) 到 [agent/options.go:165](../../agent/options.go#L165) | 全局模式语义保持 |
| 串行能力契约 | [tool/tool.go:88](../../tool/tool.go#L88) 到 [tool/tool.go:96](../../tool/tool.go#L96) | 注释需要同步修改 |
| 写权限检查只查取消、停止和租约 | [agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13) 到 [agent/dispatch_guard.go:49](../../agent/dispatch_guard.go#L49) | T6、T7 的触发点 |
| `AfterTool` 并发契约 | [agent/middleware.go:20](../../agent/middleware.go#L20)、[agent/middleware.go:21](../../agent/middleware.go#L21) | 注释需要同步修改 |
| `ToolReject` 在整批后串行回调 | [agent/middleware.go:75](../../agent/middleware.go#L75) 到 [agent/middleware.go:89](../../agent/middleware.go#L89) | T10 的时机断言 |
| `ToolStarted` 和 `ToolDone` | [core/event.go:55](../../core/event.go#L55)、[core/event.go:64](../../core/event.go#L64) | T12 |
| 现有两调用串行测试 | [agent/seqexec_test.go:68](../../agent/seqexec_test.go#L68) | 需要改名并扩成三调用 |
| 现有并行重叠测试 | [agent/seqexec_test.go:97](../../agent/seqexec_test.go#L97) | 必须继续通过 |
| 注入串行工具的现有测试 | [agent/dynamictool_test.go:272](../../agent/dynamictool_test.go#L272) | 需要扩成混合批次 |
| 同名注入替换后的串行分类测试 | [agent/stepview_test.go:531](../../agent/stepview_test.go#L531)、[agent/stepview_test.go:553](../../agent/stepview_test.go#L553) | 改名或保留 |
| 拒绝回调串行和 KV 落检查点 | [agent/reject_test.go:156](../../agent/reject_test.go#L156) | T10 必须继续通过 |
| 写权限丢失后的分发停止 | [agent/dispatch_guard_test.go:19](../../agent/dispatch_guard_test.go#L19)、[agent/dispatch_guard_test.go:371](../../agent/dispatch_guard_test.go#L371)、[agent/dispatch_guard_test.go:439](../../agent/dispatch_guard_test.go#L439) | T7 复用捕获和触发方式 |
| 暂停、批准和恢复辅助 | [agent/resume_exec_test.go:22](../../agent/resume_exec_test.go#L22)、[agent/resume_exec_test.go:58](../../agent/resume_exec_test.go#L58) | T11 复用 |
| 审批门测试中间件 | [agent/phase1_interrupt_test.go:91](../../agent/phase1_interrupt_test.go#L91)、[agent/phase1_interrupt_test.go:100](../../agent/phase1_interrupt_test.go#L100) | T11 的第一阶段暂停 |
| 全局串行下 `AfterTool` 失败仍保留整批结果 | [agent/phase1_failure_test.go:253](../../agent/phase1_failure_test.go#L253) | 必须继续通过 |
| 全局串行下慢调用不阻塞后续调用 | [agent/tooltimeout_abandon_test.go:171](../../agent/tooltimeout_abandon_test.go#L171) | 必须继续通过 |

### 2.1 对设计稿的两处按当前代码修正

1. **`TurnDone` 不是整次运行只发一次。** P0-2 之后，每个步骤结束都会发布一次 `TurnDone`（[agent/loop.go:364](../../agent/loop.go#L364)、[agent/loop.go:380](../../agent/loop.go#L380)、[agent/loop.go:404](../../agent/loop.go#L404)、[agent/loop.go:457](../../agent/loop.go#L457)）。T12 断言「工具步一次、终稿步一次，总数不随调用数增加」，不断言整次运行只有一次。
2. **写权限检查不保护状态写入。** `checkWriteAccess` 只检查取消、停止标记和租约（[agent/dispatch_guard.go:13](../../agent/dispatch_guard.go#L13) 到 [agent/dispatch_guard.go:49](../../agent/dispatch_guard.go#L49)）；并行调用写入 `State` 的安全来自 `stateMu`（[agent/exectools.go:49](../../agent/exectools.go#L49)、[agent/exectools.go:105](../../agent/exectools.go#L105)）。分段不改变这一点，T9 只验证顺序，不把 `checkWriteAccess` 当成状态锁。

## 3. 锁定的设计决定

### D1 计划只保存原调用下标

`toolCallSegment.indexes` 保存 `calls` 切片里的下标，不复制 `core.ToolCall`。`results`、`reported`、`dirs` 本来就按原下标存放，直接用下标最省事，也避免参数 JSON 被复制多份。

**为什么：**结果顺序、指令折叠和拒绝分类都依赖原下标。复制调用会让「计划里的调用」和「结果数组里的调用」出现两份身份。

### D2 连续非串行调用组成并行段，串行调用单独成段

遍历调用顺序：

- 遇到非串行调用，追加到当前并行组。
- 遇到串行调用，先把当前并行组结算成一个段（如果非空），再为这个串行调用生成一个 `serial` 段，长度为 1。
- 遍历结束后结算最后一个并行组。

**为什么：**这是设计稿 §5.3 选定的确定性形状。串行调用是计划里的边界，不参与锁竞争；段与段按顺序执行，所以串行调用两侧的调用不会与它重叠，但串行调用之后的多个调用可以在同一个并行段里重叠。

**影响：**`[A, S, B, C]` 中 A 和 S 各自独占，B、C 可以重叠；`[A, S, B]` 中 A、S、B 都独占，最大并发仍为 1。后者不是退步，而是串行独占语义的直接结果。

### D3 全局 `ToolSequential` 生成全串行计划

`mode == ToolSequential` 时，直接为每个调用生成一个 `serial` 段，不走通用分组循环。

**为什么：**执行器只需要一种「串行段」路径，事件顺序、写权限检查和旧实现一致；也避免在通用循环里为全局模式加特例。

**影响：**全局串行下的最大并发、执行顺序、超时行为和现有测试都不变。

### D4 分段只属于调度层

`run` 的职责仍然是「执行并结算一次调用」，不接收段参数，也不在内部判断串行。段只决定「什么时候调用 `run`、在哪个 goroutine 上调用、什么时候等待」。

**为什么：**`run` 里包含状态写入、`AfterTool`、持久化完成记录和事件发布，任何一处提前或延后都会改变可观察行为。保持 `run` 不变，才能把改动局限在调度层。

### D5 段按顺序执行，并行段在段末等待

- 串行段在循环 goroutine 上直接调用 `run`，不额外启动 goroutine。
- 并行段为每个调用启动一个 goroutine，段末 `wg.Wait()`。
- `AfterTool`、持久化完成记录和 `ToolDone` 都在 `run` 内，因此它们也包含在等待范围内。

**为什么：**下一段的第一个 `ToolStarted` 必须发生在前一段所有调用结算之后。这样，只要串行调用发布了 `ToolDone`，它一定早于下一段的 `ToolStarted`，消费者可以依赖这个边界。

### D6 写权限检查失败停止后续段

- 串行段启动前检查失败：记录错误，停止整个批次，不再进入后续段。
- 并行段启动某个调用前检查失败：记录错误，停止本段后续调用的启动，等已经启动的调用全部结束后，不再进入下一段。

**为什么：**与旧的并行分支「检查失败就停止继续分发」一致。已经启动的调用仍要结算，否则结果和检查点会缺一块。

### D7 结果、指令、拒绝和错误的返回结构不变

- `results[i]` 仍对应 `calls[i]`，`reportedToolResults` 仍按下标升序过滤。
- `dirs[i]` 仍对应 `calls[i]`，批次结束后仍由 `core.Resolve(dirs...)` 按模型顺序折叠。
- `rejects` 仍按完成顺序追加，`ToolReject` 仍在整批结束后串行回调。
- 最终错误仍是 `errors.Join(append(afterErrs, runErrs...)...)`。

**为什么：**这些是模型历史和中间件已经依赖的契约，分段只改变并发形状，不改变结算结果。

### D8 恢复批次只对批准的调用分段

`runResumed` 只把批准的调用传给 `execTools`，未批准的调用不进入分段计划。串行标记来自恢复视图里的当前实现。

**为什么：**被拒绝的调用已经由 `deniedResult` 在批次外生成，不执行 handler；让它们形成边界只会无谓地拆散并行段。恢复视图的串行标记漂移是已知限制，见 §8.5。

### D9 分类保守，不预判调用是否执行

计划只看 `ToolView.Sequential(name)`。一个最终会被拒绝、参数非法或命中已完成持久化记录的串行调用，仍然形成边界。

**为什么：**在执行前判断调用是否会执行，需要额外的存储查询或参数预检，会把改动扩大到 `callOne` 和持久化层。当前选择用一点吞吐换确定的调度形状。

### D10 不新增公开 API、存储字段或配置

`toolCallSegment` 和 `buildToolCallSegments` 都不导出。`ToolExecMode`、`WithToolExecution`、`tool.SequentialTool`、`tool.AsSequential` 的签名和语义不变。检查点、`StepSnapshot` 和工具执行账本不新增字段。

## 4. 内部契约

### 4.1 计划类型和构造函数

```go
// agent/toolsegments.go

type toolCallSegment struct {
    serial  bool
    indexes []int
}

// buildToolCallSegments 是纯函数：不读运行状态，不启动 goroutine，不发布事件。
func buildToolCallSegments(calls []core.ToolCall, tools *ToolView, mode ToolExecMode) []toolCallSegment
```

不变量：

1. 所有段的 `indexes` 合起来恰好覆盖 `0..len(calls)-1`，每个下标出现一次，段内升序，段间顺序也升序。
2. `serial` 为真时，`indexes` 长度必定为 1。
3. 空批次返回 nil。
4. 全局 `ToolSequential` 下，每个段都是串行段，段数等于调用数。
5. 非全局模式下，未知名字不构成边界；它和相邻的非串行调用留在同一个并行段里，之后由 `callOne` 拒绝。
6. 函数是纯函数：同样的 `calls`、`tools`、`mode` 一定得到同样的计划。

### 4.2 执行器形状

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

实现时保留 `execTools` 现有的入口检查和 `run`、`callOne`、锁、返回结构。上面的变量名可以按最终代码调整，但段顺序、等待位置和错误路径不能变。

### 4.3 事件契约

- `ToolStarted` 仍按模型调用顺序发布。并行段内每发布一个就启动一个 goroutine，所以同一并行段里快的调用可能在慢的调用 `ToolStarted` 之前就发布 `ToolDone`；这与当前并行分支一致。
- 串行调用如果发布了 `ToolDone`，它一定早于下一段的 `ToolStarted`。
- `ToolUpdate` 仍可能在 handler 运行期间随时发布，包括被超时放弃的 handler 在后续段继续发布。
- `ToolReject` 仍在整批结束后串行回调；`TurnDone` 仍按步骤发布，不按调用发布。

## 5. 交付顺序与依赖

```text
TC-01 → TC-02 → TC-03
```

按编号顺序执行。TC-01 只新增计划和单元测试，不接入执行器，现有行为不变。TC-02 接入执行器并调整核心测试。TC-03 补边界测试、注释、反向验证和全量门禁。

| 任务 | 可独立审阅的交付 | 建议规模 |
| --- | --- | --- |
| TC-01 | `toolCallSegment`、`buildToolCallSegments`、单元测试 | 0.5–1 天 |
| TC-02 | 执行器接入、T1/T2/T3/T4/T8、旧测试改名 | 1–1.5 天 |
| TC-03 | T5/T6/T7/T9/T10/T11/T12、注释、反向验证、全量验收 | 1–1.5 天 |

规模是执行安排参考，不是时限承诺。

### 5.1 开工前审计

先确认仓库里没有依赖「整批降级」的生产代码：

```bash
git status --short
git log -1 --oneline
grep -rn "AsSequential\|SequentialExecution" --include=*.go . | grep -v "_test.go"
go test ./agent -count=1
```

当前核对结果：生产代码里只有 `tool.AsSequential` 的定义（[tool/tool.go:96](../../tool/tool.go#L96)）、`isSequentialTool` 的判断（[agent/stepview.go:475](../../agent/stepview.go#L475)）和执行器的串行判断（[agent/exectools.go:146](../../agent/exectools.go#L146)）；仓库内没有内置工具声明 `SequentialTool`，只有测试和 `examples/middleware/main.go:39` 的说明文字提到串行模式。外部使用者可能有自己的串行工具，变更说明里要提醒：串行工具不再顺带串行化同批的其他调用。

基线测试如果已经失败，先停下来报告，不要带着失败开始第一张卡。

## 6. 任务卡

### TASK-TC-01：新增分段计划与单元测试

**依赖：**无。**决策：**D1、D2、D3、D10。

**文件：**新增 `agent/toolsegments.go`、`agent/toolsegments_internal_test.go`。

- [ ] 先写表驱动测试 `TestBuildToolCallSegments`，覆盖下面的输入和期望。测试在 `package agent` 内部，直接构造 `ToolView{sequential: map[string]bool{...}}`，不经过完整的 agent 运行。

| 用例 | 调用序列 | 模式 | 期望段 |
| --- | --- | --- | --- |
| 空批次 | `[]` | 并行 | 空计划 |
| 全并行 | `[a, b, c]` | 并行 | 并行 `[0,1,2]` |
| 单个串行 | `[s]` | 并行 | 串行 `[0]` |
| 串行在末尾 | `[a, b, s]` | 并行 | 并行 `[0,1]`、串行 `[2]` |
| 串行在开头 | `[s, a, b]` | 并行 | 串行 `[0]`、并行 `[1,2]` |
| 串行在中间 | `[a, s, b]` | 并行 | 并行 `[0]`、串行 `[1]`、并行 `[2]` |
| 多个串行 | `[a, s1, b, s2, c]` | 并行 | 并行 `[0]`、串行 `[1]`、并行 `[2]`、串行 `[3]`、并行 `[4]` |
| 相邻串行 | `[s1, s2]` | 并行 | 串行 `[0]`、串行 `[1]` |
| 全局串行 | `[a, b]` | `ToolSequential` | 串行 `[0]`、串行 `[1]` |
| 未知名 | `[unknown, a]` | 并行 | 并行 `[0,1]` |
| 同名串行重复 | `[s, s]` | 并行 | 串行 `[0]`、串行 `[1]` |

- [ ] 写不变量测试 `TestBuildToolCallSegmentsInvariants`：用 4 个名字和所有串行子集（16 种）覆盖两种模式，断言：
  - 每个下标恰好出现一次，段内升序，段间顺序也升序；
  - 没有空段；
  - 串行段的长度必定为 1；
  - 全局模式下所有段都是串行段；
  - 并行模式下，任意两个相邻的并行段之间一定隔着一个串行段。
- [ ] 实现 `agent/toolsegments.go`，只导入 `core`。`buildToolCallSegments` 对 nil `tools` 安全（`ToolView.Sequential` 本身对 nil 接收者返回 false）。
- [ ] `flush` 把当前组放入计划后必须把组置空，避免后续 `append` 复用底层数组改写已放入计划的 `indexes`。
- [ ] 不要在本卡接入 `execTools`，不要修改任何现有文件。

验证：

```bash
go test ./agent -run 'TestBuildToolCallSegments' -count=1
go build ./...
go vet ./agent
go test ./agent -count=1
```

**完成标准：**表驱动用例和不变量用例全部通过；空批次返回 nil；全局模式每个调用一个串行段；未知名不构成边界；现有 agent 测试全部通过（本卡不改变行为）。

**提交：**`feat(agent): plan tool call segments`

### TASK-TC-02：接入执行器并调整核心测试

**依赖：**TC-01。**决策：**D2、D4、D5、D6、D7。

**文件：**修改 `agent/exectools.go`；新增 `agent/toolsegments_test.go`；修改 `agent/seqexec_test.go`、`agent/dynamictool_test.go`、`agent/stepview_test.go`（只改测试名、调用序列和注释）。

**先写失败测试：**

- [ ] T1 `TestSegmentedBatchRunsParallelCallsAroundSerial`：调用序列 `[p1, p2, seq]`。p1、p2 在 handler 里增加活动计数、向 `started` 发送自己的名字，然后等待 `release`；测试用协调 goroutine 收满两个 `started` 后关闭 `release`。seq 在 handler 里读取活动计数，断言为 0。运行结束后读取最新检查点的最后一条工具消息，断言结果顺序是 p1、p2、seq；并行段峰值是 2。
- [ ] T2 `TestSegmentedBatchRunsGroupsOnBothSidesOfSerial`：调用序列 `[p1, p2, seq, p3, p4]`。第一组收满两个 `started` 后释放；seq 断言活动计数为 0，并记录 `seqFinished`；第二组在启动时断言 `seqFinished` 已为真，收满两个 `started` 后释放。断言两个并行段的峰值都是 2，结果顺序是 p1..p4。
- [ ] T3 `TestAdjacentSerialCallsStaySequential`：调用序列 `[s1, s2]`。复用 [agent/seqexec_test.go:18](../../agent/seqexec_test.go#L18) 的 `overlap` 统计峰值；在测试文件里写一个本地 helper，handler 启动时把名字追加到带锁切片，然后短睡眠返回。短睡眠只用于放大重叠窗口，不参与顺序控制。断言峰值 1，handler 执行顺序是 s1、s2。
- [ ] T4 `TestGlobalSequentialModeStaysSerial`：调用序列 `[p1, p2]`，使用 `agent.WithToolExecution(agent.ToolSequential)`。复用 T3 的本地 helper 和 `overlap`，断言峰值 1，执行顺序是 p1、p2。
- [ ] T8 `TestSegmentResultsKeepModelOrder`：调用序列 `[slow, fast, seq]`。slow 启动后等待释放；fast 启动后立即返回；协调 goroutine 先收满 slow、fast 的启动信号，再等 fast 完成，然后释放 slow。seq 启动时断言 slow、fast 都已结算。最终结果顺序是 slow、fast、seq，证明完成顺序不会改写模型看到的历史顺序。
- [ ] 所有等待启动信号或完成信号的协调 goroutine 都用带超时的 `select`；超时后 `t.Fatal`，并说明期望的启动数量或顺序。超时只用于防止旧实现或死锁让测试挂住，不参与正常断言。
- [ ] 把 `TestSequentialToolDowngradesWholeBatch` 改名为 `TestSequentialToolSerializesItselfOnly`，调用序列改成 `[p1, p2, seq]`。保留前半段对 `AsSequential` 委托和 `SequentialTool` 能力的断言，后半段改为断言 p1、p2 可以重叠、seq 不重叠。
- [ ] 把 `TestInjectedSequentialToolDowngradesBatch` 改名为 `TestInjectedSequentialToolSerializesItsOwnSegment`，至少改成三个调用：两个普通调用夹着一个注入的串行调用。断言两个普通调用可以重叠，串行调用启动时活动计数为 0。
- [ ] 把 `TestInjectedSequentialToolReplacingStaticDowngradesBatch` 改名为 `TestInjectedSequentialToolReplacingStaticSerializes`，更新注释。它仍然测试「同名注入替换后的分类来源」，不负责证明新的分段形状。
- [ ] `TestInjectedParallelToolReplacingStaticSequentialAllowsParallel` 的名字和断言保持，不需要改。
- [ ] 更新 `agent/dynamictool_test.go`、`agent/stepview_test.go` 里仍写着「降级整批」的测试注释。

**实现要求：**

- [ ] 用 §4.2 的分段循环替换 [agent/exectools.go:146](../../agent/exectools.go#L146) 到 [agent/exectools.go:185](../../agent/exectools.go#L185) 的 `needSeq` 分支和并行分支。
- [ ] 保留入口的 `lc.view == nil` 检查和 `checkWriteAccess` 检查。
- [ ] 串行段在循环 goroutine 上直接调用 `run`；并行段为每个调用启动 goroutine，并在段末 `wg.Wait()`。
- [ ] 串行段启动前检查失败：记录错误并停止整个批次。
- [ ] 并行段启动某个调用前检查失败：记录错误，停止本段后续启动，等已经启动的调用结束后不再进入下一段。
- [ ] 保留 `results`、`reported`、`dirs`、`rejects`、`afterErrs`、`runErrs` 的现有写入方式和最终返回值。
- [ ] 更新 `execTools` 顶部的注释（[agent/exectools.go:18](../../agent/exectools.go#L18) 到 [agent/exectools.go:24](../../agent/exectools.go#L24)）：说明串行工具形成独立段，前后连续调用仍可在各自段内并发；补充「串行段的 `ToolDone` 早于下一段的 `ToolStarted`」。
- [ ] 删除旧的 `needSeq` 注释（[agent/exectools.go:146](../../agent/exectools.go#L146) 到 [agent/exectools.go:149](../../agent/exectools.go#L149)）。
- [ ] 本卡不改 `tool/tool.go`、`agent/options.go`、`agent/stepview.go`、`agent/middleware.go` 的注释；这些放到 TC-03。

验证：

```bash
go test ./agent -run 'Test(Segmented|SequentialTool|AdjacentSerial|GlobalSequential|InjectedSequential|InjectedParallel|ParallelBatch)' -count=1
go test ./agent -count=1
go test -race ./agent -run 'Test(Segmented|SequentialTool|AdjacentSerial|GlobalSequential|InjectedSequential)' -count=1
go vet ./agent
```

**完成标准：**在旧实现下，T1 和 T2 因为 p1、p2 不能重叠而失败；接入新实现后 T1–T4、T8 通过。`TestParallelBatchOverlaps`、`TestInjectedParallelToolReplacingStaticSequentialAllowsParallel` 和两个全局串行回归测试（[agent/phase1_failure_test.go:253](../../agent/phase1_failure_test.go#L253)、[agent/tooltimeout_abandon_test.go:171](../../agent/tooltimeout_abandon_test.go#L171)）继续通过。`-race` 通过。

**提交：**`feat(agent): run tool calls in segments`

### TASK-TC-03：边界测试、注释与全量验收

**依赖：**TC-02。**决策：**D8、D9、D10。

**文件：**新增 `agent/toolsegments_boundary_test.go`、`agent/toolsegments_lease_internal_test.go`；修改 `tool/tool.go`、`agent/options.go`、`agent/stepview.go`、`agent/middleware.go`；只填本文 §10 的执行记录。

**边界测试：**

- [ ] T5 `TestSegmentedBatchFollowsInjectedSequentialImplementation`：静态工具 `serial` 不是串行工具；middleware 在 `ModifyRequest` 里用同名 `tool.AsSequential` 实现替换它。调用序列 `[p1, p2, serial]`，断言 p1、p2 在并行段里重叠（峰值 2），serial 启动时活动计数为 0。复用 [agent/dynamictool_test.go:22](../../agent/dynamictool_test.go#L22) 的 `injector`。
- [ ] T6 `TestCancelBetweenSegmentsStopsSerialCall`：调用序列 `[p1, p2, seq]`。p1、p2 启动后等待 `ctx.Done()`；协调 goroutine 收满两个启动信号后取消运行上下文。断言 seq 的 handler 调用次数为 0，运行以 `context.Canceled` 结束，p1、p2 都进入了 handler。
- [ ] T7 `TestLeaseLossInParallelSegmentStopsNextSegment`：放在 `package agent` 的内部测试里，复用 [agent/dispatch_guard_test.go:371](../../agent/dispatch_guard_test.go#L371) 的 `dispatchContextCapture` 和 [agent/dispatch_guard_test.go:439](../../agent/dispatch_guard_test.go#L439) 的 `dispatchGuard`。调用序列 `[p1, p2, seq]`；p1、p2 用 barrier 同时启动，p2 在 handler 内记录一个包装了 `checkpoint.ErrStaleFencingToken` 的租约错误。当前段结算后，下一段的写权限检查必须阻止 seq。断言 seq 的 handler 调用次数为 0，运行错误满足 `errors.Is(err, checkpoint.ErrStaleFencingToken)`。
- [ ] T9 `TestStateOpsVisibleToLaterSerialSegment`：调用序列 `[p1, seq]`。p1 返回 `Result.State = []core.StateOp{{Kind: core.OpSetKV, Key: "from_p1", Value: "yes"}}`；seq 在 handler 里读取 `tool.Context.State.KV["from_p1"]`，把读到的值写进结果。断言 seq 看到 `yes`。
- [ ] T10 `TestRejectionsSerialAfterSegmentedBatch`：调用序列包含一个未知工具、一个串行工具和一个普通工具。断言 `ToolRejecter` 在整批结束后串行回调一次，KV 写入随本步检查点提交。现有的 [agent/reject_test.go:156](../../agent/reject_test.go#L156) 必须继续通过，不修改它的断言。
- [ ] T11 `TestResumedSegmentedBatchOnlyIncludesApprovedCalls`：调用序列 `[p1, skip, p2, seq]`。第一次运行用审批门中断整批；恢复时批准 p1、p2、seq，拒绝 skip。断言 p1、p2 在恢复后的并行段里重叠（峰值 2），seq 独占，skip 的 handler 调用次数为 0，最终历史里有 skip 的拒绝结果。
- [ ] T12 `TestSegmentEventOrder`：调用序列 `[p1, p2, seq, p3]`，记录完整事件流。断言 `ToolStarted` 的顺序是 p1、p2、seq、p3；seq 的 `ToolDone` 出现在 p3 的 `ToolStarted` 之前；工具步和终稿步各发布一次 `TurnDone`，总数是 2，不随 4 个调用增加。测试里用 `t.Log` 打印实际事件序列，§10 粘贴一次真实输出。

**注释和文档：**

- [ ] 更新 [tool/tool.go:88](../../tool/tool.go#L88) 到 [tool/tool.go:92](../../tool/tool.go#L92)：串行工具只保证自己的调用独占，不再把整批降级。
- [ ] 更新 [tool/tool.go:94](../../tool/tool.go#L94) 到 [tool/tool.go:96](../../tool/tool.go#L96)：`AsSequential` 把该调用放进独立串行段，不改变同批其他连续调用的并发能力。
- [ ] 更新 [agent/options.go:95](../../agent/options.go#L95) 到 [agent/options.go:98](../../agent/options.go#L98)：`WithToolExecution` 的说明改为「串行工具单独成段，其他连续调用仍按段并发」。
- [ ] 更新 [agent/options.go:100](../../agent/options.go#L100) 到 [agent/options.go:107](../../agent/options.go#L107) 里「a sequential batch of N slow tools」的说法：超时是按调用计的，改成「一段串行执行里的每个慢调用都能用满自己的上限」。
- [ ] 更新 [agent/stepview.go:75](../../agent/stepview.go#L75) 到 [agent/stepview.go:76](../../agent/stepview.go#L76)：「force a serial batch」改为「require a serial segment」。
- [ ] 更新 [agent/stepview.go:238](../../agent/stepview.go#L238) 到 [agent/stepview.go:239](../../agent/stepview.go#L239)：`Sequential` 说明改为「该名字背后的实现要求自己的调用单独执行」。
- [ ] 更新 [agent/middleware.go:20](../../agent/middleware.go#L20) 到 [agent/middleware.go:21](../../agent/middleware.go#L21)：「across a parallel tool batch」改为「across a parallel segment」。
- [ ] 不改写 [docs/tasks/plan-step-view.md:403](plan-step-view.md#L403) 和 [docs/tasks/plan-step-view.md:627](plan-step-view.md#L627) 的历史表述。在本次提交说明和 §10 里注明：旧计划里的「整批降级」描述已被本计划取代，指向设计稿。
- [ ] 不新增示例，不修改 README；本计划没有面向使用者的新 API。

**反向验证：**

每次只改一处真实逻辑，确认编译通过并看到预期的断言失败，然后用 Edit 还原。不要用 `git checkout --` 还原，以免连带丢掉同一文件里本轮未提交的测试或注释改动。逐条记录到 §10.2。

- [ ] 反向 1：把 `buildToolCallSegments` 临时改成「只要出现任何串行调用，所有调用都生成串行段」。T1 或 T2 必须失败（p1、p2 不能重叠，峰值只能是 1）；T3、T4 仍然通过。
- [ ] 反向 2：临时让 `tools.Sequential(name)` 恒为 false。T3、T4 必须失败（相邻串行调用开始重叠）；T5 必须失败（注入的串行实现被忽略）；T7 必须失败（seq 在租约错误后仍然启动）。
- [ ] 反向 3：临时去掉「串行调用单独成段」，让串行调用留在当前并行组里。T3 或 T5 必须失败（串行调用与相邻调用重叠或顺序不再隔离）。
- [ ] 每次还原后重跑对应目标用例，确认恢复通过。编译失败不算反向验证成功，必须看到具体断言失败信息。

**全量门禁：**

```bash
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./agent -count=1
git diff --check
gofmt -l agent/toolsegments.go agent/toolsegments_internal_test.go agent/toolsegments_test.go agent/toolsegments_boundary_test.go agent/toolsegments_lease_internal_test.go agent/exectools.go agent/seqexec_test.go agent/dynamictool_test.go agent/stepview_test.go tool/tool.go agent/options.go agent/stepview.go agent/middleware.go
```

Windows 上既有文件可能是 CRLF，会被 `gofmt -l` 标出来。只处理本次新增或改动的文件，不要整仓重排；新文件用 LF。

- [ ] 检查 `git status --short`，确认提交只包含本卡的代码、测试和注释，不要使用 `git add .`。
- [ ] 填 §10：三张卡的实际提交、实际命令、结果摘要、偏差和反向验证结果。测试失败过又修好的，也记一行。

**完成标准：**T1–T12 全部通过；取消、租约、注入、恢复、拒绝和状态可见性都有固定的回归测试；注释与实际行为一致；三次反向验证都能让对应断言失败并成功还原；全仓构建、静态检查、测试和 race 门禁通过。

**提交：**`test(agent): cover segmented tool call boundaries`

## 7. 验收矩阵

| 编号 | 场景 | 断言 | 覆盖任务 |
| --- | --- | --- | --- |
| A1 | 空批次、全并行、串行在首/中/尾 | 计划形状正确，下标不重不漏 | TC-01 |
| A2 | 多个串行、相邻串行、全局串行 | 每个串行调用一个段，串行段长度为 1 | TC-01 |
| A3 | 未知名与串行名重复 | 未知名不构成边界；重复串行名各自成段 | TC-01 |
| T1 | `[p1, p2, seq]` | p1、p2 重叠（峰值 2）；seq 独占；结果顺序不变 | TC-02 |
| T2 | `[p1, p2, seq, p3, p4]` | 两个并行段峰值都是 2；seq 独占；p3、p4 在 seq 之后启动 | TC-02 |
| T3 | `[s1, s2]` | 峰值 1；执行顺序 s1、s2 | TC-02 |
| T4 | `[p1, p2]`，全局 `ToolSequential` | 峰值 1；执行顺序 p1、p2 | TC-02 |
| T5 | `[p1, p2, injectedSeq]` | 分段跟随冻结视图的注入实现；串行调用独占 | TC-03 |
| T6 | `[p1, p2, seq]`，并行段运行期间取消 | seq handler 调用次数为 0；运行以取消结束 | TC-03 |
| T7 | `[p1, p2, seq]`，并行段内租约错误 | seq 不启动；错误满足 `errors.Is(..., ErrStaleFencingToken)` | TC-03 |
| T8 | `[slow, fast, seq]` | fast 先完成不改变结果顺序；seq 在两者结算后启动 | TC-02 |
| T9 | `[p1, seq]`，p1 返回状态操作 | seq 能看到 p1 已经应用的状态 | TC-03 |
| T10 | 含未知工具的混合批次 | `ToolReject` 整批后串行回调；KV 随本步检查点提交 | TC-03 |
| T11 | 暂停恢复 `[p1, skip, p2, seq]` | 只对批准调用分段；未批准调用不形成边界 | TC-03 |
| T12 | `[p1, p2, seq, p3]`，记录事件 | Started 顺序正确；seq 的 Done 早于 p3 的 Started；TurnDone 每步一次 | TC-03 |
| E1 | 现有并行重叠测试 | `TestParallelBatchOverlaps` 仍然峰值 2 | TC-02 |
| E2 | 现有全局串行回归 | `AfterTool` 失败仍保留整批结果；慢调用不阻塞后续调用 | TC-02 |
| E3 | 现有拒绝回调测试 | 两个未知调用仍在整批后串行回调，KV 写入检查点 | TC-03 |
| E4 | 全量门禁 | build、vet、全仓测试、agent race 全部通过 | TC-03 |

验收不变量：

1. 任何批次的分段计划都恰好覆盖所有调用下标一次，且按原顺序排列。
2. 串行段只有一个调用；串行调用发布 `ToolDone` 时，它早于下一段的 `ToolStarted`。
3. 被串行调用隔开的两个并行段不会互相重叠；这是串行独占的直接结果，不是回归。
4. 结果、指令和拒绝仍按原调用顺序结算，完成顺序不影响模型看到的历史顺序。
5. 全局 `ToolSequential` 下每个调用一个串行段，最大并发为 1。
6. 所有调用仍经过 `run` 和 `callOne`；状态写入锁、持久化完成记录、`AfterTool` 和 `ToolDone` 的时机不变。
7. 并行段启动过程中写权限检查失败时，只停止后续启动和后续段，已经启动的调用仍会结算。
8. 未知名不构成串行边界，之后由 `callOne` 按现有分类拒绝。
9. 恢复批次只对批准的调用分段，未批准调用不形成边界。
10. 没有公开 API、配置或存储格式变化。

## 8. 风险、兼容性与注意事项

### 8.1 依赖偶然串行化的工具

这是最主要的风险。两个共享非可重入资源的工具，之前可能因为同批里有一个串行工具而碰巧没有并发；改动后它们会在同一个并行段里并发。仓库内没有内置工具声明 `SequentialTool`，风险主要来自外部使用者的工具和中间件。实施前已经用 §5.1 的 `grep` 核对；变更说明里要明确写：`SequentialTool` 只保护自己的调用，不再顺带串行化同批其他调用。需要互斥的工具应自己声明串行能力，或在工具内部加锁。

### 8.2 `AfterTool` 阻塞会推迟段边界

`AfterTool` 在 `run` 内执行，并行段要等它结束才能进入下一段。它本来就没有单调用超时保护；某个中间件很慢时，串行段会被推迟。旧串行路径同样要等 `AfterTool` 返回才执行下一个调用，分段只是把这种等待明确放在段边界上，没有新增阻塞类型。

### 8.3 被放弃的 handler 不受分段保护

`WithToolTimeout` 只能让循环不再等待，不能停止已经运行的 handler。超时后，前一段被放弃的 handler 可能仍在运行，甚至与后续串行段重叠。分段的「串行独占」只覆盖调度器实际启动和等待的调用，不覆盖被放弃的 worker，也不覆盖工具自己创建的 goroutine。这个限制要写进实现说明。

### 8.4 事件模式变化

旧行为里，含串行工具的批次所有调用逐个开始；新行为里，串行段之后的多个调用可以同时开始。按 `ToolStarted`/`ToolDone` 实际到达顺序渲染的消费者不受影响；仍假设「含串行工具的批次一定逐个开始」的消费者需要调整。`ToolStarted` 仍按模型顺序发布，但同一并行段里快的调用可能先发布 `ToolDone`，这一点旧并行分支已经如此。

### 8.5 恢复时的串行标记漂移

暂停批次恢复时，串行标记来自恢复时的当前实现。旧实现是串行、新实现不是：恢复后该调用可能和同段其他调用并发。旧实现不是串行、新实现是：恢复后该调用单独成段。`StepSnapshot` 不记录串行标记，现有步骤视图漂移检查发现不了这种变化。当前整批实现也有同样的暴露面，但分段后影响更细；本计划不扩大快照字段，变更说明要写清楚。

### 8.6 保守分类带来的吞吐损失

已经被持久化完成、最终不会执行 handler 的串行调用，仍然会阻塞一段；被拒绝的串行调用也一样。这是为了避免在执行前做额外的存储查询或参数预检。如果将来成为瓶颈，可以在构建计划前批量读取完成状态，但那会扩大改动面，不在本计划内。

### 8.7 兼容性

- **公开 API：**没有变化。`tool.SequentialTool`、`tool.AsSequential`、`ToolExecMode`、`WithToolExecution` 的签名不变。
- **存储：**没有变化。`Checkpoint`、`StepSnapshot`、工具执行账本都不新增字段。
- **配置：**没有变化。不新增最大并发数、段超时或其他开关。
- **全局模式：**`ToolSequential` 的最大并发仍为 1，执行顺序和超时行为不变。
- **恢复：**只对批准的调用分段；恢复视图的串行标记仍按当前实现填。
- **回滚：**TC-01 单独提交时不接入执行器，行为不变。需要回滚新调度时，用 `git revert` 回退 TC-02 和 TC-03 的提交，不要用破坏性的 `git reset --hard`。

### 8.8 测试注意事项

- 并发用例用 channel、barrier 和原子计数器制造顺序，不用 `time.Sleep` 控制顺序。超时只作为「死锁时失败」的保护，不参与断言。
- 协调 goroutine 要先于消费 `run.Iter()` 启动，否则工具 handler 等待测试释放、测试又在等事件，会互相卡住。
- 每个会阻塞的 handler 都要有 `t.Cleanup` 或取消路径释放，避免测试失败时泄漏 goroutine。
- 负向断言（串行调用不重叠）要在线程确实被 barrier 卡住时读取活动计数，不要靠「等待一小会儿再看看」。
- T7 放在 `package agent` 内部测试里，因为需要 `dispatchGuard` 和 `dispatchContextCapture`；其余行为测试放在 `agent_test`，使用公开 API。
- `rejectSpy` 的计数器故意不加锁；如果 `ToolReject` 真的并发回调，`-race` 会把它暴露出来。不要为了消除告警给它加锁，那会掩盖契约破坏。
- Windows 上既有文件可能是 CRLF。`gofmt -l` 只处理本次新增或修改的 Go 文件，不要整仓 `gofmt -w`。
- 所有测试命令加 `-count=1`，避免缓存掩盖问题。门禁输出要保留完整日志，不要用 `| tail` 截断后再判断。

## 9. 明确不做的事

- 不实现读写锁、按工具信号量或最大并发数。
- 不合并相邻串行调用，不在事件里暴露段边界。
- 不把串行标记写进 `StepSnapshot` 或检查点摘要。
- 不根据调用参数决定是否独占，不扩展 `SequentialTool` 接口。
- 不修改 `run`、`callOne`、超时放弃、状态写入锁、持久化完成记录和拒绝回调时机。
- 不修改 HITL 暂停恢复协议、检查点格式、工具执行账本和占用协议。
- 不改变 `Agent.Run`/`Stream`/`Resume` 的签名。
- 不新增示例、README 条目或公开 API。

## 10. 执行记录

执行时逐项填写。只写实际运行的命令和实际结果，不写猜测输出。

### 10.1 任务记录

| 任务 | 提交 | 运行命令 | 结果摘要 | 偏差/备注 |
| --- | --- | --- | --- | --- |
| TC-01 | `fec24d7` | `go test ./agent -run 'TestBuildToolCallSegments' -count=1`；`go build ./...`；`go vet ./agent`；`go test ./agent -count=1` | 表驱动 11 例、不变量 32 例全过；全仓 build、vet 通过；agent 全量测试 ok 13.334s | 实现前测试仅因符号未定义而编译失败，未出现断言级红；额外做了一次自检：去掉 `group = nil` 后表驱动与不变量用例按预期失败，还原后 `git diff --exit-code` 通过 |
| TC-02 | `4b862e8` | `go test ./agent -run 'Test(Segment\|SequentialTool\|AdjacentSerial\|GlobalSequential\|InjectedSequential\|InjectedParallel\|ParallelBatch)' -count=1`；`go test ./agent -count=1`；`go test -race ./agent -run 'Test(Segment\|SequentialTool\|AdjacentSerial\|GlobalSequential\|InjectedSequential)' -count=1`；`go vet ./agent` | 接执行器前 T1、T2、T8 与两个改名用例均以 “only 1 of 2 calls started within 2s” 失败，T3、T4、`TestParallelBatchOverlaps` 通过；接入后 10 个目标用例全过；agent 全量 ok 13.372s；race ok 1.510s；vet 通过 | 卡面验证正则写的是 `Test(Segmented\|…)`，匹配不到 T8 的 `TestSegmentResultsKeepModelOrder`（名字以 Segment 开头），实际改用 `Test(Segment\|…)` 并显式纳入 T8；另复跑 `TestAfterToolErrorSurvivesBatch`（含 resumed 子用例）、`TestSequentialBatchSlowToolDoesNotBlockRest`、`TestParallelBatchOverlaps`、`TestInjectedParallelToolReplacingStaticSequentialAllowsParallel` 全部通过 |
| TC-03 | `2f18017` | `go test ./agent -run 'TestBuildToolCallSegments\|TestSegmented\|TestSegment\|TestAdjacentSerial\|TestGlobalSequential\|TestSequentialToolSerializes\|TestInjectedSequential\|TestParallelBatchOverlaps\|TestCancelBetweenSegments\|TestStateOpsVisible\|TestRejectionsSerial\|TestResumedSegmented\|TestLeaseLossInParallel' -count=1`；`go build ./...`；`go vet ./...`；`go test ./... -count=1`；`go test -race ./agent -count=1`；`git diff --check`；`gofmt -l`（卡面 13 个文件） | T1–T12 共 12 个目标用例全过（含 T5–T7、T9–T12）；build、vet 无输出；`go test ./...` 34 个有测试的包全部 ok；race ok 18.395s；gofmt 无输出；diff --check 仅 CRLF 警告、退出码 0 | T5 最初只断言峰值和活动计数，反向 2 下曾因调度时序碰巧通过；已补事件顺序断言（`ToolStarted(serial)` 必须晚于 `ToolDone(p1)`、`ToolDone(p2)`）并连跑 3 次确认。`agent/toolsegments_test.go` 不在卡面文件清单里，但其 helper 重构（`latestToolResults`、`toolResultNames`）是 T11 必需的测试代码，随本提交纳入。旧计划里的「整批降级」表述已在提交说明中注明由设计稿取代，历史计划未改 |

### 10.2 反向验证记录

| 编号 | 临时改动 | 预期失败用例 | 实际失败信息 | 还原后结果 |
| --- | --- | --- | --- | --- |
| 反向 1 | 任何串行调用出现时，所有调用都生成串行段 | T1 或 T2（峰值只能是 1） | T1 `TestSegmentedBatchRunsParallelCallsAroundSerial`: `parallel calls before seq: only 1 of 2 calls started within 2s`；T2 `TestSegmentedBatchRunsGroupsOnBothSidesOfSerial` 同一信息；T3、T4 通过 | `git diff --exit-code agent/toolsegments.go` 与 HEAD 一致；目标用例重跑全过 |
| 反向 2 | `tools.Sequential` 恒为 false（`if false && tools.Sequential(c.Name)`） | T3、T4、T5、T7 | T3: `peak concurrency = 2, want 1 (adjacent serial calls each own a segment)`；T5: `serial call started before the parallel segment settled: [ToolStarted(p1) ToolStarted(p2) ToolStarted(serial) ToolDone(serial) ToolDone(p2) ToolDone(p1)]`；T7: `seq handler calls = 1, want 0 after lease loss`；T4 通过：该变异只关闭工具级信号，T4 走 `mode == ToolSequential` 的全局分支，不读取它 | `git diff --exit-code agent/toolsegments.go` 与 HEAD 一致；目标用例重跑全过 |
| 反向 3 | 串行调用不单独成段，留在当前并行组 | T3 或 T5（串行调用与相邻调用重叠） | T3: `peak concurrency = 2, want 1 (adjacent serial calls each own a segment)`；T5: `serial call started before the parallel segment settled: [ToolStarted(p1) ToolStarted(p2) ToolStarted(serial) ToolDone(p2) ToolDone(p1) ToolDone(serial)]` | `git diff --exit-code agent/toolsegments.go` 与 HEAD 一致；目标用例重跑全过 |

### 10.3 事件序列核对

T12 的 `t.Log` 实际输出（测试通过后粘贴，不要照格式编造）：

```text
[core.RunStarted TurnStarted(0) core.MessageDone ToolStarted(p1) ToolStarted(p2) ToolDone(p2) ToolDone(p1) ToolStarted(seq) ToolDone(seq) ToolStarted(p3) ToolDone(p3) TurnDone(0) TurnStarted(1) core.MessageDone TurnDone(1) core.RunDone]
```
