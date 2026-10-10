# 实施方案：参数校验拒绝的计数与升级（ADR-0030）

- 范围：`agent/exectools.go`、`agent/loop.go`、`agent/hitl.go`、`agent/middleware.go`、`core/event.go`、`core/eventjson.go`、`middleware/`、`examples/`
- 日期：2026-10-02
- 进度：**AG-01 `51df6fc`、AG-02 `adc064c`、AG-03 `7c4d40f`+`41e37ca`、AG-04 `e622dab`、AG-05（追加的两个视角示例）`19d3895`+`551ad9f` 全部完成**（均未推送）。各 TASK 的验收记在本卡对应小节的完成记录里。
- 设计文档：`docs/adr/ADR-0030-arg-reject-guard.md`
- 前置依赖：ADR-0029 的 TASK-TO-01/02（`toolOutcome` 的字段与 `RejectTimedOut` 分类）。`RejectTimedOut` 常量在本卡定义，但只有 TO-02 落地后才会真正产生；若两卡并行开发，AG-03 的默认统计范围不含它，不阻塞。

## 前置事实（逐条 grep 核实）

- `callOne` 的四个拒绝点与文案：
  - 工具名查不到 → `errResult(c, "unknown tool: "+c.Name)`（`agent/exectools.go:92-98`）
  - `PrepareArguments` 返回错误 → `"invalid arguments: "+err.Error()`（`:100-106`）
  - `tool.Validate` 返回错误 → `"invalid arguments: "+err.Error()`（`:107-109`，校验器 `tool/validate.go:21-35`）
  - handler 返回 Go error → `errResult(c, err.Error())`（`:112-114`）
  - handler 未执行时返回的就是 `core.ToolResult{IsError: true}`（`errResult`，`:119-124`）；handler 正常返回但 `Result.IsError=true` 来自 `tool.ErrorResult`（`tool/tool.go:102-104`），`eval` 的 ToolGuard 也会在 AfterTool 里翻转该位（`eval/loop.go:79-96`）—— 这两类都不属于拒绝。
- 循环里 `execTools` 的调用点只有两处：主路径 `agent/loop.go:243`，HITL 恢复路径 `agent/hitl.go:151`（`runResumed` 自行构造 `LoopContext`，`:143`）。
- 步骤收尾顺序：`execTools` → 追加 tool 消息（`agent/loop.go:244`）→ `rc.State.Messages = history` → `l.checkpoint` → `TurnDone`（`:247-249`）。把钩子放在 `:243` 与 `:244` 之间，中间件写的 `State.KV` 就能随本步 checkpoint 落盘。
- `State.KV` 是 `map[string]any`，`Apply(OpSetKV)` 直接写（`core/state.go:10-15,75-93`）；JSONL 往返后数值只能是 `float64`，LoopGuard 已按此写了容错读取（`middleware/loopguard.go:374-387`）。
- LoopGuard 的升级是同一种写法：AfterModel 写 `State.KV` 标记（`middleware/loopguard.go:101-102,157-158`）→ BeforeTool 读取并返回 `Interrupt`/`Stop`（`:171-181`）；警告走 `lc.Steer`（`agent/runtime.go:105`，drain 在 `agent/loop.go:150-152`）。
- LoopGuard 的两条判据确实覆盖不到本场景：`repeat_call` 要求调用签名相同（`middleware/loopguard.go:212-218,340-346`），`error_streak` 要求"工具名 + 错误首行"连续相同（`:221-230,292-319`）。模型每次改一点参数，两条都不满足。
- 循环没有为工具调用做过任何分类：`callOne` 只回结果，`execTools` 只收结果与指令（`agent/exectools.go:21-82`）。
- 指令合并的安全性：`core.Resolve` 取最高优先级，`Interrupt > Stop > Escalate > Transfer > Continue`，同级按参数顺序取前者（`core/directive.go:13-31,51-62`）；`Stack.BeforeTool` 逐个调用中间件并合并结果（`agent/middleware.go:157-167`）。
- 警告文案想报出的 `required` 字段可以从本步请求的工具广告里读：`req.Tools = l.advertised(rc)`（`agent/loop.go:170-172`），元素类型 `llm.ToolSchema{Name, Description, Parameters json.RawMessage}`（`llm/model.go:70-74`），`Parameters` 就是那份 JSON Schema，`required` 是其中的数组。
- 事件是密封联合：加变体要同时改 `core/event.go`（类型 + `isEvent` marker，现有 `StuckDetected` 在 `:87-94`、marker 列表在 `:117-134`）与 `core/eventjson.go`（`eventWire` 的扁平字段 `:17-33`、`MarshalEvent` `:36-90`、`UnmarshalEvent` `:96-145`），漏掉编解码会在 `default` 分支报 "cannot marshal event of type"。
- `eventWire` 现有字段里没有 `Tool` / `Count` / `Class`（`core/eventjson.go:17-33`）。

## 总设计立场（四个 TASK 共用）

1. **循环分类，中间件决策**。`RejectClass` 的判断只发生在 `callOne`（唯一知道失败原因的位置）；阈值、警告、升级一律在 `middleware.ArgGuard` 里。循环不认识"3 次""6 次"这类数字。
2. **钩子在批次结束后串行调用**，不在工作协程内。这样中间件可以直接用 `lc.State.Apply`，不需要自己加锁，也不受 `AfterTool` 的协程安全约束（`agent/middleware.go:20-21`）。代价是升级动作作用在下一步，与本设计一致（LoopGuard 同样在下一步的 BeforeTool 执行）。
3. **计数只数 handler 未被执行的那些结果**。工具自己报告的业务失败（`IsError` 但 handler 跑完了）不计，避免把"接口返回 500"当成"模型不会用工具"。
4. **计数记录从 `State.KV` 读写，不放内存**。中间件实例跨 run 共享（`agent/loop.go:63`），内存态在 HITL 暂停与进程重启后必丢；这与 LoopGuard、RunBudget 同一立场。
5. 默认统计范围 = 三类参数面拒绝（`RejectUnknownTool`、`RejectPrepareFailed`、`RejectSchemaInvalid`）。`RejectHandlerError`、`RejectTimedOut` 需显式配置才计。
6. 每个 TASK 结束时仓库可编译、全测可跑。

---

## TASK-AG-01：循环侧的分类与钩子调用 — 已完成 `51df6fc`

### 改动

**`agent/middleware.go`**

- 在 `Middleware` 接口之后、`RunFinisher` 之前新增公开类型（`RejectClass` 与合并方法同处一包，`core` 不引用它）：

```go
// RejectClass names why a tool call was refused before (or without) completing,
// decided by the loop at the one site that knows: callOne. It is a fact, not a
// verdict — the loop applies no policy for it; middleware observes and decides.
type RejectClass int

const (
    RejectUnknownTool   RejectClass = iota // name not in either tool table
    RejectPrepareFailed                     // ArgumentPreparer rejected the args
    RejectSchemaInvalid                     // tool.Validate rejected the args
    RejectHandlerError                      // the handler returned a Go error
    RejectTimedOut                          // abandoned at its bound (ADR-0029)
)

func (k RejectClass) String() string // "unknown_tool" | ... | "timed_out"

// ToolRejection is one refused call, reported to ToolRejecter after the batch.
type ToolRejection struct {
    Call   core.ToolCall
    Class  RejectClass
    Detail string // the text sent to the model as the error result
}

// ToolRejecter is an optional middleware capability for observing refused tool
// calls: unknown names, rejected arguments, handler errors, abandoned timeouts.
// The loop dispatches the batch's rejections after execTools returns and before
// the step is checkpointed, so the hook runs serially on the loop's goroutine —
// unlike AfterTool, it needs no goroutine-safety, and writes it makes to
// State.KV are checkpointed with this step. Middleware that does not implement
// it is skipped in the fold (Stack.ToolReject).
type ToolRejecter interface {
    OnToolReject(lc *LoopContext, r ToolRejection)
}
```

- `Stack.ToolReject(lc *LoopContext, rs []ToolRejection)`：按注册顺序遍历 `s.mws`，对实现了 `ToolRejecter` 的逐个调用；空列表直接返回。写法与 `Stack.ModifyRequest`（`:110-117`）一致（观察型，无 Directive）。

**`agent/exectools.go`**

- `callOne` 在四个拒绝点各返回一条拒绝信息。做法：给 `toolOutcome`（TO-02 引入，若 TO-02 尚未落地则本 TASK 先引入该结构体）加字段 `rejection *ToolRejection`，四个 `errResult` 出口分别带上对应 class 与同一份 `Detail`；`RejectHandlerError` 用在 `t.Call` 返错处。
- `execTools` 签名改为 `(results []core.Part, dirs []core.Directive, rejects []ToolRejection)`：`run(i, c)` 里 `if out.rejection != nil { rejectMu.Lock(); rejects = append(rejects, *out.rejection); rejectMu.Unlock() }`（并发批次下需要一把独立小锁，与 `stateMu` 同处，`agent/exectools.go:24`）。
- 若本卡先于 TO-02 实施：`run` 的现有写法不变，只是尾部多收集一项；`toolOutcome` 由本卡引入，TO-02 复用。两卡的 `toolOutcome` 字段合并方式在此写明：`{tr, control, ops, rejection}`。
- **不通知**的两种情况保持沉默，加注释说明：handler 正常返回但 `Result.IsError=true`；`AfterTool` 里被改写成的 `IsError`。

**`agent/loop.go`**

- 主路径：`results, dirs, rejects := l.execTools(rc, lc, calls)` 之后、追加 history 之前插入 `l.mw.ToolReject(lc, rejects)`（现 `agent/loop.go:243-244`）。
- 恢复分支：`runResumed` 返回值加 `rejects []ToolRejection`（`agent/hitl.go:142-165`），`loop.go:116` 处调用 `l.mw.ToolReject(rc 对应的 lc, rejects)`。注意 `runResumed` 内部已有自己的 `lc`（`agent/hitl.go:143`），拒绝在该 lc 上调用即可 —— 位置选在 `runResumed` 返回前、`loop.go` 拿到值之后，两处选一，实施时取"在 `runResumed` 内调用钩子"更省一层返回值传递，但会让 `runResumed` 承担循环阶段职责。**决定：在 loop 里调用**，`runResumed` 只多返回一项，阶段顺序仍集中在 loop（与 `agent/loop.go:21-25` 的阶段注释一致）。

**`agent/hitl.go`**

- `deniedResult`（人工拒绝的批次项）**不算** `RejectClass`：那是人的决定，不是调用不合格。加注释，避免以后误加。

### 验收

- `go build ./... && go vet ./...`（`gofmt -l` 对既有 CRLF 文件的标记属既有现象，见 `plan-tool-execution-timeout.md` 的 TO-01 说明，不要为此重排既有文件）。
- 新增 `agent/reject_test.go`：
  - `TestRejectClassesFromCallOne`：四类各一例（不存在的名字 / `ArgumentPreparer` 返错 / 缺 required 字段 / handler 返 `error`），断言 `Class`、`Call.Name`、`Detail` 与历史里那条结果文本一致。
  - `TestHandlerErrorResultIsNotARejection`：handler 返回 `tool.ErrorResult(...)`（`tool/tool.go:102-104`），断言钩子**未**被调用。
  - `TestRejectionsDispatchedAfterBatchSerially`：并发批次里两个工具同时被拒，断言两次 `OnToolReject` 在 loop 协程上顺序到达（用记录 goroutine 序号/互斥计数的方式断言无交叠），且 `State.KV` 两项都写上（钩子里直接 `lc.State.Apply`，不加锁）。
  - `TestRejectionsCheckpointedSameStep`：钩子写 KV 后，本步的 checkpoint 快照里能看到该键（复用 `agent/durable_test.go` 的文件 checkpointer 构造）。
  - `TestResumedBatchDispatchesRejections`：HITL 批准后放行一个参数不合法的调用，断言恢复路径也调用了钩子。
- `go test -race ./agent/` 绿；`go test ./...` 全仓绿。

---

### TASK-AG-01 完成记录（2026-10-03）

与卡里写法不同的五处，都是实施时才确定的：

1. **`RejectTimedOut` 不在 `callOne` 标注，而在 `awaitAbandoned` 的 deadline 分支**（TO-02 已落地，见 ADR-0029 第 3 节）。卡只列了 `callOne` 的四个出口。为此 `timeoutResult` 改成 `abandonedAtBound`，返回整个 `toolOutcome`：合成的那句超时文案与 `ToolRejection.Detail` 因此同源，不会各写一份。
   - run 被取消那一支（`cancelledResult`）**不产生拒绝记录**：类别表里没有"运行被取消"，而且此时已经没有下一步会让升级生效。写成注释放在代码里。
2. **另外两处沉默也补了注释**：卡只要求 `deniedResult`（人工拒绝）说明不计；`truncatedResults`（max_tokens 截断整批、handler 一个都没跑）同样容易被后来者"顺手计入"，一并写明理由。
3. **`Stack.ToolReject` 取复数签名** `(lc, rejects []ToolRejection)`（卡的 AG-01），而非 ADR §1 里那个 `Stack.ToolReject(lc, r)`。逐条 rejection 外层、逐中间件内层，这样每条拒绝都被所有观察者看到。
4. **`runResumed` 不只是多返回一项**：它的 `lc` 改由 loop 构造并传入（`runResumed(rb, lc)`，`agent/loop.go:119-121`）。恢复批次和它的拒绝上报因此共用同一个 `LoopContext`，不必在 `hitl.go` 再复制一份 `LoopContext` 字面量，也保住了卡决定的"阶段顺序集中在 loop"。
5. **用例名与断言手段**：卡的 `TestHandlerErrorResultIsNotARejection` 实现为 `TestHandlerIsErrorIsNotARejection`（handler 返 Go error 才是 `handler_error`；业务失败用 `tool.ErrorResult`，本卡不算拒绝）。`TestRejectionsCheckpointedSameStep` 用 `checkpoint.NewMemory()` 而不是 `durable_test.go` 的文件 store（`History()` 同接口，够用且免临时目录）。串行分发的断言手段：spy 的计数与 KV 读写字段**故意不加锁**——真出现并发时，显式断言和 `go test -race` 会同时报告（已实测，见下）。

卡里"前置事实"的 `agent/exectools.go` 行号是 ADR-0029 落地前测得的，现已下移：`callOne` 在 `agent/exectools.go:233`，四个拒绝出口 `:241`（unknown）/`:247`（prepare）/`:252`（schema）/`:257`（handler），`errResult` 在 `:280`；`execTools` 的串行与并发收束分别在 `:113`、`:126`。其余引用（`tool/validate.go`、`middleware/loopguard.go`、`core/state.go`、`llm/model.go`）复核后仍有效。

变异验证（每次改坏一处，跑相关用例，再还原）：

| 改动 | 结果 |
| --- | --- |
| 把 `ToolReject` 调用挪进 `run`（工作协程内、无收集锁） | `TestRejectionsDispatchedAfterBatchSerially` 失败：`OnToolReject ran concurrently`，且 `-race` 同时报竞态 |
| 删掉恢复分支里的 `l.mw.ToolReject(rlc, rejects)` | `TestResumedBatchDispatchesRejections` 失败：`rejections after resume = 0, want 1` |
| 把主路径的 `ToolReject` 挪到 `l.checkpoint` 之后 | `TestRejectionsCheckpointedSameStep` 失败：`checkpoint 0 has the rejection in history but not in KV: map[]` |
| 让 handler 自报的 `IsError` 也产出拒绝记录 | `TestHandlerIsErrorIsNotARejection` 失败：`rejections = [... handler_error], want none` |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` = 0；`go test -race ./agent/ ./middleware/` = 0。
- `gofmt -l` 对 `agent/exectools.go`、`agent/middleware.go`、`agent/loop.go`、`agent/hitl.go`、`agent/reject_test.go` 无输出（这五个文件本就是 LF，未被既有 CRLF 现象牵连）。
- 新用例 5 组（表驱动那条含 4 个子例）全绿；提交 `51df6fc`，未推送。

---

## TASK-AG-02：`core.ArgRejected` 事件 — 已完成 `adc064c`

### 改动

**`core/event.go`**（对齐 `StuckDetected` 的写法，`:87-94`）

```go
// ArgRejected is emitted by the arg-guard middleware each time a tool call is
// refused before completing. Tool names the refused tool, Class is the loop's
// classification ("unknown_tool" | "prepare_failed" | "schema_invalid" |
// "handler_error" | "timed_out"), Count is that tool's running total for this
// run including this rejection, and Step is the loop step. Unlike
// StuckDetected — which fires when a pattern is recognised — this fires per
// refused call, so a stuck model produces a readable sequence of them.
type ArgRejected struct {
    Tool  string
    Class string
    Count int
    Step  int
}
```

并在 marker 列表（`core/event.go:117-134`）加 `func (ArgRejected) isEvent() {}`。

**`core/eventjson.go`**

- `eventWire`（`:17-33`）加三个字段：`Tool string \`json:"tool,omitempty"\``、`Class string \`json:"class,omitempty"\``、`Count int \`json:"count,omitempty"\``。
- `MarshalEvent` 加 `case ArgRejected: w.Type, w.Tool, w.Class, w.Count, w.Step = "arg_rejected", e.Tool, e.Class, e.Count, e.Step`（放在 `StuckDetected` 分支旁，`:83-84`）。
- `UnmarshalEvent` 加 `case "arg_rejected": return ArgRejected{...}, nil`（放在 `:134-135` 的 `stuck_detected` 旁）。

### 验收

- `core/eventjson_test.go` 加 `TestArgRejectedEventJSONRoundTrip`：`MarshalEvent` → `UnmarshalEvent` 得回同一值（沿用该文件既有的往返表驱动写法）。
- `agent/reject_test.go` 加 `TestArgRejectedEventPublished`：用 `Stream(...).Iter()` 或 `Bus` 订阅者，断言一条被拒调用产出一条 `ArgRejected`，字段与 KV 计数一致。
- 确认 `queue`（Redis 进度总线）与 `bus` 的既有事件测试不因新变体失败：`go test ./core/ ./bus/ ./queue/`。
- 全仓 `go vet ./... && go test ./...` 绿。

---

### TASK-AG-02 完成记录（2026-10-03）

与卡里写法不同的三处：

1. **往返用例落在 `core/stuckevent_test.go`，不是 `core/eventjson_test.go` 的表驱动那张表**。仓库里 `StuckDetected`、`BudgetExceeded`、`HistoryCompacted` 三个防护事件的往返用例都在前者，各一个函数；照该体例加 `TestArgRejectedEventRoundTrip`。（顺带：`eventjson_test.go` 是既有 CRLF 文件，`stuckevent_test.go` 是 LF，往 LF 文件里加代码不需要处理行尾。）
2. **agent 侧那条发布用例需要一个发布者，而 ArgGuard 是 AG-03 的事**：实现为测试内的 `eventPublisher`（`OnToolReject` → 把计数写进 `State.KV["argcount.<tool>"]`，再 `lc.Bus.Publish(lc.Topic, core.ArgRejected{...})`，出口取法照 `middleware/loopguard.go:185`）。AG-03 落地后这条用例继续有效——它验证的是"新变体能到 Stream 订阅者、字段不丢"，与谁发布无关。
3. **模型脚本要按"模型调用次数"分支，不能按"有没有工具结果"分支**：同一条调用被连续拒绝时，历史从第一步起就带着那条工具结果，`mock.LastToolResult` 恒为真，run 会在第二个模型调用就收尾，事件只发 1 条。第一版正是这样写的（断言 3 条实收 1 条），改成计数脚本后成立。

变异验证三次（每次改坏一处、跑用例、还原）：删 `MarshalEvent` 的 `case ArgRejected` → `core: cannot marshal event of type core.ArgRejected`；删 `UnmarshalEvent` 的 `case "arg_rejected"` → `core: unknown event type "arg_rejected"`；把 `e.Class` 换成空串 → 往返后 `Class` 丢失、断言失败。另确认仓内没有第二处需要登记事件类型：`grep -rn "stuck_detected|history_compacted" --include=*.go` 除 `core/eventjson.go` 外无命中，`bus`/`queue` 都是泛型转发。

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` = 0；`go test ./core/ ./bus/ ./queue/ ./agent/ ./middleware/` = 0。
- `go test -race -count=1 ./core/ ./agent/ ./bus/ ./middleware/` = 0。
- `gofmt -l` 对 `core/event.go`、`core/eventjson.go`、`core/stuckevent_test.go`、`agent/reject_test.go` 无输出（四个文件均 LF）。
- 新用例：`core` 往返 1 条；`agent` 发布 1 条（断言三条 `ArgRejected` 的 Tool/Class/Count/Step 与 KV 计数一致）。提交 `adc064c`，未推送。
- **同批发现一条既有缺陷**（与本卡无关，登记在风险表）：`go test -race ./queue/` 的 `TestBridgeForwardsEvents` 稳定失败 3/3，竞态在 `queue.MemBus`（`Publish` 在锁外向快照里的通道发送，`Subscribe` 的 cancel 在锁内 `close` 同一通道）。本卡未改 `queue/` 任何文件。

---

## TASK-AG-03：`middleware.ArgGuard` — 已完成 `7c4d40f` + `41e37ca`

### 改动

**新增 `middleware/argguard.go`**，工厂不加 `New` 前缀：

```go
type ArgGuardPolicy int

const (
    // ArgGuardInterrupt pauses for a human decision via the existing HITL
    // checkpoint/Resume path. Default.
    ArgGuardInterrupt ArgGuardPolicy = iota
    // ArgGuardStop ends the run, keeping the last assistant message.
    ArgGuardStop
)

type ArgGuardOptions struct {
    WarnThreshold     int // per-tool rejections that trigger one warning (default 3, min 1)
    EscalateThreshold int // per-tool rejections that escalate (default 6)
    OnEscalate        ArgGuardPolicy
    Count             []agent.RejectClass // default: unknown_tool + prepare_failed + schema_invalid
    MaxInterventions  int // guard actions per run before forcing stop (default 3)
    OnDetect          func(tool string, count int, class agent.RejectClass)
}

func ArgGuard(o ArgGuardOptions) agent.Middleware
```

实现要点：

- 类型 `argGuard struct { agent.BaseMiddleware; opts ArgGuardOptions; counted map[agent.RejectClass]bool }`。`counted` 只读，构造期定形，无需锁。
- `OnToolReject(lc, r)`：不在 `Count` 内直接返回；否则读计数记录 → 该工具 `count+1` → 发 `core.ArgRejected{Tool, Class: r.Class.String(), Count, Step: lc.Step}` → 调 `OnDetect` → 到 `WarnThreshold` 且未 `warned` 则 `lc.Steer` 并置 `warned` → 到 `EscalateThreshold` 则写 `actions` 标记并 `interventions+1`。整段在一次 `saveLedger` 里落 `State.KV`。
  - 钩子在 loop 协程上串行调用（AG-01），因此实现里不加锁、直接 `lc.State.Apply`；这条前提写进方法注释，并说明"若将来改成并发调用，本方法必须加锁"。
- 计数记录的键与结构（命名沿用 LoopGuard 的 `_loopguard.*` 风格）：

```go
const (
    kvArgGuardTools         = "_argguard.tools"         // map[工具名] -> map[string]any{count, warned}
    kvArgGuardActions       = "_argguard.actions"       // map[工具名] -> "interrupt"|"stop"
    kvArgGuardInterventions = "_argguard.interventions" // float64
    WarnMarkerArg           = "[arg-guard]"
)
```

- 读取一律容错 `float64`/`int`/`int64` 与 `map[string]any`/`map[string]string` 两种形态 —— 直接照抄 `middleware/loopguard.go:374-405` 的两个 helper 的写法（`interventions()` 与 `markedAction()`），并复用其注释里"live run 见 map[string]string，resume 后见 map[string]any"这条理由。
- 警告文案：`[arg-guard] 工具 <name> 已被拒绝 <count> 次，最近一次原因：<Detail>。该工具的必填参数是 <required 列表>，请一次给全后再调用；连续被拒将中断本次运行。`
  - `required` 取自 `lc.Request.Tools` 里同名的 `llm.ToolSchema.Parameters`（`llm/model.go:70-74`；`agent/loop.go:170-172` 已把本步请求挂到 `lc.Request`）：解析 `map[string]any` 的 `required` 数组。取不到（Request 为 nil、schema 无 required、JSON 解析失败）就省略该子句，不报错 —— 与 Compaction 摘要失败时不改动历史的处理一致（`middleware/compaction.go:135-137`）。
  - 文案语言与该中间件面向的模型一致，用中文还是英文由实施时仓库既有告警文案决定：`WarnMarker`（`middleware/loopguard.go:99`）与其后的正文是英文，本卡按英文正文实现，`[arg-guard]` 前缀体例不变。（此条为推断项，实施时先读 loopguard 的 `warningMessage` 全文再定。）
- `BeforeTool(lc, c)`：查 `actions[c.Name]`，命中则按策略返回 `Interrupt` 或 `Stop`，`Reason` 以 `[arg-guard]` 起头 —— 与 `middleware/loopguard.go:171-181` 写法一致。
- 干预预算：`interventions >= MaxInterventions` 时 `BeforeTool` 一律 `Stop`（同 `middleware/loopguard.go:143-149` 的处理）。
- **不去重 LoopGuard**：两者可能同批命中，指令合并由 `core.Resolve` 保证安全（`core/directive.go:51-62`）；KV 命名空间分离。文件头注释里写一句各自管什么，避免后来者"顺手合并"。

**`middleware/argguard_internal_test.go`**：计数读写（float64 容错）、`required` 解析（含 nil Request / 无 required / 坏 JSON 三种降级）、`counted` 默认集与自定义集。

### 验收

- `go test ./middleware/` 绿。
- `middleware/argguard_test.go`（包外，走真实 loop，体例参照 `middleware/loopguard_test.go`）：
  - `TestArgGuardCountsAcrossDifferentBadArgs`：三次调用同一工具、每次参数错误都不同（缺字段 → 缺另一个字段 → 类型不对）。断言 `count=3`、历史里出现且**只**出现一条 `[arg-guard]` 警告。这条用例是背景缺口的正面验证：同一场景下 `LoopGuard` 不装也能过。
  - `TestArgGuardEscalateInterruptsThenResumes`：装到 `EscalateThreshold` → 断言 `PendingHITL` 落盘、`Run.Decide`+`Resume` 能继续。
  - `TestArgGuardStopPolicy` / `TestArgGuardInterventionBudget`：`OnEscalate: ArgGuardStop` 与超预算两条路径。
  - `TestArgGuardIgnoresHandlerIsError`：handler 返回业务错误时不计数（与 AG-01 的循环侧断言互为冗余，保留）。
  - `TestArgGuardWithLoopGuard`：两者同装、同批都命中，断言最终指令是 `Interrupt`，且 `_argguard.*` 与 `_loopguard.*` 两组键互不污染。
  - `TestArgGuardDurableAcrossResume`：`File` checkpointer + 跨"进程"恢复（沿用 `agent/filedurable_test.go` 的做法），断言恢复后计数继续累加、`float64` 读回正确。
- 全仓 `go vet ./... && go test ./...`、`go test -race ./agent/ ./middleware/` 绿。

---

### TASK-AG-03 完成记录（2026-10-03）

与卡里写法不同的十处，前两条是行为差异，其余是实施细节：

1. **干预预算在"写标记时"决定动作，不在"命中时"再查**（已经用户拍板：只拦被标记的工具）。实现：`ledger.interventions >= MaxInterventions` 时把标记本身写成 `stop`，`BeforeTool` 只读标记。
   - 卡正文写的是"预算耗尽后 `BeforeTool` 一律 Stop"，那会把与该工具无关的正常调用也打断；被引用的先例 `middleware/loopguard.go:143-149` 实际也是"命中才升级为 stop"，只作用于自己检出的那条签名。
   - 实测证据：先按"命中时查预算"实现，第一次升级就直接 Stop（用例报 `first wave interrupts = 0, want 1`）——因为写标记的那一步已经把预算花掉了，命中时再查必然超标。改成写时决定后，第一次是 Interrupt，第二次才 Stop。
2. **标记只作用一次**：`BeforeTool` 命中后从 KV 删掉（`clearArgMark`）。卡的正文没有这一步，但不删就会出问题：模型下一步把参数写对了，仍然会被上一步的旧标记拦住。内部用例 `TestArgMarkIsConsumedOnce` 覆盖，另有一条断言"预算耗尽时未标记的工具照常运行"。
3. **告警与升级在同一段 switch 里互斥，升级优先**：所以把 `EscalateThreshold` 设成与 `WarnThreshold` 相同的那次不会发告警（直接升级）。用例 `TestArgGuardWithLoopGuard` 因此取 Warn 1 / Escalate 2。
4. **KV 里的账本必须写成纯 map，不能是本包的 struct**：`argToolRecord` 的字段是小写，`json.Marshal` 得到 `{}`，恢复后计数静默归零。`TestArgLedgerSurvivesJSON` 专防这一条；`TestArgLedgerWritesPlainMaps` 断言落盘形状。
5. **`Count` 是替换默认集，不是追加**（文档措辞容易读成后者，注释里写明）。
6. **模型脚本按模型调用次数分支**（AG-02 那条经验在这里同样成立）：`loop: true` 的脚本永不出文本，靠 `WithMaxTurns(12)` 兜住；一次性脚本按 turn 计数取参数。
7. **同装 LoopGuard 的用例要把 ArgGuard 的阈值压到同一步**：LoopGuard 在 `RepeatThreshold: 2` + 同一签名下第 3 步就升级，ArgGuard 若仍是默认 3/6，该步只有 LoopGuard 动了，用例就证明不了"两者同批命中"。
8. **命名与 helper**：常量按卡取 `WarnMarkerArg`（ADR §4 原文写 `WarnMarkerArgs`，已改 ADR）；包外测试的 helper 命名避开 `loopguard_test.go` 已在同包占用的 `guardAgent`/`collect`/`anyWarning`，改用 `argAgent`/`drive`/`wave`/`countMarkers`。
9. **跨进程那条用例改名叫 `TestArgGuardDurableAcrossRestart`**：卡把它写在"`File` checkpointer + 跨进程恢复（沿用 `agent/filedurable_test.go` 的做法）"下，但这条场景没有待批准的 HITL 批次，走不了 `Agent.Resume`；正确的机制是第二个 agent 用同一个 thread 直接 `Stream(ctx, "again", agent.OnThread("t1"))` —— `Agent.restore`（`agent/agent.go:140,159`）会从最新快照取回 `State`（含 KV，数值已是 `float64`）继续跑。断言因此是"计数从 2 续到 3、且不会重复告警第二次"。

变异验证四次（每次改坏一处、跑用例、还原）：

| 改动 | 结果 |
| --- | --- |
| 去掉 `!g.counted[r.Class]` 这层筛选 | `TestArgGuardCountsOnlyConfiguredClasses` 失败：默认集数到 5 条（handler 错误与超时被并进来） |
| 告警分支去掉 `&& !rec.warned` | `TestArgGuardDurableAcrossRestart` 失败：跨进程后告警从 1 条变 2 条，说明 `warned` 位确实从 checkpoint 读回来了 |
| `BeforeTool` 里不调 `clearArgMark` | `TestArgMarkIsConsumedOnce` 失败：标记未被消费，第二次调用仍然被拦 |
| 账本改写成本包 struct（`tools[name] = rec`） | `TestArgLedgerSurvivesJSON` 失败：JSON 往返后 `count=0`，正是那条静默失效 |

另外两条是在开发过程中真实踩到后修正的，留此备考：预算在命中时查（导致第一次升级就 Stop）、同装用例里 ArgGuard 阈值设得比 LoopGuard 高（导致只验证了 LoopGuard 一侧）。

10. **告警正文取英文**（卡里列为"实施时先读 `warningMessage` 再定"，已读 `middleware/loopguard.go:409-413`：英文正文 + `[loop-guard]` 前缀）。ArgGuard 的正文同样英文，前缀 `[arg-guard]`，并多带一句"下一次被拒会有什么后果"（`escalationWords`，按 `OnEscalate` 与剩余预算取"暂停待审"或"直接终止"）——这句是卡里没有的细化：预算耗尽后进一步被拒是直接终止，告警不该继续承诺"只是暂停"。由 `TestArgWarningNamesTheRealConsequence` 钉住三种措辞与"原因只取首行"。

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` = 0；`go test -race -count=1 ./middleware/ ./agent/` = 0。
- `gofmt -l` 对 `middleware/argguard.go`、`middleware/argguard_internal_test.go`、`middleware/argguard_test.go` 无输出（三个文件均 LF）。
- 包内 8 组：默认集与自定义集各计几条、恢复形态（`map[string]any`+`float64`）、写盘形状必须是纯 map、`requiredArgs` 五种降级、标记只作用一次、预算耗尽时新标记转为 stop、账本 JSON 往返后计数仍在、告警结尾说的后果与实际会发生的后果一致。
- 包外 7 组：三种不同坏参数计数到一次告警（历史里 `[arg-guard]` 恰好一条、文案含 `required arguments are: q, limit`）、升级后 `PendingHITL` 落盘并能 `Decide+Resume`、`OnEscalate: ArgGuardStop` 不产生任何暂停、干预预算耗尽后第二次升级直接终止、handler 自报 `IsError` 一条不计、与 LoopGuard 同装时两组 KV 键各自独立、跨"进程"续计数。
- 提交 `7c4d40f`（中间件本体与 14 组用例）+ `41e37ca`（告警结尾的后果措辞 + 1 组用例），均未推送。

---

## TASK-AG-04：离线示例 — 已完成 `e622dab`

### 改动

- 新增 `examples/arg-guard/main.go`：`llm/mock` 脚本模型，故意按顺序发出三种不同的非法参数（缺 `q` / 缺 `limit` / `q` 传成字符串），工具 `lookup` 的 schema 有两个必填字段。
  - 同一脚本跑两遍对比：一遍只装 `LoopGuard`（说明它此时不动作），一遍装 `ArgGuard`（看到计数、`[arg-guard]` 警告与 schema 提示、到阈值的升级）。
  - 打印每条 `ArgRejected` 事件与最终 `State.KV["_argguard.tools"]`，让"计数在 KV 里"可见。
  - 无网络、无密钥，秒级跑完。
- README 不动，理由见"明确不做"。

### 验收

- `go vet ./... && go test ./...` 绿；`go run ./examples/arg-guard` 退出码 0，输出里能看到：三种非法参数下 `LoopGuard` 无动作、`ArgGuard` 计数递增到警告、`required` 提示含两个字段名。

### TASK-AG-04 完成记录（2026-10-03）

比卡的正文多做的事，以及实施时定下的三处形状：

1. **第二遍跑完整个阶梯，不只跑到告警**：卡只要"计数递增到警告"，示例实际配 `WarnThreshold: 3 / EscalateThreshold: 5 / MaxInterventions: 2`，外层按"段"循环：遇 `Interrupted` 就 `Decide(Allow)` + `Resume` 再来一段。于是一段输出把 AG-03 的四条行为全走了一遍——告警一次、两次暂停待审、预算花完后第三次升级直接终止（`第 3 段：被直接终止`）。
2. **"终止"与"跑完"必须能分开**：`core.Stop` 走的仍是 `RunDone`（`agent/run.go:121` 的 default 分支），只判事件类型会把防护收尾读成模型答完了。示例改按 `RunDone.Result.Message.Text()` 是否为空区分：本例的 mock 从不发纯文本，所以空文本只可能是被 Stop 收尾那一段。
3. **打印位置从 mock 的响应函数挪回主流程**：模型跑在另一个 goroutine（`Run.drive`），它在响应回调里 `Println` 会与主流程消费事件的 `Printf` 交错——第一版就有"告警抬头与正文之间插进一条 `[拒绝]`"。现在回调只把看到的告警原文存进变量，主流程在每段消费完后再打印。
4. **告警插回它所属的位置，而不是段末**：`OnToolReject` 在 `Count == WarnThreshold` 那一次既发事件又写告警，所以按 `e.Count == warnAt` 记住行号，把 `[注入 ]` 那一块插到该行之后（`insertAfter`）。第一版没有这一步，读者会以为告警发生在第 5 次被拒之后。
5. **`State.KV` 摘要只列 `_argguard.` 前缀的键并按键名排序**：map 迭代顺序随机，第一版同一份状态三次跑出三种顺序；同时把核心的 `__approvals__` 排除在外，它不属于本示例要讲的账本。
6. **`step` 的读法在示例末尾说明**：`for step := 0` 每段 run 各自数（`agent/loop.go:153`），被放行那一批仍带暂停时的 step（`rlc.Step = rb.step`，`agent/loop.go:119`）。输出里因此出现 `step 5` 之后回到 `step 0`，这是既有语义，不改。
7. 卡里"三种非法参数"的第三种写的是「`q` 传成字符串」，实现按 schema 取的是**缺 `limit`** 与 **`q` 传成整数**两种错法（`{"limit":5}` / `{"q":"cats"}` / `{"q":7,"limit":5}`），第三条文案照此更正。

变异验证一次（改坏一处、跑示例、还原）：

| 改动 | 结果 |
| --- | --- |
| `withAttempt` 原样返回参数（三次调用签名相同） | A 段立刻出现 4 条 `[卡死 ] rule=repeat_call`，说明 A 段"LoopGuard 无动作"确实是"参数每次都在变"带来的，而不是 LoopGuard 没装上 |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0（含新包）；`go test ./...` = 0；`go test -race ./agent/... ./middleware/... ./core/...` = 0。
- `gofmt -l examples/arg-guard/main.go` 无输出（LF）。
- `go run ./examples/arg-guard` 退出码 0，约 0.7 秒；连跑四次输出逐字节相同（`diff` 无差异）。
- 输出含：A 段零条防护记录 + `失败：agent: loop exceeded MaxTurns`、B 段 `[拒绝 ]` 第 1..7 次计数、`required arguments are: q, limit` 原文、两次"停在人工决定"、第三段"被直接终止"、末行 `_argguard.interventions=3` 与 `_argguard.tools=map[lookup:map[count:7 warned:true]]`、两段 `lookup 的 handler 实际跑了 0 次`。
- 提交 `e622dab`（未推送）。

---

## TASK-AG-05：再补两个视角示例（追加，非原卡条目）— 已完成 `19d3895` + `551ad9f`

AG-04 那遍演的是阶梯本身。按 ADR-0029 的做法（配置视角之外另给"写工具的人"和"读事件流的人"）再补两个离线示例，`examples/arg-guard` 一并收进 ADR-0030 §10 的三行表。

### 改动

- `examples/arg-guard-tool-author/main.go`：同一个 schema（`date` string + `room` integer 都必填）跑四段。
  - 1/2 段共用一份五步脚本（别名字段 `checkin`、字符串数字 `"7"`、真缺 `room`、名字拼错、最后写对）；第 2 段的工具多一层 `PrepareArguments`，把别名与字符串数字接住，接不住的那次自己写一句"缺什么、下一步能干什么"。
  - 3a/3b 段是同一个 handler 每次都返回 Go 错误的工具（下游 503），差别只在 `Count` 用默认范围还是把 `agent.RejectHandlerError` 并进来。
- `examples/arg-guard-ops/main.go`：三段各读一样东西。
  - 1 段把每条 `core.ArgRejected` 用 `core.MarshalEvent` 打成一行 JSON，再 `UnmarshalEvent` 解回来。
  - 2 段同一份脚本分别落 `checkpoint.NewMemory()` 与 `checkpoint.NewFile(dir)`，读同一个计数键比较 `int` 与 `float64`；再把一个带私有字段的 struct 与一份纯 map 各 marshal 一遍。
  - 3 段用 File store 跑两段：第一段走到人工决定并 `agent.Reject`，第二段换新 store/新 agent/新中间件实例、同线程再跑一次。

### TASK-AG-05 完成记录（2026-10-03）

1. **作者示例的对照必须是同一份脚本、同一个工具名**，只有 `PrepareArguments` 一层不同：第 2 段的工具直接内嵌第 1 段的类型（`type repairedRoom struct{ bookRoom }`），`Name/Schema/Call` 全部继承。否则"少记两次"就可能来自阈值或脚本报错文案的差别。
2. **`PrepareArguments` 返错时循环会加前缀**：模型与告警里看到的是 `invalid arguments: room 是必填的房间号…`（`agent/exectools.go:247`）。作者写的那句话不是原文出现在历史里，措辞要按这个前缀来定，否则读起来是"invalid arguments: invalid arguments: …"。
3. **`unknown_tool` 记在被打错的名字上**，所以拼错的第三次调用不会并进正主的计数（`middleware/argguard.go` 用 `r.Call.Name`）。这是示例里现成的一条运维读法：账本里出现陌生键名＝模型在猜名字，不是阈值太低。
4. **把 `RejectHandlerError` 并进来之后，告警会念 required 清单**：那段输出里参数一直是对的，告警却说"Supply every required argument in one call"——这就是默认范围排除 handler 错误的理由，示例照原样打印，不做修饰。
5. **`eventWire.Step` 带 `omitempty`**（`core/eventjson.go:22`），所以 step 0 那条日志里根本没有 `step` 字段。消费侧要按缺省即 0 处理，示例把这一条直接打在输出里。
6. **干预预算在写标记那一刻扣**，与随后 `Allow` 还是 `Reject` 无关（AG-03 完成记录第 1 条的运维面）；而 `Reject` 那条调用不会回到 `callOne`，所以既不计次也不动预算。示例第 3 段因此把"否决之后接着跑完"的账本再打一遍，两次完全相同。
7. **`_argguard.actions` 可以带着没兑现的标记结束一段 run**：标记只在 `BeforeTool` 兑现那一次清掉，模型改成直接答文本就永远不会兑现它。示例末段输出里那个 `map[lookup:interrupt]` 就是这个形状，读法约定第 5 条照此写。
8. 两段式驱动：`drain(run)` 收事件、`collect(agent)` 额外把 `*agent.Run` 交回来给 `Decide/Resume` 用。打印一律在收完之后（AG-04 完成记录第 3 条的同一条约束）。

变异验证两次（各改坏一处、跑示例、还原后 `diff` 确认字节一致）：

| 改动 | 结果 |
| --- | --- |
| 作者示例：把 `checkin`→`date` 那段别名修复短路掉 | 第 2 段多出一次 `schema_invalid`（`book_room` 计数 1→2、handler 跑 3→2 次），说明"少记的那几次"确实是 preparer 接住的，不是脚本变了 |
| 运维示例：第 2 段改用一个空的 store 目录 | 计数从 4 掉回 1、`warned:false`、`interventions:0`，历史里第二条告警也没了——"重启续账"完全由同一目录的 checkpoint 提供 |

### 验收（实测）

- `go build ./...` = 0；`go vet ./...` = 0；`go test ./...` 无 FAIL（31 个包 ok）；两个新目录 `gofmt -l` 无输出（LF）。
- `go run ./examples/arg-guard-tool-author`、`go run ./examples/arg-guard-ops` 退出码 0，各约 0.4s；各连跑三次输出逐字节相同。
- 作者示例输出可见：第 1 段四条 `schema_invalid`/`unknown_tool` 记录与一条含 `required arguments are: date, room` 的告警、第 2 段只剩两条记录且 handler 跑 3 次、3a 段账本为空、3b 段三条 `handler_error` 与一条训斥参数齐全调用的告警。
- 运维示例输出可见：三条 `arg_rejected` JSON 行（第一条无 `step` 字段）、`类型 int` 与 `类型 float64` 两行对照、struct 进 KV 得到 `{"lookup":{}}`、第 2 段计数从 3 续到 4 且告警没重发、末段 `_argguard.actions=map[lookup:interrupt]`。
- 提交 `19d3895`（作者视角）+ `551ad9f`（运维视角），均未推送；README 仍不动（理由同"明确不做"最后一条）。

---

## 执行顺序与提交切分

1. TASK-AG-01 → `feat(agent): classify refused tool calls and report them to middleware`（已完成 `51df6fc`）
2. TASK-AG-02 → `feat(core): ArgRejected event for refused tool calls`（已完成 `adc064c`）
3. TASK-AG-03 → `feat(middleware): ArgGuard counts refusals per tool and escalates`（已完成 `7c4d40f`，另加一条措辞修正 `41e37ca`）
4. TASK-AG-04 → `docs(examples): offline demo for argument-rejection guarding`（已完成 `e622dab`）
5. TASK-AG-05（追加）→ `docs(examples): show the tool author's side of argument-refusal guarding`（`19d3895`）+ `docs(examples): read the argument-refusal ledger from outside the run`（`551ad9f`）

前置：ADR-0029 的 TO-01/TO-02 已合入（`toolOutcome` 与 `RejectTimedOut` 有实际来源）。若顺序相反，AG-01 需先自建 `toolOutcome`，并在 TO-02 合入时做字段合并 —— 那种情况下由后合的一方负责整合，不留两套结构体。

提交信息用英文（与仓库历史一致，Conventional Commits，不加 trailer）。每 TASK 单独提交，中间态 `go build ./...` 必须通过。`git push` 等确认后再做。

## 明确不做

- 不给 `core.ToolResult` 加错误类别字段（ADR-0030 备选 A，已否决）。
- 不改 `tool.Validate` 的校验强度与文案（`tool/validate.go` 的宽松策略是刻意的：未知关键字忽略，第三方便校验而不被误拒）。
- 不做工具级重试/回退（`RetryModel`、`FallbackModel`、`CircuitBreaker` 都只包 `llm.Model`，工具版本另议）。
- 不在进程内计算成功率百分比：原始计数交给 `obs/otel` 的 `agent.tool.calls`（`obs/otel/otel.go:117-119`）与 `ArgRejected` 事件流，聚合留给消费侧。
- 与 LoopGuard 的重复触发不去重（第 5 节已说明指令合并的安全性）。
- README 不新增行：现有「示例目录详解」未收录 `examples/loop-guard` 等防护中间件示例，单独为本特性补一行会造成新的不一致。README 与 `docs/adr` 索引的整体补齐另开文档任务。

## 风险

| 风险 | 现状判断 | 处置 |
| --- | --- | --- |
| 三类参数面拒绝仍不含"handler 错误"，模型反复触发同一 handler 错误时 ArgGuard 不动 | `Count` 默认集不含 `RejectHandlerError`；这类失败文本相同时会由 LoopGuard 的 `error_streak` 接手（`middleware/loopguard.go:221-230`），文本不同则两者都不动 | 已登记不修：把 handler 错误计入是本可配置的行为（`Count` 加上即可），默认不计是为了不把远端服务故障误判为模型不会调用；卡内 AG-03 的选项文档写明取舍 |
| 按工具名计数会对"同一工具被合理地反复修正参数"报警 | 阈值 3/6 是可调项；LoopGuard 已有 `ExemptTools` 先例（`middleware/loopguard.go:89-91`） | 已修：提供 `WarnThreshold`/`EscalateThreshold` 调节 + `OnDetect` 回调；未提供 `ExemptTools`（如实施中发现噪声大再加，同样是十行左右的代码） |
| `required` 提示依赖 `lc.Request` 在本阶段仍可用 | AfterModel/OnToolReject 时 `lc.Request` 已被赋值（`agent/loop.go:172`），但 `runResumed` 分支的 `lc` 未经过 CallModel（`agent/hitl.go:143`）→ 那里 Request 为 nil | 已修：取不到就省略该子句（AG-03 的降级路径），并在内部测试覆盖 `Request == nil` |
| 钩子调用点若将来被移进工作协程，KV 写会竞争 | 当前设计明确放在批次结束后串行（AG-01） | 已修：把这条前提写进 `ToolRejecter` 与 `argGuard.OnToolReject` 的注释，改动时编译器不报错但注释会提醒（无法用类型系统强制，接受） |
| 新增事件变体漏改 `eventjson` 会在运行时才报错 | `default` 分支返回错误而非 panic（`core/eventjson.go:89-90`），Redis 总线与 trace 日志会丢事件 | 已修：AG-02 的往返测试覆盖；`bus`/`queue` 既有测试同批跑 |
| AG-03 的警告文案语言未定（中文/英文） | `middleware/loopguard.go:409-413` 的 `warningMessage` 是英文正文 | 已修：实施前读过该函数全文，ArgGuard 的告警正文与升级 `Reason` 都用英文，只保留 `[arg-guard]` 前缀的体例一致（`middleware/argguard.go` 的 `warningMessage`/`escalationWords`） |
| ArgGuard 的升级标记若不清除，模型后来把参数写对了仍会被旧标记拦住 | 卡的正文没写清除这一步 | 已修：`BeforeTool` 命中即 `clearArgMark`（AG-03 完成记录第 2 条），并由 `TestArgMarkIsConsumedOnce` 覆盖 |
| 干预预算的作用点有两种读法（写标记时 / 命中时；只拦被标记的工具 / 拦一切调用） | 卡正文与被引用的 loopguard 先例不一致 | 已修：按先例取"写标记时决定动作、只作用于被标记的工具"，已与需求方确认；预算耗尽后未标记的调用照常运行，另有一条断言守着 |
| （AG-02 期间发现，与本卡无关）`go test -race ./queue/` 稳定失败：`queue.MemBus.Publish` 在释放锁后向快照通道发送，而 `Subscribe` 返回的 cancel 在锁内 `close` 同一通道，除竞态外还可能向已关闭通道发送 | `queue/bus.go:62-89`（cancel 在 `:68` 关闭通道，`Publish` 在 `:85` 释放锁后于 `:89` 发送）；`TestBridgeForwardsEvents` 实测 3/3 失败，改动前既有（`queue/` 本卡未触碰） | 登记不修：修它要动 `MemBus` 的发送/关闭策略（发送保持在锁内，或给订阅通道加 done 通道），与本卡的参数拒绝计数无关。另开一个小任务处理，勿在 AG-03/04 里顺手改 |
