# ADR-0023: LoopGuard —— 死循环/卡死检测中间件

- 状态: Accepted（已实施：`middleware/loopguard.go` + `core.StuckDetected` + `examples/loop-guard`）
- 日期: 2026-09-29
- 关联: ADR-0022(workspace)、被删的 ADR-0001~0021 中的循环/中间件章节；代码参照 `agent/loop.go`、`middleware/`

## 背景

长任务唯一的现有防线是 `maxTurns`（`agent/loop.go:18,125`，默认 16）和 transfer 深度上限（`agent/transfer.go:18-19`）。当模型陷入"重复同样的工具调用 / 连续吃同样的错误"时，循环不会提前停下，而是把剩余轮次全部烧掉，最后以 `ErrMaxTurnsExceeded` 硬报错收场（`agent/loop.go:233`）——既浪费 token，又没有可用的收尾产物。

具体要防的三类"卡死"形态：

1. **同调用复读**：模型反复发出签名完全相同的 tool call（name + 规范化 args），拿到同样的结果也不换策略。
2. **乒乓振荡**：A→B→A→B 周期为 2 的循环。
3. **错误连击**：连续多轮工具结果为 `IsError`（含参数校验失败、handler 错误、max_tokens 截断作废回喂 `agent/loop.go:188-194`），模型机械重试。

现有件都不覆盖这个面：`middleware/retry.go`/`circuit.go` 管的是**模型调用**的传输层健康；`agent/plan.go:43-60` 的 `MaxRetries` 管的是 **DAG 节点**粒度；`truncatedResults`（`agent/loop.go:297-311`）只提示重发、不计数。

## 名词

- **调用签名(signature)**：`tool name + 规范化后的 args JSON`（键序稳定）的哈希。
- **批次(batch)**：一步中 assistant 消息里的全部 ToolCall（`core.Message.ToolCalls()`，`core/message.go:64-68,116-120`）。
- **干预(intervention)**：guard 做出的一次有代价的动作（注入警告 / Interrupt / Stop），全局计数。

## 决策概览

新增 **`middleware.LoopGuard`**：一个纯 Middleware 扩展点实现，**不改 loop 的任何阶段语义**。它只重写两个钩子：

| 钩子 | 用途 |
| --- | --- |
| `AfterModel` | 每步唯一的分析点：同时看到本批 calls（`resp.Message`）与既往全部历史（`lc.History`），先于工具执行，拦截代价最低 |
| `BeforeTool` | 升级动作的执行点：返回 `Interrupt` 走既有 HITL 通道，或 `Stop` 直接终止（与 `middleware/permission.go:57-67` 完全同形） |

外加可选实现 `agent.RunFinisher`（`agent/middleware.go:42-44`）用于清理 per-run 计数。

三个立场决定了整体形态，都与仓库既有设计一致：

- **检测状态尽量从 History 推导，不放内存**。中间件实例随 Agent 构造、跨 run 共享（`agent/loop.go:62`），内存态在 HITL resume / 进程重启后必丢。而 `lc.History` 来自 checkpoint 的 `State.Messages`（`agent/loop.go:88,222`），且 Compaction 只改写请求不改写 State（`middleware/compaction.go:27,46-59`）——**历史是完整、持久、resume 后仍在的**，从它推导检测结论天然抗断点。
- **升级动作不发明新控制流**。警告用 `RunContext.Steer`（`agent/runtime.go:104`，loop 在下一步开头 drain 进 history，`agent/loop.go:130-132`）；暂停复用 `Interrupt` → `PendingHITL` checkpoint → `Run.Decide/Resume`（`agent/loop.go:197-208`、`agent/hitl.go`）；终止复用 `Stop`。零新 Directive 语义。
- **观测走 Bus**。`RunContext.Bus/Topic` 是导出字段（`agent/runtime.go:27-28`），中间件可自行 `Bus.Publish(Topic, ev)`（与 `rc.publish` 同一行，`agent/runtime.go:107`）。新增一个事件变体 `core.StuckDetected{Rule, Reason string; Step int}`（Event 是密封联合，须在 `core/event.go` 加 marker，同 `eventjson.go` 编解码）。

## 检测规则（均在 AfterModel 内完成）

### R1 复读 + 振荡：重复批次签名

把 `lc.History` 中既往 assistant 消息的每个批次算签名集合，加入当前批次后检查：**当前批次签名是否在过去 `Window`（默认 8）个批次里出现过 ≥ `RepeatThreshold-1` 次**（默认阈值 2，即"同样的批次数第二遍"即命中）。

- 单 call 复读（`[A,A,A]`）与乒乓（`[A],[B],[A],[B]`）都是该规则的子集——A 的第二次重现即命中，不需要独立的周期检测器。
- 窗口滑动意味着"隔了很久的合理重访"不误报。

### R2 错误连击：连续 error 结果

从 history 尾部向前扫 `RoleTool` 消息（`core/message.go:72-77`），按 CallID 配对到结果，统计**连续** `IsError` 且（tool name + 规范化错误首行）相同的条数，达到 `ErrorStreakThreshold`（默认 3）即命中。

- 同类错误换工具名不误报（可能是合理改道）；文本相同才计数。
- max_tokens 截断的固定作废文案（`agent/loop.go:305`）会计入——连续截断三次本身就是卡死，符合预期。

### 规范化与豁免

- args 与错误文本先做**签名归一**：JSON 重新序列化保证键序稳定；默认剥离易变键（`id`、`timestamp` 等，可配 `StripArgKeys`）。
- `ExemptTools []string`：轮询类工具（查进度、`wait` 型）合法地重复同参数调用，默认豁免 `write_todos`（todo 状态更新本就同参，`agent/planning.go:12-24`）。批次内**全部**调用都在豁免名单时该批次不参与 R1/R2。

### 误报兜底：批内重复不算循环

同一步里模型发出两个一模一样的 call 不拦截——那可能是合法的并行读写；R1 只在**跨步**比较。

## 升级阶梯与预算

命中后按"先便宜后昂贵"的阶梯处置，每一步都发 `core.StuckDetected` 事件：

1. **警告（免费）**：`lc.Steer` 注入一条 user 消息，文案带固定前缀 `[loop-guard]`，说明命中的规则与重复的调用名，要求换策略或产出最终答复。**已警告过的判定也是从 history 推导**：若过去 Window 个批次内、上次同签名批次之后已存在含 `[loop-guard]` 前缀的消息，则本次不再重复警告、直接升级。
2. **升级（按 `OnRepeat` 策略二选一）**：
   - `Interrupt`（默认）：BeforeTool 返回 `core.Directive{Kind: core.Interrupt, Reason: "loop-guard: ..."}`，run 走 HITL 落 `PendingHITL` 快照暂停——人可 `Decide(true)` 放行一批、或干脆结束。长任务里"问人"优于"静默烧轮次"。
   - `Stop`：直接终止，`final` 消息作为结果返回（`agent/loop.go:209-214` 路径）。适合无人值守 + queue 消费场景。
3. **干预预算**：per-run 干预总次数落在 `State.KV["_loopguard"]`（KV 随 checkpoint JSON 持久，`core/state.go:10-15,60-64`；`float64` 反序列化要容忍）。超过 `MaxInterventions`（默认 3）后无论策略一律 `Stop`，防止"警告→放行→再警告"本身成为新循环。

## 配置面

```go
middleware.LoopGuard(middleware.LoopGuardOptions{
    Window:               8,   // 回溯批次数
    RepeatThreshold:      2,   // 同批签名出现次数（含当前）
    ErrorStreakThreshold: 3,   // 连续同签名错误数
    ExemptTools:          []string{"..."},
    StripArgKeys:         []string{"id", "timestamp"},
    OnRepeat:             middleware.LoopGuardInterrupt, // 或 LoopGuardStop
    MaxInterventions:     3,
    OnDetect:             func(rule, reason string, step int), // 观察回调，同 CircuitOptions.OnStateChange 风格
})
```

零值即默认，风格对齐 `Compaction`/`Circuit` 工厂（`middleware/compaction.go:29-37`、`middleware/circuit.go:62-91`）。中间件工厂不加 `New` 前缀，符合仓库例外约定。

## 与其他层的组合

- **DAG plan / subagent / workflow Loop**：都以 `subRun`/`deeper` 环境跑各自的 AgentLoop（`agent/runtime.go:63-73`、`agent/subagent.go:8-21`），LoopGuard 装在 Agent 上即自动覆盖每个子 run；plan 层的 replan 轮次上限（`agent/plan.go:38-60`）管粗粒度，LoopGuard 管步级，两层正交。
- **并发**：所有检测都在 AfterModel（每步串行点）做，`BeforeTool/AfterTool` 里只读判定结果；不触碰 `AfterTool` 的 goroutine-safe 约束（`agent/middleware.go:20-21`）。
- **transfer**：交接后双方 Agent 各自带 guard 即可，共享 State.Messages 会使签名窗口重叠——交接本身就重置了"批次"粒度，可接受，不做特殊处理。
- **与 maxTurns 的关系**：本 ADR 不做"撞 maxTurns 前摘要收尾"（那是预算护栏 ADR 的事）；LoopGuard 生效后典型表现为 maxTurns 提前触发率下降。

## 备选方案

- **A. 有内存态的 per-run 计数器（sync.Map 按 RunID 键）**：判定更省扫描，但 HITL resume / 跨进程续跑后状态丢，需 RunFinisher 清理 + 恢复重建，复杂度高。被"从 history 推导"取代；仅干预总计数值无处可推导，落 `State.KV`。
- **B. 做成 loop 的内建阶段（CheckStuck）**：性能最好，但违背"loop 阶段稳定、HITL/权限/压缩/重试全部表达为 middleware"的既有立场（`agent/middleware.go:10-18`）。否决。
- **C. 嵌进 Compaction 或 RAG**：职责混淆。否决。
- **D. 语义相似度检测（embedding 比对 assistant 文本）**：引入模型调用成本与新依赖，收益不确定；第一版只做签名级精确匹配，留 `OnDetect` 回调给外部高级检测器接线。

## 成本

每步 O(Window) 次哈希 + 一次尾部线性扫错误，微秒级；无额外模型调用。

## 测试锚点

- `signature()`：键序、易变键剥离、非法 JSON args 容错。
- 规则表驱动：复读、乒乓、窗口外重现（不命中）、豁免工具、批内重复（不命中）、错误连击含截断文案。
- 阶梯：fake model 脚本历史 → 第一步 warn（history 里出现 `[loop-guard]` steer）→ 再犯 Interrupt（断言 `PendingHITL` 落盘）→ `Decide/Resume` 放行 → 超 `MaxInterventions` 断言 Stop。
- durable：Interrupt 后跨进程 `Resume`（复用 `agent/durable_test.go`/`resume_exec_test.go` 的机制），确认 KV 计数与 history 判定在恢复后行为一致。
- KV 反序列化 `float64` 容错。

## 实施切分

1. `core/event.go` + `core/eventjson.go`：`StuckDetected` 变体。
2. `middleware/loopguard.go`：签名/归一 + R1/R2 + warn/escalate + KV 计数 + RunFinisher 清理（可选）。
3. 测试 + 一个 `examples/` 演示（带故意复读的 fake model 看阶梯全程）。
