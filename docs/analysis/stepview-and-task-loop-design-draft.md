# StepView 与外层任务循环设计草稿

日期：2026-10-21
状态：草稿提议
范围：P0-1（StepView）与 P0-2（外层任务循环）的设计提议，未实施；不改变现有代码、公开 API 或存储格式。
关联：[Codex core 运行时对 goagent 的优化建议](codex-core-runtime-optimization-proposal.md)

## 1. 结论

这两项改动分别解决两个不同的问题：

- **P0-1（StepView）**：现在"模型看到的工具、审批看到的工具、实际执行的工具"是三次独立读取，彼此可能不一致。改为每个步骤在请求定稿后冻结一份只读视图，广告、校验、执行、检查点都只认这一份。
- **P0-2（外层任务循环）**：现在模型给出终稿后，循环直接返回；终稿期间到达的追加消息留在队列里，随运行上下文一起丢掉。改为在终稿提交前做一次原子判断：还有输入就进入下一轮，没有才结束；同时给追加消息一个可查询的投递回执。

两项可以独立落地，代码不冲突：

- 如果优先修正确性问题，先做 P0-2，改动更小，直接消除消息丢失。
- 如果优先建立可测试的边界，先做 P0-1，后续 P0-2 的续跑测试会更容易写。

本文只讨论这两项。按调用粒度并发、上下文超限重试、在途工具检查点、实时状态投影等，仍留在原分析文档里。

## 2. 目标与非目标

### 2.1 目标

1. 同一步骤内，"广播的工具一定可执行，执行的工具一定广播过"成为可断言的不变量。
2. 模型给出终稿后到达的追加消息，要么进入下一轮，要么得到明确的拒绝结果，不再静默丢失。
3. 续跑期间不释放线程占用，避免同一线程出现两个写入者。
4. 检查点增加一份很小的步骤快照，恢复时可以判断工具与设置是否发生变化。
5. 对现有公开 API 的破坏控制在最低：`Steer` 返回值变化，其余以新增为主。

### 2.2 非目标

- 不改变工具执行日志、占用声明、原子提交的现有语义。
- 不引入进程内单轮互斥来替代现有线程占用。
- 不做统一的输入队列与模型请求抢占；P0-2 只处理"终稿后的输入"。
- 不把追加消息变成持久消息；需要跨进程、跨暂停的输入仍走持久收件箱。
- 不改变 `RunDone`/`RunFailed`/`Interrupted` 的终结事件语义。

## 3. 现状

### 3.1 P0-1 的现有调用链

一个步骤当前按下面的顺序读取工具状态：

1. 组装请求时广播工具：[agent/loop.go:224](../../agent/loop.go#L224) 调用 `l.advertised(rc)`。
2. `advertised` 先放静态 schema，再合并运行期注入的工具：[agent/loop.go:96](../../agent/loop.go#L96)。
3. `mergeSchema` 发现同名时保留已有项：[agent/loop.go:106](../../agent/loop.go#L106)。
4. 中间件可以继续修改请求里的工具列表：[agent/loop.go:227](../../agent/loop.go#L227)。
5. 执行时才按名字解析工具，注入表优先于静态表：[agent/exectools.go:310](../../agent/exectools.go#L310)。
6. 参数校验使用的是被解析工具的 schema：[agent/exectools.go:325](../../agent/exectools.go#L325)。
7. 是否串行也是在执行时重新判断：[agent/exectools.go:514](../../agent/exectools.go#L514)。

### 3.2 同名替换的 schema 与实现不一致

这不是假设，是当前代码就能构造出来的情况：

- `advertised` 用 `mergeSchema`，同名时静态 schema 胜出。
- `callOne` 先查运行期注入表，同名时注入实现胜出。
- 现有测试只检查"同名只广播一次"和"注入实现被执行"，没有检查广播出去的 description/schema 是否来自注入实现：[agent/dynamictool_test.go:184](../../agent/dynamictool_test.go#L184)、[agent/dynamictool_test.go:205](../../agent/dynamictool_test.go#L205)。

结果是：模型看到的是静态工具的参数说明，运行时执行的却是另一个 schema 的工具。模型按静态 schema 生成的参数，可能被注入工具判为非法。这个问题由 P0-1 一并修复。

### 3.3 P0-2 的现有调用链

1. 步骤开头读取追加消息：[agent/loop.go:181](../../agent/loop.go#L181)。
2. 同一步骤读取持久收件箱：[agent/loop.go:184](../../agent/loop.go#L184)。
3. 模型没有工具调用时，提交检查点并直接返回：[agent/loop.go:264](../../agent/loop.go#L264)。
4. 返回前释放线程占用：[agent/loop.go:269](../../agent/loop.go#L269)。
5. `Run.Steer` 没有返回值，也不检查运行是否已经结束：[agent/run.go:144](../../agent/run.go#L144)。
6. 底层队列只有 `push` 和 `drain`，没有"已关闭"状态：[agent/runtime.go:122](../../agent/runtime.go#L122)。
7. 持久收件箱的 `Inbox` 只是读取未确认消息，不会移除：[checkpoint/durable_memory.go:87](../../checkpoint/durable_memory.go#L87)。

因此在终稿的模型调用期间到达的消息，会留在队列里；运行结束后队列随运行上下文丢弃。持久收件箱里的消息不会丢，但本轮运行会报告完成，消息要等下一次运行才被处理。

### 3.4 与占用、事件、暂停有关的事实

- 普通运行在开始时通过 `BeginRun` 拿到占用：[agent/run_prepare.go:44](../../agent/run_prepare.go#L44)。
- 运行驱动在结束时释放占用：[agent/run.go:259](../../agent/run.go#L259)。
- 检查点提交可以携带"释放占用"标记，存储层据此清空占用：[checkpoint/durable_memory.go:185](../../checkpoint/durable_memory.go#L185)。
- `TurnDone` 只表示一个步骤结束，不表示运行结束：[core/event.go:40](../../core/event.go#L40)；运行结束是 `RunDone`：[core/event.go:24](../../core/event.go#L24)。
- 暂停、停止、转移都会让循环提前返回，此时队列里可能还有消息。

## 4. P0-1：StepView 设计

### 4.1 设计原则

1. **两阶段冻结**：先冻结"这个运行有哪些工具"，再在 `ModifyRequest` 之后冻结"这一步向模型广播了哪些工具"。不能只冻结一次，因为中间件有权在 `ModifyRequest` 里增加工具。
2. **单一来源**：广告 schema、参数校验、串行判断、执行解析都从同一份 `ToolView` 读取。
3. **视图不可变**：每一步生成一份新视图；运行期注入只影响下一步，或通过显式的重建影响当前步。
4. **持久化只存摘要**：检查点存工具名、schema 摘要和设置摘要，不存完整 schema，避免检查点膨胀。
5. **恢复默认宽松**：工具实现变化不应轻易阻断恢复；只有"暂停批次明确批准的调用找不到了"才直接失败。

### 4.2 核心类型

```go
// agent/stepview.go

// StepView 是一步的只读快照。请求定稿后创建，模型调用、工具审批、
// 工具执行和检查点都读它。
type StepView struct {
    step    int
    model   string
    system  string
    options llm.Options
    history []core.Message // ModifyRequest 之后模型实际看到的消息
    tools   ToolView
    digest  StepDigest
}

type ToolView struct {
    advertised []llm.ToolSchema          // 最终请求里的顺序和内容
    byName     map[string]tool.Tool      // 只包含已广播的名字
    sequential map[string]bool           // 按最终解析结果记录
    names      []string                  // 与 advertised 同序
}

type StepDigest struct {
    ToolDigest     string
    SettingsDigest string
    Advertised     []string
}
```

对中间件暴露只读方法，不暴露字段：

```go
func (lc *LoopContext) View() *StepView
func (v *StepView) Step() int
func (v *StepView) Model() string
func (v *StepView) System() string
func (v *StepView) Options() llm.Options // 只读；Metadata 不参与摘要
func (v *StepView) Messages() []core.Message
func (v *StepView) Tools() *ToolView
func (v *StepView) Digest() StepDigest

func (tv *ToolView) Schemas() []llm.ToolSchema
func (tv *ToolView) Names() []string
func (tv *ToolView) Advertised(name string) bool
func (tv *ToolView) Resolve(name string) (tool.Tool, bool)
func (tv *ToolView) Sequential(name string) bool
```

`Options()` 返回结构副本，但 `Metadata` 仍是共享 map；文档要写明"只读，不要修改"。

### 4.3 运行期工具表

在构建请求前，先按"注入优先于静态"的规则快照一份运行期工具表：

```go
type runToolTable struct {
    byName     map[string]tool.Tool
    schemas    []llm.ToolSchema
    sequential map[string]bool
}

func (l *AgentLoop) snapshotTools(rc *RunContext) *runToolTable
```

规则：

- 名字集合 = 静态工具 ∪ 运行期注入工具。
- 同名时，注入工具同时赢得 schema 和执行实现。这修复 3.2 的不一致。
- 顺序：静态工具保持注册顺序；只存在于注入表的工具按名字排序追加。同名替换时保留静态位置，但内容换成注入工具的 schema。
- 串行标记：静态工具的 `seqTool` 或注入工具实现的 `tool.SequentialTool`，按最终胜出的实现判断。
- 这一步只读 `rc.dynamic` 的一次快照，避免广播和执行之间被并发修改。

### 4.4 一步的完整生命周期

```text
1. drain 追加消息 + 持久收件箱
2. CompactHistory
3. BeforeModel
4. snapshotTools(rc)                    ← 冻结"运行有哪些工具"
5. 用工具表构建 Request
6. ModifyRequest（可 AddTool、可改 Tools/Messages/Options）
7. 校验并冻结 StepView               ← 冻结"这一步广播了哪些工具"
8. 调用模型
9. AfterModel
10. CheckToolPolicy / BeforeTool 用 StepView
11. execTools / callOne 用 StepView
12. 写检查点，附 StepSnapshot
```

第 4 步放在 `BeforeModel` 之后，是因为 `BeforeModel` 也可以调用 `AddTool`；当前请求此时还没构建，注入应当被本步看到。

第 7 步的校验内容：

- 请求里的每个工具名都必须能在运行期工具表里解析到实现；否则在调用模型前失败，返回 `ErrToolViewInvalid`。当前行为是等模型调用后再给一个"unknown tool"结果，模型已经浪费了一次调用。
- 请求里没有的工具名不进入 `ToolView.byName`；模型如果仍然调用，`callOne` 返回"本步未广播"的拒绝结果，而不是继续执行。
- 同名但中间件改了 schema 的情况：`ToolView` 记录请求里的 schema（模型看到的），执行时仍用工具实现的 schema 校验。两者不一致时，在校验阶段返回 `ErrToolViewInvalid`，要求中间件同时替换实现或不要改 schema。这样不会出现"模型按 A 生成参数、工具按 B 校验"的隐性错误。

### 4.5 核心函数改动

| 函数 | 现在 | 改为 |
|---|---|---|
| `AgentLoop.advertised` | 静态 schema + 注入 schema，同名保留静态 | 删除；由 `snapshotTools` + `ToolView.Schemas()` 取代 |
| `mergeSchema` | 同名保留已有项 | 改为 `upsertSchema`：同名替换；仅用于 `AddTool` 对当前请求的更新 |
| `AgentLoop.addTool` | 构造期注册 transfer 工具 | 不变（构造完成后不再变化） |
| `LoopContext.AddTool` | 写入注入表并直接改当前请求 | 写入注入表；视图未定稿时更新当前请求，视图已定稿时只影响下一步 |
| `AgentLoop.run` | 单层步骤循环 | 拆为 `run` → `prepareRun` + `runTask` + `runStep`（见第 5 节） |
| `AgentLoop.execTools` | 接收 `lc`，内部再查表 | 从 `lc.View().Tools()` 取解析表 |
| `AgentLoop.callOne` | `dynamic.lookup` → `byName` | `lc.View().Tools().Resolve`；先检查 `Advertised` |
| `AgentLoop.isSequential` | 静态表 + 注入表各查一次 | `lc.View().Tools().Sequential(name)` |
| `AgentLoop.checkpoint` | 位置参数逐个传 | 改为 `commitOptions`，携带 `Snapshot` |
| `checkpoint.cloneCheckpoint` | 浅拷贝后克隆已知指针字段 | 增加 `Snapshot` 的深拷贝 |

`callOne` 的解析顺序在新设计里不再需要"先注入、再静态"的运行时回退，因为 `ToolView.byName` 已经按这个优先级建好。这样执行路径只读一个 map，不会出现"广播用一套、执行用另一套"。

新增拒绝类别：

```go
const (
    RejectUnknownTool RejectClass = iota
    RejectPrepareFailed
    RejectSchemaInvalid
    RejectHandlerError
    RejectTimedOut
    RejectNotAdvertised // 新增，追加在末尾，保留已有数值
)
```

### 4.6 检查点快照与恢复

在 `checkpoint` 包新增一个小结构：

```go
type StepSnapshot struct {
    Step           int      `json:"step"`
    Advertised     []string `json:"advertised,omitempty"`
    ToolDigest     string   `json:"tool_digest,omitempty"`
    SettingsDigest string   `json:"settings_digest,omitempty"`
    Model          string   `json:"model,omitempty"`
}
```

并挂到 `Checkpoint`：

```go
// Snapshot 记录写下这个检查点的步骤当时的模型可见视图。
// 旧记录没有该字段，读取时为 nil。
Snapshot *StepSnapshot `json:"step_snapshot,omitempty"`
```

摘要算法：

- `ToolDigest`：对最终请求里的每个工具，按顺序写入 `name`、`description`、`parameters` 的原始字节，再算 sha256。按请求顺序而不是名字排序，因为顺序也是模型输入的一部分。
- `SettingsDigest`：对 `Model`、`System`、`Temperature`、`MaxTokens`、`TopP`、`Stop`、`ToolChoice`、`Stream` 做规范化 JSON 后算 sha256；`Options.Metadata` 不参与，避免不可序列化的值让摘要计算失败。
- `Advertised`：最终请求里的工具名，按顺序保存。名字列表很短，直接存。

恢复规则：

- `RecoveryResumeTools`（暂停时保存的待审批批次）：先读暂停检查点的 `Snapshot`。批次里每个调用的工具名都必须在 `Advertised` 里，并且现在仍能解析到实现；否则恢复直接失败，返回 `ErrToolNotAdvertised`。理由是这个批次已经经过人工批准，不能悄悄换一个实现执行。
- `RecoveryReplayModel`（重新问模型）：按当前工具表重建视图；如果新视图的 `ToolDigest` 与快照不同，默认只记录一条诊断，继续恢复。部署升级后工具描述变化很常见，默认失败会让恢复不可用。需要严格的场景用 `WithStepSnapshotPolicy(StepSnapshotStrict)` 改为失败。
- 旧检查点没有 `Snapshot`：跳过比较，按当前工具表运行；本次运行写的新检查点会带上快照。

兼容性：

- `Checkpoint` 新增字段是 JSON 增量，旧记录读出来是 nil，旧版本的读者会忽略未知字段。
- `cloneCheckpoint` 当前是"整体浅拷贝 + 逐个克隆已知字段"：[checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30)。必须为 `Snapshot` 增加切片深拷贝，否则提交后调用方修改快照会污染存储。
- `commitRequestDigest` 会把新字段算进提交摘要；同一进程内的重试使用同一个对象，不受影响。

### 4.7 好处

- **消除广播与执行的偏差**：同名替换现在同时替换 schema 和执行实现；"执行的工具一定广播过"可以直接写成测试。
- **提前失败**：中间件广播了没有实现的工具，在调用模型前就报 `ErrToolViewInvalid`，不浪费一次模型请求。
- **恢复更可信**：暂停批次恢复前能确认工具还在，且就是当时广播的那一套；避免人工批准了一个已经不存在或已换实现的工具。
- **中间件可观察**：中间件可以看到这一步模型实际看到的消息、工具、设置，便于写测试、审计和诊断。
- **为后续改动留边界**：按调用粒度并发、工具暴露级别、在途检查点都可以基于 `ToolView` 和 `StepSnapshot` 继续做。

### 4.8 代价与风险

- **每步多一次摘要计算**：工具数量大时有哈希开销。可以先只对最终请求的工具列表算一次；工具表没变时可缓存，但第一版不必优化。
- **检查点变大**：每个检查点多一个工具名列表和两个摘要，通常几百字节；比完整 schema 小得多。
- **同名替换的行为变化**：以前模型看到静态 schema，现在看到注入 schema。这是修复，但属于可观察变化，需要在变更说明里写清楚。
- **中间件约束变严**：以前可以只在 `req.Tools` 里塞一个没有实现的 schema，让模型调用后得到 unknown tool；现在会在请求前失败。这是有意的，但需要检查现有中间件。
- **严格恢复模式可能挡住升级**：默认用宽松模式；严格模式只给确实要求完全重放的场景。
- **视图传参遗漏的风险**：`execTools`、`callOne`、`isSequential` 都要改成读视图；漏掉一处就会退回旧行为。用"视图未设置就 panic/返回错误"的测试兜底。

### 4.9 测试与验收

必须有的用例：

1. 静态工具、注入工具、同名替换三种情况：`Advertised` 的名字集合、schema 内容、实际执行实现三者一致。
2. 同名替换时，模型收到的是注入工具的 description/schema（补上 3.2 缺的断言）。
3. 中间件在 `ModifyRequest` 里广播一个没有实现的工具：模型调用前失败，错误是 `ErrToolViewInvalid`。
4. `AddTool` 在 `BeforeModel`、`ModifyRequest`、`AfterModel` 三个位置调用：前两者影响当前步，后者只影响下一步。
5. 一个工具要求串行：`ToolView.Sequential` 与最终执行调度一致。
6. `RecoveryResumeTools` 恢复时，暂停批次里的工具被移除：恢复失败，错误是 `ErrToolNotAdvertised`。
7. 旧检查点（没有 `Snapshot`）可以正常读取和恢复。
8. `StepSnapshot` 提交后修改原对象，不会影响存储里的副本。
9. 摘要稳定：同一组工具在不同注册顺序、不同 map 遍历顺序下，按相同请求顺序得到相同摘要。

验收不变量：

- 对任意一步，`executed ⊆ advertised`。
- 对任意一步，`advertised` 中每个名字都能解析到实现。
- 恢复一个暂停批次时，批次里的每个名字都在暂停检查点的 `Advertised` 里。

## 5. P0-2：外层任务循环设计

### 5.1 设计原则

1. **一个运行只结束一次**：`RunDone` 仍然是唯一的成功终结事件；一次运行内可以有多个"步骤完成"。
2. **回合边界显式**：模型给出无工具调用的回答，就是一次回合结束。回合结束时做一次输入检查，决定是否进入下一回合。
3. **判断与关闭原子**：检查"还有没有输入"和"运行是否停止接收输入"必须在同一个锁里完成，不能先检查再关闭。
4. **投递可查询**：`Steer` 返回一个票据；消息要么被并入某一步的模型请求，要么在运行结束时得到明确的未投递原因。
5. **占用跟着运行走**：只要还要继续下一回合，就不释放线程占用；真正结束时才释放。
6. **不把临时消息变成持久消息**：临时消息仍然不落盘；需要持久性的调用方用持久收件箱。

### 5.2 循环结构

把现在的单层循环拆成三层职责，但保持同一个 `Runnable` 接口：

```go
func (l *AgentLoop) run(rc *RunContext) runOutcome {
    st, out, ok := l.prepareRun(rc)
    if !ok {
        rc.steering.close(out.inputCloseReason())
        return out
    }
    out = l.runTask(rc, st)
    rc.steering.close(out.inputCloseReason())
    return out
}

type taskState struct {
    step    int
    maxStep int
    history []core.Message
    system  string
    usePrompt bool
}

func (l *AgentLoop) runTask(rc *RunContext, st taskState) runOutcome {
    for st.step < st.maxStep {
        out, next, again := l.runStep(rc, st)
        if !again {
            return out
        }
        st = next
    }
    return l.fail(rc, st.maxStep-1, st.history, ErrMaxTurnsExceeded)
}

// runStep 执行一个步骤：读输入、调模型、执行工具、写检查点。
// again=true 表示还有下一步（工具步骤继续，或终稿后还有输入）。
func (l *AgentLoop) runStep(rc *RunContext, st taskState) (runOutcome, taskState, bool)
```

`prepareRun` 负责现在 [agent/loop.go:115](../../agent/loop.go#L115) 到 [agent/loop.go:174](../../agent/loop.go#L174) 的工作：

- 复制历史、处理暂停恢复的待执行批次。
- 恢复批次结束后得到新的起始步。
- 渲染一次系统提示。
- 计算绝对上限 `maxStep = startStep + l.maxTurns`。

**关键点：`maxStep` 必须在最外层算一次。** 如果每个回合重新调用 `runStep` 时都用 `step + maxTurns` 当上限，追加消息会让总步数超过预算。

### 5.3 终稿分支与输入判断

当前终稿分支在 [agent/loop.go:264](../../agent/loop.go#L264)。改为：

```go
if len(calls) == 0 {
    if l.outputKey != "" {
        rc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: l.outputKey, Value: final.Text()})
    }

    nextHistory, entries, continuing, err := l.settleTurn(rc, step, st.maxStep, history)
    if err != nil {
        return l.fail(rc, step, history, err), st, false
    }
    rc.State.Messages = nextHistory
    if err := l.checkpoint(rc, step, commitOptions{
        releaseClaim: !continuing,          // 继续跑就不释放占用
        snapshot:     lc.view.snapshot(),
    }); err != nil {
        return runOutcome{Err: err}, st, false
    }
    rc.publish(core.TurnDone{Step: step})

    if continuing {
        resolveDelivered(entries, step+1)
        return runOutcome{}, taskState{
            step: step + 1, maxStep: st.maxStep,
            history: nextHistory, system: st.system, usePrompt: st.usePrompt,
        }, true
    }
    return runOutcome{Result: core.Result{Message: final}}, st, false
}
```

注意两处顺序：

- **先判断输入，再写检查点**，因为检查点要决定是否释放占用。
- **继续跑时 `releaseClaim=false`**。现在这个分支固定传 `true`：[agent/loop.go:269](../../agent/loop.go#L269)。如果继续跑却释放占用，另一个运行可以在本运行还在写历史时拿到同一线程。
- 最终释放仍由运行驱动负责：[agent/run.go:259](../../agent/run.go#L259)。循环里的 `releaseClaim=false` 不会让占用永久悬挂。

### 5.4 终稿输入判断协议

`settleTurn` 必须按下面的顺序做，才能保证"检查"和"关闭"之间没有窗口：

```text
1. entries = steering.drain()                  // 取走当前所有临时消息
2. inbox  = durable.Inbox(threadID)            // 只读，不确认
3. 如果 entries 或 inbox 非空：
     返回继续；队列保持开放
4. 如果两者都为空：
     执行 steering.drainOrClose()             // 同一把锁里：有消息就取走，没消息就关闭
     - 取到消息：返回继续
     - 关闭成功：返回结束
5. 结束分支：不再接收新的临时消息；
   关闭之后到达的 Steer 返回 ErrRunClosed。
```

为什么这样排：

- 第 2 步读持久收件箱可能较慢；在它执行期间到达的临时消息会被第 4 步的 `drainOrClose` 取到。
- 第 4 步在同一把锁里完成"检查是否为空"和"设置关闭"，因此不存在"检查为空之后、关闭之前"的窗口。
- 持久收件箱在第 2 步之后到达的消息不会被本轮读取，但它仍然留在存储里，下一次运行会处理；这不是丢失。
- 临时消息在关闭之后到达，会得到 `ErrRunClosed`，不是静默丢弃。

如果第 2 步读取失败，按步开头读取收件箱失败的处理一致：让运行失败。此时终稿已经在历史里，`fail` 会把它写入检查点；收件箱消息没有被确认，仍然留给下一次运行。

### 5.5 预算用尽时的处理

如果最后一步产生了终稿，但 `step + 1 >= maxStep`，没有预算再问模型：

- 不进入下一轮；
- 终稿正常提交并返回；
- 已经取到的临时消息按 `ErrMaxTurnsExceeded` 结算为"未投递"；
- 持久收件箱消息不确认，留给下一次运行。

这样调用方拿到的是有效终稿，而不是一个被丢弃的回答；追加消息的票据也会明确说明为什么没有投递。

### 5.6 队列与票据

把 `steeringQueue` 从"只有一个切片"改成带关闭状态和票据的队列：

```go
type steeringQueue struct {
    mu        sync.Mutex
    entries   []steerEntry
    closed    bool
    closedErr error
}

type steerEntry struct {
    msg    core.Message
    ticket *SteerTicket
}

type SteerTicket struct {
    id   string
    done chan struct{}

    mu  sync.Mutex
    out SteerOutcome
}

type SteerOutcome struct {
    Delivered bool
    Step      int
    Err       error // 未投递原因：ErrRunClosed / ErrRunInterrupted / ErrMaxTurnsExceeded
}
```

对外的结算规则：

- `Steer` 在队列开放时返回票据，消息进入队列；在队列关闭时返回 `nil, closedErr`。
- 票据在消息被并入某一步的模型请求时结算为"已投递"，`Step` 是那一步的步号。
- 运行在消息投递前结束，票据结算为"未投递"，`Err` 说明原因。
- **"已投递"只表示进入了那一步的模型上下文，不表示那一步的检查点已经提交。** 需要"落盘才算投递"的调用方应使用持久收件箱，而不是临时追加。

```go
func (t *SteerTicket) Done() <-chan struct{}
func (t *SteerTicket) Outcome() SteerOutcome
func (t *SteerTicket) Wait(ctx context.Context) (SteerOutcome, error)
```

票据只创建一次、只结算一次；结算后关闭 `done`。队列关闭是幂等的，关闭时把队列里剩余票据全部结算为未投递。

### 5.7 核心函数改动

| 函数 | 现在 | 改为 |
|---|---|---|
| `AgentLoop.run` | 单层步骤循环，终稿直接返回 | `prepareRun` + `runTask`，返回前关闭输入队列 |
| `runStep`（新增） | 原循环体 | 一个步骤；返回 `(outcome, next, again)` |
| `prepareRun`（新增） | 原 `run` 的前半段 | 历史、恢复批次、系统提示、`maxStep` |
| `settleTurn`（新增） | 无 | 终稿后的输入判断协议 |
| `steeringQueue.push` | 只追加切片 | 检查关闭状态，分配票据 |
| `steeringQueue.drain` | 只取消息 | 返回带票据的条目，并结算已投递 |
| `steeringQueue.drainOrClose`（新增） | 无 | 同一锁内"取走或关闭" |
| `steeringQueue.close`（新增） | 无 | 幂等关闭，结算残留票据 |
| `RunContext.Steer` | 无返回值 | `(*SteerTicket, error)` |
| `Run.Steer` | 无返回值 | `(*SteerTicket, error)` |
| `Run.drive` | 失败/正常结束 | 在所有终结路径关闭输入队列（兜底，幂等） |
| `Run.Cancel` | 取消上下文、释放占用 | 同时关闭输入队列 |
| `l.fail` / `l.interrupt` / Stop 分支 | 写检查点后返回 | 由 `run` 统一关闭；无需逐个分支加代码 |
| `AgentLoop.checkpoint` | 位置参数 | `commitOptions`，其中 `releaseClaim` 由终稿是否继续决定 |

`run` 的关闭兜底是有意放在最外层的：`fail`、`interrupt`、`BeforeModel` 的 Stop、工具返回的 Transfer 等分支很多，逐个加关闭逻辑容易漏。只要 `run` 返回，队列就一定关闭一次；`drainOrClose` 已经关闭的情况由幂等性吸收。

### 5.8 API 变化

| API | 变化 | 兼容性 |
|---|---|---|
| `Run.Steer` | `func (r *Run) Steer(msg) (*SteerTicket, error)` | 作为语句调用仍然编译；作为方法值或接口方法使用会破坏 |
| `RunContext.Steer` | 同上 | 同上；中间件里的语句调用不受影响 |
| `SteerTicket` / `SteerOutcome` | 新增导出类型 | 纯新增 |
| `ErrRunClosed` | 新增 | 运行已结束或不再接收输入 |
| `ErrRunInterrupted` | 新增 | 运行暂停，临时消息不再接收 |
| `ErrRunStopped` | 新增 | 运行被停止或转移 |
| `core.TurnDone` | 语义不变，但一次运行内可能出现多次 | **事件消费者必须用 `RunDone` 判断运行结束** |

兼容性说明：

- Go 允许把有返回值的函数调用当语句使用，所以现有 `run.Steer(...)`、`lc.Steer(...)` 调用点不需要修改。
- 如果仓库外有人把 `Steer` 赋给函数变量，或在自己的接口里声明了 `Steer(core.Message)`，需要同步更新。仓库内没有这种用法。
- `TurnDone` 之后可能再出现 `TurnStarted` 和 `MessageDone`。这是行为变化；所有把 `TurnDone` 当作流结束的适配器都要改为等 `RunDone`。

### 5.9 与其他机制的交互

**人工审批暂停。** 循环返回暂停结果时，`run` 会关闭队列，暂停前没来得及投递的临时消息结算为未投递（`ErrRunInterrupted`）。恢复会创建新的运行上下文和新的队列，旧的临时消息不会跨暂停保留。需要跨暂停的输入请用持久收件箱。

**停止、升级、转移。** 同样是终结结果，残留票据结算为未投递（`ErrRunStopped`）。转移到的子代理使用新的运行上下文和新的队列；父队列的票据在父循环返回时结算。

**持久收件箱。** 终稿判断只读、不确认。真正消费和确认发生在下一回合开头，随该步检查点一起提交。这样即使终稿判断之后进程崩溃，消息仍在收件箱里。

**运行预算。** 追加消息会让运行继续消耗步数；`maxStep` 在最外层固定，不会被续跑重置。最后一步的追加消息按 5.5 结算为未投递。

**会话。** `Session.Send` 的语义不变：它等待整个运行结束，包括中间的所有续跑。由于 `Session` 不暴露 `Run`，暂时无法从 `Session` 追加消息；需要追加时用 `Agent.Stream` 拿到的 `Run`。

**子运行。** `deeper()`、`subRun()`、`forBranch()` 都创建新的运行上下文，队列是独立的：[agent/runtime.go:55](../../agent/runtime.go#L55)、[agent/runtime.go:74](../../agent/runtime.go#L74)、[agent/runtime.go:96](../../agent/runtime.go#L96)。子运行的票据只对子运行有效；父运行关闭不会结算子队列，反之亦然。

**占用。** 续跑期间保持占用；真正结束时由检查点或运行驱动释放。嵌套运行共享同一份占用状态，本次改动不改变这一点。

### 5.10 好处

- **不再静默丢消息**：终稿期间到达的追加消息，要么进入下一轮，要么得到明确错误；调用方可以据此重试或改用持久收件箱。
- **运行生命周期更准确**：`RunDone` 表示真正结束，`TurnDone` 表示一次回合结束；观测端能区分"回答完了"和"运行完了"。
- **占用连续**：续跑期间不会出现线程被第二个写入者拿走的窗口。
- **投递可观测**：票据让调用方知道消息投到了第几步，或者为什么没投到。
- **改动集中**：终稿判断只改一个分支和队列实现，不需要重写工具执行、暂停恢复或存储层。

### 5.11 代价与风险

- **运行可能变长**：调用方不断追加消息时，`Wait` 会一直等到 `maxStep` 用尽。这是语义变化，需要在文档里写明；调用方应把追加消息当作"继续对话"，而不是"插一句话就结束"。
- **每个终稿多一次收件箱读取**：持久存储上一次读操作，通常很小；如果存储较慢，终稿到 `RunDone` 的延迟会增加。
- **存在关闭后的拒绝**：运行刚关闭时到达的 `Steer` 会返回 `ErrRunClosed`。这是正确行为，但调用方必须处理错误；以前是静默接受后丢失。
- **临时消息仍不持久**：进程崩溃、暂停、取消都会让未投递的临时消息消失；票据能报告原因，但不能恢复消息。需要持久性就用收件箱。
- **`Steer` 签名变化**：语句调用兼容，方法值和接口不兼容。
- **`TurnDone` 语义变化**：事件消费者如果把它当结束信号，会提前结束；必须改用 `RunDone`。
- **测试难度上升**：终稿瞬间的竞态需要用可控的模型/工具钩子精确制造，不能靠 sleep。
- **票据一直不读会占一点内存**：票据在结算后关闭，未被读取的票据随着运行上下文回收；数量受追加消息数量限制，风险很低。

### 5.12 测试与验收

必须有的用例：

1. **终稿期间追加**：模型在最后一次调用时阻塞；测试在阻塞期间 `Steer`；解除阻塞后，运行进入下一步，票据结算为已投递，`Step` 是下一步。
2. **终稿瞬间追加**：用可控钩子在 `settleTurn` 前后注入消息，验证只有两种结果：进入下一轮，或返回 `ErrRunClosed`；不能出现"消息在队列里但运行结束"。
3. **没有追加**：终稿后队列关闭，后续 `Steer` 返回 `ErrRunClosed`；票据没有悬挂。
4. **持久收件箱有待处理消息**：终稿后继续下一轮，消息在下一轮开头被确认并进入历史。
5. **收件箱读取失败**：运行失败，终稿写入检查点，消息未被确认。
6. **暂停**：暂停前的票据结算为 `ErrRunInterrupted`；恢复后的新运行不再看到旧临时消息。
7. **停止/转移**：票据结算为 `ErrRunStopped`。
8. **预算边界**：最后一步收到追加消息，返回终稿，票据结算为 `ErrMaxTurnsExceeded`。
9. **占用连续**：续跑期间另一个 `BeginRun` 得到 `ErrClaimConflict`；运行结束后可以正常开始新运行。
10. **步数上限不被重置**：连续多次追加导致多回合，总步数不超过 `MaxTurns`。
11. **事件顺序**：`TurnDone` 之后可以出现 `TurnStarted`，最终只有一个 `RunDone`。
12. **兼容**：现有 `Run.Steer(...)` 语句调用、暂停恢复、失败检查点测试全部保持通过。

验收不变量：

- 任何一张票据最终都会被结算（已投递或未投递），不会永远等待。
- 运行结束后，队列里没有未结算的条目。
- 终稿判断之后，运行要么进入下一轮，要么关闭队列；不存在"已返回但队列仍开放"。
- 续跑期间不释放占用。

## 6. 合并后的架构

```text
Run
 ├─ lifecycle（原有）：RunStarted → 一个或多个回合 → RunDone/RunFailed/Interrupted
 ├─ RunContext
 │   ├─ steeringQueue（新增关闭状态与票据）
 │   ├─ dynamicTools（原有）
 │   └─ durable（原有：占用、收件箱确认、工具执行）
 └─ AgentLoop
     ├─ runTask（新增外层）
     │   ├─ prepareRun：历史、恢复批次、系统提示、maxStep
     │   └─ for step < maxStep
     │       └─ runStep（原步骤体）
     │           ├─ drain 输入 + 收件箱
     │           ├─ CompactHistory
     │           ├─ BeforeModel
     │           ├─ snapshotTools → Request → ModifyRequest
     │           ├─ finalize StepView（校验广播集合）
     │           ├─ 模型调用 → AfterModel
     │           ├─ 审批/执行（只读 StepView）
     │           ├─ 写检查点（带 StepSnapshot）
     │           └─ 工具步骤：again=true
     │               终稿：settleTurn → again=true/false
     └─ fail / interrupt：终稿或错误检查点，输入队列由 run 兜底关闭
```

数据流：

- `StepView` 决定"这一步模型能看到什么、能执行什么"。
- `StepSnapshot` 只把"看到了什么"的摘要写进检查点，供恢复比较。
- `steeringQueue` 决定"什么时候还接收输入"，票据把结果告诉调用方。
- `RunDone` 只由运行驱动发布一次，`TurnDone` 每个步骤发布一次。

## 7. 兼容性与迁移

### 7.1 存储

- `Checkpoint.Snapshot` 是新增的可选字段，旧记录读出为 nil，不需要数据迁移。
- 旧版本程序读取新记录时会忽略该字段；新旧版本可以交错读取同一存储。
- `cloneCheckpoint` 必须补快照的深拷贝，否则存储里的副本可能被调用方修改。
- `commitRequestDigest` 会把新字段纳入摘要；同一进程内的幂等重试不受影响。

### 7.2 公开 API

- `Steer` 的返回值变化对语句调用兼容；对方法值、接口实现不兼容。仓库内已检查，没有方法值用法。
- `TurnDone` 的语义收紧为"步骤结束"，消费者必须用 `RunDone` 判断运行结束。
- `RejectClass` 新增值追加在末尾，已有数值不变。
- `StepView`、`ToolView`、`SteerTicket`、`StepSnapshot` 都是新增类型，不删除现有 API。

### 7.3 行为

- 同名注入工具现在同时替换 schema 和执行实现；以前只替换执行实现。
- 中间件广播没有实现的工具，从"模型调用后得到 unknown tool"变为"调用前失败"。
- 终稿后可能继续运行，`Wait` 的持续时间可能变长。
- 运行关闭后的 `Steer` 从静默接受变为返回错误。

## 8. 待定问题

1. `StepSnapshot` 是否要保存完整 schema，而不只是摘要？建议先摘要；需要离线重放时再加。
2. 恢复漂移默认"记录并继续"还是"失败"？建议默认继续，严格模式可选。
3. `AddTool` 在 `ModifyRequest` 里同名替换时，是否允许只换实现、不换 schema？建议不允许；要么都换，要么用不同的名字。
4. 最后一步的追加消息是"返回终稿并丢弃票据"，还是"让运行失败"？建议返回终稿，票据报告未投递。
5. 临时消息是否要在暂停时写入检查点？建议不写；暂停恢复的输入走持久收件箱。
6. `StepView` 是否要直接暴露给仓库外的中间件？建议先通过 `LoopContext.View()` 暴露只读方法，字段不导出。
7. 终稿判断读持久收件箱失败时，是让运行失败还是带警告返回终稿？建议失败，和步开头的处理一致。
8. 票据"已投递"是否需要等到检查点提交？建议不等；需要落盘语义的调用方用持久收件箱。

## 9. 建议的落地拆分

按可独立验证的小步提交：

1. **StepView 运行时部分**：`ToolView`、`snapshotTools`、视图校验、`callOne`/`execTools`/`isSequential` 改用视图；不含持久化。
2. **StepSnapshot 持久化**：`checkpoint.StepSnapshot`、`Checkpoint.Snapshot`、克隆逻辑、恢复比较。
3. **队列与票据**：`steeringQueue` 关闭状态、`SteerTicket`、`Steer` 签名变化；此步只加能力，不改循环。
4. **外层循环**：`prepareRun`/`runTask`/`runStep`、终稿判断、占用释放条件；接入第 3 步的队列。
5. **文档与示例**：更新 `Steer` 用法、`TurnDone` 语义说明；补"终稿期间追加"的示例。
6. **回归**：暂停恢复、失败检查点、动态工具、串行工具、预算、事件顺序全部重跑。

每一步的完成标准：

- 第 1 步：`executed ⊆ advertised` 的断言通过；同名替换的 schema 测试通过。
- 第 2 步：旧检查点可读；暂停批次缺工具时恢复失败；快照深拷贝测试通过。
- 第 3 步：票据在暂停/停止/正常结束时都能结算；没有悬挂票据。
- 第 4 步：终稿期间追加进入下一轮；续跑期间占用不释放；总步数不超预算；`RunDone` 只出现一次。
