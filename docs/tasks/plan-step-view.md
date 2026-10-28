# P0-1 StepView 实施计划

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文只覆盖 P0-1；P0-2 不在本计划内。

**目标：**按照 [StepView 与任务循环设计草稿](../analysis/stepview-and-task-loop-design-draft.md) §4，为每一步冻结一份只读执行视图，让广告内容、参数校验、串行判断、执行解析和检查点摘要都读同一份数据。

**架构：**`LoopContext` 增加 `view` 字段；在 `ModifyRequest` 之后构造 `StepView`；检查点只保存工具名和摘要；恢复时先核对暂停批次的工具，再按策略比较重放视图。运行期注入仍然只影响本次运行，但同名注入会同时替换 schema 和执行实现。

**技术栈：**Go 1.25（本机 go1.26.4）、标准库、现有 mock LLM、checkpoint Memory/File、bus。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-21。
- 基线：`85fbb98`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./agent ./checkpoint -count=1` 确认基线。
- 设计依据：[stepview-and-task-loop-design-draft.md](../analysis/stepview-and-task-loop-design-draft.md) §4（P0-1：StepView）。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：P0-1。P0-2（外层任务循环、steering 票据、TurnDone 语义）不在本计划。

当前工作区有未跟踪的分析文档和 `.scratch/` 目录。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。本文和设计草稿的提交由用户另行安排；任务卡里的提交只包含该卡列出的代码、测试、示例和 README。

## 1. 现有能力与实施边界

### 1.1 现在一步是怎么走的

1. 构造请求时，`AgentLoop.advertised` 把静态工具 schema 和运行期注入工具 schema 合在一起；同名时保留静态 schema（[agent/loop.go:96](../../agent/loop.go#L96)、[agent/loop.go:106](../../agent/loop.go#L106)）。
2. 中间件的 `ModifyRequest` 可以改请求，也可以调用 `LoopContext.AddTool`；后者写入运行期注入表，并对当前请求做一次同名保留的合并（[agent/loopctx.go:56](../../agent/loopctx.go#L56)）。
3. 工具执行时，`callOne` 先查注入表，再查静态表（[agent/exectools.go:307](../../agent/exectools.go#L307)）；它不检查这个名字是否在本步请求里广播过。
4. 串行判断同时查静态标志和注入表（[agent/exectools.go:514](../../agent/exectools.go#L514)）。
5. 暂停恢复时，`Agent.Resume` 先拿到占位（claim），再构造待执行批次（[agent/hitl.go:86](../../agent/hitl.go#L86)、[agent/hitl.go:181](../../agent/hitl.go#L181)）；批次在第一次模型调用之前执行（[agent/loop.go:123](../../agent/loop.go#L123)），恢复过程不核对暂停时的工具广告。
6. 检查点写入是一个按位置传参的辅助函数（[agent/loop.go:376](../../agent/loop.go#L376)）；检查点里没有“本步模型看到了什么”的记录。

### 1.2 本计划要解决的问题

- 同名注入时，模型看到的是静态 schema，实际执行的是注入实现。广播和执行可能不一致。
- 中间件可以广播一个没有实现的工具；当前要等模型调用后才发现，浪费一次模型请求。
- 一个已注册但本步没有广播的工具，模型如果仍然调用，当前会真的执行。执行的工具不一定是本步广播过的。
- 审批恢复时，暂停批次里的工具可能已经被移除或替换；当前没有恢复前核对。
- 检查点无法告诉恢复过程“暂停时模型看到的是哪套工具和设置”。

### 1.3 可观察行为变化

| 场景 | 现在 | 本计划之后 | 原因 |
| --- | --- | --- | --- |
| 同名注入 | 模型看到静态 schema；执行注入实现 | 模型看到注入 schema；执行注入实现 | 广播与执行同源 |
| 广播了没有实现的工具 | 模型调用后得到 unknown tool 结果 | 调用模型前失败，返回 `ErrToolViewInvalid` | 提前失败 |
| 已注册但本步未广播的工具被调用 | 可能被执行 | 拒绝为 `RejectNotAdvertised` | 恢复“执行 ⊆ 广播” |
| 从未注册的工具被调用 | `RejectUnknownTool` | 保持不变 | 兼容已有分类 |
| 视图冻结后调用 `AddTool` | 可影响当前步的工具解析 | 只影响下一步 | 本步视图不可变 |
| 恢复时暂停批次里的工具被移除 | 执行时报 unknown tool，或按新表执行 | 恢复前失败，返回 `ErrToolUnavailable` | 人工批准过的批次不能悄悄换实现 |
| 同名静态工具注册两次 | 广告出现两个槽位，执行最后一个实现 | 广告只保留一个槽位，内容和实现都用最后一个 | 名字集合唯一 |
| 旧检查点没有 `Snapshot` | 正常恢复 | 正常恢复，跳过视图核对 | 兼容旧记录 |

这些变化是修复，但属于可观察行为，需要在提交说明和示例输出里写清楚。

### 1.4 不做什么

- 不实现 P0-2：不拆外层任务循环，不引入输入票据，不改 `TurnDone` 语义。
- 不把 `AgentLoop.run` 拆成 `prepareRun`/`runTask`/`runStep`。设计草稿 §4.5 里的这一行与 P0-2 共用，P0-1 只在内层步骤循环里插入“快照工具表”和“冻结视图”两个动作。
- 不做按调用粒度的并发、工具暴露级别、可发现工具、在途检查点。这些是后续改动。
- 不把完整 schema 写进检查点；只存工具名和摘要。
- 不把运行期注入工具跨恢复持久化。注入工具是运行作用域的，恢复时无法还原。
- 不改变 `Agent.Run`/`Stream`/`Resume` 的签名，不新增存储后端。

## 2. 已核实的事实

下表是实施前核对过的代码位置。执行时如果行号已经漂移，按函数名和代码内容定位，不要按行号硬改。

| 事实 | 位置 |
| --- | --- |
| 构造请求后调用 `ModifyRequest` | [agent/loop.go:224](../../agent/loop.go#L224)、[agent/loop.go:227](../../agent/loop.go#L227) |
| 静态 schema 和注入 schema 合并，同名保留静态 | [agent/loop.go:96](../../agent/loop.go#L96)、[agent/loop.go:106](../../agent/loop.go#L106) |
| `AddTool` 写注入表并合并当前请求 | [agent/loopctx.go:56](../../agent/loopctx.go#L56) |
| 构造期 transfer 工具只写入 `byName`/`schemas`，不写入 `tools` | [agent/loop.go:84](../../agent/loop.go#L84) |
| 执行解析先注入、后静态 | [agent/exectools.go:307](../../agent/exectools.go#L307) |
| 串行判断同时查静态标志和注入表 | [agent/exectools.go:514](../../agent/exectools.go#L514) |
| 恢复先取得 claim，再构造批次 | [agent/hitl.go:65](../../agent/hitl.go#L65)、[agent/hitl.go:86](../../agent/hitl.go#L86)、[agent/hitl.go:181](../../agent/hitl.go#L181) |
| 恢复批次在模型调用前执行 | [agent/loop.go:123](../../agent/loop.go#L123)、[agent/hitl.go:244](../../agent/hitl.go#L244) |
| 检查点辅助函数按位置传参 | [agent/loop.go:376](../../agent/loop.go#L376) |
| 检查点提交摘要使用显式字段列表 | [checkpoint/coordinator.go:42](../../checkpoint/coordinator.go#L42)、[checkpoint/coordinator.go:171](../../checkpoint/coordinator.go#L171) |
| 检查点克隆是浅拷贝加逐字段克隆 | [checkpoint/snapshot.go:30](../../checkpoint/snapshot.go#L30)、[checkpoint/snapshot.go:35](../../checkpoint/snapshot.go#L35) |
| `Checkpoint` 当前字段 | [checkpoint/checkpoint.go:32](../../checkpoint/checkpoint.go#L32) |
| 运行期注入表是 `dynamicTools` | [agent/runtime.go:147](../../agent/runtime.go#L147) |
| 中间件配置在 `config` | [agent/options.go:16](../../agent/options.go#L16) |
| 拒绝分类常量 | [agent/middleware.go:36](../../agent/middleware.go#L36) |
| `ArgRejected` 文档列出的分类 | [core/event.go:154](../../core/event.go#L154) |
| 事件标记清单 | [core/event.go:163](../../core/event.go#L163) |
| `runbudget` 会把请求工具列表清空 | [middleware/runbudget.go:181](../../middleware/runbudget.go#L181) |
| 现有同名注入测试只数广告槽位 | [agent/dynamictool_test.go:184](../../agent/dynamictool_test.go#L184)、[agent/dynamictool_test.go:205](../../agent/dynamictool_test.go#L205) |
| 从未注册工具的分类断言 | [agent/reject_test.go:86](../../agent/reject_test.go#L86) |
| 静态串行工具降级整批 | [agent/seqexec_test.go:68](../../agent/seqexec_test.go#L68) |
| 旧暂停检查点（无 Pause、无 Snapshot）必须可恢复 | [agent/phase1_interrupt_test.go:731](../../agent/phase1_interrupt_test.go#L731) |
| 注入工具的超时路径 | [agent/tooltimeout_test.go:335](../../agent/tooltimeout_test.go#L335) |

## 3. 锁定的设计决定

### D1 两阶段冻结，冻结时重新取一份工具表

第一步在 `BeforeModel` 之后、构造请求之前取一次运行期工具表，用它生成请求里的工具列表。第二步在 `ModifyRequest` 之后冻结视图。

冻结时必须重新取一份工具表，不能只用第一步那份。原因：中间件可以在 `ModifyRequest` 里调用 `AddTool`，第一步的表格看不到这次注入。冻结时重新读取静态表和注入表，才能解析刚注入的工具。现有测试 [agent/dynamictool_test.go:27](../../agent/dynamictool_test.go#L27) 和 [agent/tooltimeout_test.go:379](../../agent/tooltimeout_test.go#L379) 都是在 `ModifyRequest` 里注入工具，必须继续通过。

### D2 注入优先，顺序稳定

- 名字集合 = 静态工具 ∪ 运行期注入工具。
- 同名时，注入工具同时赢得 schema 和实现。
- 只存在于注入表的工具按名字排序，追加在静态工具之后。
- 静态工具保持注册顺序。这里的顺序以 `l.schemas` 为准，因为它包含构造期 `addTool` 追加的 transfer 工具；`l.tools` 只用于提示词构建，不含 transfer 工具。
- 同名静态工具注册多次时，只保留第一个槽位，内容和实现都用最后一个注册的实现。这是为了让广播名字唯一，也修复“两个槽位、执行最后一个”的不一致。
- 串行标记按最终胜出的实现判断，不再分别查静态表和注入表。

### D3 请求里的 schema 必须和实现一致

冻结时对请求里的每个工具做三件事：

1. 名字必须能在最终工具表里解析到实现。
2. 名字不能重复。
3. 请求里的 `name`、`description`、`parameters` 必须和实现重新生成的 schema 一致。`parameters` 按 JSON 语义比较，不按字节比较，避免键顺序不同造成误报；两边都为空（nil 或空字节）时视为相等。

任一条件不满足，返回 `ErrToolViewInvalid`，在调用模型之前失败。

这条规则意味着中间件不能只改 schema 不改实现。要替换工具，使用 `AddTool` 同时替换实现。这是设计草稿 §4.4 的要求，属于收紧的行为，需要在变更说明里写清楚。

### D4 未广播和从未注册分开

`callOne` 先判断名字是否在本步广告里：

- 不在广告里，但在冻结时的运行期工具表里：`RejectNotAdvertised`。
- 不在广告里，也不在运行期工具表里：`RejectUnknownTool`。

保留 `RejectUnknownTool` 是兼容要求。[agent/reject_test.go:86](../../agent/reject_test.go#L86) 用一个从未注册的名字断言 `unknown_tool`，这个用例必须继续通过。

`RejectNotAdvertised` 追加在常量末尾，保留已有常量的数值。`String()` 返回 `"not_advertised"`。`ArgRejected` 文档里的分类列表补上它。

### D5 AddTool 在冻结前后的语义

- 冻结前（`BeforeModel`、`ModifyRequest` 等）：写注入表；如果当前请求已经创建，对请求做同名替换（`upsertSchema`），新名字追加。
- 冻结后（`AfterModel`、`CheckToolPolicy`、`BeforeTool`、`AfterTool` 等）：只写注入表，不改当前步的视图，也不改已经发送的请求。下一步重新取表时生效。
- `AddTool` 的文档要写明这一点。现有文档已经建议从 `ModifyRequest` 调用；本计划把“冻结后只影响下一步”补清楚。

这条规则会改变一个旧行为：以前在 `AfterModel` 或 `AfterTool` 注入同名工具，可能影响当前步剩余调用的解析；现在当前步固定用冻结时的实现，下一步才切换。并发执行时旧行为本来就不确定，新行为是确定的。

### D6 检查点只存摘要，写快照的范围明确

`checkpoint.StepSnapshot` 只保存步骤号、广告工具名、工具摘要、设置摘要和模型名。不保存完整 schema，不保存消息。

写快照的范围：

- 有冻结视图的步骤：正常结束、无工具终稿、`max_tokens` 续跑、`AfterModel` 的 Stop、审批门的 Stop、两类 Interrupt、流式/AfterModel/检查策略/BeforeTool/执行错误，都写当前视图的快照。
- 冻结之前的失败或停止：`BeforeModel` 的 Interrupt/Stop、`ModifyRequest` 之前的错误、提示词构建错误、收件箱错误、达到最大轮数，写 nil。
- 恢复批次的检查点、再次暂停、恢复批次失败：写暂停检查点原来的快照；旧检查点没有快照就写 nil。

这样检查点里的快照和它对应的步骤视图一致，不会拿下一步的视图去描述上一步。

### D7 恢复批次有自己的执行视图

恢复批次不进模型调用，但要走 `CheckToolPolicy`、`BeforeTool` 和 `execTools`。这些阶段需要一个视图：

- 有暂停快照时，广告名字取暂停快照的 `Advertised`，实现按当前静态表解析。运行期注入工具不跨恢复持久化，所以不在解析结果里；暂停批次里的名字必须在恢复前已经通过核对。
- 没有暂停快照（旧检查点）时，广告名字取暂停批次里的调用名字（按原顺序去重），实现按当前静态表解析；摘要为空。
- 恢复视图的 `Request` 保持 nil。现有中间件已经处理 `Request == nil`（例如 [middleware/runbudget.go:192](../../middleware/runbudget.go#L192) 和 [middleware/argguard.go:211](../../middleware/argguard.go#L211)），不要为了视图去伪造一个请求。
- 恢复视图的 `Schemas()` 只包含当前能解析到的工具；`Names()` 和 `Advertised()` 仍按暂停快照的名字列表（旧检查点按批次名字），保证批次里的调用判断的是“暂停时广播过什么”。这个差异要写在文档里。
- 广告了但当前解析不到实现的名字只可能出现在旧检查点的恢复视图里。这种情况下 `callOne` 仍返回 `RejectUnknownTool`，与当前行为一致；有暂停快照的恢复会先经过 D8 的预检，批次里解析不到的名字到不了执行阶段。
- TASK-SV-01 落地时检查点还没有 `Snapshot`，恢复视图按“没有暂停快照”的规则构造，保证已有恢复测试继续通过；TASK-SV-03 再让它优先读取暂停快照。

### D8 恢复预检放在取得 claim 之前

`Agent.Resume` 在 `validatePause` 之后、取得 claim 之前做预检：

1. 不是 LLM agent（`a.loop == nil`）或没有待执行批次：跳过。
2. 有暂停快照时，批次里的每个工具名必须在 `Snapshot.Advertised` 里；否则返回 `ErrToolNotAdvertised`。
3. 每个工具名必须能在当前静态工具表里解析到实现；否则返回 `ErrToolUnavailable`。
4. 旧检查点没有快照时，跳过第 2 步，只做第 3 步。

预检失败时不取得 claim，不需要释放占位。预检成功后才走原来的 claim、决定读取、状态克隆和批次构造。

运行期注入工具不会被持久化，所以暂停批次里如果是注入工具，恢复会返回 `ErrToolUnavailable`。这是有意的：不能悄悄用一个不存在的工具继续执行。当前行为也不会执行它（恢复后注入表是空的），只是失败得更晚、错误更难懂。

### D9 重放恢复的漂移策略

只有“没有待执行批次的恢复”（`cp.Pending == nil || len(cp.Pending.Pending) == 0`，与 [agent/hitl.go:167](../../agent/hitl.go#L167) 的判断一致）才设置重放基线：失败后的恢复、`RecoveryReplayModel` 的暂停恢复、普通线程再次 `Resume`。待审批批次的恢复不设置基线，因为批次执行完后的下一步本来就是新的视图。

基线来自检查点的 `Snapshot`。恢复后的第一步冻结视图时，比较工具摘要和设置摘要：

- 两者都相同：不做事。
- 任一不同：
  - 默认 `StepSnapshotWarn`：发布 `core.StepViewDrifted` 事件，继续调用模型。
  - `StepSnapshotStrict`：在调用模型前返回 `ErrStepViewDrift`，运行失败。失败检查点写入当前视图的快照，下一次恢复以新视图为基线。
  - `StepSnapshotOff`：不保存基线，也不比较。

基线只消费一次。第一步比较完就清空，后面的步骤不再比较。工具或设置的摘要为空（手工构造的旧快照）时，跳过对应比较，避免误报。

系统提示如果包含时间等每次都变的内容，会触发默认的 Warn 事件。这是诊断，不是失败；需要完全重放的场景才用 Strict。

### D10 公开错误和事件

新增以下导出符号：

- `agent.ErrToolViewInvalid`：冻结视图时请求里的工具名、schema、重复名不合法。
- `agent.ErrToolNotAdvertised`：恢复批次的工具不在暂停快照的广告里。
- `agent.ErrToolUnavailable`：恢复批次的工具现在解析不到实现。
- `agent.ErrStepViewDrift`：严格策略下，重放视图和基线不一致。
- `core.StepViewDrifted`：宽松策略下的漂移诊断事件。
- `agent.StepSnapshotPolicy` 及 `StepSnapshotWarn`、`StepSnapshotStrict`、`StepSnapshotOff`。
- `agent.WithStepSnapshotPolicy`。
- `agent.RejectNotAdvertised`。

错误都用 `%w` 包装，调用方可以用 `errors.Is` 判断。

### D11 摘要算法

工具摘要：按最终请求里的顺序，对每个工具依次写入 `name`、`description`、`parameters` 的原始字节，字段之间带长度前缀，最后算 sha256。顺序也是模型输入的一部分，所以按请求顺序，不按名字排序。

设置摘要：对 `Model`、`System`、`Temperature`、`MaxTokens`、`TopP`、`Stop`、`ToolChoice`、`Stream` 做规范化 JSON 后算 sha256。`Options.Metadata` 不参与，避免不可序列化的值让摘要计算失败。`Stop` 的顺序保留。

广告名单：按请求顺序保存工具名。

工具的 `Description()` 和 `Schema()` 必须在一次运行内稳定；不稳定会导致冻结时的 schema 核对失败。这条要写进风险说明。

### D12 视图只读，Request 保留

`StepView` 和 `ToolView` 的字段不导出；访问器返回切片副本，`Options()` 还复制 `Stop` 切片。`Parameters` 的字节、`llm.Options.Metadata` 的 map 和工具实现本身是共享的，文档写明“只读，不要修改”。

`LoopContext.Request` 继续保留。中间件在 `BeforeModel`/`ModifyRequest` 仍然只能读请求；`AfterModel`、`BeforeTool`、`AfterTool` 里读 `Request` 仍然可用。视图是执行解析的权威来源，但 `Request` 不删除，避免破坏现有中间件。

### D13 诊断事件走现有事件通道

agent 包没有日志器，事件是既有的诊断通道。`StepViewDrifted` 和已有的 `StuckDetected`、`HistoryCompacted` 一样，通过 Run 的事件流发布。

## 4. 公共契约

以下是待新增的 API，不是当前已有接口。实施时按这里的名字和语义定稿，并在包注释里补齐错误语义。

### 4.1 运行期视图

```go
// agent/stepview.go

// StepView 是一步的只读快照。请求定稿后创建，模型调用、工具审批、
// 工具执行和检查点都读它。
type StepView struct {
    step    int
    model   string
    system  string
    options llm.Options
    history []core.Message
    tools   ToolView
    digest  StepDigest
}

type ToolView struct {
    advertised []llm.ToolSchema // 本步广告的 schema；正常步骤与最终请求同序
    byName     map[string]tool.Tool
    sequential map[string]bool
    names      []string
    known      map[string]struct{} // 冻结时运行期工具表里的全部名字，用于区分拒绝分类
}

type StepDigest struct {
    ToolDigest     string
    SettingsDigest string
    Advertised     []string
}

func (lc *LoopContext) View() *StepView

func (v *StepView) Step() int
func (v *StepView) Model() string
func (v *StepView) System() string
func (v *StepView) Options() llm.Options
func (v *StepView) Messages() []core.Message
func (v *StepView) Tools() *ToolView
func (v *StepView) Digest() StepDigest

func (tv *ToolView) Schemas() []llm.ToolSchema
func (tv *ToolView) Names() []string
func (tv *ToolView) Advertised(name string) bool
func (tv *ToolView) Resolve(name string) (tool.Tool, bool)
func (tv *ToolView) Sequential(name string) bool
```

`View()` 在 `BeforeModel` 和 `ModifyRequest` 里返回 nil；在 `ModelContext` 之后（模型调用、`AfterModel`、`CheckToolPolicy`、`BeforeTool`、`AfterTool`、`ToolReject`、`OnError`）返回非 nil。恢复批次的 `BeforeTool`、`AfterTool` 也返回非 nil。

副本语义：`Options()` 返回结构副本并复制 `Stop` 切片，`Metadata` 仍是共享 map；`Messages()` 只复制切片本身，消息和其中的 Part 共享；`Digest()` 返回的 `Advertised` 是副本。共享部分按只读使用。

### 4.2 检查点快照

```go
// checkpoint/checkpoint.go

// StepSnapshot 记录写下这个检查点的步骤当时的模型可见视图。
// 旧记录没有该字段，读取时为 nil。
type StepSnapshot struct {
    Step           int      `json:"step"`
    Advertised     []string `json:"advertised,omitempty"`
    ToolDigest     string   `json:"tool_digest,omitempty"`
    SettingsDigest string   `json:"settings_digest,omitempty"`
    Model          string   `json:"model,omitempty"`
}

type Checkpoint struct {
    // ...现有字段...
    Snapshot *StepSnapshot `json:"step_snapshot,omitempty"`
}
```

`Model` 记录请求里的 `Options.Model`；没有设置时为空，不拿 provider 的默认模型名冒充。

### 4.3 策略、错误和事件

```go
// agent/stepview.go 和 agent/options.go

type StepSnapshotPolicy int

const (
    StepSnapshotWarn StepSnapshotPolicy = iota // 默认：发布事件，继续
    StepSnapshotStrict                         // 返回 ErrStepViewDrift
    StepSnapshotOff                            // 不比较
)

func WithStepSnapshotPolicy(p StepSnapshotPolicy) Option

var (
    ErrToolViewInvalid   = errors.New("agent: step view invalid")
    ErrToolNotAdvertised = errors.New("agent: tool was not advertised at the pause")
    ErrToolUnavailable   = errors.New("agent: tool is no longer available")
    ErrStepViewDrift     = errors.New("agent: step view drifted from the resumed checkpoint")
)

// core/event.go
type StepViewDrifted struct {
    Step                   int
    BaselineStep           int
    ExpectedToolDigest     string
    ActualToolDigest       string
    ExpectedSettingsDigest string
    ActualSettingsDigest   string
}
```

`StepViewDrifted` 加入事件的 sealed union 和标记清单。它只在至少一个摘要不同时发布。

### 4.4 拒绝分类

```go
const (
    RejectUnknownTool   RejectClass = iota // 名字不在冻结时的运行期工具表里
    RejectPrepareFailed
    RejectSchemaInvalid
    RejectHandlerError
    RejectTimedOut
    RejectNotAdvertised // 名字在运行期工具表里，但不在本步广告里
)
```

`RejectNotAdvertised` 追加在末尾，已有数值不变。`String()` 返回 `"not_advertised"`。

## 5. 交付顺序与依赖

```text
SV-01 → SV-02 → SV-03 → SV-04 → SV-05
```

按编号顺序执行。SV-01 是运行期视图，后面的检查点、恢复和漂移策略都依赖它。SV-04 依赖 SV-03 在恢复时设置基线。SV-05 最后做示例、文档和全量验收。

| 任务 | 可独立审阅的交付 | 建议规模 |
| --- | --- | --- |
| SV-01 | 运行期 `StepView`/`ToolView`、冻结、执行解析、拒绝分类 | 2–3 天 |
| SV-02 | `StepSnapshot`、检查点深拷贝、提交摘要、写入范围 | 1–2 天 |
| SV-03 | 恢复预检、恢复视图、暂停批次执行、旧检查点兼容 | 1–2 天 |
| SV-04 | 重放漂移策略、诊断事件、配置项 | 1 天 |
| SV-05 | 示例、README、全量验收、执行记录 | 0.5–1 天 |

规模是执行安排参考，不是时限承诺。

开工前先跑：

```bash
git status --short
git log -1 --oneline
go test ./agent ./checkpoint -count=1
```

基线测试如果已经失败，先停下来报告，不要带着失败开始第一张卡。

## 6. 任务卡

### TASK-SV-01：冻结运行期步骤视图

**依赖：**无。**决策：**D1–D5、D11、D12。

**文件：**新增 `agent/stepview.go`、`agent/stepview_test.go`、`agent/stepview_internal_test.go`；修改 `agent/loop.go`、`agent/loopctx.go`、`agent/exectools.go`、`agent/runtime.go`、`agent/middleware.go`、`core/event.go`（只改 `ArgRejected` 的文档）。

- [x] 写视图生命周期测试 `TestStepViewLifecycle`：在 `BeforeModel`、`ModifyRequest` 里 `lc.View() == nil`；在 `ModelContext`、`AfterModel`、`BeforeTool`、`AfterTool` 里非 nil，并且 `Step`、`System`、`Messages`、`Options`、`Tools().Names()`、`Digest()` 与请求一致。
- [x] 写 `TestInjectedToolReplacesSchemaAndImplementation`：在 `ModifyRequest` 里用同名工具替换静态工具，断言请求里只有一次广告、description 和 parameters 来自注入工具、执行结果也来自注入工具。补上 [agent/dynamictool_test.go:184](../../agent/dynamictool_test.go#L184) 缺少的 schema 断言。
- [x] 写 `TestAdvertisedToolWithoutImplementationFailsBeforeModel`：在 `ModifyRequest` 里向 `req.Tools` 追加一个没有实现的 schema，断言返回 `ErrToolViewInvalid`，模型调用次数为 0。
- [x] 写 `TestToolSchemaRewriteFailsBeforeModel`：只改请求里的 description 或 parameters，不改实现，断言 `ErrToolViewInvalid`，模型调用次数为 0。
- [x] 写 `TestDuplicateAdvertisementFailsBeforeModel`：向请求追加同名 schema，断言 `ErrToolViewInvalid`。
- [x] 写 `TestAddToolPhaseVisibility`，至少覆盖四个位置：
  - `BeforeModel` 注入：本步请求能看到并执行。
  - `ModifyRequest` 注入：本步请求能看到并执行。这是最关键的一条，防止冻结只查请求前的旧表。
  - `AfterModel` 注入：本步请求不包含，下一步请求包含并执行。
  - `AfterTool` 注入：本步批次不包含，下一步请求包含并执行。
- [x] 写 `TestInjectedSequentialToolDowngradesBatch`：用 `tool.AsSequential` 注入一个同名替换工具，两个调用同批执行时最大并发为 1。再写一个反向用例：静态串行工具被非串行的注入实现替换后，两个调用可以并发。
- [x] 写 `TestNotAdvertisedRejectionClass`：中间件在 `ModifyRequest` 里清空 `req.Tools`，模型调用一个已经注册的工具；断言 `ToolRejecter` 收到 `RejectNotAdvertised`，模型看到“未广播”的错误文本，工具没有执行。
- [x] 写 `TestUnknownToolRejectionClassUnchanged`：从未注册的名字仍然是 `RejectUnknownTool`。可以复用 [agent/reject_test.go:86](../../agent/reject_test.go#L86) 的场景，不要改已有断言。
- [x] 写 `TestResumeBatchHasFallbackView`：旧检查点（此时还没有快照字段）恢复的批次执行时 `lc.View()` 非 nil，`Names()` 来自批次调用名，`Request` 仍为 nil，批次里的静态工具正常执行。这是 SV-01 不做恢复预检时的过渡行为，SV-03 会把它升级为优先读暂停快照。
- [x] 写 `TestStepViewReturnsCopies`：修改 `Schemas()`、`Names()`、`Digest().Advertised`、`Messages()`、`Options().Stop` 的返回值，再次读取视图不受影响。
- [x] 写摘要测试 `TestStepViewDigestStability`：同一组动态工具用不同的 map 插入顺序注入，请求顺序和摘要相同；设置里只有 `Metadata` 不同，设置摘要相同；温度或系统提示不同，设置摘要不同。
- [x] 写内部测试 `TestSnapshotToolsOrderAndOverride`：静态 a、b，注入 b 覆盖和 c；断言广告顺序为 a、b、c，b 的 schema/实现/串行标记来自注入。
- [x] 写内部测试 `TestSnapshotToolsDeduplicatesStaticNames`：同名静态工具注册两次；断言只广告一次，内容和实现都用最后一个。唯一名字继续使用构造期缓存的 schema。
- [x] 写内部测试 `TestToolDigestOrderAndContent`：相同顺序相同内容摘要相同；换顺序、换 description、换 parameters 摘要不同。
- [x] 写内部测试 `TestSettingsDigestExcludesMetadata`：`Metadata` 不同摘要相同；温度不同摘要不同。

实现要求：

- [x] 新增 `agent/stepview.go`：`StepView`、`ToolView`、`StepDigest`、`runToolTable`、`snapshotTools`、`freezeStepView`、`resumeStepView`、`upsertSchema`、`toolDigest`、`settingsDigest`、`canonicalJSON`、错误常量。`StepView`/`ToolView` 的访问器对 nil 接收者安全。`resumeStepView` 在本卡只做旧检查点的兜底：广告名字取批次调用名去重，实现按当前静态表解析，设置和摘要为空；SV-03 再让它优先读暂停快照。
- [x] `snapshotTools` 读取一次 `rc.dynamic` 的副本。这里的“静态工具”以 `l.schemas` 的顺序为准（它包含构造期 `addTool` 追加的 transfer 工具），实现从 `l.byName` 取；不要遍历 `l.tools`，那个列表只用于提示词构建，不含 transfer 工具。按名字去重时保留第一个槽位、内容和实现用最后一个注册的实现；注入工具优先替换同名项；只存在于注入表的工具按名字排序追加。唯一且未被注入替换的静态工具复用 `l.schemas` 里的缓存，同名替换和有重复的名字重新生成 schema。
- [x] `freezeStepView` 在 `ModifyRequest` 之后执行：从 `req` 取 `System`、`Messages`、`Options` 和工具列表（`history` 用 `req.Messages`，不是 `lc.History`）；校验名字唯一、名字可解析、请求 schema 与实现 schema 一致；然后构造 `ToolView`，记录 `names`、`byName`、`sequential`、`known`，计算摘要。校验失败返回 `ErrToolViewInvalid`，错误里带工具名和原因。
- [x] `agent/loop.go`：删除 `advertised`；把 `mergeSchema` 改为 `upsertSchema`（`addTool` 的构造期注册也用它）；构造请求前调用 `snapshotTools`，请求工具来自返回的表；`ModifyRequest` 之后调用 `freezeStepView`，把结果放进 `lc.view`；失败时走 `fail`。恢复批次分支用 `resumeStepView` 构造视图放进 `rlc.view`，否则 `execTools` 的非空检查会让已有恢复测试失败；同时删掉不再使用的 `slices` import、`seqTool` 字段、`newLoop` 里的串行标记构建循环和 `addTool` 里的 `seqTool` 写入。
- [x] `agent/loopctx.go`：增加 `view` 字段和 `View()`；`AddTool` 在视图冻结后只写注入表，不再改当前请求；冻结前用 `upsertSchema` 更新当前请求。
- [x] `agent/runtime.go`：`dynamicTools` 增加 `snapshot()`，返回名字到实现的副本；删除不再使用的 `advertised()` 和 `slices` import。
- [x] `agent/exectools.go`：`execTools` 开头要求 `lc.view != nil`，否则返回“没有冻结视图”的内部错误；批量串行判断改用 `lc.view.Tools().Sequential(name)`；`callOne` 先用 `Advertised` 判断，再 `Resolve` 取实现：未广播但已知的名字返回 `RejectNotAdvertised`，完全未知的名字返回 `RejectUnknownTool`，广告了但解析不到实现（只可能是旧检查点的恢复视图）也返回 `RejectUnknownTool`，与当前行为一致；删除 `isSequential`。`agent/loop.go` 同时删除 `seqTool` 字段和 `newLoop` 里的构建循环。
- [x] `agent/middleware.go`：追加 `RejectNotAdvertised` 和 `String()` 分支；更新 `ToolRejecter` 文档里的拒绝类型列表。
- [x] `core/event.go`：`ArgRejected` 的文档列表补上 `not_advertised`。
- [x] 更新 `AddTool` 注释：写清冻结前后的行为差异，以及“每步视图不可变”。

验证：

```bash
go test ./agent -run 'Test(StepView|InjectedTool|AddTool|NotAdvertised|UnknownTool|SnapshotTools|ToolDigest|SettingsDigest)' -count=1
go test ./agent -count=1
go test ./middleware -count=1
go test -race ./agent -run 'Test(StepView|InjectedTool|Sequential)' -count=1
go vet ./agent ./core
```

**完成标准：**对普通步骤，执行的工具一定在该步广告里，广告里的每个名字都能解析到实现；旧检查点的恢复视图是过渡例外（广告了但已删除的工具在执行时按未知工具拒绝，SV-03 会改为恢复前失败）。同名注入的 schema、实现和串行标记三者一致；`View()` 在冻结前为 nil；`ModifyRequest` 里注入的工具在当前步可用；`AfterModel`/`AfterTool` 里注入只影响下一步；已有 agent 和 middleware 测试全部通过。

**提交：**`feat(agent): freeze a step view for tools and settings`

### TASK-SV-02：把步骤快照写进检查点

**依赖：**SV-01。**决策：**D6、D11。

**文件：**新增 `checkpoint/stepview_test.go`、`agent/stepview_checkpoint_test.go`、`agent/stepview_checkpoint_internal_test.go`；修改 `checkpoint/checkpoint.go`、`checkpoint/snapshot.go`、`checkpoint/coordinator.go`、`agent/loop.go`、`agent/stepview.go`、`agent/hitl.go`。

- [x] 写 `TestStepSnapshotRoundTrip`，覆盖 Memory 和 File：保存带快照的检查点，`Latest`/`Load` 读回字段一致；File 重新 `NewFile` 打开目录后仍然一致。
- [x] 写 `TestStepSnapshotCloneIsolation`：读回两份，修改第一份的 `Advertised` 和摘要，第二份和存储里的副本不受影响。
- [x] 写 `TestStepSnapshotDigestRetry`：同一个检查点对象提交两次，第二次按内容相同识别为重试；只改快照内容、保持 ID 不变，返回 `ErrCheckpointConflict`。这条证明快照进入了提交摘要。
- [x] 写 `TestLegacyCheckpointDigestUnchanged`：先用 `Commit` 提交一个没有 `Snapshot` 的检查点，再用一个字段相同、同样没有快照的对象提交同一个 ID；应当被识别为重试。这条证明 `omitempty` 保住了旧记录的摘要。
- [x] 写 `TestPauseCheckpointCarriesSnapshot`：用审批门暂停一个运行，读取最新检查点，断言 `Snapshot` 非 nil、`Advertised` 包含被暂停调用的工具、两个摘要非空、`Model` 等于请求里的模型选项。
- [x] 写 `TestResumeCheckpointKeepsPausedSnapshot`：预置一个带快照的暂停检查点，恢复并执行批次；恢复批次写出的检查点 `Snapshot` 与暂停检查点一致。
- [x] 写 `TestNormalStepCheckpointCarriesSnapshot`：普通工具调用步和无工具终稿步的检查点都带快照，广告名单与请求一致。
- [x] 写 `TestFailCheckpointCarriesSnapshot`：模型在视图冻结后报错，失败检查点带当前视图的快照。冻结前的失败（例如 `BeforeModel` 停止）不带快照。
- [x] 写 `TestStepSnapshotNoStoreDoesNotFail`（内部测试）：直接构造 `AgentLoop` 和 `RunContext{Store: nil}`，只带快照调用检查点辅助函数，返回 nil；带 pending 或 pause 时仍返回 `ErrCheckpointUnavailable`。公共服务层无法构造 nil store，`New` 会把 nil 换成 Memory（[agent/agent.go:51](../../agent/agent.go#L51)）。

实现要求：

- [x] `checkpoint/checkpoint.go` 增加 `StepSnapshot` 和 `Checkpoint.Snapshot`，字段与 JSON 标签按 §4.2。注释写明旧记录为 nil、只存名字和摘要。
- [x] `checkpoint/snapshot.go` 的 `cloneCheckpoint` 增加 `Snapshot` 深拷贝，`Advertised` 切片要复制。
- [x] `checkpoint/coordinator.go` 的 `commitDigestCheckpoint` 增加 `Snapshot *StepSnapshot json:"step_snapshot,omitempty"`，并在构造 payload 时赋 `cp.Snapshot`。旧记录没有该字段时摘要保持不变。
- [x] `agent/stepview.go` 增加 `(*StepView).snapshot()` 和 `cloneStepSnapshot`；快照的 `Advertised` 与摘要来自视图，`Step`/`Model` 来自视图。
- [x] `resumeBatch` 增加 `snapshot *checkpoint.StepSnapshot` 字段；`Agent.Resume` 在构造批次后把 `cp.Snapshot` 深拷贝进去。`newResumeBatch` 的签名保持不变，避免影响内部测试。
- [x] `agent/loop.go` 把 `checkpoint` 辅助函数的按位置参数改成 `commitOptions`：

```go
type commitOptions struct {
    Step       int
    Pending    *checkpoint.PendingHITL
    Pause      *checkpoint.Pause
    Snapshot   *checkpoint.StepSnapshot
    Release    bool
    Executions []checkpoint.ToolExecution
}
```

- [x] 按下面的表改所有调用点：

| 写入点 | 快照来源 |
| --- | --- |
| 恢复批次执行后的检查点 | `rb.snapshot` |
| 恢复批次再次暂停 | `rb.snapshot` |
| 恢复批次失败 | `rb.snapshot` |
| 普通步骤正常结束、无工具终稿、`max_tokens`、`AfterModel` Stop、审批门 Stop | `lc.viewSnapshot()` |
| `AfterModel`/`BeforeTool` 的 Interrupt | `lc.viewSnapshot()` |
| 流式错误、`AfterModel` 错误、`CheckToolPolicy`/`BeforeTool` 错误、执行错误 | `lc.viewSnapshot()` |
| `BeforeModel` 的 Interrupt/Stop、`ModifyRequest` 之前的错误 | `lc.viewSnapshot()`（此时为 nil） |
| 提示词构建错误、收件箱错误、达到最大轮数 | nil |

- [x] 增加 `(*LoopContext).viewSnapshot()`，视图为 nil 时返回 nil，避免每个调用点重复判空。
- [x] `interrupt` 和 `fail` 增加一个 `*checkpoint.StepSnapshot` 参数；`interrupt` 把它放进 `commitOptions`，`fail` 同样。所有调用点按上表传入。
- [x] 保持“没有 Store 时，只有 pending 或 pause 才报 `ErrCheckpointUnavailable`”的现有规则；只有快照时仍然允许普通计算。

验证：

```bash
go test ./checkpoint -run 'Test(StepSnapshot|LegacyCheckpoint)' -count=1
go test ./agent -run 'Test(PauseCheckpoint|NormalStepCheckpoint|FailCheckpoint|StepSnapshotNoStore)' -count=1
go test ./agent ./checkpoint -count=1
go test -race ./checkpoint -count=1
```

**完成标准：**带视图的检查点都有快照；快照经过 Memory 和 File 存取后字段一致；修改读回的快照不影响存储；提交重试稳定；旧检查点在保存和重试时摘要不变；暂停检查点能支持 SV-03 的恢复预检。

**提交：**`feat(checkpoint): persist step view snapshots`

### TASK-SV-03：恢复前核对暂停视图

**依赖：**SV-02。**决策：**D7、D8。

**文件：**新增 `agent/stepview_resume_test.go`；修改 `agent/hitl.go`、`agent/stepview.go`。必要时只改 `agent/thread_recovery_test.go` 的测试辅助，不改它的行为。

- [x] 写 `TestResumeRejectsToolNotInPausedView`：暂停快照的 `Advertised` 只有 `keep`，待执行批次调用 `drop`；`Resume` 返回 `ErrToolNotAdvertised`，工具没有执行。
- [x] 写 `TestResumeRejectsRemovedTool`：快照广告过 `danger`，但当前 agent 没有这个工具；`Resume` 返回 `ErrToolUnavailable`。
- [x] 写 `TestResumeRejectsDynamicTool`：暂停批次里的名字是运行期注入工具，当前静态表没有；`Resume` 返回 `ErrToolUnavailable`，错误文本说明运行期工具不能跨恢复恢复。
- [x] 写 `TestResumePreflightBeforeClaim`：用一个包装 `checkpoint.Memory` 的测试存储记录 `Claim` 调用次数；预检失败时次数为 0，预检成功时次数为 1。
- [x] 写 `TestResumeBatchUsesPausedView`：预置一个带快照的暂停检查点，批准的调用是快照广告过的工具；在 `BeforeTool` middleware 里记录 `lc.View()` 和 `lc.Request`，断言视图非 nil、`Names()` 和 `Digest().Advertised` 是暂停快照的名字、`Request == nil`，工具执行一次，恢复批次写出的检查点保留原快照。
- [x] 写 `TestResumeReinterruptPreservesSnapshot`：第一次恢复时审批门再次 Interrupt，新的暂停检查点快照与原来的快照一致；第二次恢复用允许已批准调用的门，工具执行。
- [x] 写 `TestLegacyPendingCheckpointView`：旧检查点没有 `Pause` 和 `Snapshot`，恢复时视图非 nil，`Names()` 包含批次里的名字，`Digest()` 为空，工具执行。已有的 [agent/phase1_interrupt_test.go:731](../../agent/phase1_interrupt_test.go#L731) 必须继续通过。
- [x] 写 `TestWorkflowResumeSkipsPreflight`：没有 `a.loop` 的 workflow agent 恢复一个带待执行批次的检查点，仍然走原来的“报告未执行”分支，不因为预检报错。

实现要求：

- [x] `agent/hitl.go` 的 `Agent.Resume` 在 `validatePause` 之后、`durable.Claim` 之前调用 `preflightResumeTools(cp)`。
- [x] `preflightResumeTools` 按 D8 的顺序检查：不是 LLM agent、没有待执行批次（`cp.Pending == nil || len(cp.Pending.Pending) == 0`）直接返回；有快照先查广告名单；再查当前静态工具表；旧快照跳过广告名单。错误用 `%w` 包装，带线程、调用 ID 和工具名；`ErrToolUnavailable` 的文本同时提示运行期注入工具不会跨恢复还原。
- [x] 暂停批次的 `rb.snapshot` 已由 SV-02 加好；本卡只消费它，不再改 `Agent.Resume` 的批次构造顺序。
- [x] `agent/stepview.go` 的 `resumeStepView` 改为优先读 `rb.snapshot`：有快照时广告名字和 `Digest()` 都取快照（`Advertised`、两个摘要、`Model`）；没有快照时保持 SV-01 的兜底（批次名字，`Digest()` 为空）。SV-01 已经把视图挂到恢复批次的 `LoopContext` 上，本卡不再改 `agent/loop.go`。
- [x] 恢复批次的检查点、再次暂停和失败都使用 `rb.snapshot`；不要用 `rlc.viewSnapshot()` 给旧检查点凭空造一个摘要为空的快照。
- [x] 文档说明：运行期注入工具不能跨恢复；待审批批次的恢复只核对名字和当前可解析性，不核对 schema 是否变化；schema 变化仍会在执行时被参数校验发现。

验证：

```bash
go test ./agent -run 'TestResume|TestLegacyPending|TestWorkflowResume' -count=1
go test ./agent -count=1
go test -race ./agent -run 'TestResume' -count=1
```

**完成标准：**暂停批次里的工具不在广告里或已经不存在时，恢复在取得 claim 之前失败；批准批次在暂停视图下执行，恢复批次写出的检查点保留原快照；旧检查点和 workflow agent 的恢复行为不变。

**提交：**`feat(agent): validate resume against the paused step view`

### TASK-SV-04：重放恢复的漂移诊断

**依赖：**SV-03。**决策：**D9、D10、D13。

**文件：**新增 `agent/stepview_drift_test.go`；修改 `agent/options.go`、`agent/stepview.go`、`agent/loop.go`、`agent/runtime.go`、`agent/hitl.go`、`core/event.go`。

- [x] 写 `TestReplayDriftWarns`：线程先用工具 v1 跑完；用同一个 store、同一个线程、同名但 description 为 v2 的新 agent 调用 `Resume`；断言收到 `core.StepViewDrifted`，工具摘要不同，运行成功。
- [x] 写 `TestReplayDriftStrictFailsBeforeModel`：同样的场景加 `WithStepSnapshotPolicy(StepSnapshotStrict)`；`Wait` 返回 `ErrStepViewDrift`，模型调用次数为 0；失败检查点带当前 v2 视图的快照，后续恢复以新视图为基线。
- [x] 写 `TestReplayDriftOffIsSilent`：`StepSnapshotOff` 下没有事件，运行成功。
- [x] 写 `TestReplayDriftTracksSettings`：工具不变，只改模型温度或系统提示；收到事件，设置摘要不同。
- [x] 写 `TestReplayDriftOnlyOnce`：恢复后的第一步调用一次工具，第二步返回文本；漂移事件只有一条，第二步不再比较。
- [x] 写 `TestReplayDriftLegacySnapshotSkipped`：预置一个没有 `Snapshot` 的检查点，恢复时没有事件。
- [x] 写 `TestReplayDriftEmptyBaselineDigestSkipped`：预置快照但摘要为空，不误报。
- [x] 写 `TestUnknownStepSnapshotPolicyRejected`：`New` 收到未知策略值返回错误。
- [x] 写 `TestReplayBaselineNotInheritedBySubRun`：恢复运行里派生的子运行不会拿父运行的基线做比较。可以用一个现有的委派场景或直接检查 `deeper`/`subRun` 构造出的 `RunContext` 不带基线。

实现要求：

- [x] `config` 增加 `stepSnapshotPolicy StepSnapshotPolicy`；`WithStepSnapshotPolicy` 设置它；`Agent.New` 对未知值返回错误；`newLoop` 把它带到 `AgentLoop.stepSnapshotPolicy`。
- [x] `RunContext` 增加 `replayBaseline *checkpoint.StepSnapshot`。`deeper`、`subRun`、`forBranch` 不复制它。
- [x] `Agent.Resume` 只在没有待执行批次（`cp.Pending == nil || len(cp.Pending.Pending) == 0`）且策略不是 Off 且快照非 nil 时设置基线；待审批批次的恢复不设置。
- [x] `freezeStepView` 成功后调用 `checkReplayDrift`：取走基线并清空，比较两个摘要；基线摘要为空时跳过对应比较；Warn 发布事件，Strict 返回 `ErrStepViewDrift`，Off 不做事。
- [x] Strict 失败时把当前视图的快照传给 `fail`，让失败检查点成为下一次恢复的新基线。
- [x] `core/event.go` 增加 `StepViewDrifted` 和 `isEvent()` 标记；字段按 §4.3。
- [x] 事件只发布一次，位置在冻结之后、模型调用之前。

验证：

```bash
go test ./agent -run 'TestReplayDrift|TestUnknownStepSnapshotPolicy|TestReplayBaseline' -count=1
go test ./agent -count=1
go test -race ./agent -run 'TestReplayDrift' -count=1
```

**完成标准：**默认 Warn 只诊断不阻断；Strict 在模型调用前失败并保留新基线；Off 完全安静；基线只比较一次；旧快照和空摘要不误报；子运行不继承父运行基线。

**提交：**`feat(agent): report step view drift on replay resume`

### TASK-SV-05：示例、文档与全量验收

**依赖：**SV-04。

**文件：**新增 `examples/step-view/main.go`；修改 `README.md`、`docs/tasks/plan-step-view.md`（只填 §10 执行记录）。设计草稿本身不改正文；如需标注状态，由用户另行决定。

- [x] 写离线示例：mock 模型；静态工具 `lookup` 返回 `static`；middleware 在 `ModifyRequest` 里用同名工具替换，返回 `injected`；模型调用 `lookup`。在 `AfterModel` 或 `AfterTool` 里打印步骤号、模型名、广告名字、两个摘要的前几位和最终结果。
- [x] 示例要展示两件事：同名注入后模型看到的是注入 schema；`lc.View()` 能读到本步模型实际看到的工具和设置。
- [x] 运行 `go run ./examples/step-view`，把真实输出贴进 §10。不要照格式编造输出。
- [x] 在 [README.md](../../README.md) 的“中间件、HITL 与外部工具”表里加一行：

```markdown
| `step-view` | `go run ./examples/step-view` | 每步冻结执行视图：同名注入同时替换 schema 与实现，AfterModel 读取本步模型实际看到的工具、设置与摘要。 |
```

- [x] 跑全量门禁：

```bash
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./agent ./checkpoint -count=1
git diff --check
```

- [x] 对新文件和本次修改过的 Go 文件跑 `gofmt -l`。Windows 上既有文件可能是 CRLF，会被 `gofmt -l` 标出来；只处理本次新增或改动的文件，不要整仓重排。新文件用 LF。
- [x] 检查 `git status --short`，确认提交只包含本卡的示例、README 和必要的格式修正；不要使用 `git add .`。
- [x] 填 §10：每张卡的实际提交、实际运行的命令、结果摘要、出现的偏差。测试失败过又修好的，也记一行。
- [x] 提交 `docs(examples): demonstrate frozen step views`。本文 §10 的更新默认不随代码提交，除非用户明确要求把计划文档一起提交。

**完成标准：**示例可重复运行并输出稳定；README 能索引到示例；全量构建、静态检查、测试和 race 门禁通过；§10 记录的是实际结果，没有猜测。

## 7. 验收矩阵

| 编号 | 场景 | 断言 | 覆盖任务 |
| --- | --- | --- | --- |
| A1 | 静态工具、注入工具、同名替换 | 广告名字集合、schema 内容、执行实现三者一致 | SV-01 |
| A2 | 同名替换 | 模型收到注入工具的 description 和 parameters | SV-01 |
| A3 | `ModifyRequest` 注入 | 当前步能解析并执行，不会被当成没有实现 | SV-01 |
| A4 | `BeforeModel` 注入 | 当前步请求包含并可执行 | SV-01 |
| A5 | `AfterModel`/`AfterTool` 注入 | 当前步不生效，下一步生效 | SV-01 |
| A6 | 广播没有实现的工具 | 模型调用前返回 `ErrToolViewInvalid` | SV-01 |
| A7 | 只改 schema 不改实现 | 模型调用前返回 `ErrToolViewInvalid` | SV-01 |
| A8 | 请求里重复工具名 | 模型调用前返回 `ErrToolViewInvalid` | SV-01 |
| A9 | 已注册但未广播的工具被调用 | `RejectNotAdvertised`，不执行 | SV-01 |
| A10 | 从未注册的工具被调用 | 仍是 `RejectUnknownTool` | SV-01 |
| A11 | 注入的串行工具 | 整批降级为串行；最终实现不串行时不降级 | SV-01 |
| A12 | 视图生命周期 | `BeforeModel`/`ModifyRequest` 为 nil，之后非 nil | SV-01 |
| A13 | 视图副本 | 修改返回值不影响视图 | SV-01 |
| A14 | 摘要稳定 | 相同请求顺序和内容摘要相同；顺序、description、parameters、设置变化会改摘要；Metadata 不影响设置摘要 | SV-01 |
| B1 | 快照存取 | Memory/File 读回字段一致 | SV-02 |
| B2 | 快照克隆 | 修改读回的副本不影响存储 | SV-02 |
| B3 | 快照进入提交摘要 | 同内容重试成功，改快照同 ID 冲突 | SV-02 |
| B4 | 旧记录摘要 | 没有快照的旧检查点重试仍然被识别 | SV-02 |
| B5 | 暂停检查点 | 带快照，广告名单和摘要正确 | SV-02 |
| B6 | 普通/失败检查点 | 冻结后有视图的检查点带快照；冻结前不带 | SV-02 |
| C1 | 暂停批次工具不在广告里 | `ErrToolNotAdvertised`，不取得 claim | SV-03 |
| C2 | 暂停批次工具已移除 | `ErrToolUnavailable`，不取得 claim | SV-03 |
| C3 | 暂停批次使用动态工具 | `ErrToolUnavailable`，错误说明运行期工具不能跨恢复 | SV-03 |
| C4 | 旧暂停检查点 | 可恢复，视图按批次名字构造，摘要为空 | SV-03 |
| C5 | 恢复批次执行 | 使用暂停视图；`Request` 仍为 nil；检查点保留原快照 | SV-03 |
| C6 | 恢复后再次暂停 | 保留原快照，之后仍可恢复 | SV-03 |
| C7 | workflow agent | 跳过预检，保持原分支 | SV-03 |
| D1 | 重放漂移，默认策略 | 发布一次 `StepViewDrifted`，运行继续 | SV-04 |
| D2 | 重放漂移，严格策略 | 模型调用前返回 `ErrStepViewDrift`，新基线保留 | SV-04 |
| D3 | 重放漂移，关闭策略 | 无事件、无比较 | SV-04 |
| D4 | 设置变化 | 设置摘要不同会触发诊断 | SV-04 |
| D5 | 旧快照/空摘要 | 不误报 | SV-04 |
| D6 | 未知策略值 | `New` 返回错误 | SV-04 |
| E1 | 全量回归 | `go build ./...`、`go vet ./...`、`go test ./... -count=1` 通过 | SV-05 |
| E2 | 并发回归 | `go test -race ./agent ./checkpoint -count=1` 通过 | SV-05 |
| E3 | 示例 | `go run ./examples/step-view` 可重复运行，输出与记录一致 | SV-05 |

验收不变量：

1. 对任意一步，执行的工具一定在该步广告里。
2. 对普通步骤，广告里的每个名字都能解析到实现。旧检查点的恢复视图在 SV-01 阶段是例外（广告了但已删除的工具执行时按未知工具拒绝），SV-03 之后由恢复预检保证。
3. 同名注入时，广告、参数校验、执行实现和串行判断使用同一个实现。
4. 恢复待审批批次时，若有暂停快照，批次里的每个名字都在 `Advertised` 里；无论有没有快照，每个名字现在都能解析到实现，否则恢复在取得 claim 前失败。
5. 旧检查点没有 `Snapshot` 时，恢复行为与本文之前一致。

## 8. 风险、兼容性与注意事项

### 8.1 行为收紧

- 只改 schema 不改实现的中间件会在冻结时失败。这是设计草稿明确要求的收紧。实施前用 `grep` 检查仓库内所有 `ModifyRequest` 实现；当前仓库里只有 `AddTool` 会改工具列表，`runbudget` 会清空列表，没有只改单个 schema 的实现。
- 广播没有实现的工具会在模型调用前失败，不再给模型一次“unknown tool”的机会。错误发生在 `Run.Wait`，事件流里会先有 `RunStarted`，然后 `RunFailed`。
- 已注册但未广播的工具不再执行。依赖旧行为“模型 hallucinate 一个已注册名字也能跑”的调用方会看到 `not_advertised`。仓库内没有这样的测试；如果外部使用者依赖，需要在变更说明里提示。
- `AddTool` 在冻结后只影响下一步。之前依赖它在 `AfterTool` 里影响同批剩余调用的代码需要调整。并发执行时旧行为本来就不确定，新行为更可预测。

### 8.2 恢复限制

- 运行期注入工具不能跨恢复。暂停批次里如果是注入工具，恢复会返回 `ErrToolUnavailable`。这是已知限制，不在这批解决。
- 待审批批次的恢复只核对“名字在暂停广告里”和“现在能解析到实现”，不核对 schema 是否变化。schema 变化仍会在执行时由参数校验发现。如果以后需要“人工批准的就是同一版实现”，再增加按工具保存的摘要；本计划不扩大快照字段。
- 预检覆盖暂停批次里的所有调用，包括本次会被拒绝的调用。这是设计草稿 §4.6 的要求：批次已经过人工处理，工具表变化时宁可让恢复失败，也不在不确定的实现上继续。
- 旧检查点没有 `Pause` 时，`validatePause` 仍然放行；预检只做可解析性检查。恢复批次的视图摘要为空，写出的新检查点也没有快照，直到下一次有模型请求的步骤。

### 8.3 摘要和性能

- 工具摘要使用原始字节，工具的 `Description()`/`Schema()` 必须在运行内稳定。返回随机内容或包含时间的工具会让冻结失败或每次重放都报漂移。
- 设置摘要包含系统提示。系统提示里如果有时间、随机数或每次都变的环境信息，默认 Warn 会多一条事件；需要完全重放的场景才用 Strict。
- 唯一静态工具继续使用构造期缓存的 schema；冻结校验会再调用一次实现的 `Schema()` 做一致性检查。工具数量很大时，这一步有额外开销。第一版不做缓存优化；要优化时必须同时缓存实现版本，不能只缓存字节。
- 检查点只增加工具名和两个摘要，通常几百字节。File 存储的 journal 会随每个检查点多一点内容；没有完整 schema，增长可控。

### 8.4 兼容性

- `Checkpoint.Snapshot` 是 JSON 增量字段。旧版本的读者忽略未知字段；新版本读旧记录得到 nil。提交摘要只在快照非 nil 时包含该字段，旧记录重试的摘要不变。
- `RejectNotAdvertised` 追加在常量末尾，已有常量的数值不变。`ArgGuard` 默认只统计三个参数侧分类，不统计 `not_advertised`；需要统计的调用方显式加入。
- `LoopContext.Request` 保留。`argguard` 的 `requiredArgs`、`runbudget` 的 `BeforeTool` 仍然读它；恢复批次仍然是 nil，现有判空逻辑不变。
- 静态重复工具从“两个广告槽位”变成一个槽位。仓库内没有依赖两个槽位的测试；这是修正，但要在提交说明里写清楚。
- `AgentLoop.seqTool` 字段和 `isSequential` 删除后，串行判断全部走视图。`WithToolExecution(ToolSequential)` 的全局强制串行不变。

### 8.5 测试注意事项

- 并发用例用现有的 `overlap` 或显式 barrier，不要靠加长 sleep 来“稳定”。串行降级用最大并发数判断，不用执行顺序判断。
- 恢复基线相关的测试要共享同一个 checkpoint store，并确保线程 ID 不冲突。漂移测试要跑完第一轮再创建第二个 agent。
- 旧检查点测试要显式构造没有 `Snapshot` 的检查点，不要依赖某个新运行“碰巧”没有快照。
- 如果已有测试对检查点的完整 JSON 做字面比较，新增的 `step_snapshot` 会改变输出。优先改成只断言关心的字段，不要为了让旧期望通过而关掉快照。
- Windows 上既有文件可能是 CRLF。`gofmt -l` 只处理本次新增/修改的 Go 文件；不要整仓 `gofmt -w`。
- 所有测试命令加 `-count=1`，避免缓存掩盖问题。门禁输出要保留完整日志，不要用 `| tail` 截断后再判断。

## 9. 明确不做的事

- P0-2 的外层任务循环、输入票据、终稿之后的输入处理、`TurnDone` 语义调整。
- `AgentLoop.run` 的阶段拆分；P0-1 仍然使用现有的单层步骤循环。
- 按调用粒度的并发限制、工具暴露级别、可发现工具、`load_tool` 元工具。
- 在途检查点（工具执行前额外写一次检查点）和工具执行记录的恢复重放。
- 把完整 schema、完整消息或运行期注入工具写进检查点。
- 为检查点新增数据库存储或改变 File/Memory 的存储格式版本。
- 改变模型 provider 的请求格式或 `llm.Request` 的字段。
- 改变 `Agent.Run`/`Stream`/`Resume` 的签名和行为，除了本文列出的错误和事件。

## 10. 执行记录

执行时逐项填写。只写实际运行的命令和实际结果，不写猜测输出。

| 任务 | 提交 | 运行命令 | 结果摘要 | 偏差/备注 |
| --- | --- | --- | --- | --- |
| SV-01 | `b1bae62` | `go test ./agent -run 'Test(StepView\|InjectedTool\|AddTool\|NotAdvertised\|UnknownTool\|SnapshotTools\|ToolDigest\|SettingsDigest)' -count=1`；`go test ./agent -count=1`；`go test ./middleware -count=1`；`go test -race ./agent -run 'Test(StepView\|InjectedTool\|Sequential)' -count=1`；`go vet ./agent ./core` | 全部通过；agent 全量 13.636s、middleware 0.432s、race 1.436s；vet 无输出 | 无 |
| SV-02 | `9ac1339` | `go test ./checkpoint -run 'Test(StepSnapshot\|LegacyCheckpoint)' -count=1`；`go test ./agent -run 'Test(PauseCheckpoint\|NormalStepCheckpoint\|FailCheckpoint\|StepSnapshotNoStore)' -count=1`；`go test ./agent ./checkpoint -count=1`；`go test -race ./checkpoint -count=1` | 目标用例全部 PASS；checkpoint 0.174s、agent 焦点 0.176s；全量 agent 12.591s + checkpoint 3.013s；race 13.223s | 无 |
| SV-03 | `124e6e6` | `go test ./agent -run 'TestResume\|TestLegacyPending\|TestWorkflowResume' -count=1`；`go test ./agent -count=1`；`go test -race ./agent -run 'TestResume' -count=1` | 目标 0.214s；全量 agent 13.058s；race 1.297s；build/vet 通过 | 恢复视图先复制 `ToolView` 再填名字，`Names()` 为空导致批次被拒；改为填完再挂载后修复。SV-01 的 `TestResumeBatchHasFallbackView` 改为显式剥离快照，保留旧检查点回退语义 |
| SV-04 | `5c7daf5` | `go test ./agent -run 'TestReplayDrift\|TestUnknownStepSnapshotPolicy\|TestReplayBaseline' -count=1`；`go test ./agent -count=1`；`go test -race ./agent -run 'TestReplayDrift' -count=1`；`go test ./core -count=1` | 9 个新用例 PASS；目标 0.198s、全量 12.663s、race 1.248s、core 0.109s；build/vet 通过。反向检查两条（不清空基线、不跳过空摘要）均按预期失败后还原 | `core/eventjson.go` 与 `core/stuckevent_test.go` 不在卡片清单：新增 sealed 事件需能跨进程编码，按既有事件提交惯例补齐并加往返用例 |
| SV-05 | `4b98e33` | `go run ./examples/step-view`（两次）；`go build ./...`；`go vet ./...`；`go test ./... -count=1`；`go test -race ./agent ./checkpoint -count=1`；`git diff --check`；`gofmt -l examples/step-view/main.go` | 两次输出完全一致；build/vet/测试/race/diff-check 全部通过（全量 agent 13.925s、checkpoint 5.771s、queue 6.008s；race agent 19.170s、checkpoint 14.071s）；gofmt 无输出 | §10 在代码提交后填写，以便记录实际提交号；示例与 README 已提交，本文未入库 |

示例输出（实际运行后粘贴）：

```text
静态 lookup：在静态目录里查一个键
注入 lookup：在注入目录里查一个键（同名运行期实现）

[步骤 0] 视图：模型 "demo-model"，广告工具 [lookup]
  广告 schema：lookup — 在注入目录里查一个键（同名运行期实现）
                参数 {"properties":{"key":{"type":"string"},"source":{"type":"string"}},"required":["key"],"type":"object"}
  设置：system="你是一个查询助手。" temperature=0.2
  摘要：tool=3aa3b735f711 settings=142404c06fb4
  本步模型发起的工具调用：1 个
[步骤 0] 工具 lookup 的实际返回：injected:alpha

[步骤 1] 视图：模型 "demo-model"，广告工具 [lookup]
  广告 schema：lookup — 在注入目录里查一个键（同名运行期实现）
                参数 {"properties":{"key":{"type":"string"},"source":{"type":"string"}},"required":["key"],"type":"object"}
  设置：system="你是一个查询助手。" temperature=0.2
  摘要：tool=3aa3b735f711 settings=142404c06fb4
  本步模型发起的工具调用：0 个

最终答案: 收尾：injected:alpha
```
