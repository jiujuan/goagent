# ADR 0023：v2 架构——事件总线 + AgentSpec/Runtime/Loop + Checkpoint 可恢复编排

状态：提议中（supersedes 部分 [ADR 0001](0001-event-stream-primitive.md) /
[ADR 0004](0004-side-effects-as-actions.md)，并扩展 [ADR 0011](0011-middleware-decorator.md) /
[ADR 0013](0013-middleware-hitl.md)）

> 本文是 v2 的总纲性 ADR。它替换 v1 的两条根基决策——「`iter.Seq2` 作唯一流原语」
> 与「副作用即 `Event.Actions`、由 Runner 事务提交」——并融合 pi 的事件驱动 +
> 有状态运行句柄、LangGraph 的 checkpoint 可恢复/分支/HITL、deepagents 的子智能体
> 隔离与虚拟文件系统、LangChain v1 的循环中间件。v1 的 workflow agent 予以保留。

---

## 背景

### v1 现状

v1 把一切建模成 `core.Stream = iter.Seq2[*core.Event, error]`（[ADR 0001](0001-event-stream-primitive.md)），
并把所有副作用（state delta、transfer、escalate、stop）挂在 `Event.Actions` 上，由
Runner 在提交非 partial 事件时事务性应用（[ADR 0004](0004-side-effects-as-actions.md)）。
这套设计**适合做实验**：单一原语、可重放、可审计。

### 难以理解的根因

实践中真正抬高认知门槛的，不是 `iter.Seq2` 本身，而是**这一条事件流同时承担了两个语义**：

1. **观测流**：给 UI / 日志实时渲染（partial 增量）。
2. **提交日志**：Runner 据 `Event.Actions` 事务性地改 session 状态。

一根管子两个职责，叠加 `Event` 是一个「宽 struct + 一堆可空字段」（Message/Actions/
Partial/ParentID/SummarizesTo/Progress/Usage/Err 八个关注点混在一起），类型上无法表达
「partial 事件必无 Actions」「summary 节点必无 Message」这类不变量，全靠约定——这才是
认知负担的来源。

### 一个必须诚实交代的判断

把 `iter.Seq2`（pull）换成事件总线（push）**不会自动更简单**：pull 模型免费提供了背压与
取消，push/bus 要自己管缓冲、丢弃策略、goroutine 生命周期与顺序。**这是用一种复杂度换
另一种复杂度，不是净减少。** v2 真正的可读性收益来自三刀切分，而非 push 取代 pull：

- **观测 vs 持久化分家**：Bus 只观测，Checkpointer 只持久化与恢复。
- **控制流显式化**：用返回值 `Directive` 表达控制信号，不再塞进 `Event.Actions` 在 commit 时合并。
- **数据 vs 引擎分家**：`AgentSpec`（描述）与 `Runtime/Loop`（执行）分开，取代 v1 `Agent` 接口的双重职责。

为了不丢失 pull 的简单性，v2 在 Bus 之上保留一个 `iter.Seq2` 适配器（`Run.Iter()`），
单消费者场景仍可 `for ev := range`。

---

## 决策

### 分层总览

```
AgentRuntime ── 引擎:Compile(Spec)→Agent;拥有 Bus / Checkpointer / Store / Scheduler
  ├ Bus            ── pub/sub 事件总线(channel 实现):纯观测,多订阅者,per-subscriber 缓冲
  ├ Checkpointer   ── 状态快照树:可恢复 / 分支 / time-travel / HITL 暂停
  └ Agent(已编译) ── 一个可启动的句柄
      └ Run        ── 一次运行的有状态句柄:Events 通道 / Steer / Approve / Wait / Cancel
          └ AgentLoop ── 可控循环(显式相位机):中间件包裹每个相位
              ├ Middleware ── BeforeModel/ModifyRequest/AfterModel/BeforeTool/AfterTool/OnError
              ├ AgentSpec  ── 声明式数据:Model/Tools/SubAgents/Prompt/LoopPolicy
              └ Directive  ── 显式控制信号(取代 Actions):Continue/Stop/Transfer/Escalate/Interrupt
横切保留:workflow agents(Sequential/Parallel/Loop/Pipeline)在 Runtime 层编排 Spec
```

核心心智：**Spec 是蓝图 → Runtime 把蓝图 compile 成 Agent → Agent.Start 产出 Run →
Run 驱动一个可控 AgentLoop → loop 把事件 publish 到 Bus、把状态 snapshot 到
Checkpointer。** 这是 LangGraph `compile()→Pregel` 与 pi 有状态 Agent 句柄的融合。

---

### 1. AgentSpec —— 声明式数据（取代 Agent 接口的配置职责）

```go
// AgentSpec 只描述"一个 agent 是什么",不含任何执行逻辑。可序列化、可嵌套、可复用。
type AgentSpec struct {
    Name        string
    Description string
    Model       llm.Model
    Instruction string          // 或 Prompt *prompt.Builder
    Tools       []tool.Tool
    SubAgents   []AgentSpec      // 嵌套即委派拓扑(deepagents 风格)
    Middleware  []Middleware
    Loop        LoopPolicy       // 可控循环的策略
}

type LoopPolicy struct {
    MaxSteps      int             // 默认 16
    ToolExecution ToolExecMode    // Sequential | Parallel(默认)
    StopWhen      func(*LoopContext) bool   // 自定义停止条件
}
```

**为什么是数据而非接口**：v1 的 `Agent.Run(ictx) Stream` 把「我是谁」和「我怎么跑」
绑死，每种 agent 都要重写 Run。v2 里 LLM 循环逻辑只有一份（在 AgentLoop），Spec 纯数据
→ 易测、易序列化、易由 workflow 组合。对应 LangGraph 的 graph-as-blueprint 与
deepagents 的 `SubAgent` 声明式 spec。

---

### 2. AgentRuntime —— 引擎与编译

```go
type Runtime struct {
    store      session.Store
    checkpoint Checkpointer
    bus        *Bus
    scheduler  *Scheduler   // 背景任务/队列(承接 v1 的 queue)
}

// Compile 把 Spec(可含 workflow 组合)固化成可启动的 Agent。
// 类比 LangGraph 的 StateGraph.compile() → Pregel。
func (r *Runtime) Compile(spec AgentSpec) *Agent

// Start 非阻塞:在自己的 goroutine 里驱动 loop,立刻返回 Run 句柄。
func (a *Agent) Start(ctx context.Context, req RunRequest) (*Run, error)

// Resume 从某 thread 的最新(或指定)checkpoint 续跑,可注入 HITL 审批。
func (a *Agent) Resume(ctx context.Context, threadID string, in ResumeInput) (*Run, error)
```

`RunRequest` 携带 `(app, user, threadID, userMsg)`。Runtime 是**唯一持有持久化与总线的
层**——延续 v1「Agent 决策、Runner 持久化」的分工，但升级成显式引擎。

---

### 3. Event + Bus —— pub/sub 观测流（取代 iter.Seq2 的观测职责）

事件改成 **sealed union（pi 的 discriminated union 思路）**，字段精确，告别 v1 宽 struct：

```go
type Event interface{ isEvent() }

type RunStarted   struct{ RunID, ThreadID string }
type TurnStarted  struct{ Step int }
type MessageDelta struct{ Delta core.Message }            // 流式增量(只投递,不落库)
type MessageDone  struct{ Message core.Message; Usage *core.Usage }
type ToolStarted  struct{ Call core.ToolCall }
type ToolUpdate   struct{ CallID string; Partial core.Part }
type ToolDone     struct{ Result core.ToolResult }
type TurnDone     struct{ Step int }
type Interrupted  struct{ Pending []ApprovalRequest }     // HITL 暂停
type Progress     struct{ Job ProgressInfo }              // 长任务进度
type RunDone      struct{ Result Result }
type RunFailed    struct{ Err error }
```

Bus 用 channel 实现，**每个订阅者一条独立缓冲通道 + 投递策略**：

```go
type Bus struct{ ... }

type DeliveryMode int
const ( Lossy DeliveryMode = iota; Lossless )  // UI 可丢 partial;tracing 不可丢

func (b *Bus) Subscribe(topic Topic, mode DeliveryMode) (<-chan Event, context.CancelFunc)
func (b *Bus) Publish(topic Topic, ev Event)   // 由 loop 单 goroutine 顺序调用 → 保证有序
```

**关键设计：loop 存活不依赖订阅者快慢。** 投递 per-subscriber 异步（慢的 UI 丢 partial，
不阻塞 loop）；唯一同步点是 `Run.Wait()` 和 checkpointer 落盘——而 checkpoint 由 loop
**内联**完成，不走 bus。于是**持久性不依赖订阅者速度**，根治 v1 把 commit 与观测耦在一条
流上的问题。这是 pi「流是观测性的、不 await 异步 handler」的 Go 化。

**给简单消费者的和解方案**——Bus 之上的 `iter.Seq2` 适配器，保留 v1 体验：

```go
func (run *Run) Iter() iter.Seq2[Event, error]  // 内部 Subscribe 一条 Lossless 通道转 pull
```

**内部 push/bus（多订阅、可观测），对外仍可 pull（简单场景零负担）**——两全。

---

### 4. Directive —— 显式控制流（取代 Actions-as-data）

控制信号不再写进事件、由 Runner 在 commit 时合并，而是作为**相位函数的返回值**显式传递：

```go
type DirectiveKind int
const (
    Continue DirectiveKind = iota
    Stop
    Transfer
    Escalate
    Interrupt
)

type Directive struct {
    Kind   DirectiveKind
    Target string          // Transfer 目标
    Reason string
}

// 工具通过 Result 显式表达控制意图(而非 v1 的 ctx.Actions sink):
type Result struct {
    Content []core.Part
    IsError bool
    Control *Directive     // 工具想 escalate/stop/transfer 就填这里
    State   []StateOp      // 工具想改状态就返回 op(由 loop 显式 apply,立即可见)
}
```

**优先级明确**（修掉 v1 `mergeActions` 的 last-write-wins 模糊）：多个来源的 Directive 按
`Interrupt > Stop > Escalate > Transfer > Continue` 取最高优先级，同级按中间件顺序首个
非 Continue 胜出。读代码即知谁赢。

状态变更走 `StateOp` 由 loop 显式 apply 到 `State`，**立即可见**（命令式、易懂），不再是
「声明式延迟到 commit」。代价是丢了事件溯源的可重放性——但可恢复性改由 **checkpointer**
提供（见下），这是更直观的等价物。

---

### 5. State + Checkpointer —— 可恢复 / 分支 / time-travel / HITL

durability 不再靠「重放 Actions」，改靠 **LangGraph 式快照**：

```go
type State struct {
    Messages []core.Message
    Todos    []Todo            // deepagents 规划
    Files    Backend           // deepagents 虚拟文件系统(可插拔后端)
    KV       map[string]any
}

type Checkpoint struct {
    ID, ThreadID string
    ParentID     string        // ← 指向父快照 → 快照树 = 分支/fork
    Step         int
    State        State         // 可序列化
    Pending      *PendingHITL  // 若因 HITL 暂停,存待批工具
}

type Checkpointer interface {
    Save(ctx context.Context, cp *Checkpoint) error
    Load(ctx context.Context, threadID, checkpointID string) (*Checkpoint, error)
    Latest(ctx context.Context, threadID string) (*Checkpoint, error)
    History(ctx context.Context, threadID string) ([]*Checkpoint, error)  // time-travel
}
```

三种能力一次拿到：

- **可恢复**：进程重启 → `Latest(thread)` → loop 从该 step 续跑。
- **分支/fork**：`Load(旧 checkpoint)` → 以它为 ParentID 开新 thread → 同一历史点跑不同路径（LangGraph time-travel）。
- **HITL**：loop 在危险工具前命中 gate → 存 `Pending` 快照 + 发 `Interrupted` 事件 + return → 用户 `run.Approve(...)` → 从该快照 resume，把审批结果注入。

**为何比 v1 易懂**：checkpoint 是「整盘状态快照」，比「重放一串 StateDelta 重建状态」直观；
快照树（`Checkpoint.ParentID`）也比 v1 的 event 级 `ParentID/SummarizesTo` 粗粒度、好理解。

---

### 6. AgentLoop —— 可控循环（显式相位机）

把 v1 藏在 `LLMAgent.Run` 里的 `for range maxSteps` 翻出来，做成**显式、可被中间件拦截、
可暂停的相位机**：

```go
func (l *AgentLoop) Run(rc *RunContext) {
  for step := 0; step < l.policy.MaxSteps; step++ {
    rc.bus.Publish(TurnStarted{step})

    // 相位 1 PrepareTurn:排空 steering 队列 + BeforeModel 中间件
    if d := l.mw.BeforeModel(rc); d.Kind != Continue { l.apply(rc, d); if l.terminal(d) { return } }

    // 相位 2 CallModel:ModifyRequest → 流式 token 进 bus → AfterModel
    l.mw.ModifyRequest(rc, rc.Req)
    final := l.streamModel(rc)                 // 每个 delta → bus.Publish(MessageDelta)
    if d := l.mw.AfterModel(rc, final); d.Kind != Continue { l.apply(rc, d); if l.terminal(d) { return } }

    calls := final.ToolCalls()
    if len(calls) == 0 { l.checkpoint(rc, step); return }   // 无工具 → 收尾

    // 相位 3 ExecuteTools:BeforeTool 门禁(HITL/权限)→ 并发跑 → AfterTool
    for _, c := range calls {
      if d := l.mw.BeforeTool(rc, &c); d.Kind == Interrupt {
        l.checkpointPending(rc, step, c)        // 存 Pending + 发 Interrupted + return
        return
      }
    }
    results, dirs := l.execTools(rc, calls)     // 按 LoopPolicy 串/并发,AfterTool 收集 Directive

    // 相位 4 Checkpoint(每步快照 → 可恢复/可分支)
    l.checkpoint(rc, step)

    // 相位 5 ApplyDirectives:Continue/Stop/Transfer/Escalate 显式分派
    switch resolve(dirs).Kind {
    case Transfer:        l.transferTo(rc, target); return   // 委派(承接 v1 transfer 方向规则)
    case Escalate, Stop:  return
    }
  }
}
```

「**可控**」具体指六条，每条对应代码里一个明确点：

| 控制维度 | 实现点 |
|---|---|
| 有界 | `MaxSteps` + 递归上限 |
| 可暂停/恢复 | 每步 `checkpoint` |
| 可中断(HITL) | `BeforeTool` 返回 `Interrupt` → `checkpointPending` |
| 可操控(steering) | PrepareTurn 排空 steering 队列 |
| 可观测 | 每相位 `bus.Publish` |
| 策略化 | `LoopPolicy`(停止条件/工具并发模式) |

---

### 7. Middleware —— 包裹循环而非装饰 Model（修掉 v1 的能力边界）

v1 中间件 `func(llm.Model) llm.Model`（[ADR 0011](0011-middleware-decorator.md)）只能看到
模型调用，看不到工具执行/transfer。v2 中间件**钩在 loop 的每个相位**（LangChain v1
`create_agent` middleware 的 Go 化）：

```go
type Middleware interface {
    BeforeModel(*LoopContext) (Directive, error)
    ModifyRequest(*LoopContext, *llm.Request) error              // 只改这一次请求(compaction/RAG)
    AfterModel(*LoopContext, *llm.Response) (Directive, error)
    BeforeTool(*LoopContext, *core.ToolCall) (Directive, error)  // HITL/权限门禁
    AfterTool(*LoopContext, *core.ToolResult) (Directive, error)
    OnError(*LoopContext, error) (Directive, error)              // retry
}
// 嵌入 BaseMiddleware 取 no-op 默认,只覆盖关心的钩子。
```

v1 的 compaction/RAG/retry/ratelimit/steering 全部平移为 middleware，**且现在能看到工具
层**。HITL（[ADR 0013](0013-middleware-hitl.md)）、summarization、deepagents 的
permission/skills 也都是 middleware——这正是 LangChain v1「middleware 统一 supervisor/
swarm/deepagents/reflection」的思路。组合顺序：入站正序、出站逆序（洋葱），确定可预测。

---

### 8. Workflow agents —— 保留，在 Runtime 层重新表达

Sequential/Parallel/Loop/Pipeline 不再实现 `Agent` 接口的 Run，而是**编排 AgentSpec 的
Runtime 级组合器**，每个子运行拿一个 checkpoint 子树（= 分支）：

```go
func Sequential(name string, subs ...AgentSpec) AgentSpec   // 仍是数据
// Runtime 编译时识别组合类型,生成对应 driver:
//   Sequential → 顺序跑子 Run,后者读前者写入 State.KV(承接 OutputKey)
//   Parallel   → 每个子 Run 独立 branch(隔离 State 副本)+ goroutine,join 时 merge,事件经 Bus 合流
//   Loop       → 重复跑直到子 Run 返回 Escalate Directive(承接 v1 Escalate 语义)
//   Pipeline   → builder 串上面三者
```

**关键简化**：Parallel 用**分支隔离 + join 合并**，而非 LangGraph 的 channel/reducer 共享
状态——避免引入 reducer 心智。并发写冲突由「各跑各的 State 副本、合并时显式 merge 函数」
解决，比 reducer 直观。这是有意识地只取 LangGraph 的分支/可恢复，不取它最重的
channel/reducer 部分。

---

### 9. Subagents —— deepagents 式上下文隔离

委派（LLM 驱动的 `transfer`/`task` 工具）与 workflow 复用同一机制：子 agent 跑成
**子 Run**，

- **入站隔离**：全新 `State`，`Messages = [UserText(description)]`，父的 messages/todos
  剥离，**Files 透传**（共享协作面）。
- **出站隔离**：只把子 Run 的**最后一条 assistant 文本**作为单条 ToolMessage 回父，中间
  过程不污染父上下文。

完全对齐 deepagents 的隔离区设计，且天然落在 checkpoint 子树里。

---

## 一次 turn 的完整时序（把所有件串起来）

```
Runtime.Compile(spec) → Agent
Agent.Start(req) → 起 goroutine 跑 AgentLoop,返回 Run{Events: <-chan}
  loop step0:
    bus⇽TurnStarted
    BeforeModel mw(RAG 注入背景)→ ModifyRequest mw(compaction 裁剪)
    streamModel: 每 token bus⇽MessageDelta(UI 实时);结束 bus⇽MessageDone
    有 tool_calls:
      BeforeTool mw(HITL 门禁命中危险工具)→ Directive{Interrupt}
        → checkpointPending(step0, 待批工具)→ bus⇽Interrupted → loop return
  调用方收到 Interrupted → run.Approve("call_1", Allow)
  Agent.Resume(thread, 注入审批):
    loop 从 step0 的 Pending 快照续跑 → execTools 并发 → bus⇽ToolDone
    checkpoint(step0)
    resolve directives = Continue → step1...
    模型无 tool_calls → checkpoint(step1) → bus⇽RunDone → loop 退出
run.Wait() 此刻返回 Result(结算屏障,等 loop 结束 + checkpoint flush)
```

---

## v1 → v2 关键决策对照

| 维度 | v1 | v2 | 收益 |
|---|---|---|---|
| 流原语 | `iter.Seq2[*Event,error]`(拉) | Bus pub/sub(推)+ iter 适配器 | 多订阅;但保留 pull 简单路径 |
| 观测 vs 持久化 | **同一条事件流**兼任 | **Bus 观测 / Checkpointer 持久**分家 | 认知负担主因被消除 |
| 控制流 | `event.Actions` commit 时合并 | `Directive` 显式返回值 + 明确优先级 | 读 loop 即见控制流 |
| 持久/恢复 | 事件溯源重放 | checkpoint 快照树 | 更直观;原生分支/time-travel |
| 数据 vs 引擎 | `Agent` 接口揉一起 | `AgentSpec`(数据)/`Runtime`(引擎) | 易测、易序列化、易组合 |
| 中间件作用域 | 装饰 `llm.Model`(够不到工具) | 钩 loop 相位(全可见) | HITL/权限/重试能管工具层 |
| HITL | middleware 临时实现 | `interrupt`+`Pending` 快照+`Approve` | 一等公民、可跨进程恢复 |
| Loop | 藏在 Run 里的 for | 显式相位机 + LoopPolicy | "可控"落到具体相位 |

---

## 理由

- **拆分双职责**：Bus（观测）与 Checkpointer（持久/恢复）各司其职，消除 v1 单流双语义的
  认知负担——这是 v2 可读性的主要来源。
- **控制流显式**：`Directive` 作返回值 + 明确优先级，比 `Actions` 延迟合并好读、好测、好扩展。
- **数据/引擎分离**：`AgentSpec` 纯数据使 agent 可序列化、可被 workflow 组合、可单测。
- **checkpoint 替代事件溯源**：快照比重放直观，且一举拿下 LangGraph 的分支/time-travel/HITL。
- **middleware 看得到工具层**：修掉 v1「装饰 Model 够不到工具」的能力边界。
- **保留 pull 适配器**：不牺牲 v1 简单消费者的体验。

---

## 后果（风险与缓解，诚实清单）

1. **push/bus 引入 goroutine 生命周期与反压**——iter.Seq2 本免费的东西现在要自己管。
   *缓解*：bus 投递 per-subscriber 异步 + Lossy/Lossless 策略；loop 单 goroutine 顺序
   publish 保证有序；对外留 iter 适配器。**接受这是复杂度互换，不假装更简单。**
2. **丢失事件溯源的可重放/可审计**。*缓解*：checkpoint 树承担恢复；若仍需审计，加一个
   Lossless 的 tracing 订阅者落 JSONL（观测流落盘 ≠ 依赖它恢复）。
3. **State 必须可序列化**（checkpoint 要求）。*缓解*：Files/KV 设计成可编解码；不可序列化
   资源（连接句柄）放 context 不进 State。
4. **HITL 恢复语义**：借鉴 LangGraph 教训——若 resume 整步重跑，中断前副作用须幂等；v2 因
   「从 Pending 快照续跑已执行部分」可避免重跑已批工具，但须重点测这条路径。
5. **Directive 优先级与 workflow Escalate 的交互**须写清并覆盖测试。
6. **迁移成本**：core 根基变动牵涉所有上层包；以增量、双轨并存的方式落地（见下）。

---

## 备选方案

- **维持 v1 不动**：可读性问题持续；放弃 LangGraph 式分支/time-travel/HITL 的成熟模型。
- **push 取代 pull 但不拆双职责**：只换原语不拆语义，认知负担不降反升（多了反压管理）。否决。
- **全盘照搬 LangGraph 的 channel/reducer**：心智过重，与 goagent「接口尽量小」相悖；只取
  分支/checkpoint，不取 reducer。
- **保留 Actions 与 Directive 并存**：两套控制流并行，更乱。否决，一次性切换。

---

## 迁移步骤（增量、可验证，v1/v2 双轨并存）

1. **core 扩充**：`Event` 改 sealed union；新增 `Directive`/`State`/`Checkpoint` 类型。不删 v1。
2. **Bus + iter 适配器**：实现总线与 `Run.Iter()`，现有 example 用适配器零改动跑通。
3. **AgentLoop 相位机 + Middleware 接口**：把 v1 `LLMAgent.Run` 循环搬进显式相位机，v1
   middleware 平移。
4. **Checkpointer**：先 InMemory 后端打通 resume；再补文件后端，验证跨进程恢复（复用
   v1 `examples/persistent`）。
5. **HITL**：`BeforeTool` 门禁 + Pending 快照 + `Approve`，跑通 v1 hitl 场景。
6. **分支/time-travel**：`Checkpoint.ParentID` + `History`/`Load` fork。
7. **Workflow 重表达**：Sequential/Parallel/Loop/Pipeline 改 Spec 组合 + Runtime driver。
8. **Subagent 隔离 + deepagents 件**（Todos / Files backend / Skills / Permission）逐个接为 middleware。
9. 全绿后删 v1 的 `Stream`/`Actions`，本 ADR 转「已接受」，相应标注 0001/0004 为 superseded。

---

## v2 整体程序目录与文件骨架

> 约定：v2 新增/重构包以注释标注职责；标 `*` 者为 v1 已有、v2 重构；其余为 v2 新增。
> 保持 `core` 为依赖根、其余包尽量不互相依赖（延续 v1 的无环依赖原则）。

```
goagent/
├── core/                         # 依赖根:共享词汇,不依赖任何上层
│   ├── message.go          *     # Message/Part(sealed union)/ToolCall/ToolResult(v1 保留)
│   ├── event.go            *     # v2:Event sealed union(RunStarted/MessageDelta/.../RunFailed)
│   ├── directive.go              # v2 新增:Directive/DirectiveKind + resolve()优先级
│   ├── state.go                  # v2 新增:State(Messages/Todos/Files/KV)+ StateOp + apply
│   ├── id.go               *     # NewID
│   └── progress.go               # v2:Progress/ProgressInfo(从 v1 event.go 拆出)
│
├── bus/                          # v2 新增:pub/sub 事件总线(channel 实现)
│   ├── bus.go                    # Bus/Topic/Subscribe/Publish + DeliveryMode(Lossy/Lossless)
│   ├── subscriber.go             # per-subscriber 缓冲通道 + 丢弃/阻塞策略
│   └── bus_test.go               # 多订阅者、慢订阅者丢弃、顺序保证
│
├── checkpoint/                   # v2 新增:状态快照树(可恢复/分支/time-travel/HITL)
│   ├── checkpoint.go             # Checkpoint/PendingHITL/Checkpointer 接口
│   ├── memory.go                 # InMemory 后端(原型/测试)
│   ├── file.go                   # JSONL 文件后端(承接 v1 session.FileStore 的落盘思路)
│   ├── branch.go                 # fork/History/time-travel 辅助(ParentID 树遍历)
│   └── checkpoint_test.go        # save/load/latest/resume/branch
│
├── runtime/                      # v2 新增:引擎(替代 v1 runner 的编排角色)
│   ├── runtime.go                # Runtime{store,checkpoint,bus,scheduler} + Compile
│   ├── agent.go                  # 已编译 Agent 句柄 + Start/Resume
│   ├── run.go                    # Run 句柄:Events/Steer/Approve/Wait/Cancel/Iter
│   ├── loop.go                   # AgentLoop:显式相位机(PrepareTurn..ApplyDirectives)
│   ├── loopctx.go                # LoopContext/RunContext(穿过相位的可变上下文)
│   ├── exectools.go              # 工具并发执行(串/并发 by LoopPolicy)+ 顺序还原
│   ├── transfer.go         *     # 委派方向规则(sub/parent/peer)+ 防环深度(v1 移植)
│   ├── steering.go               # steering/follow-up 队列(pi 双队列)
│   ├── iter_adapter.go           # Run.Iter():Bus → iter.Seq2 适配器
│   ├── runtime_test.go
│   └── loop_test.go
│
├── spec/                         # v2 新增:声明式 AgentSpec 与 workflow 组合
│   ├── spec.go                   # AgentSpec/LoopPolicy/ToolExecMode
│   ├── workflow.go         *     # Sequential/Parallel/Loop/Pipeline(返回 AgentSpec,v1 重构)
│   ├── pipeline.go         *     # Pipeline builder(fluent,v1 重构)
│   ├── subagent.go               # SubAgent 隔离规格(入站剥离/出站收敛)
│   └── workflow_test.go
│
├── middleware/                   # *重构:从"装饰 llm.Model"改为"钩 loop 相位"
│   ├── middleware.go       *     # Middleware 接口(6 钩子)+ BaseMiddleware no-op + Chain
│   ├── compaction.go       *     # ModifyRequest:超阈值结构化摘要(v1 移植)
│   ├── rag.go              *     # BeforeModel/ModifyRequest:注入相关背景(v1 移植)
│   ├── retry.go            *     # OnError:指数退避重试(v1 移植)
│   ├── ratelimit.go        *     # BeforeModel:RPS/并发闸(v1 移植)
│   ├── steering.go         *     # PrepareTurn:注入 steering 消息(v1 移植)
│   ├── hitl.go             *     # BeforeTool:危险工具门禁 → Interrupt(v1 移植 + checkpoint 集成)
│   ├── summarization.go          # AfterModel/ModifyRequest:长线程压缩(deepagents)
│   ├── permission.go             # BeforeTool:allow/deny/interrupt(deepagents)
│   └── *_test.go
│
├── llm/                    *     # 不动:provider 隔离(model/image/video + anthropic/openaicompat/agnes/mock)
├── tool/                   *     # 微调:tool.Result 增 Control/State 字段;Context 去掉 Actions sink
│   ├── tool.go             *     # Tool 接口 + Result{Content,IsError,Control,State}
│   ├── function.go         *     # tool.New[In,Out] 泛型(不动)
│   ├── registry.go/schema.go *
│   ├── web/ exec/ mcp/     *     # 联网/沙箱执行/MCP 客户端(不动)
│
├── state/                        # v2 新增:State 的子能力(deepagents 借鉴)
│   ├── todos.go                  # Todo/TodoList(规划工具 write_todos 的后备存储)
│   ├── files/                    # 虚拟文件系统后端(可插拔)
│   │   ├── backend.go            # Backend 接口(ls/read/write/edit/glob/grep/delete)
│   │   ├── instate.go            # StateBackend:文件存 State,随 checkpoint(默认)
│   │   ├── disk.go               # FilesystemBackend:真实磁盘
│   │   └── backend_test.go
│   └── kv.go                     # KV 读写 + StateOp 应用
│
├── session/                *     # 重构:Store 仍管 (app,user) 维度的会话索引;历史改由 checkpoint 承载
│   ├── store.go            *     # Store 接口(GetOrCreate)
│   ├── file.go             *     # 文件后端(与 checkpoint/file.go 协同)
│   └── state.go            *     # (并入 core/state.go 后逐步废弃)
│
├── prompt/                 *     # 不动:Section/Builder(Identity/Environment/ToolGuidance/SessionState)
├── skill/                  *     # 微调:PromptSection/use_skill/run_skill_script 作为 middleware 接入
├── sandbox/                *     # 不动:Policy + process 后端
├── memory/ embeddings/     *     # 不动:向量库/RAG/embedder
├── callbacks/              *     # 重构:OnEvent → 实现为一个 Lossless Bus 订阅者
├── scheduler/                    # v2 新增:背景任务(承接 v1 queue,改订阅 Bus)
│   ├── scheduler.go              # Job/Queue/Worker(EnqueueAgent 桥接 → Bus)
│   └── scheduler_test.go
│
├── docs/
│   ├── ARCHITECTURE.md     *     # 更新:v2 分层图 + 时序图
│   ├── DESIGN.md           *     # 更新:取舍
│   └── adr/0023-v2-architecture.md   # 本文
│
└── examples/                     # v2 demo(见下)
    ├── v2-quickstart/
    ├── v2-streaming-bus/
    ├── v2-resume/
    ├── v2-hitl/
    ├── v2-branch/
    ├── v2-workflow/
    └── v2-deepagent/
```

---

## 几个 demo（可运行示例，全部 mock provider、无需 API key）

> 命名加 `v2-` 前缀与 v1 example 并存；迁移期两套都能 `go test ./...` 通过。

### 1. `examples/v2-quickstart` — 最小闭环
展示 `spec.AgentSpec` → `runtime.Compile` → `Agent.Start` → `run.Iter()` 拉事件。证明
v2 对简单单消费者仍是「一个 for 循环」，零总线心智。
```go
rt := runtime.New(runtime.Config{})
ag := rt.Compile(spec.AgentSpec{Name: "assistant", Model: mock.New(...), Tools: []tool.Tool{weather}})
run, _ := ag.Start(ctx, runtime.RunRequest{Thread: "s1", Msg: core.UserText("北京天气?")})
for ev, err := range run.Iter() {
    if md, ok := ev.(core.MessageDone); ok { fmt.Println(md.Message.Text()) }
}
```

### 2. `examples/v2-streaming-bus` — 多订阅者总线
一个 Run，**三个订阅者**同时消费：UI（Lossy，丢 partial）、JSONL tracing（Lossless）、
进度条（只看 `Progress`）。证明 push/bus 相对 v1 单流的核心增益——**多消费者无需 tee**。

### 3. `examples/v2-resume` — 跨进程可恢复
第一次进程跑到一半 `run.Cancel()`；第二次进程 `Agent.Resume(thread)` 从最新 checkpoint 续
跑。对照 v1 `examples/persistent`，证明 checkpoint 快照替代事件重放后的恢复路径。

### 4. `examples/v2-hitl` — 人在环中（中断/审批/恢复）
agent 调危险工具 `delete_file` → `permission`/`hitl` middleware 在 `BeforeTool` 返回
`Interrupt` → 落 Pending 快照 + 发 `Interrupted` 事件 → 控制台 approve/reject/改参 →
`run.Approve` → 从 Pending 快照续跑。拒绝原因反馈给模型可改道。

### 5. `examples/v2-branch` — 分支 / time-travel
同一 thread 跑到 step N 后，`checkpoint.History` 列出快照，`Load` 某历史点 `Fork` 出新
thread 跑不同后续。证明 `Checkpoint.ParentID` 快照树支持「同一历史点试两条路」。

### 6. `examples/v2-workflow` — 确定性编排（保留 v1 能力）
`spec.Pipeline("report").Then(planner).ThenParallel("gather", web, papers).Then(writer).
ThenLoop("review", 3, critic, reviser).Build()`。证明 workflow agent 在 Spec/Runtime 模型
下重新表达后行为不变，且 Parallel 用分支隔离、Loop 用 Escalate Directive 跳出。

### 7. `examples/v2-deepagent` — 深度智能体（子智能体隔离 + 规划 + 文件系统）
一个 research agent：`write_todos` 规划 → `task` 委派给隔离子智能体（入站剥离父上下文、
Files 透传、出站只回最后一条文本）→ 大工具结果卸载到虚拟文件系统 `/large_results/`，主
上下文保持精简。证明 deepagents 四支柱（提示/规划/子智能体/文件系统）以 middleware 形态接入。

---

## 关联 ADR

- supersedes（迁移完成后）：[0001](0001-event-stream-primitive.md)（流原语）、[0004](0004-side-effects-as-actions.md)（Actions）
- 扩展/重构：[0006](0006-streaming.md)（流式）、[0008](0008-transfer.md)（委派）、
  [0009](0009-jsonl-persistence.md)（持久化→checkpoint）、[0011](0011-middleware-decorator.md)（中间件）、
  [0013](0013-middleware-hitl.md)（HITL）、[0022](0022-media-generation.md)（媒体/队列→scheduler）
- 不变：[0002](0002-message-wire-boundary.md)（wire 边界）、[0005](0005-provider-isolation.md)（provider 隔离）、
  [0014](0014-prompt-builder.md)（prompt）、[0015](0015-skills.md)（skills）、[0012](0012-sandbox.md)（sandbox）
