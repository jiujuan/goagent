# PlanAgent —— DAG 计划执行器

> 把一个复杂任务拆成有依赖关系的子步骤,确定性地并发执行,支持人工审批、错误处理、
> 状态跟踪、恢复执行,以及**运行中自我重写计划并执行重写后的计划**。

PlanAgent 是 goagent 的"硬执行引擎":与 `write_todos`(软计划,LLM 自己记计划)互补,
PlanAgent 由**运行时**确定性地调度执行,带顺序/并发/审批/恢复的保证。

- 源码:[agent/plan.go](../agent/plan.go)
- 示例:[examples/plan-dag](../examples/plan-dag)(静态 DAG + 最终审批)、
  [examples/plan-approval](../examples/plan-approval)(节点级审批)、
  [examples/plan-replan](../examples/plan-replan)(动态重规划)

---

## 一、概述

```go
plan := agent.Plan{Nodes: []agent.Node{
    {ID: "research", Task: "调研「{{input}}」"},
    {ID: "risks",  Task: "据 {{research}} 列风险", DependsOn: []string{"research"}},
    {ID: "bench",  Task: "据 {{research}} 给建议", DependsOn: []string{"research"}}, // 与 risks 并发
    {ID: "report", Task: "综合 {{risks}} {{bench}} 写报告", DependsOn: []string{"risks", "bench"}, Approve: true},
}}
pa := agent.NewPlan("research", plan, agent.WithWorker(worker), agent.WithConcurrency(4))
answer, _ := pa.Run(ctx, "向量数据库选型")
```

`research` 先跑;`risks` 和 `bench` **并发**;两者完成才轮到 `report`;`report` 执行前**人工审批**。

**何时用 PlanAgent(而非 workflow / write_todos):**

| 你要的 | 用 |
|---|---|
| 固定的串/并/循环管道 | `Sequential` / `Parallel` / `Loop` / `Pipeline` |
| 开放式、LLM 自己决定下一步 | `write_todos`(软计划) |
| **任意依赖(DAG)、并发、审批、可恢复、可自我重写** | **PlanAgent**(`NewPlan`) |
| **给一句任务,让 LLM 自动拆成 DAG 并执行** | **PlanAgent**(`NewLLMPlan`,见 4.8) |

---

## 二、架构设计

### 2.1 定位:PlanAgent 是一个 `*Agent`

`NewPlan(...)` 返回一个 `*Agent`,与普通 LLM agent、workflow agent **同构**——拥有同样的
`Run / Stream / Resume` 接口。它的 `runnable` 是一个 `planRunner`(实现内部 sealed `Runnable`),
`run(rc)` 不调模型,而是**编排节点**。

```
*Agent (NewPlan)
  └ planRunner.run(rc *RunContext) runOutcome    ← 调度器
      ├ 每个节点 = 一次隔离子运行(rc.subRun → worker.runnable.run)
      ├ 事件 → rc.Bus(PlanNodeStarted/Done + 节点内部的 Turn/Tool/Message)
      ├ 状态 → rc.State.KV["__plan__"](随 checkpoint)
      └ 审批 → Interrupt / Pending / Resume(复用 HITL 机制)
```

**为什么做成 Runnable 而非独立机制?** 这样它白送复用:`Run/Stream/Wait/Resume/Steer`、
事件总线、checkpoint、以及统一的生命周期事件(`RunStarted`/`RunDone`/`Interrupted` 由 Run 句柄
统一发,planRunner 只发节点级事件)。

### 2.2 核心抽象

```go
type Node struct {
    ID         string   // 唯一;勿用保留前缀 "__"
    Task       string   // 指令;可含 {{id}} / {{input}} 占位符
    Worker     *Agent   // 跑此节点的 agent;nil 则用 WithWorker 默认
    DependsOn  []string // 依赖的节点 ID(必须先 done)
    Approve    bool     // 执行前需人工批准(节点级 HITL)
    MaxRetries int      // 失败重试次数
}
type Plan struct{ Nodes []Node }

type ErrorPolicy int  // FailFast(默认) | ContinueOnError
```

### 2.3 执行状态模型(可恢复的关键)

整盘执行状态序列化进 `core.State.KV["__plan__"]`(JSON 字符串):

```go
type planState struct {
    Status       map[string]string // nodeID -> pending|running|done|failed|skipped|rejected
    Output       map[string]string // nodeID -> 终答文本
    Attempts     map[string]int    // 重试计数
    Dynamic      []nodeSpec        // 重规划追加的节点(静态节点在 runner 里)
    ReplanRounds int               // 已重规划轮数(防失控)
}
```

因为状态在 `State` 里、`State` 随 `checkpoint` 落盘,所以 **`Agent.Resume` 重跑 runnable 时,
`run()` 开头从 `State` 读回 planState,只跑未完成节点——恢复"白送",对 PlanAgent 零特判**。
崩溃容灾:加载时把 `running` 归一为 `pending`(重跑)。

### 2.4 调度模型(拓扑 + 有界并发 + 静默处理)

`run()` 是一个循环,每轮:

1. **就绪集**:扫描所有节点,挑 `status==pending && 依赖全 done` 的;
2. **审批门**:就绪的 `Approve` 节点——`allow` 则启动,`reject` 则标记 rejected(级联跳过下游),
   未决则收进 `awaiting`,先不跑;
3. **并发启动**:其余就绪节点起 goroutine 跑(上限 `WithConcurrency`,默认 8),发 `PlanNodeStarted`;
4. **静默判定**(`inflight==0` 且无就绪):
   - 有 `awaiting` → 保存 + **暂停**(`Interrupted{Pending}`);
   - 否则 `skipBlocked` 级联跳过被 failed/skipped/rejected 阻塞的节点;
   - 再否则 → **重规划**(若配置):有新节点就合并继续,无则收尾;
5. **回收一个完成**:`done`/`failed`(按策略 + 重试)→ 更新状态、镜像输出到 `KV[id]`、发
   `PlanNodeDone`、checkpoint。

### 2.5 数据流:`DependsOn` 与 `{{id}}` 是两件事

- **`DependsOn` = 排序**:控制"谁先 done 谁才能跑";
- **`{{id}}` = 数据**:节点 `Task` 里 `{{depID}}` 在执行前被替换成该依赖节点的输出(`renderTemplate`
  over `State.KV`);`{{input}}` 是计划的原始输入。

二者正交:一个节点可以"依赖 A 排在它后面"但不引用 A 的文本,反之亦然(但要引用就通常也要依赖)。

### 2.6 节点隔离执行

每个节点跑成**隔离子运行**(`rc.subRun`):全新 `State`,`Messages=[渲染后的 Task]`,**共享 Files**
(协作面),用节点的 `Worker`(或默认 worker)的 `runnable` 跑;**只取终答文本**作为节点输出。
节点之间不共享中间对话——一个节点的推理/工具调用不会污染另一个。

### 2.7 事件模型

planRunner 发两个专门事件(`core` 里的 sealed union 成员):

```go
type PlanNodeStarted struct{ NodeID string }
type PlanNodeDone   struct{ NodeID, Status string; Err error }
```

`Status ∈ {done, failed, retry, skipped, rejected}`。重规划用合成节点 `NodeID == "__replan__"`。
生命周期事件(`RunStarted`/`RunDone`/`Interrupted`/`RunFailed`)由 Run 句柄统一发,不在节点级重复。

### 2.8 审批模型(两级)

- **节点级**(`Node.Approve`):该节点执行前暂停。`Pending` 里 `CallID == 节点ID`,`Tool=="approve_node"`,
  `Args==节点Task`。独立分支在它候审时**继续跑**。
- **整盘级**(`WithFinalApproval`):所有节点完成后、出最终结果前暂停一次,`CallID == "__plan__"`。

两者都复用统一 HITL:`Interrupted` → `Run.Decide(Allow/Reject)` → `Run.Resume`。`Resume` 把决策
**合并**进 `State.KV["__approvals__"]`(跨多次暂停累积)。拒绝是人为选择,不触发 FailFast;被拒节点
状态 `rejected`,其下游被级联 `skipped`。

### 2.9 重规划模型(自我重写并执行)

配置 `WithReplanner(agent)` 后,计划**静默时**询问重规划器是否要追加步骤:

```
执行 → 静默 → 重规划器看「原任务 + 已完成结果」→ 输出 JSON {nodes:[...], done:bool}
     → 校验合并图(ID 唯一非保留、依赖可解析、无环)→ 合并 → 继续执行 → … (有界)
```

- **加法式**:只追加节点,不改/删已有(覆盖"基于结果继续分解"的核心需求);
- 动态节点进 `planState.Dynamic` → **可恢复**;它们用默认 `WithWorker`(`*Agent` 不能进 JSON);
- `parseDelta` 容忍 ```json 围栏与前后散文;坏/空/done 的 delta 直接收尾;
- `WithMaxReplanRounds`(默认 3)防失控。

---

## 三、核心方法详解

### `NewPlan(name string, plan Plan, opts ...PlanOption) *Agent`
编译入口。建立 `planRunner`,**build 期校验 DAG**(`validateDAG`:未知依赖 + 环检测),违规存进
`buildErr`,在 `run` 时作为 `RunFailed` 抛出。返回 `*Agent`。

### `(*planRunner) run(rc *RunContext) runOutcome`
调度器主体(见 2.4)。返回 `runOutcome`:
- 正常完成 → `{Result: 叶子节点输出拼接}`;
- 失败(FailFast)→ `{Err}`;
- 等待审批 → `{Control: Interrupt, Pending}`;
- 最终审批拒绝 → `{Result: "计划结果被拒绝。"}`。

### `(*planRunner) materialize(st) (nodes, order)`
合并**静态节点**(来自 `Plan`,在 runner 里不可变)与**动态节点**(来自 `st.Dynamic`,重规划/恢复
而来),得到本次运行的工作集。调度器遍历的是它,而非 runner 的静态集——这让重规划与恢复都能把
动态节点纳入调度。

### `(*planRunner) execNode(rc, node, input) (string, error)`
跑单个节点:取 `node.Worker`(或默认 worker)→ `rc.subRun(UserText(input))` 隔离子运行 →
返回 `runOutcome.Result.Message.Text()`。未知 worker/失败转成 error。

### `(*planRunner) maybeReplan(rc, nodes, st, order) []nodeSpec`
静默时调用。跑 `replanner`(隔离子运行,prompt 含原任务 + 已完成结果)→ `parseDelta` 解析 →
`validateDelta` 校验合并图 → 返回可合并的新节点(或 nil 收尾)。受 `MaxReplanRounds` 约束。

### `validateDAG` / `findCycle` / `validateDelta`
`findCycle` 是三色 DFS 环检测;`validateDAG` 在 build 期查未知依赖 + 环;`validateDelta` 在重规划期
查新节点(ID 唯一非保留、依赖可解析、合并后无环)。build 期与 delta 期共用 `findCycle`。

### `(*planRunner) skipBlocked(nodes, order, st, rc) bool`
把"依赖已 failed/skipped/rejected"的 pending 节点标记为 `skipped` 并发事件;级联(本轮改了就返回
true,主循环 `continue` 再扫,直到稳定)。

### `(*planRunner) finalOutput(nodes, order, st) string`
计算**叶子节点**(无人依赖的 sink)的 `done` 输出,以空行拼接为最终结果;无叶子时退化为所有
`done` 输出拼接。

### `save` / `loadPlanState`
`save`:把 planState marshal 成 JSON 写入 `KV["__plan__"]`,并 `checkpoint.Save` 落盘。
`loadPlanState`:从 `KV["__plan__"]` 反序列化(恢复),`running→pending` 容灾;无则初始化全 pending。

### 审批链:`Run.Decide` / `Run.Resume` / `approvalFor`
`Run.Decide(Approval)` 记录决策;`Run.Resume(ctx)` → `Agent.Resume(thread, decisions...)` 把决策
**合并**进 `State.KV["__approvals__"]` 并重跑;调度器用 `approvalFor(state, nodeID)` 读 `allow`/`reject`。

---

## 四、使用方法

### 4.1 选项一览

| 选项 | 作用 | 默认 |
|---|---|---|
| `WithWorker(*Agent)` | 节点默认执行者(动态节点必用它) | 无(节点须各自带 Worker) |
| `WithConcurrency(n)` | 并发上限 | 8 |
| `WithErrorPolicy(p)` | `FailFast` / `ContinueOnError` | FailFast |
| `WithFinalApproval()` | 整盘完成后审批一次 | 关 |
| `WithReplanner(*Agent)` | 启用动态重规划(执行中扩展) | 关 |
| `WithMaxReplanRounds(n)` | 重规划轮数上限 | 3 |
| `WithPlanner(*Agent)` | 由 LLM 生成初始 DAG(见 4.8) | 关 |
| `WithMaxPlanAttempts(n)` | 规划失败重试次数 | 3 |

`Node` 字段:`ID / Task / Worker / DependsOn / Approve / MaxRetries`。

### 4.2 静态 DAG(最简)

```go
worker, _ := agent.New(agent.WithModel(model))
plan := agent.Plan{Nodes: []agent.Node{
    {ID: "a", Task: "做 A"},
    {ID: "b", Task: "做 B"},
    {ID: "c", Task: "合并 {{a}} 与 {{b}}", DependsOn: []string{"a", "b"}},
}}
pa := agent.NewPlan("demo", plan, agent.WithWorker(worker))
out, err := pa.Run(ctx, "原始输入")   // {{input}} == "原始输入"
```

### 4.3 错误策略 + 重试

```go
plan := agent.Plan{Nodes: []agent.Node{
    {ID: "fetch", Task: "抓取数据", MaxRetries: 2},          // 失败重试 2 次
    {ID: "parse", Task: "解析 {{fetch}}", DependsOn: []string{"fetch"}},
}}
pa := agent.NewPlan("etl", plan, agent.WithWorker(w), agent.WithErrorPolicy(agent.ContinueOnError))
// fetch 仍失败 → parse 被 skipped,其它独立分支继续;FailFast 则整盘报错。
```

### 4.4 审批 + 恢复循环(可能多波暂停)

```go
run := pa.Stream(ctx, "输入", agent.OnThread("job-1"))
for {
    var pending []core.ApprovalRequest
    done := false
    for ev, err := range run.Iter() {
        if err != nil { log.Fatal(err) }
        switch e := ev.(type) {
        case core.PlanNodeStarted: fmt.Println("▶️", e.NodeID)
        case core.PlanNodeDone:    fmt.Println("✅", e.NodeID, e.Status)
        case core.Interrupted:     pending = e.Pending
        case core.RunDone:         done = true; fmt.Println(e.Result.Message.Text())
        }
    }
    if done { break }
    for _, p := range pending {
        run.Decide(agent.Allow(p.CallID))            // 或 agent.Reject(p.CallID, "理由")
    }
    var err error
    if run, err = run.Resume(ctx); err != nil { log.Fatal(err) }
}
```

### 4.5 恢复执行(进程内)

暂停后 `run.Resume(ctx)` 即从断点续。`pa.Resume(ctx, "job-1")` 也能从该线程最新 checkpoint 续跑。
> ⚠️ **当前限制**:PlanAgent 内置**内存** checkpointer,故恢复是**进程内**的(暂停→恢复有效)。
> 跨进程持久恢复需要文件/DB checkpointer(Stage 4 的 `checkpoint` 文件后端 + 给 `NewPlan` 加注入
> 选项),尚未提供。

### 4.6 动态重规划

```go
replanner, _ := agent.New(agent.WithModel(model), agent.WithInstruction(
    `看完已完成结果,若还缺综合步骤就只输出 JSON 追加节点:`+
    `{"nodes":[{"id":"synthesis","task":"综合 {{a}} {{b}}","depends_on":["a","b"]}],"done":false};`+
    `否则只输出 {"done":true}。只输出 JSON。`))

pa := agent.NewPlan("dag", plan,
    agent.WithWorker(worker),
    agent.WithReplanner(replanner),
    agent.WithMaxReplanRounds(2))
```

### 4.7 每节点不同 Worker(可带工具/子 agent)

```go
lookup := tool.New("lookup", "查资料", func(_ *tool.Context, in struct {
    Q string `json:"q"`
}) (string, error) { return "资料:" + in.Q, nil })

researcher, _ := agent.New(agent.WithModel(model), agent.WithTools(lookup))
writer, _     := agent.New(agent.WithModel(model), agent.WithInstruction("你是作者"))
plan := agent.Plan{Nodes: []agent.Node{
    {ID: "r", Task: "调研 {{input}}", Worker: researcher},
    {ID: "w", Task: "据 {{r}} 写作", Worker: writer, DependsOn: []string{"r"}},
}}
```

> 节点 Worker 是完整 `*Agent`,自带 tools / middleware / 子 agent —— 节点内可以是一个会用工具、
> 会委派、会自我循环的复杂 agent。

### 4.8 LLM 生成计划(`NewLLMPlan`)

不手写 DAG,而是给一个 **planner** agent,让它把任务自动拆成 DAG:

```go
planner, _ := agent.New(agent.WithModel(model)) // 框架会补充"输出 JSON DAG"的指令
worker, _  := agent.New(agent.WithModel(model))

pa := agent.NewLLMPlan("auto", planner,
    agent.WithWorker(worker),
    agent.WithReplanner(planner),     // 可选:执行后再补步骤 → 全自动闭环
)
answer, _ := pa.Run(ctx, "把竞品 A/B/C 的定价做成对比报告")
```

**原理**:`NewLLMPlan` = 空静态计划 + `WithPlanner`。`run()` 起始多一个**规划相位**:跑 planner →
解析 JSON `{nodes:[...]}` → 校验(环/坏依赖/重复 ID)→ 合并为动态节点 → 之后完全复用调度器。

- 生成的计划进 `planState`(`Planned=true`),**可恢复**:resume 不重新规划。
- planner 产坏 DAG 时,把错误回喂它**重试**(`WithMaxPlanAttempts`,默认 3);仍失败则 run 报错。
- planner 输出 `{"nodes":[]}`(任务无需拆分)→ 执行器退化为**单节点**直接跑 worker on `{{input}}`。
- 规划阶段发合成事件 `PlanNodeStarted/Done{"__planner__"}`(前端可显示"正在规划")。
- 与 `WithReplanner` 组合 = **plan → execute → replan → execute** 全自动闭环。

> 静态 `NewPlan` 与 `NewLLMPlan` 共用同一执行器;前者开发者写死 DAG,后者 LLM 生成 DAG。

---

## 五、参考表

**节点状态(`PlanNodeDone.Status` / `planState.Status`)**

| 状态 | 含义 |
|---|---|
| `pending` | 待跑(依赖未满足 / 等审批 / 待重试) |
| `running` | 执行中(仅内存态;恢复时归一为 pending) |
| `done` | 成功,输出已写入 `KV[id]` |
| `failed` | 重试用尽仍失败 |
| `retry` | 本次失败,将重试(事件) |
| `skipped` | 依赖 failed/skipped/rejected 而被级联跳过 |
| `rejected` | 节点级审批被拒 |

**保留的 `State.KV` 键**

| 键 | 内容 |
|---|---|
| `__plan__` | planState 的 JSON |
| `__approvals__` | `map[callID]"allow"\|"reject"` |
| `input` | 计划原始输入(`{{input}}`) |
| `<nodeID>` | 各节点输出(供 `{{nodeID}}`) |

> 合成节点 ID:`__planner__`(LLM 初始规划)、`__replan__`(动态重规划)——只发事件,不进调度。
> 节点 ID 勿用 `__` 前缀(保留),勿与上述键冲突。

---

## 六、限制与后续

- **跨进程持久恢复**:需文件/DB checkpointer + `NewPlan` 注入选项(待补)。
- **重规划是加法式**:只追加节点,不改/删已有(覆盖主场景;全量重写待评估)。
- **节点失败时不重规划**:Step 3 在"静默"时重规划;"失败时恢复式重规划"可作后续。
- **动态节点无自定义 Worker**:统一用默认 worker(`*Agent` 不可序列化)。

---

## 七、完整可运行示例

| 示例 | 演示 |
|---|---|
| [examples/plan-dag](../examples/plan-dag) | 静态 DAG · 并发 · `{{id}}` 传值 · 最终审批 |
| [examples/plan-approval](../examples/plan-approval) | 节点级审批 · 独立分支照跑 · 拒绝级联 · 多波恢复 |
| [examples/plan-replan](../examples/plan-replan) | 动态重规划:执行→追加 synthesis→再执行 |
| [examples/plan-llm](../examples/plan-llm) | LLM 生成初始 DAG(`NewLLMPlan`)+ 重规划全自动闭环 |

设计取舍详见 [ADR 0024](adr/0024-clean-v2-layout.md);PlanAgent 三步实现见 git 历史
(`feat(agent): DAG PlanAgent — Step 1/2/3`)。
