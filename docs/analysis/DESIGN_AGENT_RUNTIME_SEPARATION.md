# Agent + Runtime 分离与 RuntimePool 并发方案（修订版）

> 本文是对初版设计文档的审核修订。初版的立论（"当前 Agent 内 loop/bus/store 混在一起导致并发竞态"）
> 经逐条对照代码后不成立，示例代码存在编译不过、忽略 workflow agent、池实现有 panic 等问题。
> 修订版：改立论为"显式生命周期 + 池化复用 + 有状态 middleware 的实例隔离"，
> 修正全部示例代码，并给出分阶段的实施路径。
>
> 文中所有 `文件:行号` 引用均已于 2026-10-12 对照当前 main 分支代码核实。

---

## 1. 现状：并发安全性逐组件核实

初版声称 `Agent` 结构体里的运行时字段造成并发竞态。实际核对结果：

| 字段 | 代码位置 | 构造后是否可变 | 并发访问路径 | 结论 |
|---|---|---|---|---|
| `loop *AgentLoop` | agent/agent.go:28 | 只读。`byName/schemas/seqTool` 仅在新 `New` 阶段经 `addTool` 写入（agent/loop.go:84-90），构造完成后各阶段均为读 | 多个 `a.Run` 并发读 | **安全**（构造后不可变） |
| `bus *bus.Bus` | agent/agent.go:31 | 内部 `sync.RWMutex`（bus/bus.go:45） | 事件按 runID 派生的 topic 隔离（agent/agent.go:208-215） | **安全** |
| `store checkpoint.Checkpointer` | agent/agent.go:32 | 接口，并发安全是 Checkpointer 自身契约 | 跨进程竞争已由 Durable/Claim 机制处理（agent/hitl.go:84-90，ADR-0032） | **取决于实现**，与 Agent 结构无关 |
| `runnable Runnable` | agent/agent.go:30 | `llmRunner` 只持 agent/loop 反向指针（agent/transfer.go:28-31），执行时的全部可变状态在每次新建的 `RunContext` 里 | 同 loop | **安全** |
| `parent *Agent` | agent/agent.go:29 | 构造期接线（agent/agent.go:60-62），transfer 目标在运行期惰性解析（agent/transfer.go:55-74） | 只读 | **安全** |

run 作用域隔离在现有架构里已经就位：事件流按 topic、状态按每 run 新建的 `RunContext`、
注入工具按 `RunContext.dynamic`（agent/runtime.go:144-168），并有
`TestInjectionDoesNotLeakAcrossRuns`（agent/dynamictool_test.go:210）钉住。

**所以本方案的收益不是"消除竞态"——那个问题不存在。**真实收益是下面三条：

1. **显式执行实例生命周期**：目前 loop/runnable 随 Agent 构造、永不释放，没有 Close 的概念；
   对持有外部资源（连接、文件句柄）的 middleware 尤其别扭。
2. **池化复用**：`newLoop` 每次运行都要重建 middleware 栈、`tool.ByName` 索引、`tool.Schemas`
   列表（agent/loop.go:50-78）。这部分开销是否值得池化，**需要基准测试后再决定**（见 §8 Phase 3 门槛）。
3. **有状态 middleware 的实例隔离**：若 middleware 实例带可变状态，当前所有 run 共享同一批实例；
   分离后每个 Runtime 可持有自己的一份 middleware 实例。注意：这是新增能力，不是修复现状。

## 2. 命名与概念关系：Runtime ≠ RunContext

包里已有注释明确"没有独立的 runtime 包，runtime 概念由 `RunContext` 承载"
（agent/agent.go:1-6、agent/runtime.go:15-21）。新增 `Runtime` 类型必须说清两者关系，否则一个包出现两个"runtime"：

- **`Runtime`（新增）**：长生命周期的执行实例句柄。持有本实例专属的 `AgentLoop` 与
  `Runnable`，可创建多个 run、可 Close、可被池复用。**不是** Context，不作为参数层层传递。
- **`RunContext`（保持现名）**：一次 run 的执行环境，随函数调用显式传递，run 结束即消失。
  现有 75 处引用几乎全是 middleware/loop 的参数签名，且 `RunContext` 出现在 `Middleware`、
  `RunFinisher` 等导出钩子签名里（agent/middleware.go:22-29、agent/agent.go:75-81），
  **不改名**——改名既不符合它的形态，也是 breaking change。

对照：`Runtime` 之于 `RunContext`，类似数据库连接池里的"连接对象"之于"单次查询的会话参数"。

## 3. 所有权划分（定稿）

初版正文说"每个 Runtime 独立 bus、store 副本"，但示例代码又默认取 `cfg.bus/cfg.store`，自相矛盾。
更关键的是**每 Runtime 独立 store 会破坏 thread 连续性**：`OnThread` 的多轮对话靠共享 checkpointer
按 threadID 取历史（agent/agent.go:102、agent/hitl.go:69），同一 thread 的 run 若落到持有不同 store 的
Runtime 上，历史直接断片。初版的 `WithStore` 示例选项还允许每个 Runtime 换不同 checkpointer，
这与 ADR-0032 的快照一致性前提冲突。

定稿的所有权划分：

| 组件 | 归属 | 理由 |
|---|---|---|
| `config`、`parent`、子 Agent 委派树 | **Agent**（共享、构造后只读） | 是声明不是运行状态；transfer 目标本就惰性解析 |
| `*AgentLoop`（配置派生物）+ `llmRunner` | **Runtime**（每实例一份） | 池化复用的单位；为有状态 middleware 预留实例边界 |
| `*bus.Bus` | **Agent**（全实例共享） | 已线程安全；事件按 topic 隔离；`Agent.Bus()` 的外部订阅契约（agent/agent.go:70）保持不变 |
| `checkpoint.Checkpointer` | **Agent**（全实例共享） | thread 连续性要求共享；并发安全由实现自身与 Durable/Claim 负责 |
| 每 run 的可变状态 | **`RunContext`**（每 run 新建，现状不变） | 已经是正确形态，不动 |

不提供 `WithBus`/`WithStore` 之类的 per-Runtime 覆盖选项。

一个必须写进文档的事实：**HITL 的 pause→Resume 连续性完全经 store 传递**——
`Resume` 从 checkpoint 重放（agent/hitl.go:65-90），`RunContext.resumed` 是每 run 新建的
（agent/runtime.go:36-37），loop 自身不携带任何 thread 状态。因此 pause 的 run 归还 Runtime、
resume 落在另一个 Runtime 上是安全的，只要两者共享同一个 store。这是池化可行的前提，也是
"Runtime 不得持有任何 thread 作用域状态"这条约束的来源。

## 4. 目标结构与 API

### 4.1 Agent（瘦身为纯声明）

```go
// agent/agent.go
type Agent struct {
    cfg      config
    parent   *Agent // 委派树接线保留在 Agent：构造期完成、运行期只读
    pool     *RuntimePool // nil = 未启用池（WithRuntimePool 在 New 内部构造）

    mu      sync.Mutex
    defaultRt *Runtime // 惰性创建的默认 Runtime，供便捷方法复用
}
```

`New` 仍然 fail-fast，并把现在藏在字段初始化里的校验都留住（修订初版遗漏）：

```go
func New(opts ...Option) (*Agent, error) {
    c := config{maxTurns: defaultMaxTurns}
    for _, o := range opts {
        o(&c)
    }
    if c.model == nil {
        return nil, errors.New("agent: WithModel is required")
    }
    if c.bus == nil {
        c.bus = bus.New()
    }
    if c.store == nil {
        c.store = checkpoint.NewMemory()
    }
    // requireThreadCoordination 校验必须留在 New：现状即 fail-fast
    // （agent/agent.go:50-54），挪到 Runtime 创建期是倒退。
    if c.requireThreadCoordination {
        if _, ok := c.store.(checkpoint.ThreadCoordinator); !ok {
            return nil, fmt.Errorf("agent: checkpointer %T: %w", c.store, checkpoint.ErrCoordinationUnavailable)
        }
    }
    a := &Agent{cfg: c}
    for _, s := range c.subAgents {
        s.parent = a
    }
    if c.poolCfg != nil {
        p, err := newRuntimePool(a, *c.poolCfg) // 修正初版"在 New 参数里引用尚未构造的 a"的编译错误
        if err != nil {
            return nil, err
        }
        a.pool = p
    }
    return a, nil
}
```

对应选项形态：

```go
// 用法（修正初版示例：池配置是 Option，池对象在 New 内部构造）
a, err := agent.New(
    agent.WithModel(model),
    agent.WithRuntimePool(RuntimePoolConfig{MaxWorkers: 50, MinWorkers: 10}),
)
```

便捷方法 `Run/Stream/Resume/finishRun/Bus` 保持现签名不变；内部经 `a.runtimeFor()`
取一个临时 Runtime（池模式：Acquire；非池模式：现造现用，或默认 Runtime，见 §5 规则 R2）：

```go
func (a *Agent) Stream(ctx context.Context, input string, opts ...RunOption) *Run {
    rt, err := a.acquireRuntime(ctx)
    if err != nil {
        run := a.emptyRunHandle(ctx) // 现有惰性句柄机制（agent/agent.go:141-151）
        run.startErr = err
        return run
    }
    return rt.Stream(ctx, input, opts...)
}
```

workflow agent（`Sequential/Parallel/Loop`，agent/workflow.go:25-68）的 `loop` 为 nil、
`runnable` 是编排器（agent/agent.go:28、agent/workflow.go:73-83）。`NewRuntime` 必须分支处理，
这是初版示例的硬伤（它无条件 `newLoop(cfg)`）：

```go
func (a *Agent) buildLoopAndRunnable() (Runnable, error) {
    if isWorkflow(a) { // cfg.model == nil 且 runnable 由 wrapWorkflow 注入
        return a.cfg.wfRunnable, nil // workflow 直接沿用配置里的编排器
    }
    l := newLoop(a.cfg)
    if tt := transferToolFor(a.transferTargets()); tt != nil {
        l.addTool(tt) // addTool 只在新构造的 loop 上调，构造后只读，与现状一致
    }
    return &llmRunner{agent: a, loop: l}, nil
}
```

> 实施注意：`wrapWorkflowWithStore` 构造的 `config` 不含 model（agent/workflow.go:81），
> 而 `New` 要求 `WithModel`——说明 workflow Agent 目前根本不走 `New`，由 workflow.go 直接
> `&Agent{...}` 构造。分离改造时 workflow 构造路径（含 `AsTool`、plan executor 等内部
> 构造点）要一并盘点，不能假设都经过 `New`。

### 4.2 Runtime（每实例一份 loop/runnable）

```go
// agent/runtime.go（与 RunContext 同文件，注释讲清二者关系）
type Runtime struct {
    agent    *Agent   // 共享配置与 bus/store
    runnable Runnable // 本实例专属：newLoop(cfg) + llmRunner / workflow 编排器

    mu       sync.Mutex
    busy     bool // 规则 R1：同一时刻至多一个 run 在使用本实例
    closed   bool
    cleanup  []func(context.Context) error
}

func (a *Agent) NewRuntime() (*Runtime, error) {
    r, err := a.buildLoopAndRunnable()
    if err != nil {
        return nil, err
    }
    return &Runtime{agent: a, runnable: r}, nil
}

// Run 用本实例执行一次 run。Run/Stream 返回的句柄绑定了本 Runtime 的 runnable；
// 句柄执行结束（run 的驱动 goroutine 退出）后才可再次使用或归还（规则 R1、R3）。
func (rt *Runtime) Stream(ctx context.Context, input string, opts ...RunOption) *Run {
    rt.mu.Lock()
    if rt.closed || rt.busy {
        rt.mu.Unlock()
        return failedRun(ctx, errors.New("agent: runtime is busy or closed"))
    }
    rt.busy = true
    rt.mu.Unlock()
    // 复用现有 Stream 的校验/冻结逻辑（agent/agent.go:136-152），
    // newRunHandle 里 runnable 换成 rt.runnable，bus/store 仍取 rt.agent 的。
    run := rt.agent.newRunHandleWithRunnable(ctx, rt)
    run.onFinished = rt.release // 见 §5 规则 R3 的归还时机
    return run
}

func (rt *Runtime) release(ctx context.Context) {
    rt.mu.Lock()
    rt.busy = false
    rt.mu.Unlock()
    if rt.agent.pool != nil {
        _ = rt.agent.pool.release(ctx, rt)
    }
}

func (rt *Runtime) Close(ctx context.Context) error { /* 遍历 cleanup，幂等 */ }
```

不删除 `Run` 上任何现有能力：`Iter/Wait/Steer/Cancel/Decide` 契约不变。

### 4.3 RuntimePool

只池化"§4.2 里那个无 thread 状态的执行实例"。核心不变量：

- I1：池中 Runtime 一定 `busy == false` 且未关闭；
- I2：总数对账在**同一把锁内**完成（修正初版 TOCTOU：它先读 `len(allRuntimes)` 再锁外判断，
  两个 goroutine 可同时通过，突破 MaxWorkers）；
- I3：`Close` 不得从已关闭的 channel 取值（初版 `close(p.available)` 后 `range` 会拿到 nil
  指针并在 `rt.Close` 上空指针 panic）。

```go
type runtimePoolImpl struct {
    agent *Agent
    cfg   RuntimePoolConfig

    mu        sync.Mutex
    idle      []*Runtime // 用切片而非 buffered channel 做空闲表，计数与内容天然一致
    total     int        // 已创建且未销毁的实例数（含借出）
    closed    bool
    waiters   chan struct{} // 容量 1 的唤醒信号
}

func (p *runtimePoolImpl) acquire(ctx context.Context) (*Runtime, error) {
    for {
        p.mu.Lock()
        if p.closed {
            p.mu.Unlock()
            return nil, errors.New("agent: runtime pool is closed")
        }
        if n := len(p.idle); n > 0 {
            rt := p.idle[n-1]
            p.idle = p.idle[:n-1]
            p.mu.Unlock()
            // 借出即置 busy，池与 Runtime 两处状态在交接点一次完成
            rt.mu.Lock()
            rt.busy = true
            rt.mu.Unlock()
            return rt, nil
        }
        if p.total < p.cfg.MaxWorkers {
            p.total++ // 名额在同一把锁内预占（I2）
            p.mu.Unlock()
            rt, err := p.agent.NewRuntime() // ResetFunc 失败即销毁并 p.total-- 后重试
            if err != nil {
                p.mu.Lock()
                p.total--
                p.mu.Unlock()
                return nil, err
            }
            return rt, nil // NewRuntime 产物天然 busy（调用方即借出方）
        }
        p.mu.Unlock()
        // 满员：等 release 唤醒或 ctx 到期
        select {
        case <-p.waiters:
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
}

func (p *runtimePoolImpl) release(ctx context.Context, rt *Runtime) error {
    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return rt.Close(ctx)
    }
    if len(p.idle) < p.cfg.MaxWorkers {
        p.idle = append(p.idle, rt)
        p.mu.Unlock()
        select {
        case p.waiters <- struct{}{}:
        default:
        }
        return nil
    }
    // 理论不可达：idle 表与 total 在同一把锁内对账。留作兜底：超容即销毁并扣减 total。
    p.total--
    p.mu.Unlock()
    return rt.Close(ctx)
}

func (p *runtimePoolImpl) close(ctx context.Context) error {
    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return nil
    }
    p.closed = true
    idle := p.idle
    p.idle = nil
    p.mu.Unlock()
    // 只关池中实例；借出未还的由各自的 release→Close 路径处理（不 range 已关闭 channel，I3）
    var errs []error
    for _, rt := range idle {
        if err := rt.Close(ctx); err != nil {
            errs = append(errs, err)
        }
    }
    return errors.Join(errs...)
}
```

**ResetFunc 语义（初版完全未定义，这里定稿为 v1 约束而非钩子）**：
被池复用的 Runtime 必须无残留状态。按 §3 的划分，Runtime 只持 loop/runnable，
loop 构造后只读、`RunContext.dynamic` 每 run 新建（agent/runtime.go:46-49），
所以 **v1 不需要 Reset 逻辑**——前提是把"Runtime 不得持有 thread 作用域状态"作为硬性约束
写进类型注释并用测试钉住。若未来 middleware 实例被证明需要 run 间复位，再单独设计 reset，
届时必须明确定义：store 里的 checkpoint 不归 Runtime 管（不属于 reset 范围）、
bus 上的既有订阅者不受影响（`Run.Iter` 的 `Subscribe/Lossless`，agent/run.go:52）。

## 5. 生命周期规则（补初版最大的设计缺口）

初版在 `Run.finish` 里自动 `Cleanup→ReleaseRuntime`，两条实现都错：若 Close 掉 Runtime，
调用方随后在同一 rt 上起的 Run 会静默失败；若不 Close 直接入池，而 run 的驱动 goroutine
还在 loop 里，池可能把它借给第二个 run，违反 R1。规则定稿：

- **R1（单飞）**：一个 Runtime 同一时刻至多服务一个在飞 run。`busy` 标志在 acquire/release
  交接点维护。注意 `Stream` 是惰性 drive 的（agent/agent.go:133-136 注释），
  acquire 成功 ≠ drive 开始，但 busy 从 acquire 起生效。
- **R2（默认路径）**：未启用池时，`a.Run/Stream/Resume` 每次现造一个即用即弃的 Runtime
  （构造成本见 §1 第 2 条，是否可接受由 Phase 2 基准回答）。启用池时经 acquire/release。
- **R3（归还时机）**：归还点在 **run 的驱动 goroutine 退出处**，不是 `finish()`。
  `finish` 可能由事件消费者 `capture()` 触发（agent/run.go:180-194），而此刻 `drive()` 起的
  goroutine 可能仍在 `runnable.run` 内（它发布终态事件后还要走 §4.2 的 release 路径），
  在 finish 归还就是允许第二个 run 与第一个并发共用同一实例。具体做法：在 drive goroutine
  的 defer 里归还。
- **R4（Stream 句柄）**：`*Run` 归还 Runtime 后仍完全可用——事件已经按 topic 发到共享 bus，
  `Wait/Iter/Steer/Decide` 不依赖 Runtime。修订初版"Run 负责释放 Runtime 生命周期"的表述：
  Run 只负责在**自己的驱动 goroutine 结束时**触发归还，句柄本身不持 Runtime 引用。
- **R5（Resume）**：`a.Resume` 同样经 acquire 取 Runtime；由于连续性全在 store（§3），
  resume 落在与原 run 不同的 Runtime 上是安全的。workflow agent 无 loop 的 resume 分支
  （agent/hitl.go:162-172）不受影响——它走的是 run 作用域路径，与实例归属无关。

## 6. 并发使用矩阵（修订版，替代初版"场景对比"）

```go
// ✅ 安全：Agent 并发 Run（每个 run 各自 acquire 一个实例）
for i := 0; i < 100; i++ {
    go func(q string) { _, _ = a.Run(ctx, q) }(q)
}

// ✅ 安全：显式持有 Runtime，串行多次执行
rt, _ := a.NewRuntime()
defer rt.Close(ctx)
rt.Run(ctx, "Q1")
rt.Run(ctx, "Q2")

// ❌ 违规：同一 Runtime 并发使用——R1 拦截，Stream 返回明确错误而非静默竞态
go rt.Stream(ctx, "Q1") // busy=true
rt.Stream(ctx, "Q2")    // 返回 "runtime is busy"

// ✅ 并发要用不同实例（或直接用池化 Agent 路径）
rt1, _ := a.NewRuntime()
rt2, _ := a.NewRuntime()
go rt1.Run(ctx, "Q1")
go rt2.Run(ctx, "Q2")
```

注意第一例：并发安全是**现状就有**的能力（§1），本方案只是把它从"依赖 loop 只读性质"
升级为"有明确单飞/归属规则"。

## 7. 与初版的对照（审核发现处置表）

| 初版问题 | 处置 |
|---|---|
| 立论"消除隐蔽竞态"，经核实不成立 | 已删；收益改为 §1 三条 |
| Runtime 与 RunContext 概念关系未提 | 已补 §2 |
| bus/store 隔离声明与默认共享矛盾；独立 store 破坏 thread 连续性 | 已定稿 §3 所有权表 |
| workflow agent（loop nil）未处理 | 已补 §4.1 `buildLoopAndRunnable` 分支与构造路径盘点 |
| `WithRuntimePool(a, ...)` 示例编译不过 | 已改为 `RuntimePoolConfig` Option，New 内部构造 |
| `requireThreadCoordination` 校验被挪到运行期 | 已留回 `New`（§4.1） |
| `Run.finish` 自动归还导致并发共用 | 已定稿 R1/R3（§5） |
| Resume/Stream 句柄与归还的关系缺失 | 已补 R4/R5 |
| 池 Acquire TOCTOU、Close 空指针 panic、超容静默丢弃 | 已重写为单锁对账 + 切片空闲表（§4.3） |
| ResetFunc 语义未定义 | 定稿为"v1 无状态约束，不设 reset 钩子"（§4.3） |
| 性能表数字（-0.2%/+0.009%）无来源 | 已删；改为 Phase 门槛（§8） |
| `Agent.Run` 示例错误路径处理零值 res | 便捷方法保持现实现，不再给示例 |
| 迁移路径/对标 Eino 章节重复两遍、```json 包裹表格、对话式残留 | 已合并/删除 |

## 8. 实施阶段与门槛

- **Phase 0（先做，可能终结后续）**：写并发基准（`go test -race -bench`：并发 `a.Run`、
  单 Agent 复用、1000 次顺序 run 的 `newLoop` 构造成本）。两个判据：
  ① 若现状并发已正确且 `newLoop` 成本占比可忽略，池化收益仅剩 API 形态，可考虑不做；
  ② 若 middleware 被证实需要每实例状态，才推进 Phase 1。判据数字**待实测**，不预设。
- **Phase 1（基础分离，不破坏 API）**：`Runtime` 类型 + `NewRuntime()` +
  `newRunHandleWithRunnable`；`Agent.Run/Stream/Resume` 内部走即用即弃 Runtime；
  workflow/AsTool/plan executor 构造点全部盘点；`requireThreadCoordination` 等校验留在 `New`。
  验收：现有 `agent` 包全量测试绿（含 `TestConcurrentResumeSingleClaim`、
  `TestInjectionDoesNotLeakAcrossRuns`）+ 新增 R1/R3 的竞态测试（`-race`）。
- **Phase 2（显式生命周期）**：`Runtime.Close`、`OnCleanup`、`Agent.Shutdown`；
  R4 的"句柄独立于实例"用测试钉住（归还后继续 `Iter/Wait`）。
- **Phase 3（池，仅当 Phase 0 判据 ② 成立）**：`WithRuntimePool`、§4.3 acquire/release；
  满员阻塞语义、ctx 取消路径、跨进程说明（池不解决多进程，那是 Durable/Claim 的领域）。
- **Phase 4（文档）**：迁移指南 + 并发场景示例（`examples/` 按项目惯例补一个池模式示例）。

## 9. 与 Eino 的对照（压缩为定性一段）

Eino 将配置（`Agent`）与执行包装配（`Runner`）分离，并发靠会话 id 隔离；本方案对齐同一思路：
`Runtime` 承担 Runner 的角色，差异在于 goagent 的便捷方法可完全向后兼容（Agent 路径自动管理
实例），且池化是内置可选层。注意：本方案**不宣称**比现状更安全（§1），只提供更明确的实例
生命周期与资源边界。

---

## 附：本修订版刻意不做的事

1. 不把 `RunContext` 改名 `Runtime`（形态不符 + 撞名 + breaking，理由见 §2）。
2. 不提供 per-Runtime 的 bus/store 覆盖选项（§3）。
3. 不在文档里给未经基准支撑的性能数字。
4. 不在 v1 引入 ResetFunc 钩子（先以"实例无 thread 状态"约束替代，§4.3）。
