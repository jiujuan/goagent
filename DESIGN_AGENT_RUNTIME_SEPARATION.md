# goagent: Agent + Runtime 分离设计方案

## 概述

本方案提出将 goagent 的 `Agent = Runtime` 模型改造为 `Agent + Runtime` 的分离架构，学习 Eino 的成熟设计模式。

**核心目标**：
- ✅ 解决隐蔽的并发竞态问题
- ✅ 支持真正的并发多运行
- ✅ 保持 API 向后兼容
- ✅ 性能无损（甚至高并发场景更优）

---

## 现状分析

### 当前架构的问题

```go
// agent/agent.go - 当前设计
type Agent struct {
    cfg      config                      // ✅ 配置（不可变）
    loop     *AgentLoop                  // ❌ 运行时（可被并发修改）
    runnable Runnable                    // ❌ 运行时（执行器）
    bus      *bus.Bus                    // ❌ 运行时（事件发布）
    store    checkpoint.Checkpointer     // ❌ 运行时（状态存储）
    parent   *Agent                      // ❌ 多Agent时的引用
}

// 使用：所有状态混杂在一个对象里
a := agent.New(...)
a.Run(ctx, "Q1")    // 写 loop、bus、store
a.Run(ctx, "Q2")    // 同时写 loop、bus、store
// 问题：并发时的竞态条件
```

### 隐蔽的竞态风险

1. **AgentLoop 并发修改**（agent/loop.go）
   ```go
   type AgentLoop struct {
       tools       []tool.Tool        // ❌ 可被并发读写
       schemas     []llm.ToolSchema   // ❌ 动态添加时竞态
       seqTool     map[string]bool    // ❌ 无锁保护
   }
   ```

2. **动态工具注入竞态**（agent/runtime.go）
   ```go
   type dynamicTools struct {
       mu     sync.Mutex
       byName map[string]tool.Tool   // ✅ 有锁
   }
   // 但 loop.tools 和 loop.schemas 无锁
   ```

3. **事件总线并发**（bus/bus.go）
   - 使用通道，应该线程安全
   - 但多个运行共享一个 Bus 可能导致事件混乱

4. **检查点并发**（checkpoint/checkpoint.go）
   - 不同 ThreadID 理论上隔离
   - 但同 ThreadID 的并发访问无保护

---

## 分离方案设计

### 三层架构

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: Agent（不可变配置）                          │
│  ├─ 创建：agent.New(...)                               │
│  ├─ 职责：仅存储配置，无运行时状态                      │
│  └─ 特性：可跨多个运行复用、完全无状态                  │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Runtime（运行时管理器）                      │
│  ├─ 创建：a.NewRuntime() 或 a.AcquireRuntime()        │
│  ├─ 职责：管理单次执行的完整生命周期                    │
│  ├─ 包含：loop、bus、store（独立副本）                │
│  └─ 特性：可并发创建，相互隔离                         │
├─────────────────────────────────────────────────────────┤
│  Layer 3: RuntimePool（资源池）                        │
│  ├─ 创建：自动（当启用池模式时）                       │
│  ├─ 职责：管理 Runtime 对象的生命周期                  │
│  ├─ 特性：可选，对开发者完全透明                       │
│  └─ 模式：Worker Pool 模式                            │
└─────────────────────────────────────────────────────────┘
```

### 核心代码结构

#### 1. Agent（新）- 纯配置容器

```go
// agent/agent.go
package agent

import (
    "context"
    "errors"
    
    "github.com/jiujuan/goagent/bus"
    "github.com/jiujuan/goagent/checkpoint"
    "github.com/jiujuan/goagent/core"
)

// Agent is now ONLY a configuration holder.
// It is stateless and can be safely shared and reused.
type Agent struct {
    cfg    config           // ✅ 不可变配置
    parent *Agent           // 多Agent时的关系
    // ❌ 移除所有运行时状态：loop, bus, store, runnable
}

// New builds an Agent (configuration only).
// No runtime state is created at this point.
func New(opts ...Option) (*Agent, error) {
    c := config{maxTurns: defaultMaxTurns}
    for _, o := range opts {
        o(&c)
    }
    if c.model == nil {
        return nil, errors.New("agent: WithModel is required")
    }

    a := &Agent{cfg: c}
    // Wire the delegation tree: become the parent of each sub-agent
    for _, s := range c.subAgents {
        s.parent = a
    }
    return a, nil
}

// Name reports the configured name.
func (a *Agent) Name() string {
    return a.cfg.name
}

// Config returns the agent's configuration (read-only).
func (a *Agent) Config() config {
    return a.cfg
}

// NewRuntime creates a new independent Runtime for this Agent.
// Each Runtime has its own loop, bus, store, etc.
func (a *Agent) NewRuntime(opts ...RuntimeOption) (*Runtime, error) {
    return NewRuntime(a, opts...)
}

// AcquireRuntime acquires a Runtime from the internal pool.
// If no pool is configured, creates and returns a new Runtime.
func (a *Agent) AcquireRuntime(ctx context.Context, opts ...RuntimeOption) (*Runtime, error) {
    // 如果启用了 Pool，从池获取；否则创建新的
    if a.cfg.runtimePool != nil {
        return a.cfg.runtimePool.Acquire(ctx)
    }
    return a.NewRuntime(opts...)
}

// ReleaseRuntime returns a Runtime to the pool (if pooling is enabled).
func (a *Agent) ReleaseRuntime(ctx context.Context, rt *Runtime) error {
    if a.cfg.runtimePool != nil {
        return a.cfg.runtimePool.Release(ctx, rt)
    }
    // If no pool, caller is responsible for cleanup
    return rt.Close(ctx)
}

// Run is a convenience method that acquires a Runtime, executes, and releases it.
// Maintains backward compatibility with the old API.
func (a *Agent) Run(ctx context.Context, input string, opts ...RunOption) (string, error) {
    rt, err := a.AcquireRuntime(ctx)
    if err != nil {
        return "", err
    }
    defer func() { _ = a.ReleaseRuntime(ctx, rt) }()

    res, err := rt.Run(ctx, input, convertRunOpts(opts)...)
    return res.Message.Text(), err
}

// Stream is a convenience method that acquires a Runtime and starts streaming.
// The returned Run handle manages the Runtime's lifecycle.
func (a *Agent) Stream(ctx context.Context, input string, opts ...RunOption) *Run {
    rt, err := a.AcquireRuntime(ctx)
    if err != nil {
        // Return a failed Run
        return newFailedRun(err)
    }
    // Run is responsible for releasing the Runtime
    return rt.Stream(ctx, input, convertRunOpts(opts)...)
}
```

#### 2. Runtime（新）- 独立运行时

```go
// agent/runtime.go（新增）
package agent

import (
    "context"
    "sync"
    "time"

    "github.com/jiujuan/goagent/bus"
    "github.com/jiujuan/goagent/checkpoint"
    "github.com/jiujuan/goagent/core"
)

// Runtime represents a single independent execution environment.
// Each Runtime has its own:
// - LLM loop (AgentLoop)
// - Event bus (Bus)
// - Checkpoint store (Checkpointer)
// - Runnable executor (Runnable)
//
// Multiple Runtimes can execute concurrently without interfering with each other.
type Runtime struct {
    // Configuration (shared, immutable)
    agent *Agent

    // Runtime state (independent per Runtime)
    loop     *AgentLoop
    runnable Runnable
    bus      *bus.Bus
    store    checkpoint.Checkpointer

    // Lifecycle management
    mu       sync.Mutex
    closed   bool
    cleanupFuncs []func(context.Context) error
}

// RuntimeOption configures a Runtime.
type RuntimeOption func(*Runtime) error

// WithBus overrides the event bus for this Runtime.
func WithBus(b *bus.Bus) RuntimeOption {
    return func(rt *Runtime) error {
        rt.bus = b
        return nil
    }
}

// WithStore overrides the checkpoint store for this Runtime.
func WithStore(s checkpoint.Checkpointer) RuntimeOption {
    return func(rt *Runtime) error {
        rt.store = s
        return nil
    }
}

// NewRuntime creates a new independent Runtime.
func NewRuntime(a *Agent, opts ...RuntimeOption) (*Runtime, error) {
    cfg := a.cfg

    // Create independent instances for this Runtime
    b := cfg.bus
    if b == nil {
        b = bus.New()
    }

    s := cfg.store
    if s == nil {
        s = checkpoint.NewMemory()
    }

    rt := &Runtime{
        agent:   a,
        bus:     b,
        store:   s,
        loop:    newLoop(cfg),
    }

    // Wire the transfer tool for multi-agent
    if tt := transferToolFor(a.transferTargets()); tt != nil {
        rt.loop.addTool(tt)
    }

    // Apply options
    for _, opt := range opts {
        if err := opt(rt); err != nil {
            return nil, err
        }
    }

    rt.runnable = &llmRunner{agent: a, loop: rt.loop}

    return rt, nil
}

// Bus returns this Runtime's event bus.
func (rt *Runtime) Bus() *bus.Bus {
    return rt.bus
}

// Store returns this Runtime's checkpoint store.
func (rt *Runtime) Store() checkpoint.Checkpointer {
    return rt.store
}

// Run executes the Agent on a single input and returns the result.
// This is the synchronous entry point.
func (rt *Runtime) Run(ctx context.Context, input string, opts ...RunOption) (core.Result, error) {
    res, err := rt.Stream(ctx, input, opts...).Wait()
    return res, err
}

// Stream starts an execution and returns a non-blocking Run handle.
func (rt *Runtime) Stream(ctx context.Context, input string, opts ...RunOption) *Run {
    rc := RunConfig{ThreadID: core.NewID("thread"), Message: core.UserText(input)}
    for _, o := range opts {
        o(&rc)
    }
    
    // Validation and message freezing
    err := core.CheckThreadID(rc.ThreadID)
    message := core.Message{}
    if err == nil {
        message, err = core.CloneMessage(rc.Message)
    }

    run := rt.newRunHandle(ctx, rc.ThreadID, &core.State{})
    run.startErr = err
    run.preparation = &runPreparation{message: message, files: rc.Files, restoreCurrent: true}
    run.runtime = rt  // ← 绑定 Runtime
    return run
}

// Close releases resources held by this Runtime.
func (rt *Runtime) Close(ctx context.Context) error {
    rt.mu.Lock()
    defer rt.mu.Unlock()

    if rt.closed {
        return nil
    }
    rt.closed = true

    var errs []error
    for _, fn := range rt.cleanupFuncs {
        if err := fn(ctx); err != nil {
            errs = append(errs, err)
        }
    }

    if len(errs) > 0 {
        return errors.Join(errs...)
    }
    return nil
}

// OnCleanup registers a cleanup function to be called when this Runtime is closed.
func (rt *Runtime) OnCleanup(fn func(context.Context) error) {
    rt.mu.Lock()
    defer rt.mu.Unlock()
    rt.cleanupFuncs = append(rt.cleanupFuncs, fn)
}

// IsClosed returns true if this Runtime has been closed.
func (rt *Runtime) IsClosed() bool {
    rt.mu.Lock()
    defer rt.mu.Unlock()
    return rt.closed
}

// newRunHandle creates a new Run for this Runtime.
func (rt *Runtime) newRunHandle(ctx context.Context, threadID string, state *core.State) *Run {
    runCtx, cancel := context.WithCancel(ctx)
    runID := core.NewID("run")
    topic := bus.Topic(runID)

    rc := &RunContext{
        Context:  runCtx,
        RunID:    runID,
        ThreadID: threadID,
        Bus:      rt.bus,      // ← 使用 Runtime 的独立 Bus
        Topic:    topic,
        Store:    rt.store,    // ← 使用 Runtime 的独立 Store
        State:    state,
        durable:  newDurableRunState(),
    }

    return &Run{
        ID:       runID,
        ThreadID: threadID,
        agent:    rt.agent,
        runtime:  rt,          // ← 绑定 Runtime
        bus:      rt.bus,
        topic:    topic,
        rc:       rc,
        runnable: rt.runnable,
        cancel:   cancel,
        done:     make(chan struct{}),
    }
}
```

#### 3. RuntimePool - 对象池

```go
// agent/runtime_pool.go（新增）
package agent

import (
    "context"
    "errors"
    "fmt"
    "sync"
    "time"
)

// RuntimePoolConfig configures a RuntimePool.
type RuntimePoolConfig struct {
    // MaxWorkers is the maximum number of Runtimes in the pool.
    MaxWorkers int

    // MinWorkers is the minimum number of pre-created Runtimes.
    MinWorkers int

    // IdleTimeout is how long a Runtime can be idle before being recycled.
    IdleTimeout time.Duration

    // FactoryFunc creates new Runtime instances.
    // If nil, defaults to creating via Agent.NewRuntime()
    FactoryFunc func(*Agent) (*Runtime, error)

    // ResetFunc resets a Runtime for reuse.
    // If nil, defaults to a no-op.
    ResetFunc func(*Runtime) error
}

// DefaultRuntimePoolConfig returns sensible defaults.
func DefaultRuntimePoolConfig() RuntimePoolConfig {
    return RuntimePoolConfig{
        MaxWorkers:  10,
        MinWorkers:  2,
        IdleTimeout: 5 * time.Minute,
    }
}

// RuntimePool manages a pool of independent Runtime objects.
// It enables efficient reuse of expensive Runtime allocations.
type RuntimePool struct {
    agent *Agent
    cfg   RuntimePoolConfig

    mu        sync.Mutex
    available chan *Runtime
    closed    bool
    allRuntimes []*Runtime
}

// NewRuntimePool creates a new RuntimePool.
func NewRuntimePool(agent *Agent, cfg RuntimePoolConfig) (*RuntimePool, error) {
    if cfg.MaxWorkers <= 0 {
        return nil, errors.New("agent: MaxWorkers must be > 0")
    }
    if cfg.MinWorkers > cfg.MaxWorkers {
        return nil, errors.New("agent: MinWorkers must be <= MaxWorkers")
    }
    if cfg.FactoryFunc == nil {
        cfg.FactoryFunc = func(a *Agent) (*Runtime, error) {
            return a.NewRuntime()
        }
    }

    pool := &RuntimePool{
        agent:       agent,
        cfg:         cfg,
        available:   make(chan *Runtime, cfg.MaxWorkers),
        allRuntimes: make([]*Runtime, 0, cfg.MaxWorkers),
    }

    // Pre-create MinWorkers
    for i := 0; i < cfg.MinWorkers; i++ {
        rt, err := cfg.FactoryFunc(agent)
        if err != nil {
            pool.Close(context.Background())
            return nil, fmt.Errorf("agent: failed to create runtime %d: %w", i, err)
        }
        pool.allRuntimes = append(pool.allRuntimes, rt)
        pool.available <- rt
    }

    return pool, nil
}

// Acquire gets a Runtime from the pool.
// If no Runtime is available, creates a new one (up to MaxWorkers).
// If MaxWorkers is reached and no Runtime is available, blocks until one is released.
func (p *RuntimePool) Acquire(ctx context.Context) (*Runtime, error) {
    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return nil, errors.New("agent: runtime pool is closed")
    }
    poolSize := len(p.allRuntimes)
    p.mu.Unlock()

    // Try to get from available pool
    select {
    case rt := <-p.available:
        // Reset the Runtime for reuse
        if p.cfg.ResetFunc != nil {
            if err := p.cfg.ResetFunc(rt); err != nil {
                // If reset fails, create a new one
                return p.cfg.FactoryFunc(p.agent)
            }
        }
        return rt, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    // No available Runtime, create a new one if under MaxWorkers
    if poolSize < p.cfg.MaxWorkers {
        p.mu.Lock()
        if len(p.allRuntimes) < p.cfg.MaxWorkers {
            p.mu.Unlock()
            rt, err := p.cfg.FactoryFunc(p.agent)
            if err != nil {
                return nil, err
            }
            p.mu.Lock()
            p.allRuntimes = append(p.allRuntimes, rt)
            p.mu.Unlock()
            return rt, nil
        }
        p.mu.Unlock()
    }

    // MaxWorkers reached, wait for available or context done
    select {
    case rt := <-p.available:
        if p.cfg.ResetFunc != nil {
            if err := p.cfg.ResetFunc(rt); err != nil {
                return p.cfg.FactoryFunc(p.agent)
            }
        }
        return rt, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

// Release returns a Runtime to the pool.
func (p *RuntimePool) Release(ctx context.Context, rt *Runtime) error {
    if rt == nil {
        return nil
    }

    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return rt.Close(ctx)
    }
    p.mu.Unlock()

    select {
    case p.available <- rt:
        return nil
    case <-ctx.Done():
        return rt.Close(ctx)
    default:
        // Pool is full, close this Runtime
        return rt.Close(ctx)
    }
}

// Close closes all Runtimes in the pool.
func (p *RuntimePool) Close(ctx context.Context) error {
    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return nil
    }
    p.closed = true

    // Drain available channel
    close(p.available)
    for rt := range p.available {
        _ = rt.Close(ctx)
    }

    // Close all tracked Runtimes
    rts := p.allRuntimes
    p.allRuntimes = nil
    p.mu.Unlock()

    var errs []error
    for _, rt := range rts {
        if err := rt.Close(ctx); err != nil {
            errs = append(errs, err)
        }
    }
    if len(errs) > 0 {
        return errors.Join(errs...)
    }
    return nil
}

// Stats returns pool statistics.
type PoolStats struct {
    TotalRuntimes  int
    AvailableCount int
    ActiveCount    int
}

func (p *RuntimePool) Stats() PoolStats {
    p.mu.Lock()
    defer p.mu.Unlock()

    totalCount := len(p.allRuntimes)
    availCount := len(p.available)

    return PoolStats{
        TotalRuntimes:  totalCount,
        AvailableCount: availCount,
        ActiveCount:    totalCount - availCount,
    }
}
```

#### 4. Run 的修改 - 管理 Runtime 生命周期

```go
// agent/run.go（修改）
package agent

import (
    "context"
    "errors"
    "sync"

    "github.com/jiujuan/goagent/bus"
    "github.com/jiujuan/goagent/core"
)

type Run struct {
    ID       string
    ThreadID string

    agent    *Agent
    runtime  *Runtime        // ← 新增：绑定的 Runtime
    bus      *bus.Bus
    topic    bus.Topic
    rc       *RunContext
    runnable Runnable
    cancel   context.CancelFunc
    startErr error
    preparation *runPreparation

    startOnce sync.Once
    doneOnce  sync.Once
    done      chan struct{}
    result    core.Result
    err       error

    mu        sync.Mutex
    decisions []Approval

    lifecycleMu  sync.Mutex
    driveStarted bool
    cancelErr    error
}

// ... 现有方法 ...

// Cleanup releases the Runtime when the Run completes.
func (r *Run) Cleanup(ctx context.Context) error {
    if r.runtime == nil {
        return nil
    }
    // Return Runtime to pool or close it
    return r.agent.ReleaseRuntime(ctx, r.runtime)
}

// 在 finish 中自动调用 Cleanup
func (r *Run) finish(res core.Result, err error) {
    r.doneOnce.Do(func() {
        r.result, r.err = res, err
        
        // Cleanup Runtime
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = r.Cleanup(ctx)
        
        close(r.done)
    })
}
```

---

## 使用方式

### 方式 1：向后兼容（推荐迁移）

```go
// 旧代码无需修改
a := agent.New(
    agent.WithModel(model),
    agent.WithTools(tool1, tool2),
)

// API 完全相同
answer, err := a.Run(ctx, "What is 2+2?")

// 内部自动：
// 1. 从池获取/创建 Runtime
// 2. 执行
// 3. 归还/关闭 Runtime
```

**优点**：零迁移成本，代码完全兼容

### 方式 2：手动管理（精细控制）

```go
// 为并发或长期运行场景设计
rt, err := a.NewRuntime()
if err != nil {
    log.Fatal(err)
}
defer rt.Close(ctx)

// 使用这个 Runtime 执行多次
result1, _ := rt.Run(ctx, "Q1")
result2, _ := rt.Run(ctx, "Q2")
```

**优点**：Runtime 生命周期由开发者控制

### 方式 3：池模式（高并发）

```go
// 启用池模式
poolCfg := RuntimePoolConfig{
    MaxWorkers: 50,
    MinWorkers: 10,
}

a := agent.New(
    agent.WithModel(model),
    agent.WithRuntimePool(agent.NewRuntimePool(a, poolCfg)),
)

// 并发安全执行
for i := 0; i < 1000; i++ {
    go func(q string) {
        answer, _ := a.Run(ctx, q)  // 自动从池获取
        // Runtime 自动归还到池
    }(fmt.Sprintf("Question %d", i))
}
```

**优点**：最高效率，自动资源管理

### 方式 4：Stream + 手动 Runtime

```go
// 需要长期持有事件流
rt := a.NewRuntime()
defer rt.Close(ctx)

run := rt.Stream(ctx, "Complex query")

// 观察事件
for ev, err := range run.Iter() {
    if err != nil {
        log.Fatal(err)
    }
    switch e := ev.(type) {
    case core.MessageDone:
        fmt.Println("Message:", e.Message.Text())
    case core.ToolDone:
        fmt.Println("Tool:", e.Result.Name)
    }
}
```

**优点**：完全控制事件处理和 Runtime 生命周期

---

## 并发安全性分析

### 场景 1：同一 Agent，并发 Run（通过池）

```go
a := agent.New(agent.WithModel(model))

// ✅ 完全安全 - 每个 Run 有独立的 Runtime
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(q string) {
        defer wg.Done()
        _, _ = a.Run(ctx, q)
    }(fmt.Sprintf("Q%d", i))
}
wg.Wait()

// 内部：
// - 每个 Run 获取独立 Runtime
// - Runtime.bus、Runtime.loop、Runtime.store 都独立
// - 无竞态
```

### 场景 2：多 Runtime，手动管理

```go
rt1, _ := a.NewRuntime()
rt2, _ := a.NewRuntime()
defer rt1.Close(ctx)
defer rt2.Close(ctx)

// ✅ 安全 - 完全隔离
go rt1.Run(ctx, "Q1")
go rt2.Run(ctx, "Q2")
```

### 场景 3：同 Runtime，串行使用

```go
rt, _ := a.NewRuntime()
defer rt.Close(ctx)

// ✅ 安全 - 串行调用
rt.Run(ctx, "Q1")
rt.Run(ctx, "Q2")  // 在 Q1 完成后
```

### 场景 4：同 Runtime，并发使用

```go
rt, _ := a.NewRuntime()
defer rt.Close(ctx)

// ❌ 不安全 - 同一 Runtime 被并发使用
go rt.Run(ctx, "Q1")
go rt.Run(ctx, "Q2")  // 竞态！
```

**规则**：每个 Runtime 内部是串行的，但多个 Runtime 可并发。

---

## 性能影响

### 基准测试预测

```
场景：单 Agent，N次顺序 Run

当前设计（复用 Runtime）：
  T(单次Run) = 5ms (验证+准备) + 1000ms (LLM) + 100ms (工具) + 10ms (检查点)
  T(N次) = N * 1115ms

分离设计（无池）：
  T(单次Run) = 2ms (Runtime创建) + 5ms (验证) + 1000ms (LLM) + 100ms + 10ms
  T(N次) = N * 1117ms (额外2ms)
  开销: +0.18%

分离设计（带池，N=10）：
  T(单次Run) = 0.1ms (从池获取) + 5ms (验证) + 1000ms (LLM) + 100ms + 10ms
  T(N次) = N * 1115.1ms
  开销: +0.009%
  
分离设计（并发场景，20个并发）：
  当前设计: 竞态（不可用）
  分离设计: 完全隔离，每个Runtime独立执行
  吞吐: 提升 ~20倍（从串行到并行）
```

### 内存影响

```
当前设计（单 Agent）：
  Agent 大小: ~5KB (config + 几个指针)
  (loop, bus, store 都内联在 Agent 里)

分离设计（单 Agent）：
  Agent 大小: ~2KB (仅 config，更小)
  Runtime 大小: ~3KB (per-runtime)
  
场景 1：单 Agent + 单 Runtime
  内存：总共 ~5KB（与当前相同）
  
场景 2：单 Agent + 20 个并发 Runtime（无池）
  内存：2KB + 20*3KB = 62KB（增加 ~60KB，用于隔离）
  
场景 3：单 Agent + 池（最大20个 Runtime）
  内存：2KB + 20*3KB = 62KB（预分配，稳定）
  相比创建100个Runtime，节省显著
```

---

## 迁移路径

### Phase 1: 基础分离（不破坏 API）

1. 创建 `Runtime` 结构体
2. 将 `loop`, `bus`, `store`, `runnable` 移到 `Runtime`
3. `Agent.Run/Stream` 内部创建临时 `Runtime`
4. 所有功能保持不变
5. **成果**：隐蔽竞态被消除，API 不变

### Phase 2: 暴露 Runtime API（可选）

1. 添加 `Agent.NewRuntime()`, `Agent.AcquireRuntime()`, `Agent.ReleaseRuntime()`
2. 高级用户可手动管理 Runtime
3. 留下向后兼容的便捷方法
4. **成果**：用户可精细控制，性能可调优

### Phase 3: 池模式（可选）

1. 实现 `RuntimePool`
2. 添加 `agent.WithRuntimePool()` 选项
3. 自动配置和管理池
4. **成果**：高并发场景性能最优

### Phase 4: 文档和示例

1. 编写迁移指南
2. 新增并发场景示例
3. 发布最佳实践
4. **成果**：生态完整

---

## 与 Eino 的对标

```
┌──────────────────────┬──────────────────────┬──────────────────────┐
│      维度            │    Eino              │  goagent (改后)      │
├──────────────────────┼──────────────────────┼──────────────────────┤
│ Agent 职责           │ 配置容器             │ 配置容器              │
│ Runner/Runtime       │ RunnerConfig         │ Runtime + RuntimePool│
│ 并发隔离             │ sessionID            │ Runtime 独立副本     │
│ 会话管理             │ SessionStore         │ checkpoint.Store     │
│ API 简洁性           │ ⭐⭐ (需创建Runner) │ ⭐⭐⭐ (自动管理)    │
│ 高级用户控制         │ ✅ 支持              │ ✅ 支持              │
│ 资源池支持           │ 需自建               │ 内置 RuntimePool     │
│ 向后兼容性           │ 否 (新框架)          │ ✅ 完全兼容          │
└──────────────────────┴──────────────────────┴──────────────────────┘
```

---

## 总结

### 核心改进

✅ **并发安全性**
- 消除隐蔽竞态
- 每个运行完全隔离
- 支持安全的并发执行

✅ **灵活性**
- 三层 API：便捷、手动、池模式
- 用户可按需选择

✅ **性能**
- 零额外开销（或通过池优化）
- 高并发场景吞吐提升显著

✅ **兼容性**
- 完全向后兼容
- 无需修改现有代码

✅ **生态对齐**
- 与 Eino 架构思路一致
- 便于互操作和组合

### 实施建议

1. **优先实现 Phase 1**：基础分离，即刻获得并发安全
2. **可选实现 Phase 2/3**：根据社区需求
3. **充分文档和示例**：关键成功因素
4. **发布 beta 测试**：收集反馈，迭代优化
