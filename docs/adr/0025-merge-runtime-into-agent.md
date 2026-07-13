# ADR 0025：合并 `runtime` 进 `agent` —— Functional-Options 门面 + 运行时内化

状态：已落地。细化 [ADR 0024](0024-clean-v2-layout.md) 的包布局：取消独立 `runtime` 包,
把"执行环境(runtime)"作为概念内化进 `agent` 包;对外收敛成 `agent.New(...)` +
`Run/Stream/Resume`,配置改用 Functional Options。

---

## 背景

[ADR 0024](0024-clean-v2-layout.md) 把执行模型拆成两个包:`agent`(Spec/Middleware/Loop)与
`runtime`(引擎/句柄),单向 `runtime → agent`。拆分**唯一的理由是跨包导入环**:`Spec` 持
`[]Middleware`、`Middleware` 钩子收 `*LoopContext`、引擎又要驱动 `Spec` 编译出的 loop。

一旦合并成**一个包**,同包内不存在循环,这个约束消失。于是可以:
- 删掉 `runtime` 中间层,职责回归 `Agent`;
- 对外只暴露 `agent.New(opts...)` + `agent.Run()`(用户要的形态);
- 配置从结构体字面量(`Spec{...}`)改成 **Functional Options**。

关键认知:**`runtime` 是"执行环境"概念,不是包名。** 它仍然清晰存在——只是住在
`agent` 包内一组有名的运行时文件里。

---

## 决策

### 单一 `agent` 包,两层职责

```
agent/
│  ── 声明 / 门面层 ──
├── options.go    # config + Option + With*(配置即数据,函数式选项)
├── agent.go      # Agent + New + Run / Stream / Resume(门面)
│
│  ── 运行时层(执行环境;原 runtime 包概念,现内化)──
├── runtime.go    # RunContext:一次运行的执行环境 + steering + cloneState
├── loop.go       # AgentLoop:相位机(循环本身)+ newLoop(config)
├── loopctx.go    # LoopContext:单步执行环境
├── middleware.go # Middleware 6 钩子 + Stack(加逻辑 / 改逻辑)
├── exectools.go  # 工具并发/串行执行
├── hitl.go       # 暂停·继续·审批闭环(Approval/Resume/applyApprovals)
└── run.go        # Run 句柄(Iter/Events/Wait/Steer/Cancel/Decide)
```

`runtime.go` 这个文件名是"执行环境"概念的**有名归宿**:包没了,概念在。

### 对外 API（Functional Options + New/Run）

```go
a, err := agent.New(
    agent.WithModel(model),
    agent.WithInstruction("你是天气助手"),
    agent.WithTools(weather),
    agent.WithMiddleware(tracing{}, rag{}, gate{}),
    agent.WithMaxSteps(8),
    // 多 agent 共享:agent.WithBus(b) / agent.WithCheckpointer(s)
)
answer, _ := a.Run(ctx, "北京天气？")                 // 阻塞,返回答案文本
run := a.Stream(ctx, "删库", agent.OnThread("s1"))    // 流式句柄
for ev := range run.Iter() { /* 见 Interrupted 弹审批 */ }
run.Decide(agent.Reject("call_1", "太危险")); run.Resume(ctx)
```

- `New(opts...) (*Agent, error)`:缺 `WithModel` 早返回错误(编程错误早暴露)。
- `Run`= 便捷阻塞,内部 `Stream(...).Wait()` 取最终文本。
- `Stream`= 非阻塞 `*Run`(惰性驱动:先订阅再 drive,零丢事件)。
- `Resume(ctx, thread, approvals...)`= HITL/崩溃恢复。

### runtime 概念的落点

| 概念 | 落点 | 机制 |
|---|---|---|
| 循环 | `AgentLoop`/loop.go | 显式相位机 |
| 循环参数 | `RunContext`(runtime.go) + `LoopContext`(loopctx.go) | 运行级环境 + 单步现场 |
| 暂停 | loop.go interrupt 分支 | `BeforeTool→Interrupt` → 落 `PendingHITL` 快照 + 发 `Interrupted` |
| 继续 | `Agent.Resume`/hitl.go | 从 Pending 快照恢复,应用审批 |
| HITL | hitl.go | `Approval`/`Allow`/`Reject`/`applyApprovals`;拒因转 ToolResult 回喂模型 |
| HOOK / middleware | middleware.go | 6 钩子;加逻辑(观测)/ 改逻辑(ModifyRequest 改写请求、返回 Directive 改控制流) |

### Functional Options 配置

`Spec` 结构体消失,字段转成不导出 `config` + `With*`:`WithModel`(必填)、`WithName`、
`WithDescription`、`WithInstruction`、`WithTools`、`WithMiddleware`、`WithSubAgents`、
`WithMaxSteps`、`WithToolExecution`、`WithModelOptions`、`WithBus`、`WithCheckpointer`。
运行级:`OnThread`、`WithMessage`、`WithRunFiles`。

---

## 理由

- **包数更少、依赖更直**:`runtime` 中间层蒸发;`agent` 单向依赖 `core/llm/tool/bus/
  checkpoint/vfs`,无环。
- **门面更顺手**:`agent.New(...).Run()` 正是用户心智;Functional Options 让配置可演进、
  零破坏(加字段不改签名)。
- **概念不丢**:运行时(执行环境)仍清晰,有名文件归宿;loop/参数/暂停/继续/HITL/hooks
  各有其位。
- **子 agent 即 `*Agent`**:同包无环,Stage 2 transfer/workflow 直接复用。

---

## 后果

- `runtime` 包删除;`runtime/{runtime,agent,run}.go` 的逻辑并入 `agent` 的 `agent.go`/
  `run.go`/`runtime.go`。`spec.go` 删除,内容进 `options.go`。
- `Compile`/`Drive` 降为包内:`New` 直接 `newLoop(config)`,`Run` 句柄直接 `runnable.run(rc)`。
- HITL 从"只暂停"补成"暂停→审批→恢复"最小可用闭环(`Approve`/`Reject`/`Resume`)。
- 验证:`go build/vet/gofmt` 干净;`agent`/`bus`/`checkpoint`/`core` 测试全绿,覆盖
  `Run` 取答案、`New` 缺 model 报错、流式事件、线程累积 + Resume、HITL 拒绝恢复、HITL
  批准恢复执行工具;quickstart 跑通。

---

## 备选方案

- **维持 0024 的 agent/runtime 双包**:除"避免循环"外无收益,而合并后循环本就不存在。
- **`New` 不返回 error(缺 model 时 Run 报错)**:延迟暴露编程错误,不如构造期早失败。
- **保留 `Spec` 结构体 + `New(Spec)`**:不如 Functional Options 可演进;且与"配置即选项"
  的门面风格不一致。
