# ADR 0007：能力即中间件——上下文压缩作为 BeforeModel 钩子

状态：已接受（中间件机制在 [ADR 0011](0011-middleware-decorator.md) 中演进为统一的模型装饰器；
`BeforeModel` 保留为便捷构造器 `middleware.BeforeModel(fn)`，压缩的算法与安全切点逻辑不变，
构造器更名为 `middleware.Compaction(summarizer, opts)`）

## 背景

长会话会撑爆模型上下文窗口。pi 用 harness 内的 `transformContext` 在每次 LLM 调用
前裁剪/注入消息并做结构化压缩；adk-go 用 Flow 的 request processor 链。两者都把
"调用前改写请求"作为扩展点。

## 决策

引入 `middleware` 包，定义最小钩子：

```go
type Middleware interface {
    BeforeModel(ctx context.Context, req *llm.Request) error
}
```

`LLMAgent.Config.Middleware` 是一个有序列表，turn 引擎在**每次** model 调用前依次
执行，允许改写 `req`。压缩、重试、prompt 注入等都是该接口的实现。

首个实现 `middleware.NewCompaction(model, opts)`：
1. 用 `chars/4` 估算 `req.Messages` 的 token（可替换 `Estimator`）。
2. 超过 `MaxTokens` 时，从尾部回溯累积到 `KeepRecentTokens` 得到切点；切点回退以
   保证**绝不把 tool 结果与其 tool call 拆开**。
3. 调用一个（廉价、无工具的）model 把切点之前的历史压成结构化摘要
   （Goal/Constraints/Progress/Decisions/Next Steps）。
4. 用 `[摘要]` 单条消息替换被压缩的前缀，保留近期消息原样。

## 理由

- **不改核心**：新能力 = 实现一个接口并塞进 `Middleware` 列表，turn 引擎不感知具体
  能力。
- **可组合**：多个中间件按序叠加。
- **安全切点**：tool call/result 配对不被破坏，避免给模型喂出非法历史。

## 后果

- 当前 token 估算是字符近似，非 provider 真实计数（pi 用 usage 锚定）。后续可让中
  间件读取 `Event.Usage` 做更精确的估算——接口无需变更。
- 摘要会发起一次额外的 model 调用（成本与延迟）。仅在超阈值时触发。

## 备选方案

- **装饰器 `func(next Handler) Handler`**：更通用但样板多；当前只需"调用前改写请求"，
  `BeforeModel` 足够且更直观。
