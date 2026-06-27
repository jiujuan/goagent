# ADR 0011：中间件统一为模型装饰器（compaction / retry / ratelimit / steering）

状态：已接受（部分取代 [ADR 0007](0007-middleware-compaction.md) 的机制部分）

## 背景

ADR 0007 把中间件定义为 `BeforeModel(ctx, *llm.Request) error`——只能在调用前**改写请求**。
这对压缩/RAG 够用，但**重试**与**限流**需要包裹 `Generate` 调用本身（决定是否重跑、何时
放行），单一的 before 钩子表达不了。设计文档（DESIGN.md）本就预告了
`Middleware func(next Handler) Handler` 的装饰器形态。

## 决策

把 `Middleware` 统一为**模型装饰器**：

```go
type Middleware func(next llm.Model) llm.Model

func Chain(base llm.Model, mws ...Middleware) llm.Model // 第一个最外层
func Wrap(next llm.Model, gen GenerateFunc) llm.Model    // 构造装饰器的积木
func BeforeModel(fn func(ctx, *llm.Request) error) Middleware // 仅改写请求的便捷形态
```

agent 在每次 run 开始时 `model := middleware.Chain(cfg.Model, cfg.Middleware...)`，之后所有
模型调用都走装饰后的链。四个内建中间件：

- **Compaction**（ADR 0007 的算法不变）= `BeforeModel`，超阈值时摘要旧历史。
- **RAG**（ADR 0010）= `BeforeModel`，注入检索到的背景到 system。
- **Retry** = 包裹 `Generate`，**仅在产出任何响应之前**出错时重试（指数退避、尊重 ctx）；
  一旦已流式输出 partial 就传播错误，避免重复已交付内容。
- **RateLimit** = 包裹 `Generate`，最小间隔（RPS）+ 并发信号量，等待时尊重 ctx；并发槽
  持有到整段流结束才释放。
- **Steering** = 线程安全队列 + `BeforeModel`，把外部 goroutine 排入的消息在下次模型调用前
  注入 `req.Messages`，实现"运行中插话"。

推荐顺序（外→内）：`RateLimit, Compaction, RAG, Retry`——限流总闸在最外，重试只重跑裸模型
调用。

## 理由

- **一种机制统管全部**：改写请求、控制调用、注入消息都用同一个 `func(llm.Model) llm.Model`，
  可任意组合与排序。
- **流式安全的重试**：只在"未产出即失败"时重试，是流式场景下唯一不会重复输出的安全策略。
- **解耦不变**：`memory.RAG` 仍以结构化方式产出 `middleware.Middleware`；agent 只多了一行
  `Chain`，turn 引擎其余逻辑未动。

## 后果

- `agent.Config.Middleware` 的元素类型语义从"请求改写器"变为"模型装饰器"（同名不同义）。
- Steering 注入的消息进入**当前 run 的模型上下文**，但不作为独立事件持久化到 session（与
  ADR 0004 的事件模型有意区分；如需持久化可后续扩展）。
- 装饰器顺序会影响语义（如 Retry 在 Compaction 内/外），交由使用者按推荐顺序组织。

## 备选方案

- **保留 BeforeModel + 另设 wrap 接口**：两套机制并存、心智负担重；装饰器单一抽象更干净。
- **引入 `func(next Handler) Handler` 的独立 Handler 类型**：与已有的 `llm.Model` 重复；直接
  装饰 `llm.Model` 复用了流式签名，零额外类型。
