# ADR 0006：流式作为 `Model` 的可选行为，而非独立接口

状态：已接受

## 背景

三个框架对流式处理不一：langchaingo 用回调 `WithStreamingFunc`，adk-go 让 Model
返回 `iter.Seq2` 多个 `LLMResponse`。我们的 `Model.Generate` 已返回
`iter.Seq2[*Response, error]`，因此"流式"天然等价于"yield 多个 Response"。

## 决策

不新增流式接口。`Model.Generate` 在 `req.Options.Stream` 为真时解析 provider 的
SSE，逐增量 yield **partial** `Response`，最后 yield 一个 **final**（非 partial）
`Response`；为假时只 yield 一个 final。

SSE 解析复用内部 `internal/sse` 包（一个把 SSE 转成 `iter.Seq2[Event,error]` 的
小读取器）。每个 provider 的流解析被拆成独立、可用 canned SSE 单测的函数
（`anthropic.ParseStream` / `openaicompat.ParseStream`）。

turn 引擎对流式**零特判**：它本就对每个 `Response` yield 一个事件，partial 标记决定
是否提交（partial 不进 history、不持久化），final 进 history。

## 理由

- **接口不膨胀**：流式与非流式同一个方法，调用方只切一个 option。
- **引擎复用**：`Event.Partial` 语义（ADR 0001/0004）让流式自动正确——这验证了
  "事件Event 只发生在 final" 的设计。
- **可测**：`ParseStream(io.Reader)` 可用字符串喂入测试，无需网络。

## 后果

- partial 只携带文本增量；tool call 只在 JSON 完整后于 final 出现（避免把残缺
  JSON 暴露给消费者）。
- provider 需各写一份 SSE 累积逻辑（Anthropic 的 content_block 模型与 OpenAI 的
  delta 模型不同），但都收敛到统一的 `Response` 序列。

## 备选方案

- **回调式流式**：与 `iter.Seq2` 架构不一致，且割裂控制流。
