# ADR 0002：内部消息模型与 provider wire 格式在边界处转换

状态：已接受

## 背景

pi 框架的核心洞见：内部始终使用灵活的 `AgentMessage`，只在调 LLM 的边界用
`convertToLlm` 转成 provider 的格式。这让自定义消息类型（压缩摘要、bash 执行
记录等）成为 transcript 的一等公民，却对 LLM 只渲染成普通文本。

如果直接用某个 provider 的消息结构贯穿全系统，就会被该 provider 的协议绑死，且
难以引入框架级的自定义消息。

## 决策

在 `core` 包定义 provider 无关的 `Message` + `Part`（密封 tagged union：
`Text`/`Thinking`/`Image`/`ToolCall`/`ToolResult`）。每个 provider 子包负责在
`Generate` 内部把 `[]core.Message` 转成自己的 wire 格式、把响应转回 `core.Message`。

`Part` 通过未导出的 `isPart()` 方法密封，只有 `core` 包能新增 part 类型，保证
类型安全的多模态内容而无需判别字段。

## 理由

- **provider 无关**：上层逻辑（agent/runner/tool）只认 `core.Message`。
- **可扩展**：未来加入压缩摘要等框架级消息类型时，只需在转换函数里决定如何渲染
  给具体 provider，不影响其余代码。
- **类型安全**：密封 union 比 `map[string]any` 或字符串判别更安全。

## 后果

- 每个 provider 要写两段转换代码（去/回）。这是隔离 provider 差异的合理代价。
- `core` 是唯一能定义 `Part` 实现的包；自定义 part 需要进 core（有意为之，保持
  词汇表集中）。

## 备选方案

- **直接用 OpenAI 格式做通用格式**：会把 Anthropic 的 thinking、tool_result
  结构等强行塞进 OpenAI 模型，损失保真度。
- **`map[string]any` 消息**：灵活但无类型安全，到处是断言。
