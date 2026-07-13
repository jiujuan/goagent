# ADR 0005：provider 隔离为子包，OpenAI 兼容协议复用单一实现

状态：已接受

## 背景

langchaingo 把每个 provider 做成 capability 的子包（`llms/openai` 等），核心零
provider 依赖。我们需要支持 Anthropic（原生协议）、DeepSeek、agnes 等模型，其中
DeepSeek 与许多国产网关都讲 OpenAI 兼容协议。

## 决策

- `llm` 包只定义 `Model` 接口与 `Request`/`Response`/`Option`，不含任何 provider
  细节。
- 每个 provider 是独立子包：
  - `llm/anthropic`：Anthropic Messages API 原生协议。
  - `llm/openaicompat`：OpenAI `/chat/completions` 协议的**单一实现**，通过
    `Config{BaseURL, APIKey, Model}` 适配不同后端；并提供便捷构造器
    `OpenAI()` / `DeepSeek()` / `Agnes(baseURL, …)`。
  - `llm/mock`：无网络、可编程，用于测试与示例。

DeepSeek、agnes、OpenAI 共用 `openaicompat`，因为它们的协议相同，差别仅在
base URL / key / model id。

## 理由

- **核心零依赖**：`llm` 不 import 任何 HTTP/SDK，依赖图干净。
- **DRY**：所有 OpenAI 兼容后端共享一份转换 + HTTP 代码。
- **易扩展**：接新 provider = 新增一个子包实现 `Model`，或对 OpenAI 兼容后端直接
  用 `openaicompat.New(Config{...})`。

## 后果

- agnes 的 base URL 是部署相关的，故 `Agnes(baseURL, model, apiKey)` 需显式传入。
  若 agnes 实际使用非 OpenAI 协议，需要为其单独写一个子包（契约不变）。agnes 的
  **图片/视频生成**正属此类（端点与协议不同），已落地为独立子包 `llm/agnes`，见
  [ADR 0022](0022-media-generation.md)。
- 当前 provider 均为非流式（一次返回一个 `Response`）。流式（SSE 增量）作为后续
  增强，`Model` 接口已用 `iter.Seq2` 预留了多 `Response` 的能力，无需改接口。

## 备选方案

- **每个国产模型各写一个子包**：重复代码多；仅当协议确实不同才需要。
