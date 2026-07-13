# ADR 0010：长期记忆 / RAG —— Store 抽象 + 工具与中间件双集成

状态：已接受

## 背景

需要让 agent 利用外部知识（长期记忆 / 检索增强生成）。langchaingo 用
`Embedder` + `VectorStore` + `Retriever` + 文档加载/切分；adk-go 用
`memory.Service`（`AddSessionToMemory` / `SearchMemory`）并经 `load_memory` 工具与
`ToolContext.SearchMemory` 暴露给 agent。两者都把"嵌入 + 相似度检索"与"如何把检索
结果喂给模型"分开。

## 决策

- **`embeddings.Embedder`**：最小接口 `Embed(ctx, texts) ([][]float32, error)`，
  provider 隔离为子包（`embeddings/mock` 哈希词袋、`embeddings/openaicompat`
  `/embeddings`）。
- **`memory.Store`**：`Add(docs...)` 与 `Search(query, k) []Document`。`Document`
  含 `Content`/`Metadata`/`Score`。`memory.InMemory(embedder)` 是暴力余弦检索实现，
  内部持有 embedder——`Search` 接受 query 字符串、内部嵌入，调用方无需关心向量。
- **两种集成**（呼应"tool = LLM 驱动 / middleware = 自动"的一贯取舍）：
  1. **`memory.SearchTool(store, k)`**：一个 `search_memory` 工具，模型自行决定何时
     检索（adk 的 load_memory 风格）。
  2. **`memory.NewRAG(store, opts)`**：一个中间件，在每次模型调用前用最近的 user
     消息检索，并把相关文档注入 system 提示（自动 RAG，模型无需决策）。

`memory.RAG` 以**结构化**方式满足 `middleware.Middleware`（仅有 `BeforeModel` 方法），
因此 `memory` 包**无需 import `middleware`** 即可被放进 `agent.Config.Middleware`。

## 理由

- **小接口 + provider 子包**：与 `llm`/`session` 一致，核心零依赖、易换后端（pgvector
  等只需再实现 `Store`）。
- **检索与喂法解耦**：同一个 `Store` 既能被工具用、也能被中间件用。
- **零依赖可演示**：mock embedder（哈希词袋，支持中英文分词）让 RAG 在无 API key 下
  也能给出有意义的排序，便于测试与示例。
- **无 import 环**：`memory → tool/embeddings/llm/core`，靠结构化接口满足避免
  `memory ↔ middleware` 互依。

## 后果

- `InMemory` 是 O(N) 暴力检索，适合开发与中小语料；规模化需换实现（接口不变）。
- 当前未内置文档切分与会话入库（`AddSessionToMemory`）；可作为 `memory` 的后续 helper
  叠加，不影响 `Store` 契约。
- RAG 中间件把上下文注入 system 提示（不污染对话历史）；超长检索结果可能挤占窗口，
  可与压缩中间件（ADR 0007）叠加。

## 备选方案

- **只做工具或只做中间件**：各自覆盖面不全；两者并存让"模型主动"与"系统自动"都可用。
- **Store 直接操作向量、由上层嵌入**：调用方负担更重；让 Store 持有 embedder 更易用。
