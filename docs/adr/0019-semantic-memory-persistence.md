# ADR 0019：语义记忆持久化与固化 —— FileStore + 短期到长期写回

状态：提议

## 背景

ADR 0010 的 `memory.InMemory` 是纯内存暴力余弦检索，进程退出即丢，README 路线图已标注
需要持久化后端。此外，语义记忆当前**只能被动检索**——没有任何机制把一次会话里产生的
有价值事实**沉淀**进长期记忆。短期记忆（ADR 0017）与长期记忆之间缺一座桥。

本 ADR 是 ADR 0010 的延伸，`memory.Store` 接口**保持不变**。

## 决策

**一、持久化后端（零依赖优先）。** 新增 `memory.File`，镜像 `session.FileStore`
（ADR 0009）的 JSONL 思路：每行一个 `{id, content, metadata, embedding}`，启动时全量
载入内存、复用现有暴力余弦检索代码。

```go
func memory.File(dir string, emb embeddings.Embedder) (memory.Store, error)
```

pgvector / qdrant 等真实向量库作为可选 build-tag 子包后置，只需再实现 `Store`，契约不变。

**二、固化中间件（consolidation）—— 短期→长期的桥。** 新增一个 `AfterRun` 钩子
（或 `runner` 层回调）：会话结束时用一个小 prompt 抽取"值得记住的事实"，按性质分流：

- 可读、精确、用户明确的 → 写入文本记忆（ADR 0018）
- 琐碎、海量、供模糊召回的 → 写入语义记忆（本 ADR）

**三、去重与遗忘。** 写入语义/文本库前按内容 hash 去重；语义库支持按 Score+时间的淘汰
（TTL 后置）。先做去重——它直接决定库会不会无限膨胀。

## 理由

- **接口不变**：`Store` 两方法契约稳定，FileStore 与 InMemory 可互换。
- **复用 JSONL 模式**：与 session 持久化同一套心智模型，零新依赖。
- **固化是记忆"长出来"的前提**：没有写回，长期记忆永远是空的；这是把五层串成系统的关键管道。

## 后果

- FileStore 全量载入内存，受限于内存与启动时间；大语料需换 build-tag 后端（接口不变）。
- 固化 prompt 会增加一次额外 LLM 调用，应可配置开关与触发条件（按轮数/token 阈值）。
- 去重按内容 hash 是保守策略，语义近重复（改写）仍可能漏；近重复合并作为后续增强。

## 备选方案

- **直接上 pgvector/qdrant**：违背零依赖、抬高演示门槛；作为可选后端而非默认。
- **不做固化、纯被动 RAG**：长期记忆无法自增长，五层退化为割裂的工具集合。
