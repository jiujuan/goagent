# goagent 顶层架构

> 一句话理念：**一切皆事件流；事件即数据；能力即中间件；接口尽量小。**

## 分层与包

```mermaid
flowchart TB
    classDef core fill:#eef,stroke:#88a
    classDef cross fill:#efe,stroke:#8a8
    classDef prov fill:#fee,stroke:#a88

    subgraph L4["L4 编排"]
        Runner["runner.Runner<br/>解析会话·提交事件·选 agent"]
    end
    subgraph L3["L3 决策"]
        LLM["agent.LLMAgent<br/>turn 引擎"]
        WF["agent.Sequential / Parallel / Loop"]
        TR["transfer 方向规则<br/>sub / parent / peer"]
        MA["agent.ImageAgent / VideoAgent<br/>模型即 Agent · 流式进度"]
    end
    subgraph L2["L2 模型调用"]
        MW["middleware.Chain<br/>compaction·rag·retry·ratelimit·steering"]
        Model["llm.Model"]
        MM["llm.ImageModel / VideoModel"]
    end
    subgraph L1["L1 原语 (core)"]
        Msg["Message / Part<br/>(Image / Video)"]
        Ev["Event / Actions / Progress"]
        St["Stream = iter.Seq2"]
    end

    subgraph X["横切 / 可插拔服务"]
        Tool["tool.Tool<br/>泛型+reflect schema"]
        Sess["session.Store<br/>InMemory / JSONL"]
        Mem["memory.Store<br/>向量检索 / RAG"]
        Emb["embeddings.Embedder"]
        CB["callbacks.Handler"]
        Q["queue<br/>Queue·Bus·Worker (独立后台)"]
    end

    subgraph P["providers (子包)"]
        An["anthropic"]
        OAI["openaicompat<br/>deepseek / agnes / openai"]
        Mock["mock"]
        Agnes["agnes<br/>image / video"]
    end

    Runner -->|InvocationContext| LLM
    Runner --> WF
    Runner --> MA
    LLM --> MW --> Model
    LLM --> TR
    LLM --> Tool
    WF --> LLM
    MA --> MM
    Model --> P
    MM --> Agnes
    Runner -.EnqueueAgent.-> Q
    Q -.run agent.-> MA
    MW -.RAG.-> Mem
    Mem --> Emb
    Runner --> Sess
    Tool --> Mem
    Runner --> CB
    LLM --> St
    Runner --> St
    St --- Ev
    Ev --- Msg

    class Msg,Ev,St core
    class Tool,Sess,Mem,Emb,CB,Q cross
    class An,OAI,Mock,Agnes prov
```

每一层都返回 `core.Stream`（`iter.Seq2[*core.Event, error]`）。消费者用
`for ev, err := range stream` 驱动，提前 `break`/`return` 即可干净停止上游。

## 一次 turn 的时序（含工具调用与委派）

```mermaid
sequenceDiagram
    autonumber
    participant U as 调用方
    participant R as Runner
    participant A as LLMAgent (root)
    participant MW as middleware.Chain
    participant M as llm.Model
    participant T as tool.Tool
    participant Sub as 子/父/同级 Agent
    participant S as session.Store

    U->>R: Run(user, sessionID, msg)
    R->>S: 提交 user 事件
    R->>A: Run(InvocationContext{Root})
    loop 每步 (≤ MaxSteps)
        A->>MW: Generate(req)
        Note over MW: compaction 改写 / RAG 注入 /<br/>ratelimit 放行 / retry 包裹
        MW->>M: Generate(req)
        M-->>A: 流式 Response(partial→final)
        A-->>R: 流式 Event(partial 不提交)
        alt 有工具调用
            A->>T: Call(args)  (并发, 保序)
            T-->>A: ToolResult + Actions
            A-->>R: tool 事件
            R->>S: 提交 (应用 Actions.StateDelta)
            opt Actions.TransferToAgent (方向校验+防环)
                A->>Sub: Run(transferTo, depth+1)
                Sub-->>R: 接管并产出事件
            end
        else 无工具调用
            A-->>R: final 事件 (可选写 OutputKey)
        end
    end
    R-->>U: 流式 Event
```

## 关键不变量

- **事件即数据**：状态变更/委派/控制信号都挂在 `Event.Actions`，由 Runner 在**提交
  非 partial 事件**时事务性应用 → 可重放、可审计、可持久化。
- **决策/执行分离**：Agent 只决策并产出事件流，**只有 Runner 持久化**。
- **provider 无关**：上层只认 `core.Message`，provider 子包在调用边界做 wire 转换。
- **能力即中间件**：compaction/RAG/retry/ratelimit/steering 都是 `func(llm.Model) llm.Model`
  装饰器，任意组合排序，turn 引擎不感知具体能力。
- **可插拔服务**：`llm.Model` / `tool.Tool` / `session.Store` / `memory.Store` /
  `embeddings.Embedder` / `callbacks.Handler` 全是小接口，换实现不动核心。

## 设计来源对照

| 精华 | 来源 |
|---|---|
| `iter.Seq2` 全栈流式 · 事件Event 驱动（Event.Actions）· workflow/transfer · 泛型工具 | google/adk-go |
| 内部消息↔wire 边界转换 · 上下文压缩 · JSONL 持久化 · 统一错误流 | earendil-works/pi |
| 小接口 · functional options · 中心 schema(core) · 决策/执行分离 · provider 子包 | tmc/langchaingo |

详见各 [ADR](adr/) 与 [DESIGN.md](DESIGN.md)。
