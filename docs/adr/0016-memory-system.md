# ADR 0016：记忆系统总览 —— 统一到 Section / Middleware / Tool 三扩展点

状态：提议

## 背景

需要把"记忆"从单一的 RAG（ADR 0010）扩展为分层体系：短期（工作记忆）、长期文本、
长期语义、项目记忆（AGENTS.md）、规则系统（Global/Project）。直觉上这像五个子系统，
但若各自造一套抽象、各自决定如何进 prompt，`agent` 与 `prompt` 包会被迅速撑大，且五块
内容争抢同一段 system prompt 时顺序无法协调。

参考 adk-go 的 `memory.Service` 与 langchaingo 的 `Memory` 接口：它们都倾向于"一个大
Memory 对象"。本项目刻意不走这条路——我们已有 `prompt.Section`（ADR 0014）、
`middleware.Middleware`（ADR 0011）、`tool.Tool` 三个最小扩展点，记忆无需新范式。

## 决策

把记忆系统拆成**两个正交维度**，五层都落在这两维的取值上，不引入新的顶层抽象。

**维度 A —— 内容何时进入 prompt（决定扩展点）：**

| 时机 | 扩展点 | 记忆层 |
|---|---|---|
| 每轮注入 | `prompt.Section` | 规则、AGENTS.md、工作记忆、文本记忆索引 |
| 按 query 动态检索 | `middleware.BeforeModel` | 语义记忆（RAG） |
| LLM 主动取用 | `tool.Tool` | 语义检索、读文本记忆、改工作记忆 |

**维度 B —— 内容存在哪里（决定 Store）：**

| 存储 | 现状 | 记忆层 | ADR |
|---|---|---|---|
| `Session.events`/`State` | 已有 | 短期 / 工作记忆 | 0017 |
| 磁盘 Markdown + 索引 | 新增 | 长期文本 | 0018 |
| 向量库（doc+embedding） | 扩展 | 长期语义 | 0019 |
| 仓库内 `AGENTS.md` | 新增 | 项目记忆 | 0020 |
| `~/.goagent` + `./.goagent` | 新增 | 规则系统 | 0021 |

**统一约束 1 —— prompt 优先级（`Section.Order`）固定排布**，避免五块互相打架：

```
10  rules        (global→project)   ← 硬约束最前
20  projectmem   (AGENTS.md)
30  workingmem   (当前任务状态)
40  textmem      (策展事实索引)
—   semantic RAG (middleware 追加在 system 末尾)
```

**统一约束 2 —— token 预算**：`memx` 装配层做总预算分配。规则与 AGENTS.md 不截断，
工作记忆与文本索引按配额截断，语义检索受 `K`/`MinScore` 限制。

**装配 facade**：新增 `memx` 包，一次性产出可挂载的三类件，呼应 Runner 的装配角色：

```go
type Memory struct {
    Sections   []prompt.Section
    Middleware []middleware.Middleware
    Tools      []tool.Tool
}
func memx.New(cfg Config) (*Memory, error)
```

**包布局**：五层与装配 facade 都置于 `memory/` 之下，作为各自独立的子包，便于集中理解：
`memory/`（语义 Store/RAG）、`memory/workingmem`、`memory/textmem`、`memory/projectmem`、
`memory/rules`、`memory/memx`。子包按目录归类不产生隐式依赖；导入边是单向的——只有
`memory/memx` 导入父包 `memory` 与其余子包，父包 `memory` 不导入任何子包，故无循环引用。

## 理由

- **零新范式**：复用三个已验证的最小扩展点，`agent`/`prompt` 包契约不变。
- **顺序集中治理**：Order 集中定义于一处，杜绝隐式冲突。
- **可增量落地**：每层独立 ADR、独立可用，互不阻塞（见各 ADR 与实现计划）。

## 后果

- 五层都向 system prompt 注入，长会话需依赖 token 预算 + 压缩中间件（ADR 0007）兜底。
- `memx` 是可选便利层；使用者仍可手动挑选单层 Section/Middleware/Tool 组合。

## 备选方案

- **单一 Memory 大对象**（adk/langchaingo 风格）：API 集中但接口臃肿、难分别测试、与现有
  Section/Middleware 重复，违背"接口尽量小"。
- **记忆全部走 middleware 注入**：middleware 只见 `llm.Request`，拿不到 tools/session，
  且每 step 重渲染（ADR 0014 已论证）。
