# ADR 0017：工作记忆 —— State 支撑、可穿越压缩的结构化便签

状态：提议

## 背景

Session 的 `events`/`Messages()` 已是短期记忆，但它有个致命弱点：压缩中间件
（ADR 0007）在 token 超阈值时会把旧消息摘要替换掉。于是"当前目标、待办、关键事实"这类
**必须长期保持**的状态，一旦被压进摘要就会丢精度或丢失。16 步的 turn 循环里这种丢失很常见。

需要一块**结构化、跨轮、能穿越压缩**的便签——工作记忆（Working Memory）。

## 决策

工作记忆**存 `Session.State`，不存消息流**——这正是它能穿越压缩的原因：压缩丢 message，
不丢 State。新增 `workingmem` 包，把一个保留 key 包装成结构化便签，**不引入新存储**。

```go
type WorkingMemory struct{ s *session.Session }
func For(s *session.Session) *WorkingMemory

func (w *WorkingMemory) Goal() string
func (w *WorkingMemory) SetGoal(g string)
func (w *WorkingMemory) Todos() []Todo
func (w *WorkingMemory) AddTodo(t Todo) ; func (w *WorkingMemory) ResolveTodo(id string)
func (w *WorkingMemory) Note(key, val string)        // 关键事实 KV
func (w *WorkingMemory) Snapshot() Snapshot           // 供 Section 渲染
```

两个集成件：
- **`workingmem.Section()`**（`prompt.Section`，Order=30）：渲染成 `## 当前工作记忆`，
  含目标 / 待办 / 关键事实；Snapshot 为空则渲染 `""`（Builder 自动省略）。
- **`workingmem.UpdateTool()`**（`tool.Tool`，名 `update_working_memory`）：让 LLM 显式
  维护目标与待办。

## 理由

- **零新存储**：复用 `Session.State`，自动获得 InMemory/FileStore 两种持久化（ADR 0009）。
- **穿越压缩**：与 ADR 0007 天然互补——压缩越激进，工作记忆价值越高。
- **先 Tool 后自动**：显式工具实现简单、可控；"压缩前自动抽取"留作演进。

## 后果

- 工作记忆占用 `State` 的一个保留命名空间（如 `wm:` 前缀），需在文档中声明避免冲突。
- LLM 维护质量取决于其纪律性；自动抽取（`BeforeModel` 钩子在压缩前固化）作为后续增强。
- Snapshot 进 prompt 有体积上限，受 ADR 0016 的 token 预算约束。

## 备选方案

- **存消息流**：会被压缩吞掉，违背设计初衷。
- **独立持久文件**：多一套存储与生命周期，而 State 已绑定 Session 生命周期，更简。
