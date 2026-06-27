# 会话树后端（分支 / fork / 重摘要）：作用与实现方案

> 配套：线性会话的取舍见 [ADR 0003](adr/0003-linear-session.md)；本文是其"后续可选扩展"的展开设计。
> 路线图条目：[DESIGN.md](DESIGN.md) 中 `[ ] 会话树（分支/fork）后端`。

## 一、现状定位

这是一个**已明确预留、尚未完成**的扩展点。

| 维度 | 现状 |
|---|---|
| 会话模型 | 线性 append-only：`Session` = 有序 `[]*core.Event` + `State`（`session/session.go`） |
| Store 契约 | 只有 `GetOrCreate` + `Append` 两个方法（`session/store.go`） |
| 持久化 | `FileStore` = 一个 JSONL 文件，一行一事件，重放恢复（`session/filestore.go`） |
| `Branch` 字段 | `core.Event.Branch` 已存在，但**仅用于并行子 agent 打标签**（`agent/workflow.go`），并未用于历史隔离 |
| 摘要 | `middleware.Compaction` 在 `BeforeModel` 阶段**临时**重写 request，结果**不落盘**（`middleware/compaction.go`） |

ADR 0003 已把取舍写清：参考框架 **pi 用树**（可移动 leaf 指针、分支、fork、重摘要），**adk-go 用线性日志 + Branch 字段**，本项目选"线性优先"，并声明"树可作为另一个 `Store` 实现 + 会话视图后续引入，而不破坏现有契约"。

**一个潜在缺口**：`Branch` 字段被记录进事件，但 `LLMAgent` 构造历史时是 `slices.Clone(ictx.Session.Messages())`，而 `Messages()` 返回**全部**事件、**不按 Branch 过滤**。所以并行子 agent 的"历史隔离"在语义上不完整——这正是会话树要补的第一块。

## 二、作用：为什么需要它

三个能力解决三类不同的真实问题。

### 1. 分支（branch）—— 同一会话内探索多条路径
从历史中任意节点起，长出一条新的事件链，不破坏原主干。
- **价值**：A/B 对比两套 prompt / 两个模型；"这一步答错了，回到上一节点重试"；HITL 场景人类在某节点介入选择不同走向。
- **现状无法做到**：线性日志只能往末尾追加，回到中间节点改写 = 丢历史。

### 2. fork —— 跨会话复制
把某会话从某节点起整体复制成一个新会话。
- **价值**：以一段"已铺垫好上下文"的对话为模板，派生多个独立后续；多用户 / 多实验共享前缀。

### 3. 重摘要（re-summarize）—— 把压缩变成"持久、可编辑的节点"
当前 `Compaction` 是临时的：每请求重算、不存、不可编辑、不可固定。会话树里，摘要是**一等事件节点**。
- **价值**：摘要只算一次并落盘（省 token、可复现）；可编辑 / 钉住某摘要节点；可从摘要节点再分支。
- 本质：把"每请求即时压缩"升级为"压缩 = 向树写入一个 summary 节点 + 迁移 leaf"。

> 一句话：**分支**给探索/重试，**fork**给复制/模板化，**重摘要**给可控、可持久的长上下文管理。

## 三、数据模型选型

### 方案 A：真·树 Store（ADR 字面建议）
新写 `TreeStore`，内部用父指针 + leaf 指针的节点结构。
- 缺点：`Store` 接口只有 `GetOrCreate`/`Append`，**不足以表达** fork / 切 leaf / 列分支 → 必然扩接口；且容易破坏 JSONL 纯追加的优势。

### 方案 B（推荐）：append-only 事件日志 + DAG 索引
保持"事件不可变、只追加"，只给每个事件加 `ParentID`，让历史从"数组顺序"变成"从 leaf 回溯到 root 的路径"。

```go
type Event struct {
    ID       string `json:"id"`
    ParentID string `json:"parent_id,omitempty"` // 新增：父事件
    // ...其余不变
}
```

- **线性会话 = 退化的树**：每事件 parent = 前一事件，旧 JSONL 零改动即可读（`ParentID==""` 时按文件顺序串成链）。
- **JSONL 仍纯追加**：分支、fork、摘要全是"再追加一个带不同 parent 的事件"，从不重写历史行。
- **leaf / 分支名**作为可变指针单独存（文件尾的 `ref` 元事件，或旁挂 `<session>.refs.json`）。

## 四、实现方案（分阶段）

### 阶段 1（本次实现）：事件加父指针 + Session 路径投影
- `core.Event` 加 `ParentID`。
- `Session` 内部增加 `byID map[string]*Event` 索引 + `leaf string`（当前活动叶）。
- `Messages()` / `Events()` 改为**从 `leaf` 回溯到 root 的路径**，而非整张表。这一步同时为修掉 Branch 隔离缺口打基础（活动路径天然只含一条分支）。
- `commit` 默认把新事件挂到当前 `leaf` 之下并推进 `leaf`；**State 仍增量应用**（线性下与路径重放等价，保证对现有"直接 `State().Set` 种子"零回归）。
- 额外提供 `replayState`/路径工具（`stateAlong`），作为阶段 2 Checkout 的状态重放机制并单测。

> **向后兼容**：线性 = 退化树，旧 JSONL 文件与现有 API 行为完全不变。

### 阶段 2：扩展 Store 契约（不破坏旧的）
新增**可选**接口，旧 `InMemoryStore`/`FileStore` 仍实现 `Store`，并**额外**实现它：

```go
type TreeStore interface {
    Store // 嵌入，保持兼容
    Checkout(ctx context.Context, s *Session, eventID string) error          // 切活动 leaf，沿新路径重放 State
    Fork(ctx context.Context, s *Session, fromEventID, newSessionID string) (*Session, error) // 复制 root..from 路径为新会话
    Branches(ctx context.Context, s *Session) ([]Ref, error)                 // 枚举树的 tip（无子事件的叶）
}
type Ref struct{ Name, LeafEventID string; Active bool }
```

实现要点（`session/tree.go` + 两个 Store）：
- **Checkout**：把 `leaf` 移到目标事件，并用阶段 1 的 `stateAlong` **沿新路径重放 State**；随后 `Append` 自然成为该节点的子事件（分支）。
- **Fork**：取 `pathTo(fromID)` 的浅拷贝事件，经 `commit` 重放种入新会话（保留各自 `ParentID`/索引/状态），原会话不受影响。
- **Branches**：tip = 不是任何事件父节点的事件；标注当前 `leaf` 为 `Active`。
- **持久化（FileStore）**：事件 JSONL 仍**纯追加**；可变的"活动叶"写到旁挂的 `<session>.refs.json`（`{"leaf": "<eventID>"}`）。`load` 先重放 JSONL 重建树，再读 refs 覆盖 `leaf`（旧文件无 refs 则回退为"最后一行"），最后 `stateAlong(leaf)` 重算 State（重放是按文件顺序的、跨分支混合的，必须按活动路径纠正）。`Fork` 用 `writeAll` 整文件落新会话。

> 阶段 1/2 均**向后兼容**：线性 = 退化树，旧 JSONL 文件与现有 API 行为不变。

### 阶段 3：重摘要节点化
把 compaction 从"中间件即时改写"升级为"向树写一个持久 summary 节点"：

```go
// core.Event 新增标记字段
SummarizesTo string `json:"summarizes_to,omitempty"` // 该 summary 节点替代 root..此ID 的前缀

// session 包：把给定文本作为 summary 节点追加为新 leaf（任意 Store 通用，复用 Append）
func Summarize(ctx, store Store, s *Session, cutEventID, summary string) error

// middleware 包：超阈值时生成摘要并持久化（compaction 退化为触发器）
func Resummarize(ctx, store session.Store, s *session.Session, summarizer llm.Model, opts *CompactionOptions) (bool, error)
```

实现要点：
- **投影**：`Session.Messages()` 扫描活动路径，取**最靠近 leaf** 的 summary 节点，用其文本替换它覆盖的前缀（root..cut），cut 之后的事件原样保留；其余 summary 标记跳过。
- **State 不受影响**：summary 纯属"视图"层面——State 仍沿完整路径重放，所以压缩不会丢状态。
- **可持久 / 可复现 / 可叠加**：summary 是 append-only 的一等事件，落盘后重载即生效；**重摘要 = 再追加一个更靠后的 summary 节点**，自动 supersede 旧的（旧 summary 落入被替换前缀）；可从 summary 节点继续分支。
- **分层**：`middleware.Resummarize` 因需要 `Store`+`Session`（请求层看不到），不放进模型装饰器链，而作为独立操作（Runner 可在每轮后调用以约束上下文）；原 `Compaction` 临时压缩中间件保留，作为不需持久化时的轻量选项。

> 三个阶段均**向后兼容**：线性 = 退化树，无 summary 节点时投影即原样，旧 JSONL 文件与现有 API 行为不变。

### 阶段 4：Runner / Agent / 上层接线
- `Runner.Run` 维护 parent=leaf（基本不变）。
- 上层（CLI/HTTP）暴露 `fork` / `checkout` / `branches`，调 `TreeStore`。
- 用活动路径替代或补全 `Branch` 隔离。

## 五、关键权衡与风险

1. **State 重放必须沿路径**（最重要）。一旦有分支，State 必须从 root 沿**活动路径**重放，否则别的分支的状态会污染当前分支。`temp:` 丢弃、`app:`/`user:` 跨会话共享语义需在此基础上重审。阶段 1 因只做线性追加，保留增量应用；分支能力（阶段 2）落地时切换到沿路径重放。
2. **JSONL 兼容**：坚持方案 B，旧文件天然是退化树，无需迁移；leaf/分支引用单独存，避免重写历史行。
3. **接口不破坏**：用嵌入 `Store` 的新接口 + 类型断言（`if ts, ok := store.(TreeStore)`），让不关心树的代码零感知——兑现 ADR 0003 的"可演进"承诺。
4. **复杂度边界**：先做分支 + fork + 路径投影（阶段 1–2），重摘要节点化（阶段 3）作为独立增量，不一次吞下。

## 六、一句话结论

会话树把会话从"只能往后写的直线"升级为"可探索、可复制、可持久压缩的图"，分别服务重试/对比、模板化派生、可控长上下文；最佳路径是保持事件 append-only 不变、给 `Event` 加 `ParentID` 把历史改为"leaf→root 路径投影"，再用嵌入 `Store` 的可选 `TreeStore` 承载 fork/checkout/branches，最后把临时 compaction 升级为持久 summary 节点——既兑现 ADR 0003 的演进承诺，又顺带补上 `Branch` "记录了却没用于隔离"的缺口。
