# ADR 0014：Prompt Builder —— 由可组合 Section 构建 system prompt

状态：已接受

## 背景

`LLMAgent` 的 system prompt 一直是单个静态字符串 `Config.Instruction`，在 turn 循环里直接写入
`req.System`。这只能表达一段固定文本，无法做"模块化、可动态组合"的 system prompt：例如自动注入
运行环境（日期/OS/cwd）、自动罗列 agent 的工具、注入选定的 session state、对委派关系敏感的指令等。

这些能力天然是**多个独立的块**，需要排序、按需省略、可被使用者覆盖或扩展。把它们都塞进一个字符串
由使用者手工拼接，既不可复用也不可测试。

## 决策

新增顶层 `prompt` 包（与 `tool`/`session` 同层，位于 `agent` 之下），提供 `Section` + `Builder`：

```go
type Section interface {
    Name() string                    // 唯一；用于按名覆盖/删除
    Order() int                      // 升序；内建按 100 递增（100/200/300/400）
    Render(Context) (string, error)  // 返回 "" => 该块省略
}

b := prompt.New().
    Add(prompt.Identity(persona)).       // 100
    Add(prompt.Environment()).           // 200：date/OS/cwd，可注入时钟
    Add(prompt.ToolGuidance()).          // 300：自动罗列 agent 工具
    Add(prompt.SessionState("plan"))     // 400：选定 state key
sys, err := b.Build(ctx)                 // 按 Order 排序→渲染→丢空→以 "\n\n" 连接
```

**关键约束：`agent` 依赖 `prompt`，故 `prompt` 不得依赖 `agent`（否则 import 环）。** 用一个小 DTO
`prompt.Context` 解耦：agent 每次调用时从 `InvocationContext` 填充它，Section 只看见
`core`/`session`/`tool`：

```go
type Context struct {
    context.Context
    Session     *session.Session
    UserContent core.Message
    AgentName, AgentDesc string
    Tools       []tool.Tool
    SubAgents   []Peer   // {Name, Description}，供委派感知的 prompt 使用
}
```

**Agent 接入（向后兼容）：** `Config` 新增 `Prompt *prompt.Builder`。`LLMAgent.Run` 在进入 turn 循环
**之前**渲染一次 system（env/state 不随 step 变化）：`Prompt != nil` 则 `Build`，否则回退到
`cfg.Instruction`。二者皆设时 **`Prompt` 优先**，`Instruction` 被忽略；渲染出错则发一个 error event
并结束（仿照 `streamModel` 的错误处理）。

## 理由

- **小接口 + 能力组合**：`Section` 与现有 `tool.Tool`/`middleware.Middleware` 一脉相承，扩展点最小化。
- **DTO 解耦**：`prompt.Context` 让 Section 可脱离 agent 单元测试，并杜绝 import 环；这是放在 agent 包内
  直接收 `InvocationContext` 所做不到的。
- **每次调用渲染一次**：而非每个 step 重渲染，避免无谓开销，语义也更清晰（system 在一轮 invocation 内恒定）。
- **零回归**：未配置 `Prompt` 的 agent 行为与之前逐字节一致。

## 后果

- system prompt 的来源出现两条路径（`Prompt` 与 `Instruction`），以 `Prompt` 优先消歧；基础人设建议放进
  `prompt.Identity(...)`。
- `Section.Order()` 决定块的相对位置；内建间隔 100 留出插入空间，顺序冲突由使用者自行协调。
- `prompt` 不依赖 `agent` 是必须维持的边界：新增 Section 若需要 agent 侧信息，应扩展 `prompt.Context` 字段，
  而非反向 import。

## 备选方案

- **Section 放在 agent 包内、直接收 `InvocationContext`**：省去 DTO，但把 Section 耦合到 agent、撑大该包、
  无法独立测试，违背"接口尽量小"。
- **用 Middleware 改写 `req.System`**：贴合"能力即中间件"，但 middleware 只看得到 `llm.Request`，拿不到
  tools/sub-agents/session，且会每个 step 重渲染。
