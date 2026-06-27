# Prompt Builder 系统

## Context

当前 `LLMAgent` 的 system prompt 只有一个静态字符串 `Config.Instruction`，在每个循环步里直接塞进 `req.System`（[agent/agent.go:170](agent/agent.go)）。这无法表达"模块化、可动态组合"的 system prompt —— 比如自动注入环境信息（日期/OS/cwd）、自动罗列 agent 的工具、注入选定的 session state、对委派关系敏感的指令等。

目标：新增一个顶层 `prompt` 包，提供可扩展的 `Section` + `Builder`，让 system prompt 由多个有序模块组合而成；并以**向后兼容**的方式接入 `agent`（`Instruction` 路径完全保留）。

关键约束：`agent` 依赖 `prompt`，因此 `prompt` **不得**依赖 `agent`（避免 import 环）。通过一个小 DTO `prompt.Context` 解耦——agent 在每次调用时从 `InvocationContext` 填充它，section 只依赖 `core`/`session`/`tool`。

## 设计

### 新包 `prompt/`（顶层，与 `tool`/`session` 同层）

**`prompt/section.go`** —— 扩展点
```go
type Section interface {
    Name() string                    // 唯一；用于覆盖/删除
    Order() int                      // 升序；内建按 100 递增
    Render(Context) (string, error)  // 返回 "" 表示省略该 section
}

// SectionFunc 适配一次性 section
type SectionFunc struct { SecName string; SecOrder int; RenderFn func(Context) (string, error) }
```

**`prompt/context.go`** —— 解耦 DTO（嵌入 context.Context，与 `tool.Context`/`InvocationContext` 同风格）
```go
type Peer struct { Name, Description string }

type Context struct {
    context.Context
    Session     *session.Session
    UserContent core.Message
    AgentName   string
    AgentDesc   string
    Tools       []tool.Tool
    SubAgents   []Peer
}
```

**`prompt/builder.go`** —— 有序列表 + 按名覆盖 + 渲染拼接
```go
func New() *Builder
func (b *Builder) Add(s Section) *Builder    // 同名则覆盖（保留可重配置）
func (b *Builder) Remove(name string) *Builder
func (b *Builder) Build(ctx Context) (string, error)  // 按 Order 排序→渲染→丢空→以 "\n\n" 连接
```

**`prompt/sections.go`** —— 内建 section（每个一个构造器，返回 `Section`）
- `Identity(instruction string)` — Order 100，原样输出基础人设/指令
- `Environment(opts ...EnvOption)` — Order 200，日期 / OS（`runtime.GOOS`）/ cwd（`os.Getwd`）。支持 `WithNow(func() time.Time)` 注入时钟，测试可确定化
- `ToolGuidance()` — Order 300，自动罗列 `ctx.Tools` 的 name+description（无工具则渲染为 ""）
- `SessionState(keys ...string)` — Order 400，从 `ctx.Session.State()` 取选定 key 渲染（缺失的 key 跳过；全缺则为 ""）

### Agent 接入（向后兼容）

`agent/agent.go`：
- `Config` 新增 `Prompt *prompt.Builder`
- `LLMAgent.Run` 中，**在 `for range a.maxSteps` 循环之前**计算一次 `system`：
  - 若 `cfg.Prompt != nil`：从 `ictx` + cfg 构造 `prompt.Context`（`SubAgents` 由 `cfg.SubAgents` 映射为 `[]prompt.Peer`），调用 `Build`。渲染出错则 yield 一个 error event 并 return（仿照 `streamModel` 的错误处理）。
  - 否则 `system = cfg.Instruction`（与现状完全一致）
- 循环内 `req.System = system`（只渲染一次/次调用，不随 step 变化）
- 二者皆设时 `Prompt` 优先，`Instruction` 被忽略

修改点集中在 [agent/agent.go:145-215](agent/agent.go) 的 `Run`，仅 import 新增 `prompt` 包。

## 涉及文件

- 新增 `prompt/section.go`、`prompt/context.go`、`prompt/builder.go`、`prompt/sections.go`
- 修改 `agent/agent.go`（Config 字段 + Run 渲染逻辑）
- 新增测试：`prompt/builder_test.go`（排序/覆盖/丢空）、`prompt/sections_test.go`（注入时钟使 Environment 确定化；ToolGuidance/SessionState 边界）、`agent/prompt_test.go`（断言 Prompt 覆盖 Instruction、且每次调用只渲染一次）
- 新增示例 `examples/prompt/main.go`（用 `llm/mock`，演示四个 section 组合，单文件，仿 `examples/quickstart`）
- 新增 ADR `docs/adr/0014-prompt-builder.md`（**注意：0012/0013 已占用**，doc 里写的 0012 需改为 0014）；记录新包 + DTO 解耦决策，沿用现有 ADR 中文模板（背景/决策/...）

## 验证

1. `go build ./...` 通过
2. `go test ./prompt/ ./agent/` 全绿（含确定化时钟的 Environment 测试）
3. `go run ./examples/prompt` 打印出由 Identity+Environment+ToolGuidance+SessionState 组合的 system prompt 与一次完整 mock 对话
4. 回归：现有 `examples/quickstart`（走 `Instruction` 路径）行为不变
