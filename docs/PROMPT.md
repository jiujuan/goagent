## Recommended approach

A new top-level **`prompt`** package, sitting at the same layer as `tool`/`session` (below `agent`). This is the cleanest fit for your conventions (小接口、provider 隔离、能力组合).

The one real design constraint: `agent` imports `prompt`, so `prompt` **must not** import `agent` (import cycle). I solve this with a small DTO — `prompt.Context` — that the agent populates from its `InvocationContext`. Sections depend only on `core`/`session`/`tool`, never on `agent`.

Two approaches I rejected:
- **Sections inside the `agent` package** (receiving `agent.InvocationContext` directly): no DTO needed, but couples sections to `agent`, bloats the package, and you can't unit-test sections in isolation. Against "接口尽量小".
- **A `Middleware` that rewrites `req.System`**: fits "能力即中间件", but middleware only sees `llm.Request` — no clean access to tools/sub-agents/session. Awkward, and re-renders every loop step needlessly.

## The design

**`prompt.Section`** — the extension point (small interface, with a `SectionFunc` adapter for one-off sections):

```go
type Section interface {
    Name() string                       // unique; used for override/remove
    Order() int                         // ascending; built-ins spaced 100 apart
    Render(Context) (string, error)     // "" => section omitted
}
```

**`prompt.Context`** — the decoupling DTO the agent fills in per invocation:

```go
type Context struct {
    context.Context
    Session     *session.Session
    UserContent core.Message
    AgentName   string
    AgentDesc   string
    Tools       []tool.Tool
    SubAgents   []Peer   // {Name, Description} — for delegation-aware prompts
}
```

**`prompt.Builder`** — ordered/priority list, override-by-name, render-and-join:

```go
b := prompt.New().
    Add(prompt.Identity(instruction)).   // Order 100
    Add(prompt.Environment()).           // Order 200 — date / OS / cwd
    Add(prompt.ToolGuidance()).          // Order 300 — auto-lists agent's tools
    Add(prompt.SessionState("plan","scratchpad"))  // Order 400 — selected State keys
b.Build(ctx) // sorts by Order, renders, drops empties, joins with "\n\n"
```

**Built-in sections (v1):** `Identity`, `Environment`, `ToolGuidance`, `SessionState` — each a constructor returning a `Section`. `Environment` takes optional overrides (a `Now func() time.Time`) so it's deterministic in tests.

**Agent integration** (backward compatible):
- Add `Config.Prompt *prompt.Builder`.
- In `LLMAgent.Run`, **once before the loop**: if `cfg.Prompt != nil`, build a `prompt.Context` from `ictx` and render → `req.System`; otherwise fall back to `cfg.Instruction` exactly as today. (Rendered once per invocation, not per step — env doesn't change mid-turn.)
- If both are set, `Prompt` wins; `Instruction` is ignored. Your base persona goes in `prompt.Identity(...)`.

**Testing:** section-level unit tests (deterministic via injected clock), a builder ordering/override test, and an `agent` test asserting `Prompt` overrides `Instruction` and renders once. Plus a runnable `examples/prompt/` demonstrating the four sections. An ADR (`0012`) documents the package + DTO-decoupling decision.

Does this match what you had in mind — particularly the new `prompt` package + `prompt.Context` DTO, and `Prompt` winning over `Instruction`? Once you confirm, I'll write the spec.