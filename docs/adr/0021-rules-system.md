# ADR 0021：规则系统 —— Global / Project 两级行为约束

状态：提议

## 背景

需要一套**始终生效的行为约束**：语气、产出格式、禁止操作、编码规范等。它有两个来源层级：
用户跨项目的全局偏好，与某仓库的项目级约束。

注意消歧：现有 `agent/transfer.go` 里的 "rules" 指 **agent 委派方向规则**（ADR 0008），
与本 ADR 无关。本 ADR 的规则是**注入 prompt 的行为/约束指令**。

## 决策

新增 `rules` 包，做**两级加载 + 优先级合并 + 单 Section 注入**。

```go
type Rule struct {
    ID, Scope, Text string
    Source string   // "global" | "project"
}
type Set struct{ rules []Rule }

func rules.Load(globalDir, projectDir string) (*Set, error)  // project 同 ID 覆盖 global
func (s *Set) Section() prompt.Section                        // Order=10，最前
```

两级来源：
- **Global**：`~/.goagent/rules/*.md` —— 用户跨项目偏好。
- **Project**：`./.goagent/rules/*.md` —— 仓库约束，优先级更高，project 同 ID 覆盖 global。

最简实现 = 两级目录 Markdown 按优先级拼接成一个 Section。结构化 `Rule`（带 id/scope/
condition，支持按上下文启用）作为演进方向。规则 Order 最高——硬约束应最先呈现给模型。

## 理由

- **两级分离**：个人偏好与团队约束各归其位，互不污染。
- **优先级明确**：project 覆盖 global，符合"越具体越优先"的直觉。
- **复用 Section**：与 AGENTS.md / 工作记忆同机制，仅 Order 与来源不同。

## 后果

- 规则不受 token 预算截断（视为硬约束）；规则过多需使用者自行精简。
- 需与 ADR 0020 划清边界：AGENTS.md = 描述性项目知识；rules = 命令式必须/禁止约束。
- 与 transfer 规则（ADR 0008）同名不同义，文档需显式区分以免混淆。

## 备选方案

- **规则并入 AGENTS.md**：丢失全局层、丢失"约束"与"背景"的语义区分。
- **结构化规则引擎（条件触发）起步**：过度设计；先做 Markdown 拼接，结构化按需演进。
