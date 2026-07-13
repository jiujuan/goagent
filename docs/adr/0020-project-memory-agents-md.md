# ADR 0020：项目记忆 —— AGENTS.md 分层发现与注入

状态：提议

## 背景

agent 在某个仓库里工作时，需要稳定的项目级上下文：构建/测试命令、目录约定、技术栈、
禁止事项。这类信息属于**仓库**而非某次会话或某个用户，应随仓库版本化。业界事实标准是
`AGENTS.md`（等价于 Claude Code 的 `CLAUDE.md`）。项目当前不加载任何此类文件。

## 决策

新增 `projectmem` 包，做**分层发现 + 单 Section 注入**。

**分层发现**：从给定起点目录向上走到仓库根，收集每层的 `AGENTS.md`，按**从根到叶**排序
（叶目录优先级最高，可覆盖/追加上层）——与 Claude Code 加载 CLAUDE.md 的层级规则一致。

```go
func projectmem.Load(startDir string) ([]Doc, error)   // 根→叶有序
func projectmem.Section(docs []Doc) prompt.Section       // Order=20，紧贴顶部
```

支持简单的 `@import ./other.md` 包含；复杂模板/条件后置。Section 渲染时把各层拼成带来源
标注的块。

## 理由

- **随仓库版本化**：项目知识进 git，团队共享、可审计，优于散落在用户配置里。
- **分层覆盖**：monorepo 中子项目可在自己目录放 AGENTS.md 覆盖根级约定。
- **Order 高**：项目背景应在工作记忆/文本记忆之前、规则之后呈现给模型。

## 后果

- 需定义发现边界（走到 `.git` 或盘符根停止），避免越界读到无关文件。
- AGENTS.md 体积不受预算截断（视为硬上下文）；过大文件由使用者自行控制。
- 与规则系统（ADR 0021）职责区分：AGENTS.md = 项目"是什么/怎么做"；规则 = 跨项目/项目级的
  "必须/禁止"约束。二者均为 Section，靠 Order 与来源区分。

## 备选方案

- **只读单个根 AGENTS.md**：无法表达 monorepo 子项目差异。
- **把项目记忆塞进语义库**：项目约束需稳定全量呈现，不适合 top-k 模糊召回。
