---
name: commit-message
description: 把一段改动描述整理成符合 Conventional Commits 规范的提交信息（纯知识技能，无脚本）
allowed-tools: [use_skill]
---
# Conventional Commits 规范

这是一个**纯知识技能**：没有脚本，读完本说明后你应直接生成提交信息文本。

格式：
```
<type>(<scope>): <subject>

<body>
```

- `type` 取值：feat / fix / docs / style / refactor / perf / test / build / ci / chore。
- `scope` 可选，用来标注受影响的模块（如 `auth`、`api`）。
- `subject` 用祈使句、现在时、首字母小写、结尾不加句号，建议 ≤ 50 字符。
- `body` 可选，解释「为什么」而非「怎么做」，与标题之间空一行。
- 破坏性变更：在 body 前加 `BREAKING CHANGE:` 段落。

示例：
```
fix(auth): increase login timeout to 30s

Slow upstream IdP responses were tripping the 10s client timeout,
causing spurious 504s during peak hours.
```
