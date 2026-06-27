# ADR 0018：长期文本记忆 —— Markdown 一事一文件 + 索引注入

状态：提议

## 背景

语义记忆（ADR 0010）擅长海量、模糊召回，但有两个短板：内容不可读、不可手工编辑、
依赖 embedding。许多长期记忆其实是**策展的、精确的、需要人能看懂能改**的事实
（用户偏好、项目约定、纠正过的反馈）。这类记忆更适合纯文本，而非向量。

参考 Claude Code 的 `memory/` 目录模型：一事一文件（Markdown + frontmatter），一个
`INDEX.md` 进 context 做"目录"，按需读全文。零 embedding，可 `git diff`。

## 决策

新增 `textmem` 包：磁盘 Markdown 文件 + 一个索引文件，复用 `skill` 包已有的 frontmatter
解析思路（ADR 0015）。

```go
type Entry struct {
    Name string   // kebab-case slug = 文件名
    Desc string   // frontmatter，进索引
    Type string   // user|feedback|project|reference
    Body string
}
type Store interface {
    Save(ctx, Entry) error
    Read(ctx, name string) (Entry, error)
    Index(ctx) ([]Entry, error)   // 仅 Name+Desc，喂索引
    Delete(ctx, name string) error
}
func File(dir string) Store        // <dir>/<name>.md + <dir>/INDEX.md
```

集成件：
- **`textmem.IndexSection(store)`**（Order=40）：把索引渲染为一行一条 `- name — desc`，
  让模型知道"有哪些记忆可读"。
- **`textmem.SaveTool` / `textmem.ReadTool`**：LLM 写入新记忆 / 按 name 读全文。

与语义记忆的分工：**文本记忆 = 策展、精确、可编辑**；**语义记忆 = 海量、模糊召回**。
二者可同时挂载，互补不互斥。

## 理由

- **可读可编辑可审计**：纯文本 + git，人能直接看懂改正，向量库做不到。
- **复用 frontmatter 解析**：与 SKILL.md 同一套，零新依赖。
- **索引-按需读两段式**：索引小、常驻；全文大、按需，控制 token 占用。

## 后果

- 索引随条目增多而变大，需 `Desc` 精炼；超量时受 ADR 0016 预算约束截断（保留高优先类型）。
- 写入由 LLM 驱动，可能产生重复/过时条目；去重与清理策略与语义记忆共用（见 ADR 0019）。
- 文件并发写需加锁，与 `session.FileStore` 同等处理。

## 备选方案

- **只用语义记忆**：丢失可读性与可编辑性，无法表达"用户明确的偏好/纠正"。
- **单一大文件存全部记忆**：无法按需读、diff 噪声大、并发写困难。
