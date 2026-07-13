# Skills 设计与实现方案

> 一句话:把领域知识、工作流程、脚本以**目录**形式放在磁盘上,让 agent **按需**取用,
> 而不是一次性塞进提示词。三层渐进加载(progressive disclosure),零外部依赖,
> 用 `io/fs` 装载,以一个 `prompt.Section` + 一个 `tool.Tool` 接入既有框架。

## 1. 动机

随着 agent 能力变多,把所有领域说明、流程、示例、脚本都写进 system prompt 会:上下文爆炸、
彼此干扰、且与具体任务无关的内容也常驻。**Skills** 把每项能力收敛成一个磁盘上的目录
(一个 `SKILL.md` + 可选资源/脚本),启动时只加载**元数据**,真正用到时模型才用工具
逐层拉取正文与资源——按需、聚焦、可扩展。

设计严格沿用框架既有理念:**接口尽量小、子包隔离、零外部依赖(纯 stdlib)、能力即可注入的
工具 / Section**(对齐 `tool/web`、`tool/exec`、`prompt`)。

## 2. 三层渐进加载

| 层级 | 内容 | 何时进入上下文 | 落点 |
|---|---|---|---|
| **Level 1** | 每个 skill 的 `name` + `description` | **总是**(每轮注入 system prompt) | `skills.PromptSection` → `prompt.Section` |
| **Level 2** | `SKILL.md` 正文(详细步骤/约束/示例) | 模型调用 `use_skill` 时 | `skills.Tool` → `use_skill` 工具 |
| **Level 3** | 目录内资源文件(`forms.md`…)与脚本(`scripts/*`) | 资源:`use_skill` 传 `resource`;脚本:`run_skill_script` 执行 | `skills.Tool` 读 + `skills.ScriptTool`(经 `sandbox`)跑 |

核心保证:**大正文在被显式请求前,既不常驻内存、也不进提示词**。Level 1 只渲染 name+description,
正文与资源在 `use_skill` 被调用时才读盘。

## 3. 目录约定

一个 skill 就是一个目录,目录内必须有 `SKILL.md`:

```
skills/
  greet/
    SKILL.md            # frontmatter(name/description/allowed-tools) + Markdown 正文
    template.md         # 可选资源(Level 3 读)
    scripts/
      greet.sh          # 可选脚本(Level 3 经 run_command 执行)
      greet.bat
```

`SKILL.md` 头部是 YAML frontmatter:

```markdown
---
name: greet
description: Compose a friendly greeting and run the bundled greeter script
allowed-tools: [use_skill, run_command]
---
# Greeting workflow

1. Call `use_skill` with `resource: "template.md"` ...
2. Run the bundled greeter with `run_command` ...
```

字段:
- `name`(必填):模型用来调用该 skill 的唯一名字。缺失则该目录在装载时被跳过并汇总报告。
- `description`:一行摘要,进 Level 1 提示词。
- `allowed-tools`:**咨询性**工具清单。会在 `use_skill` 返回正文顶部以 `Allowed tools: ...` 行
  呈现给模型,告知它应使用哪些工具,但**不**在 turn 引擎层硬拦截(见 §7)。支持
  `[a, b]` 内联或多行 `- item` 块两种写法。

## 4. 架构与包划分

```
skills/                         基元(零依赖,纯 stdlib)
  skill.go                     Skill 类型 + Library 装载器(Load/LoadDir/List/Get/Instructions/Resource)
  frontmatter.go               最小 YAML frontmatter 解析器(本特性所需子集)
  tool.go                      Tool(lib) → use_skill 工具(Level 2 + 3 读)
  script.go                    ScriptTool(lib, sb) → run_skill_script 工具(Level 3 执行脚本)
  section.go                   PromptSection(lib) → prompt.Section(Level 1)
  *_test.go                    + testdata/skills/ 固件
examples/skills/main.go        三层端到端演示(embed 技能库 + mock + 进程沙箱)
docs/SKILLS.md                 本文档
docs/adr/0015-skills.md        ADR
```

依赖方向:`skills` 导入 `prompt`、`tool`、`core`;`prompt`/`tool` **不**导入 `skills`,故无环——
与 `tool/web`、`tool/exec` 在子包提供开箱即用能力的模式同构。

## 5. 装载:用 `io/fs`

```go
func Load(fsys fs.FS) (*Library, error)   // 扫描 "*/SKILL.md",解析 frontmatter
func LoadDir(root string) (*Library, error) // = Load(os.DirFS(root)) 便捷封装
```

用 `io/fs` 的好处:一套代码同时支持

- **磁盘目录**:`skill.LoadDir("./skills")`
- **编译进二进制**:`//go:embed skills` + `fs.Sub` + `skill.Load`
- **零磁盘单测**:`testing/fstest.MapFS`

`Load` 只解析 frontmatter 得 name/description/allowed-tools(Level 1 必需),正文/资源不读。
缺 `name` 的目录、重名的 skill 会被跳过并**汇总成一个错误**返回(有效的 skill 仍然装载成功)。

`Library` 提供 `List()`(按名排序,稳定)、`Get(name)`、`Len()`。

## 6. 接入 agent

```go
import (
    "github.com/jiujuan/goagent/prompt"
    "github.com/jiujuan/goagent/skills"
)

lib, _ := skills.LoadDir("./skills")

ag := agent.New(agent.Config{
    Model: model,
    Prompt: prompt.New().
        Add(prompt.Identity("...")).
        Add(skills.PromptSection(lib)),          // Level 1:列出可用技能
    Tools: []tool.Tool{
        skills.Tool(lib),                        // use_skill:Level 2 + 3 读取
        skills.ScriptTool(lib, sb),              // run_skill_script:Level 3 执行脚本
    },
})
```

`use_skill` 入参 `{ name string, resource?: string }`:
- 只给 `name` → 返回带标题/`Allowed tools:` 行的 `SKILL.md` 正文(Level 2)。
- 再给 `resource` → 返回该资源文件原文(Level 3 读)。
- 未知技能名、越界资源路径 → `tool.ErrorResult`(作为数据回报给模型,可纠正),而非 Go error。

## 7. 执行技能脚本:`run_skill_script`

`skill.ScriptTool(lib, sb, opts...)` 让模型直接运行某技能里 bundled 的脚本(Level 3 执行)。
入参 `{ skill string, script string, args?: []string }`,流程:

1. 按名取技能,经 `Resource`(路径越界守卫)读出脚本字节——**无论技能来自磁盘还是
   `embed.FS` 都可用**。
2. 按扩展名选解释器(默认 `.sh`→`sh`、`.bash`→`bash`、`.py`→`python3`、`.js`→`node`;
   用 `WithInterpreter(ext, command)` 覆盖或新增,如 `WithInterpreter(".py", "python")`、
   `WithInterpreter("rb", "ruby")`)。
3. 把脚本写入临时文件,交给 `sandbox` 以**绝对路径**执行(故脚本的 cwd 是沙箱 WorkDir,不是
   技能目录;脚本要读 bundled 数据文件应让模型用 `use_skill` 取,而非相对自身打开)。
4. `Outcome` 格式化为模型可读报告;非零退出/超时/未知技能/越界路径 → `tool.ErrorResult`,
   解释器不在白名单等策略违规 → Go error。

```go
import (
    "github.com/jiujuan/goagent/sandbox"
    "github.com/jiujuan/goagent/sandbox/process"
)

sb, _ := process.New(sandbox.Policy{
    WorkDir:         workDir,
    Timeout:         5 * time.Second,
    MaxOutputBytes:  16 << 10,
    AllowedCommands: []string{"sh", "python3", "node"}, // 解释器须显式放行
    Env:             map[string]string{"PATH": os.Getenv("PATH")},
})

tools := []tool.Tool{
    skills.Tool(lib),
    skills.ScriptTool(lib, sb), // run_skill_script
}
```

脚本执行的全部限制(超时、输出上限、工作目录、env/命令白名单)都由所选 `sandbox` 后端强制
([ADR 0012](adr/0012-sandbox.md))——`skill` 包只负责"把技能脚本喂给沙箱",隔离强度由后端决定。

## 8. 关键决策

- **frontmatter 解析自己写**:零依赖约束下不引入 YAML 库。只支持本特性所需的最小子集
  (`key: scalar` 去引号、`[a, b]` 内联列表、`- item` 块列表),放在独立文件便于单测。
- **`allowed-tools` 先咨询、后强制**:解析并呈现给模型,但不在 turn 引擎硬门禁。硬门禁需运行时
  "当前激活技能"追踪 + 工具网关,侵入执行路径;`Skill.AllowedTools` 字段已为将来强制预留。
- **资源路径限定在 skill 目录内**:`Resource(name)` 用 `fs.ValidPath` 拒绝 `..`、绝对路径、
  空路径,任何越界返回 `ErrResourceEscapes`,防止借资源读越读到技能目录之外。
- **脚本执行复用 sandbox,不另造**:`run_skill_script` 把技能脚本喂给既有 `sandbox`
  ([ADR 0012](adr/0012-sandbox.md))执行,隔离强度由后端决定;skill 包本身只负责"读+喂",
  执行的受控隔离交给沙箱。脚本以临时文件 + 绝对路径运行,故 embed.FS 里的技能脚本也能跑。

## 9. 端到端示例

`examples/skills` 用 `//go:embed` 把一个 `greet` 技能编进二进制,mock 模型脚本化走完三层:

```
go run ./examples/skills
```

链路:列出技能(Level 1 在 prompt)→ `use_skill{name:greet}` 读正文(Level 2)→
`use_skill{name:greet, resource:"template.md"}` 读资源(Level 3 读)→
`run_skill_script{skill:greet, script:"scripts/greet.sh"}` 经沙箱跑脚本(Level 3 执行)→ 总结。
纯 mock + 进程沙箱,无需 API key / 网络;示例自动探测 host 上可用的解释器
(sh/bash/python3/python/node)并选对应的 bundled 脚本。

## 10. 测试策略

- `frontmatter_test.go`:标量/内联列表/块列表/引号/注释/CRLF+BOM/无 frontmatter/未闭合 fence。
- `skill_test.go`(`fstest.MapFS` + `testdata/`):装载与排序、缺 name 汇总报错、重名报错、
  读正文、读资源、路径越界拒绝(`ErrResourceEscapes`)。
- `tool_test.go`:`use_skill` 返回含 `# Skill:` 头与 `Allowed tools:` 行的正文、资源原文返回、
  未知技能/越界路径为 tool error。
- `script_test.go`(假 sandbox 回读临时脚本,免装真解释器):按扩展名选解释器、args 追加、
  临时文件清理、未知技能/不支持扩展/越界路径/非零退出为 tool error、`WithInterpreter` 覆盖。
- `section_test.go`:渲染 `# Active Skills` + 排序条目;空库/`nil` 库渲染为空(被 Builder 丢弃);
  Order 落在 300~400 之间。

## 11. 取舍小结

- **三层渐进加载**:Level 1 常驻、Level 2/3 按需,正文/资源请求前不进上下文,控制 token 与干扰。
- **`io/fs` 装载**:磁盘 / embed / 测试三态一套代码,零依赖。
- **小接口接入**:一个 `prompt.Section` + 两个 `tool.Tool`(读 `use_skill`、跑 `run_skill_script`),
  不改 turn 引擎,与现有扩展点同构。
- **读写分离**:skills 负责"读+喂",脚本"执行"交给既有 sandbox,职责清晰、可独立替换。
- **读写分离**:skills 只负责"读"(正文/资源),"执行"交给既有 sandbox,职责清晰、可独立替换。
