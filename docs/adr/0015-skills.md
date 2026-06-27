# ADR 0015:基于文件系统的技能系统(skill 包 + 三层渐进加载 + use_skill 工具)

状态:已接受

## 背景

随着 agent 能力增多,把所有领域说明、工作流程、示例、脚本都写进 system prompt 会导致上下文
膨胀、内容互相干扰,且与当前任务无关的部分也常驻。需要一种机制把"能力"以**目录**形式放在
磁盘上,启动时只加载元数据,真正用到时才逐层拉取详细说明与资源。约束:沿用框架理念——接口
尽量小、后端/能力子包隔离、**零外部依赖(纯 stdlib)**。

## 决策

引入 `skill` 基元包,按**三层渐进加载(progressive disclosure)**实现 Skills:

- **Level 1(总是加载)**:`skill.PromptSection(lib)`(实现 `prompt.Section`)把每个 skill 的
  `name` + `description` 注入 system prompt 的 "Active Skills" 区。
- **Level 2(按需读取)**:`skill.Tool(lib)`(`use_skill` 工具)按名返回 `SKILL.md` 正文。
- **Level 3(按需引用/执行)**:同一个 `use_skill` 读取技能目录内的资源文件;脚本的**执行**
  由 `skill.ScriptTool`(`run_skill_script` 工具)经 `sandbox`([ADR 0012](0012-sandbox.md))
  完成——按扩展名选解释器,把脚本喂给沙箱,故磁盘与 `embed.FS` 里的技能脚本都能跑。

装载基于 `io/fs`:`Load(fsys fs.FS)` 扫描 `*/SKILL.md`,`LoadDir(root)` 是 `os.DirFS` 便捷封装。
一个 skill 目录含一个 `SKILL.md`(YAML frontmatter:`name` / `description` / `allowed-tools` +
Markdown 正文),外加可选资源与脚本。

```go
func Load(fsys fs.FS) (*Library, error)
func (l *Library) List() []*Skill
func Tool(lib *Library) tool.Tool                              // use_skill(name, resource?)
func ScriptTool(lib *Library, sb sandbox.Sandbox, ...) tool.Tool // run_skill_script(skill, script, args?)
func PromptSection(lib *Library) prompt.Section
```

## 理由

- **三层加载控制上下文**:Level 1 只放 name+description;正文/资源在 `use_skill` 被调用前
  既不进提示词也不读盘。token 与干扰可控。
- **`io/fs` 装载**:磁盘目录(`LoadDir`)、编译进二进制(`embed.FS` + `Load`)、零磁盘单测
  (`fstest.MapFS`)三态一套代码,零依赖。
- **小接口接入,不动核心**:一个 `prompt.Section` + 一个 `tool.Tool` 即接入,不改 turn 引擎;
  `skill` 导入 `prompt`/`tool`/`core` 而它们不反向导入,无环,与 `tool/web`、`tool/exec`
  子包模式同构。
- **自写 frontmatter 解析**:零依赖约束下不引 YAML 库;只支持所需子集(标量去引号、内联/块列表)。
- **读写分离**:skill 包负责"读"(正文/资源)与"把脚本喂给沙箱";脚本的受控**执行**
  (`run_skill_script`)交给既有 `sandbox`,隔离强度由后端决定,职责清晰、可独立替换。脚本以
  临时文件 + 绝对路径运行,故 `embed.FS` 里的技能脚本也能执行。

## 后果

- `allowed-tools` 为**咨询性**:解析并在 `use_skill` 返回正文顶部呈现给模型,但**不**在 turn
  引擎硬拦截。硬门禁需运行时"当前激活技能"追踪 + 工具网关,侵入执行路径,留作后续;
  `Skill.AllowedTools` 字段已为其预留。
- frontmatter 解析仅覆盖本特性所需的最小 YAML 子集,不是通用 YAML 解析器(嵌套 map、锚点、
  多行标量等不支持)。
- 资源读取用 `fs.ValidPath` 限定在技能目录内,拒绝 `..`/绝对路径/空路径(`ErrResourceEscapes`);
  脚本的隔离由所选 `sandbox` 后端决定,不在 skill 包范围。

## 备选方案

- **靠通用文件读工具(Read/Bash)让模型自己打开 SKILL.md**:不够聚焦且越权——模型可读技能目录
  之外的任意文件;`use_skill` 把访问限定在已装载的技能与其目录内,更安全、语义更清晰。
- **把 skill 正文全部塞进 system prompt**:回到上下文膨胀的老问题,正是本方案要避免的。
- **引入 YAML 库解析 frontmatter**:破坏"零外部依赖";所需字段简单,最小解析器足够。
- **靠通用 `run_command` 跑技能脚本**:要求脚本先落在沙箱 WorkDir 里,对 `embed.FS` 装载的技能
  不成立,且模型得自己拼解释器+路径;`run_skill_script` 按技能名取脚本(含越界守卫)、按扩展名
  自动选解释器、临时落盘后经同一 `sandbox` 执行,既适配 embed 又更省心,沙箱限制照旧生效。
- **在 turn 引擎层按 `allowed-tools` 硬门禁工具**:侵入核心执行路径、需运行时激活态追踪;
  先做咨询性呈现,把强制留给将来更完整的设计。
