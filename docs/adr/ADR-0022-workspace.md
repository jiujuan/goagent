# ADR-0022: Workspace —— 把"工作区"提升为一等装配边界

- 状态: Proposed
- 日期: 2026-09-24
- 关联: ADR-0016(Section 排序)、ADR-0017/0018(working/text memory)、ADR-0020(projectmem)、ADR-0021(rules)

## 背景

pi 编码智能体把 **Workspace** 做成核心对象：它同时承载工作目录(cwd)、目录下的代码状态、配置、拥有的技能和安全权限，Agent 的一切上下文都从这个边界派生。

goagent 目前没有 Workspace 抽象——全仓 `workspace` 一词零命中。相关能力分散在若干正交包里，由调用方在 `main()` 手工装配。这带来四类问题：

1. **"工作区根在哪儿"有三处互不认账的实现。**
   - `memory/projectmem` 从起点向上走，遇到含 `.git` 的目录即停并把该目录纳入(`memory/projectmem/projectmem.go:36-50`)——这是仓库根检测。
   - `sandbox.Policy.WorkDir` 要求调用方预先给出绝对且已存在的目录，构造期校验(`sandbox/process/process.go:34-40`)——它只是命令的**起始目录**，不参与根的发现。
   - 文件越界防护第三遍出现在应用里：`examples/task-runner/main.go:161-171` 手写了 `inside(work, p)`。
2. **模型被告知的 cwd 与真实执行目录可能不同。** `prompt.Environment()` 渲染的是进程的 `os.Getwd()`(`prompt/sections.go:57-61`)，而 `run_command` 跑在 `sandbox.Policy.WorkDir` 下(`sandbox/process/process.go:70`)。两者一旦分叉，模型对"我在哪儿干活"的判断是错的，且没有任何机制提醒它。
3. **装配样板膨胀。** `memory/memx` 已经证明"一个 Config 换来 Sections/Tools/Middleware 三组挂载物"是有效收敛点(`memory/memx/memx.go:27-60,75-125`)，但工作区侧没有对应物：每个想要"在某个仓库里干活"的例子都得重新串一遍 rules / projectmem / sandbox / 文件工具 / 越界守卫。
4. **技能没有来源层次，声明的权限也不生效。** `skills.Load` 一次只扫一个 root、`Library` 建好即不可变(`skills/skill.go:111-157`)，所以"全局技能 + 工作区技能"没有合并入口；而 frontmatter 里的 `allowed-tools` 只被原样打印进技能正文给模型看(`skills/skill.go:44-46`、`skills/tool.go:56-58`)，任何一层都不校验——技能声称自己能用什么工具，与实际能用什么工具，是两件互不相干的事。`memory/rules` 早就做成了 global/project 两级合并、同 ID 项目胜(`memory/rules/rules.go:37-49`)，技能侧缺同一套形状。

## 名词

- **工作区(Workspace)**：一个绝对目录及其派生物——框架文件工具的可见范围、命令的执行目录、项目记忆与规则的发现起点、提示词中"我在哪儿"的权威答案。
- **仓库根(Repo Root)**：从起点向上走、第一个含 `.git` 的目录；无 `.git` 时退化为起点本身。
- **包含(containment)**：一次文件操作的目标位置落在工作区之内这一性质。

## 决策概览

新增 `workspace` 包作为**装配层**，形态对齐 `memx`（Config 进、挂载物出），并复用仓库里既有的叶子件——只做加法，不改任何既有语义：`prompt` 加一个选项、`Builder.Add` 放宽为变长（源码兼容），`skills` 加一个多来源加载入口和一个可选中间件（见"迁移与兼容性"）：

| 工作区职责 | 由谁承担 | workspace 提供的形态 |
| --- | --- | --- |
| 根目录发现 | `internal/reporoot`(新，自 projectmem 抽出) | `New` 时解析，`ws.Root()` 读出 |
| 文件读写约束 | `tool/file`(新) + 标准库 `os.Root` | `ws.Tools()` |
| 命令执行目录 | `sandbox` + `sandbox/process` | `ws.SandboxPolicy(base)` |
| 项目上下文 | `memory/rules`、`memory/projectmem` | `ws.Sections()` |
| 代码状态(只读) | `workspace` 自带 git 探测 | `ws.Git()`，并渲染进 Section |
| 配置信息 | 既有 `config` 的 `WithPath` | 无需新代码，文档给出用法 |
| 安全权限 | 既有 `middleware.Permission` 不变 | 越界由 `tool/file` 直接判为工具错误 |
| 技能与免审名单 | `skills`(加 `LoadDirs` / `Gate`) | `ws.Skills()` + `ws.SkillTools()` + `ws.Gate()` |

一句话：**workspace 不实现新机制，它把已有机制钉在同一个根目录上。** 唯一带新判断的代码是 `skills.Gate`——它也只不过把早就存在的 `Interrupt` 通道(`middleware/permission.go:61`)接到技能声明的名单上。

## 范围

### 做

- 工作区根解析，并与 `projectmem` 共用同一份实现（消除上文重复）。
- 基于 `os.Root` 的框架级文件工具（read/write/list/glob），越界一律回工具错误而不是 Go error，与"工具错误是数据"的既有立场一致(`sandbox/sandbox.go:58-60`)。
- `ws.Sections()`：仓库根内的 AGENTS.md、global+project 规则、一个新增的 `# Workspace` 事实段（根路径、是否 git 仓库、分支、HEAD、是否脏），以及配置了技能目录时的 skills Level-1 段。
- 只读 git 状态探测：是否 git 仓库、当前分支、HEAD 提交、工作树是否脏，不引入新的第三方依赖。
- 补 `prompt.WithWorkingDir(dir)` 选项，让 `Environment` 段能报告**真实执行目录**而非 `os.Getwd()`。
- `examples/task-runner` 迁移为用 workspace + tool/file，作为示范。
- 技能多来源合并：`skills.LoadDirs(global, <root>/.goagent/skills)`，同名时工作区胜（与 `rules` 同策略）；workspace 通过 `ws.Skills()` 交出合并后的 `Library`。
- 把 `AllowedTools` 变成**免审名单**：`skills.Gate(lib)` 中间件对"已激活技能名单内"的工具放行，名单外的工具调用转 HITL 人工批准。不新增控制流原语，复用既有 `Interrupt` + `Run.Decide/Resume` 链路。

### 不做（及理由）

- **不承诺命令级牢笼。** `sandbox/process` 只做 `cmd.Dir`，子进程可以 `cd ..` 出去；包注释已把强隔离留给未来容器后端(`sandbox/sandbox.go:1-11`)。workspace 的包含性只对**框架自己的**文件工具成立，对 `run_command` 起的进程不成立。文档与 `# Workspace` 段都必须把这句话说清楚，不能让模型误以为越界会被拦。
- **不改 `middleware.Rule` 签名。** 见"备选方案 B"。
- **不做 VCS 写操作**（提交、分支切换、stash、回滚），也不做工作区快照/事务。
- **不做工作区级配置 schema。** 既有 `config.Load(config.WithPath(ws.Root()))` 已能把 `config.yaml` / `config.local.yaml` 的搜索路径指到工作区，零新代码。
- **不改 `skills` 的三级披露结构与既有加载器。** 只做加法：新增 `LoadDirs` 与可选的 `Gate`，`Load`/`LoadDir`/`PromptSection`/`ScriptTool` 一律不动。`use_skill` 内部换实现（为携带 State 写入），但对模型可见的名称、描述、schema 不变；不挂 Gate 时行为与现状一致，唯一例外是加载文本里那行 allowed-tools 的措辞（见 §6 末）。
- **不改 `vfs` / `checkpoint` 的语义。** `core.State.Files` 是随 run 走的内存虚拟工件区(`core/state.go:11-24`、`vfs/instate.go:16-21`)，与磁盘工作区是两回事；`checkpoint` 快照的是 run State 而非工作区文件。本 ADR 不桥接二者，只在"非目标"里记下这条区别，避免读者误认。

## 包布局与依赖方向

```
workspace/            # 装配层：只 import 下面的包 + stdlib
  workspace.go        # Config / Workspace / New / Root / FS / Close / Skills
  policy.go           # SandboxPolicy / Sandbox
  section.go          # Section / Sections
  gate.go             # Gate()：把 skills.Gate 交出去（未启用时为 nil）
  git.go              # 只读 git 探测
tool/file/            # 只 import tool + stdlib；不知道 workspace 的存在
                      # 包名取 file 而非 fs：本包要配 root.FS() 用标准库 io/fs 的
                      # Glob/ValidPath 家族，叫 fs 就得给 io/fs 起别名
skills/               # 既有包，两处加法：loaddir.go(多来源合并) + gate.go(名单门控)
internal/reporoot/    # 只 import stdlib；被 workspace 与 projectmem 共用
```

依赖 DAG 保持单向：`workspace → tool/file, sandbox, prompt, memory/rules, memory/projectmem, skills, agent, internal/reporoot`。`skills.Gate` 为拿到 `agent.Middleware` 类型而要 import `agent`；这与 `middleware` 包的做法同构(`middleware/permission.go:3-6`)，且 `agent` 与 `skills` 今天互不引用（全仓仅 `examples/` 引用 `skills`），不构成环。`workspace` 引用 `agent` 也有 `memory/memx` 的先例(`memory/memx/memx.go:4`)。

**为什么需要 `internal/reporoot`**：若把仓库根走查直接放进 `workspace`，`projectmem` 想复用就得 import `workspace`，而 `workspace` 又要 import `projectmem` 拿 Section——成环。把走查下沉成叶子包，`projectmem.Load` 改为委托，行为与既有测试不变。

## API 草案

命名遵循仓库既有约定：构造函数 `New*`（`os.OpenRoot` 是标准库既有调用，不在此列）。

```go
// workspace/workspace.go
package workspace

// Config 选择挂载哪些部件。四个目录字段留空即用约定路径，约定路径下没有目录就跳过
// （不是错误）：
//
//	GlobalRulesDir    → $HOME/.goagent/rules
//	ProjectRulesDir   → <root>/.goagent/rules
//	GlobalSkillsDir   → $HOME/.goagent/skills
//	ProjectSkillsDir  → <root>/.goagent/skills
//
// 本 ADR 不提供显式 Disable 开关：要换来源就传自己的路径，要没内容就不建目录。
// 真出现"目录存在但必须屏蔽"的场景时再加字段，不提前设计。
type Config struct {
	// Dir 是解析根的起点。空表示当前工作目录。
	Dir string
	// ResolveRoot 为真时向上走查 .git 作为仓库根；否则 Dir 的绝对路径即根。
	ResolveRoot bool

	// 规则两级合并，同 ID 时项目胜(ADR 0021)。
	GlobalRulesDir  string
	ProjectRulesDir string

	// 技能多来源合并，同名时工作区胜（与 rules 同策略）。
	GlobalSkillsDir  string
	ProjectSkillsDir string

	// ProjectMemory 控制是否从根目录发现 AGENTS.md(ADR 0020)。默认关：注入整份
	// 项目记忆会显著改变提示词，交给调用方显式决定。
	ProjectMemory bool

	// Git 控制是否做只读 git 探测并渲染进 Section。
	Git bool

	// SkillGate 为真且技能库非空时，ws.Gate() 返回门控中间件。
	SkillGate bool
}

// Workspace 持有打开后的文件系统句柄，因此有生命周期。
type Workspace struct {
	root    string
	fs      *os.Root
	docs    []projectmem.Doc
	rules   *rules.Set
	skills  *skills.Library
	gitInfo GitInfo
}

func New(cfg Config) (*Workspace, error)

// Root 返回工作区绝对根路径（符号链接已解析）。
func (w *Workspace) Root() string
// FS 返回受包含约束的根句柄，供 tool/file 与调用方直接使用。
func (w *Workspace) FS() *os.Root
// Close 释放 FS 句柄。
func (w *Workspace) Close() error

// Sections 返回 rules、projectmem、workspace 事实三段，外加配置了技能目录时的
// skills Level-1 段（按 ADR-0016 的 Order 排序：50 / 150 / 210 / 350）。
func (w *Workspace) Sections() []prompt.Section
// Tools 返回 read_file / write_file / list_dir / glob 四个绑定了本工作区的工具。
func (w *Workspace) Tools() []tool.Tool

// Skills 返回按 global → project 合并后的技能库；未配置技能目录时为 nil。
func (w *Workspace) Skills() *skills.Library
// SkillTools 返回 use_skill 与 run_skill_script；后者通过 sb 执行技能自带脚本，
// 所以 sb 的 Policy.AllowedCommands 必须包含解释器名（见 skills.ScriptTool 文档）。
func (w *Workspace) SkillTools(sb sandbox.Sandbox) []tool.Tool
// Gate 返回技能免审门控中间件；Config.SkillGate 为假或没有技能库时返回 nil（不挂载）。
func (w *Workspace) Gate() agent.Middleware

// SandboxPolicy 把 base 的 WorkDir 填成 Root，其余字段原样保留。
// base 的 WorkDir 若非空且与 Root 不同，返回错误——不静默覆盖调用方的意图。
func (w *Workspace) SandboxPolicy(base sandbox.Policy) (sandbox.Policy, error)
// Sandbox 是便捷包装：以 SandboxPolicy 的结果构造 process 后端。
func (w *Workspace) Sandbox(base sandbox.Policy) (*process.Sandbox, error)

// Git 返回只读探测结果（非 git 仓库时 Valid 为 false，不是错误）。
type GitInfo struct {
	Valid       bool
	RepoRoot    string // .git 所在目录
	Branch      string // 空表示 detached 或未知
	HeadCommit  string // 完整 40 位，可能为空
	Dirty       bool   // 仅当调用过 Dirty 的探测方式时有意义，见下文
	StatusErr   error  // Dirty 探测失败的原因；非致命，nil 表示探测成功
}
func (w *Workspace) Git() GitInfo
```

```go
// tool/file/file.go
package file

// Tools 基于 root 构造四个模型可见工具。root 是 *os.Root，
// 因此越界（含指向外部的符号链接）由操作系统层面拒绝，本包不再手写路径清洗。
func Tools(root *os.Root) []tool.Tool

// 单独暴露，便于调用方只挑其中几个：
func ReadFile(root *os.Root) tool.Tool   // read_file
func WriteFile(root *os.Root) tool.Tool  // write_file(自动 MkdirAll 父目录)
func ListDir(root *os.Root) tool.Tool    // list_dir
func Glob(root *os.Root) tool.Tool       // glob
```

`prompt` 侧唯一的增量（非破坏）：

```go
// WithWorkingDir 覆盖 Environment 段报告的 "Working directory"。
// 用于执行目录与进程 cwd 不同的场景（沙箱内运行、多工作区）。
func WithWorkingDir(dir string) EnvOption
```

`prompt.Builder.Add` 现为单 Section(`prompt/builder.go:20`)，而 workspace 一次给多段。TASK-WS-03 把签名改为变长 `Add(sections ...Section) *Builder`：现有 `Add(x)` 调用点(`examples/prompt`、`examples/skills`、`examples/memory-layers`)无需改动即可编译，属于源码兼容的放宽。

`internal/reporoot`：

```go
// package reporoot
// Resolve 从 startDir 向上走查，返回有序的目录列表（叶子在前），
// 走查在首个含 .git 的目录（含该目录）或文件系统根处停止。
func Resolve(startDir string) ([]string, error)
// Root 是 Resolve 的叶子视角：返回仓库根绝对路径。
func Root(startDir string) (string, error)
```

`skills` 侧的两处加法：

```go
// skills/loaddir.go
// LoadDirs 依次扫描多个目录合成一个 Library：后扫描的目录里的同名技能覆盖先扫描的。
// 调用方按 global → project 顺序传入，于是工作区技能胜（与 memory/rules 同策略）。
func LoadDirs(dirs ...string) (*Library, error)
```

```go
// skills/gate.go
// ActiveKey 是 State.KV 里记录"本 run 已加载过哪些技能"的键，由 use_skill 写入。
const ActiveKey = "skills.active"

// Active 读出 st 中已激活的技能名（缺失或形态不符时返回 nil）。
func Active(st *core.State) []string

// Preapproved 返回这些已激活技能声明的 allowed-tools 并集，外加技能工具自身
// （use_skill / run_skill_script）——后者必须免检，否则无法完成加载这一步。
func Preapproved(lib *Library, active []string) map[string]bool

// Gate 是可选中间件：没有技能被激活时它恒放行（现状行为）；有激活技能且其 allowed-tools
// 非空时，名单外的工具调用转为 HITL 中断（人批准即执行，拒绝即把理由回喂模型）。
func Gate(lib *Library) agent.Middleware
```

`use_skill` 需要能写 State：`skills.Tool` 目前用泛型 `tool.New`(`skills/tool.go:21-45`)，其 handler 只能返回 `(string, error)`，携带不了 `Result.State`。改为手写 `tool.Tool` 实现即可——`memory/workingmem` 的 `updateTool`(`memory/workingmem/tool.go:25-73`) 就是同一形态的现成范式（读 `ctx.State` 拿旧名单、返回 `OpSetKV` 写回）。工具名、描述、schema 均不变。

## 关键设计

### 1. 包含性交给 `os.Root`，不再手写 `inside()`

`os.DirFS` **不做**符号链接约束——其文档明确说明 `/prefix/file` 若是指向树外的符号链接，DirFS 挡不住，且相对路径的根会受后续 `Chdir` 影响(`os.DirFS` doc)。`os.Root`（`os.OpenRoot`）则相反：任何路径分量若引用根外位置即返回错误，符号链接不得为绝对、不得指向根外，方法可并发使用。仓库 `go 1.25.0`，该原语可用。

因此：

- `tool/file` 的全部工具只接受 **相对路径**，绝对路径直接判为工具错误（模型该给相对路径，报错比"猜它想写哪儿"好）。
- 入参用 `filepath.ToSlash`/`FromSlash` 归一，正斜杠反斜杠都收。
- 写文件走 `root.MkdirAll` + `root.WriteFile`（均为 `os.Root` 现成方法，无需手工拼父目录）。
- glob 走 `root.FS()`（`os.Root` 提供的 `fs.FS` 视图）配标准库 `fs.Glob`，pattern 由 `fs.ValidPath` 家族约束。注意标准库语义是 `path.Match`：`*` **不跨目录分隔符**，没有 `**` 递归通配——这一点写进参数描述，否则模型会按 shell 习惯给出 `internal/**/x.go`。
- `list_dir` 同样走 `fs.ReadDir(root.FS(), name)`：`os.Root` 没有 `ReadDir` 方法（`WriteFile`/`MkdirAll`/`ReadFile` 有），根目录本身用 `fs.ValidPath` 认可的 `"."` 表示。
- `read_file` 有 1 MiB 上限，超出即截断并在结尾标注截断点；内容非合法 UTF-8 时拒绝返回并提示改用命令行读取。两者都是工具结果里的文本，不是 Go error。
- 越界错误**原样**回给模型，不改写、不脱敏（根路径本来就在 `# Workspace` 段里告知了模型，不存在泄漏问题）。

已知的平台怪癖要写进工具描述，因为模型会撞：Windows 上 `os.Root` 拒绝保留设备名（`NUL`、`COM1` 等）；Unix 上 `Chmod/Chown/Chtimes` 有符号链接竞态——本包的四个工具不使用这三个方法。

`os.Root` 不禁止跨文件系统边界、bind mount 与设备文件，所以包含性的准确表述是"路径解析不出根"，不是"进程被关进沙箱"。Section 与工具描述都用这个准确措辞，不写"安全沙箱"这种过强承诺。

### 2. 根解析只有一份实现

`projectmem.Load` 现在自带走查逻辑(`memory/projectmem/projectmem.go:36-50`)。TASK-WS-01 把该循环（含 `hasGit` 边界判定）平移到 `internal/reporoot`，`projectmem.Load` 变成 `reporoot.Resolve` + 逐目录读 AGENTS.md 的薄封装。`@import` 展开与"叶子文档最后、优先级最高"的排序保持不变，现有测试须全绿——这一步是纯搬迁，不夹带行为改动。

`workspace.New` 在 `ResolveRoot: true` 时用同一函数取根；起点无 `.git` 时根即起点绝对路径（不因找不到仓库而失败——很多任务就是在非 git 目录里干活）。根目录**本身不存在则是错误**：`os.OpenRoot` 失败即返回，`New` 不替你 `MkdirAll`（task-runner 例子保留它自己的建目录步骤）。建不建一个工作目录是调用方的决定，不该是装配的副作用。

约定分两层：用户级 `$HOME/.goagent/`（`rules/`、`skills/`）与工作区级 `<root>/.goagent/`（同名子目录）。这不是新发明——`config` 的默认搜索路径里早就有 `$HOME/.goagent`(`config/config.go:134`)，rules 本身也已经是 global/project 两级(`memory/rules/rules.go:37-49`)，本 ADR 只是把同一套命名推广到技能与根解析。工作区级目录应当出现在项目的 `.gitignore` 建议里：里面是本地规则与记忆，跨机器不一致。本 ADR 只定义默认路径，不创建目录，也不替调用方写 `.gitignore`。

### 3. git 状态：只读、不依赖新模块

- `RepoRoot`：从**工作区根**向上第一个含 `.git` 的目录，一直走到文件系统根（不受工作区根约束：只读三个文件，不碰任何路径）。所以根设在一个大仓库的子目录时，探测到的是外层仓库，Section 会写成 `in git repository <path>` 而不是假装根就是仓库。`.git` 是**目录**（普通仓库）还是**文件**（linked worktree，内容为 `gitdir: <path>`）都要处理；worktree 的 `HEAD` 在自己的 gitdir 里，refs 通过该目录下的 `commondir` 回到共享目录。
- `Branch` / `HeadCommit`：纯文件读，不调子进程。`HEAD` 里 `ref: refs/heads/main` 形式读 `refs/heads/main`；找不到时回落到 `packed-refs` 行扫；`HEAD` 直接是 40 位十六进制时视为 detached（Branch 为空、HeadCommit 有值）。**`git init` 后首次提交前** HEAD 指向的 ref 还不存在——这是"有分支、无提交"的有效仓库（`Valid: true`、`HeadCommit == ""`），不是意外结构；意外结构指 HEAD 既不是 ref 也不是哈希。
- `Dirty`：无法从文件可靠推断（要比对 index 与工作树），故走 `git -C <root> status --porcelain`，非空即脏（含未跟踪文件）。这条子进程**只读**、5 秒超时、环境变量给最小集：只透传 `PATH` 与 `SYSTEMROOT`，外加 `GIT_CONFIG_NOSYSTEM=1`、`GIT_TERMINAL_PROMPT=0`（绝不为凭据挂住）、`GIT_OPTIONAL_LOCKS=0`（status 会刷新 index，这条阻止它写）。失败时降级为 `Dirty == false` 且 `GitInfo` 带一个非致命的 `StatusErr`——探测失败不该让工作区不可用，但 Section 必须说 `working tree status unknown`，不能靠"没写 dirty"暗示干净。
- 探测函数签名是 `probeGit(root, status)`，脏探测以函数参数注入：这样"git 不在 PATH"这条降级路径可以确定性测试，而不必真的把 git 从环境里藏掉。端到端另有 `TestProbeGitAgainstRealRepository`（`git init`/`commit`/`worktree add` 真实跑一遍，缺 git 时 SKIP），用来钉住"我们假设的磁盘布局就是 git 实际写的那个"。
- 裸仓库（配置里 `core.bare = true`）：明确不支持，返回 `Valid: false`——裸库没有工作树可描述。子模块不需要特殊处理：它的 `.git` 文件指向 `.git/modules/<name>`，那里 HEAD/refs 齐全，报出来的分支与脏状态对该工作区就是正确的。
- 不引入 `go-git`：新增第三方依赖的评审成本高于本 ADR 的收益，且只需要三行摘要信息。

### 4. `# Workspace` Section 与 cwd 一致性

新增段沿用各记忆包导出 `Order` 常量的写法（`rules.Order = 50`、`projectmem.Order = 150`），workspace 取 `Order = 210`，落在 `Environment`(200) 与 `ToolGuidance`(300) 之间。内容为短事实陈述：

```
# Workspace
Root: /abs/path/to/repo (git repository, branch main, HEAD 0123456789ab, dirty)
File tools (read_file/write_file/list_dir/glob) are confined to this root.
Commands run with this root as their working directory, but are not jailed:
a command may read or write paths outside the root.
```

HEAD 取前 12 位（git 自己的默认缩写会随仓库增长变长，12 位足够让模型引用而不至于撞歧义）。

最后那句是刻意的——把"能力边界"和"约束边界"分开讲，避免模型（和读代码的人）误判。

同一轮里补 `prompt.WithWorkingDir(ws.Root())`，让 `Environment` 段报的 cwd 与 `SandboxPolicy` 填的 `WorkDir` 同源。这样背景 2 里的分叉在装配正确的情况下不可能再出现；`Environment()` 不带该选项时行为完全不变，兼容现有调用。

### 5. 权限：分工而非合并

- **路径级**（工作区内/外）由 `tool/file` 承担：越界即工具错误，模型能立刻看到并改路径。这属于"工具自身的输入校验"，不需要控制流介入。
- **工具名级**（哪些工具要人审、哪些禁用）继续由 `middleware.Permission` 承担(`middleware/permission.go:26-67`)，其 HITL 中断/恢复链路不变。

两者正交，各在其层。合并成一处（让权限中间件理解路径参数）是备选 B，理由见下。工具**名**粒度的第二道闸是技能免审名单，见 §6。

### 6. 技能：多来源合并与 allowed-tools 免审门控

**合并。** `skills.Load` 一次只扫一个 root 且 `Library` 建好即不可变(`skills/skill.go:88-157`)，所以新增 `LoadDirs(dirs ...string)`：按给定顺序逐个 `LoadDir`，后扫描的同名技能覆盖先扫描的，产出一个新的 `Library`。`workspace` 固定按 `GlobalSkillsDir → ProjectSkillsDir` 的顺序传，语义与 `rules.Load(globalDir, projectDir)` 完全对齐(`memory/rules/rules.go:37-49`)。既有 `Load` / `LoadDir` 不动。目录不存在就跳过（约定路径本来就是可选的），目录里某个技能坏了沿用 `Load` 的立场——能用的照样返回、错误里说明跳过了什么，但**跨目录同名不算坏**（那是覆盖）。

**激活。** `use_skill` 成功返回某技能的 SKILL.md 时（带 `resource` 的分支同理：都算"这个技能已在用"），把技能名追加进 `State.KV[skills.ActiveKey]`，通过 `Result.State` 的 `OpSetKV` 声明式提交(`core/state.go:34-48`)。选 KV 而不是进程内 map，理由是 KV 随 `State` 一起被 checkpoint 快照(`core/state.go:9-15`)，于是 resume / branch / 子 agent 传递都自然带着"当前激活了哪些技能"——门控状态得是可持久的事实，不能是中间件私有的记忆。存的是 **JSON 编码后的字符串**（`["pdf","csv"]`），跟 `memory/workingmem` 的 `wm:snapshot` 同一写法：KV 是 `map[string]any`，直接塞 `[]string` 过一趟 JSON 就变成 `[]any`，读取端得猜类型；存字符串则往返同构。已在名单里的技能不重复追加，加载失败不写名单，`ctx.State` 为 nil 时不返回 op。

**判定。** `Gate(lib).BeforeTool` 按序：

1. 无激活技能 → 放行。不挂 Gate 或没调用过 `use_skill` 时，行为与今天逐字节相同。
2. 激活技能**一个都没声明** `allowed-tools` → 放行。这条是刻意的：现网技能大多没写该字段，若把"未声明"当空集，挂上 Gate 会立刻打断所有既有例子。因此"没写"与"写了空列表"按同一处理，文档写死。名单里有已消失的技能名同样不贡献任何授权（既不放开也不收紧）。
3. 名单内（外加 `use_skill` / `run_skill_script` 本身——它们必须免检，否则加载这一步就过不去）→ 放行。
4. 名单外 → `core.Directive{Kind: core.Interrupt, Reason: "active skills (%s) do not list tool %q in their allowed-tools; approve to run it anyway"}`，与 `middleware.Permission` 用的是同一个原语(`middleware/permission.go:57-66`)。Reason 会原样进 HITL 提示，所以列出的是全部激活技能而非某一个：批准的人要能看出是谁的名单没盖住这次调用。

**与 Permission 共存。** `Stack.BeforeTool` 不短路，逐个问完再 `core.Resolve`(`agent/middleware.go:116-127`)，而 `Interrupt` 优先级高于 `Stop`(`core/directive.go:17`)。所以"禁用某工具"(Permission 给 Stop) 与"该工具不在技能名单"(Gate 给 Interrupt) 同时命中时，结果是中断问人而不是直接拒。挂载顺序不影响结论，不需要约束调用方怎么写。

**批准之后不会重复问同一个调用。** 人批准后由 `applyApprovals` 直接 `loop.callOne` 执行(`agent/hitl.go:105-128`)，而 `BeforeTool` 只在 ExecuteTools 阶段跑(`agent/loop.go:179-181`)——被批准的那一次调用不会第二次进门，不存在自循环。但**同名的后续新调用**仍会中断一次，因为审批是按 CallID 一次性消费的。这是可接受的保守代价，记在未决问题 4。

**与 sandbox 是两层，不要混。** 技能名单管"模型能调哪个 goagent 工具"，`sandbox.Policy.AllowedCommands` 管"进程能起哪个可执行程序"。`run_skill_script` 免检只意味着允许调那个工具，脚本能否真跑仍由沙箱决定——`skills.ScriptTool` 的文档早就写明这点(`skills/script.go:58-63`)。Gate 不替沙箱做决定。

**文案要跟上面一致。** `renderInstructions` 现在打的是一行中性的 `Allowed tools: ...`(`skills/tool.go:56-58`)，模型读了不会预期"名单外要问人"。改为 `Allowed tools (pre-approved; other tools will ask for human approval): ...`。纯文案，无行为影响。

## 装配示例

现状（`examples/task-runner/main.go:59-89` 的等价形态）：手工 `filepath.Abs` + `MkdirAll` + `process.New` + 两个自带 `inside()` 守卫的文件工具 + 手写 `taskInstruction` 里"只允许写工作目录内"。

目标：

```go
ws, err := workspace.New(workspace.Config{
	Dir:           os.Getenv("TASK_RUNNER_WORKDIR"),
	ResolveRoot:   true,
	ProjectMemory: true,
	Git:           true,
	// 技能：默认的 $HOME/.goagent/skills + <root>/.goagent/skills 两份，同名时工作区胜。
	SkillGate: true,
})
if err != nil {
	log.Fatal(err)
}
defer ws.Close()

sb, err := ws.Sandbox(sandbox.Policy{Timeout: 30 * time.Second, MaxOutputBytes: 16 << 10})
if err != nil {
	log.Fatal(err)
}

tools := append(ws.Tools(), ws.SkillTools(sb)...)
tools = append(tools, exec.RunCommand(sb))

opts := []agent.Option{
	agent.WithName("task-runner"),
	agent.WithModel(model),
	agent.WithPrompt(prompt.New().
		Add(prompt.Identity(taskInstruction)).
		Add(prompt.Environment(prompt.WithWorkingDir(ws.Root()))).
		Add(ws.Sections()...)),
	agent.WithTools(tools...),
	agent.WithMaxTurns(32),
}
// Gate 可能为 nil（未配置技能目录或没开 SkillGate）——nil 不挂。
if gate := ws.Gate(); gate != nil {
	opts = append(opts, agent.WithMiddleware(gate))
}

a, err := agent.New(opts...)
```

工具数与守卫代码都是净减少，且模型对根、VCS 状态、约束范围获得了一致陈述。

**已落地的可运行例子**（2026-09-29 补，均离线、无需密钥；`go run ./examples/workspace/<名>`）：

- `examples/workspace/anatomy` —— 第一幕在临时目录摆出约定布局（`.goagent/rules`、`.goagent/skills`、`AGENTS.md`）并逐段渲染 `Sections()`，直接调用四个文件工具看越界返回的是工具错误；第二幕拿本仓库当工作区，演示 `ResolveRoot` + `Git` 快照、`SandboxPolicy` 的 WorkDir 与 `prompt.WithWorkingDir` 同源、冲突时报错，以及 `Close()` 之后句柄失效。
- `examples/workspace/skills-gate` —— mock 剧本走 `use_skill` → 名单内 `read_file` → 名单外 `write_file`，断在 HITL 上；批准后续跑，并从 checkpoint 里读回激活名单。场景 B 摘掉 `SkillGate` 作对照（`Gate()` 为 nil ⇒ 静默执行）。
- `examples/workspace/policies` —— 三层边界各一幕：`os.Root` 包含性、沙箱 `AllowedCommands`/WorkDir、以及 `middleware.Permission`(Stop) 与 `skills.Gate`(Interrupt) 同时命中时的折叠结果。

`examples/task-runner` 迁移到这套装配（TASK-WS-07）后，本节的真实模型形态就有对应例子了。

## 备选方案与否决

**A. 不加抽象，只在 README 里给装配范例。**
成本最低，但背景 1-4 列的问题（三处根目录实现、一处 cwd 不一致、装配样板、技能无来源层次且声明不生效）会继续存活，且每个新例子都要重抄 `inside()`。否决理由：这不是文档能修的 bug，是同一语义的多份实现与一条没有接线的声明。

**B. 改 `middleware.Rule` 让它看到参数，从而表达"区内写放行、区外询问"。**
能做出更细的权限，但是破坏性改动：`Rule` 现为 `func(call *core.ToolCall) Decision`(`middleware/permission.go:23`)，改签名会波及其它中间件与全部调用方；且询问/拒绝的语义要从"工具"粒度降到"单次调用的某个参数"，`Decision` 的粒度承载不了（同一次运行里 `write_file` 可能一次区内一次区外）。否决理由：用一个破坏性改动换取 `tool/file` 免费就能得到的正确性。真正的路径级 HITL 需求出现时，另开 ADR 讨论"带参数判断的 Decision 扩展"，而不是顺手改。

**C. 把文件工具塞进 `workspace` 包，不建 `tool/file`。**
少一个包，但工具与"工作区根发现"绑死，无法给一个已有 `*os.Root` 或非工作区场景（如嵌入的模板目录）复用；也违背 `tool/web`、`tool/exec` 已确立的"工具子包"惯例。否决理由：复用性与既有分层惯例。

**D. 用 `os.DirFS` + 手写 `filepath.Rel` 清洗（现状 task-runner 的做法）而不换 `os.Root`。**
纯标准库、改动最小，但符号链接可逃逸，且清洗逻辑与真实打开之间存在竞态窗口。`os.Root` 正是为消除这一类问题而进的标准库。否决理由：安全性与代码量双赢的是 `os.Root`，没有理由保留手写守卫。

**E. `allowed-tools` 做成装配期的静态工具过滤（模型看不见未授权工具）。**
看着更"硬"，语义却反了：技能名单是**运行时**按"当前激活了哪些技能"生效的，装配期过滤只能取所有技能声明的并集，等于让任一技能的名单全局放大或收缩；而且"看不见工具"对模型不可解释，只表现为"这个能力怎么没有"。否决理由：粒度错配。免审名单（名单内直接跑、名单外问人）才是 `allowed-tools` 在上游生态里的原意，且它复用的 HITL 链路已经存在。

**F. 激活状态存在 `skills.Gate` 中间件自己的字段里。**
实现最省事，但状态脱离了 `core.State`：checkpoint 抓不到、resume 后归零、分支与子 agent 各持一份，同一 run 的"当前在用哪些技能"会随执行路径分裂。否决理由：门控依据必须和消息历史一起快照。

## 迁移与兼容性

- 纯新增包 + 两个 `prompt` 增量（`WithWorkingDir` 选项、`Add` 变长化）+ 两处 `skills` 加法（`LoadDirs`、`Gate`）+ `projectmem` 内部重构，**无破坏性 API 变更**：既有函数的签名与调用点一律不改，新行为只在挂载 workspace / Gate 后生效。
- `projectmem.Load(startDir)` 签名与语义不变，现有调用方(`memx`)不动。
- `examples/task-runner` 在 TASK-WS-07 迁移；迁移前它仍自包含可跑，不产生中间破坏态。
- `Environment()` 的默认行为保持 `os.Getwd()`，只有显式传 `WithWorkingDir` 才改变——不悄悄改写既有例子的输出。
- `prompt.Builder.Add` 放宽为变长参数后，其原有语义不变：按 Section 名去重并原位替换(`prompt/builder.go:17-28`)，`Build` 按 `Order` 稳定排序(`prompt/builder.go:43-47`)。因此 `ws.Sections()` 无论追加在链上哪个位置都会落到正确次序，`Add(prompt.Environment(...))` 仍然替换掉内置 environment 段而非并存。
- `skills` 的两处加法是新文件：`Load` / `LoadDir` / `PromptSection` / `ScriptTool` 的签名与行为都不动，`examples/skills` 不受影响。`use_skill` 改为手写 `tool.Tool` 实现后，工具名、描述、schema 与返回文本主体都不变，只多了一条 `OpSetKV` 写入——不挂 Gate 时，模型可见行为与今天一致（新 KV 键不会自动进提示词：`prompt.SessionState` 只渲染显式点名的键，`prompt/sections.go:89-111`）。
- 唯一的**可见**行为改动是 `renderInstructions` 那行文案(`skills/tool.go:56-58`)，它被 `skills/tool_test.go:50` 以子串断言锁着——TASK-WS-05 同时改这一处断言，不留下"文档说改了但测试还钉着旧文案"的中间态。

## 实施计划

每个任务独立提交、独立可测，按序执行；提交信息用 Conventional Commits。

- **TASK-WS-01 — `internal/reporoot` + projectmem 委托**
  纯搬迁。验收：`go test ./...` 全绿，`projectmem` 既有测试不改断言即可通过；`reporoot` 补表驱动测试覆盖"无 .git"、"起点即根"、"多层向上"、"` .git` 为文件"。
- **TASK-WS-02 — `tool/file` 子包**
  四个工具 + 越界/符号链接/绝对路径/Windows 保留名用例。验收：`go test ./tool/file` 绿；测试须在 Windows 与 Unix 语义下都能跑（符号链接不可用时 `t.Skip` 并说明原因，不掩盖）。
  已完成。本机（win32，未开开发者模式）`os.Symlink` 报 "A required privilege is not held by the client"，两条符号链接用例按约定 SKIP 并打印原因；Unix CI 上会真实执行。
- **TASK-WS-03 — `workspace` 骨架**
  `New/Root/FS/Close/Sections/Tools/SandboxPolicy/Sandbox` + `prompt.WithWorkingDir` 与 `Add` 变长化。验收：`SandboxPolicy` 与 `Environment` 报告同一路径；显式传入冲突 `WorkDir` 时报错而非覆盖；`examples/prompt`、`examples/skills`、`examples/memory-layers` 不改代码即可编译通过。
  已完成。落地时的取舍：`Config` 只含本任务用到的字段（`Git` 随 TASK-WS-04、技能三字段随 TASK-WS-06 加入，不留空壳）；未给 `Workspace` 加 `Rules()` 访问器（ADR 的 API 草案里没有，Section 已够用）；约定目录名提成常量 `userDirName = ".goagent"`，规则路径按 `$HOME/.goagent/rules` → `<root>/.goagent/rules` 回填，`os.UserHomeDir()` 失败时全局档留空（不报错）。
- **TASK-WS-04 — 只读 git 探测**
  `GitInfo` 及文件读实现，`Dirty` 走 `git status --porcelain` 且缺 `git` 时可降级。验收：在真仓库、子目录、非 git 目录、linked worktree 四种场景下断言正确；`git` 不在 PATH 时测试仍绿。
  已完成。四类场景都覆盖：真仓库走 `TestProbeGitAgainstRealRepository`（本机 git 在 PATH，非 SKIP）；子目录/非 git/worktree 用手工摆盘的 `.git` 断言。"缺 git"用注入的 `statusFunc` 返回错误来模拟，因此与测试机的 PATH 无关。非 git 场景额外加了 `isolatedTemp` 守卫：临时目录的祖先里若有 `.git`（例如家目录是 dotfiles 仓库）就 SKIP 并说明，而不是留下一个假绿的断言。
- **TASK-WS-05 — `skills` 加法：`LoadDirs` + `ActiveKey` + `Gate`**
  新文件 `skills/loaddir.go`、`skills/gate.go`；`use_skill` 改为手写 `tool.Tool` 以携带 `OpSetKV`；`renderInstructions` 文案与 `skills/tool_test.go:50` 的断言同批改。验收：同名技能后扫描目录胜、`examples/skills` 不改代码即可跑通；Gate 的四档判定（无激活 / 无声明 / 名单内 / 名单外）各有测试，其中"名单外"断言拿到的是 `Interrupt` 而不是 `Stop`；补一条 resume 后激活名单仍在的测试（走 checkpoint 的 `State.KV`）。
  已完成。`examples/skills` 零改动跑通（mock provider，真实走完 `use_skill` → 正文渲染）。名单外的断言用 `agent.NewStack(middleware.Permission(DenyFor(...)), Gate(lib))` 折叠后再断言 `Kind == Interrupt`，把"与 Permission 共存时中断优先"钉成测试而不只是文档措辞。持久性那条走 `json.Marshal(State)` → `Unmarshal` → 再问 Gate，覆盖 KV 过一趟 JSON 的类型漂移风险（因此名单存 JSON 字符串而非 `[]string`）。另加 `TestUseSkillSurfaceUnchanged`：手写工具对模型可见的 name/description/schema 必须与被 `tool.New` 生成时一致。
- **TASK-WS-06 — `workspace` 的技能侧接线**
  `Config.GlobalSkillsDir/ProjectSkillsDir/SkillGate`、`ws.Skills/SkillTools/Gate`，并把 Level-1 段并入 `ws.Sections()`。验收：`Gate()` 在未配置时为 nil；`<root>/.goagent/skills` 缺目录时 `New` 不报错。
  已完成。落地时的取舍：技能库合并后若为空，`w.skills` 归一为 **nil**（不是长度为 0 的 `Library`），于是 `Skills/SkillTools/Gate/Sections` 四处对"没有技能"的说法一致；`Gate()` 返回字面量 nil 而非"禁用的中间件"，测试里按 ADR 的装配示例 `if gate := ws.Gate(); gate != nil` 才挂载，把这条契约跑成真代码而不是注释。规则与技能的路径回填收敛成一对通用 helper（`globalDir(configured, sub)` / `w.projectDir(configured, sub)`），`sub` 只有 `"rules"` 与 `"skills"` 两个取值，不再各写一份。坏 SKILL.md 让 `New` 直接失败（与 `rules`/`projectmem` 同立场：静默丢掉调用方要求加载的能力，比拒绝启动更糟）。
  测试：`workspace/skills_test.go` 覆盖合并优先级、Level-1 段落进 `Sections()` 末尾、`SkillTools` 恰是 `use_skill,run_skill_script`、缺目录时四个出口全为空（含 `Gate()==nil` 且 `SkillGate:true`）、坏 SKILL.md 报错。`workspace/gate_run_test.go` 补上"测试策略"里那条**真 Agent 走一遍**：mock 模型先 `use_skill` 激活 pdf、再调名单外的 `write_file` ⇒ 断言 `Interrupted` 的 pending 恰是这一次调用（CallID/Tool）、且文件**没有**落盘；`Decide(Allow)` + `Resume` 后断言文件写出、末轮文本返回、checkpoint 里激活名单仍是 `[pdf]`。做过反向验证：把 `SkillGate` 关掉该测试即失败于 `loaded=1 wrote=1 pauses=0`，证明断言不是空转。
- **TASK-WS-07 — task-runner 迁移**
  改用 workspace + tool/file，删除本地 `inside()`/`readFileTool`/`writeFileTool`，并按"装配示例"接上技能与 Gate。验收：同一 task 命令行跑通，产物文件位置不变；`go vet ./...` 干净。

## 测试策略

- `tool/file` 与 `workspace` 用 `t.TempDir()` 造真实目录，不用内存 FS——包含性的价值恰恰在真实文件系统语义（符号链接、大小写、保留名）上。
- 越界用例必须断言"返回的是工具错误（`Result.IsError`）而不是 Go error"，锁死"工具错误是数据"这条契约。
- git 探测用 `git init` 构造真实小仓库（临时目录内），而非手搓 `.git` 字节；detached HEAD 用 `git checkout <sha>` 制造。若环境无 `git`，跳过该子测试并打印原因。
- Gate 的测试走真 `Agent` + mock 模型：一轮 `use_skill`、一轮越界工具调用，断言 run 停在 `Interrupted` 且 pending 里是那次调用；随后 `Run.Decide(Allow)` + `Resume` 断言工具被执行且**没有**第二次中断。另加一条共存用例：同一工具同时命中 `Permission.DenyFor`(给 Stop) 与 Gate(给 Interrupt)，断言最终是中断(`core/directive.go:17`)。
- 全仓既有 CRLF，`gofmt -l` 会标记所有文件——**不要**据此重排；只对新文件确认 `gofmt` 无 diff。

## 未决问题

1. `write_file` 是否要有"只新建不覆盖"模式（`O_EXCL`）与备份策略？涉及 agent 的 checkpoint/回滚语义，暂不纳入。
2. 跨工作区（monorepo 多包、多仓库协同）是否需要 `ws.Sub(dir)` 派生子工作区并共享 skills？`os.Root.OpenRoot` 已能给出子句柄，但 Section 与门控名单如何叠加未定。
3. 工作区内的**读**是否需要与 rules 联动（例如 `.env`、`credentials*` 默认对 read_file 隐藏）？当前只有包含性，没有内容级屏蔽。
4. **批准的粒度记忆**：`agent.Approval` 只有 `CallID/Approve/Reason`(`agent/hitl.go:17-29`)，没有"本会话内该工具以后免问"的通道。因此 Gate 对名单外工具的**每一次**新调用都会中断一次——保守但可能啰嗦。是否加 `Approval.Remember`（批准时把工具名写进 `State.KV` 的授权名单，后续同名词免问），涉及 HITL 数据结构扩展与"人到底在批准什么"的可解释性，需要单独定夺。
