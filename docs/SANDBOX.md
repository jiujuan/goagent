# Sandbox 设计与实现方案

> 一句话:给 agent 的命令执行类能力一个**受控、可移植、零依赖**的执行壳;
> 接口尽量小,默认后端用纯标准库,强隔离后端可作为子包扩展。

## 1. 动机

Agent 经常需要"执行外部程序"这类能力(code interpreter / shell / 调脚本)。直接 `os/exec`
把任意命令交给模型驱动是危险的:无超时会挂死、无输出上限会刷爆内存、继承全量环境变量会泄露
密钥、能跑任意可执行文件。本方案引入一层 **sandbox**,把这些约束收敛到一个小接口和一个默认
进程后端里,并提供开箱即用的 `run_command` 工具。

设计遵循框架既有理念:**接口尽量小、provider/后端子包隔离、零外部依赖(纯 stdlib)、跨平台**。

## 2. 范围(已确认)

默认进程后端强制以下 5 项可移植限制:

1. **超时**(wall-clock,`context` + 进程树 kill)
2. **输出大小上限**(stdout+stderr 合计,防内存/日志刷爆)
3. **工作目录限定**(所有命令只在指定目录跑)
4. **环境变量白名单**(默认空环境,只透传显式允许的 key)
5. **命令白名单**(只允许执行被允许的命令名)

**不做**(按需求确认):内存/CPU 硬限制、网络隔离。这两项需平台特定 syscall 或 namespace,
不在默认后端实现;但 `Policy` 与 `Sandbox` 接口的形状为将来的 Docker 等强隔离后端预留空间,
不破坏现有契约。

## 3. 架构与包划分

```
sandbox/                      基元(零依赖,纯 stdlib)
  sandbox.go                  Sandbox 接口 + Spec / Outcome / Policy + 类型化错误
  process/                    默认后端(进程隔离)
    process.go                New(policy) + Run 实现
    process_unix.go           //go:build !windows  进程组 + 树 kill
    process_windows.go        //go:build windows   独立进程组 + kill
    process_test.go
tool/exec/                    开箱即用工具(对齐 tool/web 模式)
  exec.go                     RunCommand(sb) → run_command 工具
  exec_test.go
examples/sandbox/main.go      演示:给 agent 挂一个受限 run_command
docs/SANDBOX.md               本文档
docs/adr/0012-sandbox.md      ADR
```

只有"进程组/树 kill"一处按平台分文件(build tag),且仍是纯 `syscall`,不引入任何外部依赖。

## 4. 核心接口与类型(`sandbox` 包)

```go
// Sandbox 在受控环境中执行一条命令。接口刻意小。
type Sandbox interface {
    Run(ctx context.Context, spec Spec) (*Outcome, error)
}

// Spec:本次要跑什么(每次调用)。
type Spec struct {
    Command string   // 可执行名或路径
    Args    []string // 参数
    Stdin   []byte   // 可选标准输入
}

// Policy:约束(后端构造时固定)。零值字段表示"该项不限制"。
type Policy struct {
    WorkDir         string            // 3 工作目录(必填,绝对路径)
    Timeout         time.Duration     // 1 wall-clock 超时(0=无)
    MaxOutputBytes  int64             // 2 stdout+stderr 合计上限(0=无)
    AllowedCommands []string          // 5 命令白名单(按 basename;空=不限制)
    Env             map[string]string // 4 环境变量白名单(nil/空=完全空环境)
}

// Outcome:跑完的结果。
type Outcome struct {
    ExitCode  int
    Stdout    []byte
    Stderr    []byte
    TimedOut  bool          // 命中超时被杀
    Truncated bool          // 输出命中上限被截断
    Duration  time.Duration
}
```

类型化错误(均在启动进程**之前**或基础设施层返回):

```go
var (
    ErrInvalidWorkDir   = errors.New("sandbox: work dir must be an absolute existing directory")
    ErrCommandNotAllowed = errors.New("sandbox: command not in allow list")
    ErrInvalidSpec      = errors.New("sandbox: empty command")
)
```

## 5. 错误处理约定(关键)

沿用框架"工具错误回报给模型、基础设施失败才是 Go error"的哲学:

- **配置/策略违规**(命令不在白名单、WorkDir 非法、空命令)→ 返回类型化 Go `error`,
  在 `exec` 之前拦截,不启动进程。
- **进程跑起来但失败**(非零退出、超时、输出截断)→ **不是** Go error,写入 `Outcome`
  (`ExitCode`/`TimedOut`/`Truncated`),由调用方决定如何回报模型。
- **无法启动**(二进制不存在等)→ 返回 Go `error`。

## 6. process 后端的数据流(Run 内部)

1. 校验 Spec:`Command` 为空 → `ErrInvalidSpec`。
2. 命令白名单:取 `Command` 的 `filepath.Base`,若 `AllowedCommands` 非空且不在其中 →
   `ErrCommandNotAllowed`(限制 5)。
3. 超时:`ctx, cancel = context.WithTimeout(ctx, policy.Timeout)`(Timeout>0 时;限制 1)。
4. 构造 `exec.CommandContext`:
   - `cmd.Dir = policy.WorkDir`(限制 3)。
   - `cmd.Env = buildEnv(policy.Env)`(限制 4;默认 `[]string{}` 即空环境,只放显式 key)。
   - `cmd.Stdin = bytes.NewReader(spec.Stdin)`(若有)。
5. 平台特定 `configureSysProcAttr(cmd)`:建独立进程组,便于超时/截断时**杀整棵子进程树**
   (Unix:Setpgid;Windows:CREATE_NEW_PROCESS_GROUP,树 kill 为 best-effort)。
6. 输出上限:stdout/stderr 各包一层 **capWriter**,累计超过 `MaxOutputBytes` 即置 `Truncated`
   并触发 `killProcessTree`(限制 2)。
7. 运行;`Wait` 后:
   - `ctx.Err()==context.DeadlineExceeded` → `TimedOut=true`。
   - 从 `*exec.ExitError` 取 `ExitCode`;正常退出取 0。
   - 记录 `Duration`。
8. 返回 `*Outcome`(配置/启动错误才返回 err)。

`New(policy)` 在构造时校验 `WorkDir`(必须绝对且存在),非法即返回 `ErrInvalidWorkDir`,
让错误尽早暴露而非每次 Run 才发现。

## 7. tool/exec —— `run_command` 工具

```go
// RunCommand 用一个已配置策略的 Sandbox 造出 run_command 工具。
func RunCommand(sb sandbox.Sandbox) tool.Tool
```

- 输入 schema(反射自结构体):`{ command string, args []string }`。
- 内部调 `sb.Run`,把 `Outcome` 格成文本(退出码 + stdout + stderr + 超时/截断标记)。
- 非零退出、超时、或策略违规 → `tool.ErrorResult`(让模型能据此纠错),正常 → `tool.TextResult`。

**刻意不做** `shell -c` 风格的 `bash` 工具:那会让命令白名单形同虚设(shell 内可任意拉起
进程)。保持"显式 command+args"。如确需 shell,属后续扩展,须把 shell 本身列入白名单并明确
风险。

## 8. 测试策略

跨平台(含 Windows)难点是找一条到处都有的命令。采用标准库 os/exec 自带的
**`TestHelperProcess` 模式**:测试把**测试二进制自身**当子进程跑(用特殊 flag +
`GO_WANT_HELPER_PROCESS=1` 环境变量触发辅助分支),纯 stdlib、确定性、各平台一致。

覆盖用例:
- 正常执行,捕获 stdout、ExitCode=0。
- 非零退出码正确透传。
- 超时:跑一个 sleep 辅助分支,确认 `TimedOut=true` 且进程被杀。
- 输出截断:辅助分支狂打输出,确认 `Truncated=true` 且总量受限。
- 命令白名单:不在白名单 → `ErrCommandNotAllowed`,且未启动进程。
- 环境隔离:辅助分支打印某 env,确认只有白名单 key 可见。
- 空命令 → `ErrInvalidSpec`;非法 WorkDir → `New` 返回 `ErrInvalidWorkDir`。

`tool/exec` 测试用一个 mock `sandbox.Sandbox` 验证 Outcome→Result 的格式化与 IsError 判定,
不依赖真实进程。

## 9. 实现步骤

1. **`sandbox/sandbox.go`** —— 定义 `Sandbox`、`Spec`、`Policy`、`Outcome`、类型化错误。无逻辑,
   纯契约。
2. **`sandbox/process/process.go`** —— `New(policy)` 校验 + `Run` 主流程(限制 1–5,除进程组部分)。
3. **`sandbox/process/process_unix.go` / `process_windows.go`** —— `configureSysProcAttr` 与
   `killProcessTree` 的平台实现(build tag)。
4. **`sandbox/process/process_test.go`** —— TestHelperProcess 模式覆盖第 8 节用例。
5. **`tool/exec/exec.go`** —— `RunCommand(sb)` 工具 + Outcome 格式化。
6. **`tool/exec/exec_test.go`** —— mock sandbox 验证格式化/IsError。
7. **`examples/sandbox/main.go`** —— 用 mock LLM agent 演示挂一个受限 `run_command`。
8. **`docs/adr/0012-sandbox.md`** —— 记录决策。
9. `go build ./... && go vet ./... && go test ./sandbox/... ./tool/...` 全绿。

## 10. 取舍小结

- **小接口 + 后端子包**:`sandbox.Sandbox` 一个方法;`process` 是默认后端,未来可加
  `sandbox/docker` 等强隔离后端而不动接口。与现有 provider 子包隔离一致。
- **可移植优先**:默认后端只做 stdlib 能确定性保证的 5 项;内存/CPU/网络等强隔离留给容器后端。
- **白名单而非黑名单**:命令与环境变量都默认拒绝、显式放行,安全姿态更稳。
- **错误二分**:配置错=Go error 早失败;运行结果(退出码/超时/截断)进 Outcome,贴合工具回报模型的范式。
