# ADR 0012:沙箱化命令执行(sandbox 接口 + process 后端 + run_command 工具)

状态:已接受

## 背景

Agent 需要"执行外部程序"这类能力(code interpreter / shell / 调脚本)。把任意命令直接
`os/exec` 交给模型驱动是危险的:无超时会挂死、无输出上限刷爆内存、继承全量环境变量泄露密钥、
能跑任意可执行文件。需要一层受控执行壳。约束:沿用框架理念——接口尽量小、后端子包隔离、
**零外部依赖(纯 stdlib)、跨平台**(含 Windows)。

## 决策

引入 `sandbox` 基元包 + `process` 默认后端 + `tool/exec` 开箱即用工具。

```go
type Sandbox interface {
    Run(ctx context.Context, spec Spec) (*Outcome, error)
}
```

`Policy`(后端构造时固定)强制 5 项**可移植**限制:超时、输出大小上限、工作目录限定、
环境变量白名单(默认空环境)、命令白名单(默认拒绝、显式放行)。

`tool/exec.RunCommand(sb)` 把一个已配置策略的 Sandbox 包成 `run_command` 工具,直接塞进
`agent.Config.Tools` 即用,对齐现有 `tool/web` 子包模式。

错误二分:**配置/策略违规**(空命令、WorkDir 非法、命令不在白名单)在启动进程前返回类型化
Go `error`;**运行结果**(非零退出、超时、输出截断)写入 `Outcome` 而非 Go error,贴合
"工具错误回报给模型"的范式([ADR 0004](0004-side-effects-as-actions.md))。

## 理由

- **小接口可扩展**:`Sandbox` 单方法,`process` 只是默认后端;未来加 `sandbox/docker` 等强隔离
  后端不动接口,与 provider 子包隔离([ADR 0005](0005-provider-isolation.md))一致。
- **可移植优先**:默认后端只做 stdlib 能确定性保证的 5 项;仅"进程组/树 kill"按平台分文件
  (build tag),仍是纯 `syscall`,零依赖不破。
- **白名单姿态**:命令与环境变量默认拒绝、显式放行,安全默认更稳。
- **不做 shell 工具**:`shell -c` 会让命令白名单形同虚设,故默认只提供"显式 command+args"的
  `run_command`。

## 后果

- 默认后端**不**提供内存/CPU 硬限制与网络隔离(需平台 syscall / namespace);这两项交给将来的
  容器后端,`Policy` 形状已为其预留。
- Windows 上"杀子进程树"为 best-effort(纯 stdlib 限制),文档已注明。
- 工具作者若需更强隔离,应替换 `Sandbox` 实现而非改 `run_command`。

## 备选方案

- **在 turn 引擎/中间件层包裹所有工具执行**:侵入核心执行路径,且对纯 Go 工具无意义;沙箱
  只对"拉起外部进程"的工具有价值,做成可注入的 Sandbox 更聚焦。
- **只给基元、不给工具**:不够开箱即用;`tool/exec` 一层很薄但省去每个用户重写格式化。
- **默认就上容器**:违背零依赖与跨平台简单性;留作可选后端更合适。
