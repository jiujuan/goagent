// Command policies 演示工作区的"安全权限"其实是三层,各管一件事,谁也不能替谁:
//
//	路径层  os.Root            框架自带文件工具的路径解析不能出根(符号链接也不行)
//	进程层  sandbox.Policy     命令的工作目录/超时/输出上限/可执行程序白名单/环境变量
//	调用层  Gate + Permission  模型能调哪个工具、哪些要先问人
//
//	go run ./examples/workspace/policies
//
// 离线、无需密钥。最后一幕把调用层的折叠跑成真代码:同一个 write_file 既被
// Permission 永久禁用(给 Stop)又落在技能名单外(给 Interrupt)时,运行停在问人而
// 不是直接结束——Interrupt 的优先级高于 Stop;把 Gate 摘掉再跑同一份剧本,结果就是
// Stop。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/workspace"
)

func main() {
	threeLayers()
	pathLayer()
	processLayer()
	callLayer()
}

// ── 总览 ─────────────────────────────────────────────────────────

// threeLayers 把三层边界和它们的"越界表现"摆在一起,并展示 workspace 段对
// "命令不在笼子里"是明说的。
func threeLayers() {
	fmt.Println(`三层边界
  路径层  os.Root          管:read_file/write_file/list_dir/glob 能解析到哪个路径
                    越界:工具错误(IsError)——模型看得见,能自我修正
  进程层  sandbox.Policy   管:命令的 WorkDir、超时、输出上限、AllowedCommands、Env
                    越界:Go error(策略/配置违规)——进程根本没起来
  调用层  Gate/Permission  管:模型能调哪个工具、哪些要先问人
                    越界:控制流指令 Interrupt(问人)或 Stop(结束)

workspace 段原文:`)
	w := newWorkspace(workspace.Config{})
	defer closeAndRemove(w)
	for _, s := range w.Sections() {
		if s.Name() == "workspace" {
			fmt.Print(indent(must(s.Render(prompt.Context{}))))
		}
	}
}

// ── 路径层 ────────────────────────────────────────────────────────

// pathLayer:根内照常读写,越界与绝对路径都是"工具错误"而不是 Go error。技能名单
// 管不到这一层,两者互不替代。
func pathLayer() {
	fmt.Println("\n\n──── 路径层:os.Root 的包含性 ────")
	w := newWorkspace(workspace.Config{})
	defer closeAndRemove(w)
	fmt.Printf("文件工具:%s\n", toolNames(w.Tools()))

	for _, call := range []struct{ tool, args string }{
		{"read_file", `{"path":"notes.txt"}`},
		{"read_file", `{"path":"../outside.txt"}`},
		{"write_file", `{"path":"../outside.txt","content":"出根了\n"}`},
		{"write_file", `{"path":"ok.txt","content":"根内,没问题\n"}`},
	} {
		res, err := lookup(w.Tools(), call.tool).
			Call(&tool.Context{Context: context.Background()}, json.RawMessage(call.args))
		check(err) // 文件工具把失败写成工具结果;返回 Go error 就是 bug
		fmt.Printf("  %-11s %s\n    → IsError=%v  %s\n",
			call.tool, clip(call.args, 46), res.IsError, clip(text(res.Content), 80))
	}
}

// ── 进程层 ────────────────────────────────────────────────────────

// processLayer:工作目录由 workspace 填、不接受第二个值;命令白名单只放行名单里的
// 可执行程序,名单外的连进程都不起。
func processLayer() {
	fmt.Println("\n\n──── 进程层:同源工作目录与命令白名单 ────")
	w := newWorkspace(workspace.Config{})
	defer closeAndRemove(w)

	p, err := w.SandboxPolicy(sandbox.Policy{})
	check(err)
	fmt.Printf("SandboxPolicy 填出的 WorkDir:%s\n", p.WorkDir)
	if _, err := w.SandboxPolicy(sandbox.Policy{WorkDir: filepath.Dir(w.Root())}); err != nil {
		fmt.Printf("调用方给了不一致的 WorkDir → %v(报错,不静默覆盖)\n", err)
	}

	sb, err := w.Sandbox(sandbox.Policy{
		Timeout:        10 * time.Second,
		MaxOutputBytes: 4 << 10,
		// 只放行 git:模型再想 curl/wget 拉东西,命令在启动前就被拒。
		AllowedCommands: []string{"git"},
		// Env 是白名单语义:不写就等于空环境,连 PATH 都不给。
		Env: map[string]string{"PATH": os.Getenv("PATH")},
	})
	check(err)

	out, err := sb.Run(context.Background(), sandbox.Spec{Command: "git", Args: []string{"--version"}})
	if err != nil {
		fmt.Printf("名单内 git --version 没起来:%v\n", err)
	} else {
		fmt.Printf("名单内 git --version → %s(退出码 %d,用时 %s)\n",
			firstLine(string(out.Stdout)), out.ExitCode, out.Duration.Round(time.Millisecond))
	}
	_, err = sb.Run(context.Background(), sandbox.Spec{Command: "curl", Args: []string{"https://example.com"}})
	fmt.Printf("名单外 curl → %v(errors.Is 认得出这是策略违规:%v)\n", err, errors.Is(err, sandbox.ErrCommandNotAllowed))
	fmt.Println("这一层管\"能起哪个程序\",技能名单管\"能调哪个工具\"——两层,别混。")
}

// ── 调用层 ────────────────────────────────────────────────────────

// callLayer 用同一份剧本跑两遍,唯一变量是挂不挂 Gate。
func callLayer() {
	for _, tc := range []struct {
		title string
		gate  bool
	}{
		{"Gate + Permission 同时挂载", true},
		{"只有 Permission,没有 Gate", false},
	} {
		fmt.Printf("\n\n──── 调用层:%s ────\n", tc.title)
		call(tc.gate)
	}
}

func call(gate bool) {
	w := newWorkspace(workspace.Config{SkillGate: gate})
	defer closeAndRemove(w)

	sb, err := w.Sandbox(sandbox.Policy{Timeout: 10 * time.Second, AllowedCommands: []string{"sh"}})
	check(err)

	store := checkpoint.NewMemory()
	turn := 0
	// 剧本:先 use_skill 加载 wordcount(它的名单只有 read_file/glob),再调 write_file。
	model := mock.New("mock", func(*llm.Request) *llm.Response {
		defer func() { turn++ }()
		switch turn {
		case 0:
			return mock.CallTool("s1", "use_skill", `{"name":"wordcount"}`)
		case 1:
			return mock.CallTool("w1", "write_file", `{"path":"report.md","content":"名单外的写法\n"}`)
		default:
			return mock.Text("收尾。")
		}
	})

	opts := []agent.Option{
		agent.WithModel(model),
		agent.WithPrompt(prompt.New().Add(w.Sections()...)),
		agent.WithTools(append(w.Tools(), w.SkillTools(sb)...)...),
		agent.WithCheckpointer(store),
		// Permission 永久禁用 write_file。
		agent.WithMiddleware(middleware.Permission(middleware.DenyFor("write_file"))),
	}
	if mw := w.Gate(); mw != nil {
		opts = append(opts, agent.WithMiddleware(mw))
		fmt.Println("挂载:Permission(DenyFor write_file) + Gate(write_file 不在技能名单里)")
	} else {
		fmt.Println("挂载:只有 Permission(DenyFor write_file)")
	}

	a, err := agent.New(opts...)
	check(err)

	ctx := context.Background()
	r := a.Stream(ctx, "把统计结果写出来", agent.OnThread("t1"))
	var pending []core.ApprovalRequest
	for ev, err := range r.Iter() {
		check(err)
		switch e := ev.(type) {
		case core.ToolStarted:
			fmt.Printf("  → %s\n", e.Call.Name)
		case core.Interrupted:
			pending = e.Pending
		}
	}

	report := filepath.Join(w.Root(), "report.md")
	if len(pending) == 0 {
		fmt.Printf("  运行按 Stop 结算,没有人可问;report.md 存在?%v\n", exists(report))
		return
	}
	p := pending[0]
	fmt.Printf("  ⏸ 停在人工批准:%s(%s)——折叠结果是 Interrupt(问人),不是 Stop\n", p.Tool, p.CallID)
	fmt.Println("  注意:人一旦批准,这一次调用会被直接执行,Permission 的 Deny 不再复核。")

	r.Decide(agent.Allow(p.CallID))
	cont, err := r.Resume(ctx)
	check(err)
	res, err := cont.Wait()
	check(err)
	fmt.Printf("  续跑收尾:%q;report.md 存在?%v\n", res.Message.Text(), exists(report))
}

// ── 小工具 ───────────────────────────────────────────────────────

// newWorkspace 在临时目录里摆一个 demo 工作区:一个声明了 allowed-tools(不含
// write_file)的技能加一个文本文件。每次调用都新建一份,免得对照的两遍互相污染——
// 前一遍批准写出的 report.md 会让后一遍的"文件存在吗"变成假答案。
func newWorkspace(cfg workspace.Config) *workspace.Workspace {
	dir := must(filepath.EvalSymlinks(must(os.MkdirTemp("", "ws-policies"))))
	write(filepath.Join(dir, ".goagent", "skills", "wordcount", "SKILL.md"),
		"---\nname: wordcount\ndescription: 统计词数(名单里没有 write_file)\nallowed-tools: [read_file, glob]\n---\n先读再写。\n")
	write(filepath.Join(dir, "notes.txt"), "one two three\n")

	cfg.Dir = dir
	w, err := workspace.New(cfg)
	check(err)
	return w
}

// closeAndRemove 释放文件系统句柄,再删掉临时目录。
func closeAndRemove(w *workspace.Workspace) {
	check(w.Close())
	check(os.RemoveAll(w.Root()))
}

func lookup(tools []tool.Tool, name string) tool.Tool {
	for _, tl := range tools {
		if tl.Name() == name {
			return tl
		}
	}
	panic("工作区没有提供工具 " + name)
}

func toolNames(tools []tool.Tool) string {
	names := make([]string, 0, len(tools))
	for _, tl := range tools {
		names = append(names, tl.Name())
	}
	return strings.Join(names, ", ")
}

func text(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func firstLine(s string) string { return strings.SplitN(strings.TrimSpace(s), "\n", 2)[0] }

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func write(path, content string) {
	check(os.MkdirAll(filepath.Dir(path), 0o755))
	check(os.WriteFile(path, []byte(content), 0o644))
}

func must[T any](v T, err error) T {
	check(err)
	return v
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
