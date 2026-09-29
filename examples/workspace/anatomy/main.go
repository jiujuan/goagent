// Command anatomy 走读 workspace 包:一份配置怎么同时决定 Agent 的「工作目录、
// 目录里的代码状态、能碰哪些文件、命令在哪跑、提示词长什么样」。
//
//	go run ./examples/workspace/anatomy
//
// 全程离线、无需密钥。第一幕在临时目录摆出一个 demo 项目(约定目录 .goagent/
// rules+skills、AGENTS.md、源码),看它被装配成什么;第二幕拿 goagent 仓库自己
// 当工作区,看只读 git 快照、沙箱工作目录与提示词里的 cwd 为什么是同一个值。
//
// 技能合并与 allowed-tools 免审门控不在这里展开,见 examples/workspace/skills-gate
// 与 examples/workspace/policies。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/workspace"
)

func main() {
	conventionalLayout()
	realRepository()
}

// ── 第一幕:约定目录 ──────────────────────────────────────────────

// conventionalLayout 用一个临时目录当"仓库根",演示 Config 的默认回填:
// 项目规则/技能都取自约定路径 <root>/.goagent/{rules,skills},而"用户级"那份
// 在例子里显式指到临时目录(真实程序不用传,默认就是 $HOME/.goagent/...)。
func conventionalLayout() {
	root, err := filepath.EvalSymlinks(must(os.MkdirTemp("", "ws-anatomy-repo")))
	check(err)
	home := must(os.MkdirTemp("", "ws-anatomy-home"))
	defer os.RemoveAll(root)
	defer os.RemoveAll(home)

	writeFile(filepath.Join(root, "AGENTS.md"), "# 项目约定\n\n- 提交信息用英文。\n")
	writeFile(filepath.Join(root, "src", "main.go"), "package main\n")
	writeFile(filepath.Join(root, "notes", "todo.md"), "- 补齐 workspace 例子\n")
	writeFile(filepath.Join(root, ".goagent", "rules", "style.md"),
		"注释与文档用中文,标识符用英文。")
	writeFile(filepath.Join(root, ".goagent", "skills", "wordcount", "SKILL.md"),
		"---\nname: wordcount\ndescription: 统计文件里的词数\nallowed-tools: [read_file]\n---\n# 词数技能\n先用 read_file 读文件,再按空白切分计数。\n")
	writeFile(filepath.Join(home, ".goagent", "rules", "terse.md"), "回答尽量简短。")

	fmt.Println("demo 项目布局(工作区根):")
	fmt.Println(tree(root))

	w, err := workspace.New(workspace.Config{
		Dir: root,
		// 项目记忆从根目录向上收集 AGENTS.md,直到仓库边界(临时目录没有 .git,
		// 所以一路走到文件系统根);根内这份会被收进来。
		ProjectMemory: true,
		// 约定路径的回退是 $HOME/.goagent/<sub>;这里换成临时目录以免读到
		// 真实家目录里的规则,顺带展示"全局 → 项目"两级合并的形状。
		GlobalRulesDir:  filepath.Join(home, ".goagent", "rules"),
		GlobalSkillsDir: filepath.Join(home, ".goagent", "skills"),
	})
	check(err)
	defer w.Close()

	// 根目录是唯一事实源:文件工具、沙箱、提示词都从它出发。
	fmt.Printf("\nRoot() = %s\n", w.Root())
	fmt.Printf("Tools() = %s\n", toolNames(w.Tools()))
	fmt.Printf("Skills() 合并结果 = %s\n", skillNames(w))

	// Sections() 交出的四段(带各自的 Order,交给 prompt.Builder 排序):
	// rules 50 / project_memory 150 / workspace 210 / skills 350。
	fmt.Printf("\nSections() 共 %d 段\n", len(w.Sections()))
	for _, s := range w.Sections() {
		body := must(s.Render(prompt.Context{}))
		fmt.Printf("\n──── %s (order %d) ────\n%s", s.Name(), s.Order(), body)
	}

	// 包含性由 os.Root 负责:根内读写照常,越界与绝对路径变成"工具错误"
	// (模型能读懂并自我修正的数据),不是 Go error。
	tools := toolByName(w.Tools())
	fmt.Println("\n──── 文件工具的边界 ────")
	show("read_file notes/todo.md", callTool(tools["read_file"], `{"path":"notes/todo.md"}`))
	show("read_file ../outside.txt", callTool(tools["read_file"], `{"path":"../outside.txt"}`))
	show("read_file /etc/passwd", callTool(tools["read_file"], `{"path":"/etc/passwd"}`))
	show("write_file out/report.md", callTool(tools["write_file"], `{"path":"out/report.md","content":"写进根内,顺带建目录\n"}`))
	show("list_dir .", callTool(tools["list_dir"], `{"path":"."}`))
	show("glob src/*.go", callTool(tools["glob"], `{"pattern":"src/*.go"}`))

	// 同一个句柄也能直接用:框架外的代码想要更窄的作用域可以 w.FS().Sub(name)。
	if b, err := w.FS().ReadFile("out/report.md"); err == nil {
		fmt.Printf("\nFS().ReadFile(out/report.md) = %q\n", string(b))
	}
}

// ── 第二幕:真实仓库 + git 快照 + 沙箱同源 ────────────────────────

// realRepository 把当前目录(通常是 goagent 仓库根)装配成工作区,演示三件在
// 手写接线时最容易漂移的事:git 状态、命令的工作目录、提示词里报给模型的 cwd。
func realRepository() {
	cwd := must(os.Getwd())
	w, err := workspace.New(workspace.Config{
		Dir:         ".",
		ResolveRoot: true, // 向上走查到 .git,仓库根即工作区根
		Git:         true, // 只读探测:分支/HEAD/是否脏
	})
	check(err)
	defer w.Close()

	fmt.Printf("\n\n──── 仓库工作区 ────\nDir 传入相对路径 \".\"(=%s)\nResolveRoot 得到 Root() = %s\n", cwd, w.Root())

	// Git 探测失败不阻断装配:没装 git 只是少一条事实,不是一个错误。
	info := w.Git()
	fmt.Printf("Git() = valid=%v branch=%q head=%.12s dirty=%v", info.Valid, info.Branch, info.HeadCommit, info.Dirty)
	if info.StatusErr != nil {
		fmt.Printf(" statusErr=%v", info.StatusErr)
	}
	fmt.Printf("\n仓库根 = %s\n", info.RepoRoot)

	// 事实块里的 git 行就是这段文本;模型据此知道自己在哪个分支、工作树是否脏。
	for _, s := range w.Sections() {
		if s.Name() == "workspace" {
			fmt.Printf("\n──── workspace 段 ────\n%s", must(s.Render(prompt.Context{})))
		}
	}

	// SandboxPolicy 只填 WorkDir,其余字段原样带走;调用方已经写了一个不一致的
	// WorkDir 时报错而不是静默覆盖——两个路径各执一词正是 workspace 要消灭的漂移。
	p, err := w.SandboxPolicy(sandbox.Policy{Timeout: 10 * time.Second, MaxOutputBytes: 4 << 10})
	check(err)
	fmt.Printf("\nSandboxPolicy → WorkDir = %s\n", p.WorkDir)
	if _, err := w.SandboxPolicy(sandbox.Policy{WorkDir: filepath.Dir(w.Root())}); err != nil {
		fmt.Printf("冲突的 WorkDir → %v\n", err)
	}

	// 命令确实从根目录起步。AllowedCommands 管"能起哪个可执行程序",与技能名单
	// (管"能调哪个工具")是两层,互不代替。
	sb, err := w.Sandbox(sandbox.Policy{Timeout: 10 * time.Second, AllowedCommands: []string{"git"}})
	check(err)
	out, err := sb.Run(context.Background(), sandbox.Spec{Command: "git", Args: []string{"rev-parse", "--show-toplevel"}})
	if err != nil {
		fmt.Printf("run git rev-parse: %v\n", err)
	} else {
		fmt.Printf("git rev-parse --show-toplevel → %s(退出码 %d)\n", strings.TrimSpace(string(out.Stdout)), out.ExitCode)
	}

	// 提示词里报给模型的 cwd 与沙箱的 WorkDir 同源:WithWorkingDir 覆盖掉
	// Environment 默认的 os.Getwd(),两者永远指向同一个目录。
	envRoot := render(prompt.Environment(prompt.WithWorkingDir(w.Root())))
	envPlain := render(prompt.Environment())
	fmt.Printf("\nEnvironment(WithWorkingDir(root)) 的 cwd 行: %s\n", cwdLine(envRoot))
	fmt.Printf("Environment() 默认(进程 cwd)      的 cwd 行: %s\n", cwdLine(envPlain))

	// Workspace 持有打开的句柄,所以有生命周期:Close 之后句柄即失效。
	check(w.Close())
	if _, err := w.FS().ReadFile("README.md"); err != nil {
		fmt.Printf("Close() 之后 FS 读取失败(预期): %v\n", err)
	} else {
		fmt.Println("Close() 之后仍能读取? 这不对。")
	}
}

// ── 小工具 ───────────────────────────────────────────────────────

func callTool(tl tool.Tool, args string) string {
	res, err := tl.Call(&tool.Context{Context: context.Background()}, json.RawMessage(args))
	if err != nil {
		// 文件工具把失败写成工具结果(数据),不会返回 Go error;真返回了就是 bug。
		return fmt.Sprintf("!! Go error(不该发生): %v", err)
	}
	var b strings.Builder
	for _, part := range res.Content {
		if t, ok := part.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	if res.IsError {
		return "IsError=true → " + b.String()
	}
	return "ok → " + strings.ReplaceAll(b.String(), "\n", "\\n")
}

func toolByName(tools []tool.Tool) map[string]tool.Tool {
	m := make(map[string]tool.Tool, len(tools))
	for _, tl := range tools {
		m[tl.Name()] = tl
	}
	return m
}

func toolNames(tools []tool.Tool) string {
	names := make([]string, 0, len(tools))
	for _, tl := range tools {
		names = append(names, tl.Name())
	}
	return strings.Join(names, ", ")
}

func skillNames(w *workspace.Workspace) string {
	lib := w.Skills()
	if lib == nil {
		return "(无)"
	}
	names := make([]string, 0, lib.Len())
	for _, s := range lib.List() {
		names = append(names, s.Name)
	}
	return strings.Join(names, ", ")
}

func render(s prompt.Section) string { return must(s.Render(prompt.Context{})) }

func cwdLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "Working directory") {
			return strings.TrimSpace(line)
		}
	}
	return "(没找到)"
}

func show(label, result string) { fmt.Printf("%-28s %s\n", label, result) }

// tree 列出目录结构(最深四层),用来对照 Config 里的约定路径。
func tree(root string) string {
	var b strings.Builder
	if err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(filepath.ToSlash(p), filepath.ToSlash(root)), "/")
		if rel == "" {
			return nil
		}
		depth := strings.Count(rel, "/")
		if depth > 3 {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		name := filepath.Base(p)
		if info.IsDir() {
			name += "/"
		}
		fmt.Fprintf(&b, "%s%s\n", strings.Repeat("  ", depth+1), name)
		return nil
	}); err != nil {
		fmt.Fprintf(&b, "  (遍历中断:%v)\n", err)
	}
	return b.String()
}

func writeFile(path, content string) {
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
