// Command skills-gate 演示 workspace 的技能侧四件套:合并后的技能库(Skills)、
// 两个技能工具(SkillTools)、提示词里的 Level-1 清单(Sections),以及把
// SKILL.md 的 allowed-tools 变成"免审名单"的门控(Gate)。
//
//	go run ./examples/workspace/skills-gate
//
// 离线、无需密钥:模型是 llm/mock 的确定性剧本,两轮 use_skill/read_file 之后
// 故意调一次技能名单外的 write_file,让运行停在 HITL 人工批准上,再批准并续跑。
// 结尾用同一剧本对比没开 SkillGate 的情形——那时 Gate() 返回 nil,越界调用不会
// 中断,这正是"不挂门控就与现状逐字节相同"的含义。
package main

import (
	"context"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/skills"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/workspace"
)

func main() {
	root, userHome := fixture()
	defer os.RemoveAll(root)
	defer os.RemoveAll(userHome)

	fmt.Println("场景 A —— 开了 SkillGate:名单外的工具调用会停下来问人")
	scenario(root, userHome, true)

	fmt.Println("\n\n场景 B —— 没开 SkillGate:Gate() 为 nil,同一剧本一路跑到底")
	scenario(root, userHome, false)
}

// scenario 装配一个 workspace + Agent,跑同一份剧本;gate 决定是否挂门控中间件。
func scenario(root, userHome string, gate bool) {
	w, err := workspace.New(workspace.Config{
		Dir:             root,
		GlobalSkillsDir: filepath.Join(userHome, ".goagent", "skills"),
		SkillGate:       gate,
	})
	check(err)
	defer w.Close()

	// 合并结果:同名技能工作区胜,只在全局存在的那些照旧可用。
	fmt.Printf("\n合并后的技能库(全局 → 工作区):\n%s", describeSkills(w))

	sb, err := w.Sandbox(sandbox.Policy{Timeout: 10 * time.Second, AllowedCommands: []string{"sh"}})
	check(err)

	store := checkpoint.NewMemory()
	turn := 0
	// 剧本:加载技能 → 用名单内的 read_file → 用名单外的 write_file → 收尾。
	model := mock.New("mock", func(*llm.Request) *llm.Response {
		defer func() { turn++ }()
		switch turn {
		case 0:
			return mock.CallTool("s1", "use_skill", `{"name":"wordcount"}`)
		case 1:
			return mock.CallTool("r1", "read_file", `{"path":"notes.txt"}`)
		case 2:
			return mock.CallTool("w1", "write_file", `{"path":"report.md","content":"# 统计结果\n12 词\n"}`)
		default:
			return mock.Text("已把统计结果写进 report.md。")
		}
	})

	tools := append(w.Tools(), w.SkillTools(sb)...)
	fmt.Printf("挂上的工具:%s\n", join(toolNames(tools)))

	opts := []agent.Option{
		agent.WithName("skills-gate-demo"),
		agent.WithModel(model),
		// Level-1 技能清单来自 ws.Sections(),use_skill 由 ws.SkillTools 提供。
		agent.WithPrompt(prompt.New().
			Add(prompt.Identity("你在一个受工作区约束的目录里干活,按技能说明行事。")).
			Add(prompt.Environment(prompt.WithWorkingDir(w.Root()))).
			Add(w.Sections()...)),
		agent.WithTools(tools...),
		agent.WithCheckpointer(store),
	}
	// Gate() 未启用或没有技能时返回 nil;nil 不挂载。
	if mw := w.Gate(); mw != nil {
		opts = append(opts, agent.WithMiddleware(mw))
		fmt.Println("门控中间件:已挂载(allowed-tools 就是免审名单)")
	} else {
		fmt.Println("门控中间件:nil(未配置 SkillGate,名单外的调用照跑)")
	}

	a, err := agent.New(opts...)
	check(err)

	ctx := context.Background()
	run := a.Stream(ctx, "统计 notes.txt 的词数并写进 report.md", agent.OnThread("t1"))
	pending := drain(run.Iter())
	if len(pending) == 0 {
		fmt.Println("  全程没有中断:没有门控时,名单外的调用照旧静默执行。")
		printReport(w)
		return
	}

	// 中断是可持久的事实:pending 已写进 checkpoint,人随时可以决定。
	resumeAndFinish(ctx, run, store, pending, w)
}

// resumeAndFinish 批准第一个待决调用并续跑,展示批准之后发生什么。
func resumeAndFinish(ctx context.Context, run *agent.Run, store *checkpoint.Memory, pending []core.ApprovalRequest, w *workspace.Workspace) {
	cp, err := store.Latest(ctx, "t1")
	check(err)
	if cp == nil || cp.Pending == nil {
		fmt.Println("  !! 中断没有落进 checkpoint,这不对")
		return
	}
	// 激活名单跟着 State.KV 一起被快照:resume / branch / 子 Agent 都带得走。
	fmt.Printf("  checkpoint 里的激活技能:%s\n", join(skills.Active(&cp.State)))

	fmt.Println("  人批准了这次 write_file(批准后由恢复流程直接执行,不再过门控),续跑:")
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	check(err)
	drain(cont.Iter())
	printReport(w)
}

// drain 打印一轮运行的过程事件,并把停在人工批准上的调用带回来。
func drain(events iter.Seq2[core.Event, error]) []core.ApprovalRequest {
	var pending []core.ApprovalRequest
	for ev, err := range events {
		check(err)
		switch e := ev.(type) {
		case core.ToolStarted:
			fmt.Printf("  → %s %s\n", e.Call.Name, clip(string(e.Call.Args), 60))
		case core.ToolDone:
			fmt.Printf("  ← %s:%s %s\n", e.Result.Name, status(e.Result), clip(text(e.Result.Content), 70))
		case core.Interrupted:
			pending = e.Pending
			for _, p := range pending {
				fmt.Printf("  ⏸ 停在人工批准:%s(%s)\n", p.Tool, p.CallID)
			}
		case core.MessageDone:
			if t := strings.TrimSpace(e.Message.Text()); t != "" {
				fmt.Printf("  🤖 %s\n", t)
			}
		}
	}
	return pending
}

func printReport(w *workspace.Workspace) {
	b, err := w.FS().ReadFile("report.md")
	if err != nil {
		fmt.Printf("  report.md 没写出来:%v\n", err)
		return
	}
	fmt.Printf("  report.md 内容:%q(工作区根 %s 之内)\n", string(b), w.Root())
}

// fixture 摆出两份技能来源:一份当"用户级家目录",一份当仓库工作区。
func fixture() (root, userHome string) {
	root = must(filepath.EvalSymlinks(must(os.MkdirTemp("", "ws-skills-repo"))))
	userHome = must(os.MkdirTemp("", "ws-skills-home"))

	write(filepath.Join(root, "notes.txt"), "one two three four five\n")
	// 工作区技能:声明了 allowed-tools,越界工具才会被门控拦下。
	write(filepath.Join(root, ".goagent", "skills", "wordcount", "SKILL.md"),
		"---\nname: wordcount\ndescription: 统计工作区里文本文件的词数\nallowed-tools: [read_file, list_dir, glob]\n---\n"+
			"# 词数技能\n1. 用 read_file 读目标文件;\n2. 按空白切分计数;\n3. 用 write_file 写出结果。\n")
	// 只存在于全局的技能,用来对照"工作区没有的技能照样可用"。
	write(filepath.Join(userHome, ".goagent", "skills", "summarize", "SKILL.md"),
		"---\nname: summarize\ndescription: 生成中文摘要(用户级技能)\n---\n# 摘要技能\n先读原文,再压成三句。\n")
	// 同名技能的两份:工作区那份必须覆盖全局那份。
	write(filepath.Join(userHome, ".goagent", "skills", "todo", "SKILL.md"),
		"---\nname: todo\ndescription: 待办清单(全局版)\n---\n全局版正文\n")
	write(filepath.Join(root, ".goagent", "skills", "todo", "SKILL.md"),
		"---\nname: todo\ndescription: 待办清单(工作区版,覆盖全局)\n---\n工作区版正文\n")
	return root, userHome
}

func describeSkills(w *workspace.Workspace) string {
	lib := w.Skills()
	if lib == nil {
		return "  (空)\n"
	}
	var b strings.Builder
	for _, s := range lib.List() {
		fmt.Fprintf(&b, "  - %s: %s\n      免审名单 allowed-tools = %s\n", s.Name, s.Description, join(s.AllowedTools))
	}
	return b.String()
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

func status(r core.ToolResult) string {
	if r.IsError {
		return "工具错误"
	}
	return "成功"
}

func toolNames(ts []tool.Tool) []string {
	out := make([]string, 0, len(ts))
	for _, tl := range ts {
		out = append(out, tl.Name())
	}
	return out
}

func join(items []string) string {
	if len(items) == 0 {
		return "(无)"
	}
	return strings.Join(items, ", ")
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
