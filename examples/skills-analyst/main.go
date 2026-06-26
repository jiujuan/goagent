// Command skills-analyst 是一个「真实大模型 + 技能系统」的复杂示例：一个由
// Agnes 网关上真实 LLM 驱动的数据分析 Agent，端到端走完技能的三层渐进式加载，
// 并在沙箱里执行技能自带的 Python 统计脚本。没有任何 mock。
//
// 流程（全部由真实模型自己决策，不是脚本编排）：
//
//	Level 1  系统提示里列出 sales-report 技能（名称 + 描述）
//	Level 2  模型调用 use_skill 读取 SKILL.md 工作流
//	Level 3a 模型调用 use_skill 读取 dataset.md（原始销售数据）和 report-template.md
//	Level 3b 模型从数据里抽取 12 个月销售额，作为参数调用 run_skill_script
//	         在沙箱中运行 scripts/stats.py，拿到真实统计指标
//	最后     模型用脚本输出的真实数字，按模板写出销售分析报告
//
// 技能包用 go:embed 编进二进制，因此示例自带数据，无需外部文件。
//
// 运行（需要本机有 python3 / python，且配置 Agnes 凭证）：
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_BASE_URL=https://apihub.agnes-ai.com/v1   # 可选，默认即此
//	export AGNES_MODEL=claude-sonnet-4-6                   # 可选，按网关支持的模型填
//	go run ./examples/skills-analyst
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/sandbox/process"
	"github.com/jiujuan/goagent/skill"
	"github.com/jiujuan/goagent/tool"
)

//go:embed skills
var bundle embed.FS

func main() {
	// 1. Agnes 凭证 / 端点 / 模型：openaicompat.Agnes 走 OpenAI 兼容的 /chat/completions。
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		log.Fatal("请先 `export AGNES_API_KEY=sk-...` 再运行本示例")
	}
	baseURL := getenv("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	modelID := getenv("AGNES_MODEL", "claude-sonnet-4-6")

	// 2. 需要一个真实的脚本解释器（python3 / python）来执行技能自带脚本。
	py, ok := pickPython()
	if !ok {
		log.Fatal("未在 PATH 上找到 python3 或 python；本示例需要 Python 来运行统计脚本")
	}

	// 3. 加载内嵌技能库（根目录为 skills/）。
	skillsFS, err := fs.Sub(bundle, "skills")
	if err != nil {
		log.Fatal(err)
	}
	lib, err := skill.Load(skillsFS)
	if err != nil {
		log.Fatal(err)
	}

	// 4. 沙箱：只允许运行选定的 Python 解释器；技能脚本在这里以受限方式执行
	//    （超时、输出上限、命令白名单）。Windows 上 python 需要 SystemRoot。
	workDir, err := os.MkdirTemp("", "goagent-analyst-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workDir)

	sb, err := process.New(sandbox.Policy{
		WorkDir:         workDir,
		Timeout:         10 * time.Second,
		MaxOutputBytes:  16 << 10,
		AllowedCommands: []string{py},
		Env: map[string]string{
			"PATH":       os.Getenv("PATH"),
			"SystemRoot": os.Getenv("SystemRoot"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// 5. 真实模型：openaicompat 默认带 5 分钟 HTTP 超时，足以覆盖多轮工具调用。
	model := openaicompat.Agnes(baseURL, modelID, key)

	// 6. 组装 Agent：
	//    - 系统提示 = 身份 + 技能清单（Level 1）
	//    - 工具 = 按需加载技能（use_skill）+ 在沙箱跑技能脚本（run_skill_script）
	//    把 .py 解释器钉成本机选中的命令（覆盖 python vs python3 差异）。
	sys := prompt.New().
		Add(prompt.Identity("你是一名严谨的数据分析师。涉及统计计算时，必须调用技能脚本，绝不自己心算。")).
		Add(skill.PromptSection(lib))

	tools := []tool.Tool{
		skill.Tool(lib),
		skill.ScriptTool(lib, sb, skill.WithInterpreter(".py", py)),
	}

	analyst := agent.New(agent.Config{
		Name:         "analyst",
		Description:  "技能驱动的数据分析师",
		Model:        model,
		Prompt:       sys,
		Tools:        tools,
		MaxSteps:     12,
		ModelOptions: []llm.Option{llm.WithTemperature(0)},
	})

	// 7. 运行一轮，把每个事件打印出来，观察模型如何逐层加载并调用技能。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	r := runner.New(runner.Config{Root: analyst})
	fmt.Printf("=== 技能驱动的数据分析（真实模型：%s @ %s，解释器：%s）===\n\n", modelID, baseURL, py)

	q := "请使用 sales-report 技能，基于内置的月度销售数据，生成一份完整的季度销售分析报告。"
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText(q)) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

// pickPython 返回本机第一个**可用**的 Python 命令。它真正执行 `--version` 验证，
// 从而跳过 Windows 应用商店里那个一运行就弹商店、并不能真正执行脚本的 python3 占位程序。
func pickPython() (string, bool) {
	for _, c := range []string{"python3", "python", "py"} {
		if _, err := exec.LookPath(c); err != nil {
			continue
		}
		out, err := exec.Command(c, "--version").CombinedOutput()
		if err == nil && strings.Contains(string(out), "Python") {
			return c, true
		}
	}
	return "", false
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

// printEvent 渲染一个事件：用户 / 助手文本 / 助手工具调用 / 工具结果。
func printEvent(ev *core.Event) {
	if ev == nil || ev.Message == nil || ev.Partial {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("👤 user:      %s\n\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				fmt.Printf("🤖 assistant: → 调用 %s(%s)\n", c.Name, string(c.Args))
			}
			fmt.Println()
			return
		}
		if t := strings.TrimSpace(ev.Message.Text()); t != "" {
			fmt.Printf("🤖 assistant:\n%s\n\n", t)
		}
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("🔧 tool [%s]:\n%s\n\n", tr.Name, indent(partsText(tr.Content)))
			}
		}
	}
}

func partsText(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "    " + l
	}
	return strings.Join(lines, "\n")
}
