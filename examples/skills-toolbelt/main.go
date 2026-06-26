// Command skills-toolbelt 是一个「真实大模型 + 多技能路由」的复杂示例：一个由
// Agnes 网关上真实 LLM 驱动的研发助理，面对一个**复合任务**，必须自己在多个技能
// 之间正确路由——既有纯知识技能（无脚本），也有需要在沙箱里跑脚本的技能。没有 mock。
//
// 库里有三个技能：
//
//	commit-message  纯知识技能（无脚本）：教模型 Conventional Commits 规范，
//	                模型读完 SKILL.md 后直接产出文本（只用到 Level 2）。
//	slugify         脚本技能：把标题转成 URL slug（Level 2 + Level 3 跑 Python）。
//	secret-scan     脚本技能（安全向）：扫描文本里疑似硬编码的密钥/凭证。
//
// 用户一句话里塞进三件事，模型需要：识别该用哪个技能、按需 use_skill 读说明、
// 该跑脚本的去沙箱跑脚本、最后汇总。技能选择与调用顺序完全由真实模型决定。
//
// 技能包用 go:embed 编进二进制。运行（需本机有 python3 / python，并配置 Agnes 凭证）：
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_BASE_URL=https://apihub.agnes-ai.com/v1   # 可选，默认即此
//	export AGNES_MODEL=claude-sonnet-4-6                   # 可选
//	go run ./examples/skills-toolbelt
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

// 复合任务：一句话里有三件事，分别对应 commit-message / slugify / secret-scan。
// 末尾故意塞了一个假的 AWS Key + sk- 形式 key，让 secret-scan 一定能命中。
const userTask = `我刚写完一个修复登录超时的功能：把客户端超时从 10s 提到 30s，避免上游 IdP 慢响应时误报 504。
请帮我做三件事：
1) 生成一条符合规范的 git commit message（type 用 fix，scope 用 auth）；
2) 为标题 "Fix login timeout for slow IdP" 生成一个 URL slug；
3) 扫描下面这段配置看有没有泄露密钥：
   AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
   OPENAI_API_KEY=sk-abcd1234efgh5678ijkl
   DB_HOST=10.0.0.5`

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		log.Fatal("请先 `export AGNES_API_KEY=sk-...` 再运行本示例")
	}
	baseURL := getenv("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	modelID := getenv("AGNES_MODEL", "claude-sonnet-4-6")

	py, ok := pickPython()
	if !ok {
		log.Fatal("未在 PATH 上找到 python3 或 python；slugify / secret-scan 技能需要 Python")
	}

	skillsFS, err := fs.Sub(bundle, "skills")
	if err != nil {
		log.Fatal(err)
	}
	lib, err := skill.Load(skillsFS)
	if err != nil {
		log.Fatal(err)
	}

	// 沙箱只放行选定的 Python 解释器。
	workDir, err := os.MkdirTemp("", "goagent-toolbelt-*")
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

	model := openaicompat.Agnes(baseURL, modelID, key)

	sys := prompt.New().
		Add(prompt.Identity("你是一名研发助理。面对复合请求时，先判断每个子任务该用哪个技能，再按需逐一处理，最后清晰汇总每一项的结果。")).
		Add(skill.PromptSection(lib))

	tools := []tool.Tool{
		skill.Tool(lib),
		skill.ScriptTool(lib, sb, skill.WithInterpreter(".py", py)),
	}

	assistant := agent.New(agent.Config{
		Name:         "devhelper",
		Description:  "技能驱动的研发助理",
		Model:        model,
		Prompt:       sys,
		Tools:        tools,
		MaxSteps:     16,
		ModelOptions: []llm.Option{llm.WithTemperature(0)},
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	r := runner.New(runner.Config{Root: assistant})
	fmt.Printf("=== 多技能路由（真实模型：%s @ %s，解释器：%s）===\n\n", modelID, baseURL, py)

	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText(userTask)) {
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

func printEvent(ev *core.Event) {
	if ev == nil || ev.Message == nil || ev.Partial {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("👤 user:\n%s\n\n", ev.Message.Text())
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
