// Command skills demonstrates the filesystem-based skill system end to end,
// exercising all three progressive-disclosure levels with no API key and no
// network (a scripted mock model plus a process sandbox):
//
//	Level 1  the agent's system prompt lists the "greet" skill (name + desc)
//	Level 2  the model calls use_skill to read the skill's SKILL.md body
//	Level 3  the model calls use_skill to read a bundled resource (template.md),
//	         then run_skill_script to execute a bundled script in the sandbox
//
// The skill bundle (SKILL.md + template.md + scripts for sh/python/node) is
// compiled into the binary with go:embed, so the example is self-contained. The
// example picks whichever interpreter is present on the host.
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/sandbox/process"
	"github.com/jiujuan/goagent/skill"
	"github.com/jiujuan/goagent/tool"
)

//go:embed skills
var bundle embed.FS

// candidate pairs an interpreter command with the bundled script and extension
// it runs, in preference order. The example uses the first one found on PATH.
type candidate struct {
	command string // interpreter to run (must be on PATH and sandbox-allowed)
	script  string // bundled script path within the skill
	ext     string // its extension, to pin the interpreter for that type
}

func main() {
	// 1. Load the embedded skill library (rooted at the "skills" directory).
	skillsFS, err := fs.Sub(bundle, "skills")
	if err != nil {
		log.Fatal(err)
	}
	lib, err := skill.Load(skillsFS)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Pick an interpreter that exists on this host.
	pick, ok := pickInterpreter()
	if !ok {
		log.Fatal("no supported interpreter (sh/bash/python3/python/node) found on PATH")
	}

	// 3. A sandbox that may run only the chosen interpreter. run_skill_script
	//    materializes the bundled script to a temp file and runs it here, so the
	//    sandbox's limits (timeout, output cap, env/command allow-lists) apply.
	workDir, err := os.MkdirTemp("", "goagent-skills-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workDir)

	sb, err := process.New(sandbox.Policy{
		WorkDir:         workDir,
		Timeout:         5 * time.Second,
		MaxOutputBytes:  16 << 10,
		AllowedCommands: []string{pick.command},
		Env:             map[string]string{"PATH": os.Getenv("PATH")},
	})
	if err != nil {
		log.Fatal(err)
	}

	// 4. The agent's prompt advertises skills (Level 1); its tools are the
	//    on-demand skill loader plus the skill script runner. Pin the chosen
	//    interpreter for the script's extension (covers python vs python3).
	sys := prompt.New().
		Add(prompt.Identity("你是一个会按需调用技能的助手。")).
		Add(skill.PromptSection(lib))

	tools := []tool.Tool{
		skill.Tool(lib),
		skill.ScriptTool(lib, sb, skill.WithInterpreter(pick.ext, pick.command)),
	}

	// 5. A scripted mock model walking the three levels in order. It branches on
	//    how many tool steps have already completed.
	scriptCall := fmt.Sprintf(`{"skill":"greet","script":%q}`, pick.script)
	model := mock.New("mock-opus", func(req *llm.Request) *llm.Response {
		switch countToolSteps(req) {
		case 0: // Level 2: read the skill's instructions.
			return mock.CallTool("c1", "use_skill", `{"name":"greet"}`)
		case 1: // Level 3a: read a bundled resource.
			return mock.CallTool("c2", "use_skill", `{"name":"greet","resource":"template.md"}`)
		case 2: // Level 3b: run a bundled script in the sandbox.
			return mock.CallTool("c3", "run_skill_script", scriptCall)
		default: // Done: summarize.
			tr, _ := mock.LastToolResult(req)
			return mock.Text("已按 greet 技能执行脚本，输出：\n" + partsText(tr.Content))
		}
	})

	assistant := agent.New(agent.Config{
		Name:        "assistant",
		Description: "技能驱动的助手",
		Model:       model,
		Prompt:      sys,
		Tools:       tools,
	})

	r := runner.New(runner.Config{Root: assistant})
	ctx := context.Background()
	fmt.Printf("=== 渐进式技能系统：三层加载演示（解释器：%s）===\n", pick.command)
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText("请用 greet 技能跟 Ada 打个招呼。")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

// pickInterpreter returns the first interpreter found on PATH and the bundled
// script it should run.
func pickInterpreter() (candidate, bool) {
	for _, c := range []candidate{
		{"sh", "scripts/greet.sh", ".sh"},
		{"bash", "scripts/greet.sh", ".sh"},
		{"python3", "scripts/greet.py", ".py"},
		{"python", "scripts/greet.py", ".py"},
		{"node", "scripts/greet.js", ".js"},
	} {
		if _, err := exec.LookPath(c.command); err == nil {
			return c, true
		}
	}
	return candidate{}, false
}

// countToolSteps reports how many tool-result turns are already in the history,
// which the mock uses to advance through the three levels.
func countToolSteps(req *llm.Request) int {
	n := 0
	for _, m := range req.Messages {
		if m.Role == core.RoleTool {
			n++
		}
	}
	return n
}

func printEvent(ev *core.Event) {
	if ev == nil || ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("👤 user:      %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				fmt.Printf("🤖 assistant: →调用工具 %s(%s)\n", c.Name, string(c.Args))
			}
			return
		}
		fmt.Printf("🤖 assistant: %s\n", ev.Message.Text())
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("🔧 tool:      %s ->\n%s\n", tr.Name, indent(partsText(tr.Content)))
			}
		}
	}
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}

func indent(s string) string {
	const pad = "             "
	out := pad
	for _, r := range s {
		out += string(r)
		if r == '\n' {
			out += pad
		}
	}
	return out
}
