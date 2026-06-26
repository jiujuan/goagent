// Command sandbox demonstrates giving an agent a restricted run_command tool.
// The mock provider (no API key) asks to run one allow-listed command inside a
// sandbox.Policy that confines the working directory, caps output, bounds
// runtime, and exposes only an explicit set of environment variables.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/sandbox/process"
	"github.com/jiujuan/goagent/tool"
	toolexec "github.com/jiujuan/goagent/tool/exec"
)

func main() {
	// Pick a command that exists on this OS. Empty environments break some
	// shells, so we opt a couple of variables through the allow-list.
	command, cmdArgs, env := platformCommand()

	workDir, err := os.MkdirTemp("", "goagent-sandbox-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workDir)

	// 1. A process sandbox enforcing the five portable limits.
	sb, err := process.New(sandbox.Policy{
		WorkDir:         workDir,
		Timeout:         5 * time.Second,
		MaxOutputBytes:  64 * 1024,
		AllowedCommands: []string{command}, // only this command may run
		Env:             env,               // only these vars are exposed
	})
	if err != nil {
		log.Fatal(err)
	}

	// 2. Wrap it as a run_command tool the agent can call.
	runCmd := toolexec.RunCommand(sb)

	// 3. A scripted mock model: first call requests run_command, second call
	//    summarizes the tool result.
	model := mock.New("mock-opus", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("命令已在沙箱中执行，输出：\n" + partsText(tr.Content))
		}
		args, _ := json.Marshal(map[string]any{"command": command, "args": cmdArgs})
		return mock.CallTool("call_1", "run_command", string(args))
	})

	assistant := agent.New(agent.Config{
		Name:        "assistant",
		Description: "会用沙箱执行命令的助手",
		Model:       model,
		Instruction: "你可以在受限沙箱中执行命令。",
		Tools:       []tool.Tool{runCmd},
	})

	r := runner.New(runner.Config{Root: assistant})
	ctx := context.Background()
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText("帮我在沙箱里跑个命令看看。")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

// platformCommand returns an allow-listed command, its args, and the minimal
// environment it needs, chosen per OS.
func platformCommand() (string, []string, map[string]string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "echo hello from sandbox"},
			map[string]string{
				"PATH":       os.Getenv("PATH"),
				"SystemRoot": os.Getenv("SystemRoot"),
			}
	}
	return "echo", []string{"hello from sandbox"},
		map[string]string{"PATH": os.Getenv("PATH")}
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
				fmt.Printf("🔧 tool:      %s ->\n%s\n", tr.Name, partsText(tr.Content))
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
