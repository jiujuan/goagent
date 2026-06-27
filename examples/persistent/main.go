// Command persistent shows durable, cross-process resume with the file
// checkpointer: a run pauses for human approval and is written to disk; a fresh
// agent (simulating a process restart) resumes it from the same directory. Uses
// the mock provider, so it runs offline and deterministically.
//
//	go run ./examples/persistent
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func buildAgent(store checkpoint.Checkpointer) *agent.Agent {
	deleteFile := tool.New("delete_file", "删除文件", func(_ *tool.Context, in struct {
		Path string `json:"path"`
	}) (string, error) {
		return "已删除 " + in.Path, nil
	})
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("完成:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "delete_file", `{"path":"/tmp/old.log"}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(deleteFile),
		agent.WithMiddleware(middleware.Permission(middleware.RequireApprovalFor("delete_file"))),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		log.Fatal(err)
	}
	return a
}

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "goagent-persist-*")
	defer os.RemoveAll(dir)
	const thread = "job-1"

	// 进程 1:跑到审批点暂停,状态落盘。
	fmt.Println("【进程 1】启动,落盘目录:", dir)
	s1, _ := checkpoint.NewFile(dir)
	run := buildAgent(s1).Stream(ctx, "请删除 /tmp/old.log", agent.OnThread(thread))
	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
			fmt.Printf("  ⏸️  暂停等待审批:%s,已 checkpoint 到磁盘。进程可以退出了。\n", it.Pending[0].Tool)
		}
	}

	// 进程 2(模拟重启):全新 agent + 全新 File(同目录)→ 从磁盘恢复并批准。
	fmt.Println("【进程 2】重启,从磁盘恢复线程", thread)
	s2, _ := checkpoint.NewFile(dir)
	a2 := buildAgent(s2)
	for _, p := range pending {
		run.Decide(agent.Allow(p.CallID))
	}
	cont, err := a2.Resume(ctx, thread, agent.Allow(pending[0].CallID))
	if err != nil {
		log.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  ▶️  恢复并完成 →", res.Message.Text())
}
