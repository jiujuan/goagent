package agent_test

// BC-06（ADR-0033 V9）：跨进程读回终态。子进程走真实后台路径（入队 + worker +
// WaitRecorded）并确认落盘后退出，父进程用全新 File store 读出 completed 终态与文件，
// 再从这份终态起新运行读回文件。这里不是"同一进程换一个 Agent 实例"冒充跨进程。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/vfs"
)

const runStatusProcessHelper = "GOAGENT_RUN_STATUS_PROCESS_HELPER"

const (
	runStatusProcessDir    = "GOAGENT_RUN_STATUS_PROCESS_DIR"
	runStatusProcessThread = "GOAGENT_RUN_STATUS_PROCESS_THREAD"
)

// runStatusProcessReport 是子进程写回父进程的事实：它在自己那一侧读到的终态。
type runStatusProcessReport struct {
	JobID        string `json:"job_id"`
	RunID        string `json:"run_id"`
	CheckpointID string `json:"checkpoint_id"`
	Revision     uint64 `json:"revision"`
	Reply        string `json:"reply"`
	Kind         string `json:"kind"`
	FileText     string `json:"file_text"`
}

func TestRunStatusAcrossProcess(t *testing.T) {
	if os.Getenv(runStatusProcessHelper) == "1" {
		runStatusAcrossProcessChild(t)
		return
	}

	dir := t.TempDir()
	threadID := "cross-process-status"
	child := startRunStatusProcess(t, dir, threadID)

	// 子进程已经退出：它那一侧的 store 句柄连同进程一起没了，父进程只能靠磁盘。
	report := child.wait(t)

	// 全新的 File store，全新的 Agent 实例：只认磁盘上的 JSONL。
	opened := mustReopenFile(t, dir)
	reader := recordAgent(t, opened, "父进程不该驱动运行")

	status, err := reader.RunStatus(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Exists || status.Paused {
		t.Fatalf("跨进程读到的 head 不对: exists=%v paused=%v", status.Exists, status.Paused)
	}
	if status.Completion == nil || status.Completion.Kind != checkpoint.CompletionCompleted {
		t.Fatalf("跨进程没有读到 completed 终态: %+v", status.Completion)
	}
	if status.Completion.RunID != report.RunID {
		t.Fatalf("终态归属与子进程读到的不一致: %q vs %q", status.Completion.RunID, report.RunID)
	}
	if status.Latest.ID != report.CheckpointID || status.Revision != report.Revision {
		t.Fatalf("head 与子进程读到的不是同一条: %s/%d vs %s/%d",
			status.Latest.ID, status.Revision, report.CheckpointID, report.Revision)
	}
	if got := status.Completion.Result.Message.Text(); got != report.Reply {
		t.Fatalf("终态结果与子进程报告不一致: %q vs %q", got, report.Reply)
	}
	if got := string(status.Latest.FileSnapshot["note.txt"]); got != report.FileText {
		t.Fatalf("跨进程读回的文件 = %q, want %q", got, report.FileText)
	}

	// 终态记录不只是"历史里的一条"：父进程的新运行从它恢复文件。
	peeker := terminalAgent(t, mustReopenFile(t, dir), peekModel(), peekFile())
	out, err := peeker.Run(context.Background(), "文件里写了什么？", agent.OnThread(threadID))
	if err != nil {
		t.Fatal(err)
	}
	if out != "read:"+report.FileText {
		t.Fatalf("新运行读回 %q, want read:%s", out, report.FileText)
	}

	// 新运行推进了 head，子进程写下的那条终态记录仍在历史里，且仍只有这一条。
	after, err := reader.RunStatus(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision <= status.Revision {
		t.Fatalf("新运行没有推进 head: revision %d -> %d", status.Revision, after.Revision)
	}
	if got := statusTerminalCount(t, opened, threadID); got != 1 {
		t.Fatalf("历史里的终态记录 = %d 条, want 1", got)
	}
}

// runStatusAcrossProcessChild 在子进程里跑完整后台链路：worker 由队列驱动，提交 ctx
// 在入队后立即取消（V1 的跨进程版本），WaitRecorded 返回 nil 才认为终态已落盘。
func runStatusAcrossProcessChild(t *testing.T) {
	t.Helper()
	dir := os.Getenv(runStatusProcessDir)
	threadID := os.Getenv(runStatusProcessThread)
	if dir == "" || threadID == "" {
		t.Fatal("子进程缺少目录或线程参数")
	}

	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(
		agent.WithModel(stashThenAnswerModel("子进程的终态回答", "cross-process-secret")),
		agent.WithCheckpointer(store),
		agent.WithTools(stashFile()),
	)
	if err != nil {
		t.Fatal(err)
	}

	q := queue.NewMemQueue(4)
	pool := queue.NewPool(q, 1)
	poolCtx, stopPool := context.WithCancel(context.Background())
	defer stopPool()
	go pool.Run(poolCtx)

	submitCtx, cancelSubmit := context.WithCancel(context.Background())
	defer cancelSubmit()
	h, err := queue.EnqueueAgent(submitCtx, q, a, "把笔记存下来",
		agent.OnThread(threadID), agent.WithRunFiles(vfs.NewInState()))
	if err != nil {
		t.Fatal(err)
	}
	// 提交方立刻退出：已入队的运行归队列所有，必须照常跑完并写下终态。
	cancelSubmit()

	res, err := h.WaitRecorded()
	if err != nil {
		t.Fatalf("WaitRecorded: %v", err)
	}

	// 子进程自己确认落盘事实，报告给父进程比对。
	status, err := a.RunStatus(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Completion == nil {
		t.Fatal("WaitRecorded 之后没有终态记录")
	}
	if status.Latest.Completion.RunID != status.Completion.RunID {
		t.Fatal("终态记录字段不一致")
	}

	report := runStatusProcessReport{
		JobID:        h.ID,
		RunID:        status.Completion.RunID,
		CheckpointID: status.Latest.ID,
		Revision:     status.Revision,
		Reply:        res.Message.Text(),
		Kind:         status.Completion.Kind,
		FileText:     string(status.Latest.FileSnapshot["note.txt"]),
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		t.Fatal(err)
	}
	q.Close()
}

// stashThenAnswerModel 先调一次 stash 工具（于是有中间提交边界与文件快照），再回答。
func stashThenAnswerModel(reply, text string) llm.Model {
	return mock.New("child", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text(reply)
		}
		return mock.CallTool("s1", "stash", fmt.Sprintf(`{"path":"note.txt","text":%q}`, text))
	})
}

type runStatusProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr *bytes.Buffer
	output *bytes.Buffer
	cancel context.CancelFunc
	drain  chan struct{}
}

func startRunStatusProcess(t *testing.T, dir, threadID string) *runStatusProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestRunStatusAcrossProcess$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(),
		runStatusProcessHelper+"=1",
		runStatusProcessDir+"="+dir,
		runStatusProcessThread+"="+threadID,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}

	// 子进程的全部输出先收进内存：测试框架的失败信息也走 stdout，边退出边解析会撞车。
	p := &runStatusProcess{cmd: cmd, stdout: stdout, stderr: &stderr, output: &bytes.Buffer{}, cancel: cancel, drain: make(chan struct{})}
	t.Cleanup(p.cleanup)
	go func() {
		defer close(p.drain)
		_, _ = io.Copy(p.output, stdout)
	}()
	return p
}

// wait 等子进程退出并交出它报告的事实：非零退出、报告缺失或字段为空都直接结束测试，
// 并把子进程的完整输出带进失败信息。
func (p *runStatusProcess) wait(t *testing.T) runStatusProcessReport {
	t.Helper()
	err := p.cmd.Wait()
	<-p.drain
	if err != nil {
		p.cancel()
		t.Fatalf("子进程退出失败: %v\nstdout:\n%s\nstderr:\n%s", err, p.output.String(), p.stderr.String())
	}
	p.cancel()

	var report runStatusProcessReport
	for _, line := range strings.Split(p.output.String(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
			continue
		}
		if decodeErr := json.Unmarshal([]byte(line), &report); decodeErr == nil {
			break
		}
	}
	if report.RunID == "" || report.CheckpointID == "" || report.Kind == "" {
		t.Fatalf("子进程没有交出终态报告\nstdout:\n%s\nstderr:\n%s", p.output.String(), p.stderr.String())
	}
	return report
}

func (p *runStatusProcess) cleanup() {
	_ = p.stdout.Close()
	if p.cmd.Process != nil && p.cmd.ProcessState == nil {
		_ = p.cmd.Process.Kill()
	}
	// Wait 可能已由 wait 调过；这里只兜底，忽略重复等待的错误。
	_ = p.cmd.Wait()
	<-p.drain
	p.cancel()
}

func mustReopenFile(t *testing.T, dir string) checkpoint.Checkpointer {
	t.Helper()
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
