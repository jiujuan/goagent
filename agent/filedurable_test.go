package agent_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/vfs"
)

// stashFile writes into the run's virtual filesystem; peekFile reads it back.
// Together they prove whether State.Files survived a process boundary.
func stashFile() tool.Tool {
	return tool.New("stash", "stash a file", func(tctx *tool.Context, a struct {
		Path string `json:"path"`
		Text string `json:"text"`
	}) (string, error) {
		if err := tctx.State.Files.Write(a.Path, []byte(a.Text)); err != nil {
			return "", err
		}
		return "stashed", nil
	})
}

func peekFile() tool.Tool {
	return tool.New("peek", "peek a file", func(tctx *tool.Context, a struct {
		Path string `json:"path"`
	}) (string, error) {
		b, err := tctx.State.Files.Read(a.Path)
		if err != nil {
			return "MISSING", nil // tool error would also be visible; keep it text
		}
		return string(b), nil
	})
}

// peekResult finds a peek tool result by name — the restored history already
// contains process 1's stash result, so "last result" would be ambiguous.
func peekResult(req *llm.Request) (string, bool) {
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && tr.Name == "peek" {
				if len(tr.Content) > 0 {
					if t, ok := tr.Content[0].(core.Text); ok {
						return t.Text, true
					}
				}
			}
		}
	}
	return "", false
}

func peekModel() llm.Model {
	return mock.New("p2", func(req *llm.Request) *llm.Response {
		if got, ok := peekResult(req); ok {
			return mock.Text("read:" + got)
		}
		return mock.CallTool("p1", "peek", `{"path":"note.txt"}`)
	})
}

// dangerGate interrupts only the "danger" tool, so the stash step runs freely
// before the pause.
type dangerGate struct{ agent.BaseMiddleware }

func (dangerGate) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if c.Name == "danger" {
		if lc.IsApproved(c) {
			return core.Directive{}, nil
		}
		return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
	}
	return core.Directive{}, nil
}

// TestDurableHITLResumeRestoresFiles covers the Agent.Resume half of ADR-0026:
// process 1 stashes a file and pauses for approval; process 2 resumes the
// thread from disk and the peek after the approved call still sees the file.
func TestDurableHITLResumeRestoresFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	danger := tool.New("danger", "gated op", func(_ *tool.Context, _ struct{}) (string, error) {
		return "banged", nil
	})

	m1 := mock.New("p1", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok && tr.Name == "stash" {
			return mock.CallTool("d1", "danger", `{}`)
		}
		return mock.CallTool("s1", "stash", `{"path":"note.txt","text":"secret"}`)
	})
	s1, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := agent.New(agent.WithModel(m1), agent.WithTools(stashFile(), danger),
		agent.WithMiddleware(dangerGate{}), agent.WithCheckpointer(s1))
	if err != nil {
		t.Fatal(err)
	}

	run := a1.Stream(ctx, "stash then danger", agent.OnThread("h1"))
	var interrupted bool
	for _, ev := range collectStream(run) {
		if _, ok := ev.(core.Interrupted); ok {
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatal("expected HITL pause")
	}

	s2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := agent.New(agent.WithModel(peekModel()), agent.WithTools(stashFile(), peekFile(), danger),
		agent.WithCheckpointer(s2))
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := a2.Resume(ctx, "h1", agent.Allow("d1"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := resumed.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Message.Text(), "read:secret") {
		t.Fatalf("HITL resume lost the file state: %q", res.Message.Text())
	}
}

func collectStream(run *agent.Run) []core.Event {
	var out []core.Event
	for ev, err := range run.Iter() {
		if err != nil {
			break
		}
		out = append(out, ev)
	}
	return out
}

func fileAgents(m1, m2 llm.Model, dir string) (*agent.Agent, *agent.Agent) {
	s1, err := checkpoint.NewFile(dir)
	if err != nil {
		panic(err)
	}
	s2, err := checkpoint.NewFile(dir)
	if err != nil {
		panic(err)
	}
	a1, _ := agent.New(agent.WithModel(m1), agent.WithTools(stashFile(), peekFile()), agent.WithCheckpointer(s1))
	a2, _ := agent.New(agent.WithModel(m2), agent.WithTools(stashFile(), peekFile()), agent.WithCheckpointer(s2))
	return a1, a2
}

// TestDurableResumeRestoresFiles is ADR-0026's end-to-end contract: process 1
// stashes a file into State.Files and finishes; process 2 (a fresh Agent over
// the same checkpoint directory) resumes the thread and still sees the file.
func TestDurableResumeRestoresFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	m1 := mock.New("p1", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("stored it")
		}
		return mock.CallTool("s1", "stash", `{"path":"note.txt","text":"secret"}`)
	})
	a1, a2 := fileAgents(m1, peekModel(), dir)

	if _, err := a1.Run(ctx, "stash the note", agent.OnThread("notes")); err != nil {
		t.Fatal(err)
	}
	out, err := a2.Run(ctx, "what was stashed?", agent.OnThread("notes"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "read:secret") {
		t.Fatalf("file state lost across processes: %q", out)
	}
}

// TestDurableRunFilesOverridePrecedence pins priority: an explicit
// WithRunFiles backend wins over the checkpoint's rehydrated snapshot (T4).
func TestDurableRunFilesOverridePrecedence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	m1 := mock.New("p1", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("stored it")
		}
		return mock.CallTool("s1", "stash", `{"path":"note.txt","text":"secret"}`)
	})
	a1, a2 := fileAgents(m1, peekModel(), dir)

	if _, err := a1.Run(ctx, "stash the note", agent.OnThread("notes2")); err != nil {
		t.Fatal(err)
	}
	empty := vfs.NewInState()
	out, err := a2.Run(ctx, "what was stashed?", agent.OnThread("notes2"), agent.WithRunFiles(empty))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "read:MISSING") {
		t.Fatalf("explicit WithRunFiles must override the snapshot: %q", out)
	}
}

// stashModel calls stash once, then answers.
func stashModel() llm.Model {
	return mock.New("p1", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("stored it")
		}
		return mock.CallTool("s1", "stash", `{"path":"note.txt","text":"secret"}`)
	})
}

// processOneStashes runs the first process of a two-process scenario: it stashes
// note.txt through the given file backend and checkpoints the thread into
// ckptDir. The backend is the caller's to close.
func processOneStashes(t *testing.T, ckptDir string, files core.FileStore) {
	t.Helper()
	store, err := checkpoint.NewFile(ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.WithModel(stashModel()),
		agent.WithTools(stashFile(), peekFile()), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "stash the note",
		agent.OnThread("artifacts"), agent.WithRunFiles(files)); err != nil {
		t.Fatal(err)
	}
}

// processTwoPeeks runs the second process: a fresh Agent over the same
// checkpoint directory, asked what was stashed. Pass files nil to resume without
// re-attaching a backend.
func processTwoPeeks(t *testing.T, ckptDir string, files core.FileStore) string {
	t.Helper()
	store, err := checkpoint.NewFile(ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.WithModel(peekModel()),
		agent.WithTools(stashFile(), peekFile()), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	var opts []agent.RunOption
	if files != nil {
		opts = append(opts, agent.WithRunFiles(files))
	}
	out, err := a.Run(context.Background(), "what was stashed?",
		append([]agent.RunOption{agent.OnThread("artifacts")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestDurableDirStoreResumeAcrossInstances is ADR-0027's end-to-end contract:
// the artifacts live on disk, not in the checkpoint, so a second process that
// re-attaches a DirStore over the same directory reads what the first one wrote.
func TestDurableDirStoreResumeAcrossInstances(t *testing.T) {
	ckptDir := t.TempDir()
	artifacts := filepath.Join(t.TempDir(), "thread-artifacts")

	first, err := vfs.NewDirStore(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	processOneStashes(t, ckptDir, first)

	// Nothing had to be unpacked from the checkpoint to get the file back: it is
	// an ordinary file, openable by anything outside the framework.
	if got, err := os.ReadFile(filepath.Join(artifacts, "note.txt")); err != nil {
		t.Fatalf("artifact never reached the disk: %v", err)
	} else if string(got) != "secret" {
		t.Fatalf("artifact on disk = %q", got)
	}

	second, err := vfs.NewDirStore(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if out := processTwoPeeks(t, ckptDir, second); !strings.Contains(out, "read:secret") {
		t.Fatalf("re-attached DirStore lost the artifacts: %q", out)
	}
}

// TestDurableDirStoreForgetsHandleYieldsEmpty records the cost of external
// management (ADR-0027): resuming the thread from the checkpoint alone gives an
// empty file surface, because the checkpoint never carried the DirStore's bytes.
// The framework cannot tell "this thread has no artifacts" from "the caller
// forgot to re-attach the handle" — both look like an empty store, so the
// requirement to re-attach is documented on agent.WithRunFiles instead.
func TestDurableDirStoreForgetsHandleYieldsEmpty(t *testing.T) {
	ckptDir := t.TempDir()
	artifacts := filepath.Join(t.TempDir(), "thread-artifacts")

	first, err := vfs.NewDirStore(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	processOneStashes(t, ckptDir, first)

	// The file is still there, untouched by the resume that cannot see it.
	if _, err := os.Stat(filepath.Join(artifacts, "note.txt")); err != nil {
		t.Fatalf("artifact disappeared: %v", err)
	}
	if out := processTwoPeeks(t, ckptDir, nil); !strings.Contains(out, "read:MISSING") {
		t.Fatalf("resuming without the handle should see an empty file surface: %q", out)
	}
}
