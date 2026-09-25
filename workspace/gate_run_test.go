package workspace

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/skills"
)

// The gate is only a declaration until it is mounted on a run. This drives the
// whole loop through a real Agent: use_skill activates the skill, the next call
// to a tool the skill did not claim pauses for a human, and approving it lets
// the write land in the workspace root without a second pause.
func TestGateOnLiveRunInterruptsOnceThenApproves(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	skillTreeAt(t, filepath.Join(dir, userDirName, "skills"), map[string]string{
		"pdf": "---\nname: pdf\ndescription: pdfs\nallowed-tools: [read_file]\n---\nread pdfs with read_file",
	})
	w := newAt(t, Config{Dir: dir, SkillGate: true})

	sb, err := w.Sandbox(sandbox.Policy{AllowedCommands: []string{"sh"}})
	if err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewMemory()
	turn := 0
	model := mock.New("mock", func(*llm.Request) *llm.Response {
		defer func() { turn++ }()
		switch turn {
		case 0:
			return mock.CallTool("s1", "use_skill", `{"name":"pdf"}`)
		case 1:
			return mock.CallTool("w1", "write_file", `{"path":"notes.txt","content":"from the model"}`)
		default:
			return mock.Text("finished")
		}
	})

	opts := []agent.Option{
		agent.WithName("workspace-gate"),
		agent.WithModel(model),
		agent.WithTools(append(w.Tools(), w.SkillTools(sb)...)...),
		agent.WithCheckpointer(store),
	}
	// ADR 0022's mounting rule, exercised rather than asserted elsewhere: a nil
	// gate is not mounted at all.
	if gate := w.Gate(); gate != nil {
		opts = append(opts, agent.WithMiddleware(gate))
	}
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "take notes", agent.OnThread("t1"))
	var loaded, wrote, pauses int
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			if e.Result.Name == "use_skill" {
				loaded++
			}
			if e.Result.Name == "write_file" {
				wrote++
			}
		case core.Interrupted:
			pauses++
			if len(e.Pending) != 1 || e.Pending[0].Tool != "write_file" || e.Pending[0].CallID != "w1" {
				t.Fatalf("pending = %+v, want the single unlisted write_file call", e.Pending)
			}
		}
	}
	if loaded != 1 || wrote != 0 || pauses != 1 {
		t.Fatalf("first pass: loaded=%d wrote=%d pauses=%d, want 1/0/1", loaded, wrote, pauses)
	}
	if _, err := w.FS().ReadFile("notes.txt"); err == nil {
		t.Fatal("write_file ran despite the gate")
	}

	run.Decide(agent.Allow("w1"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatalf("continued run: %v", err)
	}
	if res.Message.Text() != "finished" {
		t.Errorf("final message = %q", res.Message.Text())
	}
	// The approved call actually executed, and the checkpoint it ran from still
	// lists pdf as active — so the same tool needs no further approval.
	if b, err := w.FS().ReadFile("notes.txt"); err != nil || string(b) != "from the model" {
		t.Errorf("notes.txt = %q (%v), want the approved write to land in the root", b, err)
	}
	if active := activeSkills(t, store, "t1"); len(active) != 1 || active[0] != "pdf" {
		t.Errorf("active skills after resume = %v, want [pdf]", active)
	}
}

// activeSkills reads the activation list back out of the checkpointed state, the
// way a resumed run sees it.
func activeSkills(t *testing.T, store *checkpoint.Memory, threadID string) []string {
	t.Helper()
	cp, err := store.Latest(context.Background(), threadID)
	if err != nil || cp == nil {
		t.Fatalf("Latest(%s): %v", threadID, err)
	}
	return skills.Active(&cp.State)
}
