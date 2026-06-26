package session_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// TestFileStorePersistAndRecover runs a tool-using turn against a FileStore,
// then opens a brand-new FileStore over the same directory and verifies the
// full conversation (and derived state) is recovered from disk.
func TestFileStorePersistAndRecover(t *testing.T) {
	dir := t.TempDir()

	echo := tool.New("echo", "echo input",
		func(_ *tool.Context, in struct {
			Text string `json:"text"`
		}) (string, error) {
			return "echo:" + in.Text, nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("final after " + partsText(tr.Content))
		}
		return mock.CallTool("c1", "echo", `{"text":"hi"}`)
	})

	ag := agent.New(agent.Config{
		Name:      "a",
		Model:     model,
		Tools:     []tool.Tool{echo},
		OutputKey: "answer",
	})

	// --- session 1: run and persist ---
	store1, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r1 := runner.New(runner.Config{AppName: "app", Root: ag, Store: store1})
	for _, err := range r1.Run(context.Background(), "user", "sess", core.UserText("go")) {
		if err != nil {
			t.Fatal(err)
		}
	}

	// The JSONL file should exist on disk.
	path := filepath.Join(dir, "app", "user", "sess.jsonl")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("session file not written: %v", err)
	}

	// --- session 2: fresh store, recover from disk ---
	store2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store2.GetOrCreate(context.Background(), "app", "user", "sess")
	if err != nil {
		t.Fatal(err)
	}

	msgs := s2.Messages()
	// Expect: user, assistant(tool_call), tool, assistant(final).
	if len(msgs) != 4 {
		t.Fatalf("recovered %d messages, want 4: %+v", len(msgs), msgs)
	}
	wantRoles := []core.Role{core.RoleUser, core.RoleAssistant, core.RoleTool, core.RoleAssistant}
	for i, want := range wantRoles {
		if msgs[i].Role != want {
			t.Fatalf("msg[%d] role = %v, want %v", i, msgs[i].Role, want)
		}
	}
	if last := msgs[3].Text(); last != "final after echo:hi" {
		t.Fatalf("recovered final = %q", last)
	}
	// Derived state (OutputKey) should be reconstructed from replayed events.
	if v, ok := s2.State().Get("answer"); !ok || v != "final after echo:hi" {
		t.Fatalf("recovered state[answer] = %v (ok=%v)", v, ok)
	}

	// --- continuing the recovered session appends, not overwrites ---
	r2 := runner.New(runner.Config{AppName: "app", Root: ag, Store: store2})
	for _, err := range r2.Run(context.Background(), "user", "sess", core.UserText("again")) {
		if err != nil {
			t.Fatal(err)
		}
	}
	s3, _ := store2.GetOrCreate(context.Background(), "app", "user", "sess")
	if got := len(s3.Messages()); got <= 4 {
		t.Fatalf("expected history to grow past 4, got %d", got)
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

// TestFileStorePersistsParentChain verifies ParentID edges are written to disk
// and recovered, so a reloaded session reconstructs the same active path with
// each event linked to its predecessor.
func TestFileStorePersistsParentChain(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store1, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := store1.GetOrCreate(ctx, "app", "u", "sess")
	if err != nil {
		t.Fatal(err)
	}

	m1, m2 := core.UserText("one"), core.AssistantText("two")
	for _, m := range []*core.Message{&m1, &m2} {
		if err := store1.Append(ctx, s1, &core.Event{Author: "x", Message: m}); err != nil {
			t.Fatal(err)
		}
	}
	id1, id2 := s1.Events()[0].ID, s1.Events()[1].ID
	if id1 == "" || id2 == "" {
		t.Fatal("events missing IDs")
	}

	// Reload from a fresh store and check the chain reconstructs identically.
	store2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store2.GetOrCreate(ctx, "app", "u", "sess")
	if err != nil {
		t.Fatal(err)
	}
	ev := s2.Events()
	if len(ev) != 2 {
		t.Fatalf("recovered %d events, want 2", len(ev))
	}
	if ev[0].ParentID != "" {
		t.Fatalf("root ParentID = %q, want empty", ev[0].ParentID)
	}
	if ev[1].ParentID != ev[0].ID {
		t.Fatalf("second ParentID = %q, want %q", ev[1].ParentID, ev[0].ID)
	}
	if s2.Leaf() != ev[1].ID {
		t.Fatalf("recovered leaf = %q, want %q", s2.Leaf(), ev[1].ID)
	}
}
