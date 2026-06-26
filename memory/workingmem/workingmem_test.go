package workingmem

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

// applyUpdate calls UpdateTool with the given JSON args and commits the
// resulting StateDelta as an event, mirroring what the turn engine does.
func applyUpdate(t *testing.T, store session.Store, s *session.Session, args string) {
	t.Helper()
	ctx := context.Background()
	actions := &core.Actions{}
	tctx := &tool.Context{Context: ctx, State: s.State(), Actions: actions}
	if _, err := UpdateTool().Call(tctx, []byte(args)); err != nil {
		t.Fatalf("tool call: %v", err)
	}
	if err := store.Append(ctx, s, &core.Event{Actions: *actions}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func TestUpdateToolPersistsViaStateDelta(t *testing.T) {
	ctx := context.Background()
	store := session.InMemory()
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")

	applyUpdate(t, store, s, `{"goal":"写测试","add_todo":"覆盖 section"}`)
	applyUpdate(t, store, s, `{"note_key":"db","note_val":"postgres"}`)

	wm := For(s)
	if got := wm.Goal(); got != "写测试" {
		t.Errorf("goal = %q, want 写测试", got)
	}
	if got := wm.Todos(); len(got) != 1 || got[0].Text != "覆盖 section" || got[0].Done {
		t.Errorf("todos = %+v", got)
	}
	if got := wm.Notes()["db"]; got != "postgres" {
		t.Errorf("note db = %q, want postgres", got)
	}
}

func TestResolveTodo(t *testing.T) {
	ctx := context.Background()
	store := session.InMemory()
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")

	applyUpdate(t, store, s, `{"add_todo":"任务A"}`)
	id := For(s).Todos()[0].ID
	applyUpdate(t, store, s, `{"resolve_todo_id":"`+id+`"}`)

	if td := For(s).Todos(); len(td) != 1 || !td[0].Done {
		t.Errorf("todo not resolved: %+v", td)
	}
}

func TestSectionOmitsWhenEmpty(t *testing.T) {
	store := session.InMemory()
	s, _ := store.GetOrCreate(context.Background(), "app", "u", "sess")

	out, err := Section().Render(prompt.Context{Context: context.Background(), Session: s})
	if err != nil {
		t.Fatal(err)
	}
	if out != "" {
		t.Errorf("empty working memory should render empty, got %q", out)
	}
}

func TestSectionRenders(t *testing.T) {
	ctx := context.Background()
	store := session.InMemory()
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")
	applyUpdate(t, store, s, `{"goal":"实现记忆系统","add_todo":"工作记忆"}`)

	out, err := Section().Render(prompt.Context{Context: ctx, Session: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# 当前工作记忆", "目标：实现记忆系统", "工作记忆"} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q in:\n%s", want, out)
		}
	}
}

// TestPersistsAcrossFileStoreReload verifies the snapshot survives a JSONL
// round-trip: the encoded string is stable where a raw []Todo would reload as
// []any of map[string]any.
func TestPersistsAcrossFileStoreReload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")
	applyUpdate(t, store, s, `{"goal":"持久化","add_todo":"重载验证"}`)

	// Reload from disk with a fresh store.
	store2, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := store2.GetOrCreate(ctx, "app", "u", "sess")

	wm := For(s2)
	if wm.Goal() != "持久化" {
		t.Errorf("goal after reload = %q", wm.Goal())
	}
	if td := wm.Todos(); len(td) != 1 || td[0].Text != "重载验证" {
		t.Errorf("todos after reload = %+v", td)
	}
}
