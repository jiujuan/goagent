package workingmem

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

// applyUpdate calls UpdateTool and applies its StateOps to st, mirroring what
// the agent loop does after a tool returns.
func applyUpdate(t *testing.T, st *core.State, args string) {
	t.Helper()
	res, err := UpdateTool().Call(&tool.Context{Context: context.Background(), State: st}, []byte(args))
	if err != nil {
		t.Fatal(err)
	}
	st.Apply(res.State...)
}

func TestUpdateToolGoalTodosNotes(t *testing.T) {
	st := &core.State{}
	applyUpdate(t, st, `{"goal":"ship v2"}`)
	applyUpdate(t, st, `{"add_todo":"write tests"}`)
	applyUpdate(t, st, `{"note_key":"db","note_val":"postgres"}`)

	wm := For(st)
	if wm.Goal() != "ship v2" {
		t.Fatalf("goal = %q", wm.Goal())
	}
	if td := wm.Todos(); len(td) != 1 || td[0].Text != "write tests" {
		t.Fatalf("todos = %+v", td)
	}
	if wm.Notes()["db"] != "postgres" {
		t.Fatalf("notes = %+v", wm.Notes())
	}

	// Resolve the todo by id.
	id := wm.Todos()[0].ID
	applyUpdate(t, st, `{"resolve_todo_id":"`+id+`"}`)
	if td := For(st).Todos(); len(td) != 1 || !td[0].Done {
		t.Fatalf("todo not resolved: %+v", td)
	}
}

func TestSectionRendersAndOmitsWhenEmpty(t *testing.T) {
	ctx := context.Background()
	out, err := Section().Render(prompt.Context{Context: ctx, State: &core.State{}})
	if err != nil {
		t.Fatal(err)
	}
	if out != "" {
		t.Fatalf("empty working memory should render empty, got %q", out)
	}

	st := &core.State{}
	applyUpdate(t, st, `{"goal":"build the thing"}`)
	out, err = Section().Render(prompt.Context{Context: ctx, State: st})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "build the thing") {
		t.Fatalf("section missing goal: %q", out)
	}
}

func TestSnapshotSurvivesJSONRoundTrip(t *testing.T) {
	// Stored as a JSON string in KV, the snapshot survives a checkpoint's
	// marshal/unmarshal (the reason it is encoded as one string).
	st := &core.State{}
	applyUpdate(t, st, `{"goal":"g","add_todo":"t"}`)
	raw, _ := st.KV[stateKey].(string)
	var snap Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Goal != "g" || len(snap.Todos) != 1 {
		t.Fatalf("round-trip lost data: %+v", snap)
	}
}
