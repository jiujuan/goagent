package skills

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/middleware"
)

// gateLib holds one skill that declares a tool list and one that does not, which
// is the distinction the gate turns on.
func gateLib(t *testing.T) *Library {
	t.Helper()
	return mapLib(t, map[string]string{
		"narrow/SKILL.md": "---\nname: narrow\ndescription: reads files only\nallowed-tools: [read_file]\n---\nbody\n",
		"silent/SKILL.md": "---\nname: silent\ndescription: declares no tools\n---\nbody\n",
	})
}

// activeState builds a run State as use_skill would leave it.
func activeState(t *testing.T, names ...string) *core.State {
	t.Helper()
	raw, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	return &core.State{KV: map[string]any{ActiveKey: string(raw)}}
}

func beforeTool(t *testing.T, mw agent.Middleware, st *core.State, call string) core.Directive {
	t.Helper()
	lc := &agent.LoopContext{RunContext: &agent.RunContext{State: st}}
	d, err := mw.BeforeTool(lc, &core.ToolCall{Name: call})
	if err != nil {
		t.Fatalf("BeforeTool(%s): %v", call, err)
	}
	return d
}

// The four verdicts, in the order the gate considers them.
func TestGateDecisions(t *testing.T) {
	lib := gateLib(t)
	gate := Gate(lib)

	cases := []struct {
		name   string
		st     *core.State
		call   string
		wantIn bool // true when the call must interrupt
	}{
		{"nothing active yet", &core.State{}, "write_file", false},
		{"active skill declares nothing", activeState(t, "silent"), "write_file", false},
		{"declared tool", activeState(t, "narrow"), "read_file", false},
		{"skill tools are always usable", activeState(t, "narrow"), "use_skill", false},
		{"script runner is always usable", activeState(t, "narrow"), "run_skill_script", false},
		{"undeclared tool", activeState(t, "narrow"), "write_file", true},
		{"mixed list, one grants", activeState(t, "silent", "narrow"), "read_file", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := beforeTool(t, gate, tc.st, tc.call)
			if got := d.Kind == core.Interrupt; got != tc.wantIn {
				t.Fatalf("directive for %q = %+v, interrupting=%v, want interrupting=%v", tc.call, d, got, tc.wantIn)
			}
			if tc.wantIn && !strings.Contains(d.Reason, tc.call) {
				t.Errorf("interrupt reason should name the tool: %q", d.Reason)
			}
		})
	}
}

// An unknown active skill name (a skill dropped from the library since) grants
// nothing rather than throwing the gate wide open.
func TestGateUnknownActiveSkill(t *testing.T) {
	d := beforeTool(t, Gate(gateLib(t)), activeState(t, "ghost"), "read_file")
	if d.Kind != core.Continue {
		t.Errorf("directive = %+v, want pass-through: no known skill declared anything", d)
	}
}

// A library with no skills at all must not pause anything.
func TestGateEmptyLibrary(t *testing.T) {
	lib, err := LoadDirs("testdata/does-not-exist")
	if err != nil {
		t.Fatalf("LoadDirs on a missing directory: %v", err)
	}
	if lib.Len() != 0 {
		t.Fatalf("library = %d skills, want 0", lib.Len())
	}
	if d := beforeTool(t, Gate(lib), activeState(t, "narrow"), "write_file"); d.Kind != core.Continue {
		t.Errorf("directive = %+v, want pass-through", d)
	}
}

// The gate pauses rather than refusing, even against a policy that denies the
// same tool: Interrupt outranks Stop, so a human gets the deciding call.
func TestGateInterruptOutranksPermissionStop(t *testing.T) {
	stack := agent.NewStack(
		middleware.Permission(middleware.DenyFor("write_file")),
		Gate(gateLib(t)),
	)
	d := beforeTool(t, stack, activeState(t, "narrow"), "write_file")
	if d.Kind != core.Interrupt {
		t.Errorf("folded directive = %+v, want Interrupt", d)
	}
}

// The activation list lives in State.KV, so it is whatever the checkpointer
// snapshots: after a JSON round-trip the pre-approved list still stands.
func TestActiveSurvivesCheckpointRoundTrip(t *testing.T) {
	original := activeState(t, "narrow")
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var resumed core.State
	if err := json.Unmarshal(raw, &resumed); err != nil {
		t.Fatal(err)
	}

	got := Active(&resumed)
	if len(got) != 1 || got[0] != "narrow" {
		t.Fatalf("Active() after round-trip = %v, want [narrow]", got)
	}
	if d := beforeTool(t, Gate(gateLib(t)), &resumed, "read_file"); d.Kind != core.Continue {
		t.Errorf("resumed run lost the grant: %+v", d)
	}
	if d := beforeTool(t, Gate(gateLib(t)), &resumed, "write_file"); d.Kind != core.Interrupt {
		t.Errorf("resumed run lost the restriction: %+v", d)
	}
}

func TestPreapproved(t *testing.T) {
	lib := gateLib(t)
	set := Preapproved(lib, []string{"narrow", "silent", "ghost"})
	for _, name := range []string{"read_file", "use_skill", "run_skill_script"} {
		if !set[name] {
			t.Errorf("%s should be pre-approved", name)
		}
	}
	if set["write_file"] {
		t.Error("write_file is in no skill's allowed-tools")
	}
}
