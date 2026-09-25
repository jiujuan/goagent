package skills

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// ActiveKey is the State.KV slot listing the skills a run has loaded through
// use_skill. It lives in State rather than in the middleware because State is
// what the checkpointer snapshots: the pre-approval list has to survive
// resume, branching and hand-off to a sub-agent, none of which preserve
// in-process memory. See ADR 0022.
const ActiveKey = "skills.active"

// alwaysPreapproved are usable-without-asking regardless of any skill's list:
// a tool that loads skills and a tool that runs a loaded skill's own scripts.
// Gating them would make the first use_skill call unapprovable.
var alwaysPreapproved = []string{"use_skill", "run_skill_script"}

// Active returns the skill names loaded so far, in activation order.
func Active(st *core.State) []string {
	if st == nil || st.KV == nil {
		return nil
	}
	raw, ok := st.KV[ActiveKey].(string)
	if !ok {
		return nil
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil
	}
	return names
}

// Preapproved returns the tool names covered by the active skills: the union of
// their allowed-tools declarations plus the skill tools themselves. A skill
// with no declared list contributes nothing, which is why a run whose skills
// declare nothing leaves the gate open (see Gate).
func Preapproved(lib *Library, active []string) map[string]bool {
	set := make(map[string]bool, len(alwaysPreapproved)+len(active))
	for _, name := range alwaysPreapproved {
		set[name] = true
	}
	if lib == nil {
		return set
	}
	for _, name := range active {
		s, ok := lib.Get(name)
		if !ok {
			continue
		}
		for _, t := range s.AllowedTools {
			set[t] = true
		}
	}
	return set
}

// Gate returns middleware that turns the frontmatter declarations into a
// pre-approval list: a tool call an active skill did not claim pauses for human
// approval instead of running silently.
//
// It deliberately does nothing until a skill with a declared tool list is
// active, so mounting it cannot interrupt a run that never used the field. It
// is orthogonal to middleware.Permission, which decides a tool may never run:
// both hooks are consulted per call and their directives folded by precedence,
// where Interrupt outranks Stop, so a call that is both denied and unlisted
// pauses for a human rather than ending the run.
func Gate(lib *Library) agent.Middleware {
	return &skillGate{lib: lib}
}

type skillGate struct {
	agent.BaseMiddleware
	lib *Library
}

func (g *skillGate) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	active := Active(lc.State)
	if !anyDeclaresTools(g.lib, active) {
		return core.Directive{}, nil
	}
	if Preapproved(g.lib, active)[c.Name] {
		return core.Directive{}, nil
	}
	return core.Directive{
		Kind: core.Interrupt,
		Reason: fmt.Sprintf("active skills (%s) do not list tool %q in their allowed-tools; approve to run it anyway",
			strings.Join(active, ", "), c.Name),
	}, nil
}

// anyDeclaresTools reports whether at least one active skill states a tool list.
// Undeclared and "declared empty" are treated alike on purpose: neither is a
// grant, so neither can pre-approve, but blocking everything because no skill
// bothered to list tools would break runs that never opted in.
func anyDeclaresTools(lib *Library, active []string) bool {
	if lib == nil {
		return false
	}
	for _, name := range active {
		if s, ok := lib.Get(name); ok && len(s.AllowedTools) > 0 {
			return true
		}
	}
	return false
}

// activate records name as active, returning the state op that commits it. No
// op is returned when the skill is already active or the run has no State to
// write to.
func activate(st *core.State, name string) []core.StateOp {
	if st == nil {
		return nil
	}
	active := Active(st)
	for _, n := range active {
		if n == name {
			return nil
		}
	}
	raw, err := json.Marshal(append(append(make([]string, 0, len(active)+1), active...), name))
	if err != nil {
		return nil
	}
	return []core.StateOp{{Kind: core.OpSetKV, Key: ActiveKey, Value: string(raw)}}
}
