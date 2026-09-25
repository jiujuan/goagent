package skills

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// useArgs is the model-facing input schema for the use_skill tool.
type useArgs struct {
	Name     string `json:"name" desc:"name of the skill to load (as listed under Active Skills)"`
	Resource string `json:"resource,omitempty" desc:"optional file inside the skill to read instead of SKILL.md, e.g. \"forms.md\" or \"scripts/run.sh\""`
}

// Tool builds the use_skill tool that performs on-demand loading (Level 2 and
// Level 3). Called with just a name, it returns that skill's SKILL.md body
// prefixed with its allowed-tools line; called with a resource, it returns that
// bundled file's contents. Unknown skills and out-of-bounds paths come back as
// tool errors (data the model can recover from), not Go errors.
//
// Loading a skill also records it as active in State.KV, which is what the Gate
// middleware reads: a skill's allowed-tools only pre-approves tools once the
// skill has actually been loaded. The tool is written by hand rather than via
// tool.New because only a Result can carry that state op.
func Tool(lib *Library) tool.Tool { return &useSkillTool{lib: lib} }

type useSkillTool struct{ lib *Library }

func (*useSkillTool) Name() string { return "use_skill" }

func (*useSkillTool) Description() string {
	return "Load a skill's full instructions on demand. Pass `name` to read its SKILL.md, or also `resource` to read a bundled file (e.g. a template or script). Run scripts via run_command."
}

func (*useSkillTool) Schema() json.RawMessage { return tool.SchemaFor[useArgs]() }

func (t *useSkillTool) Call(ctx *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in useArgs
	if len(args) > 0 {
		if err := json.Unmarshal(args, &in); err != nil {
			return tool.ErrorResult("invalid arguments: " + err.Error()), nil
		}
	}
	name := strings.TrimSpace(in.Name)
	s, ok := t.lib.Get(name)
	if !ok {
		return tool.ErrorResult(fmt.Sprintf("unknown skill %q; call use_skill with one of the names listed under Active Skills", name)), nil
	}

	text := ""
	if res := strings.TrimSpace(in.Resource); res != "" {
		data, err := s.Resource(res)
		if err != nil {
			return tool.ErrorResult(fmt.Sprintf("read resource %q from skill %q: %v", res, name, err)), nil
		}
		text = string(data)
	} else {
		body, err := s.Instructions()
		if err != nil {
			return tool.ErrorResult(fmt.Sprintf("read skill %q: %v", name, err)), nil
		}
		text = renderInstructions(s, body)
	}

	return &tool.Result{
		Content: []core.Part{core.Text{Text: text}},
		State:   activate(ctx.State, s.Name),
	}, nil
}

// renderInstructions formats a skill's loaded body with a header and the
// allowed-tools line, so the model sees which tools the skill expects to use
// before following its steps. The wording says what the list is for: tools
// outside it reach a human, under skills.Gate.
func renderInstructions(s *Skill, body string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Skill: %s\n", s.Name)
	if s.Description != "" {
		fmt.Fprintf(&b, "%s\n", s.Description)
	}
	if len(s.AllowedTools) > 0 {
		fmt.Fprintf(&b, "Allowed tools (pre-approved; other tools will ask for human approval): %s\n",
			strings.Join(s.AllowedTools, ", "))
	}
	b.WriteString("\n")
	b.WriteString(body)
	return strings.TrimRight(b.String(), "\n")
}
