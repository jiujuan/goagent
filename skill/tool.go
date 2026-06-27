package skill

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/tool"
)

// useArgs is the model-facing input schema for the use_skill tool.
type useArgs struct {
	Name     string `json:"name" desc:"name of the skill to load (as listed under Active Skills)"`
	Resource string `json:"resource,omitempty" desc:"optional file inside the skill to read instead of SKILL.md, e.g. \"forms.md\" or \"scripts/run.sh\""`
}

// Tool builds the use_skill tool that performs on-demand loading (Level 2 and
// Level 3). Called with just a name, it returns that skill's SKILL.md body
// prefixed with its advisory allowed-tools line; called with a resource, it
// returns that bundled file's contents. Unknown skills and out-of-bounds paths
// come back as tool errors (data the model can recover from), not Go errors.
func Tool(lib *Library) tool.Tool {
	return tool.New("use_skill",
		"Load a skill's full instructions on demand. Pass `name` to read its SKILL.md, or also `resource` to read a bundled file (e.g. a template or script). Run scripts via run_command.",
		func(_ *tool.Context, in useArgs) (string, error) {
			name := strings.TrimSpace(in.Name)
			s, ok := lib.Get(name)
			if !ok {
				return "", fmt.Errorf("unknown skill %q; call use_skill with one of the names listed under Active Skills", name)
			}

			if res := strings.TrimSpace(in.Resource); res != "" {
				data, err := s.Resource(res)
				if err != nil {
					return "", fmt.Errorf("read resource %q from skill %q: %w", res, name, err)
				}
				return string(data), nil
			}

			body, err := s.Instructions()
			if err != nil {
				return "", fmt.Errorf("read skill %q: %w", name, err)
			}
			return renderInstructions(s, body), nil
		})
}

// renderInstructions formats a skill's loaded body with a header and the
// advisory allowed-tools line, so the model sees which tools the skill expects
// to use before following its steps.
func renderInstructions(s *Skill, body string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Skill: %s\n", s.Name)
	if s.Description != "" {
		fmt.Fprintf(&b, "%s\n", s.Description)
	}
	if len(s.AllowedTools) > 0 {
		fmt.Fprintf(&b, "Allowed tools: %s\n", strings.Join(s.AllowedTools, ", "))
	}
	b.WriteString("\n")
	b.WriteString(body)
	return strings.TrimRight(b.String(), "\n")
}
