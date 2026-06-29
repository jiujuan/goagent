package skills

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// orderSkills places the Active Skills block between the built-in tool guidance
// (300) and session state (400) sections.
const orderSkills = 350

// PromptSection returns the Level-1 prompt block: it lists every loaded skill's
// name and description so the model knows what capabilities exist, and tells it
// to call use_skill to load a skill's full instructions on demand. It renders
// empty (and is dropped by the Builder) when the library holds no skills.
func PromptSection(lib *Library) prompt.Section {
	return prompt.SectionFunc{
		SecName:  "skills",
		SecOrder: orderSkills,
		RenderFn: func(prompt.Context) (string, error) {
			if lib == nil || lib.Len() == 0 {
				return "", nil
			}
			var b strings.Builder
			b.WriteString("# Active Skills\n")
			b.WriteString("These skills are available. When one applies, call the use_skill tool with its name to read its full instructions before acting.\n")
			for _, s := range lib.List() {
				fmt.Fprintf(&b, "- %s: %s\n", s.Name, s.Description)
			}
			return b.String(), nil
		},
	}
}
