package projectmem

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Order places project memory just after identity and before environment, so
// repo conventions frame the whole prompt. It sits below rules (the hard
// constraints) per ADR 0016.
const Order = 150

// Section renders the discovered AGENTS.md documents into one block, root-first,
// each labeled with its source path. It renders empty when no documents were
// found. Pass the slice returned by Load.
func Section(docs []Doc) prompt.Section {
	return prompt.SectionFunc{
		SecName:  "project_memory",
		SecOrder: Order,
		RenderFn: func(prompt.Context) (string, error) {
			if len(docs) == 0 {
				return "", nil
			}
			var b strings.Builder
			b.WriteString("# 项目记忆（AGENTS.md）\n")
			for i, d := range docs {
				if i > 0 {
					b.WriteString("\n")
				}
				fmt.Fprintf(&b, "<!-- %s -->\n%s\n", d.Path, d.Content)
			}
			return strings.TrimRight(b.String(), "\n"), nil
		},
	}
}
