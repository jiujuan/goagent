package rules

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Order is the smallest among memory sections: hard constraints render first,
// ahead of identity and everything else. See ADR 0016.
const Order = 50

// Section renders the merged rules as the leading system-prompt block. It
// renders empty when the set has no rules.
func (s *Set) Section() prompt.Section {
	return prompt.SectionFunc{
		SecName:  "rules",
		SecOrder: Order,
		RenderFn: func(prompt.Context) (string, error) {
			if s == nil || len(s.rules) == 0 {
				return "", nil
			}
			var b strings.Builder
			b.WriteString("# 规则（必须遵守）\n")
			for _, r := range s.rules {
				fmt.Fprintf(&b, "- (%s) %s\n", r.Source, r.Text)
			}
			return strings.TrimRight(b.String(), "\n"), nil
		},
	}
}
