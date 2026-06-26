package memx

import (
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Budgeted wraps a Section so its rendered output is capped at maxRunes
// (approximate token budget — one rune ≈ one CJK token). When the inner section
// renders longer, it is truncated at a line boundary and marked. maxRunes <= 0
// disables truncation. Rules and project memory are intentionally NOT budgeted
// (they are hard context); working memory and the text-memory index are. See
// ADR 0016.
func Budgeted(inner prompt.Section, maxRunes int) prompt.Section {
	return prompt.SectionFunc{
		SecName:  inner.Name(),
		SecOrder: inner.Order(),
		RenderFn: func(c prompt.Context) (string, error) {
			out, err := inner.Render(c)
			if err != nil || maxRunes <= 0 {
				return out, err
			}
			r := []rune(out)
			if len(r) <= maxRunes {
				return out, nil
			}
			truncated := string(r[:maxRunes])
			if i := strings.LastIndexByte(truncated, '\n'); i > 0 {
				truncated = truncated[:i]
			}
			return truncated + "\n…（已按预算截断）", nil
		},
	}
}
