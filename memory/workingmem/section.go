package workingmem

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Order places the working-memory section after identity/environment but before
// the broader session-state dump, so the model sees its current task front and
// center. See ADR 0016 for the overall ordering.
const Order = 350

// Section renders the working memory (goal, todos, notes) into the system
// prompt. It renders empty when the scratchpad is empty, so the Builder omits
// it. It mirrors prompt.SessionState in reading from c.Session.State().
func Section() prompt.Section {
	return prompt.SectionFunc{
		SecName:  "working_memory",
		SecOrder: Order,
		RenderFn: func(c prompt.Context) (string, error) {
			if c.State == nil {
				return "", nil
			}
			snap := readSnapshot(c.State)
			if snap.Empty() {
				return "", nil
			}

			var b strings.Builder
			b.WriteString("# 当前工作记忆\n")
			if snap.Goal != "" {
				fmt.Fprintf(&b, "目标：%s\n", snap.Goal)
			}
			if len(snap.Todos) > 0 {
				b.WriteString("待办：\n")
				for _, t := range snap.Todos {
					mark := " "
					if t.Done {
						mark = "x"
					}
					fmt.Fprintf(&b, "- [%s] (%s) %s\n", mark, t.ID, t.Text)
				}
			}
			if len(snap.Notes) > 0 {
				b.WriteString("关键事实：\n")
				for _, k := range slices.Sorted(maps.Keys(snap.Notes)) {
					fmt.Fprintf(&b, "- %s：%s\n", k, snap.Notes[k])
				}
			}
			return strings.TrimRight(b.String(), "\n"), nil
		},
	}
}
