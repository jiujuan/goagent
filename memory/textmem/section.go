package textmem

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Order places the text-memory index after session state, near the end of the
// system prompt: it is reference material the model consults, not a constraint.
// See ADR 0016.
const Order = 450

// IndexSection injects the memory index (one line per entry: "- name — desc")
// so the model knows what curated memories exist and can read_memory for the
// full text. It renders empty when the store has no entries.
func IndexSection(store Store) prompt.Section {
	return prompt.SectionFunc{
		SecName:  "text_memory_index",
		SecOrder: Order,
		RenderFn: func(c prompt.Context) (string, error) {
			entries, err := store.Index(c)
			if err != nil {
				return "", fmt.Errorf("textmem: index: %w", err)
			}
			if len(entries) == 0 {
				return "", nil
			}
			var b strings.Builder
			b.WriteString("# 长期记忆索引\n以下是已存的长期记忆条目，需要详情时用 read_memory 按 name 读取：\n")
			for _, e := range entries {
				fmt.Fprintf(&b, "- %s — %s\n", e.Name, e.Desc)
			}
			return strings.TrimRight(b.String(), "\n"), nil
		},
	}
}
