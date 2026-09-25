package workspace

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/prompt"
)

// Order places the workspace facts block after the environment it is grounded
// in (200) and before tool guidance (300), so the model reads where it is
// before reading what it can do there. See ADR 0016 for the ordering scheme.
const Order = 210

// Section renders the workspace facts block. It is exported so a caller can
// place it alone; Sections returns it with the memory blocks.
func (w *Workspace) Section() prompt.Section {
	return prompt.SectionFunc{
		SecName:  "workspace",
		SecOrder: Order,
		RenderFn: func(prompt.Context) (string, error) {
			var b strings.Builder
			b.WriteString("# Workspace\n")
			fmt.Fprintf(&b, "Root: %s%s\n", w.root, w.gitInfo.describe(w.root))
			b.WriteString("File tools (read_file/write_file/list_dir/glob) are confined to this root.\n")
			b.WriteString("Commands run with this root as their working directory, but are not jailed: a command may read or write paths outside the root.")
			return b.String(), nil
		},
	}
}
