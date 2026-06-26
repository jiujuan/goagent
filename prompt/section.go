// Package prompt builds an agent's system prompt from ordered, composable
// Sections. Each Section renders an independent block (identity, environment,
// tool guidance, ...); the Builder sorts them by Order, renders each against a
// Context, drops the empties, and joins the rest.
//
// The package depends only on core/session/tool, never on agent: the agent
// fills a prompt.Context DTO from its InvocationContext, so Sections stay
// unit-testable in isolation and no import cycle forms.
package prompt

// Section is the extension point: one self-contained block of the system
// prompt. Implementations should be cheap and deterministic given a Context.
type Section interface {
	// Name uniquely identifies the section; the Builder uses it to override or
	// remove a section by name.
	Name() string
	// Order sets render position, ascending. Built-ins are spaced 100 apart so
	// custom sections can slot between them.
	Order() int
	// Render produces the section's text. Returning "" (no error) omits the
	// section entirely from the joined prompt.
	Render(Context) (string, error)
}

// SectionFunc adapts a render function into a Section for one-off sections
// without declaring a new type.
type SectionFunc struct {
	SecName  string
	SecOrder int
	RenderFn func(Context) (string, error)
}

func (s SectionFunc) Name() string                    { return s.SecName }
func (s SectionFunc) Order() int                       { return s.SecOrder }
func (s SectionFunc) Render(c Context) (string, error) { return s.RenderFn(c) }

var _ Section = SectionFunc{}
