package prompt

import (
	"slices"
	"strings"
)

// Builder is an ordered, override-by-name collection of Sections. The zero
// value is not usable; construct one with New.
type Builder struct {
	sections []Section
}

// New returns an empty Builder.
func New() *Builder { return &Builder{} }

// Add appends a Section. If a section with the same Name is already present it
// is replaced in place (keeping its position), so built-ins can be reconfigured
// by adding a same-named section. Returns the Builder for chaining.
func (b *Builder) Add(s Section) *Builder {
	for i, existing := range b.sections {
		if existing.Name() == s.Name() {
			b.sections[i] = s
			return b
		}
	}
	b.sections = append(b.sections, s)
	return b
}

// Remove drops the section with the given name, if present. Returns the Builder
// for chaining.
func (b *Builder) Remove(name string) *Builder {
	b.sections = slices.DeleteFunc(b.sections, func(s Section) bool {
		return s.Name() == name
	})
	return b
}

// Build renders every section against ctx in ascending Order, drops the ones
// that render empty, and joins the rest with a blank line. The first section
// that errors aborts the build.
func (b *Builder) Build(ctx Context) (string, error) {
	ordered := slices.Clone(b.sections)
	slices.SortStableFunc(ordered, func(a, c Section) int {
		return a.Order() - c.Order()
	})

	var blocks []string
	for _, s := range ordered {
		text, err := s.Render(ctx)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		blocks = append(blocks, strings.TrimRight(text, "\n"))
	}
	return strings.Join(blocks, "\n\n"), nil
}
