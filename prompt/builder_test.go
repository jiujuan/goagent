package prompt

import (
	"errors"
	"strings"
	"testing"
)

// fixed returns a Section with the given name/order rendering a fixed string.
func fixed(name string, order int, text string) Section {
	return SectionFunc{SecName: name, SecOrder: order, RenderFn: func(Context) (string, error) {
		return text, nil
	}}
}

func TestBuildSortsByOrder(t *testing.T) {
	out, err := New().
		Add(fixed("c", 300, "third")).
		Add(fixed("a", 100, "first")).
		Add(fixed("b", 200, "second")).
		Build(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if out != "first\n\nsecond\n\nthird" {
		t.Fatalf("unexpected order/join: %q", out)
	}
}

func TestBuildDropsEmpty(t *testing.T) {
	out, err := New().
		Add(fixed("a", 100, "keep")).
		Add(fixed("b", 200, "   ")). // whitespace-only -> dropped
		Add(fixed("c", 300, "")).    // empty -> dropped
		Add(fixed("d", 400, "tail")).
		Build(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if out != "keep\n\ntail" {
		t.Fatalf("empties not dropped: %q", out)
	}
}

func TestAddOverridesByName(t *testing.T) {
	b := New().
		Add(fixed("identity", 100, "old")).
		Add(fixed("identity", 100, "new"))
	out, err := b.Build(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if out != "new" {
		t.Fatalf("override by name failed: %q", out)
	}
	if strings.Contains(out, "old") {
		t.Fatalf("stale section retained: %q", out)
	}
}

func TestRemove(t *testing.T) {
	out, err := New().
		Add(fixed("a", 100, "first")).
		Add(fixed("b", 200, "second")).
		Remove("a").
		Build(Context{})
	if err != nil {
		t.Fatal(err)
	}
	if out != "second" {
		t.Fatalf("remove failed: %q", out)
	}
}

func TestBuildPropagatesError(t *testing.T) {
	want := errors.New("boom")
	_, err := New().
		Add(SectionFunc{SecName: "bad", SecOrder: 100, RenderFn: func(Context) (string, error) {
			return "", want
		}}).
		Build(Context{})
	if !errors.Is(err, want) {
		t.Fatalf("error not propagated: %v", err)
	}
}
