package agent

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

type viewTestTool struct {
	name   string
	desc   string
	schema string
	seq    bool
}

func (t *viewTestTool) Name() string            { return t.name }
func (t *viewTestTool) Description() string     { return t.desc }
func (t *viewTestTool) Schema() json.RawMessage { return json.RawMessage(t.schema) }
func (t *viewTestTool) Call(*tool.Context, json.RawMessage) (*tool.Result, error) {
	return tool.TextResult("ok"), nil
}
func (t *viewTestTool) SequentialExecution() bool { return t.seq }

func toolTableNames(schemas []llm.ToolSchema) []string {
	out := make([]string, len(schemas))
	for i, s := range schemas {
		out[i] = s.Name
	}
	return out
}

// TestSnapshotToolsOrderAndOverride pins the final tool table: static order,
// injected tools replacing by name, injected-only names sorted after.
func TestSnapshotToolsOrderAndOverride(t *testing.T) {
	a := &viewTestTool{name: "a", desc: "A", schema: `{"type":"object","properties":{}}`}
	b := &viewTestTool{name: "b", desc: "B", schema: `{"type":"object","properties":{}}`}
	injectedB := &viewTestTool{
		name:   "b",
		desc:   "B-injected",
		schema: `{"type":"object","properties":{"x":{"type":"string"}}}`,
		seq:    true,
	}
	c := &viewTestTool{name: "c", desc: "C", schema: `{"type":"object","properties":{}}`}

	l := newLoop(config{tools: []tool.Tool{a, b}})
	rc := &RunContext{}
	rc.dynamic.set(injectedB)
	rc.dynamic.set(c)

	table := l.snapshotTools(rc)
	if got := toolTableNames(table.schemas); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("advertised order = %v, want [a b c]", got)
	}
	if table.schemas[1].Description != "B-injected" {
		t.Fatalf("b description = %q, want the injected schema", table.schemas[1].Description)
	}
	if table.byName["b"].Description() != "B-injected" {
		t.Fatalf("b implementation = %q, want the injected one", table.byName["b"].Description())
	}
	if !table.sequential["b"] {
		t.Fatal("b must inherit the injected implementation's sequential flag")
	}
	if table.sequential["a"] || table.sequential["c"] {
		t.Fatal("only the injected replacement is sequential")
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, ok := table.known[name]; !ok {
			t.Fatalf("known table is missing %q", name)
		}
	}
}

// TestSnapshotToolsDeduplicatesStaticNames: duplicates keep the first slot and
// the last implementation.
func TestSnapshotToolsDeduplicatesStaticNames(t *testing.T) {
	first := &viewTestTool{name: "dup", desc: "first", schema: `{"type":"object"}`}
	last := &viewTestTool{name: "dup", desc: "last", schema: `{"type":"object","properties":{}}`}
	unique := &viewTestTool{name: "unique", desc: "U", schema: `{"type":"object","properties":{}}`}

	l := newLoop(config{tools: []tool.Tool{first, last, unique}})
	table := l.snapshotTools(&RunContext{})

	if got := toolTableNames(table.schemas); !slices.Equal(got, []string{"dup", "unique"}) {
		t.Fatalf("advertised names = %v, want one dup slot", got)
	}
	if table.schemas[0].Description != "last" {
		t.Fatalf("dup description = %q, want the last registration", table.schemas[0].Description)
	}
	if table.byName["dup"].Description() != "last" {
		t.Fatalf("dup implementation = %q, want the last registration", table.byName["dup"].Description())
	}
	if string(table.schemas[1].Parameters) != string(l.schemas[2].Parameters) {
		t.Fatal("a unique static tool must reuse its construction-time schema")
	}
}

// TestToolDigestOrderAndContent pins the digest inputs: order, description and
// parameters all matter.
func TestToolDigestOrderAndContent(t *testing.T) {
	a := llm.ToolSchema{Name: "a", Description: "A", Parameters: json.RawMessage(`{"type":"object"}`)}
	b := llm.ToolSchema{Name: "b", Description: "B", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}

	base := toolDigest([]llm.ToolSchema{a, b})
	if base == "" {
		t.Fatal("digest is empty")
	}
	if got := toolDigest([]llm.ToolSchema{a, b}); got != base {
		t.Fatal("same order and content must produce the same digest")
	}
	if got := toolDigest([]llm.ToolSchema{b, a}); got == base {
		t.Fatal("tool order must change the digest")
	}
	changedDesc := a
	changedDesc.Description = "A2"
	if got := toolDigest([]llm.ToolSchema{changedDesc, b}); got == base {
		t.Fatal("description must change the digest")
	}
	changedParams := a
	changedParams.Parameters = json.RawMessage(`{"type":"object","properties":{"x":{"type":"string"}}}`)
	if got := toolDigest([]llm.ToolSchema{changedParams, b}); got == base {
		t.Fatal("parameters must change the digest")
	}
	if toolDigest(nil) != toolDigest([]llm.ToolSchema{}) {
		t.Fatal("no tools and an empty list must digest the same")
	}
}

// TestSettingsDigestExcludesMetadata: Metadata is provider passthrough, not part
// of the model-visible settings; Stop order is.
func TestSettingsDigestExcludesMetadata(t *testing.T) {
	base := llm.Options{
		Model:       "m",
		Temperature: 0.2,
		MaxTokens:   256,
		TopP:        0.9,
		Stop:        []string{"a", "b"},
		ToolChoice:  "auto",
		Stream:      true,
	}
	withMeta := base
	withMeta.Metadata = map[string]any{"bad": func() {}}

	if got, want := settingsDigest("sys", withMeta), settingsDigest("sys", base); got != want {
		t.Fatal("Metadata must not affect the settings digest")
	}
	hotter := base
	hotter.Temperature = 0.3
	if settingsDigest("sys", hotter) == settingsDigest("sys", base) {
		t.Fatal("Temperature must affect the settings digest")
	}
	if settingsDigest("other", base) == settingsDigest("sys", base) {
		t.Fatal("System must affect the settings digest")
	}
	reordered := base
	reordered.Stop = []string{"b", "a"}
	if settingsDigest("sys", reordered) == settingsDigest("sys", base) {
		t.Fatal("Stop order must affect the settings digest")
	}
}

// TestSameToolSchemaIgnoresJSONKeyOrder pins the JSON-semantic comparison and
// the empty-parameters rule.
func TestSameToolSchemaIgnoresJSONKeyOrder(t *testing.T) {
	want := llm.ToolSchema{
		Name:        "t",
		Description: "d",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`),
	}
	got := llm.ToolSchema{
		Name:        "t",
		Description: "d",
		Parameters:  json.RawMessage(`{"required":["q"],"properties":{"q":{"type":"string"}},"type":"object"}`),
	}
	if !sameToolSchema(want, got) {
		t.Fatal("key order alone must not be a mismatch")
	}
	got.Description = "other"
	if sameToolSchema(want, got) {
		t.Fatal("a description change is a mismatch")
	}
	got = want
	got.Parameters = json.RawMessage(`{"type":"object"}`)
	if sameToolSchema(want, got) {
		t.Fatal("a parameters change is a mismatch")
	}
	emptyA := llm.ToolSchema{Name: "t", Description: "d"}
	emptyB := llm.ToolSchema{Name: "t", Description: "d", Parameters: json.RawMessage{}}
	if !sameToolSchema(emptyA, emptyB) {
		t.Fatal("nil and empty parameters must compare equal")
	}
}
