package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/tool"
)

// ErrToolViewInvalid is returned when the tool list a step is about to freeze
// cannot be trusted: a name has no implementation, the same name is advertised
// twice, or an advertisement differs from the schema its implementation would
// generate. The check runs before the model call, so a middleware that rewrites
// tools cannot spend a request on a table execution could not honor.
var ErrToolViewInvalid = errors.New("agent: step view invalid")

// StepView is one step's frozen, read-only view. The loop creates it once the
// request is final (after ModifyRequest) and every later phase — the model call,
// tool approval, tool execution and the step digest — reads the same data.
//
// The fields are deliberately unexported. Accessors return copies of every slice
// the loop owns; the tool implementations, message parts and schema parameter
// bytes are shared and must be treated as read-only. llm.Options.Metadata is
// shared too.
type StepView struct {
	step    int
	model   string
	system  string
	options llm.Options
	history []core.Message
	tools   ToolView
	digest  StepDigest
}

// ToolView is the frozen tool table: what this step advertised, what each name
// resolves to, and which implementations force a serial batch.
type ToolView struct {
	advertised []llm.ToolSchema
	byName     map[string]tool.Tool
	sequential map[string]bool
	names      []string
	known      map[string]struct{} // every name in the runtime table at freeze time
}

// StepDigest summarizes what the model saw: the advertised tools in request
// order and the settings that shaped the call.
type StepDigest struct {
	ToolDigest     string
	SettingsDigest string
	Advertised     []string
}

// View returns the step view once it is frozen. It is nil in BeforeModel and
// ModifyRequest (the request is not final), and non-nil from the provider call
// onward: AfterModel, CheckToolPolicy, BeforeTool, AfterTool, ToolReject and
// OnError. A resumed batch also carries a view.
func (lc *LoopContext) View() *StepView {
	if lc == nil {
		return nil
	}
	return lc.view
}

// Step reports the loop step this view belongs to.
func (v *StepView) Step() int {
	if v == nil {
		return 0
	}
	return v.step
}

// Model reports the model id from the frozen request options. It is empty when
// the request did not set one.
func (v *StepView) Model() string {
	if v == nil {
		return ""
	}
	return v.model
}

// System reports the system prompt sent with the request.
func (v *StepView) System() string {
	if v == nil {
		return ""
	}
	return v.system
}

// Options returns a copy of the decoding settings. Stop is copied; Metadata is
// shared and read-only.
func (v *StepView) Options() llm.Options {
	if v == nil {
		return llm.Options{}
	}
	return cloneOptions(v.options)
}

// Messages returns a copy of the request's message slice. The messages and
// their parts are shared and read-only.
func (v *StepView) Messages() []core.Message {
	if v == nil {
		return nil
	}
	return slices.Clone(v.history)
}

// Tools returns the frozen tool table. A nil view returns nil.
func (v *StepView) Tools() *ToolView {
	if v == nil {
		return nil
	}
	return &v.tools
}

// Digest returns a copy of the step digest; Advertised is copied.
func (v *StepView) Digest() StepDigest {
	if v == nil {
		return StepDigest{}
	}
	out := v.digest
	out.Advertised = slices.Clone(v.digest.Advertised)
	return out
}

// Schemas returns a copy of the advertised schemas. The parameter bytes are
// shared and read-only.
func (tv *ToolView) Schemas() []llm.ToolSchema {
	if tv == nil {
		return nil
	}
	return slices.Clone(tv.advertised)
}

// Names returns a copy of the advertised names in request order.
func (tv *ToolView) Names() []string {
	if tv == nil {
		return nil
	}
	return slices.Clone(tv.names)
}

// Advertised reports whether the model was offered this tool during the step.
func (tv *ToolView) Advertised(name string) bool {
	if tv == nil {
		return false
	}
	for _, have := range tv.names {
		if have == name {
			return true
		}
	}
	return false
}

// Resolve returns the implementation the frozen table dispatches to.
func (tv *ToolView) Resolve(name string) (tool.Tool, bool) {
	if tv == nil {
		return nil, false
	}
	t, ok := tv.byName[name]
	return t, ok
}

// Sequential reports whether the implementation behind name forces its batch to
// run one call at a time.
func (tv *ToolView) Sequential(name string) bool {
	if tv == nil {
		return false
	}
	return tv.sequential[name]
}

// knownName distinguishes a registered-but-unadvertised tool from a name that
// was never in the runtime table. The refusal class depends on it.
func (tv *ToolView) knownName(name string) bool {
	if tv == nil {
		return false
	}
	_, ok := tv.known[name]
	return ok
}

// runToolTable is the final tool table a step is built from: static tools in
// registration order, injected tools replacing by name, injected-only tools
// sorted after them.
type runToolTable struct {
	schemas    []llm.ToolSchema
	byName     map[string]tool.Tool
	sequential map[string]bool
	known      map[string]struct{}
}

// snapshotTools reads one consistent copy of the runtime tool tables and
// resolves the final winner for every name. Injected tools win both the schema
// and the implementation. A static name registered more than once keeps its
// first slot but advertises and executes the last registration.
func (l *AgentLoop) snapshotTools(rc *RunContext) runToolTable {
	injected := rc.dynamic.snapshot()
	table := runToolTable{
		byName:     make(map[string]tool.Tool, len(l.byName)+len(injected)),
		sequential: make(map[string]bool, len(l.byName)+len(injected)),
		known:      make(map[string]struct{}, len(l.byName)+len(injected)),
	}
	for name := range l.byName {
		table.known[name] = struct{}{}
	}

	occurrences := make(map[string]int, len(l.schemas))
	for _, s := range l.schemas {
		occurrences[s.Name]++
	}
	seen := make(map[string]struct{}, len(l.schemas))
	for _, s := range l.schemas {
		name := s.Name
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		table.known[name] = struct{}{}

		if t, ok := injected[name]; ok {
			table.byName[name] = t
			table.sequential[name] = isSequentialTool(t)
			table.schemas = append(table.schemas, tool.SchemaOf(t))
			continue
		}
		t, ok := l.byName[name]
		if !ok {
			continue
		}
		table.byName[name] = t
		table.sequential[name] = isSequentialTool(t)
		if occurrences[name] == 1 {
			// The construction-time schema is still valid for the only
			// implementation that ever had this name.
			table.schemas = append(table.schemas, s)
		} else {
			table.schemas = append(table.schemas, tool.SchemaOf(t))
		}
	}

	var extra []string
	for name := range injected {
		if _, ok := seen[name]; ok {
			continue
		}
		extra = append(extra, name)
	}
	slices.Sort(extra)
	for _, name := range extra {
		t := injected[name]
		table.known[name] = struct{}{}
		table.byName[name] = t
		table.sequential[name] = isSequentialTool(t)
		table.schemas = append(table.schemas, tool.SchemaOf(t))
	}
	return table
}

// resumeStepView builds the view a resumed batch executes under. Without a
// paused snapshot (every checkpoint until snapshots are persisted) the names
// come from the batch itself and the digest is empty. Tools the current static
// table no longer has stay advertised by name but cannot resolve; the restore
// preflight added later rejects that case before execution.
func (l *AgentLoop) resumeStepView(rb *resumeBatch) *StepView {
	tv := ToolView{
		byName:     make(map[string]tool.Tool, len(rb.calls)),
		sequential: make(map[string]bool, len(rb.calls)),
		known:      make(map[string]struct{}, len(l.byName)),
	}
	for name := range l.byName {
		tv.known[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(rb.calls))
	for _, c := range rb.calls {
		if _, ok := seen[c.Name]; ok {
			continue
		}
		seen[c.Name] = struct{}{}
		tv.names = append(tv.names, c.Name)
		t, ok := l.byName[c.Name]
		if !ok {
			continue
		}
		tv.byName[c.Name] = t
		tv.sequential[c.Name] = isSequentialTool(t)
		tv.known[c.Name] = struct{}{}
		tv.advertised = append(tv.advertised, tool.SchemaOf(t))
	}
	return &StepView{step: rb.step, tools: tv}
}

// freezeStepView validates the final request against the runtime tool tables
// and freezes it into a view. The tables are read again here, not reused from
// request construction, because ModifyRequest may have injected tools.
func (l *AgentLoop) freezeStepView(lc *LoopContext, req *llm.Request) (*StepView, error) {
	table := l.snapshotTools(lc.RunContext)
	tv := ToolView{
		advertised: make([]llm.ToolSchema, len(req.Tools)),
		byName:     make(map[string]tool.Tool, len(req.Tools)),
		sequential: make(map[string]bool, len(req.Tools)),
		names:      make([]string, len(req.Tools)),
		known:      table.known,
	}
	seen := make(map[string]struct{}, len(req.Tools))
	for i, s := range req.Tools {
		if _, dup := seen[s.Name]; dup {
			return nil, fmt.Errorf("%w: tool %q is advertised more than once", ErrToolViewInvalid, s.Name)
		}
		seen[s.Name] = struct{}{}
		impl, ok := table.byName[s.Name]
		if !ok {
			return nil, fmt.Errorf("%w: tool %q has no implementation", ErrToolViewInvalid, s.Name)
		}
		if !sameToolSchema(s, tool.SchemaOf(impl)) {
			return nil, fmt.Errorf("%w: tool %q does not match its implementation's schema", ErrToolViewInvalid, s.Name)
		}
		tv.advertised[i] = s
		tv.byName[s.Name] = impl
		tv.names[i] = s.Name
		tv.sequential[s.Name] = isSequentialTool(impl)
	}
	view := &StepView{
		step:    lc.Step,
		model:   req.Options.Model,
		system:  req.System,
		options: cloneOptions(req.Options),
		history: slices.Clone(req.Messages),
		tools:   tv,
		digest: StepDigest{
			ToolDigest:     toolDigest(tv.advertised),
			SettingsDigest: settingsDigest(req.System, req.Options),
			Advertised:     slices.Clone(tv.names),
		},
	}
	return view, nil
}

// upsertSchema replaces the schema of an existing name in place and appends a
// new name, keeping exactly one advertisement slot per tool.
func upsertSchema(list []llm.ToolSchema, s llm.ToolSchema) []llm.ToolSchema {
	for i := range list {
		if list[i].Name == s.Name {
			list[i] = s
			return list
		}
	}
	return append(list, s)
}

func isSequentialTool(t tool.Tool) bool {
	s, ok := t.(tool.SequentialTool)
	return ok && s.SequentialExecution()
}

func cloneOptions(o llm.Options) llm.Options {
	o.Stop = slices.Clone(o.Stop)
	return o
}

// sameToolSchema compares an advertisement with the schema its implementation
// would generate. Parameters are compared by JSON semantics so key order alone
// is not a mismatch; nil and empty parameter bytes are equal.
func sameToolSchema(a, b llm.ToolSchema) bool {
	if a.Name != b.Name || a.Description != b.Description {
		return false
	}
	return sameJSON(a.Parameters, b.Parameters)
}

func sameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return reflect.DeepEqual(a, b)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return reflect.DeepEqual(a, b)
	}
	return reflect.DeepEqual(av, bv)
}

// toolDigest hashes the advertised tools in request order, length-prefixing each
// field so concatenation cannot blur tool or field boundaries.
func toolDigest(schemas []llm.ToolSchema) string {
	h := sha256.New()
	writeField := func(b []byte) {
		_, _ = h.Write(strconv.AppendInt(nil, int64(len(b)), 10))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write(b)
	}
	writeField(strconv.AppendInt(nil, int64(len(schemas)), 10))
	for _, s := range schemas {
		writeField([]byte(s.Name))
		writeField([]byte(s.Description))
		writeField(s.Parameters)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// stepSettings is the canonical shape hashed into SettingsDigest. Metadata is
// deliberately absent: provider passthrough is not part of the model-visible
// settings, and arbitrary values may not serialize.
type stepSettings struct {
	Model       string   `json:"model"`
	System      string   `json:"system"`
	Temperature float64  `json:"temperature"`
	MaxTokens   int      `json:"max_tokens"`
	TopP        float64  `json:"top_p"`
	Stop        []string `json:"stop,omitempty"`
	ToolChoice  string   `json:"tool_choice,omitempty"`
	Stream      bool     `json:"stream"`
}

func settingsDigest(system string, opts llm.Options) string {
	encoded, err := canonicalJSON(stepSettings{
		Model:       opts.Model,
		System:      system,
		Temperature: opts.Temperature,
		MaxTokens:   opts.MaxTokens,
		TopP:        opts.TopP,
		Stop:        opts.Stop,
		ToolChoice:  opts.ToolChoice,
		Stream:      opts.Stream,
	})
	if err != nil {
		// stepSettings holds only strings, numbers, bools and a string slice.
		encoded = []byte(err.Error())
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical json: %w", err)
	}
	return b, nil
}
