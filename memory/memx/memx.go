package memx

import (
	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/projectmem"
	"github.com/jiujuan/goagent/memory/rules"
	"github.com/jiujuan/goagent/memory/textmem"
	"github.com/jiujuan/goagent/memory/workingmem"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

// RAGConfig configures the auto-RAG middleware mounted over the semantic store.
type RAGConfig struct {
	// K is the number of documents to retrieve (default 4).
	K int
	// MinScore drops retrieved documents below this similarity (default 0).
	MinScore float64
	// Header is the preamble placed before injected context.
	Header string
}

// Config selects which memory layers to mount and where they persist. Every
// field is optional: a zero/empty field disables that layer, so callers opt into
// exactly the layers they want.
type Config struct {
	// Rules (ADR 0021). Either directory may be empty.
	GlobalRulesDir  string
	ProjectRulesDir string

	// ProjectRoot is the start directory for AGENTS.md discovery (ADR 0020).
	// Empty disables project memory.
	ProjectRoot string

	// EnableWorkingMemory mounts the working-memory section + update tool (ADR 0017).
	EnableWorkingMemory bool

	// TextMemDir enables file-backed text memory (ADR 0018) when non-empty.
	TextMemDir string

	// Semantic is a pre-built vector store (e.g. memory.File or memory.InMemory).
	// When set, RAG and/or the search tool are mounted per the flags below.
	Semantic memory.Store
	// RAG, when non-nil and Semantic is set, mounts the auto-RAG middleware.
	RAG *RAGConfig
	// EnableSearchTool mounts the model-driven search_memory tool when Semantic
	// is set.
	EnableSearchTool bool
	// SearchK is the top-k for the search tool (default 4).
	SearchK int

	// SectionBudget caps the rune length of the truncatable sections (working
	// memory, text-memory index). 0 means unbounded.
	SectionBudget int
}

// Memory is the assembled set of mountable pieces plus the stores that were
// constructed, so callers can also drive Consolidate with the same stores.
type Memory struct {
	Sections   []prompt.Section
	Middleware []agent.Middleware
	Tools      []tool.Tool

	// TextStore is the constructed text-memory store (nil if disabled).
	TextStore textmem.Store
	// Semantic echoes Config.Semantic (nil if none), for convenience.
	Semantic memory.Store
}

// New assembles the memory layers selected by cfg. The returned Sections are
// added to a prompt.Builder, Middleware to the model chain, and Tools to the
// agent. Section ordering is governed by each layer's Order constant (ADR 0016).
func New(cfg Config) (*Memory, error) {
	m := &Memory{Semantic: cfg.Semantic}

	// Rules — highest priority, never budgeted.
	if cfg.GlobalRulesDir != "" || cfg.ProjectRulesDir != "" {
		set, err := rules.Load(cfg.GlobalRulesDir, cfg.ProjectRulesDir)
		if err != nil {
			return nil, err
		}
		m.Sections = append(m.Sections, set.Section())
	}

	// Project memory (AGENTS.md) — hard context, never budgeted.
	if cfg.ProjectRoot != "" {
		docs, err := projectmem.Load(cfg.ProjectRoot)
		if err != nil {
			return nil, err
		}
		m.Sections = append(m.Sections, projectmem.Section(docs))
	}

	// Working memory — budgeted; mounts a tool.
	if cfg.EnableWorkingMemory {
		m.Sections = append(m.Sections, Budgeted(workingmem.Section(), cfg.SectionBudget))
		m.Tools = append(m.Tools, workingmem.UpdateTool())
	}

	// Text memory — budgeted index; mounts save/read tools.
	if cfg.TextMemDir != "" {
		store, err := textmem.File(cfg.TextMemDir)
		if err != nil {
			return nil, err
		}
		m.TextStore = store
		m.Sections = append(m.Sections, Budgeted(textmem.IndexSection(store), cfg.SectionBudget))
		m.Tools = append(m.Tools, textmem.SaveTool(store), textmem.ReadTool(store))
	}

	// Semantic memory — RAG middleware and/or search tool.
	if cfg.Semantic != nil {
		if cfg.RAG != nil {
			m.Middleware = append(m.Middleware, middleware.RAG(middleware.RAGOptions{
				Retriever: memory.NewRetriever(cfg.Semantic, cfg.RAG.MinScore),
				K:         cfg.RAG.K,
				Header:    cfg.RAG.Header,
			}))
		}
		if cfg.EnableSearchTool {
			m.Tools = append(m.Tools, memory.SearchTool(cfg.Semantic, cfg.SearchK))
		}
	}

	return m, nil
}
