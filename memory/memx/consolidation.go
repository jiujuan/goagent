package memx

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/textmem"
)

// DefaultConsolidationTimeout bounds one end-of-run consolidation.
const DefaultConsolidationTimeout = 30 * time.Second

// ConsolidationConfig configures the automatic short-term -> long-term bridge:
// a middleware that consolidates a run's messages when the run finishes.
type ConsolidationConfig struct {
	// Model extracts the durable facts. Required. It is deliberately separate
	// from the agent's own model, which may be a cheap or local one.
	Model llm.Model
	// Text is the curated destination; nil falls back to the store built from
	// Config.TextMemDir (see memx.New).
	Text textmem.Store
	// Semantic is the bulk destination; nil falls back to Config.Semantic.
	Semantic memory.Store

	// MaxTranscript caps the transcript at that many runes, keeping the tail.
	// Zero uses DefaultMaxTranscriptRunes; negative means uncapped.
	MaxTranscript int

	// Async runs the consolidation on its own goroutine instead of blocking the
	// run's terminal event: Wait/Iter then return before the facts are stored.
	Async bool
	// Timeout bounds one consolidation call, sync or async. Zero or negative
	// uses DefaultConsolidationTimeout.
	Timeout time.Duration
	// Log receives consolidation failures; they are never turned into run
	// failures. nil uses the standard logger.
	Log *log.Logger
}

// Consolidator returns the run-end middleware described by cfg. It implements
// agent.RunFinisher, so it mounts with agent.WithMiddleware — memx.New does
// that when Config.Consolidation is set.
func Consolidator(cfg ConsolidationConfig) agent.Middleware {
	max := cfg.MaxTranscript
	if max == 0 {
		max = DefaultMaxTranscriptRunes
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultConsolidationTimeout
	}
	logger := cfg.Log
	if logger == nil {
		logger = log.Default()
	}
	return &consolidator{cfg: cfg, max: max, timeout: timeout, log: logger, done: map[string]int{}}
}

type consolidator struct {
	agent.BaseMiddleware
	cfg     ConsolidationConfig
	max     int
	timeout time.Duration
	log     *log.Logger

	// mu guards done, which remembers how many messages of each thread were
	// consolidated in this process. State.KV cannot carry the cursor: the last
	// checkpoint is written before the run ends, so a post-run write would be
	// lost. Cross-process repeats are covered by content-key dedup (ADR 0019).
	mu   sync.Mutex
	done map[string]int
}

// FinishRun implements agent.RunFinisher. A run that failed is still
// consolidated — its transcript can hold facts worth keeping — and runErr only
// reaches the log line.
func (c *consolidator) FinishRun(rc *agent.RunContext, _ core.Result, runErr error) {
	if rc == nil || rc.State == nil || c.cfg.Model == nil {
		return
	}
	// The caller abandoned this run; do not spend another model call on it.
	if rc.Err() != nil {
		return
	}

	msgs, upto := c.pending(rc)
	if len(msgs) == 0 {
		return
	}
	// Snapshot: a resumed or compacted run reshapes State.Messages, and an async
	// consolidation must not read the live slice from another goroutine.
	msgs = append([]core.Message(nil), msgs...)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(rc.Context), c.timeout)
	if c.cfg.Async {
		go func() {
			defer cancel()
			c.run(ctx, rc.ThreadID, msgs, upto, runErr)
		}()
		return
	}
	defer cancel()
	c.run(ctx, rc.ThreadID, msgs, upto, runErr)
}

// pending returns the thread's messages not consolidated yet, and the offset.
func (c *consolidator) pending(rc *agent.RunContext) ([]core.Message, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	upto := c.done[rc.ThreadID]
	if upto > len(rc.State.Messages) {
		upto = 0 // branched or replayed: shorter than remembered
	}
	return rc.State.Messages[upto:], upto
}

// run performs the extraction and remembers the new offset. A failure is
// logged, never escalated: consolidation is a bonus on top of a settled run.
func (c *consolidator) run(ctx context.Context, threadID string, msgs []core.Message, upto int, runErr error) {
	if err := consolidate(ctx, c.cfg.Model, msgs, c.cfg.Text, c.cfg.Semantic, c.max); err != nil {
		c.log.Printf("memx: end-of-run consolidation failed (run error %v): %v", runErr, err)
		return
	}
	c.mu.Lock()
	c.done[threadID] = upto + len(msgs)
	c.mu.Unlock()
}
