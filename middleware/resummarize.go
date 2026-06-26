package middleware

import (
	"context"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/session"
)

// Resummarize compacts a session in place by writing a persistent summary node
// when its projected history grows past the configured threshold. Unlike the
// Compaction middleware — which rewrites each request ephemerally and recomputes
// the summary every call — this persists the summary as a tree node via
// session.Summarize, so the compaction:
//
//   - survives restarts (it is a committed event),
//   - is computed once instead of per request,
//   - can be inspected, edited, or branched from (re-summarization is just
//     calling Resummarize again, which writes a newer node that supersedes the
//     older one),
//   - leaves derived State untouched (state still replays over every event).
//
// It returns true if a summary node was written. opts shares CompactionOptions
// with the Compaction middleware (MaxTokens / KeepRecentTokens / Estimator).
//
// A Runner can call this after each turn to keep a session bounded; it is kept
// out of the model-decorator chain because it needs the Store and Session, which
// the request layer does not see.
func Resummarize(ctx context.Context, store session.Store, s *session.Session, summarizer llm.Model, opts *CompactionOptions) (bool, error) {
	c := newCompactor(summarizer, opts)

	// Message-bearing events on the active path, with their messages. Working on
	// the projection (s.Messages) would lose the event IDs we need for the cut;
	// working on the events keeps both in lockstep.
	var bearing []*core.Event
	var msgs []core.Message
	total := 0
	for _, e := range s.Events() {
		if e.Message == nil {
			continue
		}
		bearing = append(bearing, e)
		msgs = append(msgs, *e.Message)
		total += c.estimate(*e.Message)
	}
	if total <= c.maxTokens {
		return false, nil
	}

	// findCut splits older (msgs[:cut], to summarize) from recent (msgs[cut:],
	// to keep), never stranding a tool result from its call.
	cut := c.findCut(msgs)
	if cut <= 0 {
		return false, nil // nothing safely summarizable
	}

	summary, err := c.summarize(ctx, msgs[:cut])
	if err != nil {
		return false, err
	}

	// The cut event is the last one covered; recent history begins after it.
	text := summaryPrefix + summary + summarySuffix
	if err := session.Summarize(ctx, store, s, bearing[cut-1].ID, text); err != nil {
		return false, err
	}
	return true, nil
}
