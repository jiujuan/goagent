package session

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/core"
)

// SummaryAuthor is the Author stamped on summary-node events.
const SummaryAuthor = "summarizer"

// Summarize appends a persistent summary node to the session: a new leaf whose
// Message (text) stands in for the conversation prefix from the root up to and
// including cutEventID. After this, Session.Messages projects [summary, ...recent]
// instead of the full prefix, shrinking the model's context while keeping the
// event log append-only and the derived State intact.
//
// cutEventID must be an event on the current active path. Re-summarizing is just
// calling Summarize again with a later cut — the new node supersedes the old one
// (the summary nearest the leaf wins), and the superseded summary falls into the
// replaced prefix.
//
// It works with any Store (it is a plain Append of a marked event), persisting
// wherever the store does.
func Summarize(ctx context.Context, store Store, s *Session, cutEventID, summary string) error {
	if indexOfEvent(s.activePath(), cutEventID) < 0 {
		return fmt.Errorf("session: summarize cut %q not on active path", cutEventID)
	}
	msg := core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: summary}}}
	e := &core.Event{
		Author:       SummaryAuthor,
		Message:      &msg,
		SummarizesTo: cutEventID,
	}
	return store.Append(ctx, s, e)
}
