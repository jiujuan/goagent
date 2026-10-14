package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Query and MetadataReader are the optional read-only views over a checkpointer
// (ADR-0034 D7). They answer "what is on this thread, when was it committed, how did the
// run end" without ever handing back a State, a message, a tool argument or a file blob.
//
// This matters because the record a list caller wants is one row out of a snapshot that
// carries the whole conversation. Reading full checkpoints to fill a session list makes the
// cost of "list my sessions" proportional to the size of every conversation, which is the
// opposite of what a reconnecting client or an audit screen needs. A store that implements
// these interfaces keeps a small projection of its own, and a page walk reads that.
//
// Both interfaces are optional: a third-party Checkpointer that does not implement them is
// still a working Checkpointer, and the caller must say so rather than fall back to
// scanning everything it holds.

const (
	// DefaultQueryLimit is the page size when ListOptions.Limit is not set.
	DefaultQueryLimit = 50
	// MaxQueryLimit is the largest page size a caller may ask for. Asking for more is an
	// error rather than a silent clamp: a caller that believes it fetched 1000 rows and
	// got 200 would compute a wrong answer, not a slow one.
	MaxQueryLimit = 200
)

var (
	// ErrInvalidCursor means the token is not a token this protocol speaks: corrupt,
	// unknown version, out-of-range position, or reused under different filters.
	ErrInvalidCursor = errors.New("checkpoint: invalid cursor")
	// ErrCursorExpired means the token is well formed but the snapshot projection it
	// pinned is gone — its time-to-live ran out, or the store dropped projections to stay
	// within its cache limit. Start a new query; do not retry the same token.
	ErrCursorExpired = errors.New("checkpoint: cursor expired")
	// ErrInvalidQuery means the options themselves are unusable, such as a page size
	// above MaxQueryLimit.
	ErrInvalidQuery = errors.New("checkpoint: invalid query")
	// ErrCapabilityUnavailable means the configured store does not offer the lightweight
	// summary views. A caller must say so rather than fall back to reading and decoding
	// whole checkpoints, which is exactly the cost these views exist to avoid.
	ErrCapabilityUnavailable = errors.New("checkpoint: lightweight summary query unavailable")
)

// CheckpointMetadata is the payload-free projection of one checkpoint. Every field is one
// the store already recorded; nothing here is inferred.
type CheckpointMetadata struct {
	// Ref locates the snapshot. A summary is always about a (thread, checkpoint) pair,
	// because a checkpoint ID alone cannot be resolved across threads.
	Ref Ref `json:"ref"`
	// ParentRef is this record's lineage edge, nil when the record carries none.
	ParentRef *Ref `json:"parent_ref,omitempty"`
	// LegacyParentUnresolved marks the other case: the record has a parent_id but was
	// written before a parent thread could be recorded. The reader must not guess which
	// thread the parent lives on (ADR-0034 D2), so no ParentRef is offered for it.
	LegacyParentUnresolved bool `json:"legacy_parent_unresolved,omitempty"`
	// Revision is the per-thread sequence the durable store assigned.
	Revision uint64 `json:"revision"`
	Step     int    `json:"step"`
	// RunID and TurnID are the execution identity that committed this snapshot, empty on
	// records committed before run identity existed.
	RunID  string `json:"run_id,omitempty"`
	TurnID string `json:"turn_id,omitempty"`
	// CreatedAt is when the record first became visible. It stays zero for records that
	// predate the timestamp rather than being back-filled with the read time, so a caller
	// can tell "unknown" from "committed at the epoch".
	CreatedAt time.Time `json:"created_at,omitzero"`
	// Paused marks a snapshot a run stopped at for a human decision.
	Paused bool `json:"paused,omitempty"`
	// CompletionKind is CompletionCompleted or CompletionFailed for a record that has a
	// terminal record attached, and empty for one that does not — which is not the same
	// claim as "this run completed".
	CompletionKind string `json:"completion_kind,omitempty"`
}

// SessionSummary is one persisted conversation: a thread and the metadata of its head
// snapshot. A thread only appears in a session list once a checkpoint has become visible on
// it, so an open session that has committed nothing is not reported as durable state.
type SessionSummary struct {
	ThreadID string             `json:"thread_id"`
	Head     CheckpointMetadata `json:"head"`
}

// ListOptions is the filter and paging input the checkpoint queries share.
//
// Time bounds are the half-open interval [CreatedFrom, CreatedBefore). Records whose
// CreatedAt is unknown are returned when no time bound is set and excluded as soon as one
// is: a bound cannot be judged against a time the store never recorded, and silently
// matching such a record would put "unknown" inside every range.
type ListOptions struct {
	// Limit is the page size: zero or negative takes DefaultQueryLimit, and anything
	// above MaxQueryLimit is ErrInvalidQuery.
	Limit int
	// Cursor is the opaque token from a previous page's NextCursor. Empty starts a new
	// query, which pins the projection the rest of the walk reads.
	Cursor         string
	ThreadID       string
	RunID          string
	CreatedFrom    time.Time
	CreatedBefore  time.Time
	CompletionKind string
}

// MetadataPage is one page of checkpoint summaries. NextCursor is empty when the page was
// the last one, so a caller never has to fetch an empty page to learn the walk finished.
type MetadataPage struct {
	Items      []CheckpointMetadata `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// SessionPage is one page of session summaries, paged by the same rules.
type SessionPage struct {
	Items      []SessionSummary `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// MetadataReader resolves one summary by identity. It is the read a reconciler needs:
// "does this run's completion record exist, and on which thread", without loading the
// snapshot's state.
type MetadataReader interface {
	// ReadCheckpointMetadata returns the summary of one snapshot. ok is false when the
	// store has no such record; only Ref.ThreadID can match, since a checkpoint ID does
	// not identify a thread.
	ReadCheckpointMetadata(ctx context.Context, ref Ref) (metadata CheckpointMetadata, ok bool, err error)
	// ReadSessionHead returns the summary of a thread's most visible snapshot, or false
	// when the thread has committed nothing.
	ReadSessionHead(ctx context.Context, threadID string) (metadata CheckpointMetadata, ok bool, err error)
}

// Query lists summaries. Both methods page over a snapshot projection pinned by the first
// call, so records committed afterwards appear only to a new query.
type Query interface {
	// QueryCheckpoints lists snapshot summaries. With ListOptions.ThreadID it is one
	// thread's timeline ordered by Revision; without it the walk spans threads and is
	// ordered by (CreatedAt, ThreadID, Revision, ID).
	QueryCheckpoints(ctx context.Context, opts ListOptions) (MetadataPage, error)
	// ListSessions enumerates threads that have a visible snapshot, ordered by ThreadID,
	// with each thread's head summary attached. Filters apply to the head.
	ListSessions(ctx context.Context, opts ListOptions) (SessionPage, error)
}

// checkpointMetadataOf projects a record without touching its state.
func checkpointMetadataOf(cp *Checkpoint) CheckpointMetadata {
	metadata := CheckpointMetadata{
		Ref:       Ref{ThreadID: cp.ThreadID, CheckpointID: cp.ID},
		Revision:  cp.Revision,
		Step:      cp.Step,
		RunID:     cp.RunID,
		TurnID:    cp.TurnID,
		CreatedAt: cp.CreatedAt,
		Paused:    cp.Pending != nil || cp.Pause != nil,
	}
	if cp.ParentRef != nil {
		parent := *cp.ParentRef
		metadata.ParentRef = &parent
	} else if cp.ParentID != "" {
		metadata.LegacyParentUnresolved = true
	}
	if cp.Completion != nil {
		metadata.CompletionKind = cp.Completion.Kind
	}
	return metadata
}

// cloneCheckpointMetadata copies the one indirection the projection carries, so a caller
// holding a page cannot write through into the store's index.
func cloneCheckpointMetadata(metadata CheckpointMetadata) CheckpointMetadata {
	if metadata.ParentRef != nil {
		parent := *metadata.ParentRef
		metadata.ParentRef = &parent
	}
	return metadata
}

// normalizeQueryLimit turns an unset page size into the default and rejects one the protocol
// does not allow.
func normalizeQueryLimit(limit int) (int, error) {
	if limit <= 0 {
		return DefaultQueryLimit, nil
	}
	if limit > MaxQueryLimit {
		return 0, fmt.Errorf("checkpoint: query limit %d exceeds the maximum %d: %w", limit, MaxQueryLimit, ErrInvalidQuery)
	}
	return limit, nil
}

func (o ListOptions) hasTimeBounds() bool {
	return !o.CreatedFrom.IsZero() || !o.CreatedBefore.IsZero()
}

// matches applies every filter to one summary. Identity and category filters compare
// recorded values; a time bound never matches a record whose time is unknown.
//
// The thread check is the shared meaning of ListOptions.ThreadID. A store that enumerates
// every thread relies on it here; Memory additionally narrows the threads it visits, so
// serving a single-thread timeline does not walk the whole index.
func (o ListOptions) matches(metadata CheckpointMetadata) bool {
	if o.ThreadID != "" && metadata.Ref.ThreadID != o.ThreadID {
		return false
	}
	if o.RunID != "" && metadata.RunID != o.RunID {
		return false
	}
	if o.CompletionKind != "" && metadata.CompletionKind != o.CompletionKind {
		return false
	}
	if o.hasTimeBounds() {
		if metadata.CreatedAt.IsZero() {
			return false
		}
		if !o.CreatedFrom.IsZero() && metadata.CreatedAt.Before(o.CreatedFrom) {
			return false
		}
		if !o.CreatedBefore.IsZero() && !metadata.CreatedAt.Before(o.CreatedBefore) {
			return false
		}
	}
	return true
}

// orderCheckpointMetadata sorts a pinned list. Both orders are total: a tie on every
// preceding key falls through to the checkpoint ID, so two runs of the same query on the
// same projection cannot come out differently.
func orderCheckpointMetadata(items []CheckpointMetadata, singleThread bool) {
	if singleThread {
		sort.SliceStable(items, func(i, j int) bool {
			a, b := items[i], items[j]
			if a.Revision != b.Revision {
				return a.Revision < b.Revision
			}
			return a.Ref.CheckpointID < b.Ref.CheckpointID
		})
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		if a.Ref.ThreadID != b.Ref.ThreadID {
			return a.Ref.ThreadID < b.Ref.ThreadID
		}
		if a.Revision != b.Revision {
			return a.Revision < b.Revision
		}
		return a.Ref.CheckpointID < b.Ref.CheckpointID
	})
}

func orderSessionSummaries(items []SessionSummary) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].ThreadID != items[j].ThreadID {
			return items[i].ThreadID < items[j].ThreadID
		}
		return items[i].Head.Revision < items[j].Head.Revision
	})
}

// pinnedPage is where a page walk stands: which projection, under which filters, and how
// many rows the caller has already received.
type pinnedPage struct {
	filter     string
	generation uint64
	offset     int
}

// pinnedList is the paging rule every store shares: the first call filters, orders and
// freezes the result, and a later call resumes that same list. Building it only on the first
// page is what keeps a walk free of repeats and gaps while the store keeps taking writes.
func pinnedList[T any](cache *projectionCache, domain string, opts ListOptions, build func() ([]T, error)) ([]T, pinnedPage, error) {
	filter := cursorFingerprint(domain, opts)
	if opts.Cursor == "" {
		items, err := build()
		if err != nil {
			return nil, pinnedPage{}, err
		}
		return items, pinnedPage{filter: filter, generation: cache.pin(items)}, nil
	}
	payload, err := decodeCursor(opts.Cursor)
	if err != nil {
		return nil, pinnedPage{}, err
	}
	if err := cursorFilterMatch(payload, filter); err != nil {
		return nil, pinnedPage{}, err
	}
	pinned, err := cache.resolve(payload.Generation)
	if err != nil {
		return nil, pinnedPage{}, err
	}
	items, ok := pinned.items.([]T)
	if !ok {
		return nil, pinnedPage{}, fmt.Errorf("checkpoint: cursor generation %d pins %T, not the expected summary list: %w",
			payload.Generation, pinned.items, ErrInvalidCursor)
	}
	return items, pinnedPage{filter: filter, generation: payload.Generation, offset: payload.Offset}, nil
}

// checkpointMetadataPage slices a pinned list and hands back copies, so a caller that edits
// a returned summary cannot rewrite what the next page reads.
func checkpointMetadataPage(items []CheckpointMetadata, page pinnedPage, limit int) (MetadataPage, error) {
	if page.offset > len(items) {
		return MetadataPage{}, fmt.Errorf("checkpoint: cursor position %d is past the end of a %d-row projection: %w",
			page.offset, len(items), ErrInvalidCursor)
	}
	end := page.offset + limit
	if end > len(items) {
		end = len(items)
	}
	out := MetadataPage{Items: make([]CheckpointMetadata, 0, end-page.offset)}
	for _, item := range items[page.offset:end] {
		out.Items = append(out.Items, cloneCheckpointMetadata(item))
	}
	if end < len(items) {
		out.NextCursor = encodeCursor(cursorPayload{
			Version:    cursorFormatVersion,
			Generation: page.generation,
			Filter:     page.filter,
			Offset:     end,
		})
	}
	return out, nil
}

func sessionSummaryPage(items []SessionSummary, page pinnedPage, limit int) (SessionPage, error) {
	if page.offset > len(items) {
		return SessionPage{}, fmt.Errorf("checkpoint: cursor position %d is past the end of a %d-row projection: %w",
			page.offset, len(items), ErrInvalidCursor)
	}
	end := page.offset + limit
	if end > len(items) {
		end = len(items)
	}
	out := SessionPage{Items: make([]SessionSummary, 0, end-page.offset)}
	for _, item := range items[page.offset:end] {
		out.Items = append(out.Items, SessionSummary{
			ThreadID: item.ThreadID,
			Head:     cloneCheckpointMetadata(item.Head),
		})
	}
	if end < len(items) {
		out.NextCursor = encodeCursor(cursorPayload{
			Version:    cursorFormatVersion,
			Generation: page.generation,
			Filter:     page.filter,
			Offset:     end,
		})
	}
	return out, nil
}
