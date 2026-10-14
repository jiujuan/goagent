package checkpoint

import (
	"context"
	"fmt"
	"sort"

	"github.com/jiujuan/goagent/core"
)

// Memory answers the query interfaces from the summary index it maintains while snapshots
// become visible (ADR-0034 D7). It never walks byThread to serve a page, so the cost of
// "list my sessions" does not depend on how many messages those threads hold, and a page
// walk cannot hand out a pointer into a stored Checkpoint.
//
// The index is written in the same critical section that appends the snapshot, which gives
// the two properties the paging rules depend on: a commit that failed left no summary
// behind, and a commit that was recognised as a retry added no second one.

var (
	_ Query          = (*Memory)(nil)
	_ MetadataReader = (*Memory)(nil)
)

// QueryCheckpoints pages over checkpoint summaries.
func (m *Memory) QueryCheckpoints(_ context.Context, opts ListOptions) (MetadataPage, error) {
	limit, err := normalizeQueryLimit(opts.Limit)
	if err != nil {
		return MetadataPage{}, err
	}
	filter := cursorFingerprint(cursorDomainCheckpoint, opts)
	if opts.Cursor != "" {
		payload, err := decodeCursor(opts.Cursor)
		if err != nil {
			return MetadataPage{}, err
		}
		if err := cursorFilterMatch(payload, filter); err != nil {
			return MetadataPage{}, err
		}
		pinned, err := m.projections.resolve(payload.Generation)
		if err != nil {
			return MetadataPage{}, err
		}
		items, ok := pinned.items.([]CheckpointMetadata)
		if !ok {
			return MetadataPage{}, fmt.Errorf("checkpoint: cursor generation %d pins %T, not checkpoint summaries: %w",
				payload.Generation, pinned.items, ErrInvalidCursor)
		}
		return checkpointMetadataPage(items, payload.Offset, limit, filter, payload.Generation)
	}

	items := m.filteredCheckpointMetadata(opts)
	generation := m.projections.pin(items)
	return checkpointMetadataPage(items, 0, limit, filter, generation)
}

// ListSessions pages over the threads that have a visible snapshot, with each head summary
// attached. A thread that only holds inbox traffic is not a persisted session.
func (m *Memory) ListSessions(_ context.Context, opts ListOptions) (SessionPage, error) {
	limit, err := normalizeQueryLimit(opts.Limit)
	if err != nil {
		return SessionPage{}, err
	}
	filter := cursorFingerprint(cursorDomainSession, opts)
	if opts.Cursor != "" {
		payload, err := decodeCursor(opts.Cursor)
		if err != nil {
			return SessionPage{}, err
		}
		if err := cursorFilterMatch(payload, filter); err != nil {
			return SessionPage{}, err
		}
		pinned, err := m.projections.resolve(payload.Generation)
		if err != nil {
			return SessionPage{}, err
		}
		items, ok := pinned.items.([]SessionSummary)
		if !ok {
			return SessionPage{}, fmt.Errorf("checkpoint: cursor generation %d pins %T, not session summaries: %w",
				payload.Generation, pinned.items, ErrInvalidCursor)
		}
		return sessionSummaryPage(items, payload.Offset, limit, filter, payload.Generation)
	}

	items := m.filteredSessionSummaries(opts)
	generation := m.projections.pin(items)
	return sessionSummaryPage(items, 0, limit, filter, generation)
}

// ReadCheckpointMetadata looks one summary up by identity. Only the thread named in the Ref
// is searched: a checkpoint ID alone does not identify a thread.
func (m *Memory) ReadCheckpointMetadata(_ context.Context, ref Ref) (CheckpointMetadata, bool, error) {
	if err := core.CheckThreadID(ref.ThreadID); err != nil {
		return CheckpointMetadata{}, false, err
	}
	if ref.CheckpointID == "" {
		return CheckpointMetadata{}, false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, metadata := range m.summaries[ref.ThreadID] {
		if metadata.Ref.CheckpointID == ref.CheckpointID {
			return cloneCheckpointMetadata(metadata), true, nil
		}
	}
	return CheckpointMetadata{}, false, nil
}

// ReadSessionHead returns the summary of the snapshot a thread committed last.
func (m *Memory) ReadSessionHead(_ context.Context, threadID string) (CheckpointMetadata, bool, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return CheckpointMetadata{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	index := m.summaries[threadID]
	if len(index) == 0 {
		return CheckpointMetadata{}, false, nil
	}
	return cloneCheckpointMetadata(index[len(index)-1]), true, nil
}

// filteredCheckpointMetadata takes the projection the first page pins: every summary the
// filters select, in the order the walk will use. Threads are visited by name so two calls
// on unchanged contents produce the same list.
func (m *Memory) filteredCheckpointMetadata(opts ListOptions) []CheckpointMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]CheckpointMetadata, 0, m.summaryCountLocked())
	for _, threadID := range m.summaryThreadsLocked(opts) {
		for _, metadata := range m.summaries[threadID] {
			if opts.matches(metadata) {
				items = append(items, cloneCheckpointMetadata(metadata))
			}
		}
	}
	orderCheckpointMetadata(items, opts.ThreadID != "")
	return items
}

func (m *Memory) filteredSessionSummaries(opts ListOptions) []SessionSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]SessionSummary, 0, len(m.summaries))
	for _, threadID := range m.summaryThreadsLocked(opts) {
		index := m.summaries[threadID]
		if len(index) == 0 {
			continue
		}
		summary := SessionSummary{ThreadID: threadID, Head: cloneCheckpointMetadata(index[len(index)-1])}
		if opts.matches(summary.Head) {
			items = append(items, summary)
		}
	}
	orderSessionSummaries(items)
	return items
}

// summaryThreadsLocked lists the indexed threads, narrowed to the requested one when the
// caller named it. Sorted names keep a cross-thread projection reproducible: ranging the map
// directly would give one order per process and another per run.
func (m *Memory) summaryThreadsLocked(opts ListOptions) []string {
	if opts.ThreadID != "" {
		if _, ok := m.summaries[opts.ThreadID]; !ok {
			return nil
		}
		return []string{opts.ThreadID}
	}
	threads := make([]string, 0, len(m.summaries))
	for threadID := range m.summaries {
		threads = append(threads, threadID)
	}
	sort.Strings(threads)
	return threads
}

func (m *Memory) summaryCountLocked() int {
	total := 0
	for _, index := range m.summaries {
		total += len(index)
	}
	return total
}

// checkpointMetadataPage slices a pinned list, then hands back copies: a caller that edits a
// returned summary must not be able to rewrite what the next page reads.
func checkpointMetadataPage(items []CheckpointMetadata, offset, limit int, filter string, generation uint64) (MetadataPage, error) {
	if offset > len(items) {
		return MetadataPage{}, fmt.Errorf("checkpoint: cursor position %d is past the end of a %d-row projection: %w",
			offset, len(items), ErrInvalidCursor)
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	page := MetadataPage{Items: make([]CheckpointMetadata, 0, end-offset)}
	for _, item := range items[offset:end] {
		page.Items = append(page.Items, cloneCheckpointMetadata(item))
	}
	if end < len(items) {
		page.NextCursor = encodeCursor(cursorPayload{
			Version:    cursorFormatVersion,
			Generation: generation,
			Filter:     filter,
			Offset:     end,
		})
	}
	return page, nil
}

func sessionSummaryPage(items []SessionSummary, offset, limit int, filter string, generation uint64) (SessionPage, error) {
	if offset > len(items) {
		return SessionPage{}, fmt.Errorf("checkpoint: cursor position %d is past the end of a %d-row projection: %w",
			offset, len(items), ErrInvalidCursor)
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	page := SessionPage{Items: make([]SessionSummary, 0, end-offset)}
	for _, item := range items[offset:end] {
		page.Items = append(page.Items, SessionSummary{
			ThreadID: item.ThreadID,
			Head:     cloneCheckpointMetadata(item.Head),
		})
	}
	if end < len(items) {
		page.NextCursor = encodeCursor(cursorPayload{
			Version:    cursorFormatVersion,
			Generation: generation,
			Filter:     filter,
			Offset:     end,
		})
	}
	return page, nil
}
