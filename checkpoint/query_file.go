package checkpoint

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jiujuan/goagent/core"
)

// File answers the query interfaces from the rebuildable summary projection
// (ADR-0034 D7). A page is served by reading each thread's projection, which is re-derived
// from the journal tail only when the journal has grown. Nothing on this path decodes a
// State, restores a file blob, or calls Load, Latest or History — reading a hundred
// conversations to name them is the cost this layer exists to remove.
//
// Cross-thread enumeration walks the store directory. That is the sanctioned way to find
// threads for now: the journal remains the source of truth, and a global directory index
// would be a second one to keep correct.

var (
	_ Query          = (*File)(nil)
	_ MetadataReader = (*File)(nil)
)

// QueryCheckpoints pages over checkpoint summaries.
func (f *File) QueryCheckpoints(ctx context.Context, opts ListOptions) (MetadataPage, error) {
	limit, err := normalizeQueryLimit(opts.Limit)
	if err != nil {
		return MetadataPage{}, err
	}
	items, page, err := pinnedList(f.projections, cursorDomainCheckpoint, opts, func() ([]CheckpointMetadata, error) {
		return f.filteredCheckpointMetadata(ctx, opts)
	})
	if err != nil {
		return MetadataPage{}, err
	}
	return checkpointMetadataPage(items, page, limit)
}

// ListSessions pages over the threads that have at least one visible snapshot.
func (f *File) ListSessions(ctx context.Context, opts ListOptions) (SessionPage, error) {
	limit, err := normalizeQueryLimit(opts.Limit)
	if err != nil {
		return SessionPage{}, err
	}
	items, page, err := pinnedList(f.projections, cursorDomainSession, opts, func() ([]SessionSummary, error) {
		return f.filteredSessionSummaries(ctx, opts)
	})
	if err != nil {
		return SessionPage{}, err
	}
	return sessionSummaryPage(items, page, limit)
}

// ReadCheckpointMetadata looks one summary up by identity, in the thread the Ref names.
func (f *File) ReadCheckpointMetadata(ctx context.Context, ref Ref) (CheckpointMetadata, bool, error) {
	if ref.CheckpointID == "" {
		return CheckpointMetadata{}, false, nil
	}
	summaries, err := f.threadMetadata(ctx, ref.ThreadID)
	if err != nil {
		return CheckpointMetadata{}, false, err
	}
	for _, metadata := range summaries {
		if metadata.Ref.CheckpointID == ref.CheckpointID {
			return cloneCheckpointMetadata(metadata), true, nil
		}
	}
	return CheckpointMetadata{}, false, nil
}

// ReadSessionHead returns the summary of the snapshot a thread committed last.
func (f *File) ReadSessionHead(ctx context.Context, threadID string) (CheckpointMetadata, bool, error) {
	summaries, err := f.threadMetadata(ctx, threadID)
	if err != nil {
		return CheckpointMetadata{}, false, err
	}
	if len(summaries) == 0 {
		return CheckpointMetadata{}, false, nil
	}
	return cloneCheckpointMetadata(summaries[len(summaries)-1]), true, nil
}

// filteredCheckpointMetadata collects the summaries the filters select across the threads
// this query spans, in the order the walk will use.
func (f *File) filteredCheckpointMetadata(ctx context.Context, opts ListOptions) ([]CheckpointMetadata, error) {
	threads, err := f.queryThreads(opts)
	if err != nil {
		return nil, err
	}
	var items []CheckpointMetadata
	for _, threadID := range threads {
		summaries, err := f.threadMetadata(ctx, threadID)
		if err != nil {
			return nil, err
		}
		for _, metadata := range summaries {
			if opts.matches(metadata) {
				items = append(items, cloneCheckpointMetadata(metadata))
			}
		}
	}
	orderCheckpointMetadata(items, opts.ThreadID != "")
	return items, nil
}

func (f *File) filteredSessionSummaries(ctx context.Context, opts ListOptions) ([]SessionSummary, error) {
	threads, err := f.queryThreads(opts)
	if err != nil {
		return nil, err
	}
	var items []SessionSummary
	for _, threadID := range threads {
		summaries, err := f.threadMetadata(ctx, threadID)
		if err != nil {
			return nil, err
		}
		if len(summaries) == 0 {
			continue
		}
		summary := SessionSummary{ThreadID: threadID, Head: cloneCheckpointMetadata(summaries[len(summaries)-1])}
		if opts.matches(summary.Head) {
			items = append(items, summary)
		}
	}
	orderSessionSummaries(items)
	return items, nil
}

// queryThreads names the threads a query spans: the one the caller asked for, or every
// thread that has a journal. Sidecars, directories and any other file in the store directory
// are not threads.
func (f *File) queryThreads(opts ListOptions) ([]string, error) {
	if opts.ThreadID != "" {
		if err := core.CheckThreadID(opts.ThreadID); err != nil {
			return nil, err
		}
		return []string{opts.ThreadID}, nil
	}
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list threads: %w", err)
	}
	var threads []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, metadataSidecarSuffix) || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		threadID := strings.TrimSuffix(name, ".jsonl")
		if core.CheckThreadID(threadID) != nil {
			continue // not a file this backend wrote
		}
		threads = append(threads, threadID)
	}
	return threads, nil
}
