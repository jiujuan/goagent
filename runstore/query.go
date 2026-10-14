package runstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/internal/paging"
)

// Query is the optional read-only list over task records (ADR-0034 D7). A reconnecting
// client — a different process, a refreshed UI — uses it to answer "what is running, what
// ended, what is waiting on me" in one page, instead of reading every record it holds.
//
// A row is a projection, not a record: identity, state, times, the checkpoint references a
// run produced, and the error *codes*. The frozen result body, the failure text and the
// recording error text stay in the record, because a list of a thousand runs must not carry
// a thousand conversations. Get is still the only way to read one record whole, and it stays
// a pure read: nothing in this file reconciles, renews a lease or repairs state.
//
// Paging follows the same rules as the checkpoint summaries, from the same shared core: the
// first page freezes the filtered, ordered rows and later pages read that frozen copy, so a
// record that changes state mid-walk cannot move out from under a caller.

const (
	// DefaultRunListLimit is the page size when RunListOptions.Limit is not set.
	DefaultRunListLimit = paging.DefaultLimit

	// MaxRunListLimit is the largest page size a caller may ask for. Asking for more is an
	// error rather than a silent clamp.
	MaxRunListLimit = paging.MaxLimit

	// runCursorFormatVersion is the token layout this build writes and accepts.
	runCursorFormatVersion = paging.FormatVersion

	// runCursorTTL is how long a pinned projection outlives the page that created it.
	runCursorTTL = paging.TTL

	// maxPinnedRunProjections is how many projections one store keeps at once.
	maxPinnedRunProjections = paging.MaxProjections
)

var (
	// ErrInvalidQuery means the options themselves are unusable, such as a page size above
	// MaxRunListLimit.
	ErrInvalidQuery = errors.New("runstore: invalid run query")
	// ErrInvalidCursor means the token is not one this protocol speaks: corrupt, unknown
	// version, out-of-range position, or reused under different filters.
	ErrInvalidCursor = errors.New("runstore: invalid run cursor")
	// ErrCursorExpired means the token is well formed but the projection it pinned is gone —
	// its time-to-live ran out, or the store dropped projections to stay within its cap.
	// Start a new query; do not retry the same token.
	ErrCursorExpired = errors.New("runstore: run cursor expired")
)

// runPaging binds the shared paging rules to this package's sentinels.
var runPaging = paging.Domain{
	Name:          "runstore",
	InvalidCursor: ErrInvalidCursor,
	CursorExpired: ErrCursorExpired,
	InvalidQuery:  ErrInvalidQuery,
}

// runCursor is the decoded form of a page token.
type runCursor = paging.Page

func encodeRunCursor(cursor runCursor) string { return runPaging.Encode(cursor) }

func decodeRunCursor(token string) (runCursor, error) { return runPaging.Decode(token) }

// RunSummary is one row of the task list. Every field is something the record already
// carries; nothing here is inferred, and nothing here is the run's payload.
type RunSummary struct {
	RunID         string `json:"run_id"`
	ThreadID      string `json:"thread_id"`
	TurnID        string `json:"turn_id,omitempty"`
	JobID         string `json:"job_id,omitempty"`
	ResumeOfRunID string `json:"resume_of_run_id,omitempty"`

	// Status is the lifecycle state as the store holds it.
	Status Status `json:"status"`
	// FinishReason classifies a terminal outcome.
	FinishReason FinishReason `json:"finish_reason,omitempty"`
	// ErrorCode is the code of a failed run's error, empty otherwise. The message stays in
	// the record: a code is enough to decide what to show, and not safe enough to retry on.
	ErrorCode string `json:"error_code,omitempty"`
	// RecordErrorCode is the code carried by the terminal checkpoint record error. A run can
	// be succeeded while this says the bookkeeping failed, so a list has to show both.
	RecordErrorCode string `json:"record_error_code,omitempty"`
	// HasResult reports that the record froze a result, without the result itself.
	HasResult bool `json:"has_result,omitempty"`

	// LatestCheckpointRef, CheckpointRevision and CompletionRef locate the snapshots this run
	// produced, by reference only.
	LatestCheckpointRef *checkpoint.Ref `json:"latest_checkpoint_ref,omitempty"`
	CheckpointRevision  uint64          `json:"checkpoint_revision,omitempty"`
	CompletionRef       *checkpoint.Ref `json:"completion_ref,omitempty"`

	WorkerID    string `json:"worker_id,omitempty"`
	WorkerEpoch uint64 `json:"worker_epoch,omitempty"`

	CreatedAt         time.Time `json:"created_at,omitzero"`
	UpdatedAt         time.Time `json:"updated_at,omitzero"`
	HeartbeatAt       time.Time `json:"heartbeat_at,omitzero"`
	CancelRequestedAt time.Time `json:"cancel_requested_at,omitzero"`
	// WorkerLeaseUntil bounds ownership. Passing it makes a record stale for a human to
	// recover; the list reports the bound and never decides what it means (ADR-0034 D6).
	WorkerLeaseUntil time.Time `json:"worker_lease_until,omitzero"`

	// Version is the record's own compare-and-swap counter, so a caller can follow one run
	// across pages without confusing it with a checkpoint revision.
	Version uint64 `json:"version"`
}

// RunListOptions is the filter and paging input of ListRuns.
//
// Time bounds are the half-open interval [CreatedFrom, CreatedBefore). A record whose
// creation time is unknown is returned when no bound is set and excluded as soon as one is:
// a bound cannot be judged against a time the store never recorded. Every record the store
// publishes carries a creation time, so the unknown case is only reachable for a row a
// caller built by hand.
//
// Status is the run's lifecycle state, deliberately separate from a checkpoint's terminal
// record kind: "succeeded" here means the execution produced a result, which is not the same
// claim as "its terminal checkpoint was committed".
type RunListOptions struct {
	// Limit is the page size: zero or negative takes DefaultRunListLimit, and anything above
	// MaxRunListLimit is ErrInvalidQuery. The page size is not part of the cursor binding, so
	// a walk may change it between pages.
	Limit  int
	Cursor string

	ThreadID string
	RunID    string
	Status   Status

	CreatedFrom   time.Time
	CreatedBefore time.Time
}

// RunPage is one page of task summaries. NextCursor is empty when the page was the last one.
type RunPage struct {
	Items      []RunSummary `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// Query lists task records. Records created or changed after the first page are invisible to
// that walk; they appear to a new query.
type Query interface {
	ListRuns(ctx context.Context, opts RunListOptions) (RunPage, error)
}

// runSummaryOf projects a record onto a list row. The references are copied, so a caller
// holding a page cannot reach what the store keeps.
func runSummaryOf(record RunRecord) RunSummary {
	summary := RunSummary{
		RunID:              record.RunID,
		ThreadID:           record.ThreadID,
		TurnID:             record.TurnID,
		JobID:              record.JobID,
		ResumeOfRunID:      record.ResumeOfRunID,
		Status:             record.Status,
		FinishReason:       record.FinishReason,
		HasResult:          record.Result != nil,
		CheckpointRevision: record.CheckpointRevision,
		WorkerID:           record.WorkerID,
		WorkerEpoch:        record.WorkerEpoch,
		CreatedAt:          record.CreatedAt,
		UpdatedAt:          record.UpdatedAt,
		HeartbeatAt:        record.HeartbeatAt,
		CancelRequestedAt:  record.CancelRequestedAt,
		WorkerLeaseUntil:   record.WorkerLeaseUntil,
		Version:            record.Version,
	}
	if record.Error != nil {
		summary.ErrorCode = record.Error.Code
	}
	if record.RecordError != nil {
		summary.RecordErrorCode = record.RecordError.Code
	}
	summary.LatestCheckpointRef = cloneRef(record.LatestCheckpointRef)
	summary.CompletionRef = cloneRef(record.CompletionRef)
	return summary
}

func cloneRunSummary(summary RunSummary) RunSummary {
	summary.LatestCheckpointRef = cloneRef(summary.LatestCheckpointRef)
	summary.CompletionRef = cloneRef(summary.CompletionRef)
	return summary
}

func (o RunListOptions) hasTimeBounds() bool {
	return !o.CreatedFrom.IsZero() || !o.CreatedBefore.IsZero()
}

// matches applies every filter to one row. A time bound never matches a row whose time is
// unknown.
func (o RunListOptions) matches(summary RunSummary) bool {
	if o.ThreadID != "" && summary.ThreadID != o.ThreadID {
		return false
	}
	if o.RunID != "" && summary.RunID != o.RunID {
		return false
	}
	if o.Status != "" && summary.Status != o.Status {
		return false
	}
	if o.hasTimeBounds() {
		if summary.CreatedAt.IsZero() {
			return false
		}
		if !o.CreatedFrom.IsZero() && summary.CreatedAt.Before(o.CreatedFrom) {
			return false
		}
		if !o.CreatedBefore.IsZero() && !summary.CreatedAt.Before(o.CreatedBefore) {
			return false
		}
	}
	return true
}

// orderRunSummaries sorts a pinned list by (CreatedAt, RunID). The order is total: RunID is
// the only key, and it is the key, so two runs of the same query on the same projection
// cannot come out differently however the store enumerated them.
func orderRunSummaries(items []RunSummary) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.RunID < b.RunID
	})
}

// projectionCache holds this store's pinned run projections. The clock stays with the store
// that owns it, so a projection ages on the same clock as the records it describes.
type projectionCache struct {
	core *paging.Cache
	now  func() time.Time
}

func newProjectionCache(ttl time.Duration, limit int, now func() time.Time) *projectionCache {
	return &projectionCache{core: runPaging.NewCache(ttl, limit), now: now}
}

func (c *projectionCache) setNow(now func() time.Time) { c.now = now }

func (c *projectionCache) count() int { return c.core.Count() }

// pinnedRunList freezes the filtered rows on the first page and resumes the same frozen list
// afterwards.
func pinnedRunList(cache *projectionCache, opts RunListOptions, build func() ([]RunSummary, error)) ([]RunSummary, runCursor, error) {
	return paging.Resume(cache.core, cache.now(), runFingerprint(opts), opts.Cursor, build)
}

// runFingerprint is the canonical identity of one filter set. The page size is deliberately
// excluded: position is an index into the pinned list.
func runFingerprint(opts RunListOptions) string {
	return runPaging.Fingerprint("run",
		opts.ThreadID,
		opts.RunID,
		string(opts.Status),
		runPaging.CanonicalTime(opts.CreatedFrom),
		runPaging.CanonicalTime(opts.CreatedBefore),
	)
}

// runSummaryPage takes one page out of a pinned list and hands back copies.
func runSummaryPage(items []RunSummary, page runCursor, limit int) (RunPage, error) {
	out, nextCursor, err := paging.Slice(runPaging, items, page, limit, cloneRunSummary)
	if err != nil {
		return RunPage{}, err
	}
	return RunPage{Items: out, NextCursor: nextCursor}, nil
}

var (
	_ Query = (*Memory)(nil)
	_ Query = (*File)(nil)
)

// ListRuns pages over the task records this store holds.
func (m *Memory) ListRuns(ctx context.Context, opts RunListOptions) (RunPage, error) {
	limit, err := runPaging.NormalizeLimit(opts.Limit)
	if err != nil {
		return RunPage{}, err
	}
	items, page, err := pinnedRunList(m.projections, opts, func() ([]RunSummary, error) {
		return m.filteredRunSummaries(opts)
	})
	if err != nil {
		return RunPage{}, err
	}
	return runSummaryPage(items, page, limit)
}

func (m *Memory) filteredRunSummaries(opts RunListOptions) ([]RunSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]RunSummary, 0, len(m.byRun))
	for _, record := range m.byRun {
		summary := runSummaryOf(record)
		if opts.matches(summary) {
			items = append(items, summary)
		}
	}
	orderRunSummaries(items)
	return items, nil
}

// ListRuns pages over the journals in the runs directory. Enumerating them is the cost of a
// new query: a run record is one small journal, and replaying it is the same read Get does.
// A page walk after the first reads the frozen projection, not the directory.
func (f *File) ListRuns(ctx context.Context, opts RunListOptions) (RunPage, error) {
	limit, err := runPaging.NormalizeLimit(opts.Limit)
	if err != nil {
		return RunPage{}, err
	}
	items, page, err := pinnedRunList(f.projections, opts, func() ([]RunSummary, error) {
		return f.filteredRunSummaries(ctx, opts)
	})
	if err != nil {
		return RunPage{}, err
	}
	return runSummaryPage(items, page, limit)
}

func (f *File) filteredRunSummaries(ctx context.Context, opts RunListOptions) ([]RunSummary, error) {
	ids, err := f.queryRunIDs(opts)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	items := make([]RunSummary, 0, len(ids))
	for _, runID := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var summary RunSummary
		var matched bool
		err := f.withRunRead(ctx, runID, func(latest *RunRecord) error {
			if latest == nil {
				return nil
			}
			summary = runSummaryOf(*latest)
			matched = opts.matches(summary)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if matched {
			items = append(items, summary)
		}
	}
	orderRunSummaries(items)
	return items, nil
}

// queryRunIDs names the runs a query spans: the one the caller asked for, or every journal in
// the runs directory. Lock files, journals whose name is not a RunID, other files and
// directories are not runs.
func (f *File) queryRunIDs(opts RunListOptions) ([]string, error) {
	if opts.RunID != "" {
		if err := CheckRunID(opts.RunID); err != nil {
			return nil, err
		}
		return []string{opts.RunID}, nil
	}
	entries, err := os.ReadDir(f.runs)
	if err != nil {
		return nil, fmt.Errorf("runstore: list runs: %w", err)
	}
	var ids []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		runID := strings.TrimSuffix(name, ".jsonl")
		if CheckRunID(runID) != nil {
			continue // not a journal this backend wrote
		}
		ids = append(ids, runID)
	}
	return ids, nil
}
