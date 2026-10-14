package runstore

// The run list is the reconnecting client's view: one row per run, no conversation, no
// result body. These tests run against both stores so Memory and File promise one rule set
// rather than two hand-written expectations, the same way the lifecycle contract tests do.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

const runPayloadMarker = "PAYLOAD-MARKER-4471"

var runClockBase = time.Date(2026, 10, 14, 7, 0, 0, 0, time.UTC)

// runAt is one point of the pinned clock, so a seeded record's CreatedAt is predictable.
func runAt(minutes int) time.Time { return runClockBase.Add(time.Duration(minutes) * time.Minute) }

type runQueryBackend struct {
	name  string
	store Store
	query Query
	// setNow pins the store clock. CreatedAt comes from the seeded record, so the clock only
	// decides UpdatedAt and the age of a pinned projection.
	setNow func(t *testing.T, at time.Time)
	// pinned reports how many projections this store holds right now.
	pinned func() int
	// dir is the store root for the file backend, empty for memory.
	dir string
}

type runQueryStore struct {
	name string
	open func(t *testing.T) runQueryBackend
}

func runQueryStores() []runQueryStore {
	return []runQueryStore{
		{name: "memory", open: openMemoryRunBackend},
		{name: "file", open: openFileRunBackend},
	}
}

func openMemoryRunBackend(t *testing.T) runQueryBackend {
	t.Helper()
	m := NewMemory()
	setNow := func(t *testing.T, at time.Time) {
		t.Helper()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.now = func() time.Time { return at }
		m.projections.setNow(func() time.Time { return at })
	}
	setNow(t, runClockBase)
	return runQueryBackend{
		name:   "memory",
		store:  m,
		query:  m,
		setNow: setNow,
		pinned: func() int { return m.projections.count() },
	}
}

func openFileRunBackend(t *testing.T) runQueryBackend {
	t.Helper()
	dir := t.TempDir()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	setNow := func(t *testing.T, at time.Time) {
		t.Helper()
		f.now = func() time.Time { return at }
		f.projections.setNow(func() time.Time { return at })
	}
	setNow(t, runClockBase)
	return runQueryBackend{
		name:   "file",
		store:  f,
		query:  f,
		setNow: setNow,
		pinned: func() int { return f.projections.count() },
		dir:    dir,
	}
}

// runSpec is one record to seed. The four statuses that need evidence get it from
// seedRun, so a caller only names the identity, the state and the creation time.
type runSpec struct {
	id     string
	thread string
	status Status
	at     time.Time
}

func seedRun(t *testing.T, s Store, spec runSpec) RunRecord {
	t.Helper()
	record := RunRecord{
		RunID:               spec.id,
		ThreadID:            spec.thread,
		TurnID:              "turn-" + spec.id,
		JobID:               "job-" + spec.id,
		Status:              spec.status,
		CreatedAt:           spec.at,
		WorkerLeaseUntil:    spec.at.Add(time.Minute),
		LatestCheckpointRef: &checkpoint.Ref{ThreadID: spec.thread, CheckpointID: "cp-" + spec.id},
		CheckpointRevision:  uint64(len(spec.id)),
	}
	switch spec.status {
	case StatusQueued, StatusRunning:
	case StatusCancelling:
		record.CancelRequestedAt = spec.at.Add(30 * time.Second)
	case StatusCancelled:
		record.CancelRequestedAt = spec.at.Add(30 * time.Second)
		record.FinishReason = FinishReasonCancelled
	case StatusFailed:
		record.FinishReason = FinishReasonFailed
		record.Error = &RunError{Code: ErrCodeExecutionFailed, Message: runPayloadMarker + " boom"}
	case StatusSucceeded:
		record.FinishReason = FinishReasonCompleted
		record.Result = settledResult(runPayloadMarker + " answer")
		record.CompletionRef = &checkpoint.Ref{ThreadID: spec.thread, CheckpointID: "done-" + spec.id}
	case StatusPaused:
		record.FinishReason = FinishReasonPaused
	}
	created, err := s.Create(context.Background(), record)
	if err != nil {
		t.Fatalf("create run %q: %v", spec.id, err)
	}
	return created
}

// seedEightRuns is the fixed data set the paging tests walk: two threads, every status, one
// deliberate CreatedAt tie so the RunID tie-breaker has to decide the order.
func seedEightRuns(t *testing.T, s Store) []string {
	t.Helper()
	specs := []runSpec{
		{id: "r-2", thread: "alpha", status: StatusRunning, at: runAt(1)},
		{id: "r-1", thread: "alpha", status: StatusQueued, at: runAt(2)},
		{id: "r-3", thread: "beta", status: StatusSucceeded, at: runAt(3)},
		{id: "r-4", thread: "alpha", status: StatusFailed, at: runAt(4)},
		{id: "r-5", thread: "beta", status: StatusCancelling, at: runAt(5)},
		{id: "r-6", thread: "alpha", status: StatusPaused, at: runAt(6)},
		{id: "r-8", thread: "beta", status: StatusQueued, at: runAt(7)},
		{id: "r-7", thread: "alpha", status: StatusCancelled, at: runAt(7)},
	}
	order := make([]string, 0, len(specs))
	for _, spec := range specs {
		seedRun(t, s, spec)
		order = append(order, spec.id)
	}
	return order
}

func runIDs(items []RunSummary) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.RunID)
	}
	return out
}

func sameRunIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestRunListOrderingAndFilters(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			ctx := context.Background()

			// Cross-thread order is (CreatedAt, RunID): the two records made at the same
			// minute are separated by their ids, not by insertion order.
			full, err := b.query.ListRuns(ctx, RunListOptions{})
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			want := []string{"r-2", "r-1", "r-3", "r-4", "r-5", "r-6", "r-7", "r-8"}
			if got := runIDs(full.Items); !sameRunIDs(got, want) {
				t.Fatalf("order = %v, want %v", got, want)
			}
			if full.NextCursor != "" {
				t.Errorf("a page that held every row still handed out a cursor: %q", full.NextCursor)
			}

			cases := []struct {
				name string
				opts RunListOptions
				want []string
			}{
				{"thread", RunListOptions{ThreadID: "beta"}, []string{"r-3", "r-5", "r-8"}},
				{"run id", RunListOptions{RunID: "r-4"}, []string{"r-4"}},
				{"status running", RunListOptions{Status: StatusRunning}, []string{"r-2"}},
				{"status queued", RunListOptions{Status: StatusQueued}, []string{"r-1", "r-8"}},
				{"status cancelled", RunListOptions{Status: StatusCancelled}, []string{"r-7"}},
				{"from is inclusive", RunListOptions{CreatedFrom: runAt(6)}, []string{"r-6", "r-7", "r-8"}},
				{"before is exclusive", RunListOptions{CreatedBefore: runAt(3)}, []string{"r-2", "r-1"}},
				{"half open interval", RunListOptions{CreatedFrom: runAt(3), CreatedBefore: runAt(6)}, []string{"r-3", "r-4", "r-5"}},
				{"thread and status", RunListOptions{ThreadID: "alpha", Status: StatusPaused}, []string{"r-6"}},
				{"no match", RunListOptions{ThreadID: "gamma"}, []string{}},
			}
			for _, tc := range cases {
				page, err := b.query.ListRuns(ctx, tc.opts)
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				if got := runIDs(page.Items); !sameRunIDs(got, tc.want) {
					t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				}
			}
		})
	}
}

func TestRunListPagesAreFrozen(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			ctx := context.Background()

			first, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3})
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			if got := runIDs(first.Items); !sameRunIDs(got, []string{"r-2", "r-1", "r-3"}) {
				t.Fatalf("first page = %v", got)
			}

			// A new run and a state change land while the walk is in progress. Neither may
			// move a row this caller has not reached yet.
			seedRun(t, b.store, runSpec{id: "r-9", thread: "gamma", status: StatusRunning, at: runAt(0)})
			// r-8 sits at position seven of the walk, a row this caller has not reached. A
			// worker claiming it must not move it under the walk.
			queued, err := b.store.Get(ctx, "r-8")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			queued.Status = StatusRunning
			queued.WorkerID = "worker-1"
			queued.WorkerEpoch = 1
			if _, err := b.store.CompareAndSwap(ctx, queued.RunID, queued.Version, queued); err != nil {
				t.Fatalf("cas: %v", err)
			}

			var walked []RunSummary
			page := first
			for {
				walked = append(walked, page.Items...)
				if page.NextCursor == "" {
					break
				}
				next, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: page.NextCursor})
				if err != nil {
					t.Fatalf("resume: %v", err)
				}
				page = next
			}
			want := []string{"r-2", "r-1", "r-3", "r-4", "r-5", "r-6", "r-7", "r-8"}
			if got := runIDs(walked); !sameRunIDs(got, want) {
				t.Fatalf("walked = %v, want %v", got, want)
			}
			for _, item := range walked {
				if item.RunID == "r-8" && item.Status != StatusQueued {
					t.Errorf("frozen row r-8 reports status %q, want the status the walk pinned", item.Status)
				}
			}

			// A fresh query sees both changes.
			fresh, err := b.query.ListRuns(ctx, RunListOptions{})
			if err != nil {
				t.Fatalf("fresh query: %v", err)
			}
			if len(fresh.Items) != 9 {
				t.Errorf("fresh query rows = %d, want 9", len(fresh.Items))
			}
			for _, item := range fresh.Items {
				if item.RunID == "r-8" && (item.Status != StatusRunning || item.WorkerID != "worker-1") {
					t.Errorf("fresh query still reports r-8 as %q by %q", item.Status, item.WorkerID)
				}
			}
		})
	}
}

func TestRunListSummariesCarryNoPayload(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			page, err := b.query.ListRuns(context.Background(), RunListOptions{})
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			encoded, err := json.Marshal(page)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			// The result body and the failure text live only in the record, never in a row.
			if strings.Contains(string(encoded), runPayloadMarker) {
				t.Fatalf("page JSON carries a payload marker: %s", encoded)
			}
			if strings.Contains(string(encoded), `"messages"`) || strings.Contains(string(encoded), `"result"`) {
				t.Fatalf("page JSON carries a result body: %s", encoded)
			}
			for _, item := range page.Items {
				if item.Status == StatusSucceeded && !item.HasResult {
					t.Errorf("run %q succeeded but the row says there is no result", item.RunID)
				}
				if item.Status == StatusFailed && item.ErrorCode != ErrCodeExecutionFailed {
					t.Errorf("run %q failed but the row carries error code %q", item.RunID, item.ErrorCode)
				}
			}

			// A row type that cannot hold a body is what keeps this true as fields are added.
			assertNoPayloadTypes(t, reflect.TypeOf(RunSummary{}))
		})
	}
}

// assertNoPayloadTypes walks a summary type and refuses anything that could carry a
// conversation, a message or a result body.
func assertNoPayloadTypes(t *testing.T, typ reflect.Type) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array, reflect.Map:
		assertNoPayloadTypes(t, typ.Elem())
	case reflect.Struct:
		if typ.PkgPath() == "github.com/jiujuan/goagent/core" {
			t.Errorf("summary reaches into core at %s; a list row carries identity and state only", typ)
			return
		}
		if typ.PkgPath() == "github.com/jiujuan/goagent/checkpoint" && typ.Name() != "Ref" {
			t.Errorf("summary reaches into checkpoint at %s; only a Ref may locate a snapshot", typ)
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			assertNoPayloadTypes(t, typ.Field(i).Type)
		}
	}
}

func TestRunListLimitAndEmptyResult(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			ctx := context.Background()

			empty, err := b.query.ListRuns(ctx, RunListOptions{})
			if err != nil {
				t.Fatalf("empty store: %v", err)
			}
			if len(empty.Items) != 0 || empty.Items == nil || empty.NextCursor != "" {
				t.Fatalf("empty store = %#v", empty)
			}
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: MaxRunListLimit + 1}); !errors.Is(err, ErrInvalidQuery) {
				t.Errorf("limit above the maximum: err = %v, want ErrInvalidQuery", err)
			}
			if _, err := b.query.ListRuns(ctx, RunListOptions{Cursor: "not a token"}); !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("garbage cursor: err = %v, want ErrInvalidCursor", err)
			}

			seedEightRuns(t, b.store)
			if page, err := b.query.ListRuns(ctx, RunListOptions{}); err != nil || page.NextCursor != "" {
				t.Errorf("default page should hold eight rows: %v, %v", err, page.NextCursor)
			}
			if page, err := b.query.ListRuns(ctx, RunListOptions{Limit: MaxRunListLimit}); err != nil || len(page.Items) != 8 {
				t.Errorf("limit at the maximum: %v, %d rows", err, len(page.Items))
			}
		})
	}
}

func TestRunListRejectsForeignCursors(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			ctx := context.Background()

			first, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3})
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			token := first.NextCursor
			if token == "" {
				t.Fatal("a three-row page of eight rows must hand out a cursor")
			}
			payload, err := decodeRunCursor(token)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			// Each forged token changes one field of a real token, so the refusal can only
			// come from the field that moved.
			for _, forged := range []struct {
				name   string
				token  string
				reason error
			}{
				{"unknown version", encodeRunCursor(runCursor{Version: runCursorFormatVersion + 1, Generation: payload.Generation, Filter: payload.Filter, Offset: payload.Offset}), ErrInvalidCursor},
				{"reserved generation", encodeRunCursor(runCursor{Version: runCursorFormatVersion, Generation: 0, Filter: payload.Filter, Offset: payload.Offset}), ErrInvalidCursor},
				{"negative offset", encodeRunCursor(runCursor{Version: runCursorFormatVersion, Generation: payload.Generation, Filter: payload.Filter, Offset: -1}), ErrInvalidCursor},
			} {
				if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: forged.token}); !errors.Is(err, forged.reason) {
					t.Errorf("%s: err = %v, want %v", forged.name, err, forged.reason)
				}
			}
			// A token that decodes but is not laid out as this protocol says is refused too:
			// the field count is checked, not just the base64.
			tooManyParts := base64.RawURLEncoding.EncodeToString([]byte("1." + fmt.Sprint(payload.Generation) + "." + payload.Filter + ".0.0"))
			for _, broken := range []string{"!!!", "a.b.c", tooManyParts} {
				if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: broken}); !errors.Is(err, ErrInvalidCursor) {
					t.Errorf("token %q: err = %v, want ErrInvalidCursor", broken, err)
				}
			}
			// "" is not an error: an empty cursor starts a new query.
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: ""}); err != nil {
				t.Errorf("empty cursor: %v", err)
			}

			// The same token under another filter pages a different list, so it is refused.
			for _, opts := range []RunListOptions{
				{Limit: 3, ThreadID: "alpha", Cursor: token},
				{Limit: 3, Status: StatusRunning, Cursor: token},
				{Limit: 3, RunID: "r-3", Cursor: token},
				{Limit: 3, CreatedFrom: runAt(2), Cursor: token},
				{Limit: 3, CreatedBefore: runAt(6), Cursor: token},
			} {
				if _, err := b.query.ListRuns(ctx, opts); !errors.Is(err, ErrInvalidCursor) {
					t.Errorf("foreign filter %+v: err = %v, want ErrInvalidCursor", opts, err)
				}
			}
			// The page size is deliberately not part of the binding: a caller may shrink or
			// grow its pages while walking the same projection.
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 5, Cursor: token}); err != nil {
				t.Errorf("other page size: %v", err)
			}

			// A position past the end of the frozen list is a broken token, not an empty page.
			past := encodeRunCursor(runCursor{
				Version:    runCursorFormatVersion,
				Generation: payload.Generation,
				Filter:     payload.Filter,
				Offset:     len(seedEightRunWantedIDs()) + 4,
			})
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: past}); !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("offset past the end: err = %v, want ErrInvalidCursor", err)
			}
		})
	}
}

func seedEightRunWantedIDs() []string {
	return []string{"r-2", "r-1", "r-3", "r-4", "r-5", "r-6", "r-7", "r-8"}
}

func TestRunListCursorExpiry(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			ctx := context.Background()

			first, err := b.query.ListRuns(ctx, RunListOptions{Limit: 2})
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			token := first.NextCursor

			// Aging the store clock past the projection's lifetime expires the walk; a fresh
			// query still works.
			b.setNow(t, runClockBase.Add(runCursorTTL+time.Minute))
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 2, Cursor: token}); !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("aged cursor: err = %v, want ErrCursorExpired", err)
			}
			if page, err := b.query.ListRuns(ctx, RunListOptions{Limit: 2}); err != nil || len(page.Items) != 2 {
				t.Fatalf("query after expiry: %v, %#v", err, page)
			}

			// A store asked to hold more projections than its cap drops the oldest, and that
			// walk expires recognizably instead of paging a different list.
			b.setNow(t, runClockBase)
			oldest, err := b.query.ListRuns(ctx, RunListOptions{Limit: 1})
			if err != nil {
				t.Fatalf("oldest walk: %v", err)
			}
			for i := 0; i < maxPinnedRunProjections; i++ {
				if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 1, ThreadID: fmt.Sprintf("thread-%d", i)}); err != nil {
					t.Fatalf("filling the cache: %v", err)
				}
			}
			if got := b.pinned(); got != maxPinnedRunProjections {
				t.Fatalf("pinned projections = %d, want the cap %d", got, maxPinnedRunProjections)
			}
			if _, err := b.query.ListRuns(ctx, RunListOptions{Limit: 1, Cursor: oldest.NextCursor}); !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("evicted cursor: err = %v, want ErrCursorExpired", err)
			}
		})
	}
}

func TestRunListRowsAreCopies(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedEightRuns(t, b.store)
			ctx := context.Background()

			first, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3})
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			// A caller that rewrites what it got back cannot change the frozen walk, and
			// cannot reach the store's own records either.
			for i := range first.Items {
				first.Items[i].RunID = "scribbled"
				if first.Items[i].LatestCheckpointRef != nil {
					first.Items[i].LatestCheckpointRef.CheckpointID = "scribbled"
				}
				first.Items[i].Status = StatusQueued
			}
			second, err := b.query.ListRuns(ctx, RunListOptions{Limit: 3, Cursor: first.NextCursor})
			if err != nil {
				t.Fatalf("second page: %v", err)
			}
			if got := runIDs(second.Items); !sameRunIDs(got, []string{"r-4", "r-5", "r-6"}) {
				t.Fatalf("second page after scribbling = %v", got)
			}
			record, err := b.store.Get(ctx, "r-2")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if record.LatestCheckpointRef == nil || record.LatestCheckpointRef.CheckpointID != "cp-r-2" {
				t.Fatalf("stored record reached through a page row: %#v", record.LatestCheckpointRef)
			}
		})
	}
}

func TestRunListOptionsTimeBoundsSkipUnknownTime(t *testing.T) {
	// A record with no creation time is only reachable by hand — the store always stamps one
	// — so the rule is checked on the filter itself: no bound matches it, any bound excludes it.
	unknown := RunSummary{RunID: "r-0", ThreadID: "alpha", Status: StatusQueued}
	known := RunSummary{RunID: "r-1", ThreadID: "alpha", Status: StatusQueued, CreatedAt: runAt(4)}

	for _, tc := range []struct {
		name string
		opts RunListOptions
		pass bool
	}{
		{"no bounds", RunListOptions{}, true},
		{"from", RunListOptions{CreatedFrom: runAt(1)}, false},
		{"before", RunListOptions{CreatedBefore: runAt(9)}, false},
	} {
		if got := tc.opts.matches(unknown); got != tc.pass {
			t.Errorf("%s: matches(unknown time) = %v, want %v", tc.name, got, tc.pass)
		}
		if !tc.opts.matches(known) && tc.name != "before" && tc.name != "from" {
			t.Errorf("%s: a known time stopped matching", tc.name)
		}
	}
	if (RunListOptions{CreatedFrom: runAt(4)}).matches(known) != true {
		t.Error("the lower bound is not inclusive")
	}
	if (RunListOptions{CreatedBefore: runAt(4)}).matches(known) != false {
		t.Error("the upper bound is not exclusive")
	}
}

func TestFileRunListEnumeration(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	f.now = func() time.Time { return runClockBase }
	ctx := context.Background()
	seedRun(t, f, runSpec{id: "r-1", thread: "alpha", status: StatusQueued, at: runAt(1)})
	seedRun(t, f, runSpec{id: "r-2", thread: "alpha", status: StatusRunning, at: runAt(2)})

	runs := filepath.Join(dir, "runs")
	// A lock anchor, a foreign file, a directory and a journal whose name is not a RunID are
	// all things a list must walk past rather than report or refuse on.
	if err := os.WriteFile(filepath.Join(runs, "r-1.lock"), []byte("lock\n"), 0o644); err != nil {
		t.Fatalf("lock file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runs, "notes.txt"), []byte("notes\n"), 0o644); err != nil {
		t.Fatalf("foreign file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runs, "not a run.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("bad name: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(runs, "subdir.jsonl"), 0o755); err != nil {
		t.Fatalf("directory: %v", err)
	}
	// A journal-shaped file whose name is a legal RunID but has no .jsonl suffix is not a run
	// this store wrote, even though reading it would decode cleanly.
	strayLine := journalEvent{
		Version:     runFormatVersion,
		Kind:        eventKindRecord,
		PrevVersion: 0,
		Record:      &RunRecord{RunID: "stray", ThreadID: "alpha", Status: StatusSucceeded, FinishReason: FinishReasonCompleted, Version: 1},
	}
	encoded, err := json.Marshal(strayLine)
	if err != nil {
		t.Fatalf("marshal stray: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runs, "stray"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("stray journal: %v", err)
	}
	// A plain file whose name would be a legal RunID must not be read as a journal either: a
	// list is allowed to look at the store, not to leave a lock anchor behind for every name
	// it found there.
	if err := os.WriteFile(filepath.Join(runs, "notes"), []byte("not a journal\n"), 0o644); err != nil {
		t.Fatalf("name-shaped file: %v", err)
	}

	page, err := f.ListRuns(ctx, RunListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := runIDs(page.Items); !sameRunIDs(got, []string{"r-1", "r-2"}) {
		t.Fatalf("listed %v, want the two runs this store wrote", got)
	}
	if _, err := os.Stat(filepath.Join(runs, "notes.lock")); err == nil {
		t.Fatal("the list took a lock for a name this store never wrote")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat notes.lock: %v", err)
	}

	// A torn tail is the last append that never finished: the record it would have published
	// is not durable, so the list reports what is.
	journal := filepath.Join(runs, "r-2.jsonl")
	tail, err := os.ReadFile(journal)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if err := os.WriteFile(journal, append(append([]byte(nil), tail...), []byte(`{"version":1,"kind":"record","prev_version":1,"record":{"run_id":"r-2","thread_id":"alpha","status":"succeeded"`)...), 0o644); err != nil {
		t.Fatalf("torn append: %v", err)
	}
	page, err = f.ListRuns(ctx, RunListOptions{})
	if err != nil {
		t.Fatalf("list with a torn tail: %v", err)
	}
	for _, item := range page.Items {
		if item.RunID == "r-2" && item.Status != StatusRunning {
			t.Fatalf("a half-written append changed the listed state to %q", item.Status)
		}
	}

	// A line that ended but cannot be replayed is corruption in the middle of a record this
	// list has to report: silently skipping the run would hide it from its owner.
	if err := os.WriteFile(journal, append(append([]byte(nil), tail...), []byte("{\"version\":1,\"kind\":\"record\",\"prev_version\":9,\"record\":{\"run_id\":\"r-2\",\"thread_id\":\"alpha\",\"status\":\"succeeded\"}}\n")...), 0o644); err != nil {
		t.Fatalf("broken append: %v", err)
	}
	if _, err := f.ListRuns(ctx, RunListOptions{}); err == nil {
		t.Fatal("a corrupt journal was skipped instead of reported")
	}
}

func TestFileRunListPageStaysPayloadFree(t *testing.T) {
	// A page over journals that each carry a frozen result must still hand back rows with no
	// body, while the record path keeps returning the body it stores.
	dir := t.TempDir()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	f.now = func() time.Time { return runClockBase }
	for i := 0; i < 6; i++ {
		seedRun(t, f, runSpec{
			id:     fmt.Sprintf("r-%d", i),
			thread: "alpha",
			status: StatusSucceeded,
			at:     runAt(i),
		})
	}
	page, err := f.ListRuns(context.Background(), RunListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("page = %d rows", len(page.Items))
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), runPayloadMarker) {
		t.Fatalf("file list carried a result body: %s", encoded)
	}
	record, err := f.Get(context.Background(), "r-3")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if record.Result == nil || !strings.Contains(coreText(record), runPayloadMarker) {
		t.Fatalf("record lost its result: %#v", record.Result)
	}
}

func coreText(r RunRecord) string {
	if r.Result == nil {
		return ""
	}
	encoded, err := json.Marshal(r.Result)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// BenchmarkRunListPages measures a page of 50 rows taken from a store holding 500 runs. The
// file number is the cost of replaying every journal a query has to enumerate; the memory
// number is what a sidecar-style projection would have to beat.
func BenchmarkRunListPages(b *testing.B) {
	for _, backend := range []string{"memory", "file"} {
		b.Run(backend, func(b *testing.B) {
			store := benchRunStore(b, backend, 500)
			q := store.(Query)
			warm, err := q.ListRuns(context.Background(), RunListOptions{Limit: 50})
			if err != nil {
				b.Fatalf("first page: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := q.ListRuns(context.Background(), RunListOptions{Limit: 50, Cursor: warm.NextCursor}); err != nil {
					b.Fatalf("second page: %v", err)
				}
			}
		})
	}
}

func benchRunStore(b *testing.B, backend string, runs int) Store {
	b.Helper()
	var store Store
	switch backend {
	case "memory":
		m := NewMemory()
		m.now = func() time.Time { return runClockBase }
		store = m
	case "file":
		f, err := NewFile(b.TempDir())
		if err != nil {
			b.Fatalf("NewFile: %v", err)
		}
		f.now = func() time.Time { return runClockBase }
		store = f
	}
	for i := 0; i < runs; i++ {
		status := StatusQueued
		if i%4 == 0 {
			status = StatusSucceeded
		}
		record := RunRecord{
			RunID:     fmt.Sprintf("run-%04d", i),
			ThreadID:  fmt.Sprintf("thread-%03d", i%20),
			Status:    status,
			CreatedAt: runAt(i),
		}
		if status == StatusSucceeded {
			record.FinishReason = FinishReasonCompleted
			record.Result = settledResult("answer")
		}
		if _, err := store.Create(context.Background(), record); err != nil {
			b.Fatalf("seed %d: %v", i, err)
		}
	}
	return store
}
