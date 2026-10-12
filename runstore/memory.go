package runstore

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Memory keeps run records for the lifetime of the process. It applies the same
// lifecycle and idempotency rules as every other Store, so a caller can be tested
// against it and run unchanged on the durable implementation.
type Memory struct {
	mu    sync.RWMutex
	byRun map[string]RunRecord
	now   func() time.Time
}

var _ Store = (*Memory)(nil)

// NewMemory constructs an empty in-memory run store.
func NewMemory() *Memory {
	return &Memory{byRun: map[string]RunRecord{}, now: storageClock}
}

// stamp takes one reading of this store's clock for one write attempt.
func (m *Memory) stamp() time.Time {
	if m.now == nil {
		return storageClock()
	}
	return m.now()
}

// Create records a new run. See Store.Create.
func (m *Memory) Create(ctx context.Context, record RunRecord) (RunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var stored *RunRecord
	if existing, ok := m.byRun[record.RunID]; ok {
		stored = &existing
	}
	created, err := prepareCreate(stored, record, m.stamp())
	if err != nil {
		return RunRecord{}, err
	}
	m.byRun[created.RunID] = created
	// The stored value shares its pointers with the caller's copy, so the caller gets a
	// fresh clone instead of it.
	return created.clone()
}

// Get reads a record without changing it. See Store.Get.
func (m *Memory) Get(ctx context.Context, runID string) (RunRecord, error) {
	if err := CheckRunID(runID); err != nil {
		return RunRecord{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	record, ok := m.byRun[runID]
	if !ok {
		return RunRecord{}, runNotFound(runID)
	}
	return record.clone()
}

// CompareAndSwap replaces a record only at the version the caller read. See
// Store.CompareAndSwap.
func (m *Memory) CompareAndSwap(ctx context.Context, runID string, expectedVersion uint64, next RunRecord) (RunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var stored *RunRecord
	if existing, ok := m.byRun[runID]; ok {
		stored = &existing
	}
	updated, err := prepareCAS(stored, runID, expectedVersion, next, m.stamp())
	if err != nil {
		return RunRecord{}, err
	}
	// A terminal retry returns the stored record unchanged; writing it back is the same
	// value, so the map stays consistent either way.
	m.byRun[updated.RunID] = updated
	return updated.clone()
}

// runNotFound keeps the not-found message uniform across implementations.
func runNotFound(runID string) error {
	return fmt.Errorf("runstore: %w for run %q", ErrNotFound, runID)
}
