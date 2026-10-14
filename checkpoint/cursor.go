package checkpoint

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A page cursor is a versioned, opaque token carrying three things the store needs to keep a
// walk stable: which snapshot projection it belongs to, which filters that projection was
// built for, and how far the walk has got (ADR-0034 D7).
//
// The projection is a frozen copy of the filtered, ordered summaries taken when the first
// page was served. Later commits cannot enter a walk already in progress, and an update
// cannot move a row out of the page the caller has not reached yet — which is the whole
// reason an offset into a live list is not a cursor. The projection is also bounded in both
// time and count, so a store that is used for paging never grows without limit.

const (
	// cursorFormatVersion is the token layout this build writes and accepts. A token
	// naming another version is not guessed at: it is ErrInvalidCursor.
	cursorFormatVersion = 1

	// CursorTTL is how long a pinned projection outlives the page that created it.
	CursorTTL = 15 * time.Minute

	// MaxPinnedProjections is how many projections one store keeps at once. Beyond that
	// the oldest is dropped, and its cursors report ErrCursorExpired rather than
	// silently reading a different list.
	MaxPinnedProjections = 128
)

// The two paging domains are kept apart by the filter fingerprint, so a checkpoint cursor
// cannot be replayed against a session list and vice versa.
const (
	cursorDomainCheckpoint = "checkpoint"
	cursorDomainSession    = "session"
)

// cursorPayload is the decoded form of a token.
type cursorPayload struct {
	Version    int
	Generation uint64
	Filter     string
	Offset     int
}

// encodeCursor renders a token as base64url("version.generation.filter.offset"). It is
// opaque to callers but readable by a test that needs to forge one deliberately.
func encodeCursor(payload cursorPayload) string {
	raw := fmt.Sprintf("%d.%d.%s.%d", payload.Version, payload.Generation, payload.Filter, payload.Offset)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a token and rejects every shape this protocol does not speak.
func decodeCursor(token string) (cursorPayload, error) {
	invalid := func(reason string, err error) error {
		if err != nil {
			return fmt.Errorf("checkpoint: %s cursor token: %v: %w", reason, err, ErrInvalidCursor)
		}
		return fmt.Errorf("checkpoint: %s cursor token: %w", reason, ErrInvalidCursor)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursorPayload{}, invalid("undecodable", err)
	}
	parts := strings.Split(string(raw), ".")
	if len(parts) != 4 {
		return cursorPayload{}, invalid("malformed", nil)
	}
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return cursorPayload{}, invalid("bad version", err)
	}
	if version != cursorFormatVersion {
		return cursorPayload{}, invalid(fmt.Sprintf("unsupported version %d", version), nil)
	}
	generation, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return cursorPayload{}, invalid("bad generation", err)
	}
	if generation == 0 {
		return cursorPayload{}, invalid("generation zero is reserved", nil)
	}
	offset, err := strconv.Atoi(parts[3])
	if err != nil {
		return cursorPayload{}, invalid("bad offset", err)
	}
	if offset < 0 {
		return cursorPayload{}, invalid(fmt.Sprintf("negative offset %d", offset), nil)
	}
	return cursorPayload{Version: version, Generation: generation, Filter: parts[2], Offset: offset}, nil
}

// cursorFingerprint is the canonical identity of a filter set within one paging domain. The
// page size is deliberately excluded: position is an index into the pinned list, so a caller
// may ask for smaller or larger pages while walking the same projection.
func cursorFingerprint(domain string, opts ListOptions) string {
	var builder strings.Builder
	write := func(field, value string) {
		builder.WriteString(field)
		builder.WriteByte('=')
		builder.WriteString(value)
		builder.WriteByte(0)
	}
	write("domain", domain)
	write("thread", opts.ThreadID)
	write("run", opts.RunID)
	write("kind", opts.CompletionKind)
	write("from", canonicalCursorTime(opts.CreatedFrom))
	write("before", canonicalCursorTime(opts.CreatedBefore))
	digest := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(digest[:16])
}

func canonicalCursorTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// cursorFilterMatch is the binding a page walk has to honour: the token carries the
// fingerprint of the query that minted it, and resuming under any other filter set — or under
// the other paging domain — is rejected instead of quietly paging a different list.
func cursorFilterMatch(payload cursorPayload, filter string) error {
	if payload.Filter != filter {
		return fmt.Errorf("checkpoint: cursor was issued for a different query: %w", ErrInvalidCursor)
	}
	return nil
}

// pinnedProjection is one frozen page walk. items is []CheckpointMetadata or
// []SessionSummary; only the matching domain can reach it, because the domain is part of the
// filter fingerprint a token has to carry.
type pinnedProjection struct {
	items     any
	expiresAt time.Time
}

// projectionCache holds the pinned projections of one store, bounded by count and age.
type projectionCache struct {
	mu          sync.Mutex
	ttl         time.Duration
	limit       int
	now         func() time.Time
	generations map[uint64]*pinnedProjection
	order       []uint64
	next        uint64
}

func newProjectionCache(ttl time.Duration, limit int, now func() time.Time) *projectionCache {
	return &projectionCache{
		ttl:         ttl,
		limit:       limit,
		now:         now,
		generations: map[uint64]*pinnedProjection{},
	}
}

// pin freezes one filtered list and returns its generation. Projections past the limit drop
// the oldest, whose cursors then expire recognizably instead of paging a different list.
func (c *projectionCache) pin(items any) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	generation := c.next
	c.generations[generation] = &pinnedProjection{
		items:     items,
		expiresAt: c.now().UTC().Add(c.ttl),
	}
	c.order = append(c.order, generation)
	for len(c.order) > c.limit {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.generations, oldest)
	}
	return generation
}

// resolve looks one projection up. A generation that is gone, or one whose time is up, is
// ErrCursorExpired: the token was honest, the store simply no longer holds that snapshot.
func (c *projectionCache) resolve(generation uint64) (*pinnedProjection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pinned, ok := c.generations[generation]
	if !ok {
		return nil, fmt.Errorf("checkpoint: cursor generation %d is no longer held: %w", generation, ErrCursorExpired)
	}
	if !c.now().UTC().Before(pinned.expiresAt) {
		c.dropLocked(generation)
		return nil, fmt.Errorf("checkpoint: cursor generation %d expired after %v: %w", generation, c.ttl, ErrCursorExpired)
	}
	return pinned, nil
}

func (c *projectionCache) dropLocked(generation uint64) {
	delete(c.generations, generation)
	for i, candidate := range c.order {
		if candidate == generation {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// count reports how many projections are pinned now. A test uses it to prove the cache is
// bounded rather than accumulating one list per page request.
func (c *projectionCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.generations)
}
