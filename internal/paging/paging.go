// Package paging is the one implementation of the page-walk rules the checkpoint and run
// stores share (ADR-0034 D7): a versioned opaque cursor token, the filter fingerprint that
// binds a token to the query that minted it, and the bounded cache of frozen snapshot
// projections a later page resumes from.
//
// The stores hold different kinds of summaries, so the rules — not the payload types — live
// here. Two stores promising the same cursor semantics in two hand-written copies is how the
// two drift apart: one starts accepting a token the other rejects, or one let a projection
// live past its time-to-live while the other expired it.
//
// A page walk freezes the filtered, ordered summary list when the first page is served, and
// every later page reads that same frozen copy. Committing more records cannot enter a walk
// already in progress, and an update cannot move a row out of a page the caller has not
// reached yet — which is the whole reason an offset into a live list is not a cursor. The
// cache is bounded in both age and count, so a store used for paging never grows without
// limit, and a token whose projection is gone says so instead of paging a different list.
package paging

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

const (
	// FormatVersion is the token layout this build writes and accepts. A token naming
	// another version is not guessed at: it is an invalid cursor.
	FormatVersion = 1

	// DefaultLimit is the page size a caller gets when it did not ask for one.
	DefaultLimit = 50

	// MaxLimit is the largest page size a caller may ask for. Asking for more is an error
	// rather than a silent clamp: a caller that believes it fetched 1000 rows and got 200
	// would compute a wrong answer, not a slow one.
	MaxLimit = 200

	// TTL is how long a pinned projection outlives the page that created it.
	TTL = 15 * time.Minute

	// MaxProjections is how many projections one store keeps at once. Beyond that the
	// oldest is dropped, and its cursors report an expired projection rather than silently
	// reading a different list.
	MaxProjections = 128
)

// Page is where a walk stands: which projection, under which filters, and how many rows the
// caller has already received. It is also the decoded form of a token, and Version is what
// lets a reader tell a token this protocol speaks from one it does not.
type Page struct {
	Version    int
	Generation uint64
	Filter     string
	Offset     int
}

// Domain names one store's use of these rules and carries the sentinel errors its callers
// match on. Errors are reported with the domain's own prefix and its own sentinels, so a
// checkpoint client never has to know this package exists.
type Domain struct {
	Name          string
	InvalidCursor error
	CursorExpired error
	InvalidQuery  error
}

// NewCache starts the pinned projections of one store.
func (d Domain) NewCache(ttl time.Duration, limit int) *Cache {
	return &Cache{
		domain:      d,
		ttl:         ttl,
		limit:       limit,
		generations: map[uint64]*pinnedProjection{},
	}
}

// NormalizeLimit turns an unset page size into the default and rejects one the protocol does
// not allow.
func (d Domain) NormalizeLimit(limit int) (int, error) {
	if limit <= 0 {
		return DefaultLimit, nil
	}
	if limit > MaxLimit {
		return 0, fmt.Errorf("%s: query limit %d exceeds the maximum %d: %w", d.Name, limit, MaxLimit, d.InvalidQuery)
	}
	return limit, nil
}

// Fingerprint is the canonical identity of one filter set within one paging scope. Values are
// positional — the caller passes the same fields in the same order on every page — and the
// page size is deliberately excluded: position is an index into the pinned list, so a caller
// may ask for smaller or larger pages while walking the same projection.
func (d Domain) Fingerprint(scope string, values ...string) string {
	var builder strings.Builder
	builder.WriteString(d.Name)
	builder.WriteByte(0)
	builder.WriteString(scope)
	builder.WriteByte(0)
	for i, value := range values {
		builder.WriteString(strconv.Itoa(i))
		builder.WriteByte('=')
		builder.WriteString(value)
		builder.WriteByte(0)
	}
	digest := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(digest[:16])
}

// CanonicalTime renders a time bound so two callers filtering at the same instant hash to the
// same fingerprint, and an unset bound stays the empty string.
func (d Domain) CanonicalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Encode renders a token as base64url("version.generation.filter.offset"). It is opaque to
// callers but readable by a test that needs to forge one deliberately.
func (d Domain) Encode(page Page) string {
	raw := fmt.Sprintf("%d.%d.%s.%d", page.Version, page.Generation, page.Filter, page.Offset)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// Decode parses a token and rejects every shape this protocol does not speak.
func (d Domain) Decode(token string) (Page, error) {
	invalid := func(reason string, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %s cursor token: %v: %w", d.Name, reason, err, d.InvalidCursor)
		}
		return fmt.Errorf("%s: %s cursor token: %w", d.Name, reason, d.InvalidCursor)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Page{}, invalid("undecodable", err)
	}
	parts := strings.Split(string(raw), ".")
	if len(parts) != 4 {
		return Page{}, invalid("malformed", nil)
	}
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return Page{}, invalid("bad version", err)
	}
	if version != FormatVersion {
		return Page{}, invalid(fmt.Sprintf("unsupported version %d", version), nil)
	}
	generation, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return Page{}, invalid("bad generation", err)
	}
	if generation == 0 {
		return Page{}, invalid("generation zero is reserved", nil)
	}
	offset, err := strconv.Atoi(parts[3])
	if err != nil {
		return Page{}, invalid("bad offset", err)
	}
	if offset < 0 {
		return Page{}, invalid(fmt.Sprintf("negative offset %d", offset), nil)
	}
	return Page{Version: version, Generation: generation, Filter: parts[2], Offset: offset}, nil
}

// pinnedProjection is one frozen page walk. items is the store's own summary slice; only a
// caller that asked for the same scope and filters can reach it, because both are part of the
// fingerprint a token has to carry.
type pinnedProjection struct {
	items     any
	expiresAt time.Time
}

// Cache holds the pinned projections of one store, bounded by count and age.
type Cache struct {
	domain      Domain
	ttl         time.Duration
	limit       int
	mu          sync.Mutex
	generations map[uint64]*pinnedProjection
	order       []uint64
	next        uint64
}

// pin freezes one filtered list and returns its generation. Projections past the limit drop
// the oldest, whose cursors then expire recognizably instead of paging a different list.
func (c *Cache) Pin(items any, now time.Time) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	generation := c.next
	c.generations[generation] = &pinnedProjection{
		items:     items,
		expiresAt: now.UTC().Add(c.ttl),
	}
	c.order = append(c.order, generation)
	for len(c.order) > c.limit {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.generations, oldest)
	}
	return generation
}

// resolve looks one projection up. A generation that is gone, or one whose time is up, is the
// domain's expired error: the token was honest, the store simply no longer holds that
// snapshot.
func (c *Cache) resolve(generation uint64, now time.Time) (*pinnedProjection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pinned, ok := c.generations[generation]
	if !ok {
		return nil, fmt.Errorf("%s: cursor generation %d is no longer held: %w", c.domain.Name, generation, c.domain.CursorExpired)
	}
	if !now.UTC().Before(pinned.expiresAt) {
		c.dropLocked(generation)
		return nil, fmt.Errorf("%s: cursor generation %d expired after %v: %w", c.domain.Name, generation, c.ttl, c.domain.CursorExpired)
	}
	return pinned, nil
}

func (c *Cache) dropLocked(generation uint64) {
	delete(c.generations, generation)
	for i, candidate := range c.order {
		if candidate == generation {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// Count reports how many projections are pinned now. A test uses it to prove the cache is
// bounded rather than accumulating one list per page request.
func (c *Cache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.generations)
}

// Resume returns the summary list one page walk reads. With an empty cursor it builds the
// list, freezes it, and reports the generation it pinned; with a token it resumes exactly the
// list that token was minted for, so rows committed meanwhile cannot join a walk already
// under way. now is taken per call so a store's clock stays injectable.
func Resume[T any](c *Cache, now time.Time, filter, cursor string, build func() ([]T, error)) ([]T, Page, error) {
	if cursor == "" {
		items, err := build()
		if err != nil {
			return nil, Page{}, err
		}
		return items, Page{Version: FormatVersion, Generation: c.Pin(items, now), Filter: filter, Offset: 0}, nil
	}
	page, err := c.domain.Decode(cursor)
	if err != nil {
		return nil, Page{}, err
	}
	if page.Filter != filter {
		return nil, Page{}, fmt.Errorf("%s: cursor was issued for a different query: %w", c.domain.Name, c.domain.InvalidCursor)
	}
	pinned, err := c.resolve(page.Generation, now)
	if err != nil {
		return nil, Page{}, err
	}
	items, ok := pinned.items.([]T)
	if !ok {
		return nil, Page{}, fmt.Errorf("%s: cursor generation %d pins %T, not the expected summary list: %w",
			c.domain.Name, page.Generation, pinned.items, c.domain.InvalidCursor)
	}
	return items, page, nil
}

// Slice takes one page out of a frozen list and hands back copies, so a caller that edits a
// returned summary cannot rewrite what the next page reads. nextCursor is empty when the page
// was the last one, so a caller never has to fetch an empty page to learn the walk finished.
func Slice[T any](d Domain, items []T, page Page, limit int, clone func(T) T) (out []T, nextCursor string, err error) {
	if page.Offset > len(items) {
		return nil, "", fmt.Errorf("%s: cursor position %d is past the end of a %d-row projection: %w",
			d.Name, page.Offset, len(items), d.InvalidCursor)
	}
	end := page.Offset + limit
	if end > len(items) {
		end = len(items)
	}
	out = make([]T, 0, end-page.Offset)
	for _, item := range items[page.Offset:end] {
		if clone != nil {
			item = clone(item)
		}
		out = append(out, item)
	}
	if end < len(items) {
		next := page
		next.Offset = end
		nextCursor = d.Encode(next)
	}
	return out, nextCursor, nil
}
