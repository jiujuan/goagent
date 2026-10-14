package checkpoint

import (
	"time"

	"github.com/jiujuan/goagent/internal/paging"
)

// A page cursor is a versioned, opaque token carrying three things the store needs to keep a
// walk stable: which snapshot projection it belongs to, which filters that projection was
// built for, and how far the walk has got (ADR-0034 D7).
//
// The rules themselves live in internal/paging, which the run store uses too, so the two
// stores cannot drift into accepting different tokens or expiring projections at different
// ages. What stays here is the checkpoint-specific part: the paging scopes, the fingerprint's
// field list, and the sentinel errors this package's callers match on.

const (
	// cursorFormatVersion is the token layout this build writes and accepts. A token
	// naming another version is not guessed at: it is ErrInvalidCursor.
	cursorFormatVersion = paging.FormatVersion

	// CursorTTL is how long a pinned projection outlives the page that created it.
	CursorTTL = paging.TTL

	// MaxPinnedProjections is how many projections one store keeps at once. Beyond that
	// the oldest is dropped, and its cursors report ErrCursorExpired rather than
	// silently reading a different list.
	MaxPinnedProjections = paging.MaxProjections
)

// The two paging domains are kept apart by the filter fingerprint, so a checkpoint cursor
// cannot be replayed against a session list and vice versa.
const (
	cursorDomainCheckpoint = "checkpoint"
	cursorDomainSession    = "session"
)

// checkpointPaging binds the shared paging rules to this package's error sentinels.
var checkpointPaging = paging.Domain{
	Name:          "checkpoint",
	InvalidCursor: ErrInvalidCursor,
	CursorExpired: ErrCursorExpired,
	InvalidQuery:  ErrInvalidQuery,
}

// cursorPayload is the decoded form of a token.
type cursorPayload = paging.Page

// encodeCursor renders a token as base64url("version.generation.filter.offset"). It is
// opaque to callers but readable by a test that needs to forge one deliberately.
func encodeCursor(payload cursorPayload) string {
	return checkpointPaging.Encode(payload)
}

// decodeCursor parses a token and rejects every shape this protocol does not speak.
func decodeCursor(token string) (cursorPayload, error) {
	return checkpointPaging.Decode(token)
}

// cursorFingerprint is the canonical identity of a filter set within one paging domain. The
// page size is deliberately excluded: position is an index into the pinned list, so a caller
// may ask for smaller or larger pages while walking the same projection.
func cursorFingerprint(domain string, opts ListOptions) string {
	return checkpointPaging.Fingerprint(domain,
		opts.ThreadID,
		opts.RunID,
		opts.CompletionKind,
		checkpointPaging.CanonicalTime(opts.CreatedFrom),
		checkpointPaging.CanonicalTime(opts.CreatedBefore),
	)
}

// projectionCache holds the pinned projections of one store, bounded by count and age. The
// clock stays here so a store's projection ages on the same clock as its records.
type projectionCache struct {
	core *paging.Cache
	now  func() time.Time
}

func newProjectionCache(ttl time.Duration, limit int, now func() time.Time) *projectionCache {
	return &projectionCache{core: checkpointPaging.NewCache(ttl, limit), now: now}
}

// pin freezes one filtered list and returns its generation. Projections past the limit drop
// the oldest, whose cursors then expire recognizably instead of paging a different list.
func (c *projectionCache) pin(items any) uint64 {
	return c.core.Pin(items, c.now())
}

// count reports how many projections are pinned now. A test uses it to prove the cache is
// bounded rather than accumulating one list per page request.
func (c *projectionCache) count() int {
	return c.core.Count()
}
