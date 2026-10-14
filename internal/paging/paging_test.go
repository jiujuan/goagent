package paging

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The shared rules get their own direct evidence here. The checkpoint and run stores each
// hold contract tests over their own summaries; this file pins the machinery both of them
// depend on, so a change here cannot quietly become a change in only one of the two.

var (
	errInvalidCursor = errors.New("test: invalid cursor")
	errCursorExpired = errors.New("test: cursor expired")
	errInvalidQuery  = errors.New("test: invalid query")
)

func testDomain(name string) Domain {
	return Domain{Name: name, InvalidCursor: errInvalidCursor, CursorExpired: errCursorExpired, InvalidQuery: errInvalidQuery}
}

func at(seconds int) time.Time {
	return time.Date(2026, 10, 14, 9, 0, seconds, 0, time.UTC)
}

func TestTokenRoundTripAndTamper(t *testing.T) {
	d := testDomain("test")
	page := Page{Version: FormatVersion, Generation: 7, Filter: strings.Repeat("f", 32), Offset: 40}
	token := d.Encode(page)
	got, err := d.Decode(token)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != page {
		t.Fatalf("round trip = %+v, want %+v", got, page)
	}

	// Each forged token changes one field of a real token, so the rejection can only come
	// from the field that was changed. The filter is not checked at decode time — that is the
	// resume's job — so it stays out of this list.
	for _, forged := range []Page{
		{Version: FormatVersion + 1, Generation: 7, Filter: page.Filter, Offset: 40},
		{Version: 0, Generation: 7, Filter: page.Filter, Offset: 40},
		{Version: FormatVersion, Generation: 0, Filter: page.Filter, Offset: 40},
		{Version: FormatVersion, Generation: 7, Filter: page.Filter, Offset: -1},
	} {
		if _, err := d.Decode(d.Encode(forged)); !errors.Is(err, errInvalidCursor) {
			t.Errorf("forged %+v: err = %v, want invalid cursor", forged, err)
		}
	}

	for _, broken := range []string{"", "!!!", "a.b.c", d.Encode(page) + "x"} {
		if _, err := d.Decode(broken); !errors.Is(err, errInvalidCursor) {
			t.Errorf("undecodable %q: err = %v, want invalid cursor", broken, err)
		}
	}
}

func TestProjectionExpiry(t *testing.T) {
	d := testDomain("test")
	cache := d.NewCache(time.Minute, 4)
	generation := cache.Pin([]int{1, 2, 3}, at(0))

	if _, err := cache.resolve(generation, at(30)); err != nil {
		t.Fatalf("resolve inside the window: %v", err)
	}
	if _, err := cache.resolve(generation, at(60)); !errors.Is(err, errCursorExpired) {
		t.Fatalf("resolve after the window: err = %v, want cursor expired", err)
	}
	// An expired projection is dropped, not left to age forever.
	if cache.Count() != 0 {
		t.Fatalf("count after expiry = %d, want 0", cache.Count())
	}
	if _, err := cache.resolve(generation, at(10)); !errors.Is(err, errCursorExpired) {
		t.Fatalf("resolve a dropped generation: err = %v, want cursor expired", err)
	}
}

func TestProjectionCapEvictsOldest(t *testing.T) {
	d := testDomain("test")
	cache := d.NewCache(time.Hour, 3)
	first := cache.Pin([]int{1}, at(0))
	for i := 0; i < 3; i++ {
		cache.Pin([]int{i + 2}, at(i))
	}
	if got := cache.Count(); got != 3 {
		t.Fatalf("count = %d, want the limit 3", got)
	}
	if _, err := cache.resolve(first, at(0)); !errors.Is(err, errCursorExpired) {
		t.Fatalf("oldest generation: err = %v, want cursor expired", err)
	}
}

func TestResumeFreezesTheList(t *testing.T) {
	d := testDomain("test")
	cache := d.NewCache(time.Hour, 8)
	live := []int{1, 2, 3, 4, 5}
	builds := 0
	build := func() ([]int, error) {
		builds++
		return append([]int(nil), live...), nil
	}

	filter := d.Fingerprint("scope")
	items, page, err := Resume(cache, at(0), filter, "", build)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(items) != 5 || page.Offset != 0 || page.Generation == 0 {
		t.Fatalf("first page = %v, %+v", items, page)
	}

	// The live list grows and a row changes; the walk in progress reads the frozen copy and
	// builds nothing again.
	live = append(live, 6, 7)
	resumed, resumedPage, err := Resume(cache, at(1), filter, d.Encode(page), build)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if builds != 1 {
		t.Fatalf("build calls = %d, want 1: a resume must not rebuild the projection", builds)
	}
	if len(resumed) != 5 || resumedPage.Generation != page.Generation {
		t.Fatalf("resumed = %v, %+v; want the frozen list", resumed, resumedPage)
	}

	// A different filter — or a different scope — cannot resume this walk.
	if _, _, err := Resume(cache, at(2), d.Fingerprint("scope", "extra"), d.Encode(page), build); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("other filter: err = %v, want invalid cursor", err)
	}
	if _, _, err := Resume(cache, at(2), d.Fingerprint("other scope"), d.Encode(page), build); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("other scope: err = %v, want invalid cursor", err)
	}
	// A token that names a generation no longer held expires recognizably.
	missing := d.Encode(Page{Version: FormatVersion, Generation: 999, Filter: filter, Offset: 1})
	if _, _, err := Resume(cache, at(3), filter, missing, build); !errors.Is(err, errCursorExpired) {
		t.Fatalf("missing generation: err = %v, want cursor expired", err)
	}
	// A token whose generation pins another summary type is refused, not asserted into.
	crossToken := d.Encode(Page{Version: FormatVersion, Generation: page.Generation, Filter: filter, Offset: 0})
	if _, _, err := Resume(cache, at(4), filter, crossToken, func() ([]string, error) {
		return nil, nil
	}); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("cross-type resume: err = %v, want invalid cursor", err)
	}
}

func TestSlicePagesAndClones(t *testing.T) {
	d := testDomain("test")
	items := []int{1, 2, 3, 4, 5}
	clone := func(v int) int { return v }

	var walked []int
	page := Page{Version: FormatVersion, Generation: 1, Filter: "f"}
	for i := 0; i < 10; i++ {
		out, next, err := Slice(d, items, page, 2, clone)
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		walked = append(walked, out...)
		if next == "" {
			break
		}
		decoded, err := d.Decode(next)
		if err != nil {
			t.Fatalf("decode %q: %v", next, err)
		}
		page = decoded
	}
	if len(walked) != 5 || walked[0] != 1 || walked[4] != 5 {
		t.Fatalf("walked = %v, want all five rows once each", walked)
	}

	if _, _, err := Slice(d, items, Page{Version: FormatVersion, Generation: 1, Filter: "f", Offset: 6}, 2, clone); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("offset past the end: err = %v, want invalid cursor", err)
	}
	// An offset exactly at the end is the empty last page, not an error.
	if out, next, err := Slice(d, items, Page{Version: FormatVersion, Generation: 1, Filter: "f", Offset: 5}, 2, nil); err != nil || len(out) != 0 || next != "" {
		t.Fatalf("empty tail page = %v, %q, %v", out, next, err)
	}

	// The clone hook is what keeps a caller's edits out of the frozen list.
	type row struct{ n int }
	rows := []*row{{1}}
	page1 := Page{Version: FormatVersion, Generation: 1, Filter: "f"}
	cloner := func(v *row) *row { copied := *v; return &copied }
	out, _, err := Slice(d, rows, page1, 1, cloner)
	if err != nil {
		t.Fatalf("clone page: %v", err)
	}
	out[0].n = 99
	if rows[0].n != 1 {
		t.Fatalf("pinned row was rewritten through the page: %d", rows[0].n)
	}
}

func TestNormalizeLimit(t *testing.T) {
	d := testDomain("test")
	for _, limit := range []int{0, -1} {
		if got, err := d.NormalizeLimit(limit); err != nil || got != DefaultLimit {
			t.Errorf("NormalizeLimit(%d) = %d, %v; want the default", limit, got, err)
		}
	}
	if got, err := d.NormalizeLimit(MaxLimit); err != nil || got != MaxLimit {
		t.Errorf("NormalizeLimit(max) = %d, %v", got, err)
	}
	if _, err := d.NormalizeLimit(MaxLimit + 1); !errors.Is(err, errInvalidQuery) {
		t.Errorf("over the maximum: err = %v, want invalid query", err)
	}
}

func TestFingerprintBindsScopeAndValues(t *testing.T) {
	d := testDomain("test")
	base := d.Fingerprint("scope", "a", "b")
	if d.Fingerprint("scope", "a", "b") != base {
		t.Fatal("the same filter set hashed differently")
	}
	if d.Fingerprint("other", "a", "b") == base {
		t.Error("scope is not part of the fingerprint")
	}
	if d.Fingerprint("scope", "ab", "") == base {
		t.Error("positional values collided across a field boundary")
	}
	if d.Fingerprint("scope", "a", "b", "") == base {
		t.Error("extra values did not change the fingerprint")
	}
	if d.CanonicalTime(time.Time{}) != "" {
		t.Error("zero time must stay empty")
	}
	if d.CanonicalTime(at(0).In(time.FixedZone("x", 3600))) != d.CanonicalTime(at(0)) {
		t.Error("the same instant in another location hashed differently")
	}
}

func TestDomainsKeepTheirOwnSentinels(t *testing.T) {
	leftInvalid := errors.New("test: left cursor")
	rightInvalid := errors.New("test: right cursor")
	left := Domain{
		Name:          "left",
		InvalidCursor: leftInvalid,
		CursorExpired: errCursorExpired,
		InvalidQuery:  errInvalidQuery,
	}
	right := Domain{
		Name:          "right",
		InvalidCursor: rightInvalid,
		CursorExpired: errCursorExpired,
		InvalidQuery:  errInvalidQuery,
	}

	_, err := left.Decode("!!!")
	if !errors.Is(err, leftInvalid) || errors.Is(err, rightInvalid) || !strings.HasPrefix(err.Error(), "left:") {
		t.Fatalf("left decode: %v", err)
	}
	_, err = right.Decode("!!!")
	if !errors.Is(err, rightInvalid) || errors.Is(err, leftInvalid) || !strings.HasPrefix(err.Error(), "right:") {
		t.Fatalf("right decode: %v", err)
	}

	// One store's expired token never matches the other store's sentinel, even though both
	// expired for the same reason in the same cache implementation.
	cache := left.NewCache(time.Minute, 2)
	generation := cache.Pin([]int{1}, at(0))
	if _, err := cache.resolve(generation, at(120)); !errors.Is(err, errCursorExpired) {
		t.Fatalf("expiry: %v", err)
	}
	if _, _, err := Resume(cache, at(121), left.Fingerprint("scope"), left.Encode(Page{Version: FormatVersion, Generation: generation, Filter: left.Fingerprint("scope")}), func() ([]int, error) {
		return nil, nil
	}); !errors.Is(err, errCursorExpired) {
		t.Fatalf("resume after expiry: %v", err)
	}
	if _, err := left.NormalizeLimit(MaxLimit + 1); !errors.Is(err, errInvalidQuery) {
		t.Fatalf("limit: %v", err)
	}
}
