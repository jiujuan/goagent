package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/session"
)

// TestResummarizePersistsSummaryNode drives a session past the compaction
// threshold and verifies Resummarize writes a persistent summary node that
// shrinks the projected history, while the raw event log keeps every event.
func TestResummarizePersistsSummaryNode(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")

	// A summarizer that ignores its input and returns a fixed marker.
	summarizer := mock.New("sum", func(*llm.Request) *llm.Response {
		return mock.Text("RECAP")
	})

	// Seed a long history (each message ~40 chars => ~11 tokens by chars/4).
	long := strings.Repeat("x", 40)
	for i := 0; i < 6; i++ {
		m := core.UserText(long)
		if err := st.Append(ctx, s, &core.Event{Author: "user", Message: &m}); err != nil {
			t.Fatal(err)
		}
	}
	before := len(s.Messages())

	did, err := middleware.Resummarize(ctx, st, s, summarizer, &middleware.CompactionOptions{
		MaxTokens:        20,
		KeepRecentTokens: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !did {
		t.Fatal("Resummarize did not trigger on an over-threshold session")
	}

	after := s.Messages()
	if len(after) >= before {
		t.Fatalf("projection did not shrink: before=%d after=%d", before, len(after))
	}
	if !strings.Contains(after[0].Text(), "RECAP") {
		t.Fatalf("first projected message = %q, want it to contain the summary", after[0].Text())
	}
	// Nothing is lost from the append-only log: 6 originals + 1 summary node.
	if n := len(s.Events()); n != 7 {
		t.Fatalf("raw events = %d, want 7", n)
	}
}

// TestResummarizeNoopUnderThreshold verifies a short session is left untouched.
func TestResummarizeNoopUnderThreshold(t *testing.T) {
	ctx := context.Background()
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "s")

	summarizer := mock.New("sum", func(*llm.Request) *llm.Response { return mock.Text("RECAP") })

	m := core.UserText("hi")
	if err := st.Append(ctx, s, &core.Event{Author: "user", Message: &m}); err != nil {
		t.Fatal(err)
	}

	did, err := middleware.Resummarize(ctx, st, s, summarizer, &middleware.CompactionOptions{MaxTokens: 8000})
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("Resummarize triggered under threshold")
	}
	if n := len(s.Events()); n != 1 {
		t.Fatalf("raw events = %d, want 1 (untouched)", n)
	}
}
