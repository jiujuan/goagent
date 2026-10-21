package agent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// TestLeaseLossInParallelSegmentStopsNextSegment: a lease error recorded while
// a parallel segment runs makes the next segment's write-access check fail, so
// its serial handler never starts.
func TestLeaseLossInParallelSegmentStopsNextSegment(t *testing.T) {
	leaseErr := fmt.Errorf("parallel lease lost: %w", checkpoint.ErrStaleFencingToken)
	capture := &dispatchContextCapture{}
	started := make(chan string, 2)
	barrier := make(chan struct{})
	var seqCalls atomic.Int32

	p1 := tool.New("p1", "waits for its peer", func(*tool.Context, struct{}) (string, error) {
		started <- "p1"
		<-barrier
		return "p1", nil
	})
	p2 := tool.New("p2", "loses the lease", func(*tool.Context, struct{}) (string, error) {
		started <- "p2"
		<-barrier
		capture.fail(leaseErr)
		return "p2", nil
	})
	seq := tool.AsSequential(tool.New("seq", "must not start", func(*tool.Context, struct{}) (string, error) {
		seqCalls.Add(1)
		return "seq", nil
	}))

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return dispatchToolResponse(
			core.ToolCall{ID: "c1", Name: "p1", Args: []byte(`{}`)},
			core.ToolCall{ID: "c2", Name: "p2", Args: []byte(`{}`)},
			core.ToolCall{ID: "c3", Name: "seq", Args: []byte(`{}`)},
		)
	})
	a, err := New(
		WithModel(model),
		WithTools(p1, p2, seq),
		WithMiddleware(capture),
		WithCheckpointer(checkpoint.NewMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	coordErr := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				coordErr <- fmt.Errorf("only %d of 2 parallel calls started", i)
				close(barrier)
				return
			}
		}
		close(barrier)
		coordErr <- nil
	}()

	_, runErr := a.Stream(context.Background(), "go").Wait()
	if err := <-coordErr; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(runErr, checkpoint.ErrStaleFencingToken) {
		t.Fatalf("run error = %v, want ErrStaleFencingToken", runErr)
	}
	if got := seqCalls.Load(); got != 0 {
		t.Fatalf("seq handler calls = %d, want 0 after lease loss", got)
	}
}
