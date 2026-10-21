package agent

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/jiujuan/goagent/core"
)

func segTestCalls(names ...string) []core.ToolCall {
	calls := make([]core.ToolCall, 0, len(names))
	for i, name := range names {
		calls = append(calls, core.ToolCall{ID: fmt.Sprintf("c%d", i+1), Name: name})
	}
	return calls
}

func segTestToolView(sequential ...string) *ToolView {
	tv := &ToolView{sequential: make(map[string]bool, len(sequential))}
	for _, name := range sequential {
		tv.sequential[name] = true
	}
	return tv
}

func parallelSeg(indexes ...int) toolCallSegment {
	return toolCallSegment{indexes: indexes}
}

func serialSeg(index int) toolCallSegment {
	return toolCallSegment{serial: true, indexes: []int{index}}
}

func TestBuildToolCallSegments(t *testing.T) {
	tests := []struct {
		name       string
		calls      []string
		sequential []string
		mode       ToolExecMode
		want       []toolCallSegment
	}{
		{name: "empty batch", calls: nil, want: nil},
		{name: "all parallel", calls: []string{"a", "b", "c"}, want: []toolCallSegment{parallelSeg(0, 1, 2)}},
		{name: "single serial", calls: []string{"s"}, sequential: []string{"s"}, want: []toolCallSegment{serialSeg(0)}},
		{
			name:       "serial at end",
			calls:      []string{"a", "b", "s"},
			sequential: []string{"s"},
			want:       []toolCallSegment{parallelSeg(0, 1), serialSeg(2)},
		},
		{
			name:       "serial at start",
			calls:      []string{"s", "a", "b"},
			sequential: []string{"s"},
			want:       []toolCallSegment{serialSeg(0), parallelSeg(1, 2)},
		},
		{
			name:       "serial in middle",
			calls:      []string{"a", "s", "b"},
			sequential: []string{"s"},
			want:       []toolCallSegment{parallelSeg(0), serialSeg(1), parallelSeg(2)},
		},
		{
			name:       "multiple serial",
			calls:      []string{"a", "s1", "b", "s2", "c"},
			sequential: []string{"s1", "s2"},
			want: []toolCallSegment{
				parallelSeg(0), serialSeg(1), parallelSeg(2), serialSeg(3), parallelSeg(4),
			},
		},
		{
			name:       "adjacent serial",
			calls:      []string{"s1", "s2"},
			sequential: []string{"s1", "s2"},
			want:       []toolCallSegment{serialSeg(0), serialSeg(1)},
		},
		{
			name:  "global sequential",
			calls: []string{"a", "b"},
			mode:  ToolSequential,
			want:  []toolCallSegment{serialSeg(0), serialSeg(1)},
		},
		{name: "unknown name", calls: []string{"unknown", "a"}, want: []toolCallSegment{parallelSeg(0, 1)}},
		{
			name:       "repeated serial name",
			calls:      []string{"s", "s"},
			sequential: []string{"s"},
			want:       []toolCallSegment{serialSeg(0), serialSeg(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildToolCallSegments(segTestCalls(tt.calls...), segTestToolView(tt.sequential...), tt.mode)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("segments = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBuildToolCallSegmentsInvariants(t *testing.T) {
	names := []string{"a", "b", "c", "d"}
	wantFlat := []int{0, 1, 2, 3}
	for mask := 0; mask < 1<<len(names); mask++ {
		var sequential []string
		for i, name := range names {
			if mask&(1<<i) != 0 {
				sequential = append(sequential, name)
			}
		}
		for _, mode := range []ToolExecMode{ToolParallel, ToolSequential} {
			t.Run(fmt.Sprintf("mask=%04b/mode=%d", mask, mode), func(t *testing.T) {
				segments := buildToolCallSegments(segTestCalls(names...), segTestToolView(sequential...), mode)
				var flat []int
				for si, seg := range segments {
					if len(seg.indexes) == 0 {
						t.Fatalf("segment %d is empty", si)
					}
					if seg.serial && len(seg.indexes) != 1 {
						t.Fatalf("serial segment %d has %d calls, want 1", si, len(seg.indexes))
					}
					if !slices.IsSorted(seg.indexes) {
						t.Fatalf("segment %d indexes %v are not ascending", si, seg.indexes)
					}
					if mode == ToolSequential && !seg.serial {
						t.Fatalf("segment %d is parallel under ToolSequential: %+v", si, seg)
					}
					if mode == ToolParallel && si > 0 && !segments[si-1].serial && !seg.serial {
						t.Fatalf("adjacent parallel segments %d and %d are not separated by a serial segment", si-1, si)
					}
					flat = append(flat, seg.indexes...)
				}
				if !slices.Equal(flat, wantFlat) {
					t.Fatalf("indexes = %v, want every call exactly once in order %v", flat, wantFlat)
				}
			})
		}
	}
}
