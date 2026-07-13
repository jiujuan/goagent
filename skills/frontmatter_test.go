package skills

import (
	"reflect"
	"testing"
)

func TestSplitFrontmatter(t *testing.T) {
	tests := []struct {
		name     string
		src      string
		wantMeta map[string]any
		wantBody string
	}{
		{
			name:     "scalars and inline list",
			src:      "---\nname: pdf\ndescription: Work with PDFs\nallowed-tools: [run_command, web_fetch]\n---\n# Body\ntext\n",
			wantMeta: map[string]any{"name": "pdf", "description": "Work with PDFs", "allowed-tools": []string{"run_command", "web_fetch"}},
			wantBody: "# Body\ntext\n",
		},
		{
			name:     "block list",
			src:      "---\nname: x\nallowed-tools:\n  - run_command\n  - use_skill\n---\nbody",
			wantMeta: map[string]any{"name": "x", "allowed-tools": []string{"run_command", "use_skill"}},
			wantBody: "body",
		},
		{
			name:     "quoted scalar and comment line",
			src:      "---\n# a comment\nname: \"my skill\"\ndescription: 'quoted desc'\n---\nbody",
			wantMeta: map[string]any{"name": "my skill", "description": "quoted desc"},
			wantBody: "body",
		},
		{
			name:     "CRLF line endings and BOM",
			src:      "\uFEFF---\r\nname: win\r\n---\r\nbody\r\n",
			wantMeta: map[string]any{"name": "win"},
			wantBody: "body\n",
		},
		{
			name:     "blank lines before body are trimmed",
			src:      "---\nname: a\n---\n\n\nbody",
			wantMeta: map[string]any{"name": "a"},
			wantBody: "body",
		},
		{
			name:     "no frontmatter returns whole input as body",
			src:      "# Just markdown\nno fences",
			wantMeta: nil,
			wantBody: "# Just markdown\nno fences",
		},
		{
			name:     "opening fence but no close is body only",
			src:      "---\nname: x\nstill header no close",
			wantMeta: nil,
			wantBody: "---\nname: x\nstill header no close",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, body := splitFrontmatter([]byte(tt.src))
			if !reflect.DeepEqual(meta, tt.wantMeta) {
				t.Errorf("meta = %#v, want %#v", meta, tt.wantMeta)
			}
			if body != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
		})
	}
}

func TestParseInlineListEmpty(t *testing.T) {
	if got := parseInlineList("[]"); len(got) != 0 {
		t.Errorf("parseInlineList(\"[]\") = %#v, want empty", got)
	}
}

func TestStrList(t *testing.T) {
	tests := []struct {
		in   any
		want []string
	}{
		{[]string{"a", "b"}, []string{"a", "b"}},
		{"solo", []string{"solo"}},
		{"", nil},
		{42, nil},
		{nil, nil},
	}
	for _, tt := range tests {
		if got := strList(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("strList(%#v) = %#v, want %#v", tt.in, got, tt.want)
		}
	}
}
