package session

import (
	"testing"

	"github.com/jiujuan/goagent/core"
)

func TestOverlayIsolatesWritesAndExportsPatch(t *testing.T) {
	base := NewState()
	base.Set("keep", 1)
	base.Set("remove", 2)
	overlay := NewOverlay(base)

	overlay.Set("keep", 3)
	overlay.Set("added", 4)
	overlay.Delete("remove")
	overlay.Apply(core.Actions{StateDelta: map[string]any{"event": 5}})

	if got, _ := base.Get("keep"); got != 1 {
		t.Fatalf("base keep = %v, want 1", got)
	}
	if _, ok := base.Get("added"); ok {
		t.Fatal("overlay write leaked into base")
	}
	patch := overlay.Patch()
	if got := patch.Delta["keep"]; got != 3 {
		t.Fatalf("patch keep = %v, want 3", got)
	}
	if got := patch.Delta["event"]; got != 5 {
		t.Fatalf("patch event = %v, want 5", got)
	}
	if len(patch.Delete) != 1 || patch.Delete[0] != "remove" {
		t.Fatalf("patch delete = %v, want [remove]", patch.Delete)
	}
}
