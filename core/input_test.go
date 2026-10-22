package core_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
)

func TestNormalizeInputPhase(t *testing.T) {
	tests := []struct {
		name string
		in   core.InputPhase
		want core.InputPhase
	}{
		{name: "empty means before model", in: "", want: core.InputBeforeModel},
		{name: "before model", in: core.InputBeforeModel, want: core.InputBeforeModel},
		{name: "after model", in: core.InputAfterModel, want: core.InputAfterModel},
		{name: "after tools", in: core.InputAfterTools, want: core.InputAfterTools},
		{name: "next turn", in: core.InputNextTurn, want: core.InputNextTurn},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := core.NormalizeInputPhase(tc.in)
			if err != nil {
				t.Fatalf("NormalizeInputPhase(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeInputPhase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	for _, in := range []core.InputPhase{"before-model", "BEFORE_MODEL", "later", " "} {
		got, err := core.NormalizeInputPhase(in)
		if !errors.Is(err, core.ErrUnknownInputPhase) {
			t.Fatalf("NormalizeInputPhase(%q) error = %v, want ErrUnknownInputPhase", in, err)
		}
		if got != "" {
			t.Fatalf("NormalizeInputPhase(%q) = %q, want the empty phase on error", in, got)
		}
	}
}

func TestInputPhaseValuesAreStable(t *testing.T) {
	// These literals cross the wire: inbox records, event JSON and external
	// producers all spell them out, so renaming one is a deliberate change.
	phases := map[core.InputPhase]string{
		core.InputBeforeModel: "before_model",
		core.InputAfterModel:  "after_model",
		core.InputAfterTools:  "after_tools",
		core.InputNextTurn:    "next_turn",
	}
	for got, want := range phases {
		if string(got) != want {
			t.Fatalf("input phase = %q, want %q", got, want)
		}
	}

	sources := map[core.InputSource]string{
		core.InputVolatile: "volatile",
		core.InputDurable:  "durable",
	}
	for got, want := range sources {
		if string(got) != want {
			t.Fatalf("input source = %q, want %q", got, want)
		}
	}

	states := map[core.InputState]string{
		core.InputStateQueued:    "queued",
		core.InputStateDelivered: "delivered",
		core.InputStateRejected:  "rejected",
	}
	for got, want := range states {
		if string(got) != want {
			t.Fatalf("input state = %q, want %q", got, want)
		}
	}
}

func TestErrUnknownInputPhaseText(t *testing.T) {
	if text := core.ErrUnknownInputPhase.Error(); !strings.HasPrefix(text, "core: ") {
		t.Fatalf("error text = %q, want the %q package prefix", text, "core: ")
	}
}
