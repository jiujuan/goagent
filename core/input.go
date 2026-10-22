package core

import (
	"errors"
	"fmt"
)

// InputPhase is the latest point in a run at which an input may be delivered.
// The empty value means InputBeforeModel, so records written before phases
// existed keep their current delivery time.
type InputPhase string

const (
	// InputBeforeModel delivers at the next turn's or the next step's PrepareTurn.
	InputBeforeModel InputPhase = "before_model"
	// InputAfterModel delivers no earlier than the step after the model answered.
	InputAfterModel InputPhase = "after_model"
	// InputAfterTools delivers no earlier than the step after a tool batch joined.
	InputAfterTools InputPhase = "after_tools"
	// InputNextTurn delivers only when a new turn starts, never mid-turn.
	InputNextTurn InputPhase = "next_turn"
)

// InputSource records which physical channel an input arrived on. It is carried
// by delivery events and receipts for display; it does not affect scheduling.
type InputSource string

const (
	// InputVolatile comes from a run's in-process steering queue.
	InputVolatile InputSource = "volatile"
	// InputDurable comes from the persistent inbox.
	InputDurable InputSource = "durable"
)

// InputState is the receipt state of one input.
type InputState string

const (
	// InputQueued means the input was accepted but not yet delivered.
	InputQueued InputState = "queued"
	// InputDelivered means the input entered a step's working history.
	InputDelivered InputState = "delivered"
	// InputRejected means the input was refused and never delivered.
	InputRejected InputState = "rejected"
)

// ErrUnknownInputPhase reports a phase outside the closed set.
var ErrUnknownInputPhase = errors.New("core: unknown input phase")

// NormalizeInputPhase maps the empty phase to InputBeforeModel and refuses
// anything else outside the closed set, so no caller has to guess what an
// unrecognized delivery instruction means.
func NormalizeInputPhase(p InputPhase) (InputPhase, error) {
	if p == "" {
		return InputBeforeModel, nil
	}
	switch p {
	case InputBeforeModel, InputAfterModel, InputAfterTools, InputNextTurn:
		return p, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownInputPhase, p)
	}
}
