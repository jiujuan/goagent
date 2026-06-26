// Package event holds v2's observational event union — the unit that flows over
// the Bus (package bus) to UIs, loggers and tracers. It is purely for
// observation; durability and control flow live elsewhere (Checkpointer and
// core.Directive respectively). This is the clean split ADR 0023 makes vs v1,
// where one iter.Seq2[*core.Event] stream carried observation AND the commit
// log at once.
//
// Note on placement: ADR 0023's skeleton names this core/event.go, but v1's
// core.Event struct still occupies that name during the dual-track migration.
// To keep v1 compiling untouched, the v2 union lives here as event.Event; it
// folds back into core once v1's Event/Actions are removed (migration step 9).
//
// Event is a sealed (tagged) union: only the variants in this file implement it
// via the unexported isEvent marker, giving exhaustive type switches without a
// discriminator field — the same technique core.Part uses for message content.
package event

import (
	"encoding/json"

	"github.com/jiujuan/goagent/core"
)

// Event is the sealed union of everything observable about a run.
type Event interface{ isEvent() }

// --- Lifecycle --------------------------------------------------------------

// RunStarted is emitted once when a run begins.
type RunStarted struct {
	RunID    string
	ThreadID string
}

// RunDone is the terminal success event; its Result summarizes the outcome.
// Stream adapters (bus.Adapt) treat it as end-of-stream.
type RunDone struct {
	Result Result
}

// RunFailed is the terminal failure event; Err travels with it so subscribers
// never special-case errors on a side channel.
type RunFailed struct {
	Err error
}

// --- Turn / message ---------------------------------------------------------

// TurnStarted marks the start of one model-call step (0-indexed).
type TurnStarted struct{ Step int }

// TurnDone marks the end of one step.
type TurnDone struct{ Step int }

// MessageDelta is a streaming increment of the assistant message. It is
// delivered for live rendering only and is never persisted.
type MessageDelta struct{ Delta core.Message }

// MessageDone carries the completed (aggregated) assistant message of a step.
type MessageDone struct {
	Message core.Message
	Usage   *core.Usage
}

// --- Tool -------------------------------------------------------------------

// ToolStarted is emitted before a tool call executes.
type ToolStarted struct{ Call core.ToolCall }

// ToolUpdate carries a partial result streamed by a long-running tool.
type ToolUpdate struct {
	CallID  string
	Partial core.Part
}

// ToolDone carries a tool's final result.
type ToolDone struct{ Result core.ToolResult }

// --- Control / async --------------------------------------------------------

// Interrupted is emitted when the loop pauses for human-in-the-loop. The run is
// checkpointed; the caller resumes it after deciding on Pending.
type Interrupted struct{ Pending []ApprovalRequest }

// Progress reports the state of a long-running asynchronous job (media
// generation, background work) on transient events.
type Progress struct{ Job ProgressInfo }

// --- Marker -----------------------------------------------------------------

func (RunStarted) isEvent()   {}
func (RunDone) isEvent()      {}
func (RunFailed) isEvent()    {}
func (TurnStarted) isEvent()  {}
func (TurnDone) isEvent()     {}
func (MessageDelta) isEvent() {}
func (MessageDone) isEvent()  {}
func (ToolStarted) isEvent()  {}
func (ToolUpdate) isEvent()   {}
func (ToolDone) isEvent()     {}
func (Interrupted) isEvent()  {}
func (Progress) isEvent()     {}

// --- Payload types ----------------------------------------------------------

// Result is the summary value carried by RunDone (the settlement payload a
// caller gets from Run.Wait()).
type Result struct {
	Message core.Message
}

// ApprovalRequest describes one tool call awaiting a human decision.
type ApprovalRequest struct {
	CallID string          `json:"call_id"`
	Tool   string          `json:"tool"`
	Args   json.RawMessage `json:"args"`
}

// ProgressInfo mirrors a provider-neutral async job lifecycle.
type ProgressInfo struct {
	JobID   string `json:"job_id,omitempty"`
	Kind    string `json:"kind,omitempty"` // "image" | "video" | ...
	Status  string `json:"status,omitempty"`
	Percent int    `json:"percent,omitempty"`
}
