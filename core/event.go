package core

// Event is the observational unit that flows over the Bus to UIs, loggers and
// tracers. It is purely for observation; durability and control flow live
// elsewhere (Checkpointer and Directive respectively). This is the clean split
// from v1, where one iter.Seq2[*Event] stream carried observation AND the
// commit log at once.
//
// Event is a sealed (tagged) union: only the variants in this file implement it
// via the unexported isEvent marker, giving exhaustive type switches without a
// discriminator field — the same technique Part uses for message content.
type Event interface{ isEvent() }

// --- Lifecycle --------------------------------------------------------------

// RunStarted is emitted once when a run begins.
type RunStarted struct {
	RunID    string
	ThreadID string
}

// RunDone is the terminal success event; Result summarizes the outcome. Stream
// adapters treat it as end-of-stream.
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

// MessageDelta is a streaming increment of the assistant message, for live
// rendering only; it is never persisted.
type MessageDelta struct{ Delta Message }

// MessageDone carries the completed assistant message of a step.
type MessageDone struct {
	Message Message
	Usage   *Usage
}

// --- Tool -------------------------------------------------------------------

// ToolStarted is emitted before a tool call executes.
type ToolStarted struct{ Call ToolCall }

// ToolUpdate carries a partial result streamed by a long-running tool.
type ToolUpdate struct {
	CallID  string
	Partial Part
}

// ToolDone carries a tool's final result.
type ToolDone struct{ Result ToolResult }

// --- Control / async --------------------------------------------------------

// Interrupted is emitted when the loop pauses for human-in-the-loop. The run is
// checkpointed; the caller resumes after deciding on Pending.
type Interrupted struct{ Pending []ApprovalRequest }

// Progress reports the state of a long-running asynchronous job on transient
// events (media generation, background work).
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
	Message Message
}

// ApprovalRequest describes one tool call awaiting a human decision.
type ApprovalRequest struct {
	CallID string `json:"call_id"`
	Tool   string `json:"tool"`
	Args   []byte `json:"args,omitempty"`
}
