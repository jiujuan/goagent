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

// PlanNodeStarted is emitted when a DAG plan node begins executing.
type PlanNodeStarted struct{ NodeID string }

// PlanNodeDone is emitted when a DAG plan node settles. Status is
// "done" | "failed" | "skipped" | "retry"; Err is set on failure/retry.
type PlanNodeDone struct {
	NodeID string
	Status string
	Err    error
}

// StuckDetected is emitted by the loop-guard middleware when it spots a
// repetition pattern. Rule is "repeat_call" | "error_streak"; Reason explains
// the concrete hit, Step is the loop step that triggered it.
type StuckDetected struct {
	Rule   string
	Reason string
	Step   int
}

// BudgetExceeded is emitted by the RunBudget middleware the moment a per-run
// resource cap is hit. Resource is "input_tokens" | "output_tokens" |
// "total_tokens" | "cost" | "duration" | "turns"; from the next model call on,
// the run enters wrap-up mode.
type BudgetExceeded struct {
	Resource string
	Step     int
}

// HistoryCompacted is emitted when the Compaction middleware rewrites the
// conversation history itself (persist mode): Dropped leading messages are
// replaced by one summary note, Kept trailing messages survive verbatim, and
// EstTokens is the calibrated estimate of the history size that triggered it.
type HistoryCompacted struct {
	Dropped   int
	Kept      int
	EstTokens int
	Step      int
}

// WindowTrimmed is emitted by the Window middleware each time it evicts message
// units from what a step would otherwise send. Strategy names the policy
// ("recent_n" | "sliding_window" | "importance_weighted"), Dropped and Kept
// count messages (not units) after the pairing repair, EstTokens is the
// calibrated estimate of the history before eviction, and Step is the loop step.
// Unlike HistoryCompacted — which fires only when a durable summarization
// rewrote the history — this fires per actual eviction, so a request-mode window
// emits one for each step it trimmed, distinguishable by Step.
type WindowTrimmed struct {
	Strategy  string
	Dropped   int
	Kept      int
	EstTokens int
	Step      int
}

// ArgRejected is emitted by the argument-guard middleware each time a tool call is
// refused before completing. Tool names the refused tool, Class is the loop's
// classification ("unknown_tool" | "prepare_failed" | "schema_invalid" |
// "handler_error" | "timed_out"), Count is that tool's running total for this run
// including this rejection, and Step is the loop step. Unlike StuckDetected — which
// fires when a pattern is recognised — this fires per refused call, so a model that
// cannot satisfy one tool's arguments produces a readable sequence of them.
type ArgRejected struct {
	Tool  string
	Class string
	Count int
	Step  int
}

// --- Marker -----------------------------------------------------------------

func (RunStarted) isEvent()       {}
func (RunDone) isEvent()          {}
func (RunFailed) isEvent()        {}
func (TurnStarted) isEvent()      {}
func (TurnDone) isEvent()         {}
func (MessageDelta) isEvent()     {}
func (MessageDone) isEvent()      {}
func (ToolStarted) isEvent()      {}
func (ToolUpdate) isEvent()       {}
func (ToolDone) isEvent()         {}
func (Interrupted) isEvent()      {}
func (Progress) isEvent()         {}
func (PlanNodeStarted) isEvent()  {}
func (PlanNodeDone) isEvent()     {}
func (StuckDetected) isEvent()    {}
func (BudgetExceeded) isEvent()   {}
func (HistoryCompacted) isEvent() {}
func (WindowTrimmed) isEvent()    {}
func (ArgRejected) isEvent()      {}

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
