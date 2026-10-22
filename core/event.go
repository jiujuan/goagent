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

// RunDone is the terminal success event; Result summarizes the outcome. A run
// emits exactly one RunDone, after its last turn, and stream adapters treat it as
// end-of-stream. TurnDone is not a run-end signal: one run can emit many.
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

// TurnDone marks the end of one turn. A run can emit it several times when
// steering or inbox input continues the conversation; only RunDone ends the run.
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

// ToolBatchInFlight is emitted after a tool batch's checkpoint committed and
// before any handler starts. Its presence on the event stream means the batch
// can be recovered after a crash; it says nothing about whether the tools have
// run yet.
type ToolBatchInFlight struct {
	Step  int
	Calls []ToolCall
}

// ToolBatchRecovered is emitted after a resumed run has settled the tool batch
// its in-flight checkpoint left behind and committed the result. Replayed counts
// calls whose stored result was reused, Executed counts calls that reached a
// handler (whether it succeeded or failed), Unknown counts calls whose earlier
// outcome could not be determined and were reported as such, and Rejected counts
// calls refused before a handler ran. The counts are diagnostic: a call can fall
// outside all four (for example a write-access failure before dispatch), so they
// are not a conservation equation over the batch.
type ToolBatchRecovered struct {
	Step     int
	Replayed int
	Executed int
	Unknown  int
	Rejected int
}

// ToolUpdate carries a partial result streamed by a long-running tool.
type ToolUpdate struct {
	CallID  string
	Partial Part
}

// ToolDone carries a tool's final result.
type ToolDone struct{ Result ToolResult }

// ApprovalDecided is the durable audit event for a structured human decision.
// It intentionally does not overload ToolResult: the model still receives a
// readable rejection result, while observers can retain the decision identity,
// pause binding and policy evidence separately.
type ApprovalDecided struct {
	Decision  ApprovalDecision
	Duplicate bool
}

// --- Control / async --------------------------------------------------------

// Interrupted is emitted when the loop pauses. The run is checkpointed; Phase,
// Reason and Recovery explain whether callers should decide on Pending or expect
// the next run to replay a model request.
type Interrupted struct {
	Pending  []ApprovalRequest
	Phase    string
	Reason   string
	Recovery string
}

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
// conversation history itself: persist mode on a normal threshold hit, or the
// context-overflow recovery path (even in request mode, because the recovery is
// persisted before the retry). Dropped leading messages are replaced by one
// summary note, Kept trailing messages survive verbatim, and EstTokens is the
// calibrated estimate of the history size that triggered the rewrite.
type HistoryCompacted struct {
	Dropped   int
	Kept      int
	EstTokens int
	Step      int
}

// WindowTrimmed is emitted by the Window middleware each time it evicts message
// units from what a step would otherwise send. Strategy names the policy
// ("recent_n" | "sliding_window" | "importance_weighted" | "overflow_tail"),
// where "overflow_tail" is the emergency tail eviction used to recover from a
// typed context-length overflow. Dropped and Kept count messages (not units)
// after the pairing repair, EstTokens is the calibrated estimate of the history
// before eviction, and Step is the loop step. Unlike HistoryCompacted — which
// fires only when a durable summarization rewrote the history — this fires per
// actual eviction, so a request-mode window emits one for each step it trimmed,
// distinguishable by Step.
type WindowTrimmed struct {
	Strategy  string
	Dropped   int
	Kept      int
	EstTokens int
	Step      int
}

// ContextOverflowRetried is emitted after the agent loop recovered from a typed
// context-length overflow and persisted the shortened history, immediately
// before the retried model call in the same step. Attempt counts recovery rounds
// within the step starting at 1. Model is the frozen view's model, while
// Provider, Limit and Requested come from the failed call's typed error and stay
// empty or zero when unknown. MessagesBefore and MessagesAfter count the working
// history around the recovery.
type ContextOverflowRetried struct {
	Step           int
	Attempt        int
	Model          string
	Provider       string
	Limit          int
	Requested      int
	MessagesBefore int
	MessagesAfter  int
}

// ArgRejected is emitted by the argument-guard middleware each time a tool call is
// refused before completing. Tool names the refused tool, Class is the loop's
// classification ("unknown_tool" | "not_advertised" | "prepare_failed" |
// "schema_invalid" | "handler_error" | "timed_out"), Count is that tool's running
// total for this run including this rejection, and Step is the loop step. Unlike
// StuckDetected — which fires when a pattern is recognised — this fires per
// refused call, so a model that cannot satisfy one tool's arguments produces a
// readable sequence of them.
type ArgRejected struct {
	Tool  string
	Class string
	Count int
	Step  int
}

// StepViewDrifted is emitted by the default step-snapshot policy when the first
// step frozen after a resume differs from the view the resumed checkpoint
// recorded. Tool and settings digests are reported separately so the change is
// attributable; an empty expected digest means that half was not compared (a
// legacy or hand-built snapshot). The run continues: this is a diagnosis, not a
// veto — StepSnapshotStrict turns the same difference into ErrStepViewDrift
// before the model is called.
type StepViewDrifted struct {
	Step                   int
	BaselineStep           int
	ExpectedToolDigest     string
	ActualToolDigest       string
	ExpectedSettingsDigest string
	ActualSettingsDigest   string
}

// InputDelivered is emitted when inputs enter one step's working history, after
// the step's PrepareTurn has appended them. Source says which channel they came
// from, Phase is the normalized delivery phase, and Count is how many messages
// arrived. MessageIDs is filled only for durable inputs: a volatile steering
// message has no store identity. Preempted marks a delivery that a preempted
// model call retried inside the same step.
//
// For durable inputs this is not a persistence acknowledgement: the inbox
// acknowledgement commits with the next checkpoint, so a producer watching for
// consumption must read the inbox and this event together, and accept
// at-least-once delivery.
type InputDelivered struct {
	Step       int
	Source     InputSource
	Phase      InputPhase
	Count      int
	MessageIDs []string
	Preempted  bool
}

// --- Marker -----------------------------------------------------------------

func (RunStarted) isEvent()             {}
func (RunDone) isEvent()                {}
func (RunFailed) isEvent()              {}
func (TurnStarted) isEvent()            {}
func (TurnDone) isEvent()               {}
func (MessageDelta) isEvent()           {}
func (MessageDone) isEvent()            {}
func (ToolStarted) isEvent()            {}
func (ToolBatchInFlight) isEvent()      {}
func (ToolBatchRecovered) isEvent()     {}
func (ToolUpdate) isEvent()             {}
func (ToolDone) isEvent()               {}
func (ApprovalDecided) isEvent()        {}
func (Interrupted) isEvent()            {}
func (Progress) isEvent()               {}
func (PlanNodeStarted) isEvent()        {}
func (PlanNodeDone) isEvent()           {}
func (StuckDetected) isEvent()          {}
func (BudgetExceeded) isEvent()         {}
func (HistoryCompacted) isEvent()       {}
func (WindowTrimmed) isEvent()          {}
func (ContextOverflowRetried) isEvent() {}
func (ArgRejected) isEvent()            {}
func (StepViewDrifted) isEvent()        {}
func (InputDelivered) isEvent()         {}

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

// ApprovalDecision is the durable form of a human approval. Digest covers all
// fields that change its meaning; ParameterDigest separately exposes the exact
// approved tool argument identity without storing arguments a second time.
type ApprovalDecision struct {
	DecisionID      string `json:"decision_id"`
	PauseID         string `json:"pause_id"`
	CallID          string `json:"call_id"`
	Tool            string `json:"tool"`
	ParameterDigest string `json:"parameter_digest"`
	PolicyVersion   string `json:"policy_version"`
	Approved        bool   `json:"approved"`
	Reason          string `json:"reason,omitempty"`
	Digest          string `json:"digest"`
}
