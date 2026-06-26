package core

// Event is the unit that flows through every layer of goagent. An Event carries
// an optional Message plus Actions: declarative side effects (state mutations,
// delegation, control signals) that the Runner applies transactionally when it
// commits a non-partial event. Modeling side effects as data — rather than
// letting components mutate shared state directly — keeps runs replayable and
// auditable, and lets errors travel the same stream as normal output.
type Event struct {
	ID           string `json:"id"`
	InvocationID string `json:"invocation_id"`
	Author       string `json:"author"`
	Branch       string `json:"branch,omitempty"`

	// ParentID links this event to the one it was appended after, forming the
	// session's history as a tree rather than a flat list. An empty ParentID
	// means "append after the current leaf" (a linear session is the degenerate
	// tree where every event's parent is its predecessor). The active message
	// history a model sees is the path from the active leaf back to the root.
	ParentID string `json:"parent_id,omitempty"`

	// SummarizesTo, when non-empty, marks this event as a summary node: its
	// Message stands in for the conversation prefix from the root up to and
	// including the event with this ID. In the projected message history that
	// prefix is replaced by this summary; events after the cut are kept verbatim.
	// State is unaffected — it still replays over every event on the path — so
	// summarization is purely a view concern. Summary nodes are persistent and
	// supersede one another (the one nearest the leaf wins), which makes
	// re-summarization just "append a new summary node".
	SummarizesTo string `json:"summarizes_to,omitempty"`

	// Message is the content produced at this step, if any.
	Message *Message `json:"message,omitempty"`

	// Partial marks a streaming increment. Partial events are delivered to
	// subscribers for live rendering but are never committed to the session
	// store; only the final aggregated event is persisted.
	Partial bool `json:"partial,omitempty"`

	// Actions are the side effects requested by this event.
	Actions Actions `json:"actions,omitzero"`

	// Usage is the provider-reported token usage, used to anchor context-size
	// estimation for compaction.
	Usage *Usage `json:"usage,omitempty"`

	// Progress, when set, reports the state of a long-running asynchronous
	// operation this event is about (e.g. queued image/video generation). It
	// travels on transient (Partial) events so a frontend can render live
	// progress; the final result arrives as a committed event carrying the
	// generated media in Message.
	Progress *Progress `json:"progress,omitempty"`

	// Err carries a failure down the same stream as normal events, so
	// subscribers never special-case errors.
	Err error `json:"-"`
}

// Actions are the declarative side effects an Event requests.
type Actions struct {
	// StateDelta is merged into session state when the event is committed.
	StateDelta map[string]any `json:"state_delta,omitempty"`

	// TransferToAgent, when set, hands control to the named agent.
	TransferToAgent string `json:"transfer_to_agent,omitempty"`

	// Escalate signals an enclosing loop agent to stop iterating.
	Escalate bool `json:"escalate,omitempty"`

	// Stop requests that the current turn end after this event.
	Stop bool `json:"stop,omitempty"`
}

// Usage reports token consumption for a model call.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Progress is the state of a long-running asynchronous job (e.g. media
// generation) reported on an Event. JobID ties a stream of progress events and
// the final result together; Status mirrors the provider-neutral lifecycle
// ("queued"/"running"/"succeeded"/"failed").
type Progress struct {
	JobID   string `json:"job_id,omitempty"`
	Kind    string `json:"kind,omitempty"` // "image" | "video"
	Status  string `json:"status,omitempty"`
	Percent int    `json:"percent,omitempty"`
	Err     string `json:"error,omitempty"`
}

// IsFinalResponse reports whether this event is a committed assistant message
// with no outstanding tool calls — i.e. a turn-ending reply.
func (e *Event) IsFinalResponse() bool {
	if e == nil || e.Partial || e.Message == nil {
		return false
	}
	if e.Message.Role != RoleAssistant {
		return false
	}
	return len(e.Message.ToolCalls()) == 0
}
