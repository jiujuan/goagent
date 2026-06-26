// Package runner orchestrates an invocation: it resolves the session, appends
// the user message, runs the root agent, and commits non-partial events to the
// store as they stream. It is the only layer that persists; agents stay pure.
package runner

import (
	"context"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/callbacks"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

// Config configures a Runner.
type Config struct {
	AppName string
	Root    agent.Agent
	Store   session.Store
	Handler callbacks.Handler // optional observability
}

// Runner ties an agent tree to a session store.
type Runner struct {
	appName string
	root    agent.Agent
	store   session.Store
	handler callbacks.Handler
}

// New constructs a Runner, defaulting the store to in-memory.
func New(cfg Config) *Runner {
	store := cfg.Store
	if store == nil {
		store = session.InMemory()
	}
	app := cfg.AppName
	if app == "" {
		app = "goagent"
	}
	return &Runner{appName: app, root: cfg.Root, store: store, handler: cfg.Handler}
}

// Run executes one turn for (userID, sessionID) with the given user message and
// streams every event. Non-partial events are committed to the store before
// being yielded, so a consumer that stops early still leaves a consistent log.
func (r *Runner) Run(ctx context.Context, userID, sessionID string, msg core.Message) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		sess, err := r.store.GetOrCreate(ctx, r.appName, userID, sessionID)
		if err != nil {
			yield(nil, err)
			return
		}

		invID := core.NewID("inv")
		userEvt := &core.Event{
			ID:           core.NewID("evt"),
			InvocationID: invID,
			Author:       "user",
			Message:      &msg,
		}
		if err := r.store.Append(ctx, sess, userEvt); err != nil {
			yield(nil, err)
			return
		}
		r.fireEvent(ctx, userEvt)
		if !yield(userEvt, nil) {
			return
		}

		ictx := agent.InvocationContext{
			Context:      ctx,
			InvocationID: invID,
			Agent:        r.root,
			Root:         r.root,
			Session:      sess,
			UserContent:  msg,
		}

		for ev, err := range r.root.Run(ictx) {
			if err != nil {
				yield(ev, err)
				return
			}
			if ev != nil && !ev.Partial {
				if cerr := r.store.Append(ctx, sess, ev); cerr != nil {
					yield(ev, cerr)
					return
				}
				r.fireEvent(ctx, ev)
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

func (r *Runner) fireEvent(ctx context.Context, ev *core.Event) {
	if r.handler != nil {
		r.handler.OnEvent(ctx, ev)
	}
}
