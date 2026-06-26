package runner

import (
	"context"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/session"
)

// BusKey is the queue/bus routing key for a session. A frontend subscribes with
// the same value (queue.Bus.Subscribe(BusKey(app,user,session))) to receive a
// background job's progress and result.
func BusKey(appName, userID, sessionID string) string {
	return appName + "/" + userID + "/" + sessionID
}

// EnqueueAgent runs ag in the background via q instead of blocking the caller.
// It builds a queue.Job that invokes the agent for (userID, sessionID) with msg
// as the user content, forwarding the agent's Partial events to the bus and
// committing its final event to store. It returns the job id immediately; the
// agent's progress and result arrive on the bus under BusKey(app,user,session).
//
// This is the bridge between the agent layer and the standalone queue package:
// the queue itself knows nothing about agents or sessions, so this glue lives in
// runner, which already ties agents to a store.
func EnqueueAgent(ctx context.Context, q queue.Queue, store session.Store, appName, userID, sessionID string, ag agent.Agent, msg core.Message) (string, error) {
	jobID := core.NewID("job")
	job := queue.Job{
		ID:  jobID,
		Key: BusKey(appName, userID, sessionID),
		Run: func(rctx context.Context, emit func(*core.Event)) (*core.Event, error) {
			sess, err := store.GetOrCreate(rctx, appName, userID, sessionID)
			if err != nil {
				return nil, err
			}
			ictx := agent.InvocationContext{
				Context:      rctx,
				InvocationID: jobID,
				Agent:        ag,
				Root:         ag,
				Session:      sess,
				UserContent:  msg,
			}
			var final *core.Event
			for ev, err := range ag.Run(ictx) {
				if err != nil {
					return nil, err
				}
				if ev == nil {
					continue
				}
				if ev.Partial {
					emit(ev)
					continue
				}
				if err := store.Append(rctx, sess, ev); err != nil {
					return nil, err
				}
				final = ev
			}
			return final, nil
		},
	}
	if err := q.Enqueue(ctx, job); err != nil {
		return "", err
	}
	return jobID, nil
}
