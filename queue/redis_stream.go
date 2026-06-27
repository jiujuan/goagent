package queue

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jiujuan/goagent/core"
)

// redisStreamQueue is a Redis Streams-backed Queue and Consumer. Jobs are stored
// on a stream (XADD) and drained by a consumer group (XREADGROUP), giving
// at-least-once delivery that survives a worker restart: a job a worker read but
// did not ack stays in the group's Pending Entries List (PEL) and is reclaimed
// by a janitor goroutine after it has been idle past idleThreshold.
//
// Because Job.Run is a closure that cannot cross a process boundary, this backend
// only accepts jobs that carry Type+Payload; the draining worker (a Pool with a
// Registry) rebuilds the work.
//
// Reliability knobs (see factory Options):
//   - idleThreshold: how long a delivered-but-unacked job waits before reclaim.
//     MUST exceed the longest expected job runtime, or a running job is run twice.
//   - maxDeliveries: a job delivered more than this is poison; it goes to the DLQ.
//   - maxLen: approximate MAXLEN trim so the stream does not grow unbounded.
type redisStreamQueue struct {
	rdb           *redis.Client
	stream        string
	group         string
	consumer      string
	deadStream    string
	idleThreshold time.Duration
	maxDeliveries int
	maxLen        int64

	retryCh chan pendingItem
	start   sync.Once
}

type pendingItem struct {
	job Job
	ack func() error
}

// wireJob is the serialized shape of a Job on the stream (Run never travels).
type wireJob struct {
	ID      string          `json:"id"`
	Key     string          `json:"key"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

const ackTimeout = 5 * time.Second

func newRedisStreamQueue(rdb *redis.Client, c *config) *redisStreamQueue {
	return &redisStreamQueue{
		rdb:           rdb,
		stream:        c.stream,
		group:         c.group,
		consumer:      core.NewID("worker"),
		deadStream:    c.deadStream,
		idleThreshold: c.idleThreshold,
		maxDeliveries: c.maxDeliveries,
		maxLen:        c.maxLen,
		retryCh:       make(chan pendingItem, 128),
	}
}

// Enqueue implements Queue. It rejects Run-only jobs: a closure cannot be
// serialized onto the stream.
func (q *redisStreamQueue) Enqueue(ctx context.Context, job Job) error {
	if job.Type == "" {
		return errors.New("queue: redis backend requires Job.Type (a Run closure cannot be serialized)")
	}
	data, err := json.Marshal(wireJob{ID: job.ID, Key: job.Key, Type: job.Type, Payload: job.Payload})
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		MaxLen: q.maxLen,
		Approx: true,
		Values: map[string]any{"job": data},
	}).Err()
}

// Dequeue implements Consumer. It prefers reclaimed retries over new messages,
// then blocks on XREADGROUP. The returned ack XACKs the delivery.
func (q *redisStreamQueue) Dequeue(ctx context.Context) (Job, func() error, bool, error) {
	q.ensureStarted(ctx)

	const blockDur = 2 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return Job{}, nil, false, err
		}

		select {
		case it := <-q.retryCh:
			return it.job, it.ack, true, nil
		default:
		}

		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    q.group,
			Consumer: q.consumer,
			Streams:  []string{q.stream, ">"},
			Count:    1,
			Block:    blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return Job{}, nil, false, ctx.Err()
			}
			return Job{}, nil, false, err
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				job, perr := q.toJob(msg)
				if perr != nil {
					q.toDead(msg, "malformed: "+perr.Error())
					_ = q.ack(msg.ID)
					continue
				}
				return job, q.ackFn(msg.ID), true, nil
			}
		}
	}
}

func (q *redisStreamQueue) ensureStarted(ctx context.Context) {
	q.start.Do(func() {
		ictx, cancel := context.WithTimeout(context.Background(), ackTimeout)
		_ = q.rdb.XGroupCreateMkStream(ictx, q.stream, q.group, "$").Err()
		cancel()
		go q.janitor(ctx)
	})
}

func (q *redisStreamQueue) janitor(ctx context.Context) {
	t := time.NewTicker(q.idleThreshold)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q.reclaimOnce(ctx)
		}
	}
}

func (q *redisStreamQueue) reclaimOnce(ctx context.Context) {
	pending, err := q.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.stream,
		Group:  q.group,
		Idle:   q.idleThreshold,
		Start:  "-",
		End:    "+",
		Count:  32,
	}).Result()
	if err != nil {
		return
	}
	for _, p := range pending {
		claimed, err := q.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   q.stream,
			Group:    q.group,
			Consumer: q.consumer,
			MinIdle:  q.idleThreshold,
			Messages: []string{p.ID},
		}).Result()
		if err != nil || len(claimed) == 0 {
			continue
		}
		msg := claimed[0]

		if p.RetryCount >= int64(q.maxDeliveries) {
			q.toDead(msg, "exceeded max deliveries")
			_ = q.ack(msg.ID)
			continue
		}
		job, perr := q.toJob(msg)
		if perr != nil {
			q.toDead(msg, "malformed: "+perr.Error())
			_ = q.ack(msg.ID)
			continue
		}
		select {
		case q.retryCh <- pendingItem{job: job, ack: q.ackFn(msg.ID)}:
		case <-ctx.Done():
			return
		default:
			// retry buffer full; leave claimed, picked up on a later tick
		}
	}
}

func (q *redisStreamQueue) toDead(msg redis.XMessage, reason string) {
	raw, _ := msg.Values["job"].(string)
	bctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	_ = q.rdb.XAdd(bctx, &redis.XAddArgs{
		Stream: q.deadStream,
		Values: map[string]any{"job": raw, "reason": reason, "orig_id": msg.ID},
	}).Err()
}

func (q *redisStreamQueue) toJob(msg redis.XMessage) (Job, error) {
	raw, ok := msg.Values["job"].(string)
	if !ok {
		return Job{}, errors.New("queue: stream entry missing job field")
	}
	var w wireJob
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return Job{}, err
	}
	return Job{ID: w.ID, Key: w.Key, Type: w.Type, Payload: w.Payload}, nil
}

func (q *redisStreamQueue) ack(id string) error {
	bctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	return q.rdb.XAck(bctx, q.stream, q.group, id).Err()
}

func (q *redisStreamQueue) ackFn(id string) func() error {
	return func() error { return q.ack(id) }
}

// Close releases the underlying Redis client.
func (q *redisStreamQueue) Close() error { return q.rdb.Close() }

var (
	_ Queue    = (*redisStreamQueue)(nil)
	_ Consumer = (*redisStreamQueue)(nil)
)
