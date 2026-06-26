package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jiujuan/goagent/core"
)

// redisBus is a Bus backed by Redis Pub/Sub. It mirrors MemBus semantics across
// processes: delivery is fire-and-forget and lossy. A slow subscriber's events
// are dropped (never block the publisher), and a subscriber that connects after
// an event was published does not receive it. This fits the queue's contract —
// progress events are advisory and the authoritative final result is persisted
// by the job (e.g. to a session.Store), which subscribers read from there.
//
// Pub/Sub (not a stream) is the deliberate choice: the Bus must not retain or
// replay history, exactly matching MemBus.
type redisBus struct {
	rdb    *redis.Client
	prefix string
}

const busPublishTimeout = 5 * time.Second

func newRedisBus(rdb *redis.Client) *redisBus {
	return &redisBus{rdb: rdb, prefix: "goagent:bus:"}
}

func (b *redisBus) channel(key string) string { return b.prefix + key }

// Publish implements Bus. It serializes ev and PUBLISHes it; delivery failures
// are swallowed, matching the lossy, advisory contract.
func (b *redisBus) Publish(key string, ev *core.Event) {
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), busPublishTimeout)
	defer cancel()
	_ = b.rdb.Publish(ctx, b.channel(key), data).Err()
}

// Subscribe implements Bus. It SUBSCRIBEs to key's channel and decodes incoming
// events onto a buffered Go channel; if that buffer is full (a slow consumer)
// events are dropped rather than blocking. The returned cancel stops the
// subscription and closes the channel.
func (b *redisBus) Subscribe(key string) (<-chan *core.Event, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := b.rdb.Subscribe(ctx, b.channel(key))
	out := make(chan *core.Event, 64)

	go func() {
		defer close(out)
		defer sub.Close()
		in := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-in:
				if !ok {
					return
				}
				var ev core.Event
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					continue
				}
				select {
				case out <- &ev:
				default: // slow subscriber: drop rather than block
				}
			}
		}
	}()

	return out, cancel
}

var _ Bus = (*redisBus)(nil)
