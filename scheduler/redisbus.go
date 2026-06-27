package scheduler

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jiujuan/goagent/core"
)

// redisBus is a Bus backed by Redis Pub/Sub: it mirrors MemBus semantics across
// processes. Delivery is fire-and-forget and lossy (a slow subscriber drops
// events; a late subscriber misses earlier ones). Pub/Sub (not a stream) is the
// deliberate choice — the bus must not retain or replay history, matching
// MemBus. The authoritative final result is persisted by the job; subscribers
// read that separately.
type redisBus struct {
	rdb    *redis.Client
	prefix string
}

const busPublishTimeout = 5 * time.Second

func newRedisBus(rdb *redis.Client) *redisBus {
	return &redisBus{rdb: rdb, prefix: "goagent:bus:"}
}

func (b *redisBus) channel(key string) string { return b.prefix + key }

// Publish implements Bus. It serializes ev (core.MarshalEvent) and PUBLISHes it;
// failures are swallowed, matching the lossy, advisory contract.
func (b *redisBus) Publish(key string, ev core.Event) {
	data, err := core.MarshalEvent(ev)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), busPublishTimeout)
	defer cancel()
	_ = b.rdb.Publish(ctx, b.channel(key), data).Err()
}

// Subscribe implements Bus. It SUBSCRIBEs to key's channel and decodes incoming
// events onto a buffered Go channel; a full buffer drops rather than blocks.
func (b *redisBus) Subscribe(key string) (<-chan core.Event, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := b.rdb.Subscribe(ctx, b.channel(key))
	out := make(chan core.Event, 64)

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
				ev, err := core.UnmarshalEvent([]byte(msg.Payload))
				if err != nil {
					continue
				}
				select {
				case out <- ev:
				default: // slow consumer: drop
				}
			}
		}
	}()
	return out, cancel
}

var _ Bus = (*redisBus)(nil)
