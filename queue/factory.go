package queue

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// config is the resolved configuration for New, populated by Options. It is
// unexported: callers touch it only through With* functions.
type config struct {
	// redisURL selects the backend: empty means in-process (MemQueue);
	// non-empty switches to a Redis Streams queue.
	redisURL string

	// In-process queue.
	memSize int

	// Redis Stream queue.
	stream        string
	group         string
	deadStream    string
	idleThreshold time.Duration
	maxDeliveries int
	maxLen        int64
}

// Option configures New.
type Option func(*config)

// WithRedis switches the queue to Redis Streams at the given URL
// (e.g. "redis://localhost:6379/0"). Omit it for the in-process backend.
func WithRedis(url string) Option { return func(c *config) { c.redisURL = url } }

// WithMemSize sets the in-process queue buffer (default 16). Ignored for Redis.
func WithMemSize(n int) Option { return func(c *config) { c.memSize = n } }

// WithStream sets the Redis stream key (default "goagent:jobs").
func WithStream(name string) Option { return func(c *config) { c.stream = name } }

// WithGroup sets the Redis consumer group workers drain from (default "workers").
func WithGroup(name string) Option { return func(c *config) { c.group = name } }

// WithDeadStream sets the dead-letter stream for poison messages
// (default: the main stream + ":dead").
func WithDeadStream(name string) Option { return func(c *config) { c.deadStream = name } }

// WithIdleThreshold sets how long a delivered-but-unacked job waits before it is
// reclaimed for retry. MUST exceed the longest expected job runtime. Default 5m.
func WithIdleThreshold(d time.Duration) Option { return func(c *config) { c.idleThreshold = d } }

// WithMaxDeliveries caps deliveries before a job is treated as poison and routed
// to the dead-letter stream. Default 3.
func WithMaxDeliveries(n int) Option { return func(c *config) { c.maxDeliveries = n } }

// WithMaxLen sets the approximate (MAXLEN ~) cap on the Redis stream length.
// Default 100000.
func WithMaxLen(n int64) Option { return func(c *config) { c.maxLen = n } }

func defaults() *config {
	return &config{
		memSize:       16,
		stream:        "goagent:jobs",
		group:         "workers",
		idleThreshold: 5 * time.Minute,
		maxDeliveries: 3,
		maxLen:        100_000,
	}
}

func apply(opts []Option) *config {
	c := defaults()
	for _, opt := range opts {
		opt(c)
	}
	if c.deadStream == "" {
		c.deadStream = c.stream + ":dead"
	}
	return c
}

// New builds the producer (Queue) and consumer (Consumer) sides of a queue.
// Without WithRedis it returns an in-process MemQueue (one value serves as both);
// with WithRedis it returns a durable Redis Streams queue. Pair the Consumer with
// a Pool (and, for Redis, Pool.WithRegistry).
func New(opts ...Option) (Queue, Consumer, error) {
	c := apply(opts)
	if c.redisURL == "" {
		q := NewMemQueue(c.memSize)
		return q, q, nil
	}
	rdb, err := newRedisClient(c.redisURL)
	if err != nil {
		return nil, nil, err
	}
	q := newRedisStreamQueue(rdb, c)
	return q, q, nil
}

func newRedisClient(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opt), nil
}

// NewBus builds the progress Bus for the same backend as New: an in-process
// MemBus by default, or a Redis Pub/Sub bus with WithRedis(url). Taking the same
// Options lets one WithRedis switch the queue and the bus together.
func NewBus(opts ...Option) (Bus, error) {
	c := apply(opts)
	if c.redisURL == "" {
		return NewMemBus(), nil
	}
	rdb, err := newRedisClient(c.redisURL)
	if err != nil {
		return nil, err
	}
	return newRedisBus(rdb), nil
}
