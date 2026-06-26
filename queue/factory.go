package queue

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// config is the resolved configuration for New and NewBus, populated by Options.
// It is unexported: callers only ever touch it through With* functions, so new
// knobs can be added without breaking existing call sites.
type config struct {
	// redisURL selects the backend: empty means in-process (MemQueue + MemBus);
	// non-empty switches both the queue and the bus to Redis.
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

	// Shared.
	registry Registry
}

// Option configures New / NewBus.
type Option func(*config)

// WithRedis switches both the queue and the bus to Redis at the given URL
// (e.g. "redis://localhost:6379/0"). Omit it to use the in-process backend.
func WithRedis(url string) Option { return func(c *config) { c.redisURL = url } }

// WithMemSize sets the in-process queue's buffer (pending jobs before Enqueue
// blocks). Ignored by the Redis backend. Default 16.
func WithMemSize(n int) Option { return func(c *config) { c.memSize = n } }

// WithStream sets the Redis stream key jobs are stored on. Default "goagent:jobs".
func WithStream(name string) Option { return func(c *config) { c.stream = name } }

// WithGroup sets the Redis consumer group workers drain from. Default "workers".
func WithGroup(name string) Option { return func(c *config) { c.group = name } }

// WithDeadStream sets the dead-letter stream poison messages are moved to.
// Default: the main stream with a ":dead" suffix.
func WithDeadStream(name string) Option { return func(c *config) { c.deadStream = name } }

// WithIdleThreshold sets how long a delivered-but-unacked job waits before it is
// reclaimed for retry. It MUST exceed the longest expected job runtime, or a
// still-running job is reclaimed and run twice. Default 5m.
func WithIdleThreshold(d time.Duration) Option { return func(c *config) { c.idleThreshold = d } }

// WithMaxDeliveries caps how many times a job may be delivered before it is
// treated as poison and routed to the dead-letter stream. Default 3.
func WithMaxDeliveries(n int) Option { return func(c *config) { c.maxDeliveries = n } }

// WithMaxLen sets the approximate (MAXLEN ~) cap on the Redis stream length so
// it does not grow unbounded. Keep it well above the in-flight backlog. Default
// 100000.
func WithMaxLen(n int64) Option { return func(c *config) { c.maxLen = n } }

// WithRegistry supplies the Job.Type -> Handler map the worker uses to rebuild
// serialized (Redis) jobs. Required for the Redis backend; unused in-process.
func WithRegistry(reg Registry) Option { return func(c *config) { c.registry = reg } }

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
		c.deadStream = c.stream + ":dead" // depends on stream; resolve after opts
	}
	return c
}

// New builds the producer (Queue) and consumer (Consumer) sides of a queue. With
// no WithRedis option it returns an in-process MemQueue (the same value serves as
// both Queue and Consumer); with WithRedis it returns a Redis Streams queue.
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

// NewBus builds the event Bus for the same backend as New. It takes the same
// Options so a single WithRedis switches the queue and the bus together,
// preventing a Redis queue from being paired with an in-process bus by mistake.
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

func newRedisClient(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opt), nil
}
