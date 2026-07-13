package queue

import (
	"context"
	"fmt"
	"sync"
)

// Pool is a bounded worker pool draining a Consumer.
type Pool struct {
	c           Consumer
	concurrency int
	registry    Registry
}

// NewPool builds a worker pool running up to concurrency jobs at once (min 1).
func NewPool(c Consumer, concurrency int) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Pool{c: c, concurrency: concurrency}
}

// WithRegistry supplies the Type -> Handler map used to run serialized (Redis)
// jobs. Unused for in-process Run jobs. Returns the pool for chaining.
func (p *Pool) WithRegistry(reg Registry) *Pool {
	p.registry = reg
	return p
}

// Run drains the queue, running jobs concurrently up to the pool's limit. It
// acks a job only on success, so a failed Redis job is redelivered. It blocks
// until ctx is cancelled or the queue is closed and drained, then waits for
// in-flight jobs.
func (p *Pool) Run(ctx context.Context) {
	sem := make(chan struct{}, p.concurrency)
	var wg sync.WaitGroup
	for {
		// Acquire a slot before dequeuing, so in-flight work never exceeds the limit.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		job, ack, ok, err := p.c.Dequeue(ctx)
		if err != nil || !ok {
			<-sem
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(job Job, ack func() error) {
			defer wg.Done()
			defer func() { <-sem }()
			if execErr := p.exec(ctx, job); execErr == nil && ack != nil {
				_ = ack()
			}
		}(job, ack)
	}
}

func (p *Pool) exec(ctx context.Context, job Job) error {
	if job.Run != nil {
		return job.Run(ctx)
	}
	if h := p.registry[job.Type]; h != nil {
		return h(ctx, job.Payload)
	}
	return fmt.Errorf("queue: no handler for job type %q", job.Type)
}
