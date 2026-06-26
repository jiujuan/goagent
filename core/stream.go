package core

import "iter"

// Stream is the single streaming primitive that composes every layer of
// goagent — Runner, Agent, and the turn engine all return a Stream. It is a
// Go 1.23 range-over-func iterator, so consumers drive it with `for ev, err :=
// range stream { ... }`, and returning early (or `break`) cleanly stops
// upstream production. Cancellation and back-pressure fall out of the pull
// model for free.
type Stream = iter.Seq2[*Event, error]

// Once returns a Stream that yields the given events in order, then ends.
func Once(events ...*Event) Stream {
	return func(yield func(*Event, error) bool) {
		for _, e := range events {
			if !yield(e, nil) {
				return
			}
		}
	}
}

// Fail returns a Stream that yields a single error and ends.
func Fail(err error) Stream {
	return func(yield func(*Event, error) bool) {
		yield(nil, err)
	}
}

// Empty is a Stream that yields nothing.
func Empty() Stream {
	return func(func(*Event, error) bool) {}
}

// Pipe forwards every (event, error) from src into yield, returning false if
// the consumer asked to stop. Helpers that wrap a sub-stream use it to forward
// while preserving early-stop semantics.
func Pipe(src Stream, yield func(*Event, error) bool) bool {
	for ev, err := range src {
		if !yield(ev, err) {
			return false
		}
	}
	return true
}
