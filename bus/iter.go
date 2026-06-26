package bus

import (
	"iter"

	"github.com/jiujuan/goagent/event"
)

// Adapt turns a subscriber channel into a pull-style iter.Seq2 — the bridge
// that preserves v1's `for ev, err := range stream` ergonomics on top of v2's
// push bus (ADR 0023). It ends the stream on the terminal events: RunFailed
// yields its error, RunDone yields then stops. Run.Iter() will wrap this over
// its own subscription.
//
// Adapt does not own the channel's lifecycle: the caller that obtained ch from
// Subscribe is responsible for calling cancel (see Iter for the owned variant).
func Adapt(ch <-chan event.Event) iter.Seq2[event.Event, error] {
	return func(yield func(event.Event, error) bool) {
		for ev := range ch {
			if rf, ok := ev.(event.RunFailed); ok {
				yield(ev, rf.Err)
				return
			}
			if !yield(ev, nil) {
				return
			}
			if _, ok := ev.(event.RunDone); ok {
				return
			}
		}
	}
}

// Iter is the convenience form: it Subscribes a Lossless channel on topic,
// adapts it, and cancels the subscription when iteration ends (terminal event
// or early break). Use this for a single pull-style consumer of one run.
func Iter(b *Bus, topic Topic) iter.Seq2[event.Event, error] {
	return func(yield func(event.Event, error) bool) {
		ch, cancel := b.Subscribe(topic, Lossless)
		defer cancel()
		for ev, err := range Adapt(ch) {
			if !yield(ev, err) {
				return
			}
		}
	}
}
