package bus

import (
	"iter"

	"github.com/jiujuan/goagent/core"
)

// Adapt turns a subscriber channel into a pull-style iter.Seq2 — the bridge
// that gives `for ev, err := range stream` ergonomics on top of the push bus.
// It ends the stream on any terminal event: RunFailed yields its error then
// stops; RunDone (success) and Interrupted (paused, awaiting Resume) yield then
// stop.
//
// Adapt does not own the channel's lifecycle: the caller that obtained ch from
// Subscribe is responsible for calling cancel (see Iter for the owned variant).
func Adapt(ch <-chan core.Event) iter.Seq2[core.Event, error] {
	return func(yield func(core.Event, error) bool) {
		for ev := range ch {
			if rf, ok := ev.(core.RunFailed); ok {
				yield(ev, rf.Err)
				return
			}
			if !yield(ev, nil) {
				return
			}
			switch ev.(type) {
			case core.RunDone, core.Interrupted:
				return
			}
		}
	}
}

// Iter Subscribes a Lossless channel on topic, adapts it, and cancels the
// subscription when iteration ends. Use this for a single pull-style consumer
// of one run.
func Iter(b *Bus, topic Topic) iter.Seq2[core.Event, error] {
	return func(yield func(core.Event, error) bool) {
		ch, cancel := b.Subscribe(topic, Lossless)
		defer cancel()
		for ev, err := range Adapt(ch) {
			if !yield(ev, err) {
				return
			}
		}
	}
}
