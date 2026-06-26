// Package sse provides a minimal Server-Sent Events reader as a Go 1.23
// iterator. It is shared by the streaming model providers, which differ only in
// how they interpret each event's data payload.
package sse

import (
	"bufio"
	"io"
	"iter"
	"strings"
)

// Event is one parsed SSE event: an optional name (the "event:" field) and the
// accumulated "data:" payload.
type Event struct {
	Name string
	Data string
}

// Scan reads SSE events from r until EOF, yielding each complete event. Events
// are separated by a blank line; "data:" fields accumulate across lines, and
// ":" comment lines are ignored.
func Scan(r io.Reader) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		sc := bufio.NewScanner(r)
		// Allow large payloads (tool-call JSON can be sizeable).
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

		var ev Event
		flush := func() bool {
			if ev.Name == "" && ev.Data == "" {
				return true
			}
			ok := yield(ev, nil)
			ev = Event{}
			return ok
		}

		for sc.Scan() {
			line := strings.TrimSuffix(sc.Text(), "\r")
			if line == "" {
				if !flush() {
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue // comment
			}
			field, val, found := strings.Cut(line, ":")
			if !found {
				field = line
				val = ""
			}
			val = strings.TrimPrefix(val, " ")
			switch field {
			case "event":
				ev.Name = val
			case "data":
				if ev.Data != "" {
					ev.Data += "\n"
				}
				ev.Data += val
			}
		}
		if err := sc.Err(); err != nil {
			yield(Event{}, err)
			return
		}
		flush()
	}
}
