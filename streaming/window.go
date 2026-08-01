// Package streaming implements the tumbling-window aggregation job
// described in docs/design_stream_processing.md — processing-time
// windowing over a stream of message timestamps, no event-time/
// watermark handling (explicitly out of scope for this version).
package streaming

import "time"

// Sink receives one emitted window count at a time. A real sink would
// write to a file/log; InMemorySink (below) is what the test suite reads
// directly to assert correctness — same pattern as storage.ColdStore in
// v0.9.
type Sink interface {
	Emit(windowStart time.Time, count int)
}

// InMemorySink is an inspectable Sink for tests.
type InMemorySink struct {
	Counts map[int64]int // keyed by windowStart.Unix()
}

func NewInMemorySink() *InMemorySink {
	return &InMemorySink{Counts: make(map[int64]int)}
}

func (s *InMemorySink) Emit(windowStart time.Time, count int) {
	s.Counts[windowStart.Unix()] = count
}

// ProcessTumblingWindows buckets events into fixed, non-overlapping
// windows of windowSize, then emits one count per window from the
// earliest to the latest window touched by any event — including windows
// with zero events in between, so a gap in traffic is visible as an
// explicit 0, not a missing entry.
func ProcessTumblingWindows(events []time.Time, windowSize time.Duration, sink Sink) {
	if len(events) == 0 {
		return
	}

	counts := make(map[int64]int)
	var minWindow, maxWindow time.Time
	for i, e := range events {
		w := e.Truncate(windowSize)
		counts[w.Unix()]++
		if i == 0 || w.Before(minWindow) {
			minWindow = w
		}
		if i == 0 || w.After(maxWindow) {
			maxWindow = w
		}
	}

	for w := minWindow; !w.After(maxWindow); w = w.Add(windowSize) {
		sink.Emit(w, counts[w.Unix()])
	}
}
