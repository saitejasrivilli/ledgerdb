package regression

import (
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/streaming"
)

// TestV13_WindowedAggregationCorrectness feeds a hand-constructed
// sequence of timestamps spanning several one-minute windows, including
// one window with zero messages and messages landing exactly on a window
// boundary, and asserts the emitted counts match hand-computed values
// exactly.
func TestV13_WindowedAggregationCorrectness(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	windowSize := time.Minute

	events := []time.Time{
		base.Add(10 * time.Second), // window 0 (00:00)
		base.Add(50 * time.Second), // window 0
		base.Add(70 * time.Second), // window 1 (00:01)
		// window 2 (00:02) deliberately gets zero events
		base.Add(3 * time.Minute),               // window 3, exactly on the boundary
		base.Add(3*time.Minute + 5*time.Second), // window 3
	}

	sink := streaming.NewInMemorySink()
	streaming.ProcessTumblingWindows(events, windowSize, sink)

	expected := map[int64]int{
		base.Add(0 * time.Minute).Unix(): 2,
		base.Add(1 * time.Minute).Unix(): 1,
		base.Add(2 * time.Minute).Unix(): 0,
		base.Add(3 * time.Minute).Unix(): 2,
	}

	if len(sink.Counts) != len(expected) {
		t.Fatalf("got %d windows emitted, want %d: %v", len(sink.Counts), len(expected), sink.Counts)
	}
	for windowUnix, wantCount := range expected {
		gotCount, ok := sink.Counts[windowUnix]
		if !ok {
			t.Fatalf("window %v never emitted", time.Unix(windowUnix, 0).UTC())
		}
		if gotCount != wantCount {
			t.Fatalf("window %v: got count %d, want %d", time.Unix(windowUnix, 0).UTC(), gotCount, wantCount)
		}
	}
}

func TestV13_SingleEventEmitsOneWindow(t *testing.T) {
	base := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	sink := streaming.NewInMemorySink()
	streaming.ProcessTumblingWindows([]time.Time{base.Add(3 * time.Second)}, time.Minute, sink)

	if len(sink.Counts) != 1 {
		t.Fatalf("expected exactly 1 window emitted, got %d", len(sink.Counts))
	}
	if sink.Counts[base.Unix()] != 1 {
		t.Fatalf("expected count 1 for the sole window, got %d", sink.Counts[base.Unix()])
	}
}

func TestV13_NoEventsEmitsNoWindows(t *testing.T) {
	sink := streaming.NewInMemorySink()
	streaming.ProcessTumblingWindows(nil, time.Minute, sink)
	if len(sink.Counts) != 0 {
		t.Fatalf("expected no windows emitted for an empty event stream, got %d", len(sink.Counts))
	}
}
