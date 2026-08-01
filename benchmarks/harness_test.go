package benchmarks

import (
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/producer"
)

// TestSmoke_ThroughputRuns confirms the benchmark harness itself runs and
// produces plausible, non-empty output — not a correctness test (v0.7
// adds measurement, not new system behavior, per the design doc).
func TestSmoke_ThroughputRuns(t *testing.T) {
	result := RunThroughput(producer.AckAll, "ack=all", 200*time.Millisecond)
	if result.Writes == 0 {
		t.Fatalf("expected at least one write in the smoke run")
	}
	if result.OpsPerSec <= 0 {
		t.Fatalf("expected positive ops/sec, got %f", result.OpsPerSec)
	}
}

func TestSmoke_ChaosRuns(t *testing.T) {
	result := RunChaos()
	if result.DetectionMs <= 0 || result.RecoveryMs <= 0 {
		t.Fatalf("expected positive detection/recovery times, got %+v", result)
	}
	if result.RecoveryMs < result.DetectionMs {
		t.Fatalf("recovery time (%f ms) should be >= detection time (%f ms)", result.RecoveryMs, result.DetectionMs)
	}
}
