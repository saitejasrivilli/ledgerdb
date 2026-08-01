package benchmarks

import (
	"testing"
	"time"
)

// TestSmoke_MVCCComparisonRuns confirms the comparison harness runs and
// produces plausible output — not a correctness test for MVCC itself
// (that's tests/regression/v1_0_document_store_test.go's job).
func TestSmoke_MVCCComparisonRuns(t *testing.T) {
	result := RunMVCCComparison(4, 200*time.Millisecond)
	if result.LockedReadsPerSec <= 0 {
		t.Fatalf("expected positive locked reads/sec, got %f", result.LockedReadsPerSec)
	}
	if result.MVCCReadsPerSec <= 0 {
		t.Fatalf("expected positive MVCC reads/sec, got %f", result.MVCCReadsPerSec)
	}
}
