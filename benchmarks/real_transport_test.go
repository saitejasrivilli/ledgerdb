package benchmarks

import "testing"

// TestSmoke_RealTransportChaosRuns confirms the real-transport chaos
// harness runs and produces plausible output — not a correctness test
// (that's tests/regression/v0_14_real_transport_test.go's job).
func TestSmoke_RealTransportChaosRuns(t *testing.T) {
	result := RunRealTransportChaos()
	if result.DetectionMs <= 0 || result.RecoveryMs <= 0 {
		t.Fatalf("expected positive detection/recovery times, got %+v", result)
	}
	if result.RecoveryMs < result.DetectionMs {
		t.Fatalf("recovery time (%f ms) should be >= detection time (%f ms)", result.RecoveryMs, result.DetectionMs)
	}
}
