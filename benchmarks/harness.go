// Package benchmarks implements the load generator and chaos harness
// described in docs/design_benchmarking.md. Every number this package
// produces comes from an actual run — nothing here is estimated.
package benchmarks

import (
	"sort"
	"time"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/replication"
)

// ThroughputResult holds measured throughput/latency for one ack level.
type ThroughputResult struct {
	AckLevel    string  `json:"ack_level"`
	Writes      int     `json:"writes"`
	DurationSec float64 `json:"duration_sec"`
	OpsPerSec   float64 `json:"ops_per_sec"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
}

// ChaosResult holds measured leader-failure recovery timing.
type ChaosResult struct {
	DetectionMs float64 `json:"detection_ms"`
	RecoveryMs  float64 `json:"recovery_ms"`
}

// BaselineResults is the full v0.7 baseline written to
// benchmarks/results/v0.7_baseline.json.
type BaselineResults struct {
	GeneratedAt string             `json:"generated_at"`
	Throughput  []ThroughputResult `json:"throughput"`
	Chaos       ChaosResult        `json:"chaos"`
}

func setupCluster(n int) (*raft.Network, []*replication.ReplicatedPartition, func()) {
	net := raft.MakeNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	nodes := make([]*replication.ReplicatedPartition, n)
	dirs := make([]string, n)
	for i := 0; i < n; i++ {
		dir, err := makeTempDir()
		if err != nil {
			panic(err)
		}
		dirs[i] = dir
		rp, err := replication.NewReplicatedPartition(net, i, peers, dir)
		if err != nil {
			panic(err)
		}
		nodes[i] = rp
	}
	cleanup := func() {
		for _, n := range nodes {
			n.Close()
		}
	}
	return net, nodes, cleanup
}

func findLeader(net *raft.Network, nodes []*replication.ReplicatedPartition, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if !net.IsConnected(i) {
				continue
			}
			if _, isLeader := n.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

// RunThroughput drives writes against the current leader at ack for the
// given duration, returning measured ops/sec and latency percentiles.
func RunThroughput(ack producer.AckLevel, ackName string, duration time.Duration) ThroughputResult {
	net, nodes, cleanup := setupCluster(3)
	defer cleanup()

	leader := findLeader(net, nodes, 3*time.Second)
	if leader == -1 {
		panic("no leader elected during throughput benchmark setup")
	}

	var latencies []time.Duration
	start := time.Now()
	for time.Since(start) < duration {
		writeStart := time.Now()
		_, err := producer.Write(nodes[leader], []byte("benchmark-payload"), ack)
		if err != nil {
			// leader may have changed under sustained load in later
			// versions; for this in-process single-burst benchmark we
			// just re-resolve and continue rather than abort the run
			leader = findLeader(net, nodes, 1*time.Second)
			if leader == -1 {
				break
			}
			continue
		}
		latencies = append(latencies, time.Since(writeStart))
	}
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	return ThroughputResult{
		AckLevel:    ackName,
		Writes:      len(latencies),
		DurationSec: elapsed.Seconds(),
		OpsPerSec:   float64(len(latencies)) / elapsed.Seconds(),
		P50Ms:       percentileMs(latencies, 0.50),
		P95Ms:       percentileMs(latencies, 0.95),
		P99Ms:       percentileMs(latencies, 0.99),
	}
}

func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Microseconds()) / 1000.0
}

// RunChaos kills the current leader mid-cluster and measures actual
// wall-clock time to detect a new leader and to complete the first
// successful write afterward.
func RunChaos() ChaosResult {
	net, nodes, cleanup := setupCluster(3)
	defer cleanup()

	leader := findLeader(net, nodes, 3*time.Second)
	if leader == -1 {
		panic("no leader elected during chaos benchmark setup")
	}
	// warm up with a few writes so the cluster is in steady state
	for i := 0; i < 5; i++ {
		producer.Write(nodes[leader], []byte("warmup"), producer.AckAll)
	}

	killedAt := time.Now()
	net.Disconnect(leader)

	newLeader := -1
	deadline := time.Now().Add(5 * time.Second)
	for newLeader == -1 && time.Now().Before(deadline) {
		for i, n := range nodes {
			if i == leader || !net.IsConnected(i) {
				continue
			}
			if _, isLeader := n.GetState(); isLeader {
				newLeader = i
				break
			}
		}
		if newLeader == -1 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	detectedAt := time.Now()
	if newLeader == -1 {
		panic("no new leader elected within chaos benchmark deadline")
	}

	var recoveredAt time.Time
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := producer.Write(nodes[newLeader], []byte("post-kill"), producer.AckAll)
		if err == nil {
			recoveredAt = time.Now()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if recoveredAt.IsZero() {
		panic("no successful write completed within chaos benchmark deadline")
	}

	return ChaosResult{
		DetectionMs: float64(detectedAt.Sub(killedAt).Microseconds()) / 1000.0,
		RecoveryMs:  float64(recoveredAt.Sub(killedAt).Microseconds()) / 1000.0,
	}
}
