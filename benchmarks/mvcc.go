package benchmarks

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saitejasrivilli/ledgerdb/docstore"
	"github.com/saitejasrivilli/ledgerdb/raft"
)

// MVCCComparisonResult holds measured concurrent-read throughput for both
// the lock-based and MVCC document stores under identical load — the
// number that makes "MVCC helped" citable (docs/design_mvcc.md).
type MVCCComparisonResult struct {
	LockedReadsPerSec float64 `json:"locked_reads_per_sec"`
	MVCCReadsPerSec   float64 `json:"mvcc_reads_per_sec"`
	Speedup           float64 `json:"speedup"`
	ConcurrentReaders int     `json:"concurrent_readers"`
	DurationSec       float64 `json:"duration_sec"`
}

func docStoreCluster(n int, indexField string) (*raft.Network, []*docstore.ReplicatedDocStore, func()) {
	net := raft.MakeNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	nodes := make([]*docstore.ReplicatedDocStore, n)
	for i := 0; i < n; i++ {
		dir, err := makeTempDir()
		if err != nil {
			panic(err)
		}
		ds, err := docstore.NewReplicatedDocStore(net, i, peers, dir, indexField)
		if err != nil {
			panic(err)
		}
		nodes[i] = ds
	}
	cleanup := func() {
		for _, n := range nodes {
			n.Close()
		}
	}
	return net, nodes, cleanup
}

func lockedDocStoreCluster(n int) (*raft.Network, []*docstore.ReplicatedLockedDocStore, func()) {
	net := raft.MakeNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	nodes := make([]*docstore.ReplicatedLockedDocStore, n)
	for i := 0; i < n; i++ {
		dir, err := makeTempDir()
		if err != nil {
			panic(err)
		}
		ds, err := docstore.NewReplicatedLockedDocStore(net, i, peers, dir)
		if err != nil {
			panic(err)
		}
		nodes[i] = ds
	}
	cleanup := func() {
		for _, n := range nodes {
			n.Close()
		}
	}
	return net, nodes, cleanup
}

func findMVCCLeader(net *raft.Network, nodes []*docstore.ReplicatedDocStore) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if net.IsConnected(i) {
				if _, isLeader := n.GetState(); isLeader {
					return i
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("no leader elected")
}

func findLockedLeader(net *raft.Network, nodes []*docstore.ReplicatedLockedDocStore) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if net.IsConnected(i) {
				if _, isLeader := n.GetState(); isLeader {
					return i
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("no leader elected")
}

// RunMVCCComparison measures concurrent-read throughput on both store
// variants under identical load: concurrentReaders goroutines hammering
// Get while a steady stream of writes commits in the background.
func RunMVCCComparison(concurrentReaders int, duration time.Duration) MVCCComparisonResult {
	lockedReads := runLockedReadBenchmark(concurrentReaders, duration)
	mvccReads := runMVCCReadBenchmark(concurrentReaders, duration)

	speedup := 0.0
	if lockedReads > 0 {
		speedup = mvccReads / lockedReads
	}

	return MVCCComparisonResult{
		LockedReadsPerSec: lockedReads,
		MVCCReadsPerSec:   mvccReads,
		Speedup:           speedup,
		ConcurrentReaders: concurrentReaders,
		DurationSec:       duration.Seconds(),
	}
}

func runLockedReadBenchmark(concurrentReaders int, duration time.Duration) float64 {
	net, nodes, cleanup := lockedDocStoreCluster(3)
	defer cleanup()
	leader := findLockedLeader(net, nodes)

	idx, isLeader, err := nodes[leader].Propose([]docstore.Mutation{
		{Op: "put", ID: "doc-1", Data: []byte(`{"n":0}`)},
	})
	if err != nil || !isLeader || !nodes[leader].WaitApplied(idx, 2*time.Second) {
		panic("locked benchmark setup write failed")
	}

	stop := make(chan struct{})
	var writeCount int64
	go func() {
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			n++
			nodes[leader].Propose([]docstore.Mutation{
				{Op: "put", ID: "doc-1", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))},
			})
			atomic.AddInt64(&writeCount, 1)
			time.Sleep(time.Millisecond)
		}
	}()

	var totalReads int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrentReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < duration {
				nodes[leader].Get("doc-1")
				atomic.AddInt64(&totalReads, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(stop)

	return float64(totalReads) / elapsed.Seconds()
}

func runMVCCReadBenchmark(concurrentReaders int, duration time.Duration) float64 {
	net, nodes, cleanup := docStoreCluster(3, "")
	defer cleanup()
	leader := findMVCCLeader(net, nodes)

	idx, isLeader, err := nodes[leader].Propose([]docstore.Mutation{
		{Op: "put", ID: "doc-1", Data: []byte(`{"n":0}`)},
	})
	if err != nil || !isLeader || !nodes[leader].WaitApplied(idx, 2*time.Second) {
		panic("mvcc benchmark setup write failed")
	}

	stop := make(chan struct{})
	go func() {
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			n++
			nodes[leader].Propose([]docstore.Mutation{
				{Op: "put", ID: "doc-1", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))},
			})
			time.Sleep(time.Millisecond)
		}
	}()

	var totalReads int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrentReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < duration {
				snap := nodes[leader].Snapshot()
				snap.Get("doc-1")
				atomic.AddInt64(&totalReads, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(stop)

	return float64(totalReads) / elapsed.Seconds()
}
