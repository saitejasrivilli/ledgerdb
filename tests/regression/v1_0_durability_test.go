package regression

import (
	"strconv"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/docstore"
)

// TestV1_0_HundredKillAndRecoverCyclesNoDataLoss is the durability check
// named in the versioned build plan for v1.0: repeated kill-and-recover
// cycles against the document store, confirming zero data loss across
// all of them. Scaled to 100 cycles of a 3-node cluster kill/rejoin
// (not 100 full process restarts, which the in-process Network doesn't
// model) — each cycle disconnects the current leader, waits for a new
// election, writes through the new leader, then reconnects the old
// leader and confirms every previously committed document is still
// present and correct before starting the next cycle.
func TestV1_0_HundredKillAndRecoverCyclesNoDataLoss(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	written := make(map[string]string)
	const cycles = 100

	for cycle := 0; cycle < cycles; cycle++ {
		leader := findDocStoreLeader(t, net, nodes)

		id := docIDFor(cycle)
		value := docValueFor(cycle)
		mustPropose(t, nodes[leader], []docstore.Mutation{
			{Op: "put", ID: id, Data: []byte(`{"v":"` + value + `"}`)},
		})
		written[id] = value

		net.Disconnect(leader)

		newLeader := -1
		// generous deadline: election timeout is randomized 300-600ms
		// (raft/raft.go), but under full-suite parallel test load (many
		// other Raft clusters competing for CPU) a single cycle can take
		// noticeably longer to resolve — seen directly: 5/5 clean in
		// isolation, one flake in 100 cycles under full `go test ./...`
		// contention. This is CPU-scheduling noise, not a correctness
		// issue, so the fix is a deadline that tolerates real contention
		// rather than a tighter one that fails under it.
		deadline := time.Now().Add(6 * time.Second)
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
			time.Sleep(10 * time.Millisecond)
		}
		if newLeader == -1 {
			t.Fatalf("cycle %d: no new leader elected after disconnecting %d", cycle, leader)
		}

		net.Connect(leader)

		// confirm every document written so far is still correct on the
		// new leader before moving to the next cycle
		if !nodes[newLeader].WaitApplied(mustLatestIndex(t, nodes[newLeader]), 6*time.Second) {
			t.Fatalf("cycle %d: new leader never caught up", cycle)
		}
		snap := nodes[newLeader].Snapshot()
		for wantID, wantValue := range written {
			doc, ok := snap.Get(wantID)
			if !ok {
				t.Fatalf("cycle %d: document %q lost after kill/recover cycle", cycle, wantID)
			}
			if doc["v"] != wantValue {
				t.Fatalf("cycle %d: document %q = %v, want %q", cycle, wantID, doc["v"], wantValue)
			}
		}
	}
}

func docIDFor(cycle int) string {
	return "doc-" + strconv.Itoa(cycle)
}

func docValueFor(cycle int) string {
	return "value-" + strconv.Itoa(cycle)
}

// mustLatestIndex proposes a no-op marker write and returns its index, so
// the caller has a concrete raft index to WaitApplied on for "caught up."
func mustLatestIndex(t *testing.T, ds *docstore.ReplicatedDocStore) int {
	t.Helper()
	idx, isLeader, err := ds.Propose([]docstore.Mutation{{Op: "put", ID: "__marker__", Data: []byte(`{}`)}})
	if err != nil || !isLeader {
		return 0
	}
	return idx
}
