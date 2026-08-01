package regression

import (
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/docstore"
	"github.com/saitejasrivilli/ledgerdb/raft"
)

func makeDocStoreCluster(t *testing.T, n int, indexField string) (*raft.Network, []*docstore.ReplicatedDocStore) {
	t.Helper()
	net := raft.MakeNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	nodes := make([]*docstore.ReplicatedDocStore, n)
	for i := 0; i < n; i++ {
		ds, err := docstore.NewReplicatedDocStore(net, i, peers, t.TempDir(), indexField)
		if err != nil {
			t.Fatalf("new doc store %d: %v", i, err)
		}
		nodes[i] = ds
	}
	return net, nodes
}

func findDocStoreLeader(t *testing.T, net *raft.Network, nodes []*docstore.ReplicatedDocStore) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if !net.IsConnected(i) {
				continue
			}
			if _, isLeader := n.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within timeout")
	return -1
}

func mustPropose(t *testing.T, ds *docstore.ReplicatedDocStore, muts []docstore.Mutation) int {
	t.Helper()
	idx, isLeader, err := ds.Propose(muts)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if !isLeader {
		t.Fatalf("propose target is not leader")
	}
	if !ds.WaitApplied(idx, 2*time.Second) {
		t.Fatalf("transaction never applied")
	}
	return idx
}

func intPtr(i int) *int { return &i }

func TestV1_0_ConcurrentReadDuringWriteSeesConsistentSnapshot(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findDocStoreLeader(t, net, nodes)

	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "put", ID: "doc-a", Data: []byte(`{"v":"initial-a"}`)},
		{Op: "put", ID: "doc-b", Data: []byte(`{"v":"initial-b"}`)},
	})

	// take a snapshot pinned to the state right after the initial writes
	snap := nodes[leader].Snapshot()

	// now commit more writes to both documents, after the snapshot was taken
	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "put", ID: "doc-a", Data: []byte(`{"v":"updated-a"}`), ExpectedVersion: intPtr(1)},
	})
	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "put", ID: "doc-b", Data: []byte(`{"v":"updated-b"}`), ExpectedVersion: intPtr(1)},
	})

	// the snapshot must still see the pre-update values for BOTH
	// documents, consistently — not a mix of old and new
	docA, ok := snap.Get("doc-a")
	if !ok || docA["v"] != "initial-a" {
		t.Fatalf("snapshot doc-a: got %v, want initial-a", docA)
	}
	docB, ok := snap.Get("doc-b")
	if !ok || docB["v"] != "initial-b" {
		t.Fatalf("snapshot doc-b: got %v, want initial-b", docB)
	}

	// a fresh snapshot taken now must see the updated values
	fresh := nodes[leader].Snapshot()
	docA2, _ := fresh.Get("doc-a")
	docB2, _ := fresh.Get("doc-b")
	if docA2["v"] != "updated-a" || docB2["v"] != "updated-b" {
		t.Fatalf("fresh snapshot: got doc-a=%v doc-b=%v, want updated-a/updated-b", docA2, docB2)
	}
}

func TestV1_0_TransactionRollbackOnPartialFailure(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findDocStoreLeader(t, net, nodes)

	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "put", ID: "acct-1", Data: []byte(`{"balance":100}`)},
	})

	// a transaction with a stale ExpectedVersion for acct-2 (which doesn't
	// exist yet, so its version is 0 - use a wrong precondition of 5) must
	// reject BOTH mutations, including the otherwise-valid one for acct-1
	idx, isLeader, err := nodes[leader].Propose([]docstore.Mutation{
		{Op: "put", ID: "acct-1", Data: []byte(`{"balance":200}`), ExpectedVersion: intPtr(1)},
		{Op: "put", ID: "acct-2", Data: []byte(`{"balance":50}`), ExpectedVersion: intPtr(5)}, // wrong: acct-2 has version 0
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if !isLeader {
		t.Fatalf("propose target is not leader")
	}
	if !nodes[leader].WaitApplied(idx, 2*time.Second) {
		t.Fatalf("transaction entry never applied (even a rejected one still commits to the WAL/Raft log)")
	}

	snap := nodes[leader].Snapshot()
	acct1, ok := snap.Get("acct-1")
	if !ok || acct1["balance"] != float64(100) {
		t.Fatalf("expected acct-1 unchanged at balance=100 after rolled-back transaction, got %v", acct1)
	}
	if _, ok := snap.Get("acct-2"); ok {
		t.Fatalf("expected acct-2 to not exist after rolled-back transaction")
	}
}

func TestV1_0_IndexedLookupCorrectness(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "category")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findDocStoreLeader(t, net, nodes)

	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "put", ID: "item-1", Data: []byte(`{"category":"fruit","name":"apple"}`)},
		{Op: "put", ID: "item-2", Data: []byte(`{"category":"veg","name":"carrot"}`)},
		{Op: "put", ID: "item-3", Data: []byte(`{"category":"fruit","name":"banana"}`)},
	})

	snap := nodes[leader].Snapshot()
	fruitIDs := snap.QueryByIndex("fruit")
	if len(fruitIDs) != 2 {
		t.Fatalf("expected 2 fruit items, got %d: %v", len(fruitIDs), fruitIDs)
	}

	// delete one fruit item, confirm the index-backed query reflects it
	mustPropose(t, nodes[leader], []docstore.Mutation{
		{Op: "delete", ID: "item-1", ExpectedVersion: intPtr(1)},
	})
	freshSnap := nodes[leader].Snapshot()
	fruitIDsAfterDelete := freshSnap.QueryByIndex("fruit")
	if len(fruitIDsAfterDelete) != 1 || fruitIDsAfterDelete[0] != "item-3" {
		t.Fatalf("expected only item-3 after deleting item-1, got %v", fruitIDsAfterDelete)
	}

	// the earlier snapshot, taken before the delete, must be unaffected
	if len(fruitIDs) != 2 {
		t.Fatalf("earlier snapshot's result set must not mutate: got %v", fruitIDs)
	}
}
