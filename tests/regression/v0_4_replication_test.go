package regression

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/replication"
)

func makeReplicatedCluster(t *testing.T, n int) (*raft.Network, []*replication.ReplicatedPartition) {
	t.Helper()
	net := raft.MakeNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	nodes := make([]*replication.ReplicatedPartition, n)
	for i := 0; i < n; i++ {
		rp, err := replication.NewReplicatedPartition(net, i, peers, t.TempDir())
		if err != nil {
			t.Fatalf("new replicated partition %d: %v", i, err)
		}
		nodes[i] = rp
	}
	return net, nodes
}

// findLeader polls until exactly one node reports itself leader, returning
// its index. Fails the test if none emerges within the timeout.
func findLeader(t *testing.T, net *raft.Network, nodes []*replication.ReplicatedPartition) int {
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

// proposeAndWait proposes payload via the given leader and waits for it to
// be applied on all currently-connected replicas.
func proposeAndWait(t *testing.T, net *raft.Network, nodes []*replication.ReplicatedPartition, leader int, payload []byte) {
	t.Helper()
	idx, _, isLeader := nodes[leader].Propose(payload)
	if !isLeader {
		t.Fatalf("node %d rejected proposal, not leader anymore", leader)
	}
	for i, n := range nodes {
		if !net.IsConnected(i) {
			continue
		}
		if !n.WaitApplied(idx, 2*time.Second) {
			t.Fatalf("node %d never applied raft index %d", i, idx)
		}
	}
}

func TestV04_KillFollowerAndRecover(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := findLeader(t, net, nodes)
	proposeAndWait(t, net, nodes, leader, []byte("before-failure"))

	follower := -1
	for i := range nodes {
		if i != leader {
			follower = i
			break
		}
	}
	net.Disconnect(follower)

	// with 2 of 3 still up, quorum writes still commit
	proposeAndWait(t, net, nodes, leader, []byte("during-failure-1"))
	proposeAndWait(t, net, nodes, leader, []byte("during-failure-2"))

	net.Connect(follower)
	// give the reconnected follower time to catch up via AppendEntries
	// conflict backtracking (v0.1, unmodified) + the apply loop
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && nodes[follower].NextLocalOffset() < 3 {
		time.Sleep(50 * time.Millisecond)
	}
	if nodes[follower].NextLocalOffset() != 3 {
		t.Fatalf("follower %d never caught up: local offset %d, want 3", follower, nodes[follower].NextLocalOffset())
	}

	want := [][]byte{[]byte("before-failure"), []byte("during-failure-1"), []byte("during-failure-2")}
	for off, w := range want {
		got, err := nodes[follower].ReadLocal(off)
		if err != nil {
			t.Fatalf("follower read offset %d: %v", off, err)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("follower offset %d: got %q want %q", off, got, w)
		}
	}
}

func TestV04_KillLeaderElectNewOne(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader1 := findLeader(t, net, nodes)
	proposeAndWait(t, net, nodes, leader1, []byte("pre-kill"))

	net.Disconnect(leader1)

	var leader2 int
	deadline := time.Now().Add(3 * time.Second)
	for {
		leader2 = -1
		for i, n := range nodes {
			if i == leader1 || !net.IsConnected(i) {
				continue
			}
			if _, isLeader := n.GetState(); isLeader {
				leader2 = i
				break
			}
		}
		if leader2 != -1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader2 == -1 {
		t.Fatalf("no new leader elected after killing leader %d", leader1)
	}

	proposeAndWait(t, net, nodes, leader2, []byte("post-election"))

	net.Connect(leader1)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && nodes[leader1].NextLocalOffset() < 2 {
		time.Sleep(50 * time.Millisecond)
	}
	if nodes[leader1].NextLocalOffset() != 2 {
		t.Fatalf("old leader %d never caught up after rejoining: local offset %d, want 2", leader1, nodes[leader1].NextLocalOffset())
	}

	want := [][]byte{[]byte("pre-kill"), []byte("post-election")}
	for off, w := range want {
		got, err := nodes[leader1].ReadLocal(off)
		if err != nil {
			t.Fatalf("rejoined node read offset %d: %v", off, err)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("rejoined node offset %d: got %q want %q", off, got, w)
		}
	}
}

func TestV04_CommittedWritesSurviveAcrossReplicas(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := findLeader(t, net, nodes)
	for i := 0; i < 10; i++ {
		proposeAndWait(t, net, nodes, leader, []byte(fmt.Sprintf("entry-%d", i)))
	}

	for off := 0; off < 10; off++ {
		var first []byte
		for i, n := range nodes {
			got, err := n.ReadLocal(off)
			if err != nil {
				t.Fatalf("node %d read offset %d: %v", i, off, err)
			}
			if first == nil {
				first = got
			} else if !bytes.Equal(first, got) {
				t.Fatalf("replicas disagree at offset %d: %q vs %q", off, first, got)
			}
		}
	}
}
