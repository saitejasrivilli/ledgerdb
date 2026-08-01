package regression

import (
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/producer"
)

// TestV06_AckAllSurvivesLeaderCrash is the hard invariant: a write that
// returned successfully under ack=all must be present on whichever
// replica the cluster elects as the new leader after the original leader
// is killed. Any failure here is a correctness bug, not a semantics note.
func TestV06_AckAllSurvivesLeaderCrash(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := findLeader(t, net, nodes)
	idx, err := producer.Write(nodes[leader], []byte("must-survive"), producer.AckAll)
	if err != nil {
		t.Fatalf("ack=all write failed: %v", err)
	}

	net.Disconnect(leader)

	var newLeader int = -1
	deadline := time.Now().Add(3 * time.Second)
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
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == -1 {
		t.Fatalf("no new leader elected after killing original leader %d", leader)
	}

	if !nodes[newLeader].WaitApplied(idx, 2*time.Second) {
		t.Fatalf("ack=all entry (raft index %d) never applied on new leader %d — durability violated", idx, newLeader)
	}
	got, err := nodes[newLeader].ReadLocal(0)
	if err != nil {
		t.Fatalf("read offset 0 on new leader: %v", err)
	}
	if string(got) != "must-survive" {
		t.Fatalf("new leader offset 0 = %q, want %q", got, "must-survive")
	}
}

// TestV06_AckLeaderCanLoseDataOnLeaderCrash demonstrates the documented
// weakness of ack=1: if the leader is isolated from all followers before
// a write, the write can return success (leader accepted it locally) yet
// never reach any surviving replica once the leader is killed.
func TestV06_AckLeaderCanLoseDataOnLeaderCrash(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := findLeader(t, net, nodes)
	var followers []int
	for i := range nodes {
		if i != leader {
			followers = append(followers, i)
		}
	}

	// isolate the leader from both followers before the write, so the
	// entry can never replicate — Network.Call checks both ends'
	// connected state, so disconnecting the followers already blocks the
	// leader's outbound RPCs to them.
	for _, f := range followers {
		net.Disconnect(f)
	}

	_, err := producer.Write(nodes[leader], []byte("may-be-lost"), producer.AckLeader)
	if err != nil {
		t.Fatalf("ack=1 write should succeed locally on the leader (isolated or not): %v", err)
	}

	// now kill the leader for good and let the followers, which never saw
	// the entry, reconnect and elect among themselves
	net.Disconnect(leader)
	for _, f := range followers {
		net.Connect(f)
	}

	var newLeader int = -1
	deadline := time.Now().Add(3 * time.Second)
	for newLeader == -1 && time.Now().Before(deadline) {
		for _, f := range followers {
			if _, isLeader := nodes[f].GetState(); isLeader {
				newLeader = f
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == -1 {
		t.Fatalf("no new leader elected among surviving followers")
	}

	// the entry must NOT be present — this is the point being demonstrated
	if nodes[newLeader].NextLocalOffset() != 0 {
		t.Fatalf("expected ack=1 entry to be lost (new leader offset 0), got offset %d — if this now passes, ack=1's documented weakness no longer holds and the design doc needs updating", nodes[newLeader].NextLocalOffset())
	}
}

// TestV06_AckNoneNeverBlocksOrPanics: ack=0 makes no durability promise,
// but it must never block indefinitely or return a panic/error the caller
// can't handle, even against a non-leader.
func TestV06_AckNoneNeverBlocksOrPanics(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := findLeader(t, net, nodes)
	follower := -1
	for i := range nodes {
		if i != leader {
			follower = i
			break
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := producer.Write(nodes[follower], []byte("fire-and-forget"), producer.AckNone)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ack=0 should never surface an error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("ack=0 write blocked, expected immediate return")
	}
}
