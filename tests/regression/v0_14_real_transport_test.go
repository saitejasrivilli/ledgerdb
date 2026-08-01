package regression

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
)

// freePorts grabs n OS-assigned free ports by briefly listening and
// closing — a standard test pattern (net/httptest uses the same trick)
// to get real port numbers before the real listeners that will use them
// are created.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		ports[i] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	return ports
}

type tcpCluster struct {
	transports []*raft.TCPTransport
	rafts      []*raft.Raft
	applyChs   []chan raft.ApplyMsg
}

func makeTCPCluster(t *testing.T, n int) *tcpCluster {
	t.Helper()
	ports := freePorts(t, n)
	addrs := make(map[int]string, n)
	for i, p := range ports {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", p)
	}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	c := &tcpCluster{
		transports: make([]*raft.TCPTransport, n),
		rafts:      make([]*raft.Raft, n),
		applyChs:   make([]chan raft.ApplyMsg, n),
	}
	for i := 0; i < n; i++ {
		transport, svc, err := raft.NewTCPTransport(i, addrs)
		if err != nil {
			t.Fatalf("new tcp transport %d: %v", i, err)
		}
		applyCh := make(chan raft.ApplyMsg, 256)
		rf := raft.MakeWithTransport(transport, i, peers, applyCh)
		svc.Bind(rf)

		c.transports[i] = transport
		c.rafts[i] = rf
		c.applyChs[i] = applyCh
	}
	return c
}

func (c *tcpCluster) cleanup() {
	for i, rf := range c.rafts {
		rf.Kill()
		c.transports[i].Close()
	}
}

func (c *tcpCluster) findLeader(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, rf := range c.rafts {
			if _, isLeader := rf.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

// TestRealTransportElectsLeaderAndCommits confirms the basics work over
// real sockets, not just the in-process simulation every other test uses.
func TestRealTransportElectsLeaderAndCommits(t *testing.T) {
	c := makeTCPCluster(t, 3)
	defer c.cleanup()

	leader := c.findLeader(t, 3*time.Second)
	if leader == -1 {
		t.Fatalf("no leader elected over real TCP transport")
	}

	idx, _, isLeader := c.rafts[leader].Start([]byte("hello-over-tcp"))
	if !isLeader {
		t.Fatalf("leader rejected Start")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case msg := <-c.applyChs[leader]:
			if msg.CommandIndex == idx {
				if string(msg.Command.([]byte)) != "hello-over-tcp" {
					t.Fatalf("applied command = %q, want %q", msg.Command, "hello-over-tcp")
				}
				return
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("entry never applied over real transport")
}

// TestRealNetworkPartitionOnlyMajoritySideCommits is the test this
// version exists to add: a real network partition (whichever node is
// leader at partition time, cut off from the other two, over real TCP —
// see TCPTransport.BlockPeer), not a simulated process-kill. The
// majority side must keep electing a leader and committing; the isolated
// node must never commit anything while cut off, and must catch up
// cleanly once reconnected.
func TestRealNetworkPartitionOnlyMajoritySideCommits(t *testing.T) {
	c := makeTCPCluster(t, 3)
	defer c.cleanup()

	// isolate whichever node is CURRENTLY the leader — this is the
	// interesting case: an isolated leader doesn't know it's cut off, and
	// (correctly, per Raft) keeps reporting isLeader=true from GetState
	// forever, since nothing ever tells it otherwise. The real invariant
	// isn't "it stops believing it's leader" (it doesn't, and shouldn't
	// have to) — it's "it can never get anything COMMITTED while cut off,"
	// which is what actually matters for correctness. An earlier version
	// of this test asserted the former and flaked ~2/10 runs whenever the
	// isolated node happened to already be leader at partition time.
	isolated := c.findLeader(t, 3*time.Second)
	if isolated == -1 {
		t.Fatalf("no initial leader elected")
	}
	var majority []int
	for i := range c.rafts {
		if i != isolated {
			majority = append(majority, i)
		}
	}

	// partition isolated from the other two: block both directions for
	// real — isolated's transport refuses to call the majority nodes, and
	// their transports refuse to call it back.
	for _, m := range majority {
		c.transports[isolated].BlockPeer(m)
		c.transports[m].BlockPeer(isolated)
	}

	// the majority side must elect (or keep) a leader among themselves
	// and be able to commit
	var majorityLeader int = -1
	deadline := time.Now().Add(4 * time.Second)
	for majorityLeader == -1 && time.Now().Before(deadline) {
		for _, i := range majority {
			if _, isLeader := c.rafts[i].GetState(); isLeader {
				majorityLeader = i
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if majorityLeader == -1 {
		t.Fatalf("majority side %v never elected a leader while partitioned from node %d", majority, isolated)
	}

	idx, _, isLeader := c.rafts[majorityLeader].Start([]byte("majority-side-write"))
	if !isLeader {
		t.Fatalf("majority leader rejected Start")
	}
	committed := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case msg := <-c.applyChs[majorityLeader]:
			if msg.CommandIndex == idx {
				committed = true
			}
		case <-time.After(50 * time.Millisecond):
		}
		if committed {
			break
		}
	}
	if !committed {
		t.Fatalf("majority side never committed a write while node %d was partitioned", isolated)
	}

	// the isolated node must never have applied that write while cut off
	// — that's the actual correctness property (not whether it still
	// locally believes it's a leader, which it may, harmlessly, since it
	// can never persuade anyone else of that belief).
	select {
	case msg := <-c.applyChs[isolated]:
		if msg.CommandValid && string(msg.Command.([]byte)) == "majority-side-write" {
			t.Fatalf("isolated node %d applied a write it should have been cut off from", isolated)
		}
	default:
	}

	// heal the partition
	for _, m := range majority {
		c.transports[isolated].UnblockPeer(m)
		c.transports[m].UnblockPeer(isolated)
	}

	// the previously isolated node must rejoin as a follower and
	// eventually see the write that committed while it was cut off
	deadline = time.Now().Add(4 * time.Second)
	caughtUp := false
	for time.Now().Before(deadline) {
		select {
		case msg := <-c.applyChs[isolated]:
			if msg.CommandValid && string(msg.Command.([]byte)) == "majority-side-write" {
				caughtUp = true
			}
		case <-time.After(50 * time.Millisecond):
		}
		if caughtUp {
			break
		}
	}
	if !caughtUp {
		t.Fatalf("node %d never caught up to the majority-side write after the partition healed", isolated)
	}
}
