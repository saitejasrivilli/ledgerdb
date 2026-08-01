package benchmarks

import (
	"fmt"
	"net"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
)

func freeTCPPorts(n int) []int {
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		ports[i] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	return ports
}

func setupTCPCluster(n int) ([]*raft.TCPTransport, []*raft.Raft, []chan raft.ApplyMsg, func()) {
	ports := freeTCPPorts(n)
	addrs := make(map[int]string, n)
	for i, p := range ports {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", p)
	}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	transports := make([]*raft.TCPTransport, n)
	rafts := make([]*raft.Raft, n)
	applyChs := make([]chan raft.ApplyMsg, n)
	for i := 0; i < n; i++ {
		transport, svc, err := raft.NewTCPTransport(i, addrs)
		if err != nil {
			panic(err)
		}
		applyCh := make(chan raft.ApplyMsg, 256)
		rf := raft.MakeWithTransport(transport, i, peers, applyCh)
		svc.Bind(rf)
		transports[i] = transport
		rafts[i] = rf
		applyChs[i] = applyCh
	}

	cleanup := func() {
		for i, rf := range rafts {
			rf.Kill()
			transports[i].Close()
		}
	}
	return transports, rafts, applyChs, cleanup
}

func findRaftLeader(rafts []*raft.Raft, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, rf := range rafts {
			if _, isLeader := rf.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

// RunRealTransportChaos re-measures the same kill-leader recovery
// scenario as RunChaos, but over a real TCPTransport instead of the
// in-process simulated Network — see docs/design_real_transport.md for
// why the v0.7 numbers only describe the simulated path and this is a
// separate, real-sockets measurement, not a replacement for it.
//
// "Killing" the leader here means blocking it from every peer
// bidirectionally over real TCP (TCPTransport.BlockPeer) — from every
// survivor's point of view this looks exactly like a real socket
// failure (dropped/refused connections), which is what a killed process
// actually looks like to everyone else on a real network.
func RunRealTransportChaos() ChaosResult {
	transports, rafts, applyChs, cleanup := setupTCPCluster(3)
	defer cleanup()

	leader := findRaftLeader(rafts, 3*time.Second)
	if leader == -1 {
		panic("no leader elected during real-transport chaos benchmark setup")
	}
	for i := 0; i < 5; i++ {
		rafts[leader].Start([]byte("warmup"))
		time.Sleep(5 * time.Millisecond)
	}

	killedAt := time.Now()
	for i := range rafts {
		if i == leader {
			continue
		}
		transports[leader].BlockPeer(i)
		transports[i].BlockPeer(leader)
	}

	newLeader := -1
	deadline := time.Now().Add(5 * time.Second)
	for newLeader == -1 && time.Now().Before(deadline) {
		for i, rf := range rafts {
			if i == leader {
				continue
			}
			if _, isLeader := rf.GetState(); isLeader {
				newLeader = i
				break
			}
		}
		if newLeader == -1 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	detectedAt := time.Now()
	if newLeader == -1 {
		panic("no new leader elected within real-transport chaos benchmark deadline")
	}

	// match v0.7's recovery_ms definition: "first successful write
	// completed" means committed (applied), not just locally accepted —
	// so wait for the applyCh confirmation, same as RunChaos's
	// producer.Write(AckAll) did.
	var idx int
	var isLeader bool
	writeDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(writeDeadline) {
		idx, _, isLeader = rafts[newLeader].Start([]byte("post-kill"))
		if isLeader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !isLeader {
		panic("no successful write accepted within real-transport chaos benchmark deadline")
	}

	var recoveredAt time.Time
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && recoveredAt.IsZero() {
		select {
		case msg := <-applyChs[newLeader]:
			if msg.CommandIndex == idx {
				recoveredAt = time.Now()
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if recoveredAt.IsZero() {
		panic("post-kill write never committed within real-transport chaos benchmark deadline")
	}

	return ChaosResult{
		DetectionMs: float64(detectedAt.Sub(killedAt).Microseconds()) / 1000.0,
		RecoveryMs:  float64(recoveredAt.Sub(killedAt).Microseconds()) / 1000.0,
	}
}
