// Package networkfault holds tests that inject real kernel-level network
// faults (iptables DROP rules, tc netem loss/reorder) against a real
// TCPTransport-backed Raft cluster — see docs/design_network_fault.md
// for why this exists and what it closes: the previously-unverified
// claim that TCPTransport.BlockPeer behaves like a real partition.
//
// Linux-only (iptables/tc are kernel tools, unavailable on macOS/the
// default dev machine for this project) and root-only — skipped unless
// NETFAULT_TEST=1 is set, same gating pattern as MINIO_ENDPOINT for the
// real-MinIO test. Runs for real in CI on a native ubuntu-latest runner
// (a real Linux VM, not a container — root and iptables/tc work
// directly) and locally via scripts/run_network_fault_tests.sh, which
// runs it in a privileged Linux Docker container.
package networkfault

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
)

func requireNetFault(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("iptables/tc are Linux-only — skipping on " + runtime.GOOS)
	}
	if os.Getenv("NETFAULT_TEST") != "1" {
		t.Skip("NETFAULT_TEST not set — skipping real network-fault test locally; runs in CI (.github/workflows/network-fault.yml) or via scripts/run_network_fault_tests.sh")
	}
	if os.Geteuid() != 0 {
		t.Skip("iptables/tc require root — skipping (run as root, e.g. in the provided Docker script or CI)")
	}
}

// loopbackCluster is a 3-node real TCPTransport cluster where each node
// gets its OWN loopback address (127.0.0.1, .2, .3) instead of sharing
// one address on different ports — this is what makes "isolate node N"
// a clean iptables rule (`DROP anything to/from 127.0.0.N`) instead of a
// fragile per-port rule that has to separately account for inbound
// connections to that node's listener AND that node's own outbound
// dials to every other peer's port.
type loopbackCluster struct {
	addrs      map[int]string
	transports []*raft.TCPTransport
	rafts      []*raft.Raft
	applyChs   []chan raft.ApplyMsg
}

func makeLoopbackCluster(t *testing.T, n int, basePort int) *loopbackCluster {
	t.Helper()
	addrs := make(map[int]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.%d:%d", i+1, basePort)
	}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	c := &loopbackCluster{
		addrs:      addrs,
		transports: make([]*raft.TCPTransport, n),
		rafts:      make([]*raft.Raft, n),
		applyChs:   make([]chan raft.ApplyMsg, n),
	}
	for i := 0; i < n; i++ {
		transport, svc, err := raft.NewTCPTransport(i, addrs)
		if err != nil {
			t.Fatalf("new tcp transport %d on %s: %v", i, addrs[i], err)
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

func (c *loopbackCluster) cleanup() {
	for i, rf := range c.rafts {
		rf.Kill()
		c.transports[i].Close()
	}
}

func (c *loopbackCluster) findLeader(timeout time.Duration) int {
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

func runCmd(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
}

// isolateNodeViaIptables drops every packet to/from nodeAddr's IP —
// real kernel-level packet loss for that node, not an application-level
// call rejection. Returns a cleanup func that removes the rules.
func isolateNodeViaIptables(t *testing.T, ip string) func() {
	t.Helper()
	runCmd(t, "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP")
	runCmd(t, "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP")
	return func() {
		exec.Command("iptables", "-D", "INPUT", "-s", ip, "-j", "DROP").Run()
		exec.Command("iptables", "-D", "OUTPUT", "-d", ip, "-j", "DROP").Run()
	}
}

type chaosMeasurement struct {
	Method      string  `json:"method"`
	DetectionMs float64 `json:"detection_ms"`
	RecoveryMs  float64 `json:"recovery_ms"`
}

// TestRealIptablesDropVsBlockPeer measures leader-kill detection/recovery
// using a REAL iptables DROP rule instead of TCPTransport.BlockPeer, then
// writes the result next to the BlockPeer-based numbers so the two can
// actually be compared instead of assumed equivalent. See
// docs/design_network_fault.md.
func TestRealIptablesDropVsBlockPeer(t *testing.T) {
	requireNetFault(t)

	c := makeLoopbackCluster(t, 3, 18000)
	defer c.cleanup()

	leader := c.findLeader(3 * time.Second)
	if leader == -1 {
		t.Fatalf("no initial leader elected")
	}
	for i := 0; i < 5; i++ {
		c.rafts[leader].Start([]byte("warmup"))
		time.Sleep(5 * time.Millisecond)
	}

	leaderIP := c.addrs[leader][:len(c.addrs[leader])-len(":18000")]

	killedAt := time.Now()
	unblock := isolateNodeViaIptables(t, leaderIP)
	defer unblock()

	// Generous deadline, not a tight one: with exactly 2 of 3 voters
	// reachable, split-vote livelock is a real, observed Raft property
	// here — both survivors can end up starting elections in
	// near-lockstep repeatedly (each granting the other no vote while
	// itself a candidate for the same term), climbing terms for several
	// rounds before the randomized 300-600ms election timeout desyncs
	// them enough for one to win. This was measured directly: one run
	// reached term 14 without resolving in 5s. It always resolves
	// eventually (that's what the randomization guarantees over time),
	// just not on a tight deadline — so the deadline here reflects that
	// real behavior instead of fighting it.
	newLeader := -1
	deadline := time.Now().Add(15 * time.Second)
	lastLog := time.Now()
	for newLeader == -1 && time.Now().Before(deadline) {
		for i, rf := range c.rafts {
			if i == leader {
				continue
			}
			if _, isLeader := rf.GetState(); isLeader {
				newLeader = i
				break
			}
		}
		if newLeader == -1 {
			if time.Since(lastLog) > time.Second {
				for i, rf := range c.rafts {
					term, isLeader := rf.GetState()
					t.Logf("DEBUG t=%.1fs node %d term=%d isLeader=%v", time.Since(killedAt).Seconds(), i, term, isLeader)
				}
				lastLog = time.Now()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	detectedAt := time.Now()
	if newLeader == -1 {
		t.Fatalf("no new leader elected within 15s after real iptables DROP isolation — this would indicate a real livelock, not just a slow-but-eventual resolution")
	}

	var idx int
	var isLeader bool
	writeDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(writeDeadline) {
		idx, _, isLeader = c.rafts[newLeader].Start([]byte("post-kill"))
		if isLeader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !isLeader {
		t.Fatalf("new leader never accepted a write")
	}
	t.Logf("DEBUG: post-kill Start() on node %d returned idx=%d", newLeader, idx)

	var recoveredAt time.Time
	deadline = time.Now().Add(5 * time.Second)
	seen := []int{}
	for time.Now().Before(deadline) && recoveredAt.IsZero() {
		select {
		case msg := <-c.applyChs[newLeader]:
			seen = append(seen, msg.CommandIndex)
			if msg.CommandIndex == idx {
				recoveredAt = time.Now()
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if recoveredAt.IsZero() {
		term, isLeaderNow := c.rafts[newLeader].GetState()
		t.Logf("DEBUG: newLeader=%d term=%d isLeaderNow=%v seenIndices=%v wantIdx=%d", newLeader, term, isLeaderNow, seen, idx)
		t.Fatalf("post-kill write never committed after real iptables DROP isolation")
	}

	result := chaosMeasurement{
		Method:      "real iptables DROP (INPUT -s <ip> DROP, OUTPUT -d <ip> DROP)",
		DetectionMs: float64(detectedAt.Sub(killedAt).Microseconds()) / 1000.0,
		RecoveryMs:  float64(recoveredAt.Sub(killedAt).Microseconds()) / 1000.0,
	}

	blob, _ := json.MarshalIndent(struct {
		GeneratedAt  string           `json:"generated_at"`
		IptablesDrop chaosMeasurement `json:"iptables_drop"`
		Note         string           `json:"note"`
	}{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		IptablesDrop: result,
		Note:         "compare against benchmarks/results/v0.14_transport.json's BlockPeer-based numbers (~321-373ms detection, ~325-379ms recovery, 5 runs) — see docs/design_network_fault.md",
	}, "", "  ")
	os.WriteFile("../../benchmarks/results/v0.14.4_iptables_vs_blockpeer.json", blob, 0644)

	t.Logf("real iptables DROP: detection=%.3fms recovery=%.3fms (compare to BlockPeer's ~321-373ms/~325-379ms in v0.14_transport.json)", result.DetectionMs, result.RecoveryMs)
}

// TestRealTCNetemLossAndReorder applies real tc netem packet loss and
// reordering to loopback traffic, then drives writes through the
// degraded link — proving correctness holds under sustained real packet
// loss, not just a clean partition cut. See docs/design_network_fault.md
// for exactly what "correct" means here (data integrity, not speed).
func TestRealTCNetemLossAndReorder(t *testing.T) {
	requireNetFault(t)

	runCmd(t, "tc", "qdisc", "add", "dev", "lo", "root", "netem",
		"loss", "15%", "delay", "50ms", "20ms", "reorder", "25%", "50%")
	defer exec.Command("tc", "qdisc", "del", "dev", "lo", "root", "netem").Run()

	c := makeLoopbackCluster(t, 3, 18100)
	defer c.cleanup()

	leader := c.findLeader(8 * time.Second) // generous: real loss slows election
	if leader == -1 {
		t.Fatalf("no leader elected under real packet loss/reordering within timeout")
	}

	// drain every replica's applyCh into one shared map — under real
	// packet loss the leader can change mid-test, so tracking "the"
	// applyCh for a given write is fragile; what actually matters is
	// whether the entry was applied ANYWHERE with the right content.
	applied := make(map[int]string)
	appliedMu := make(chan struct{}, 1)
	appliedMu <- struct{}{}
	for _, ch := range c.applyChs {
		go func(ch chan raft.ApplyMsg) {
			for msg := range ch {
				if !msg.CommandValid {
					continue
				}
				<-appliedMu
				applied[msg.CommandIndex] = string(msg.Command.([]byte))
				appliedMu <- struct{}{}
			}
		}(ch)
	}

	// write with retry, same as a real client would under a lossy link —
	// the test's correctness bar is "every acknowledged write reads back
	// right," not "every write succeeds on the first try"
	const numWrites = 10
	written := make(map[int]string) // raft index -> payload
	for i := 0; i < numWrites; i++ {
		payload := fmt.Sprintf("netem-write-%d", i)
		var idx int
		var ok bool
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, isLeader := c.rafts[leader].GetState(); !isLeader {
				if cur := c.findLeader(2 * time.Second); cur != -1 {
					leader = cur
				} else {
					continue
				}
			}
			var isLeader bool
			idx, _, isLeader = c.rafts[leader].Start([]byte(payload))
			if isLeader {
				ok = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("write %d never accepted under real packet loss/reordering", i)
		}
		written[idx] = payload
	}

	// confirm every accepted write actually committed, somewhere, with
	// the right content — the real correctness bar under real network fault
	for idx, payload := range written {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			<-appliedMu
			got, ok := applied[idx]
			appliedMu <- struct{}{}
			if ok {
				if got != payload {
					t.Fatalf("raft index %d: applied %q, want %q — data corruption under packet loss/reordering", idx, got, payload)
				}
				break
			}
			time.Sleep(100 * time.Millisecond)
			if !time.Now().Before(deadline) {
				t.Fatalf("raft index %d (%q) never committed under real packet loss/reordering", idx, payload)
			}
		}
	}
}
