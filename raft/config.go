package raft

import (
	"sync"
	"testing"
	"time"
)

// config wires up a cluster of in-process Raft peers over a simulated
// Network, modeled on the MIT 6.5840 test harness pattern: tests drive
// the cluster through this handle rather than touching Raft internals.
type config struct {
	t       *testing.T
	net     *Network
	n       int
	rafts   []*Raft
	applyCh []chan ApplyMsg

	mu   sync.Mutex
	logs []map[int]interface{} // per-server committed log, index -> command
}

func makeConfig(t *testing.T, n int) *config {
	cfg := &config{
		t:       t,
		net:     MakeNetwork(),
		n:       n,
		rafts:   make([]*Raft, n),
		applyCh: make([]chan ApplyMsg, n),
		logs:    make([]map[int]interface{}, n),
	}
	peers := make([]int, n)
	for i := 0; i < n; i++ {
		peers[i] = i
	}
	for i := 0; i < n; i++ {
		cfg.logs[i] = make(map[int]interface{})
		cfg.applyCh[i] = make(chan ApplyMsg, 100)
		cfg.rafts[i] = Make(cfg.net, i, peers, cfg.applyCh[i])
		go cfg.applierLoop(i)
	}
	return cfg
}

func (cfg *config) applierLoop(server int) {
	for msg := range cfg.applyCh[server] {
		if !msg.CommandValid {
			continue
		}
		cfg.mu.Lock()
		cfg.logs[server][msg.CommandIndex] = msg.Command
		cfg.mu.Unlock()
	}
}

func (cfg *config) cleanup() {
	for i := 0; i < cfg.n; i++ {
		cfg.rafts[i].Kill()
	}
}

func (cfg *config) disconnect(i int) {
	cfg.net.Disconnect(i)
}

func (cfg *config) connect(i int) {
	cfg.net.Connect(i)
}

func (cfg *config) setReliable(r bool) {
	cfg.net.SetReliable(r)
}

// checkOneLeader waits (up to ~2s across retries) for the cluster to agree
// on exactly one leader in the same term, then returns that leader's index.
// Fails the test if no consistent single leader emerges.
func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 15; iters++ {
		time.Sleep(150 * time.Millisecond)
		leaders := make(map[int][]int) // term -> leader indices
		for i := 0; i < cfg.n; i++ {
			if !cfg.net.IsConnected(i) {
				continue
			}
			term, isLeader := cfg.rafts[i].GetState()
			if isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		lastTermWithLeader := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				cfg.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}
		if lastTermWithLeader != -1 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("no leader elected within timeout")
	return -1
}

// checkTerms confirms all connected servers agree on the same term.
func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if !cfg.net.IsConnected(i) {
			continue
		}
		xterm, _ := cfg.rafts[i].GetState()
		if term == -1 {
			term = xterm
		} else if term != xterm {
			cfg.t.Fatalf("servers disagree on term")
		}
	}
	return term
}

// one submits cmd to the current leader and waits for it to be committed
// on at least the given number of servers. Fails the test if it never
// commits within the timeout.
func (cfg *config) one(cmd int, expectedServers int) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var index = -1
		for i := 0; i < cfg.n; i++ {
			if !cfg.net.IsConnected(i) {
				continue
			}
			idx, _, isLeader := cfg.rafts[i].Start(cmd)
			if isLeader {
				index = idx
				break
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		commitDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(commitDeadline) {
			if cfg.nCommitted(index) >= expectedServers {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("command %d never committed on %d servers", cmd, expectedServers)
	return -1
}

func (cfg *config) nCommitted(index int) int {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	count := 0
	var cmd interface{}
	for i := 0; i < cfg.n; i++ {
		if c, ok := cfg.logs[i][index]; ok {
			if count > 0 && c != cmd {
				cfg.t.Fatalf("committed values differ at index %d: %v vs %v", index, cmd, c)
			}
			cmd = c
			count++
		}
	}
	return count
}
