package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Network is a simplified stand-in for MIT 6.5840's labrpc: simulates an
// unreliable network between in-process Raft peers so tests can inject
// delay, drops, and partitions without real sockets.
type Network struct {
	mu        sync.Mutex
	reliable  bool
	longDelay bool
	servers   map[int]*Raft
	connected map[int]bool
}

func MakeNetwork() *Network {
	return &Network{
		reliable:  true,
		servers:   make(map[int]*Raft),
		connected: make(map[int]bool),
	}
}

func (n *Network) AddServer(id int, rf *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[id] = rf
	n.connected[id] = true
}

func (n *Network) SetReliable(r bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = r
}

func (n *Network) SetLongDelay(l bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelay = l
}

func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = true
}

func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = false
}

func (n *Network) IsConnected(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.connected[id]
}

// Call delivers an RPC from `from` to `to`, applying simulated network
// conditions. Returns false on simulated drop/timeout, mirroring the
// ok-bool contract real Go RPC clients return on failure.
func (n *Network) Call(from, to int, fn func(*Raft) bool) bool {
	n.mu.Lock()
	reliable := n.reliable
	longDelay := n.longDelay
	fromOK := n.connected[from]
	toOK := n.connected[to]
	peer := n.servers[to]
	n.mu.Unlock()

	if !fromOK || !toOK || peer == nil {
		return false
	}

	if !reliable {
		// short random delay
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(1000) < 100 {
			return false // simulated drop
		}
	}
	if longDelay && rand.Intn(1000) < 200 {
		time.Sleep(time.Duration(200+rand.Intn(2000)) * time.Millisecond)
	}

	ok := fn(peer)

	n.mu.Lock()
	stillUp := n.connected[from] && n.connected[to]
	n.mu.Unlock()
	if !stillUp {
		return false
	}
	return ok
}
