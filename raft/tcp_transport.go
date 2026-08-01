package raft

import (
	"crypto/tls"
	"encoding/gob"
	"net"
	"net/rpc"
	"sync"
	"time"
)

func init() {
	// LogEntry.Command is interface{} — every producer in this project
	// puts []byte in it (see replication.ReplicatedPartition), but gob
	// needs the concrete type registered to encode/decode an interface
	// value over a real connection.
	gob.Register([]byte{})
}

// rpcService is the net/rpc-registered service a TCPTransport listens
// for — thin wrappers delegating to *Raft's existing, unmodified
// RequestVote/AppendEntries methods. Bound to a *Raft after
// construction (see NewTCPTransport) since building the transport and
// building the Raft peer are mutually dependent: the transport needs a
// listener running before Make can return, but the RPC handlers need
// the *Raft that Make produces.
type rpcService struct {
	mu sync.RWMutex
	rf *Raft
}

func (s *rpcService) Bind(rf *Raft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rf = rf
}

func (s *rpcService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.mu.RLock()
	rf := s.rf
	s.mu.RUnlock()
	rf.RequestVote(args, reply)
	return nil
}

func (s *rpcService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.mu.RLock()
	rf := s.rf
	s.mu.RUnlock()
	rf.AppendEntries(args, reply)
	return nil
}

// TCPTransport is a real, socket-based Transport (v0.14) — see
// docs/design_real_transport.md for why this exists and what it proves
// that the in-process simTransport can't: real packet loss, real
// timeouts, and a real network partition (a client that literally cannot
// open a connection to a peer, not a simulated "disconnected" flag).
type TCPTransport struct {
	me        int
	addrs     map[int]string
	timeout   time.Duration
	listener  net.Listener
	clientTLS *tls.Config // nil means plain TCP dial

	mu      sync.Mutex
	clients map[int]*rpc.Client
	blocked map[int]bool
}

// NewTCPTransport starts listening on addrs[me] over plain TCP and
// returns the transport plus the rpcService it registered — callers
// must call Bind(rf) on that service once the *Raft peer using this
// transport exists (see raft.MakeWithTransport).
func NewTCPTransport(me int, addrs map[int]string) (*TCPTransport, *rpcService, error) {
	return newTCPTransport(me, addrs, nil, nil)
}

// NewTCPTransportTLS is the same as NewTCPTransport but listens and
// dials over TLS — serverTLSConfig secures this node's listener,
// clientTLSConfig is used for every outbound dial to a peer. Callers
// build both with the real cert/handshake machinery from package
// security (v0.10) — this package can't import security directly (that
// would be an import cycle: security -> replication -> raft), so it
// takes plain *tls.Config instead, which is exactly what security's
// ServerTLSConfig/ClientTLSConfig already return.
func NewTCPTransportTLS(me int, addrs map[int]string, serverTLSConfig, clientTLSConfig *tls.Config) (*TCPTransport, *rpcService, error) {
	return newTCPTransport(me, addrs, serverTLSConfig, clientTLSConfig)
}

func newTCPTransport(me int, addrs map[int]string, serverTLSConfig, clientTLSConfig *tls.Config) (*TCPTransport, *rpcService, error) {
	svc := &rpcService{}
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", svc); err != nil {
		return nil, nil, err
	}

	var listener net.Listener
	var err error
	if serverTLSConfig != nil {
		listener, err = tls.Listen("tcp", addrs[me], serverTLSConfig)
	} else {
		listener, err = net.Listen("tcp", addrs[me])
	}
	if err != nil {
		return nil, nil, err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	t := &TCPTransport{
		me:        me,
		addrs:     addrs,
		timeout:   500 * time.Millisecond,
		listener:  listener,
		clientTLS: clientTLSConfig,
		clients:   make(map[int]*rpc.Client),
		blocked:   make(map[int]bool),
	}
	return t, svc, nil
}

// BlockPeer makes every outbound call from this transport to peer fail
// immediately, as if unreachable — dropping any cached connection first.
// A real network partition is simulated by calling this on BOTH sides of
// the cut (this transport blocking peer, and peer's own transport
// blocking this node back) — this sandbox has no OS-level network
// namespace/iptables control to sever a real socket path outright.
//
// This is NOT verified to behave like a real partition, and that claim
// should not be assumed: this fails a call immediately, closer to a TCP
// RST/REJECT, whereas a real `iptables DROP` typically vanishes packets
// silently and leaves the caller waiting out a full TCP timeout before
// it learns anything failed — a materially different timing profile,
// and timing is exactly what the chaos-recovery numbers measure. Real
// TCP connections and real dial/RPC errors are exercised for every
// unblocked pair, which is a genuine improvement over v0.1-v1.0's fully
// simulated Network — but the silent-packet-loss case specifically has
// not been tested against real iptables/tc netem rules.
func (t *TCPTransport) BlockPeer(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[peer] = true
	if c, ok := t.clients[peer]; ok {
		c.Close()
		delete(t.clients, peer)
	}
}

func (t *TCPTransport) UnblockPeer(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, peer)
}

// Addr returns the actual listening address (useful when addrs[me] used
// port 0 to get an OS-assigned free port, as tests do).
func (t *TCPTransport) Addr() string {
	return t.listener.Addr().String()
}

func (t *TCPTransport) getClient(peer int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[peer]; ok {
		return c, nil
	}

	var conn net.Conn
	var err error
	if t.clientTLS != nil {
		dialer := &net.Dialer{Timeout: t.timeout}
		conn, err = tls.DialWithDialer(dialer, "tcp", t.addrs[peer], t.clientTLS)
	} else {
		conn, err = net.DialTimeout("tcp", t.addrs[peer], t.timeout)
	}
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	t.clients[peer] = client
	return client, nil
}

// dropClient discards a cached client after a failure, so the next call
// attempts a fresh dial — this is what makes a real partition and a real
// process-kill look the same to the caller: RPCs just stop succeeding.
func (t *TCPTransport) dropClient(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[peer]; ok {
		c.Close()
		delete(t.clients, peer)
	}
}

func (t *TCPTransport) call(peer int, method string, args, reply interface{}) bool {
	t.mu.Lock()
	blocked := t.blocked[peer]
	t.mu.Unlock()
	if blocked {
		return false
	}

	client, err := t.getClient(peer)
	if err != nil {
		return false
	}

	done := make(chan *rpc.Call, 1)
	call := client.Go(method, args, reply, done)
	select {
	case res := <-call.Done:
		if res.Error != nil {
			t.dropClient(peer)
			return false
		}
		return true
	case <-time.After(t.timeout):
		t.dropClient(peer)
		return false
	}
}

func (t *TCPTransport) SendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	ok := t.call(peer, "Raft.RequestVote", args, reply)
	return reply, ok
}

func (t *TCPTransport) SendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	ok := t.call(peer, "Raft.AppendEntries", args, reply)
	return reply, ok
}

func (t *TCPTransport) Close() error {
	t.mu.Lock()
	for _, c := range t.clients {
		c.Close()
	}
	t.mu.Unlock()
	return t.listener.Close()
}
