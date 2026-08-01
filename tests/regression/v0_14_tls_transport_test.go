package regression

import (
	"fmt"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/security"
)

// makeTLSTCPCluster wires n Raft peers over TCPTransport secured with
// TLS (v0.10's real cert/handshake machinery, finally plugged into a
// transport that has real sockets to secure — the gap the README
// flagged as the natural next step once v0.14 existed).
func makeTLSTCPCluster(t *testing.T, n int) *tcpCluster {
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

	ca, err := security.GenerateCA()
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}

	c := &tcpCluster{
		transports: make([]*raft.TCPTransport, n),
		rafts:      make([]*raft.Raft, n),
		applyChs:   make([]chan raft.ApplyMsg, n),
	}
	for i := 0; i < n; i++ {
		serverCert, serverKey, err := ca.IssueServerCert("127.0.0.1")
		if err != nil {
			t.Fatalf("issue server cert for node %d: %v", i, err)
		}
		serverTLS, err := security.ServerTLSConfig(serverCert, serverKey)
		if err != nil {
			t.Fatalf("server tls config for node %d: %v", i, err)
		}
		clientTLS, err := security.ClientTLSConfig(ca.CertPEM)
		if err != nil {
			t.Fatalf("client tls config for node %d: %v", i, err)
		}
		clientTLS.ServerName = "127.0.0.1"

		transport, svc, err := raft.NewTCPTransportTLS(i, addrs, serverTLS, clientTLS)
		if err != nil {
			t.Fatalf("new tls tcp transport %d: %v", i, err)
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

// TestV0_14_TLSSecuredRaftTrafficWorks confirms Raft elects a leader and
// commits over a TLS-secured TCPTransport, not just plain TCP.
func TestV0_14_TLSSecuredRaftTrafficWorks(t *testing.T) {
	c := makeTLSTCPCluster(t, 3)
	defer c.cleanup()

	leader := c.findLeader(t, 3*time.Second)
	if leader == -1 {
		t.Fatalf("no leader elected over TLS-secured transport")
	}

	idx, _, isLeader := c.rafts[leader].Start([]byte("secured-write"))
	if !isLeader {
		t.Fatalf("leader rejected Start")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case msg := <-c.applyChs[leader]:
			if msg.CommandIndex == idx {
				if string(msg.Command.([]byte)) != "secured-write" {
					t.Fatalf("applied command = %q, want %q", msg.Command, "secured-write")
				}
				return
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("entry never applied over TLS-secured transport")
}

// TestV0_14_WrongCARaftPeerCannotJoin confirms a peer using the wrong CA
// (a different, unrelated CA than the rest of the cluster) can never
// successfully exchange RPCs with the real cluster — its dials fail the
// TLS handshake, which looks like an unreachable peer to Raft, exactly
// like a real misconfigured/malicious node would.
func TestV0_14_WrongCARaftPeerCannotJoin(t *testing.T) {
	ports := freePorts(t, 2)
	addrs := map[int]string{
		0: fmt.Sprintf("127.0.0.1:%d", ports[0]),
		1: fmt.Sprintf("127.0.0.1:%d", ports[1]),
	}
	peers := []int{0, 1}

	realCA, err := security.GenerateCA()
	if err != nil {
		t.Fatalf("generate real CA: %v", err)
	}
	wrongCA, err := security.GenerateCA()
	if err != nil {
		t.Fatalf("generate wrong CA: %v", err)
	}

	// node 0 uses the real CA throughout
	cert0, key0, _ := realCA.IssueServerCert("127.0.0.1")
	server0, _ := security.ServerTLSConfig(cert0, key0)
	client0, _ := security.ClientTLSConfig(realCA.CertPEM)
	client0.ServerName = "127.0.0.1"
	transport0, svc0, err := raft.NewTCPTransportTLS(0, addrs, server0, client0)
	if err != nil {
		t.Fatalf("node 0 transport: %v", err)
	}
	defer transport0.Close()
	applyCh0 := make(chan raft.ApplyMsg, 256)
	rf0 := raft.MakeWithTransport(transport0, 0, peers, applyCh0)
	svc0.Bind(rf0)
	defer rf0.Kill()

	// node 1 (the impostor) has a server cert signed by the WRONG CA, and
	// trusts only the wrong CA for its own outbound dials — it can never
	// complete a handshake with node 0 in either direction
	cert1, key1, _ := wrongCA.IssueServerCert("127.0.0.1")
	server1, _ := security.ServerTLSConfig(cert1, key1)
	client1, _ := security.ClientTLSConfig(wrongCA.CertPEM)
	client1.ServerName = "127.0.0.1"
	transport1, svc1, err := raft.NewTCPTransportTLS(1, addrs, server1, client1)
	if err != nil {
		t.Fatalf("node 1 transport: %v", err)
	}
	defer transport1.Close()
	applyCh1 := make(chan raft.ApplyMsg, 256)
	rf1 := raft.MakeWithTransport(transport1, 1, peers, applyCh1)
	svc1.Bind(rf1)
	defer rf1.Kill()

	// give both nodes time to try electing — neither can ever reach 2 of
	// 2 (both need the other's vote, and every cross-node handshake
	// fails), so neither should ever become leader
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, isLeader0 := rf0.GetState()
		_, isLeader1 := rf1.GetState()
		if isLeader0 || isLeader1 {
			t.Fatalf("a 2-node cluster with mismatched CAs must never elect a leader (needs both votes, handshake always fails)")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
