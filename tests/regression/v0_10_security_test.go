package regression

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/security"
)

func TestV10_TLSHandshakeRequiresCorrectCA(t *testing.T) {
	ca, err := security.GenerateCA()
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	serverCert, serverKey, err := ca.IssueServerCert("localhost")
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	serverTLSConfig, err := security.ServerTLSConfig(serverCert, serverKey)
	if err != nil {
		t.Fatalf("server tls config: %v", err)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLSConfig)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// tls.Listen defers the handshake to first Read/Write —
			// force it now so a wrong-CA client's Dial actually sees the
			// handshake failure instead of racing an early Close.
			go func(c net.Conn) {
				defer c.Close()
				if tlsConn, ok := c.(*tls.Conn); ok {
					tlsConn.Handshake()
				}
			}(conn)
		}
	}()

	// wrong CA (a second, unrelated CA) must fail the handshake
	wrongCA, err := security.GenerateCA()
	if err != nil {
		t.Fatalf("generate wrong CA: %v", err)
	}
	wrongClientConfig, err := security.ClientTLSConfig(wrongCA.CertPEM)
	if err != nil {
		t.Fatalf("wrong client tls config: %v", err)
	}
	wrongClientConfig.ServerName = "localhost"

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	_, err = tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), wrongClientConfig)
	if err == nil {
		t.Fatalf("expected TLS handshake to fail with the wrong CA")
	}

	// correct CA must succeed
	correctClientConfig, err := security.ClientTLSConfig(ca.CertPEM)
	if err != nil {
		t.Fatalf("correct client tls config: %v", err)
	}
	correctClientConfig.ServerName = "localhost"

	conn, err := tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), correctClientConfig)
	if err != nil {
		t.Fatalf("expected TLS handshake to succeed with the correct CA: %v", err)
	}
	conn.Close()
}

func TestV10_UnauthorizedClientRejected(t *testing.T) {
	net_, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findLeader(t, net_, nodes)

	acl := security.NewACL()
	// deliberately grant nothing to "stranger"

	_, err := security.CheckedWrite(acl, "stranger", 0, nodes[leader], []byte("nope"), producer.AckAll)
	if err != security.ErrUnauthorized {
		t.Fatalf("expected ErrUnauthorized for ungranted identity, got %v", err)
	}

	_, err = security.CheckedRead(acl, "stranger", 0, nodes[leader], 0)
	if err != security.ErrUnauthorized {
		t.Fatalf("expected ErrUnauthorized on read for ungranted identity, got %v", err)
	}
}

func TestV10_AuthorizedClientStillWorks(t *testing.T) {
	net_, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findLeader(t, net_, nodes)

	acl := security.NewACL()
	acl.Grant("trusted-service", 0, security.Write)
	acl.Grant("trusted-service", 0, security.Read)

	idx, err := security.CheckedWrite(acl, "trusted-service", 0, nodes[leader], []byte("allowed"), producer.AckAll)
	if err != nil {
		t.Fatalf("expected authorized write to succeed: %v", err)
	}
	if idx <= 0 {
		t.Fatalf("expected a positive raft index, got %d", idx)
	}

	got, err := security.CheckedRead(acl, "trusted-service", 0, nodes[leader], 0)
	if err != nil {
		t.Fatalf("expected authorized read to succeed: %v", err)
	}
	if string(got) != "allowed" {
		t.Fatalf("got %q want %q", got, "allowed")
	}
}
