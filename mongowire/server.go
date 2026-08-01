package mongowire

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/saitejasrivilli/ledgerdb/docstore"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Store is the subset of docstore.ReplicatedDocStore this server needs —
// an interface so tests can also run it against
// docstore.ReplicatedLockedDocStore or a fake, without pulling in Raft.
type Store interface {
	Propose(muts []docstore.Mutation) (raftIndex int, isLeader bool, err error)
	WaitApplied(raftIndex int, timeout time.Duration) bool
	Snapshot() *docstore.Snapshot
}

// Server is a TCP listener speaking the wire-protocol subset described
// in docs/design_wire_protocol.md, translating commands into calls
// against a single docstore.ReplicatedDocStore (one collection per
// server — no database/collection namespacing, stated as a scope
// boundary in the design doc, not an oversight).
type Server struct {
	store    Store
	listener net.Listener
}

// Listen starts a Server on addr backed by store.
func Listen(addr string, store Store) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{store: store, listener: l}
	go s.acceptLoop()
	return s, nil
}

func (s *Server) Addr() string { return s.listener.Addr().String() }

func (s *Server) Close() error { return s.listener.Close() }

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		reqID, cmd, opCode, err := readMessage(conn)
		if err != nil {
			return
		}
		reply := s.dispatch(cmd)
		if err := writeReply(conn, opCode, reqID, reply); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(cmd bson.M) bson.M {
	switch {
	case has(cmd, "hello"), has(cmd, "ismaster"), has(cmd, "isMaster"):
		return bson.M{
			"ismaster":                     true,
			"isWritablePrimary":            true,
			"maxWireVersion":               int32(17),
			"minWireVersion":               int32(0),
			"maxBsonObjectSize":            int32(16 * 1024 * 1024),
			"maxMessageSizeBytes":          int32(48 * 1024 * 1024),
			"maxWriteBatchSize":            int32(1000),
			"localTime":                    time.Now(),
			"logicalSessionTimeoutMinutes": int32(30),
			"ok":                           1.0,
		}
	case has(cmd, "ping"):
		return bson.M{"ok": 1.0}
	case has(cmd, "insert"):
		return s.handleInsert(cmd)
	case has(cmd, "find"):
		return s.handleFind(cmd)
	case has(cmd, "update"):
		return s.handleUpdate(cmd)
	case has(cmd, "delete"):
		return s.handleDelete(cmd)
	default:
		return bson.M{"ok": 0.0, "errmsg": fmt.Sprintf("mongowire: unsupported command %v", cmd)}
	}
}

func has(cmd bson.M, key string) bool {
	_, ok := cmd[key]
	return ok
}

func (s *Server) propose(muts []docstore.Mutation) error {
	idx, isLeader, err := s.store.Propose(muts)
	if err != nil {
		return err
	}
	if !isLeader {
		return fmt.Errorf("mongowire: not leader")
	}
	if !s.store.WaitApplied(idx, 2*time.Second) {
		return fmt.Errorf("mongowire: write did not commit")
	}
	return nil
}

// docToJSON converts a decoded BSON document (bson.M, produced by
// readMessage) into the JSON bytes docstore.Mutation.Data expects —
// docstore's storage format is JSON (see docs/design_document_store.md),
// so this is the one conversion point between the two representations.
func docToJSON(doc map[string]interface{}) ([]byte, error) {
	// bson.M decodes BSON types (dates, binary, etc.) into Go values
	// json.Marshal already knows how to render sensibly for this
	// version's scope (strings, numbers, bools, nested docs/arrays) —
	// exotic BSON types (Decimal128, custom binary subtypes) are outside
	// this version's stated scope.
	return json.Marshal(doc)
}
