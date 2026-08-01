package security

import (
	"errors"
	"sync"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/replication"
)

type Permission int

const (
	Read Permission = iota
	Write
)

var ErrUnauthorized = errors.New("security: identity not authorized for this partition/permission")

// ACL maps client identities to allowed (partition, permission) pairs.
// Deny-by-default: an identity with no explicit grant for a partition is
// rejected, not allowed — see design doc for why that default matters.
type ACL struct {
	mu     sync.RWMutex
	grants map[string]map[int]map[Permission]bool
}

func NewACL() *ACL {
	return &ACL{grants: make(map[string]map[int]map[Permission]bool)}
}

// Grant authorizes identity for perm on partition.
func (a *ACL) Grant(identity string, partition int, perm Permission) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.grants[identity] == nil {
		a.grants[identity] = make(map[int]map[Permission]bool)
	}
	if a.grants[identity][partition] == nil {
		a.grants[identity][partition] = make(map[Permission]bool)
	}
	a.grants[identity][partition][perm] = true
}

// Allowed reports whether identity may perform perm on partition.
func (a *ACL) Allowed(identity string, partition int, perm Permission) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	perms, ok := a.grants[identity][partition]
	if !ok {
		return false
	}
	return perms[perm]
}

// CheckedWrite enforces the ACL before proposing to Raft — the same
// enforcement point a real request-handling layer (gRPC interceptor,
// REST middleware) would use once one exists.
func CheckedWrite(acl *ACL, identity string, partition int, node *replication.ReplicatedPartition, payload []byte, ack producer.AckLevel) (int, error) {
	if !acl.Allowed(identity, partition, Write) {
		return 0, ErrUnauthorized
	}
	return producer.Write(node, payload, ack)
}

// CheckedRead enforces the ACL before reading from local storage.
func CheckedRead(acl *ACL, identity string, partition int, node *replication.ReplicatedPartition, offset int) ([]byte, error) {
	if !acl.Allowed(identity, partition, Read) {
		return nil, ErrUnauthorized
	}
	return node.ReadLocal(offset)
}
