package schema

import (
	"fmt"
	"sync"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/replication"
	"github.com/saitejasrivilli/ledgerdb/security"
)

// Registry holds one current schema per partition. Registering a new
// schema for a partition that already has one is rejected if the new
// schema isn't backward compatible with the current one — checked at
// registration time, not deferred to individual writes.
type Registry struct {
	mu      sync.RWMutex
	current map[int]*Schema
}

func NewRegistry() *Registry {
	return &Registry{current: make(map[int]*Schema)}
}

// Register sets s as partition's current schema, checking backward
// compatibility against whatever schema (if any) is already registered.
func (r *Registry) Register(partition int, s *Schema) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.current[partition]; ok {
		if compatible, reason := IsCompatible(existing, s); !compatible {
			return fmt.Errorf("schema: incompatible schema change for partition %d: %s", partition, reason)
		}
	}
	r.current[partition] = s
	return nil
}

func (r *Registry) CurrentSchema(partition int) (*Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.current[partition]
	return s, ok
}

// CheckedWrite validates docJSON against the partition's registered
// schema (if any), then delegates to security.CheckedWrite — schema
// rejection happens before the ACL-gated Raft proposal, so an invalid
// write never spends a consensus round trip.
func CheckedWrite(reg *Registry, acl *security.ACL, identity string, partition int, node *replication.ReplicatedPartition, docJSON []byte, ack producer.AckLevel) (int, error) {
	if s, ok := reg.CurrentSchema(partition); ok {
		if err := Validate(s, docJSON); err != nil {
			return 0, err
		}
	}
	return security.CheckedWrite(acl, identity, partition, node, docJSON, ack)
}
