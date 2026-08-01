// Package producer implements the three acknowledgment levels described
// in docs/design_ack_levels.md, built on top of replication.ReplicatedPartition.
package producer

import (
	"errors"
	"time"

	"github.com/saitejasrivilli/ledgerdb/replication"
)

type AckLevel int

const (
	// AckNone (ack=0): fire-and-forget, no wait, no leader check.
	AckNone AckLevel = iota
	// AckLeader (ack=1): wait only for the leader to accept the entry
	// into its own Raft log, not for replication to followers.
	AckLeader
	// AckAll (ack=all): wait for Raft's own majority-commit rule to fire,
	// i.e. a quorum of replicas has the entry before returning.
	AckAll
)

var (
	ErrNotLeader     = errors.New("producer: target replica is not the leader")
	ErrCommitTimeout = errors.New("producer: entry did not commit within timeout")
)

const defaultCommitTimeout = 2 * time.Second

// Write submits payload to node at the given ack level. See design doc for
// exactly what each level does and does not guarantee under a leader crash
// immediately after this call returns.
func Write(node *replication.ReplicatedPartition, payload []byte, ack AckLevel) (raftIndex int, err error) {
	idx, _, isLeader := node.Propose(payload)

	switch ack {
	case AckNone:
		// fire-and-forget: return immediately, don't even surface a
		// not-leader rejection — that's the point of this level.
		return idx, nil

	case AckLeader:
		if !isLeader {
			return 0, ErrNotLeader
		}
		return idx, nil

	case AckAll:
		if !isLeader {
			return 0, ErrNotLeader
		}
		if !node.WaitApplied(idx, defaultCommitTimeout) {
			return 0, ErrCommitTimeout
		}
		return idx, nil

	default:
		return 0, errors.New("producer: unknown ack level")
	}
}
