// Package replication combines a Raft group (package raft) with a
// segment-based log (package storage) per docs/design_replication.md:
// Raft owns commit ordering, storage.Log is the durable copy every
// replica applies committed entries into.
package replication

import (
	"fmt"
	"sync"
	"time"

	"github.com/saitejasrivilli/ledgerdb/batch"
	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/storage"
)

// ReplicatedPartition is one replica of one partition: a Raft peer plus
// the local durable log its apply loop feeds.
type ReplicatedPartition struct {
	rf      *raft.Raft
	log     *storage.Log
	applyCh chan raft.ApplyMsg

	mu                   sync.Mutex
	lastAppliedRaftIndex int
}

// NewReplicatedPartition wires up a Raft peer over net and a storage.Log
// rooted at dir, then starts the apply loop bridging the two.
func NewReplicatedPartition(net *raft.Network, me int, peers []int, dir string) (*ReplicatedPartition, error) {
	storageLog, err := storage.Open(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("open storage log: %w", err)
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	rf := raft.Make(net, me, peers, applyCh)

	rp := &ReplicatedPartition{
		rf:      rf,
		log:     storageLog,
		applyCh: applyCh,
	}
	go rp.applyLoop()
	return rp, nil
}

// applyLoop drains committed Raft entries in order and appends each
// payload to the local storage log — the bridge described in the design
// doc. Because Raft commits in the same order on every replica, the
// resulting storage offset for a given logical write is identical across
// replicas.
//
// Since v0.8: a committed entry may be a batch.Encode-d, gzip-compressed
// blob of several messages (see docs/design_batching_compression.md).
// TryDecode transparently unpacks it back into individual messages before
// they're appended, so consumers still see one message per storage
// offset regardless of whether the producer batched them. A payload
// that isn't batch-encoded (every write from v0.4-v0.7, unmodified) is
// appended as-is — TryDecode's ok=false path preserves that behavior
// exactly.
func (rp *ReplicatedPartition) applyLoop() {
	for msg := range rp.applyCh {
		if !msg.CommandValid {
			continue
		}
		payload, ok := msg.Command.([]byte)
		if !ok {
			continue
		}

		messages := [][]byte{payload}
		if decoded, isBatch, err := batch.TryDecode(payload); err == nil && isBatch {
			messages = decoded
		}

		rp.mu.Lock()
		for _, m := range messages {
			if _, err := rp.log.Append(m); err != nil {
				break
			}
		}
		rp.lastAppliedRaftIndex = msg.CommandIndex
		rp.mu.Unlock()
	}
}

// Propose submits payload to Raft. Only succeeds (isLeader=true) if this
// replica currently believes it's the leader — callers must retry against
// another replica otherwise, this version does no forwarding.
func (rp *ReplicatedPartition) Propose(payload []byte) (raftIndex int, term int, isLeader bool) {
	return rp.rf.Start(payload)
}

func (rp *ReplicatedPartition) GetState() (term int, isLeader bool) {
	return rp.rf.GetState()
}

// WaitApplied blocks until the given raft log index has been applied to
// this replica's local storage log, or the timeout elapses.
func (rp *ReplicatedPartition) WaitApplied(raftIndex int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rp.mu.Lock()
		applied := rp.lastAppliedRaftIndex
		rp.mu.Unlock()
		if applied >= raftIndex {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ReadLocal reads directly from this replica's local storage log — no
// consensus round-trip, callers are responsible for only reading offsets
// they know are committed (e.g. via WaitApplied). storage.Log isn't
// internally safe for concurrent access, so this method serializes against
// the apply loop's own Append calls via rp.mu.
func (rp *ReplicatedPartition) ReadLocal(offset int) ([]byte, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.log.Read(offset)
}

func (rp *ReplicatedPartition) NextLocalOffset() int {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.log.NextOffset()
}

func (rp *ReplicatedPartition) Close() error {
	rp.rf.Kill()
	return rp.log.Close()
}
