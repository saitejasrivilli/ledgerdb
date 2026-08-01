package docstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/storage"
)

// ReplicatedDocStore is the MVCC document store wired to a Raft group
// (v0.1) with the replicated log (v0.2) as its WAL — a transaction is
// proposed as one Raft entry, and once committed, the apply loop both
// durably logs it (WAL) and applies it to the in-memory MVCCStore.
type ReplicatedDocStore struct {
	rf      *raft.Raft
	wal     *storage.Log
	store   *MVCCStore
	applyCh chan raft.ApplyMsg

	mu                   sync.Mutex
	lastAppliedRaftIndex int
}

func NewReplicatedDocStore(net *raft.Network, me int, peers []int, dir string, indexField string) (*ReplicatedDocStore, error) {
	wal, err := storage.Open(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	rf := raft.Make(net, me, peers, applyCh)

	ds := &ReplicatedDocStore{
		rf:      rf,
		wal:     wal,
		store:   NewMVCCStore(indexField),
		applyCh: applyCh,
	}
	go ds.applyLoop()
	return ds, nil
}

func (ds *ReplicatedDocStore) applyLoop() {
	for msg := range ds.applyCh {
		if !msg.CommandValid {
			continue
		}
		payload, ok := msg.Command.([]byte)
		if !ok {
			continue
		}

		ds.mu.Lock()
		ds.wal.Append(payload)
		ds.mu.Unlock()

		muts, err := DecodeTransaction(payload)
		if err == nil {
			// a transaction rejected by ApplyTransaction (failed
			// precondition) is still durably logged in the WAL above —
			// consensus committed the *request*, the state machine
			// separately decided not to apply it. That's the "logged as
			// a rejected transaction rather than partially applied"
			// behavior from the design doc.
			ds.store.ApplyTransaction(muts, msg.CommandIndex)
		}

		ds.mu.Lock()
		ds.lastAppliedRaftIndex = msg.CommandIndex
		ds.mu.Unlock()
	}
}

// Propose submits a transaction (batch of mutations) to Raft. Only
// succeeds if this replica currently believes it's the leader.
func (ds *ReplicatedDocStore) Propose(muts []Mutation) (raftIndex int, isLeader bool, err error) {
	blob, err := EncodeTransaction(muts)
	if err != nil {
		return 0, false, err
	}
	idx, _, isLeader := ds.rf.Start(blob)
	return idx, isLeader, nil
}

func (ds *ReplicatedDocStore) GetState() (term int, isLeader bool) {
	return ds.rf.GetState()
}

func (ds *ReplicatedDocStore) WaitApplied(raftIndex int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ds.mu.Lock()
		applied := ds.lastAppliedRaftIndex
		ds.mu.Unlock()
		if applied >= raftIndex {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// Snapshot returns a pinned, consistent read view — see MVCCStore.Snapshot.
func (ds *ReplicatedDocStore) Snapshot() *Snapshot {
	return ds.store.Snapshot()
}

func (ds *ReplicatedDocStore) GCBefore(raftIndex int) {
	ds.store.GCBefore(raftIndex)
}

func (ds *ReplicatedDocStore) Close() error {
	ds.rf.Kill()
	return ds.wal.Close()
}
