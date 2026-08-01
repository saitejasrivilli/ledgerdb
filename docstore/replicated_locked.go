package docstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/saitejasrivilli/ledgerdb/raft"
	"github.com/saitejasrivilli/ledgerdb/storage"
)

// ReplicatedLockedDocStore is structurally identical to
// ReplicatedDocStore but backed by LockedStore instead of MVCCStore —
// the "before" baseline the design doc requires building first, wired up
// the same way so it can be benchmarked head-to-head against the MVCC
// version under identical concurrent load.
type ReplicatedLockedDocStore struct {
	rf      *raft.Raft
	wal     *storage.Log
	store   *LockedStore
	applyCh chan raft.ApplyMsg

	mu                   sync.Mutex
	lastAppliedRaftIndex int
}

func NewReplicatedLockedDocStore(net *raft.Network, me int, peers []int, dir string) (*ReplicatedLockedDocStore, error) {
	wal, err := storage.Open(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	rf := raft.Make(net, me, peers, applyCh)

	ds := &ReplicatedLockedDocStore{
		rf:      rf,
		wal:     wal,
		store:   NewLockedStore(),
		applyCh: applyCh,
	}
	go ds.applyLoop()
	return ds, nil
}

func (ds *ReplicatedLockedDocStore) applyLoop() {
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

		if muts, err := DecodeTransaction(payload); err == nil {
			ds.store.ApplyTransaction(muts)
		}

		ds.mu.Lock()
		ds.lastAppliedRaftIndex = msg.CommandIndex
		ds.mu.Unlock()
	}
}

func (ds *ReplicatedLockedDocStore) Propose(muts []Mutation) (raftIndex int, isLeader bool, err error) {
	blob, err := EncodeTransaction(muts)
	if err != nil {
		return 0, false, err
	}
	idx, _, isLeader := ds.rf.Start(blob)
	return idx, isLeader, nil
}

func (ds *ReplicatedLockedDocStore) GetState() (term int, isLeader bool) {
	return ds.rf.GetState()
}

func (ds *ReplicatedLockedDocStore) WaitApplied(raftIndex int, timeout time.Duration) bool {
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

func (ds *ReplicatedLockedDocStore) Get(id string) (Document, bool) {
	return ds.store.Get(id)
}

func (ds *ReplicatedLockedDocStore) Close() error {
	ds.rf.Kill()
	return ds.wal.Close()
}
