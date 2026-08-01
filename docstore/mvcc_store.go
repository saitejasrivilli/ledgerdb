package docstore

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// versionedDocument is one entry in a document's version chain.
type versionedDocument struct {
	version     int
	data        Document
	committedAt int // raft log index this version was committed at
	deleted     bool
}

// MVCCStore keeps a version chain per document ID instead of a single
// current value, so a reader can pin its view to one Raft-commit-ordered
// point in time (a Snapshot) and see a consistent view throughout its
// read regardless of writes that commit afterward. See docs/design_mvcc.md.
type MVCCStore struct {
	mu          sync.RWMutex
	chains      map[string][]versionedDocument
	index       map[string][]string // indexField value -> doc IDs that ever had it (superset, verified on read)
	indexField  string
	commitIndex int
}

func NewMVCCStore(indexField string) *MVCCStore {
	return &MVCCStore{
		chains:     make(map[string][]versionedDocument),
		index:      make(map[string][]string),
		indexField: indexField,
	}
}

// latestVersionLocked returns the current (highest) version number for
// id, or 0 if the document has never been written. Caller must hold mu.
func (s *MVCCStore) latestVersionLocked(id string) int {
	chain := s.chains[id]
	if len(chain) == 0 {
		return 0
	}
	return chain[len(chain)-1].version
}

// ApplyTransaction validates every mutation's ExpectedVersion precondition
// against the CURRENT state (two-phase: validate all, then commit all)
// before committing any of them — the all-or-nothing contract described
// in the design doc. raftIndex is the Raft commit index this transaction
// was applied at, becoming every committed version's committedAt stamp.
func (s *MVCCStore) ApplyTransaction(muts []Mutation, raftIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range muts {
		if m.ExpectedVersion != nil && s.latestVersionLocked(m.ID) != *m.ExpectedVersion {
			return fmt.Errorf("docstore: expected version %d for %q, got %d", *m.ExpectedVersion, m.ID, s.latestVersionLocked(m.ID))
		}
	}

	for _, m := range muts {
		switch m.Op {
		case "put":
			var doc Document
			if err := json.Unmarshal(m.Data, &doc); err != nil {
				return fmt.Errorf("docstore: invalid document data for %q: %w", m.ID, err)
			}
			newVersion := s.latestVersionLocked(m.ID) + 1
			s.chains[m.ID] = append(s.chains[m.ID], versionedDocument{
				version:     newVersion,
				data:        doc,
				committedAt: raftIndex,
			})
			if s.indexField != "" {
				if val, ok := doc[s.indexField]; ok {
					key := fmt.Sprintf("%v", val)
					if s.index[key] == nil {
						s.index[key] = []string{}
					}
					s.index[key] = append(s.index[key], m.ID)
				}
			}
		case "delete":
			newVersion := s.latestVersionLocked(m.ID) + 1
			s.chains[m.ID] = append(s.chains[m.ID], versionedDocument{
				version:     newVersion,
				committedAt: raftIndex,
				deleted:     true,
			})
		}
	}
	s.commitIndex = raftIndex
	return nil
}

// GCBefore removes every version of every document older than the newest
// version at-or-before raftIndex — the versions a reader snapshotted at
// or after raftIndex could still possibly need, nothing older.
func (s *MVCCStore) GCBefore(raftIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, chain := range s.chains {
		keepFrom := 0
		for i, v := range chain {
			if v.committedAt <= raftIndex {
				keepFrom = i
			} else {
				break
			}
		}
		if keepFrom > 0 {
			s.chains[id] = chain[keepFrom:]
		}
	}
}

// Snapshot pins a read to the store's current commit index at the moment
// Snapshot is called — every Get through it sees exactly that point in
// time, even if writes commit to the store afterward.
type Snapshot struct {
	store *MVCCStore
	at    int
}

func (s *MVCCStore) Snapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &Snapshot{store: s, at: s.commitIndex}
}

// Get returns the document as of the snapshot's pinned commit index —
// the latest version with committedAt <= the snapshot point, or false if
// the document didn't exist or was deleted as of that point.
func (sn *Snapshot) Get(id string) (Document, bool) {
	sn.store.mu.RLock()
	defer sn.store.mu.RUnlock()

	chain := sn.store.chains[id]
	// chain is append-only in increasing committedAt order (entries are
	// applied in Raft commit order), so the last version at-or-before the
	// snapshot point can be found with a binary search instead of a scan
	// — without this, a hot document's chain grows unboundedly under
	// sustained writes and every read degrades to O(chain length).
	idx := sort.Search(len(chain), func(i int) bool {
		return chain[i].committedAt > sn.at
	}) - 1

	if idx < 0 || chain[idx].deleted {
		return nil, false
	}
	return chain[idx].data, true
}

// QueryByIndex returns document IDs whose indexed field currently (as of
// this snapshot) equals value. The index itself is an unversioned
// superset (see design doc); Get re-verifies each candidate against the
// snapshot so results are always snapshot-consistent even though the
// index isn't.
func (sn *Snapshot) QueryByIndex(value string) []string {
	sn.store.mu.RLock()
	candidates := append([]string{}, sn.store.index[value]...)
	sn.store.mu.RUnlock()

	seen := make(map[string]bool)
	var result []string
	for _, id := range candidates {
		if seen[id] {
			continue
		}
		seen[id] = true
		doc, ok := sn.Get(id)
		if !ok {
			continue
		}
		if fmt.Sprintf("%v", doc[sn.store.indexField]) == value {
			result = append(result, id)
		}
	}
	return result
}
