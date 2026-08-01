package docstore

import (
	"encoding/json"
	"fmt"
	"sync"
)

// LockedStore is the naive, obviously-correct baseline described in the
// design doc: one mutex around every read and write. Built and measured
// first so "MVCC helped" (docs/design_mvcc.md) is a comparison against a
// real number, not an assumption.
type LockedStore struct {
	mu      sync.Mutex
	docs    map[string]Document
	version map[string]int
}

func NewLockedStore() *LockedStore {
	return &LockedStore{
		docs:    make(map[string]Document),
		version: make(map[string]int),
	}
}

// ApplyTransaction validates every mutation's ExpectedVersion precondition
// against current state before committing any of them — same all-or-
// nothing contract as MVCCStore.ApplyTransaction, just with a single lock
// instead of version chains.
func (s *LockedStore) ApplyTransaction(muts []Mutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range muts {
		if m.ExpectedVersion != nil && s.version[m.ID] != *m.ExpectedVersion {
			return fmt.Errorf("docstore: expected version %d for %q, got %d", *m.ExpectedVersion, m.ID, s.version[m.ID])
		}
	}

	for _, m := range muts {
		switch m.Op {
		case "put":
			var doc Document
			if err := json.Unmarshal(m.Data, &doc); err != nil {
				return fmt.Errorf("docstore: invalid document data for %q: %w", m.ID, err)
			}
			s.docs[m.ID] = doc
			s.version[m.ID]++
		case "delete":
			delete(s.docs, m.ID)
			s.version[m.ID]++
		}
	}
	return nil
}

func (s *LockedStore) Get(id string) (Document, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, ok := s.docs[id]
	return doc, ok
}

func (s *LockedStore) Version(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version[id]
}
