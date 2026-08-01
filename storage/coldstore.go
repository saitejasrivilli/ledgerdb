package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ColdStore is the minimal object-store abstraction tiering writes
// through — see docs/design_tiered_storage.md for why this is
// intentionally not MinIO-specific.
type ColdStore interface {
	Put(key string, logBytes, indexBytes []byte) error
	Get(key string) (logBytes, indexBytes []byte, err error)
	Delete(key string) error
}

// LocalDirColdStore is a ColdStore backed by a local directory — stands
// in for a real MinIO/S3 bucket in tests, satisfying the project's "no
// cloud services required" constraint while still exercising the real
// tiering logic (upload-then-delete-local, fetch-on-cold-read).
type LocalDirColdStore struct {
	mu  sync.Mutex
	dir string
}

func NewLocalDirColdStore(dir string) (*LocalDirColdStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &LocalDirColdStore{dir: dir}, nil
}

func (s *LocalDirColdStore) Put(key string, logBytes, indexBytes []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.WriteFile(filepath.Join(s.dir, key+".log"), logBytes, 0644); err != nil {
		return fmt.Errorf("cold put log: %w", err)
	}
	if err := os.WriteFile(filepath.Join(s.dir, key+".index"), indexBytes, 0644); err != nil {
		return fmt.Errorf("cold put index: %w", err)
	}
	return nil
}

func (s *LocalDirColdStore) Get(key string) (logBytes, indexBytes []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logBytes, err = os.ReadFile(filepath.Join(s.dir, key+".log"))
	if err != nil {
		return nil, nil, fmt.Errorf("cold get log: %w", err)
	}
	indexBytes, err = os.ReadFile(filepath.Join(s.dir, key+".index"))
	if err != nil {
		return nil, nil, fmt.Errorf("cold get index: %w", err)
	}
	return logBytes, indexBytes, nil
}

func (s *LocalDirColdStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	os.Remove(filepath.Join(s.dir, key+".log"))
	os.Remove(filepath.Join(s.dir, key+".index"))
	return nil
}

// FailingColdStore wraps a ColdStore and forces Put to fail — used to
// test the "never delete local data before the remote copy is confirmed
// durable" invariant.
type FailingColdStore struct {
	Err error
}

func (f *FailingColdStore) Put(key string, logBytes, indexBytes []byte) error {
	return f.Err
}
func (f *FailingColdStore) Get(key string) ([]byte, []byte, error) {
	return nil, nil, f.Err
}
func (f *FailingColdStore) Delete(key string) error {
	return f.Err
}
