// Package docstore implements the document store + MVCC layer described
// in docs/design_document_store.md and docs/design_mvcc.md, built on top
// of raft.Raft (v0.1) and storage.Log (v0.2) directly — the same
// primitives v0.4's ReplicatedPartition composes, applied here to an
// indexed in-memory document map instead of a flat append log.
package docstore

import "encoding/json"

// Document is a decoded JSON document body (excluding the _id/_version
// bookkeeping fields, which live in versionedDocument instead).
type Document map[string]interface{}

// Mutation is one document change within a transaction (a transaction is
// simply []Mutation proposed as a single Raft entry — see design doc).
type Mutation struct {
	Op   string          `json:"op"` // "put" or "delete"
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data,omitempty"`
	// ExpectedVersion, if non-nil, is an optimistic-concurrency
	// precondition: the mutation (and the whole transaction it belongs
	// to) is rejected at apply time if the document's current version
	// doesn't match. nil means "no precondition."
	ExpectedVersion *int `json:"expected_version,omitempty"`
}

// EncodeTransaction marshals a batch of mutations into the bytes proposed
// as one Raft entry.
func EncodeTransaction(muts []Mutation) ([]byte, error) {
	return json.Marshal(muts)
}

// DecodeTransaction reverses EncodeTransaction.
func DecodeTransaction(blob []byte) ([]Mutation, error) {
	var muts []Mutation
	if err := json.Unmarshal(blob, &muts); err != nil {
		return nil, err
	}
	return muts, nil
}
