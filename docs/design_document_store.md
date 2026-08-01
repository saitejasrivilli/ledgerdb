# Design: Document Store Layer (v1.0)

## Scope
A document store built on the v0.4 replicated log as its write-ahead log.
JSON documents, query-by-field, one index (hash) on a chosen field,
multi-document transactions (all-or-nothing commit). This is the version
that answers DocumentDB specifically — see `docs/design_mvcc.md` for the
MVCC layer, which is named by acronym in that JD, the most direct
keyword-to-substance match anywhere in this project.

## Why the replicated log is the WAL, not a separate one
`replication.ReplicatedPartition` (v0.4) already gives durable, ordered,
quorum-replicated commit — exactly what a document store's WAL needs.
Building a second WAL underneath the document store would duplicate what
v0.4 already proved works (kill-leader/kill-follower recovery, tested).
Every document mutation is proposed as a Raft entry through the existing
`producer.Write` (v0.6, unmodified); the document store's job is purely
what happens to that committed entry once applied — indexing it,
versioning it, exposing it to queries.

## Document format
```json
{"_id": "...", "_version": N, "field1": ..., "field2": ...}
```
`_id` assigned by the writer (or generated if omitted); `_version` is
managed internally by the MVCC layer (see design_mvcc.md), never set by
the caller directly.

## Storage layout on top of the WAL
The applied WAL entries are the source of truth for what happened, in
order — but querying by scanning the whole log for every read would be
useless at any real data volume. So each replica also maintains an
in-memory `documentStore` (a `map[string]*versionedDocument`) rebuilt
from replaying the WAL from offset 0 on startup, then kept current by the
same apply-loop bridge pattern used since v0.4. This mirrors exactly how
v0.4 built a durable copy (storage.Log) from Raft's commit stream —
here the "durable copy" is an indexed in-memory structure instead of a
flat file, same bridge pattern, different sink.

## One index: hash index on a configurable field
`Index(fieldName string)` builds and maintains a
`map[fieldValue][]docID` alongside the primary `map[docID]*document`.
Updated transactionally alongside the primary map on every apply — never
a separate, laggy rebuild step. Query-by-indexed-field is O(1) average
lookup + O(k) for k matching documents; query by a non-indexed field
falls back to a full scan, stated explicitly rather than silently slow.

## Multi-document transactions
A transaction is a batch of document mutations proposed as a single Raft
entry (reusing v0.8's batch-encoding exactly — a transaction IS a batch,
with an explicit "transaction" marker so the apply loop applies all-or-
nothing: either every mutation in the batch updates the in-memory store,
or (if any single mutation in the batch is invalid against the *current*
committed state at apply time) none do, and the whole entry is logged as
a rejected transaction rather than partially applied). This all-or-
nothing property is enforced at apply time, not at proposal time,
because proposal time can't know if a concurrent transaction changed the
relevant documents first — apply time, single-threaded per replica, is
the only point where "the current state" is unambiguous.

## Why build the naive lock-based version first
Per the versioned plan's explicit instruction: build the lock-based
"before" deliberately, not skip straight to MVCC. `lockedStore.go`
implements the document store with a single mutex around all reads and
writes — dead simple, obviously correct, and slow under concurrent
readers (every read blocks on any in-flight write). This isn't thrown
away as scaffolding; it's the baseline `benchmarks/results/v1.0_mvcc.json`
compares against, so "MVCC helped" is a measured claim, not an assumed
one.

## What v1.0 deliberately does NOT do
- No secondary index beyond the one configurable hash index
- No document schema enforcement layered in here (v0.12's schema
  validation already exists as a separate, composable concern — wiring
  it into every document write is additive, not required by this
  version's definition of done)
- No cross-partition transactions — a transaction's documents must all
  route to the same partition in this scope
