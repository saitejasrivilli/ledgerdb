# Design: MVCC (v1.0)

## Scope
Readers see a consistent snapshot of the document store during concurrent
writes — the specific property named by acronym in the DocumentDB JD this
project targets. Built as a genuine replacement for the naive lock-based
store (`docs/design_document_store.md`), not a redesign from a blank
page — same document map, same apply-loop bridge, different concurrency
strategy underneath.

## Why the lock-based version had to exist first
A single mutex around the whole store is trivially correct (there's only
ever one active reader-or-writer, so "consistent snapshot" is automatic
by construction) but serializes all concurrent readers behind any
in-flight write — the exact cost MVCC exists to remove. Without measuring
the lock-based baseline first, "MVCC is faster under concurrent reads"
would be an assumed claim instead of the measured one
`benchmarks/results/v1.0_mvcc.json` records.

## MVCC mechanism: version chains, not copy-on-write snapshots
Each document ID maps to a chain of versions:
`[]versionedDocument{ {version: 1, data: ..., committedAt: raftIndex1},
{version: 2, data: ..., committedAt: raftIndex2}, ... }`. A reader
captures the store's current `commitIndex` at the start of its read
(its "snapshot point") and, for each document it looks up, walks that
document's version chain to find the latest version whose
`committedAt <= snapshot point` — never a version committed after the
reader's snapshot was taken, even if that write has already landed in
the chain by the time the reader gets to it. This is what "readers see a
consistent snapshot" means concretely: not that writes are blocked, but
that a reader's view is pinned to one Raft-commit-ordered point in time
throughout its whole read, regardless of what commits afterward.

## Garbage collection of old versions
Old versions accumulate in each document's chain forever unless
collected. `GCBefore(raftIndex)` removes any version older than the
newest version at-or-before raftIndex for a given document (keeping
exactly the versions a still-active reader below that watermark could
still need) — analogous to Postgres's vacuum, named here so the concept
isn't uncredited. Not run automatically in this version's scope (a
background scheduler is additive, not required for the core correctness
property being proven); exposed as a method callers can invoke.

## Concurrent read during write: the test that proves this, not just
## argues it
`TestConcurrentReadDuringWriteSeesConsistentSnapshot`: a reader takes a
snapshot, then (from another goroutine) several writes commit to the
same document. The reader, mid-iteration over multiple document lookups,
must see the *same* version of every document it looks up — the version
current at its snapshot point — never a mix of pre-snapshot and
post-snapshot versions for different documents in the same logical read.

## Transaction rollback on partial failure
A transaction (v1.0's batch-as-one-Raft-entry, see document store design
doc) that fails validation partway through at apply time must leave the
document store completely unchanged — not partially applied. Implemented
by validating every mutation in the transaction against the current
snapshot *before* committing any of them to the version chains (a
two-phase apply: validate-all, then commit-all, entirely within the
single-threaded apply loop, so no interleaving is possible).

## Benchmark: MVCC vs. lock-based, concurrent read throughput — a real,
## non-flattering measured result
`benchmarks/results/v1.0_mvcc.json` — 8 concurrent readers hammering
`Get` on a single hot document while a steady 1kHz write stream commits,
measured ops/sec for both stores under identical load. The measured
result: the lock-based store is *faster* here (~10.2M reads/sec vs.
~5.9M reads/sec for MVCC, i.e. MVCC at roughly 0.58x). This is reported
as measured, not adjusted to look better, per the project's hard
constraint that no number is ever estimated — an honest negative result
is worth more than a flattering one that wouldn't survive someone else
re-running it.

**Why MVCC loses on this specific workload:** the benchmark hammers one
document with a single writer whose actual work (map/slice update under
lock) is nanosecond-fast — so the plain mutex in `LockedStore` is almost
never actually contended, and Go's `sync.Mutex` fast path is cheap.
MVCC's per-read overhead (an extra lock/unlock pair — one in `Snapshot()`,
one in `Get()` — plus a `sort.Search` closure allocation per call) is pure
added cost on top of a lookup that was already fast, without ever hitting
the scenario MVCC is actually for: a *slow or long-held* write blocking
many readers, or many distinct keys creating real lock contention under a
single global mutex. This benchmark exercises the wrong shape of load to
show MVCC's benefit — a fact discovered by measuring, not assumed going
in, and left as an honestly-reported open question for a future version
rather than reshaped until the number looked better.

## What v1.0 deliberately does NOT do
- No automatic/background GC scheduling (exposed as a manual method only)
- No snapshot isolation across multiple documents spanning a
  transaction boundary beyond what's needed for single-reader consistency
  (true serializable isolation with write-write conflict detection across
  concurrent transactions is a further step past this version's scope)
