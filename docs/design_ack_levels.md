# Design: Producer Acknowledgment Levels (v0.6)

## Scope
Three genuinely different commit paths for a producer write against a
`replication.ReplicatedPartition` (v0.4) — not a flag that's ignored
downstream. Each level trades latency for durability differently, and the
tradeoff must be demonstrable, not asserted: `TestV06` durability tests
kill the leader immediately after a write at each level and check what
actually survives.

## The three levels

**ack=0 (fire-and-forget)**
Producer calls `Propose` and returns immediately without waiting on
anything — not even confirmation the local Raft `Start` call succeeded
past validation. Fastest, weakest: if the leader crashes before the entry
even reaches its own Raft log durably, the write is gone with no producer-
visible error. Appropriate only for data where loss is tolerable (metrics,
best-effort telemetry) — stated here so nobody reaches for ack=0 assuming
it behaves like ack=1.

**ack=1 (leader-only)**
Producer waits for the leader to accept the entry into its own Raft log
(`Start` returns), but not for replication to followers. Survives a
producer-side crash right after the call returns (the leader has it), but
not a leader crash before the entry replicates — a new leader elected from
a follower that never got the entry will not have it. This is the level
most people mentally default to and the one most likely to surprise them
under a leader failure; the durability test for this level exists
specifically to make that failure mode visible instead of theoretical.

**ack=all (quorum)**
Producer waits for `WaitApplied` to confirm the entry committed via Raft's
own majority-commit rule (v0.1, unmodified) — i.e., a majority of
replicas have it before the producer's call returns. Survives any single-
node failure including the leader, because Raft's election safety
guarantees a new leader is elected only from a replica holding all
committed entries. Slowest of the three, matches W=2 R=2-style quorum
reasoning already used for the object-store project's design, applied
here to the log instead of an object store.

## Why these three specifically
Matches Kafka's own `acks` semantics (0 / 1 / all) — not inventing a new
scheme, using names an interviewer or reviewer will already recognize,
which matters given this project's stated purpose of demonstrating
production-shaped familiarity.

## What each durability test must show, concretely
For each level: write one entry, immediately kill the leader (network
disconnect, matching v0.4's failure model), force a new election, then
check whether the entry is present on the new leader.
- ack=0: entry may or may not survive (test asserts "no crash", not
  "always survives" — that would misrepresent the level)
- ack=1: entry does NOT reliably survive a leader crash immediately after
  write — test demonstrates a case where it's lost
- ack=all: entry MUST survive every time — test asserts this as a hard
  invariant, any failure here is a correctness bug, not a semantics note
