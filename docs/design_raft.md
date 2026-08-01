# Design: Raft Consensus Core (v0.1)

## Why Raft, not Paxos
Paxos proven but hard reason about, harder implement correct (multi-decree
Paxos has no canonical spec — every impl improvises). Raft split into
independent subproblems: leader election, log replication, safety, membership
change. Understandability was explicit design goal of Raft paper — matters
here cuz every later version (partitions in v0.3, document store in v1.0)
build atop this core. Bug in consensus poisons everything above it.

## Scope of v0.1
Single Raft group, no partitioning yet. Goal: prove core correct in
isolation before multiplying it across partitions in v0.4.

- Leader election (Lab 3A equivalent)
- Log replication + commit (Lab 3B equivalent)
- Persistence deferred to explicit follow-up (state must survive crash —
  currentTerm, votedFor, log[] persisted before responding to RPCs)
- No snapshotting yet (log compaction lands with v0.2 segment work)

## Core state (per Raft paper Figure 2)

**Persistent on all servers:**
- `currentTerm` — latest term server has seen
- `votedFor` — candidateId voted for in current term (or null)
- `log[]` — command entries, each with term + index

**Volatile on all servers:**
- `commitIndex` — highest log index known committed
- `lastApplied` — highest log index applied to state machine

**Volatile on leaders (reinit after election):**
- `nextIndex[]` — next log entry to send each follower
- `matchIndex[]` — highest log entry known replicated on each follower

## RPCs

**RequestVote** — candidates solicit votes during election. Voter grants
vote only if candidate's log at least as up-to-date as voter's (last log
term higher, or same term + longer log) — this is the safety mechanism
that guarantees no committed entry ever gets overwritten.

**AppendEntries** — leader replicates log entries, also serves as
heartbeat when entries empty. Follower rejects if `prevLogIndex`/
`prevLogTerm` don't match its own log (forces leader to walk `nextIndex`
back until logs agree).

## Election safety argument
Election timeout randomized per server (typical 150-300ms range) so
split votes resolve quickly — if all timeouts were identical, every
election would tie forever. A candidate only becomes leader with votes
from majority of cluster, and at most one leader can get a majority in
a given term since each server votes once per term. Guarantees at most
one leader per term.

## What "correct" means for this version
Definition of done ties to the MIT 6.5840 Lab 3A/3B test harness — not
hand-waved. `go test ./raft/... -v` and `go test ./raft/... -race` both
green, repeated runs (`go test ./raft/... -race -count=10`) to catch
timing-dependent flakes before tagging v0.1.

## Addendum (found during v0.6): commit rule for prior-term entries
§5.4.2 of the Raft paper: a leader must never advance `commitIndex` by
counting replicas of a log entry from an earlier term, even if a majority
already has it — only entries from the leader's *own* current term can be
counted directly (doing so then transitively commits everything before
them too, since logs are prefix-consistent). Without this rule a newly
elected leader can sit indefinitely with a correctly-replicated entry it
never marks committed. Fixed by appending a no-op entry immediately on
election, giving the new leader something in its own term to commit,
which pulls everything before it along. See `v0.6` changelog entry for
the test that caught this (`TestV06_AckAllSurvivesLeaderCrash`).

## Deliberate non-goals for v0.1
- Cluster membership changes (add/remove nodes) — out of scope until
  explicitly revisited, not needed until real deployment tooling exists
- Log compaction / snapshots — lands in v0.2
- Multiple Raft groups — lands in v0.4 (replication combined w/ partitions)
