# Design: Replication — Raft + Partitions Combined (v0.4)

## Scope
Each partition is now backed by its own Raft group (v0.1's consensus
core), with the v0.2 segment log as the durable, queryable copy of
committed data on every replica. This is the first version where "what
happens if a broker dies mid-write" has a real, testable answer — the
checkpoint the versioned plan calls out explicitly.

## Why the Raft log and the storage log are two different things
Raft already maintains its own in-memory `[]LogEntry` (raft.go) purely for
consensus bookkeeping — term/index/command, used for election safety and
commit-majority counting. That log is not what a client reads from; it's
not compacted, not segmented, and lives in memory only. The v0.2
`storage.Log` is the durable, disk-backed, segment-based copy that
consumers actually read. Keeping them separate means neither has to
compromise: Raft's log stays simple and paper-shaped, storage stays
segment/compaction-shaped, and the only bridge between them is the apply
loop below.

## The apply bridge
Every replica runs one goroutine per partition that drains Raft's
`applyCh` and appends each committed command's payload to that replica's
local `storage.Log`, in commit order. Because Raft guarantees all replicas
apply entries in the same order, every replica's local storage offset for
a given logical write is identical — offset `N` means the same record on
every node. This is the property the test suite checks directly (not
assumed): read the same offset on two different replicas, same node
reboots, or a mid-write follower disconnect must all show it holding.

## Write path
1. Client proposes payload to whichever replica it can reach
2. If that replica isn't the leader, the proposal is rejected (client
   retries against another node — this version does no
   leader-forwarding, that's left as a client concern, not a broker one)
3. Leader calls `raft.Start(payload)`, gets back a raft log index
4. Once a majority of the Raft group has replicated that index, Raft's
   own commit logic (already built in v0.1, unmodified here) marks it
   committed and the apply loop fires on every replica including the
   leader

## Failure scenarios this version proves, not just claims
- **Kill a follower mid-write:** remaining 2 of 3 still form a Raft
  majority, writes keep committing, disconnected follower's local storage
  log simply stops growing
- **Reconnect the follower:** Raft's existing AppendEntries conflict-
  backtracking (v0.1, unmodified) catches the follower's Raft log up;
  the apply loop then catches its storage log up to match
- **Kill the leader:** remaining 2 servers elect a new leader (v0.1's
  election, unmodified); new leader keeps accepting writes; killed
  leader rejoining as a follower catches up the same way

## What v0.4 deliberately does NOT do
- No leader-forwarding/redirection for clients hitting a follower
  (v0.4's REST layer doesn't exist yet — that's v0.6+/API concern)
- No dynamic partition-to-node reassignment
- No snapshotting of the Raft log (still relies on full log replay on
  rejoin — acceptable at this data scale, revisit if it becomes a real
  bottleneck)
