# Changelog

## v0.7 — Benchmarking + chaos harness

**Adds:** `benchmarks/` load generator + chaos harness, `cmd/benchmark`
runnable that produces `benchmarks/results/v0.7_baseline.json` — the
first real, citable numbers in this project. Nothing before this version
gets a resume bullet per the versioned build plan.

**Design doc:** `docs/design_benchmarking.md` — includes an honest
disclosure that `ack=all` latency is dominated by a 10ms poll interval in
`WaitApplied`, not pure consensus latency.

**Measured (see `benchmarks/results/v0.7_baseline.json` for full output):**
- ack=0: ~332k ops/sec (in-process, no quorum wait)
- ack=1: ~447k ops/sec (leader-local accept only)
- ack=all: ~60 ops/sec, p50 21ms (bounded by poll granularity, disclosed above)
- Chaos: leader-kill detection ~316ms, first successful write after
  recovery ~326ms

**Tests:** `benchmarks/harness_test.go` — smoke tests only (per design
doc: this version adds measurement, not new correctness surface, so no
new regression-suite entries this time).

**Breaking check:** full v0.1–v0.6 regression suite re-ran, still green —
`go test ./... -race -count=5` clean.

## v0.6 — Producer acknowledgment levels

**Adds:** `producer.Write` with three genuinely different commit paths —
`AckNone` (fire-and-forget), `AckLeader` (wait for leader-local accept
only), `AckAll` (wait for Raft's own majority-commit rule via
`WaitApplied` on the leader).

**Design doc:** `docs/design_ack_levels.md`

**Tests:** `tests/regression/v0_6_ack_levels_test.go`
- `TestV06_AckAllSurvivesLeaderCrash` — hard invariant: an ack=all write
  is present on whatever replica wins the next election, no exceptions
- `TestV06_AckLeaderCanLoseDataOnLeaderCrash` — demonstrates ack=1's
  documented weakness concretely: isolate leader, write, kill leader,
  entry is provably gone from the surviving quorum
- `TestV06_AckNoneNeverBlocksOrPanics` — ack=0 never blocks or errors,
  even against a non-leader

**Real Raft bug found and fixed by `TestV06_AckAllSurvivesLeaderCrash`:**
a newly-elected leader could hold a committed entry in its log yet never
advance `commitIndex` past it, because Raft's own commit rule (§5.4.2)
forbids counting replicas of a prior-term entry directly — only entries
from the leader's *current* term can be counted, and doing so transitively
commits everything before them. Fixed by appending a no-op entry
immediately on election (`raft/raft.go` `becomeLeader`), which gives the
new leader something in its own term to commit. `applyTicker` now skips
emitting `ApplyMsg` for no-op entries (`Command == nil`) so downstream
apply loops don't need to special-case them.

**Breaking check:** full v0.1–v0.5 regression suite re-ran, still green —
`go test ./... -race -count=10` clean, 10 consecutive runs (bumped from 5
given this touched raft.go's core commit logic).

## v0.5 — Consumer groups + rebalancing

**Adds:** `group.Coordinator` — round-robin partition assignment across
live consumers in a group, deterministic recompute on join/leave, and a
heartbeat-expiry loop (same threshold-based pattern as v0.1's Raft health
checks) that evicts and rebalances on crash.

**Design doc:** `docs/design_consumer_groups.md`

**Tests:** `tests/regression/v0_5_consumer_groups_test.go`
- `TestV05_RebalanceOnJoin` — assignment stays balanced as members join
- `TestV05_RebalanceOnCrash` — a consumer that stops heartbeating gets
  evicted, its partitions redistributed
- `TestV05_RebalanceOnLeave` — clean-shutdown path rebalances immediately,
  no heartbeat-timeout wait
- `TestV05_NoPartitionStarvation` — assignment stays within
  floor/ceil(P/C) across several partition/consumer count combinations

**Breaking check:** full v0.1–v0.4 regression suite re-ran unmodified,
still green — `go test ./... -race -count=5` clean.

**Not yet implemented:** no managed consumer-offset commit/tracking API
(consumers track their own read position); stop-the-world rebalancing
only, no cooperative/incremental variant.

## v0.4 — Replication (Raft + partitions combined)

**Adds:** `replication.ReplicatedPartition` — one Raft group (v0.1,
unmodified) per partition, with an apply loop bridging committed Raft
entries into each replica's local `storage.Log` (v0.2, unmodified). Same
raft index commits in the same order on every replica, so a given logical
write lands at the same storage offset everywhere — verified by test, not
assumed.

**Design doc:** `docs/design_replication.md`

**Tests:** `tests/regression/v0_4_replication_test.go`
- `TestV04_KillFollowerAndRecover` — 2-of-3 quorum keeps committing with a
  follower down; reconnected follower catches up to the exact same offsets
- `TestV04_KillLeaderElectNewOne` — new leader elected, keeps accepting
  writes; old leader rejoins as follower and catches up
- `TestV04_CommittedWritesSurviveAcrossReplicas` — 10 writes, every
  replica agrees byte-for-byte at every offset

**Bug caught by `-race`:** `ReplicatedPartition.ReadLocal` /
`NextLocalOffset` called `storage.Log` directly without synchronizing
against the apply loop's own `Append` calls — `storage.Log` was never
meant to be safe for concurrent access (documented in v0.2). Fixed by
serializing all local log access through the partition's own mutex.

**Breaking check:** full v0.1–v0.3 regression suite re-ran unmodified,
still green — `go test ./... -race -count=5` clean, 5 consecutive runs.

**Not yet implemented:** no client-facing leader-forwarding/redirection
(a proposal to a follower is simply rejected) — that's the REST API's
concern later; no Raft log snapshotting yet (full replay on rejoin).

## v0.3 — Partitioning

**Adds:** `storage.PartitionedLog` — routes keys across N independent
`storage.Log` instances (FNV-1a hash mod N), each with its own segment
directory and offset space. Pure routing layer, no changes to the v0.2
segment format.

**Design doc:** `docs/design_partitioning.md`

**Tests:** `tests/regression/v0_3_partitioning_test.go`
- `TestV03_PartitionIsolation` — same key always routes to same partition
- `TestV03_IndependentOffsetSpaces` — writing to one partition never
  advances another's offset counter
- `TestV03_ReadBackByPartitionAndOffset` — round-trip correctness across
  all partitions

**Breaking check:** full v0.1–v0.2 regression suite re-ran unmodified,
still green — `go test ./... -race -count=5` clean.

**Not yet implemented:** no replication (each partition lives on one
process only) — lands v0.4.

## v0.2 — Single-partition append-only log

**Adds:** Segment-based storage layer (`storage/`) — fixed-size segment
files with length-prefixed records and a fixed-width offset index, segment
rolling, retention-based compaction (delete whole old segments, never the
active one), crash recovery via index rebuild-by-scan on open.

**Design doc:** `docs/design_log_storage.md`

**Tests:** `tests/regression/v0_2_log_storage_test.go`
- `TestV02_SequentialWriteRead` — 100 sequential offsets round-trip
- `TestV02_SegmentRoll` — tiny max-segment-size forces rolls, reads still
  correct across segment boundaries
- `TestV02_CrashRecovery` — close/reopen rebuilds index, offsets continue
  correctly after recovery
- `TestV02_Compaction` — old segments deleted, retained data still readable,
  compacted-away offsets correctly error (not silently wrong)

**Breaking check:** all v0.1 Raft tests re-ran unmodified, still green —
`go test ./... -race -count=5` clean across both packages.

**Not yet implemented:** no Raft integration for this log yet (v0.4), no
partitioning (v0.3), no compression (v0.8).

## v0.1 — Raft consensus foundation

**Adds:** Leader election (randomized timeout, RequestVote RPC w/
up-to-date-log check) and log replication (AppendEntries RPC, log
matching + conflict backtracking, majority-commit rule restricted to
current-term entries). Single Raft group, in-process simulated network
(`raft/network.go`) standing in for labrpc — supports reliable/unreliable
modes and connect/disconnect for partition tests.

**Design doc:** `docs/design_raft.md`

**Tests:** `raft/raft_test.go`
- `TestInitialElection` — fresh cluster converges on one leader, term stable
- `TestReElection` — leader failure triggers new election; recovers after
  reconnection
- `TestBasicAgreement` — command commits on all 3 servers
- `TestFailAgree` — cluster keeps committing via quorum with one follower
  disconnected; disconnected follower catches up on reconnect
- `TestUnreliableAgree` — agreement still completes under simulated packet
  loss/delay

**Verified:** `go test ./raft/... -v` and `go test ./raft/... -race -count=10`
both green (see terminal output at tag time — 10 consecutive race-detector
runs, no flakes).

**Not yet implemented (explicit non-goals for this version):**
- Persistence across restart (state kept in-memory only)
- Log compaction / snapshotting (lands in v0.2)
- Cluster membership changes
- Multiple Raft groups (lands in v0.4)
