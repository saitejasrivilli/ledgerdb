# Changelog

## v0.13 — Stream processing integration

**Adds:** `streaming.ProcessTumblingWindows` — real tumbling-window
(processing-time) aggregation over a stream of timestamps, emitting a
count per window from earliest to latest touched, including zero-count
gap windows. `streaming.Sink` interface + `InMemorySink` (same "interface
the test double satisfies" pattern as `storage.ColdStore` in v0.9).

**Design doc:** `docs/design_stream_processing.md` — states explicitly
why this is a real Go windowing job rather than a real Flink/Spark
cluster (infra plumbing vs. the actual thing being proven: correct
windowed-aggregation semantics), a considered tradeoff, not a shortcut
taken quietly.

**Tests:** `tests/regression/v0_13_stream_processing_test.go`
- `TestV13_WindowedAggregationCorrectness` — hand-constructed timestamp
  sequence spanning 4 windows (including one all-zero window and events
  landing exactly on a window boundary), emitted counts checked exactly
  against hand-computed values
- `TestV13_SingleEventEmitsOneWindow` / `TestV13_NoEventsEmitsNoWindows` —
  edge cases at the boundaries of the aggregation function

**Breaking check:** full v0.1–v0.12 regression suite re-ran, still green
— `go test ./... -race -count=5` clean.

**Not yet implemented:** no event-time windowing/watermarks/late-data
handling (processing-time only); no windowed joins or multi-stream
aggregation.

## v0.12 — Schema validation

**Adds:** `schema.Registry` — one current schema per partition, JSON
Schema-like minimal representation (field name → type + required),
compatibility checked at registration time (not deferred to individual
writes). `schema.CheckedWrite` validates a document against the
partition's schema before delegating to `security.CheckedWrite` — an
invalid document never reaches Raft at all.

**Design doc:** `docs/design_schema_evolution.md` — explains explicitly
why JSON Schema was chosen over Avro/Protobuf for this version (no binary
schema-registry/wire-format layer exists in this project, and building
one correctly is out of scope here — the compatibility *rules* enforced
are the same either way).

**Tests:** `tests/regression/v0_12_schema_test.go`
- `TestV12_CompatibleSchemaChangeAccepted` — adding an optional field is
  accepted; both old- and new-shaped documents validate under the new
  schema
- `TestV12_BreakingSchemaChangeRejected` — required-field removal, type
  narrowing, and a new required-field-with-no-default are each rejected
  at registration time; the registry's current schema is unchanged after
  every rejected attempt
- `TestV12_CheckedWriteRejectsInvalidDocumentBeforeConsensus` — an
  invalid document is rejected with zero Raft entries committed; a valid
  one succeeds end-to-end

**Breaking check:** full v0.1–v0.11 regression suite re-ran, still green
— `go test ./... -race -count=5` clean.

**Not yet implemented:** no Avro/Protobuf binary schema support; one
schema per partition only, no per-message multi-schema versioning.

## v0.11 — Observability

**Adds:** `metrics/` Prometheus instrumentation — `ledgerdb_writes_total`
(by ack level + result), `ledgerdb_write_latency_seconds` (histogram, by
ack level), `ledgerdb_raft_is_leader` (gauge, by node), and
`ledgerdb_consumer_lag` (gauge, by group/consumer). Wraps existing types
(`producer.Write`, `ReplicatedPartition.GetState`) rather than modifying
them, same composition pattern used throughout this project.
`observability/dashboard.json` — a real Grafana dashboard definition
referencing all four metrics, checked into the repo.

**Design doc:** `docs/design_observability.md`

**Tests:** `tests/regression/v0_11_observability_test.go` —
`TestV11_MetricsEndpointReturnsExpectedFields` drives real writes/reads
through the instrumented wrappers, scrapes the Prometheus handler, and
confirms all four metric names and their expected labels appear (not
just zero-initialized placeholders).

**Breaking check:** full v0.1–v0.10 regression suite re-ran, still green
— `go test ./... -race -count=5` clean.

**Not yet implemented:** no alerting rules, no distributed tracing —
both stated as separate observability axes out of this version's scope.

## v0.10 — Security: TLS + ACLs

**Adds:** `security/tls.go` — real self-signed CA + server cert
generation, `crypto/tls` config construction, tested against a real
`net/tcp` + TLS listener/dial pair (not simulated). `security/acl.go` —
deny-by-default ACL (`Grant`/`Allowed`) enforced at the request-handling
boundary via `CheckedWrite`/`CheckedRead` wrappers around
`producer.Write`/`ReplicatedPartition.ReadLocal`.

**Design doc:** `docs/design_security.md` — states explicitly that TLS
isn't wired into the in-process Raft transport (which has no real
sockets to secure) rather than implying it is.

**Tests:** `tests/regression/v0_10_security_test.go`
- `TestV10_TLSHandshakeRequiresCorrectCA` — real TLS listener/dial: wrong
  CA fails the handshake, correct CA succeeds
- `TestV10_UnauthorizedClientRejected` — an identity with no ACL grant is
  rejected on both read and write
- `TestV10_AuthorizedClientStillWorks` — a granted identity's read/write
  both succeed end-to-end through a real replicated cluster

**Bug caught while writing the TLS test:** `tls.Listen`'s `Accept()`
returns before the handshake completes (handshake is lazy, deferred to
first Read/Write) — the test server was closing the connection
immediately after accept, racing the client's handshake and producing a
spurious EOF even for the correct-CA case. Fixed by explicitly calling
`Handshake()` server-side before closing.

**Breaking check:** full v0.1–v0.9 regression suite re-ran, still green —
`go test ./... -race -count=10` clean.

**Not yet implemented, stated honestly:** TLS not wired into the live
Raft/replication RPC path (no real socket transport exists yet); no
at-rest encryption; no dynamic ACL management API.

## v0.9 — Tiered storage

**Adds:** `storage.ColdStore` interface (+ `LocalDirColdStore`, a local-
directory stand-in for MinIO/S3, keeping the "no cloud services
required" constraint intact) and `Log.TierSegments()` — migrates closed
segments to cold storage, upload-confirmed-before-local-delete, never
tiers the active segment. Reads transparently resolve to cold storage
per-call (no caching, by design — see doc) when a segment has been
tiered out.

**Design doc:** `docs/design_tiered_storage.md`

**Tests:** `tests/regression/v0_9_tiered_storage_test.go`
- `TestV09_ReadFromColdTierAfterMigration` — 40 records across several
  segments, tier them out, every offset still reads back byte-identical
- `TestV09_NoDataLossOnFailedUpload` — a `FailingColdStore` forces the
  upload to error; local segment data must survive untouched (the
  "never delete before confirmed durable" invariant, tested directly)
- `TestV09_ActiveSegmentNeverTiers` — the sole/active segment is never
  eligible for migration

**Breaking check:** full v0.1–v0.8 regression suite re-ran, still green —
`go test ./... -race -count=10` clean (this touched `storage/segment.go`
internals every earlier version's tests already exercise).

**Not yet implemented:** no real MinIO wiring exercised in tests (the
`ColdStore` interface makes that a drop-in swap later, not a rework); no
persistence of which segments are tiered across a process restart; no
re-tiering back to local once cold.

## v0.8 — Batching + compression

**Adds:** `batch.Batcher` — flushes on count or linger time, whichever
fires first, gzip-compressing the whole batch into one Raft entry.
`replication.ReplicatedPartition`'s apply loop transparently unpacks
batch-encoded blobs back into individual messages at individual storage
offsets (magic-marker detection, so v0.4-v0.7's unbatched writes are
unaffected — verified by test, not just by argument).

**Design doc:** `docs/design_batching_compression.md`

**Tests:** `tests/regression/v0_8_batching_compression_test.go`
- `TestV08_CompressedDataRoundTrips` — batch of varied-length (incl.
  empty) messages round-trips byte-exact through Encode/TryDecode
- `TestV08_NonBatchPayloadDecodesAsRaw` — a plain, non-batch payload is
  correctly identified as not-a-batch (`ok=false`), not misparsed
- `TestV08_BatchFlushTriggersOnCount` / `..OnLinger` — both flush
  triggers fire independently and exactly once
- `TestV08_BatchedWriteAppliesAsIndividualMessages` — full path through
  a real replicated cluster: batch commits as one Raft entry, unpacks to
  3 sequential storage offsets, next unbatched write lands correctly
  right after

**Benchmark:** `benchmarks/results/v0.8_compression.json` — 1000
realistic log-line messages, 96,690 bytes uncompressed → 3,342 bytes
compressed, ~28.9x ratio (real measured gzip output, not estimated).

**Breaking check:** full v0.1–v0.7 regression suite re-ran, still green —
`go test ./... -race -count=10` clean (bumped run count since this
touched the shared apply loop every replicated write goes through).

**Not yet implemented:** no configurable compression codec (gzip only),
no at-rest encryption (noted as a real gap, not silently skipped).

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
