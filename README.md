# ledgerdb

A distributed commit log + document store, built version by version —
each tag (`v0.1` ... `v1.0`) builds on every prior one unmodified, with a
growing regression suite that must pass in full before the next version
is tagged. The commit history is the evidence: check out any tag and see
a working, tested system at that stage.

## What's here, in build order

| Version | What it adds | Design doc |
|---|---|---|
| v0.1 | Raft consensus (leader election + log replication) | [docs/design_raft.md](docs/design_raft.md) |
| v0.2 | Segment-based append-only log storage | [docs/design_log_storage.md](docs/design_log_storage.md) |
| v0.3 | Partitioning | [docs/design_partitioning.md](docs/design_partitioning.md) |
| v0.4 | Replication (Raft group per partition) | [docs/design_replication.md](docs/design_replication.md) |
| v0.5 | Consumer groups + rebalancing | [docs/design_consumer_groups.md](docs/design_consumer_groups.md) |
| v0.6 | Producer ack levels (0/1/all) | [docs/design_ack_levels.md](docs/design_ack_levels.md) |
| v0.7 | Benchmarking + chaos harness | [docs/design_benchmarking.md](docs/design_benchmarking.md) |
| v0.8 | Batching + compression | [docs/design_batching_compression.md](docs/design_batching_compression.md) |
| v0.9 | Tiered storage | [docs/design_tiered_storage.md](docs/design_tiered_storage.md) |
| v0.10 | Security: TLS + ACLs | [docs/design_security.md](docs/design_security.md) |
| v0.11 | Observability (Prometheus + Grafana) | [docs/design_observability.md](docs/design_observability.md) |
| v0.12 | Schema validation | [docs/design_schema_evolution.md](docs/design_schema_evolution.md) |
| v0.13 | Stream processing (tumbling windows) | [docs/design_stream_processing.md](docs/design_stream_processing.md) |
| v1.0 | Document store + MVCC | [docs/design_document_store.md](docs/design_document_store.md), [docs/design_mvcc.md](docs/design_mvcc.md) |

See [CHANGELOG.md](CHANGELOG.md) for what shipped, what tests cover it,
and what bugs were caught and fixed along the way, per version.

## Measured, not estimated

Every number below traces to a file in `benchmarks/results/`.

- **Chaos recovery** (`v0.7_baseline.json`): leader-kill detection ~316ms,
  first successful write after recovery ~326ms. This is a *process-crash*
  recovery number, not a network-partition one — see "What this project
  is honest about not doing" below.
- **Compression** (`v0.8_compression.json`): ~6.9x gzip ratio on 1000
  log-line messages with realistic per-line variation (varying paths,
  latencies, UUIDs, occasional free-text errors). An earlier corpus with
  only two varying integers per line measured ~28.9x — an inflated number
  from unrealistically repetitive test data, replaced once noticed.
- **MVCC vs. lock-based reads** (`v1.0_mvcc.json`): measured honestly —
  MVCC was *slower* (~0.58x) on a single-hot-document, low-contention
  workload; see [docs/design_mvcc.md](docs/design_mvcc.md) for why, and
  why the number wasn't reshaped to look better

## Durability

100 kill-and-recover cycles against the document store, zero data loss
across all of them — `tests/regression/v1_0_durability_test.go`. Last
re-verified against current HEAD (not just trusted from when it was
written) on 2026-08-01: `go test ./tests/regression/... -run TestV1_0_HundredKillAndRecoverCyclesNoDataLoss -count=1 -v` — clean.

## Running the tests

```
go build ./...
go test ./... -race -count=5
```

## Running the benchmarks

```
go run ./cmd/benchmark              # throughput, latency, chaos recovery
go run ./cmd/compression_benchmark  # batching + compression ratio
go run ./cmd/mvcc_benchmark         # MVCC vs. lock-based concurrent reads
```

## What this project is honest about not doing

Every design doc includes an explicit "what this version deliberately
does NOT do" section — gaps are stated, not hidden.

**The one that matters most: no real network transport.** Raft,
replication, and every chaos/recovery number in this repo run over an
in-process simulated network (`raft/network.go`), not real sockets. That
means none of it has ever been exercised against real packet loss,
reordering, or an actual network partition (both sides of a split still
thinking they're the leader) — only simulated process disconnect, which
is a *process-crash* fault model, not a *partition* fault model. Those
are different claims: "recovers from a node dying" is proven here;
"tolerates a network partition" is not, and shouldn't be implied by the
316ms/326ms numbers above. Swapping in a real TCP/gRPC transport (even
just localhost, or across two Docker containers) so packet drops and
partitions can be injected for real is the highest-value gap left in
this project.

**Everything else here is interface-tested, not integration-tested
against the real external system:**
- TLS (v0.10): real cert generation and a real TLS handshake are tested
  against a real `net/tcp` listener — but it's not wired into the Raft/
  replication transport above, which has no real sockets to secure yet.
- Tiered storage (v0.9): `ColdStore` is a real interface with real
  upload-then-delete migration logic, tested against a local-directory
  stand-in — never run against an actual MinIO/S3 endpoint.
- Stream processing (v0.13): the windowing/aggregation logic is real and
  tested — it's not a Flink or Spark job, by design (see
  `docs/design_stream_processing.md`).

When any of these numbers or components go into a resume bullet, that
distinction should travel with them — "implemented and unit-tested TLS
handshake + ACL enforcement" is accurate; "TLS-secured broker
communication" is not, since no broker communication is on a real socket
yet. Same pattern for "designed and tested against a MinIO-compatible
interface" vs. "integrated with MinIO."
