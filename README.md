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
  first successful write after recovery ~326ms
- **Compression** (`v0.8_compression.json`): ~28.9x gzip ratio on 1000
  realistic log-line messages
- **MVCC vs. lock-based reads** (`v1.0_mvcc.json`): measured honestly —
  MVCC was *slower* (~0.58x) on a single-hot-document, low-contention
  workload; see [docs/design_mvcc.md](docs/design_mvcc.md) for why, and
  why the number wasn't reshaped to look better

## Durability

100 kill-and-recover cycles against the document store, zero data loss
across all of them — `tests/regression/v1_0_durability_test.go`.

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
does NOT do" section — gaps are stated, not hidden. Notable ones: no real
network transport (Raft/replication run over an in-process simulated
network throughout — see `docs/design_benchmarking.md`), no TLS wired
into that transport (v0.10 builds and tests the real cert/handshake
machinery, explicitly not plugged into a transport that doesn't have real
sockets), no at-rest encryption, no real MinIO/Flink/Spark integration
(v0.9/v0.13 build the correctness-proving logic against interfaces those
systems would satisfy, not against the real systems themselves).
