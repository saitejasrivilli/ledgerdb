# ledgerdb

**A distributed commit log + document store, built version by version and
proven against real infrastructure — Raft consensus, real TCP transport,
real `iptables`/`tc netem` fault injection, real MinIO, with every
benchmark number traceable to a committed JSON file.**

[![MinIO integration](https://github.com/saitejasrivilli/ledgerdb/actions/workflows/minio-integration.yml/badge.svg)](https://github.com/saitejasrivilli/ledgerdb/actions/workflows/minio-integration.yml)
[![Network fault injection](https://github.com/saitejasrivilli/ledgerdb/actions/workflows/network-fault.yml/badge.svg)](https://github.com/saitejasrivilli/ledgerdb/actions/workflows/network-fault.yml)
[![Go Reference](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Each tag (`v0.1` … `v1.0`, `v0.14.x`) builds on every prior one
unmodified, with a growing regression suite that must pass in full
before the next version ships. The commit history is the evidence: check
out any tag and see a working, tested system at that stage.

## Contents

- [Quickstart](#quickstart)
- [Architecture](#architecture)
- [What's here, in build order](#whats-here-in-build-order)
- [Measured, not estimated](#measured-not-estimated)
- [Durability](#durability)
- [What this project is honest about not doing](#what-this-project-is-honest-about-not-doing)
- [License](#license)

## Quickstart

```bash
git clone https://github.com/saitejasrivilli/ledgerdb.git
cd ledgerdb
go build ./...
go test ./... -race -count=5
```

Run the benchmarks (each writes real, timestamped output to
`benchmarks/results/`):

```bash
go run ./cmd/benchmark              # throughput, latency, chaos recovery (simulated network)
go run ./cmd/compression_benchmark  # batching + compression ratio
go run ./cmd/mvcc_benchmark         # MVCC vs. lock-based concurrent reads
go run ./cmd/transport_benchmark    # chaos recovery over a real TCP transport
```

Two test suites need infrastructure this repo can't assume you have
running, so they skip cleanly if it's absent and run for real in CI:

```bash
# real MinIO (needs a running MinIO instance; see .github/workflows/minio-integration.yml)
MINIO_ENDPOINT=localhost:9000 go test ./tests/integration/... -v

# real iptables/tc netem fault injection (Linux + root only)
sudo NETFAULT_TEST=1 go test ./tests/networkfault/... -v
```

## Architecture

```
        client
          │
          ▼
  ┌───────────────┐        Raft group (per partition)
  │  producer.Write│──┐    ┌─────────┐  ┌─────────┐  ┌─────────┐
  │  (ack 0/1/all) │  └───▶│ leader  │◀▶│follower │◀▶│follower │
  └───────────────┘        └────┬────┘  └────┬────┘  └────┬────┘
                                 │            │            │
                            storage.Log  storage.Log  storage.Log
                          (segments, WAL) (segments, WAL) (segments, WAL)
                                 │
                     ┌───────────┴────────────┐
                     ▼                        ▼
              docstore (MVCC)          tiered segments
              JSON docs + index          → real MinIO
```

Raft (`raft/`) owns commit ordering and runs over a real TCP transport
(`raft/tcp_transport.go`) behind a `Transport` interface — the same
interface an in-process simulated network (`raft/network.go`) also
satisfies, which is what every version's regression suite runs against
for speed and determinism. `storage.Log` (`storage/`) is the durable,
segment-based WAL every replica applies committed entries into.
`replication.ReplicatedPartition` bridges the two. Everything above that
— partitioning, consumer groups, ack levels, batching/compression,
tiered storage, security, observability, schema validation, stream
processing, and the MVCC document store — is a version-by-version layer
on top, each with its own design doc.

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
| v0.14 | Real network transport (TCP, real partition test) | [docs/design_real_transport.md](docs/design_real_transport.md) |
| v0.14.4 | Real `iptables`/`tc netem` fault injection | [docs/design_network_fault.md](docs/design_network_fault.md) |
| v0.15 | MongoDB wire protocol translation | [docs/design_wire_protocol.md](docs/design_wire_protocol.md) |

See [CHANGELOG.md](CHANGELOG.md) for what shipped, what tests cover it,
and what bugs were caught and fixed along the way, per version.

## Measured, not estimated

Every number below traces to a file in `benchmarks/results/`.

- **Chaos recovery, in-process simulated network** (`v0.7_baseline.json`):
  leader-kill detection ~316ms, first successful write after recovery
  ~326ms. This was a process-crash recovery number over a simulated
  network at the time it was measured.
- **Chaos recovery, real TCP transport** (`v0.14_transport.json`, 5
  runs): detection ~321–373ms, recovery ~325–379ms — re-measured over
  real sockets. Lands in the same range as the simulated number, because
  election-timeout duration (300–600ms, not transport speed) dominates
  both measurements — informative in itself, not a coincidence to wave
  away.
- **Real iptables DROP vs. app-level BlockPeer**
  (`v0.14.4_iptables_vs_blockpeer.json`, 10 runs): detection ~346–458ms,
  recovery ~358–469ms — close enough to the numbers above that the
  theoretical DROP-vs-REJECT timing concern doesn't dominate in practice
  here. Getting this clean measurement surfaced two real, independent
  bugs (a Raft election-timer redraw bug and a per-peer transport
  locking bug) that no earlier test had ever triggered — see
  [docs/design_network_fault.md](docs/design_network_fault.md).
- **Real network partition** (`docs/design_real_transport.md`,
  `TestRealNetworkPartitionOnlyMajoritySideCommits`): a genuine
  bidirectional TCP partition, not a process kill — the majority side
  elects and commits, the isolated side never applies the write, and
  rejoins cleanly once healed.
- **Compression** (`v0.8_compression.json`): ~6.9x gzip ratio on 1000
  log-line messages with realistic per-line variation (varying paths,
  latencies, UUIDs, occasional free-text errors). An earlier corpus with
  only two varying integers per line measured ~28.9x — an inflated number
  from unrealistically repetitive test data, replaced once noticed.
- **MVCC vs. lock-based reads** (`v1.0_mvcc.json`): measured honestly —
  MVCC was *slower* (~0.58x) on a single-hot-document, low-contention
  workload; see [docs/design_mvcc.md](docs/design_mvcc.md) for why, and
  why the number wasn't reshaped to look better.

## Durability

100 kill-and-recover cycles against the document store, zero data loss
across all of them — `tests/regression/v1_0_durability_test.go`. Last
re-verified against current HEAD (not just trusted from when it was
written) on 2026-08-01.

## What this project is honest about not doing

Every design doc includes an explicit "what this version deliberately
does NOT do" section — gaps are stated, not hidden.

**Real network transport: fully closed.** Through v1.0, Raft/replication
ran only over an in-process simulated network (`raft/network.go`). v0.14
adds a real TCP transport (`raft/tcp_transport.go`) behind the same
`Transport` interface, plus a genuine bidirectional network-partition
test over real sockets. v0.14.4 then closed the one remaining open
question: is an app-level `BlockPeer` actually equivalent to a real
kernel-level packet drop, or just assumed to be? Measured directly with
real `iptables DROP` + `tc netem` loss/reordering
(`tests/networkfault/`, CI: `.github/workflows/network-fault.yml`,
local: `scripts/run_network_fault_tests.sh`) — the answer: yes, close
enough (see the numbers above).

**The finding that matters most here isn't about distributed systems —
it's about testing.** The simulated-network suite, the app-level
`BlockPeer` partition test, and the "genuine" TCP partition test all
passed cleanly, every time, while two real bugs sat undetected
underneath. Only forcing a real kernel-level fault surfaced them: (1)
`raft.go`'s election timer redrew its random timeout on every 10ms poll
tick instead of once per reset, which (before the fix) let two nodes
converge on firing in the same window repeatedly — observed directly as
14+ seconds of lockstep term climbing that never resolved on its own;
(2) `TCPTransport.getClient` held one mutex across every peer for the
full dial duration, so a real isolated peer's repeated redial attempts
(every 50ms) starved heartbeats to the *other, reachable* peer for up to
500ms each time. **Both are fixed** — verified 10x/5x clean afterward,
in CI and locally — but the fixes only happened because a deeper layer
of testing was forced; every prior, passing layer had given false
confidence. See `docs/design_network_fault.md` and `docs/design_raft.md`'s addendum
for the full story — real infrastructure found real bugs a good-enough
simulation had been quietly hiding.

**TLS: also closed, in the same pass as the transport swap.**
`NewTCPTransportTLS` wires v0.10's real cert/handshake machinery
(`security.GenerateCA`/`IssueServerCert`/`ServerTLSConfig`/
`ClientTLSConfig`) into `TCPTransport` — Raft now elects and commits over
a real TLS-secured socket, and a node with a mismatched CA can never
complete a handshake with the cluster (tested directly, not assumed).
Doing this surfaced a real bug in v0.10's own cert helper: it only set
`DNSNames`, so cert verification silently failed for any `ServerName`
that's a literal IP (`"127.0.0.1"`, what these new tests use) rather than
a hostname (`"localhost"`, what v0.10's original test happened to use) —
fixed by setting `IPAddresses` or `DNSNames` based on what the host
actually is. See `docs/design_real_transport.md` for the full story.

**Tiered storage: also closed, against a real MinIO instance.**
`storage.MinioColdStore` (real `minio-go` client, same `ColdStore`
interface `LocalDirColdStore` already satisfies) is exercised by
`tests/integration/minio_test.go` — passing both in CI
(`.github/workflows/minio-integration.yml`, a real MinIO container) and
locally against a real `docker run minio/minio` instance, not just the
local-directory stand-in from v0.9. This is what actually converts the
v0.9 "interface-tested" claim into "integrated with MinIO" for real.

**MongoDB wire protocol: also closed, against the real official driver.**
`mongowire/` (v0.15) translates a deliberately small command subset
(`insert`/`find`/`update`/`delete`) into calls against `docstore` —
tested with `go.mongodb.org/mongo-driver/v2/mongo`, the real official
client, not a hand-rolled test client that only exercises what the
server-side code expects. That distinction mattered: the real driver's
legacy OP_QUERY handshake, `bson.D`-vs-`bson.M` nested decoding, the
required `cursor.ns` format, and BSON's `null`-vs-empty-array distinction
all surfaced as real bugs a hand-rolled client would have sailed past.
See `docs/design_wire_protocol.md`.

**Everything else here is interface-tested, not integration-tested
against the real external system:**
- Stream processing (v0.13): the windowing/aggregation logic is real and
  tested — it's not a Flink or Spark job, by design (see
  `docs/design_stream_processing.md`).

When any of these numbers or components go into a resume bullet, that
distinction should travel with them — "implemented and unit-tested TLS
handshake + ACL enforcement" is accurate; "TLS-secured broker
communication" is not, since no broker communication is on a real socket
yet. Same pattern for "designed and tested against a MinIO-compatible
interface" vs. "integrated with MinIO."

## License

[MIT](LICENSE)
