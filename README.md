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
| v0.14 | Real network transport (TCP, replaces the in-process simulation for a real partition test) | [docs/design_real_transport.md](docs/design_real_transport.md) |

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
  real sockets (see v0.14 below). Lands in the same range as the
  simulated number, because election-timeout duration (300–600ms, not
  transport speed) dominates both measurements — informative in itself,
  not a coincidence to wave away.
- **Real network partition** (`docs/design_real_transport.md`,
  `TestRealNetworkPartitionOnlyMajoritySideCommits`): a genuine bidirectional
  TCP partition, not a process kill — the majority side elects and
  commits, the isolated side never applies the write, and rejoins cleanly
  once healed.
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
go run ./cmd/benchmark              # throughput, latency, chaos recovery (simulated network)
go run ./cmd/compression_benchmark  # batching + compression ratio
go run ./cmd/mvcc_benchmark         # MVCC vs. lock-based concurrent reads
go run ./cmd/transport_benchmark    # chaos recovery over a real TCP transport
```

## What this project is honest about not doing

Every design doc includes an explicit "what this version deliberately
does NOT do" section — gaps are stated, not hidden.

**Real network transport: closed as of v0.14, partially.** Through v1.0,
Raft/replication ran only over an in-process simulated network
(`raft/network.go`). v0.14 adds a real TCP transport
(`raft/tcp_transport.go`, real `net.Listen`/`net.Dial`, real `net/rpc`)
behind the same `Transport` interface, plus a genuine bidirectional
network-partition test — not a process kill, an actual pair of nodes each
unable to reach the other over real sockets, confirming only the
majority side commits. What's still not covered: real packet
loss/reordering under sustained load, and partitioning is done via an
application-level connection block (`TCPTransport.BlockPeer`) rather than
OS-level firewall/netns rules, since this sandbox has no control over
those. **The claim that this behaves like a real partition is assumed,
not verified** — an app-level block fails a call immediately (closer to
a TCP RST/REJECT), while a real `iptables DROP` typically causes packets
to vanish silently, leaving the sender waiting out a full TCP timeout
before it even learns the connection failed. Those are different timing
profiles, and timing is exactly what the chaos-recovery numbers measure.
The honest answer to "did you test the silent-packet-loss case" is no,
not yet — this hasn't been checked against real `iptables`/`tc netem`
rules, which don't need Docker, just a Linux box with netns/root access.

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

**Everything else here is interface-tested, not integration-tested
against the real external system:**
- Tiered storage (v0.9): `ColdStore` is a real interface with real
  upload-then-delete migration logic, tested against a local-directory
  stand-in — never run against an actual MinIO/S3 endpoint. Attempted as
  a companion fix alongside the transport/TLS work above, but requires a
  running Docker daemon, which wasn't available when this was built —
  left as an explicit, actionable next step, not skipped silently.
- Stream processing (v0.13): the windowing/aggregation logic is real and
  tested — it's not a Flink or Spark job, by design (see
  `docs/design_stream_processing.md`).

When any of these numbers or components go into a resume bullet, that
distinction should travel with them — "implemented and unit-tested TLS
handshake + ACL enforcement" is accurate; "TLS-secured broker
communication" is not, since no broker communication is on a real socket
yet. Same pattern for "designed and tested against a MinIO-compatible
interface" vs. "integrated with MinIO."
