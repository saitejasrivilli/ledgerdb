# Design: Observability (v0.11)

## Scope
Prometheus metrics export covering throughput, consumer lag, partition
leader status, replication health, plus a Grafana dashboard definition
checked into the repo. A team claiming to "own deployment and
operations" (as the target JDs describe) with zero observability
undercuts that claim — this version exists to make that claim honest.

## What gets exported
- `ledgerdb_writes_total{ack_level}` — counter, incremented on every
  `producer.Write` call (success or failure both counted, with a
  `result` label, so failure rate is visible not just success volume)
- `ledgerdb_write_latency_seconds{ack_level}` — histogram, wraps the same
  measurement `benchmarks/harness.go` already does manually for v0.7 —
  this version makes that number continuously available, not just a
  one-shot benchmark artifact
- `ledgerdb_raft_is_leader{node}` — gauge, 1 if that node currently
  believes it's the Raft leader, 0 otherwise — reading this across all
  nodes is a real replication-health signal (exactly one leader = healthy)
- `ledgerdb_consumer_lag{group,consumer}` — gauge, difference between a
  partition's latest offset and that consumer's last-read offset

## Why wrap existing types instead of instrumenting them directly
`raft.Raft`, `replication.ReplicatedPartition`, and `producer.Write`
already have clean, tested APIs from earlier versions — instrumenting
them in place would mean touching code that's been stable since v0.1/
v0.4/v0.6 for a concern (metrics) unrelated to their correctness. Instead,
`metrics/instrumented.go` wraps `producer.Write` and exposes a
`Sample(node)` function that reads `GetState()` — same pattern used
throughout this project of composing versions rather than modifying them
(see how v0.4 built on v0.1 and v0.2 unmodified, v0.6 built on v0.4
unmodified).

## Dashboard
`observability/dashboard.json` — a Grafana dashboard definition
referencing the four metrics above, checked into the repo so a reviewer
can load it against a real Prometheus without having to invent panel
queries themselves.

## What proves this correct
`TestV11_MetricsEndpointReturnsExpectedFields` — start the Prometheus
HTTP handler in-process, scrape it, confirm all four metric names appear
in the exposition output with the expected label sets, after driving a
few real writes/reads through instrumented wrappers so the values aren't
just zero-initialized placeholders.

## What v0.11 deliberately does NOT do
- No alerting rules (Prometheus alerting config is a deployment/ops
  concern layered on top of correct metrics, not required to prove
  metrics themselves work)
- No distributed tracing (a different observability axis entirely, out
  of scope for this version)
