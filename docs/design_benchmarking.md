# Design: Benchmarking + Chaos Harness (v0.7)

## Scope
This version adds measurement, not new system behavior. No new
correctness tests — the regression suite is unchanged from v0.6. What's
new: a load generator, throughput/latency/lag instrumentation, and chaos
scripts that kill a leader mid-burst and measure recovery time. Output
lands in `benchmarks/results/v0.7_baseline.json` — per the project's hard
constraint, this is the only number anyone is allowed to quote afterward;
nothing before this version gets a resume bullet.

## What gets measured
- **Throughput:** writes/sec sustained against a single `ReplicatedPartition`
  at three ack levels (v0.6), so the ack=0/1/all latency-vs-durability
  tradeoff has a real number instead of a plausible-sounding guess
- **Latency:** p50/p95/p99 per-write latency at each ack level
- **Recovery time:** wall-clock from leader disconnect to a new leader
  accepting writes again, measured directly (not estimated) via the same
  kill-leader mechanism already exercised in `TestV06_AckAllSurvivesLeaderCrash`

## Why in-process load generation, not a separate binary yet
The benchmark harness drives the same in-process `raft.Network` /
`replication.ReplicatedPartition` the regression tests use — there's no
real network stack yet (that's implicit until an actual gRPC/TCP layer
exists, which this project doesn't target before v1.0's document store).
Measuring the in-process path is honest about what it is: consensus +
storage overhead, not network overhead. Stated explicitly so the number
in the README can't be misread as "network-inclusive throughput."

## Chaos script requirements
`benchmarks/chaos.go` (a `go run`-able harness, not a shell script, so it
shares the same Raft/replication types as the tests instead of re-
implementing cluster setup in bash) must:
1. Start a 3-node replicated cluster
2. Run a write burst at a fixed rate
3. Mid-burst, disconnect the current leader
4. Keep writing against whichever node is reachable, retrying until a new
   leader accepts
5. Record: time-to-detect (new leader elected) and time-to-first-successful-
   write-after-kill
6. Write results to `benchmarks/results/v0.7_baseline.json`

## Smoke test, not a new correctness test
Per the versioned plan: this version doesn't add new system behavior, so
no new correctness test is required — but the benchmark code itself gets
a smoke test confirming it runs and produces valid, non-empty JSON output.
That's `benchmarks/harness_test.go`.

## Known measurement artifact, stated honestly
`ack=all` latency in `v0.7_baseline.json` is dominated by
`ReplicatedPartition.WaitApplied`'s 10ms poll interval, not pure Raft
consensus latency — the p50/p95/p99 cluster tightly around 10-20ms
because that's the poll granularity, not because that's how long
consensus actually takes. This is disclosed here rather than presented as
"quorum commit takes ~20ms" — an honest baseline is more useful later than
a flattering one. A future version could reduce this by using a
condition-variable wakeup instead of polling; not done here because it
adds complexity this benchmarking version doesn't need to prove its point
(ack levels behave differently under failure, which they measurably do).

## What v0.7 deliberately does NOT do
- No cross-machine/network benchmarking (no real network layer exists)
- No comparison against Kafka/other systems — this is a baseline for
  this project's own future versions to be compared against, not a
  competitive benchmark
