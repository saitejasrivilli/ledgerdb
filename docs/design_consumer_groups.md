# Design: Consumer Groups + Rebalancing (v0.5)

## Scope
Multiple consumers coordinate to split ownership of a partition set
(built on v0.3's `PartitionedLog`). Automatic rebalancing triggers on
consumer join, leave, or crash — detected via heartbeat, same mechanism
Raft already uses at the replication layer (v0.1), applied here at the
consumer-coordination layer instead.

## Why centralized group coordination, not per-consumer negotiation
A single `GroupCoordinator` per consumer group assigns partitions —
consumers don't negotiate with each other directly. Mirrors the tradeoff
already named honestly in `DESIGN_DECISIONS.md` for heartbeat-based
failure detection: simpler to reason about and implement correctly than
a distributed negotiation protocol, at the cost of the coordinator being
a single point of failure for *that group's* rebalancing (not for the
partitions' data — that's still Raft-replicated independently).

## Assignment strategy
Round-robin over sorted partition IDs across sorted, currently-alive
consumer IDs — deterministic given the same membership set, so two
coordinators (or the same one recomputing) produce the same assignment
without needing to diff against the previous one. Simplicity chosen over
"sticky" assignment (minimizing partition movement on membership change)
since sticky assignment is an optimization this scope doesn't need yet.

## Heartbeat + failure detection
Each consumer sends a heartbeat to the coordinator on a fixed interval.
Missing N consecutive heartbeats marks the consumer dead — same
threshold-based pattern as v0.1's Raft health checks, reused
deliberately rather than inventing a second failure-detection mechanism.

## Rebalance triggers
- Consumer joins (registers with the coordinator) — triggers immediate
  recompute
- Consumer's heartbeat deadline expires — triggers immediate recompute,
  its partitions redistributed among remaining live consumers
- Consumer explicitly leaves (clean shutdown) — same as crash path, just
  without waiting out the heartbeat timeout first

## No-starvation guarantee
Round-robin over sorted IDs means every live consumer gets
`floor(P/C)` or `ceil(P/C)` partitions (P partitions, C consumers) — no
consumer is ever assigned zero partitions while another holds more than
one extra, as long as C <= P. If C > P, some consumers legitimately get
zero — stated explicitly rather than treated as a bug.

## What v0.5 deliberately does NOT do
- No offset commit/tracking for consumers yet (that's implicit in this
  version — consumers own tracking their own read position; a managed
  "committed offset" API is a reasonable future addition, not required
  by this version's definition of done)
- No cooperative/incremental rebalancing (stop-the-world reassignment
  only, acceptable at this scale)
