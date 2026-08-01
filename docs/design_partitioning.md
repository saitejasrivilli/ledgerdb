# Design: Partitioning (v0.3)

## Scope
Split the log into N independent partitions, each with its own offset
space and its own directory of segments (reuses `storage.Log` from v0.2
unchanged — one instance per partition). No replication yet (v0.4 puts a
Raft group behind each partition); this version only proves isolation and
routing.

## Why partition at all
Single log = single append point = throughput ceiling of one disk/CPU.
Partitioning lets independent keys write/read in parallel with zero
coordination between partitions. Tradeoff stated honestly: ordering is
only guaranteed within a partition, never across partitions — a consumer
wanting global order across keys has no way to get it here (matches
Kafka's own model, not a gap unique to this project).

## Partition assignment
`partition(key) = hash(key) % numPartitions`, FNV-1a hash — deterministic,
no external dependency, good-enough distribution for this scope (not
claiming cryptographic properties, just avoiding clustering on typical
string keys).

## Directory layout
```
data/
  partition-0/
    00000000000000000000.log
    00000000000000000000.index
  partition-1/
    ...
```
Each partition directory is exactly what `storage.Open` already expects —
partitioning is a routing layer on top, not a change to segment format.

## Isolation guarantee (this version's core correctness claim)
A write to partition A must never be readable from partition B, and a
partition's offset space starts at 0 independent of any other partition's
offset count. Both are structural (separate `storage.Log` instances, no
shared state) rather than enforced by a runtime check — verified by test,
not by assertion in the hot path.

## What v0.3 deliberately does NOT do
- No replication — a partition lives on exactly one process for now (v0.4)
- No rebalancing / dynamic repartitioning — partition count fixed at
  cluster creation for this scope
- No cross-partition transactions (that's v1.0's document store concern,
  and even there scoped to a single partition's log as WAL)
