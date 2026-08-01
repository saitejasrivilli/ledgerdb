# Design: Batching + Compression (v0.8)

## Scope
Producer-side batching (flush every N messages or T ms, whichever first)
and compression (gzip) applied to the batch before it's proposed as a
single Raft entry. Directly named in the target JD material: "batch,
compress, and encrypt the data before loading it" (Firehose-facing).

## Why batch before compress, and both before Raft
Compressing one message at a time has terrible ratio (gzip needs a
window of similar bytes to find redundancy in) — batching first, then
compressing the whole batch as one blob, is why this pairs as a single
feature instead of two independent ones. The batch (as one compressed
blob) becomes a single Raft log entry — this means a batch commits or
doesn't as one atomic unit relative to Raft, not partially, which
simplifies the failure story: no such thing as "half a batch committed."

## Batch flush triggers (first to fire wins)
- **Count-based:** batch reaches `maxBatchSize` messages
- **Time-based:** `maxBatchLinger` elapses since the first message was
  added to the current (non-empty) batch

Both triggers exist because count-alone stalls under low traffic (a
half-full batch could wait forever), and time-alone wastes compression
efficiency under bursty traffic if it flushes too eagerly.

## Wire format of a batched entry
```
[4 bytes: message count][for each message: 4 bytes length + payload bytes]
```
gzip-compressed as one blob before being handed to `producer.Write`. On
the consuming side (the apply loop in `replication.ReplicatedPartition`),
the blob is gunzipped and unpacked back into individual messages before
each is appended to `storage.Log` — consumers still see individual
messages at individual offsets, batching is invisible above the producer/
apply-loop boundary.

## Correctness requirement: order preserved within and across batches
Messages within one batch must decode back out in the same order they
were added. Batches themselves commit in Raft order, so cross-batch
ordering is already guaranteed by v0.1's unmodified consensus core —
this version only has to prove it doesn't break that guarantee by
introducing its own internal reordering bug.

## What v0.8 deliberately does NOT do
- No per-message compression (batch-level only, per the ratio argument
  above)
- No configurable compression codec selection yet (gzip only; Snappy/lz4
  swap would be a drop-in codec-interface change later, not attempted
  now since nothing in this version's scope needs it)
- No encryption (that's TLS in v0.10 for in-transit; at-rest encryption
  isn't in the versioned plan at all — noting the gap rather than
  quietly not doing it)
