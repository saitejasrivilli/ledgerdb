# Design: Single-Partition Append-Only Log (v0.2)

## Scope
Storage layer alone, no Raft integration yet (that lands v0.4 when each
partition gets its own Raft group). Prove: append record, get offset back,
read any offset back byte-identical, survive crash/restart, reclaim disk
via segment deletion.

## Why segments, not one giant file
Kafka-style design: log split into segment files, each capped at
`maxSegmentBytes`. Reasons:
- Deleting old data = unlink whole segment file, no in-place rewrite
  (rewriting a multi-GB file to drop old records is the thing this avoids)
- Crash recovery scans only the active (last) segment, not entire log —
  older segments assumed durable once rolled
- Bounds any single file's size, avoids OS/filesystem large-file edge cases

## Layout on disk
```
data/
  00000000000000000000.log   # record bytes, sequential
  00000000000000000000.index # offset -> byte position, fixed-width entries
  00000000000000000512.log   # next segment, base offset 512
  00000000000000000512.index
```
Segment filename = its base offset, zero-padded — sorts lexicographically
in file listing order, matches offset order.

## Record format (per entry in .log file)
```
[4 bytes: length][length bytes: payload]
```
Index file entries are fixed-width `(relative offset uint32, position uint32)`
pairs — relative offset from segment's base offset, keeps index files small
lookups O(1) via arithmetic instead of scan.

## Append path
1. Active segment accepts write if `size + len(payload) <= maxSegmentBytes`
2. Otherwise roll: close active segment, open new one with base offset =
   next offset
3. Append length-prefixed payload to .log, append index entry to .index
4. Return assigned offset to caller

## Read path
1. Binary-search segment list for the segment whose base offset <= target
   offset < next segment's base offset
2. Within segment, index lookup gives byte position, read length prefix,
   read payload

## Crash recovery
On startup, scan `data/` directory for segment file pairs in order. For
each segment except possibly the last, trust the index file as-is (assumed
fsynced). For the last (active) segment, rebuild the index by scanning the
.log file from the start — protects against a crash between writing the
.log entry and its .index entry, which would otherwise leave the index
short one record relative to the log.

## Compaction (this version's scope: retention-based deletion)
Not log-structured merge / dedup-by-key compaction — that's a document
store concern (v1.0's MVCC layer), not this layer. Here "compaction" means
Kafka's simpler meaning: delete whole segments older than a retention
threshold (by count of segments retained, first cut — time/size-based
retention can layer on later without an interface change). Never deletes
the active segment.

## What v0.2 deliberately does NOT do
- No replication (v0.4)
- No partitioning (v0.3 — this version is a single log only)
- No compression (v0.8)
- No tiered/cold storage (v0.9)
