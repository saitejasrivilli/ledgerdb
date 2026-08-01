# Design: Tiered Storage (v0.9)

## Scope
Segments older than a configurable age/size move from local disk to an
S3-compatible object store (MinIO locally) transparently — a consumer
reading an old offset still works, fetching from cold storage instead of
local disk, without knowing the difference. Named explicitly in the MSK
JD's own linked reference material as a real thing that team ships.

## Why this sits on top of v0.2's segment boundary, not inside it
Segments (v0.2) are already the unit of deletion for retention-based
compaction — tiering reuses that exact boundary as the unit of migration
instead of invention a second one. A segment is either: active (being
written), local-cold (closed, still on local disk), or tiered (closed,
uploaded to object storage, local copy removed). Read path already
resolves an offset to a segment first (`Log.findSegment`) — tiering only
adds one more thing that lookup needs to check: is this segment local or
remote.

## Interface: a minimal object-store abstraction, not a MinIO-specific one
```go
type ColdStore interface {
    Put(key string, logBytes, indexBytes []byte) error
    Get(key string) (logBytes, indexBytes []byte, err error)
    Delete(key string) error
}
```
A `LocalDirColdStore` implementation (just another directory, standing in
for MinIO) is what the test suite runs against — this project already
established that "no cloud services required, everything runs locally"
is a hard constraint, and a real MinIO round-trip is an integration
concern more than a correctness-of-the-tiering-logic concern. The
interface is what would change to point at real MinIO later, not the
tiering logic itself.

## Migration policy
A background loop scans segments older than `tierAfter` (age) — closed
segments only, active segment is never eligible. For each eligible
segment: upload its .log and .index bytes to the ColdStore under a key
derived from the segment's base offset, confirm the upload succeeded,
then delete the local files. Order matters: never delete local data
before the remote copy is confirmed durable — a failed upload must leave
the local segment untouched, so a crash mid-migration never loses data,
only wastes a retry.

## Read path after tiering
`Log.Read(offset)` resolves to a segment as before. If the segment is
marked tiered (its local files no longer exist), fetch its bytes from
ColdStore on that read and serve the answer directly out of the fetched
bytes. This version deliberately fetches on every read rather than
caching the reconstructed segment — caching is a real future optimization
(repeated cold reads of the same segment would benefit from it) but isn't
needed to prove the correctness property this version is scoped to: that
a cold read still returns the right bytes at all. Adding a cache later is
additive, not a rework of this read path.

## What proves this correct
- **Read from cold tier after migration:** write data, force a segment to
  tier out, read an offset from that now-cold segment, confirm byte-
  identical to what was written
- **No data loss during move:** simulate an upload failure (ColdStore
  that errors on Put), confirm the segment's local files are still intact
  and still directly readable afterward — the "never delete before
  confirmed durable" invariant, tested, not just documented

## What v0.9 deliberately does NOT do
- No real MinIO wiring in this version's tests (interface makes it a
  drop-in swap later; actually exercising it would need a running MinIO
  container, which is a docker-compose/integration concern for later,
  not this correctness-focused version)
- No tiering of the active segment, ever
- No re-tiering back to local (once cold, stays cold in this scope)
