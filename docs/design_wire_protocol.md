# Design: MongoDB Wire Protocol Translation (v0.15)

## Why this exists
Amazon DocumentDB's actual mechanism, not just its marketing description,
is a MongoDB-wire-protocol-compatible frontend over a different storage
engine underneath — the JD's "DocumentDB" keyword match is this
translation layer specifically, more than MVCC alone (v1.0 already
covers MVCC). This version builds the real thing: a TCP server speaking
the real MongoDB wire protocol (message framing + BSON), translating a
deliberately small set of commands into calls against the existing,
unmodified `docstore` (v1.0).

## Scope, stated as a hard boundary up front
One collection per `docstore.ReplicatedDocStore` — no database/collection
namespacing, no sharding-by-collection. Commands supported:
`hello`/`isMaster` (handshake), `ping`, `insert`, `find` (filter by `_id`
equality or empty filter = full collection scan), `update` (`$set` only,
filter by `_id`), `delete` (filter by `_id` only). Everything else
(aggregation pipelines, indexes beyond the one hash index already in
docstore, transactions beyond docstore's existing batch mechanism,
multi-collection anything) is explicitly out of scope — this proves the
wire-protocol-translation mechanism works, not that this is a MongoDB
reimplementation.

## Why the real MongoDB wire protocol, not a look-alike
`mongowire/protocol.go` implements the actual message framing: a 16-byte
header (`messageLength`, `requestID`, `responseTo`, `opCode`), body
sections per the OP_MSG spec (kind 0 = single BSON document, kind 1 =
document sequence — this version only needs kind 0). This is real
enough that a real `mongosh`/driver's initial handshake
(`hello`/`isMaster`) can complete against it — proving the framing is
correct, not just plausible-looking.

## Why the real BSON library, not a hand-rolled encoder
`go.mongodb.org/mongo-driver/v2/bson` is a real, widely-used BSON
implementation. Reinventing BSON encode/decode would spend this
version's effort on a solved, well-tested problem instead of the actual
thing being proven: wire protocol framing + command translation. Same
principle already applied to `prometheus/client_golang` (v0.11) and
`minio-go` (v0.14.3) — compose a real library for the parts that are a
solved problem, spend the engineering effort on the part that's actually
novel here.

## Command translation, concretely
- `hello`/`isMaster` → static handshake response (`{ok:1, ismaster:true,
  maxWireVersion:17, ...}`) — enough for a real client's connection
  handshake to succeed
- `insert {documents:[...]}` → one `docstore.Mutation{Op:"put"}` per
  document (each proposed as its own transaction — batching multiple
  inserts into one Raft entry is a reasonable future optimization, not
  required here), `_id` generated if absent
- `find {filter:{...}}` → `_id`-equality filter resolves via
  `Snapshot.Get`; empty filter triggers `Snapshot.Scan()` (new method,
  added to `docstore.Snapshot` for this version — full scan was never
  needed before since docstore's own tests always know their ID or
  indexed field)
- `update {updates:[{q:{_id},u:{$set:{...}}}]}` → fetch via
  `Snapshot.Get`, merge `$set` fields, propose as a `put`
- `delete {deletes:[{q:{_id}}]}` → propose as a `delete`

## What proves this correct
`TestWireProtocol_InsertFindUpdateDelete` drives the full command cycle
using a real `go.mongodb.org/mongo-driver/v2/mongo` client dialed against
a locally running `mongowire` server backed by a real
`docstore.ReplicatedDocStore` — not a hand-rolled test client sending
raw bytes. If the real official driver's insert/find/update/delete calls
succeed and return correct data, the wire protocol implementation is
correct by the same measure a real application would judge it.

## What using the real official driver caught, that a hand-rolled test client wouldn't have
- **Legacy OP_QUERY handshake:** the real Go driver's very first message
  on a fresh connection is a legacy OP_QUERY (opcode 2004) `isMaster`
  against `$cmd`, not OP_MSG — it doesn't yet know this server supports
  OP_MSG. A server implementing only OP_MSG never gets past the first
  handshake with any real driver. Fixed by handling OP_QUERY/OP_REPLY for
  that one handshake path, OP_MSG for everything after.
- **Nested BSON sub-documents decode as `bson.D`, not `bson.M`:** a
  top-level command document unmarshaled into `bson.M` still decodes
  every *nested* document field (a filter, an update's `u` document,
  `$set`) as ordered `bson.D`, not `bson.M` — a `.(bson.M)` type
  assertion on those silently returns `nil` instead of erroring, which
  made every filter/update look empty until traced down. Fixed with a
  `toM` conversion helper applied at every nested-document access point.
- **`cursor.ns` must be `"db.collection"`, not just the collection
  name** — the driver validates the namespace format on `find` responses
  and rejects a bare collection name.
- **An empty result set must encode as an empty BSON array, not
  `null`** — a nil Go slice marshals to BSON `null`, which the driver
  rejects for `cursor.firstBatch` even when zero documents match.

None of these would have been caught testing against a hand-rolled
client that only exercises what the server-side code expects to receive
— each came from the real driver's own behavior, which is exactly why
the design doc's correctness section insists on using it.

## What v0.15 deliberately does NOT do
- No database/collection namespacing (one collection, stated above)
- No aggregation pipeline, no transactions beyond docstore's existing
  batch mechanism, no indexes beyond the one already in docstore
- No authentication (SCRAM etc.) — connections are unauthenticated,
  matching this project's existing security scope boundary (v0.10's ACL
  layer isn't wired into this frontend either, an explicit gap, not a
  silent one)
