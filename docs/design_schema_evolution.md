# Design: Schema Validation (v0.12)

## Scope
Schema support on writes, rejecting a write that breaks backward
compatibility with the currently-registered schema for a partition. JSON
Schema chosen over Avro/Protobuf for this version — reasoning below —
enforced at the same request-handling boundary v0.10's ACL check already
established (`security.CheckedWrite`), not inside `producer.Write` itself.

## Why JSON Schema, not Avro/Protobuf, for this version
Avro and Protobuf both require a schema registry + binary encoding layer
that this project doesn't have yet (no wire-format binary encoding scheme
exists here beyond the batch.Encode blob format from v0.8, which isn't a
schema system). Building a real Avro/Protobuf compatibility checker
correctly is a substantial project on its own; JSON Schema gives the same
compatibility-checking property (does a schema change break existing
readers) using Go's stdlib-adjacent tooling, without a detour into
building a binary serialization framework this project's scope doesn't
call for elsewhere. The compatibility *rules* enforced here (below) are
the same ones Avro/Protobuf registries enforce — only the schema
representation differs.

## What "backward compatible" means here, concretely
A new schema is backward-compatible with the current one for a partition
if every document valid under the *old* schema is still valid under the
*new* one — i.e. existing readers built against the old schema keep
working against data written under the new schema. Concretely, a schema
change is REJECTED if it:
- Removes a required field
- Narrows a field's allowed type (e.g. `string` → `integer`)
- Adds a new required field with no default (old documents wouldn't have it)

A schema change is ACCEPTED if it:
- Adds a new optional field
- Widens a field's type (e.g. `integer` → `number`)
- Relaxes a required field to optional

## Enforcement point
`schema.Registry` holds one current schema per partition. `schema.CheckedWrite`
validates the payload against the partition's registered schema before
calling `security.CheckedWrite` — schema rejection happens before the ACL
check's Raft proposal, so an invalid write never reaches consensus at all
(fail fast, don't spend a Raft round trip on data that was never going to
be accepted).

## What proves this correct
- `compatible-schema-change-accepted`: register schema v1, write valid
  data under it; register schema v2 that only adds an optional field;
  confirm v2 write succeeds and old v1-shaped data is still considered
  valid under v2
- `breaking-schema-change-rejected`: register schema v1; attempt to
  register a v2 that removes a required field v1 had; confirm the
  registry rejects the v2 registration itself (compatibility is checked
  at schema-registration time, not deferred to individual writes —
  catching the break early is strictly better than catching it per-write)

## What v0.12 deliberately does NOT do
- No Avro/Protobuf binary schema support (stated above, explicitly)
- No per-message schema versioning/multi-schema-per-partition — one
  current schema per partition, replaced only by a compatible successor
