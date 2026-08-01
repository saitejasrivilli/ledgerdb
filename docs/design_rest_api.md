# Design: REST API (v0.16)

## Scope
A thin HTTP layer over the existing `docstore` (v1.0) — `PUT`/`GET`/
`DELETE` for individual documents, plus a query-by-indexed-field list
endpoint. No new storage/consensus logic; this version's job is
translation (HTTP request → `docstore.Mutation`/`Snapshot` call →
HTTP response), same composition pattern as `mongowire` (v0.15) applied
to a different wire format.

## Endpoints
```
PUT    /docs/{id}         body: JSON document        → upsert
GET    /docs/{id}                                    → 200 + JSON, or 404
DELETE /docs/{id}                                    → 204, or 404
GET    /docs?field=value                              → query by the
                                                         one indexed
                                                         field docstore
                                                         supports
GET    /docs                                          → full scan
```

## Why synchronous request/response, matching ack=all semantics
Every write blocks until `WaitApplied` confirms the entry committed
(the same guarantee `producer.AckAll`, v0.6, already proved) — an HTTP
`200`/`204` response means the write is durable and visible to the next
read, not just accepted locally. A REST API that returned success before
consensus committed would be a silent regression from every ack-level
guarantee this project already built and tested; this version doesn't
reintroduce that gap for the sake of a faster HTTP response.

## Error mapping
- Document not found → `404`
- Not leader (write attempted against a follower) → `503` with a
  `Retry-After` style message (no leader-forwarding, same boundary
  already stated for `producer.Write` since v0.6 — a client retries
  against another node)
- Malformed JSON body → `400`
- Write proposed but never committed within timeout → `504`

## What proves this correct
`TestRESTAPI_CRUDRoundTrip` drives PUT/GET/DELETE/query through real
`net/http` requests (via `httptest.NewServer`, a real listening HTTP
server, not `httptest.NewRecorder`'s in-process request/response
shortcut) against a real 3-node replicated cluster, confirming each
operation's HTTP status code and body match what actually happened in
the underlying store.

## What v0.16 deliberately does NOT do
- No pagination on the full-scan/query-by-field endpoints (fine at this
  project's data scale; a real production API would need it)
- No authentication wired in (v0.10's ACL layer exists but isn't
  connected to this HTTP layer — same explicit gap already stated for
  `mongowire`)
- No OpenAPI/Swagger spec generation
