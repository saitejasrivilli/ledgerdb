# Design: Security — TLS + ACLs (v0.10)

## Scope
This project has no real network transport yet (Raft and replication run
over an in-process simulated `Network`, deliberately — see
`docs/design_benchmarking.md`'s honesty note about what's actually being
measured). So "TLS between brokers and clients" can't be wired into a
socket layer that doesn't exist. What this version can do, and does
correctly rather than faking: build the two pieces that are transport-
independent — a real TLS certificate/handshake helper usable the moment a
real listener exists, and a request-layer ACL enforcement point that
works today against the in-process client identity concept.

## TLS: what's real vs. what's deferred
Real, tested now:
- Certificate generation helper (`security/tls.go`) — self-signed CA +
  server cert, real `crypto/tls` config construction
- A round-trip test: stand up a real `net/tcp` + `tls` listener/dial pair
  using the generated certs, confirm a client without the CA cert fails
  the handshake, a client with it succeeds

Deferred, explicitly, not silently:
- Wiring this into the actual Raft peer-to-peer RPC path — that requires
  replacing the in-process `raft.Network` with a real socket transport,
  which is out of scope for every version up to this one and isn't being
  retrofitted here just to check a TLS box. Doing so honestly needs its
  own version-shaped effort, not a rushed add-on.

## ACLs: enforced at the request-handling layer, now
Unlike TLS, ACL enforcement doesn't need a real socket to be meaningful —
it's a decision made once a request is already being handled, regardless
of transport. `security.ACL` maps a client identity string to a set of
allowed (partition, permission) pairs. Enforcement point: a thin
`producer.Write` wrapper (`security.CheckedWrite`) rejects before
`Propose` is ever called if the identity isn't authorized — the same
place a real gRPC interceptor would sit later.

## Permission model
Two permissions: `Read`, `Write`, checked per partition index. An
identity with no explicit grant for a partition is denied by default
(deny-by-default, not allow-by-default) — stated because the opposite
default is the more common accidental security bug in systems that bolt
ACLs on late.

## What proves this correct
- `unauthorized-client-rejected`: identity with no grant for partition 0
  gets rejected on both read and write attempts
- `TLS-handshake-required`: a bare TCP dial without the CA cert fails to
  complete a TLS handshake against the test listener
- `authorized-client-still-works`: an identity with an explicit grant
  succeeds, and a bare TLS handshake with the correct CA cert succeeds

## What v0.10 deliberately does NOT do
- No wiring into the live Raft/replication RPC path (stated above,
  explicitly, as a real gap — not implied to be done)
- No encryption at rest (same gap already noted honestly in v0.8)
- No dynamic ACL updates via an API — ACLs are constructed programmatically
  for this scope, a management API is a reasonable later addition
