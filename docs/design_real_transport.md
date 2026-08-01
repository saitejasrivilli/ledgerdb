# Design: Real Network Transport (v0.14)

## Why this version exists
Every version through v1.0 ran Raft and replication over
`raft/network.go`'s in-process simulated `Network` — a design choice that
kept the test suite fast and deterministic, but meant every chaos/
recovery number in this repo (the 316ms/326ms figures in
`benchmarks/results/v0.7_baseline.json`) was a *process-crash* recovery
measurement, not a *network-partition* one. Those are genuinely different
claims: a process dying is detected the instant the simulated network
marks it disconnected; a real network partition means two nodes can each
still reach a majority of followers while being unable to reach each
other — the classic split-brain scenario, and the question that actually
separates "I understand Raft" from "I've implemented Raft" in an
interview. This version closes that gap: real TCP sockets, a real
partition test, and re-measured recovery numbers over the real transport.

## What changed, and what deliberately didn't
`raft.go`'s consensus logic — election, log replication, the §5.4.2
no-op-on-election fix from v0.6, everything — is unmodified. What changed
is a single seam: a `Transport` interface (`raft/transport.go`) that
`startElection` and `sendAppendEntriesTo` call instead of reaching into
`*Network` directly. `simTransport` adapts the existing `Network` to that
interface, so every test written for v0.1–v1.0 keeps passing against the
in-process simulation with zero changes — this version adds a second
`Transport` implementation, it doesn't replace the first one.

## TCPTransport
Built on Go's standard `net/rpc` over real `net.Listen("tcp", ...)`
sockets — not a hand-rolled wire protocol, since the RPC semantics needed
(call a named method on a remote peer, get a typed reply, get a real
error if the peer is unreachable) are exactly what `net/rpc` already
provides correctly. Each node runs an `net/rpc` server exposing
`RequestVote`/`AppendEntries` (thin wrappers delegating to the existing,
unmodified `*Raft` methods) and dials peers on demand, caching the
connection. A failed call (timeout, connection refused, connection
reset) drops the cached client so the next call attempts to redial —
this is what makes a real partition or a real process-kill both look the
same to the caller: RPCs to that peer simply stop succeeding, exactly
the property Raft's election-timeout mechanism already depends on.

## The partition test this version exists to add
`TestRealNetworkPartitionOnlyMajoritySideCommits`: three real TCP-backed
nodes. Whichever node is leader at partition time (call it A) gets cut
off from the other two (B, C) bidirectionally — A can't reach B or C,
and B/C can't reach A, but B and C can still reach each other. This is a
genuine network partition, not a killed process: A is still running,
still trying, just unreachable. The test asserts:
- The {B, C} side (still a majority) elects a leader and keeps
  committing writes
- A never applies the write that committed on {B, C} while still cut off

**A subtlety this test got wrong on the first pass, worth stating
explicitly:** the test originally also asserted "A must stop reporting
itself as leader once partitioned" — that's wrong, and flaked ~2 runs in
10 once discovered. A real isolated Raft leader has no way to learn it's
been cut off (nothing ever tells it, by definition of being isolated), so
it correctly keeps believing it's leader indefinitely — it just can never
get a majority to actually commit anything. Asserting the *belief* is
gone is asserting something Raft doesn't guarantee and shouldn't; the
actual correctness property — provable and tested — is that A never gets
anything **committed** while isolated, and rejoins/catches up cleanly
once the partition heals. This is the real academic reason production
systems needing strongly-consistent reads from a leader use a mechanism
like leader leases or check-quorum, not "ask the leader if it's still
leader" — this project doesn't implement that (out of scope here), and
the test now reflects the actual invariant instead of a stronger one
Raft doesn't provide.

## Re-measured recovery numbers
`benchmarks/results/v0.14_transport.json` — the same kill-leader chaos
measurement from v0.7, re-run over `TCPTransport` instead of the
simulated network (5 runs): detection ranged ~321–373ms, recovery
~325–379ms. This lands in the same ballpark as v0.7's simulated 316ms/
326ms, which makes sense once you look at what dominates the number:
detection time is bounded below by the randomized election timeout
(300–600ms, set in `raft/raft.go`), not by transport overhead — real
TCP/RPC round-trips here are single-digit milliseconds at most, so
swapping the transport barely moves a number that a much larger,
unrelated constant already dominates. Reported as its own citable
figure, not a replacement that makes the v0.7 number wrong — the two
numbers agreeing is itself informative (it says the earlier chaos number
was never really testing transport speed, just consensus timeout
behavior, regardless of transport).

## TLS, wired in for real (added same version, after initial review)
`NewTCPTransportTLS` accepts plain `*tls.Config` for both the listener
and outbound dials, built by v0.10's real cert/handshake machinery
(`security.GenerateCA`, `IssueServerCert`, `ServerTLSConfig`,
`ClientTLSConfig`) — `raft` can't import `security` directly (that would
cycle: `security` → `replication` → `raft`), so the transport takes
`*tls.Config` instead, which is exactly what those functions already
return. `TestV0_14_TLSSecuredRaftTrafficWorks` proves Raft elects and
commits over TLS; `TestV0_14_WrongCARaftPeerCannotJoin` proves a node
with a mismatched CA can never complete a handshake with the real
cluster, so it can never contribute a vote or win an election.

**A real cert bug this surfaced:** `security.IssueServerCert` (v0.10)
only set `DNSNames`, never `IPAddresses`. That's invisible when the
`ServerName` used for verification is an actual hostname (`"localhost"`,
what v0.10's own test used) — Go's TLS hostname check consults
`DNSNames` for that case. But `TCPTransport`'s tests all use
`"127.0.0.1"` as the address and `ServerName`, a literal IP — and Go's
verification consults `IPAddresses` for a literal-IP `ServerName`, not
`DNSNames`, so every handshake silently failed until this was fixed.
Fixed by setting whichever SAN field actually matches what `net.ParseIP`
says about the host. v0.10's original localhost-based test was
unaffected and still passes; this was a real gap in that version's
coverage that a wider caller (this one, using IPs) exposed.

## Addendum (found during v0.14.4's network-fault testing): per-peer connection locking
`TCPTransport.getClient` originally held a single mutex shared across
every peer for the full duration of a dial attempt. Under a real
isolation/partition, the isolated peer's connection gets dropped and
redialed on every failed heartbeat (every 50ms), and each redial held
that one shared lock for up to the full 500ms dial timeout — blocking
`getClient` calls to the OTHER, perfectly-reachable peer for that same
window, repeatedly. This alone was enough to cause real, repeated
spurious elections and leadership instability that had nothing to do
with the election-timer bug above (`docs/design_raft.md`'s addendum) —
two independent bugs producing the same symptom. Fixed with a per-peer
`peerConn` struct (its own mutex), populated once at construction so the
outer map needs no synchronization afterward; a slow dial to one peer
can no longer block traffic to any other. See
`docs/design_network_fault.md` for the full investigation and how a real
transport with real dial latency was needed to expose this — the
simulated `Network` and even this version's own `BlockPeer` (fails
instantly, no real dial delay) could not have triggered it.

## What v0.14 deliberately does NOT do
- No gRPC (net/rpc is sufficient for what this version needs to prove;
  swapping transports later is what the `Transport` interface is for)
- No real multi-machine test (loopback TCP on one machine — genuinely
  exercises real sockets/syscalls, but doesn't add real cross-host
  latency; that's a meaningfully different, larger claim this version
  doesn't make)

Tiered storage against a real MinIO instance — considered a companion
gap here, but closed separately in v0.14.3 (`storage.MinioColdStore`,
`tests/integration/minio_test.go`, verified both in CI and locally
against a real `docker run minio/minio` instance once a Docker daemon
became available).
