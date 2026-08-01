# Design: Real Packet Loss/Reordering + iptables-vs-BlockPeer Comparison

## Why this exists
v0.14's `TCPTransport.BlockPeer` simulates a partition by failing outbound
calls immediately at the application layer — documented explicitly as an
*unverified* claim that this behaves like a real network partition. A
real `iptables DROP` typically vanishes packets silently, leaving the
sender to wait out a full TCP timeout before it learns anything failed —
a materially different timing profile, and timing is exactly what the
chaos-recovery numbers measure. This version closes that gap for real:
actual `iptables`/`tc netem` rules, actual measured numbers, actual
side-by-side comparison — not an assumption anymore.

## Two separate things this proves

**1. iptables DROP vs BlockPeer — do the numbers actually differ?**
`TestRealIptablesDropVsBlockPeer` measures leader-kill detection/recovery
using a real `iptables` rule (`-j DROP` on the isolated node's port, both
directions) instead of `TCPTransport.BlockPeer`, then compares the result
directly against `benchmarks/results/v0.14_transport.json`'s BlockPeer-
based numbers. Both numbers are written to
`benchmarks/results/v0.14.4_iptables_vs_blockpeer.json` so the comparison
is citable, not asserted.

**2. Does the cluster stay correct under real, sustained packet loss and
reordering — not just a clean cut?**
`TestRealTCNetemLossAndReorder` applies `tc qdisc ... netem loss ...
reorder ...` to the loopback interface, then drives writes through the
degraded link. This is a different claim than the partition test: a
partition is a *clean* cut (some pairs get through perfectly, others get
nothing); netem loss/reorder is *messy* — every packet has a chance of
being dropped or delivered out of order, on every path, all the time.
TCP's own retransmission and resequencing already absorb reordering at
the byte-stream level (the application/RPC layer literally cannot see
reordering TCP already fixed) — what this test actually exercises is
whether Raft still elects and commits correctly when a meaningful
fraction of packets never arrive the first time, forcing real
retransmits and real timeouts under `net/rpc`'s own request/response
model, not the simulated `Network`'s statistical delay/drop from v0.1.

## Why this can't run on this project's default dev machine
`iptables` and `tc` are Linux kernel networking tools — unavailable on
macOS. Both tests require root/`NET_ADMIN` and are skipped by default
(guarded by the `NETFAULT_TEST=1` environment variable and a Linux-only
build check) so `go test ./...` stays fully self-contained everywhere
else, same pattern as `MINIO_ENDPOINT` gating the real-MinIO test.
Verified two ways: a GitHub Actions job running natively on an
`ubuntu-latest` runner (which is a real Linux VM with root, not a
container — `iptables`/`tc` work directly, no privileged Docker needed
there), and locally via a privileged Linux Docker container
(`scripts/run_network_fault_tests.sh`) on this Mac.

## What "correct" means for netem test, concretely
Not "fast" — degraded network is allowed to be slow. The invariant is:
every write that the test's own retry loop eventually gets a success
response for is later readable at the offset it claims, and no write is
silently lost or duplicated because of the packet loss/reordering. Writes
that time out are retried by the test harness (this is what a real
client would do too) — the test fails only if data is ever wrong, not
merely if it's slow.

## What this version deliberately does NOT do
- No cross-host test (loopback only, same as v0.14)
- No sustained soak test (runs for seconds, not hours) — this proves the
  mechanism, not long-run stability under fault injection

## Real result: iptables DROP vs. BlockPeer (the question this exists to answer)
Measured, 10 runs: real `iptables DROP` detection ~346–458ms, recovery
~358–469ms. `BlockPeer`'s numbers from `v0.14_transport.json`: ~321–373ms
detection, ~325–379ms recovery. **The two are in the same range** —
close enough that the theoretical DROP-vs-REJECT timing concern this
version set out to check turns out not to matter much in practice here,
because detection time is dominated by Raft's own election-timeout
window (300–600ms), not by how the underlying transport fails. That's a
real, checked answer now, not an assumption either way.

## Two real bugs found and fixed getting to that answer
Getting a clean, real measurement here required finding and fixing two
genuine bugs that the simulated-network tests (v0.1–v1.0) and even
v0.14's `BlockPeer`-based partition test never surfaced — both are worth
recording plainly, since "the test kept failing until these were fixed"
is more informative than a clean first-try number would have been.

**1. `electionTicker` redrew its random timeout on every 10ms poll tick**
(`raft/raft.go`), instead of once per reset. Once elapsed time exceeds
the timeout range's upper bound (600ms), every subsequent redraw is
guaranteed to be `<=` elapsed, so the "random" timeout collapses to a
near-deterministic ceiling. Two nodes reset around the same moment (e.g.
by mutually processing each other's RequestVotes) then converge on
firing within the same 10ms window repeatedly — a *persistent* split
vote, not the rare one-off randomization is supposed to make unlikely.
Observed directly: two survivor nodes climbing terms in lockstep (3, 3 →
6, 6 → 9, 8 → … → 39, 38) for 14+ seconds without resolving. Fixed by
drawing the random deadline once, at reset time, and comparing against
it directly (`electionDeadline time.Time` instead of re-deriving a
timeout duration every tick).

**2. `TCPTransport.getClient` held ONE mutex shared across every peer**
for the full duration of a dial attempt (`raft/tcp_transport.go`). Under
real isolation, the isolated peer's connection gets dropped and redialed
on every failed heartbeat — every 50ms — and each redial held that
single shared lock for up to the full 500ms dial timeout, blocking
`getClient` calls to the OTHER, perfectly-reachable peer for that same
window, repeatedly. This starved heartbeats and vote RPCs to the
reachable peer and produced real, repeated spurious elections and
leadership instability — a second, independent way to get the same
symptom as bug #1, only exposed once a real (slow-to-fail) dial was in
the picture, which the simulated `Network` and even `BlockPeer` (fails
instantly, no dial delay) never exercised. Fixed with a per-peer
`peerConn` (its own mutex), populated once at construction so the outer
map itself never needs synchronization — a slow dial to one peer can no
longer block traffic to any other.

Both bugs were pre-existing since much earlier versions (`raft.go`'s
since v0.1, `tcp_transport.go`'s since v0.14) and were invisible to every
prior test because: the simulated `Network` calls are synchronous
in-process function calls with no real dial latency (bug #2 can't
manifest without a slow dial), and `BlockPeer`'s instant-fail design
means a blocked peer never actually holds a lock waiting on a real OS
timeout (also can't trigger bug #2), while bug #1 needs enough real
concurrent timing pressure to make two nodes' resets land close enough
together often enough to notice, which loopback TCP's very low, very
consistent latency apparently provides far more reliably than the
in-process simulation's essentially-zero, less structured scheduling
noise did. Real infrastructure surfaced real bugs that a good-enough
simulation had been quietly hiding.
