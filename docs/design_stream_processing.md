# Design: Stream Processing Integration (v0.13)

## Scope
A real windowed aggregation job that consumes from the log (v0.2/v0.4's
storage/replication, unmodified) and performs a genuine transformation —
windowed rolling message-count-per-minute — writing results to a sink
that can be inspected. Both target JDs name Spark/Flink/Storm explicitly;
this version proves competent consumption of that kind of ecosystem tool,
not just building the log itself.

## Why an in-process Go windowing job, not a real Flink/Spark cluster
Standing up a real Flink or Spark cluster to process a toy log would be
mostly infrastructure plumbing, not stream-processing logic — and this
project's hard constraint is "no cloud services required, everything
runs locally," which a JVM-based cluster underlines rather than
satisfies for a fast, reliable test suite. What's actually being proven
here — correct windowed aggregation semantics over an ordered stream — is
implementation-agnostic, and a real Go job doing real tumbling-window
counting is more honest about what's tested than a Flink SQL job whose
correctness would mostly be Flink's own to claim credit for. The
concept (tumbling windows, watermarks, late-data handling) is the same
either way; only the runtime differs.

## Windowing model: tumbling windows by write-time
Each message read from the log carries an implicit timestamp assigned at
read-processing time (not embedded in the payload — this version doesn't
require producers to stamp their own event time, keeping the scope to
processing-time windowing, the simpler of the two valid choices). Windows
are fixed, non-overlapping (`tumbling`), one-minute wide. A window closes
(emits its count) once its end time has passed relative to the
processor's clock.

## Sink
`streaming/sink.go` — an in-memory, inspectable sink
(`map[windowStart]count`) that the test suite reads directly to assert
correctness. A file/log sink would be the production equivalent; the
interface (`Sink.Emit(windowStart time.Time, count int)`) is what a real
sink would implement, same pattern as `storage.ColdStore` in v0.9 — an
interface the test double satisfies, not the point being tested.

## Correctness proof
`TestV13_WindowedAggregationCorrectness` feeds a known, hand-constructed
sequence of message timestamps spanning several windows (including a
window with zero messages, and messages landing exactly on a window
boundary) and asserts the emitted per-window counts match hand-computed
expected values exactly — not a fuzzy/approximate check.

## What v0.13 deliberately does NOT do
- No real Flink/Spark job (stated above, explicitly, as a considered
  tradeoff, not an oversight)
- No event-time windowing / watermarks / late-data handling — processing-
  time only, for this version's scope
- No windowed joins or multi-stream aggregation — single-stream count
  only, the minimum that proves the windowing mechanism works correctly
