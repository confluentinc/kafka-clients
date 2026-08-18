---
name: review-m9-m2-windowed-stats
description: M9 Phase M2 (windowed/sampled metrics stats) value-parity review — verification heuristics and clean result
metadata:
  type: project
---

Milestone-9 Phase M2 (SampledStat, Rate/SimpleRate, Avg/Max/Min, Meter, WindowedSum/Count,
Sensor::add_compound) reviewed clean — 0 issues, value-parity PASS. Comments in
design/history/Milestone-9-metrics/Phase-M2-windowed-stats/COMMENTS.42.md.

**Why:** This phase is value-parity-critical (every Avg/Rate value must equal Java's). The
Actor's HYBRID port was faithful enough that the only output was 4 non-blocking observations.

**How to apply (verification heuristics that paid off — reuse for M3+ metrics phases):**
- Don't trust "tests pass". Hand-trace `testTimeWindowing` and `testSampleIsKeptIfOverlapsAndExtra`
  event-by-event through current/advance/is_complete/purge. The ring is `max_samples = samples+1`,
  wrap `(current+1)%max_samples`, purge resets sample when `now-lastEventMs >= samples*windowMs`.
  Both expected literals (2.0, 3.0) are Java-correct.
- Rate.windowSize: confirm numFullWindows/minFullWindows correction, per-field timeWindow
  selection, `(int)` cast, and `max(elapsed,1)`. The Actor matched Rate.java line-for-line.
- Meter.stats() order is [total, rate] (Java Arrays.asList(total, rate)) — verify order, not just count.
- testRateWindowing windowSize==75s downcast is NOT ported (no Any seam on MetricValueProvider);
  covered transitively by rate=sum/elapsedSecs assertion + direct rate.rs window_size tests. Sound.

**Non-bug observations worth knowing for future metrics review:**
- Each SampledStat has its OWN std::Mutex, so Rate::measure takes the lock 3x (measure + purge +
  oldest) — NOT atomic vs a concurrent record like Java's shared metricLock. Safe ONLY under the
  documented single-writer (bg-task per-fetch) usage; would tear if recorded from multiple tasks.
  Do NOT flag as a bug under current usage, but re-raise if M3 wires multi-task recording.
- getTags / MetricConfig.tags use BTreeMap (sorted) not Java LinkedHashMap (insertion order);
  no value impact because tags are an unordered map for MetricName equality. Don't flag.
- WindowedCount delegates to a free `WindowedSumKind.update(...,1.0,...)` to emulate Java
  `super.update(...,1.0,...)`; is_windowed_sum()=true AND is_windowed_count()=true (extends WindowedSum).
- Meter rejects non-WindowedSum rate stat via assert!/panic (ctor-time programming error) — matches
  Java IllegalArgumentException; acceptable per CLAUDE.md §10.1.
