---
name: review-m9-m3-fetch-metrics
description: M3 FetchMetricsManager review — eager-clone-before-early-return regression; Java SensorBuilder default level is INFO not DEBUG; perf-gate audit heuristics
metadata:
  type: project
---

M3 (commit b06b0dc) FetchMetricsRegistry/Manager/Aggregator/SensorBuilder wired into fetch path.

**Two real findings:**

1. **Eager-clone-before-early-return (perf, §11).** `maybe_update_assignment` (called first in `prepare_fetch_requests`, bg task, EVERY poll cycle) computed `(assignment_id(), assigned_partitions())` into ONE tuple *before* the `if id unchanged { return }` check. Java (`FetchMetricsManager.java:164-169`) reads only `assignmentId()` first and calls `assignedPartitions()` ONLY inside the changed-id `if`. `assigned_partitions()` = fresh `HashSet<TopicPartition>` with a String clone per partition → N allocs/poll-cycle in steady state, Java does 0. **Heuristic: whenever Java guards an allocating call behind an early-return/`if`, check the Rust tuple-destructure didn't hoist the alloc above the guard.** This is independent of recording level — default consumers pay it.

2. **Java SensorBuilder default RecordingLevel = INFO, not DEBUG.** `SensorBuilder.java` always calls `metrics.sensor(name)` → `Metrics.sensor(String)` → `RecordingLevel.INFO` (`Metrics.java:325`). So Java records-lag/records-lead (client + per-partition) are INFO. Actor gated them to DEBUG for perf → default Rust consumer is MISSING `records-lag-max`/`records-lead-min`/per-partition lag-lead. Permitted-with-rationale deviation, but flag for conscious user sign-off (commonly-alerted metrics go empty). **When reviewing a recording-level decision, always trace Java's actual default — don't assume "looks like a debug metric" means Java made it DEBUG.**

**Verified CLEAN (perf-gate audit heuristics that paid off):**
- Per-record loop pure i32 (no Sensor.record/alloc); aggregator records once per partition in drain(), once per fetch after last partition.
- Per-fetch budget +1/partition = aggregator's tracked-set TopicPartition clone (Java does identical `new HashSet<>(responseData.keySet())`). Budget test scales with partition COUNT not payload SIZE → still guards byte copy.
- DEBUG gate checked BEFORE per-partition lock/compute (collector) and before sensor registration (`record_partition_lag/lead` short-circuit on `!should_record()`). `test_partition_metrics_not_recorded_at_info` proves 0 registrations at INFO.
- Single-writer "race": aggregator fetch/topic sensors written from app/poll task (collect_fetch→drain), latency/assignment from bg task — same split as Java AsyncKafkaConsumer; disjoint sensors; `Metrics` is internally Mutex-sync'd. NOT a race.

**Poison-test reorder verdict pattern:** old test poisoned SubscriptionState mutex to prove short-circuit skipped the lock. Reorder (maybeUpdateAssignment first) made that premise invalid — Java DOES read SubscriptionState first. Rename + assert functional short-circuit was honest; no invariant lost (expensive fetchable scan still skipped). But the reorder is what surfaced finding #1 — faithful to Java's *call* but not Java's *cost*.
