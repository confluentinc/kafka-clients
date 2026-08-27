# Critic 43 — Phase M3 (FetchMetricsManager) — RESOLVED

Both issues from the review of `b06b0dc` fixed. Fixups committed referencing `b06b0dc`.

---

## Issue 1 (RESOLVED): `maybe_update_assignment` eagerly cloned the assigned-partition set on every fetch-prepare

- **File**: `src/consumer/internals/fetch_metrics_manager.rs` `maybe_update_assignment`
- **Java reference**: `FetchMetricsManager.java:164-198`
- **Fix**: Reordered to mirror Java's lazy ordering. We now read only the cheap
  `assignment_id()` first, take the assignment-tracking guard, compare, and
  early-return on an unchanged id BEFORE acquiring `assigned_partitions()` (the
  allocating `HashSet<TopicPartition>` clone). `assigned_partitions()` is now
  read only inside the changed-assignment branch.
- **Confirmation**: A steady-state poll with an unchanged assignment now does
  ZERO `TopicPartition` clones (one cheap `assignment_id()` lock-read + compare,
  then return) — back to pre-M3 behavior. Functional behavior on an actual
  assignment change (sensor cleanup + preferred-replica gauge registration) is
  unchanged and still Java-faithful. `test_maybe_update_assignment` and
  `test_maybe_update_assignment_with_additional_registered_metrics` pass.

---

## Issue 2 (RESOLVED): records-lag-max / records-lead-min recording level — now matches Java

User decision: MATCH JAVA. The client-level `records-lag-max` / `records-lead-min`
are now INFO (on by default), exactly as Java registers them; only the DETAILED
per-partition lag/lead sensors stay DEBUG (the kept, documented perf deviation).

- **Files**: `fetch_metrics_manager.rs` (ctor sensor levels, `record_partition_lag/lead`,
  `should_record_partition_metrics`), `fetch_collector.rs` (per-partition record path).
- **Fix**:
  - Client-level `records-lag` / `records-lead` sensors built at `RecordingLevel::Info`
    (matching Java's `SensorBuilder` → `metrics.sensor(name)` default).
  - `record_partition_lag` / `record_partition_lead` now record the client-level
    INFO max/min sensor UNCONDITIONALLY (as Java does); only the per-partition
    DETAIL sensor registration/recording is gated behind
    `should_record_partition_metrics()`.
  - `should_record_partition_metrics()` re-defined to check the DEBUG level
    against the metrics config (`RecordingLevel::Debug.should_record(config_level)`),
    NOT the (now-INFO) client-level sensor — otherwise it would open the detail
    path at INFO. Made private (`fn`), used only inside the manager.
  - `fetch_collector.rs` no longer block-skips the lag/lead computation at INFO;
    it now computes `partition_lag`/`partition_lead` and calls the record methods
    unconditionally, mirroring Java `FetchCollector` (`recordPartitionLag/Lead`).
    This is per-partition-per-poll, NOT per-record (it lives in `fetch_records`
    per-partition, outside the per-record loop — confirmed by the unchanged
    per-record allocation-budget test).

### Per-sensor recording-level table (Java vs Rust — now matching)

| Sensor                                   | Java level | Rust level (after fix) | Match |
|------------------------------------------|------------|------------------------|-------|
| fetch-throttle-time                      | INFO       | INFO                   | yes   |
| bytes-fetched (client)                   | INFO       | INFO                   | yes   |
| records-fetched (client)                 | INFO       | INFO                   | yes   |
| fetch-latency                            | INFO       | INFO                   | yes   |
| records-lag (client → records-lag-max)   | INFO       | INFO                   | yes   |
| records-lead (client → records-lead-min) | INFO       | INFO                   | yes   |
| per-topic bytes/records-fetched          | INFO       | INFO                   | yes   |
| per-partition records-lag/-avg/-max      | INFO       | DEBUG (kept deviation) | dev   |
| per-partition records-lead/-min/-avg     | INFO       | DEBUG (kept deviation) | dev   |
| partition preferred-read-replica gauge   | n/a (gauge, registered on assignment) | same | yes |

Net effect: a default (INFO) consumer now exposes `records-lag-max` /
`records-lead-min` (the consumer-lag metric) exactly as Java, but NOT the
per-partition detail (kept perf deviation, documented in the ctor doc).

- **Test changes**: The old `test_partition_metrics_not_recorded_at_info`
  (asserted ZERO partition metrics at INFO) was corrected to Java's actual
  behavior and renamed `test_partition_metrics_recording_level`. It now pins:
  (a) `records-lag-max` = 14 and `records-lead-min` = 11 ARE recorded at INFO;
  (b) NO per-partition detail metrics register at INFO (count unchanged);
  (c) at DEBUG the per-partition detail registers (6 metrics: lag value/max/avg
  + lead value/min/avg). `FetchMetricsManagerTest` value-parity intact
  (`test_partition_lag` / `test_partition_lead` unchanged, still pass at DEBUG).

---

## Verification

- `cargo build`: clean.
- `cargo test --lib`: 2079 passed, 0 failed.
- `cargo xtask lint`: no issues.
- `cargo xtask format-check`: clean.
- §27 per-record allocation-budget test `test_collect_fetch_per_record_allocation_budget`
  UNCHANGED and passing.
- `test_handle_fetch_success_does_not_copy_payload` UNCHANGED and passing
  (still guards against a byte-copy).

---

## Final decision (user, 2026-06-22): FULL Java parity — all fetch sensors INFO, no DEBUG gating

The prior fixup kept the per-partition lag/lead DETAIL sensors at DEBUG behind a
`should_record_partition_metrics()` gate as a perf deviation. The user verified
Java has NO such gating: `SensorBuilder` (SensorBuilder.java:61) creates EVERY
`FetchMetricsManager` sensor via `metrics.sensor(name)`, which defaults to
`RecordingLevel.INFO` — including the per-partition `records-lag` /
`records-lead` detail sensors (FetchMetricsManager.java:133,148). Decision:
match Java EXACTLY, do NOT DEBUG-gate.

Changes made:

1. **Per-partition lag/lead detail sensors → INFO.** `record_partition_lag` /
   `record_partition_lead` now build their `{tp}.records-lag(.max/.avg)` and
   `{tp}.records-lead(.min/.avg)` sensors at `RecordingLevel::Info` (were
   `Debug`). The deprecated-metric variants
   (`maybe_record_deprecated_partition_lag/lead`) were also corrected to INFO —
   Java builds those via `new SensorBuilder(...)` too (default INFO).

2. **`should_record_partition_metrics()` gate REMOVED entirely.** Both
   `record_partition_lag` and `record_partition_lead` now register + record the
   per-partition detail unconditionally, exactly like Java. The helper method is
   deleted.

3. **Issue-1 lazy-clone fix PRESERVED.** `maybe_update_assignment` still reads
   the cheap `assignment_id()` first and clones `assigned_partitions()` ONLY on
   an assignment change — untouched, a real perf fix unrelated to recording
   levels.

4. **All fetch sensors now INFO — per-sensor table:**

   | Sensor (name)                              | Level | Source |
   |--------------------------------------------|-------|--------|
   | `fetch-throttle-time`                      | INFO  | ctor   |
   | `bytes-fetched`                            | INFO  | ctor   |
   | `records-fetched`                          | INFO  | ctor   |
   | `fetch-latency`                            | INFO  | ctor   |
   | `records-lag` (client `records-lag-max`)   | INFO  | ctor   |
   | `records-lead` (client `records-lead-min`) | INFO  | ctor   |
   | `{topic}.bytes-fetched` (+ deprecated)     | INFO  | record |
   | `{topic}.records-fetched` (+ deprecated)   | INFO  | record |
   | `{tp}.records-lag` (+ deprecated)          | INFO  | record |
   | `{tp}.records-lead` (+ deprecated)         | INFO  | record |

   This is exactly Java: every sensor INFO, no DEBUG gating.

5. **Per-record path + budget test UNTOUCHED.** Per-partition lag/lead recording
   stays in `FetchCollector::fetch_records_from_partition` (once per partition
   per poll, NOT in the per-record loop). `completed_fetch.rs` per-record loop is
   pure `i32` counters, no `Sensor.record()`. The §27 budget test
   `test_collect_fetch_per_record_allocation_budget` is UNCHANGED and passes.

6. **Test changes (full Java parity):**
   - `test_partition_lag` / `test_partition_lead`: now run at default INFO
     (`setup()`) instead of DEBUG — matching the Java tests exactly. Metric
     values and registration counts unchanged.
   - `test_maybe_update_assignment_with_additional_registered_metrics`: now runs
     at default INFO. Counts unchanged.
   - `test_partition_metrics_recording_level`: the old assertion "per-partition
     detail ABSENT at INFO" was the wrong (deviation) behavior. Corrected to
     assert the per-partition detail IS present at INFO: recording lag/lead for
     one non-deprecated partition registers exactly 6 new metrics
     (lag value/max/avg + lead value/min/avg) AND the per-partition values are
     readable (lag=14, lead=11). No longer references the deleted gate helper.
   - `setup_with_level` removed (no test varies the level anymore); `setup()`
     builds the INFO fixture directly.
   - `FetchMetricsManagerTest` value-parity intact across all tests.

7. **Accepted cost:** a default (INFO) consumer now records the full
   per-partition metric set per partition per poll — the Java-parity cost, to be
   measured in M8.

Verification (all green): `cargo build`, `cargo test --lib` (2079 passed),
`cargo xtask lint` (no issues), `cargo xtask format-check` (clean).
