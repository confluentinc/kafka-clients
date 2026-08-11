---
name: phaseM3-fetch-metrics
description: Milestone-9 Phase M3 fetch metrics — FetchMetricsRegistry/Manager/Aggregator/SensorBuilder; FINAL: all sensors INFO full Java parity NO DEBUG gating (commit b02e070); Arc-shared manager + interior-mutable assignment, per-fetch-not-per-record wiring, Metrics ownership for M7
metadata:
  type: project
---

Phase M3 (Actor 43) translated the consumer fetch-metrics layer and wired it into
the fetch path. Commit `b06b0dc` on `consumer-impl`. Builds on M1 (metrics core) +
M2 (windowed stats). Plan + this commit's PLAN.md:
`design/history/Milestone-9-metrics/Phase-M3-fetch-metrics/PLAN.md`.

**Why M3 is the perf-critical phase:** it touches the tuned fetch path. Hard user
constraint: per-record path stays pure atomic-counter accumulation; Sensor.record()
fires per-fetch / per-partition only; DEBUG sensors gated OFF at default INFO.

**Files (Java clients/consumer/internals -> consumer::internals, all pub(crate)):**
- `fetch_metrics_registry.rs` — 29 MetricNameTemplates (client/topic/partition levels).
- `sensor_builder.rs` — SensorBuilder (Java helper). Holds `Arc<Metrics>` NOT `&Metrics`
  (the `&'a Metrics` lifetime made `.and_then(|b| ...)` closure inference fail with E0282;
  owning the Arc removes the lifetime). `withXxx` take `self`, return `Result<Self>`.
- `fetch_metrics_manager.rs` — FetchMetricsManager + the FetchMetricsManagerTest (14 tests).
- `fetch_metrics_aggregator.rs` — per-response aggregator.

**Reusable patterns:**

- **Arc-shared manager + interior-mutable assignment.** The manager is `Arc<FetchMetricsManager>`
  (per-response aggregators hold clones), so `maybe_update_assignment` CANNOT take `&mut self`.
  Java's `assignmentId`/`assignedPartitions` fields → `Mutex<AssignmentTracking>` (cheap per-poll
  guard, never per-record, never across `.await`). `maybe_update_assignment(&self)`.
- **Aggregator threading deviation (documented).** Java single-thread: `CompletedFetch.drain()`
  → `aggregator.record` → `metricsManager.recordBytesFetched`. In Rust the CompletedFetch is
  built on the bg task but DRAINED on the poll task. So the once-per-response sensor record fires
  on whichever task drains the LAST partition (poll task in steady state). This is NOT per-record
  (Java's exactly-once-per-response contract preserved); Sensor recording is internally
  synchronized (M1/M2), so it's safe. The "bg-task-only" constraint protects the PER-RECORD path
  (pure i32 in completed_fetch), which is untouched.
- **Recording-level: FULL Java parity — all fetch sensors INFO, NO DEBUG gating (FINAL, commit
  b02e070, 2026-06-22).** Java's `SensorBuilder` → `metrics.sensor(name)` (SensorBuilder.java:61)
  defaults EVERY fetch sensor to INFO — client-level records-lag/lead AND the per-partition
  lag/lead DETAIL sensors (FetchMetricsManager.java:133,148) AND their deprecated variants. There
  is NO DEBUG gating in Java. Final user decision = match EXACTLY:
  - ALL sensors → **INFO**: client-level `records-lag`/`records-lead`, per-partition
    `{tp}.records-lag`/-avg/-max + `{tp}.records-lead`/-min/-avg, and the deprecated-variant
    sensors in `maybe_record_deprecated_partition_lag/lead` (Java builds those via
    `new SensorBuilder(...)` too → default INFO).
  - `should_record_partition_metrics()` gate **REMOVED entirely**. `record_partition_lag/lead`
    register + record the per-partition detail unconditionally, exactly like Java.
  - `setup_with_level` removed; `setup()` builds the INFO fixture directly. All tests run at
    default INFO (matching the Java tests): `test_partition_lag`/`test_partition_lead`/
    `test_maybe_update_assignment_with_additional_registered_metrics` use `setup()`.
  - `test_partition_metrics_recording_level` now asserts the per-partition detail IS present at
    INFO: recording one non-deprecated partition registers 6 new metrics + values readable
    (lag=14, lead=11). (The intermediate DEBUG-gated version is gone.)
  - Accepted cost: default INFO consumer records the full per-partition set per-partition-per-poll
    — the Java-parity cost, to be measured in M8.
  - **History (do not re-introduce):** b06b0dc had records-lag/lead at DEBUG + gated the whole
    fetch_collector block (hid records-lag-max). 2635f3f (Critic round-1) made client-level INFO
    but kept per-partition detail DEBUG behind `should_record_partition_metrics()`. b02e070 (this
    final correction) removed all gating per user verification of Java source. Lesson: verify the
    Java source's actual recording level before assuming any "perf gating" is faithful — Java
    defaults everything to INFO here.
- **Per-record loop stays pure.** `completed_fetch.rs` `records_read += 1; bytes_read += size;`
  (i32, no Sensor, no alloc). Aggregator.record ONLY in `drain()`, never in `fetch_records`.
- **Allocation-budget test bump.** The faithful aggregator adds a per-FETCH cost: `partitions:
  HashSet<TopicPartition>` clones each TP (Java `new HashSet<>(responseData.keySet())`) + 1
  aggregator alloc. `test_handle_fetch_success_does_not_copy_payload` budget rose PER_PARTITION 7→8,
  OVERHEAD 2→3 (8 parts: 58→67 ≥ measured 64). A payload byte-clone still adds a SECOND alloc/part
  → breaks budget. The `test_collect_fetch_per_record_allocation_budget` (≤4/record) was NOT touched
  — lag/lead recording is per-partition-per-poll in `fetch_records_from_partition` (outside the
  per-record loop), so even at full INFO parity the per-record budget is unaffected (verified b02e070).
- **maybe_update_assignment lazy ordering (Critic-43 Issue 1).** Java reads only `assignmentId()`
  first, early-returns on unchanged id, and reads/clones `assignedPartitions()` (a
  `HashSet<TopicPartition>` + per-TP String clone) ONLY inside the changed branch. The original
  b06b0dc eagerly computed BOTH into a tuple before the early-return → N TopicPartition clones every
  poll cycle in steady state. Fix: read assignment_id under one lock, compare against
  self.assignment.assignment_id, early-return, then re-lock subscriptions for assigned_partitions()
  only when changed. Steady-state unchanged poll = ZERO TP clones. General lesson: any
  cheap-id-then-expensive-set "maybe update" must preserve Java's read-id-first / clone-set-only-on-change
  order, not collapse both into one tuple before the guard.

- **Java ordering: maybeUpdateAssignment FIRST in prepareFetchRequests** (AbstractFetch.java:423),
  before any early-return. This BROKE the Phase-26 poison test
  (`test_prepare_fetch_requests_all_nodes_pending_skips_subscription_lock`): that test poisoned
  SubscriptionState to prove the short-circuit never locks it, but maybe_update_assignment now reads
  assignmentId (a lock) first — faithful to Java. Renamed to `..._skips_fetchable_scan` and dropped
  the poison; it now asserts the functional short-circuit (empty map, pending-set untouched). The
  Phase-26 short-circuit still avoids the EXPENSIVE per-partition scan, just not the cheap id read.

**Core additions:** `Metrics::add_metric_if_absent` (Java addMetricIfAbsent, idempotent gauge
register). `ClientResponse::latency_ms()` (Java requestLatencyMs) + threaded `request_latency_ms`
through `PendingFetchCompletion::Response` → `handle_fetch_success` (uses `fetch_target.id()` as the
node string for the per-node `node-{id}.latency` sensor, mirroring Java `resp.destination()`).

**Metrics ownership / M7 handoff:** `AsyncKafkaConsumer::create_fetch_metrics_manager(config)`
builds `Arc<Metrics>` (MetricConfig from metrics.num.samples/sample.window.ms/recording.level +
single `client-id` tag, group prefix `"consumer"`) + `Arc<FetchMetricsManager>`. Consumer stores
`metrics: Arc<Metrics>` field (#[allow(dead_code)] until M7) in BOTH AsyncKafkaConsumerComponents
and the struct + both ctors (prod 1452, test 4469). M7 adds `consumer.metrics()` over the SAME
registry — no re-plumb. The manager keeps the registry alive even without the field.

**Carry-overs (documented in code + PLAN):**
- Throttle-time sensor is registered + recordable + exposed via `throttle_time_sensor()`, but the
  delegate-side recording (reading broker throttle_time_ms into the sensor) is NOT wired —
  NetworkClientDelegate metrics pass. Test records through the sensor directly.
- `for_test()` ctor uses empty registry tags + default Metrics (no `client-id` default tag) so the
  template tag sets match runtime tags (registration won't panic); fetch-path tests don't assert values.

**Test-call-site fanout gotcha:** adding the FetchMetricsManager param to AbstractFetch::new /
FetchCollector::new / FetchRequestManager::new + the aggregator to CompletedFetch::new_full +
request_latency_ms to handle_fetch_success touched ~25 test call sites across abstract_fetch,
completed_fetch, fetch_collector, fetch_request_manager (two test mods: `tests` w/ super::*, and
`round_trip` w/ explicit imports), events/application_event_processor, request_managers. Helpers:
`FetchMetricsManager::for_test()`, per-file `agg_for(&tp)` / `test_aggregator()`.
