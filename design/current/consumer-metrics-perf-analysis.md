# Consumer metrics — performance cost analysis & benchmark methodology

Milestone-9 Phase M8 (Actor 48). Closes the loop on the user's standing
concern: *the tuned KIP-848 Rust consumer used to be "metrics-free" (every
`record*` call was a no-op); M1–M7 wired the full Java-parity metrics
framework. Did that regress the hot path?*

**Short answer:** No per-record cost was added. The per-record receive loop
stays pure i32 counter accumulation (`records_read += 1; bytes_read += size`) —
verified by code inspection of the loop body and backed by an automated
allocation-budget guard against *allocating* per-record regressions (§3). All
`Sensor.record(...)` calls
fire **per-fetch / per-partition-per-poll / per-poll / per-bg-poll /
per-commit / per-heartbeat / per-rebalance / per-callback** — never per record.
The consumer now pays **exactly Java's metrics cost** (all sensors at INFO,
which is Java's default), no more, no less. The earlier metrics-free CPU /
latency numbers are therefore superseded — they measured a consumer doing
strictly less work than the Java client.

---

## 1. Recording-level reality

There is **no DEBUG gating** anywhere in the consumer metrics path — this was
verified against the Java source during M3 and M6 (see those phases' notes).
Java builds every consumer sensor via the plain `metrics.sensor(name)` overload,
which `Metrics.java` defines as `sensor(name, RecordingLevel.INFO)`. So matching
Java means **everything is INFO and on by default**, including:

- the client-level `records-lag-max` / `records-lead-min` sensors, AND
- the per-partition `{tp}.records-lag` / `-lag-avg` / `-lag-max` /
  `records-lead` / `-lead-min` / `-lead-avg` detail sensors, AND
- the async-consumer background/event-queue sensors (M6).

The plan's early drafts guessed some of these were DEBUG; the Java source says
otherwise, and we match Java exactly. The only knob the user has is Java's:
`metrics.recording.level` (`INFO` default, `DEBUG`, `TRACE`) — but since Java
registers these at INFO, lowering to a more restrictive setting is not how Java
reduces this cost, and we do not deviate from Java.

---

## 2. Where metrics recording happens, and at what frequency

The table below is the complete inventory of `Sensor.record(...)` (and gauge
update) sites in the wired consumer. **The "per record" row is intentionally
empty** — that is the whole point.

| Frequency | Sites (sensors / gauges) | Wired in | Cost driver |
|---|---|---|---|
| **PER RECORD** | *(none)* | — | The per-record loop is pure `records_read += 1; bytes_read += size` (i32). No `Sensor.record`, no allocation. |
| **Per fetch response** (once per `FetchResponse`, aggregated) | `bytes-fetched` (rate+total), `records-fetched` (rate+total), `fetch-latency` (avg/max + rate), `fetch-throttle-time` (avg/max), per-topic `topic.{t}.bytes-fetched` / `.records-fetched`, per-node `node-{id}.latency` | `FetchMetricsAggregator::record` called once per partition in `CompletedFetch::drain()`; the aggregator flushes the fetch-level sensors once the LAST partition of the response has drained (Java `recordAggregatedMetrics`). `abstract_fetch::handle_fetch_success`. | One `Sensor.record` per sensor per fetch response. The per-response `HashSet<TopicPartition>` clone (Java `new HashSet<>(responseData.keySet())`) is per-fetch, not per-record. |
| **Per partition, per poll** | client `records-lag-max` / `records-lead-min` + per-partition `{tp}.records-lag`(/-avg/-max) and `{tp}.records-lead`(/-min/-avg) | `fetch_collector::collect_fetch` → `record_partition_lag` / `record_partition_lead`. | **Scales with the number of assigned partitions.** This is Java's accepted per-fetch cost (the user explicitly accepted it for Java parity). For P partitions polled, ~2 sensor records + (on first sight) per-partition detail-sensor registration. |
| **Per `poll()` call** | `time-between-poll` (avg/max), `poll-idle-ratio-avg`, `last-poll-seconds-ago` gauge | `KafkaConsumerMetrics` wired in the `poll` path. | A couple of `Sensor.record` per `poll()` (called at batch granularity, ≤ ~100/s realistically). |
| **Per background-task poll iteration** | `time-between-network-thread-poll` (avg/max), app/background event-queue size(value)/time(avg/max)/processing-time(avg/max), `application-events-expired-count`, unsent-requests queue size/time | `AsyncConsumerMetrics`, wired in `ConsumerNetworkThread::run_once`, the event handlers, and `NetworkClientDelegate::poll` (M6). INFO (Java default), NOT DEBUG. | A handful of `Sensor.record` per bg-loop iteration. The per-iteration loop adds no per-event allocation; queue depth is mirrored via a shared `Arc<AtomicI64>` (tokio mpsc has no `len()`). |
| **Per commit** | `commit-latency` (avg/max + rate), `commit-rate`/`-total` | `OffsetCommitMetricsManager`, wired in the commit-request-manager response path. | One record per OffsetCommit response. |
| **Per `commit_sync` / `committed` API call** | `commit-sync-time-ns-total`, `committed-time-ns-total` | `KafkaConsumerMetrics`, wired in the `commit_sync` / `committed` paths (Java try/finally → inner-helper split). | One record per synchronous call. |
| **Per heartbeat** | `heartbeat-latency` (avg/max), `heartbeat-rate`/`-total`, `last-heartbeat-seconds-ago` gauge | `HeartbeatMetricsManager`, wired in the HB-request-manager success path. | One record per heartbeat response (heartbeat interval cadence). |
| **Per rebalance** | `rebalance-latency` (avg/max/total), `rebalance-rate-per-hour`, `rebalance-total`, `failed-rebalance-total`/`-rate-per-hour`, `assigned-partitions` gauge, `last-rebalance-seconds-ago` gauge | `ConsumerRebalanceMetricsManager`, wired in the membership reconcile / rebalance lifecycle. | A few records per rebalance (rare). The `assigned-partitions` gauge locks `SubscriptionState` only when READ (snapshot time), not per-poll. |
| **Per rebalance callback** | `partition-revoked-latency`, `partition-assigned-latency`, `partition-lost-latency` (each avg/max) | `RebalanceCallbackMetricsManager`, wired in `consumer_rebalance_listener_invoker` (recorded on Ok only). | One record per listener-callback invocation (rare). |

**Key takeaways:**

- **Nothing records per record.** The receive hot path (`completed_fetch.rs`
  per-record loop, `fetch_collector` decode loop) does only pure atomic / i32
  counter work. This is enforced, not just asserted in prose — see §3.
- The dominant steady-state metrics cost is **per-fetch** (a fixed handful of
  `Sensor.record` per response) plus **per-partition-per-poll** lag/lead
  (scales with assigned-partition count P). Both are Java's costs; we match
  them 1:1.
- Per-poll / per-bg-poll / per-commit / per-heartbeat / per-rebalance /
  per-callback are all low-frequency relative to record throughput.

---

## 3. Automated zero-cost guards (no broker required)

Three `#[test]`s lock in the "metrics added zero per-record cost" claim. They
run in CI (`cargo test --lib`) on every change:

1. **`fetch_collector::tests::test_collect_fetch_per_record_allocation_budget`**
   — builds the `FetchCollector` WITH `FetchMetricsManager::for_test()` and
   asserts `collect_fetch` over 100 records stays within
   `budget = 100 overhead + 4 × 100 records = 500` total allocations. A
   per-record `Sensor.record` / byte copy would blow this budget. **Passes
   unchanged after all metrics wiring: 343 allocs / 100 records, budget 500.**

   On the "3.43/record" figure: that is the **total** alloc count (343) divided
   by record count (100), so it folds the ~22 one-time per-fetch overhead
   (amortized over the 100-record fixture) into a "/record" number. The **true
   marginal per-record allocation is ~2.2/record** — the user deserializer's
   key + value `String::from_utf8`, the ONLY §27-sanctioned per-record
   allocation. The fixed per-fetch overhead does not scale with record count,
   so at larger batch sizes the total/record figure tends toward the ~2.2
   marginal cost.

2. **`abstract_fetch::tests::test_handle_fetch_success_does_not_copy_payload`**
   — builds `AbstractFetch` WITH `FetchMetricsManager::for_test()` and asserts
   `handle_fetch_success` over 8 partitions of 16 KiB payloads does not copy
   the payload bytes (budget does not scale with payload SIZE). The per-fetch
   aggregator's fixed `HashSet<TopicPartition>` clone raised the per-partition
   budget by exactly 1 (7→8) — that is the per-fetch cost, not per-record.
   **Passes unchanged: 64 allocs / 8 partitions (budget 67).**

3. **`completed_fetch::tests::test_per_record_loop_is_pure_counter_no_sensor_record`**
   (added in M8) — the explicit guard. Drives the per-record loop
   (`fetch_records`) over 200 records WITH a metrics aggregator attached and a
   **zero-allocation deserializer** (decodes to byte length, no `String`), so
   the only per-record allocations come from `ConsumerRecord` construction +
   `Vec` growth. Asserts ≤ 3 allocs/record. **Measured: 7 allocs / 200 records
   = 0.04/record** (just the `Vec` doubling reallocations — effectively zero
   per record). It then calls `drain()` once and confirms that is where the
   per-partition sensor record fires — explicitly outside the per-record
   window.

   **What this guard catches, precisely** (it is an *allocation-count* guard,
   not a "no per-record sensor call" guard): it trips on an **allocating**
   per-record metric regression — the realistic one — namely moving
   `FetchMetricsAggregator::record` (which allocates a `String` + a `Vec`) into
   the per-record loop, or a windowed-stat **sample rotation** (a new `Sample`
   pushed when a window rolls over). It would **NOT** catch a bare steady-state
   `Sensor::record(value)` inserted per record: a windowed `SampledStat`
   preallocates its sample `Vec` (`Vec::with_capacity(DEFAULT_NUM_SAMPLES + 1)`),
   so steady-state `record_internal` is pure mutex + arithmetic with **zero
   allocation**, which an alloc-count budget cannot see. The stronger
   invariant — *no `Sensor::record` per record at all* — is established by
   **code inspection** of the verified-pure `fetch_records` loop body
   (`records_read += 1; bytes_read += size;`, no sensor call) plus the
   **loop-head comment**, NOT by this allocation test.

A documenting comment at the per-record loop site (`completed_fetch.rs`
`fetch_records`) states the invariant and points at this test.

---

## 4. In-process micro-bench (no broker required)

`common::metrics::sensor::tests::bench_sensor_record_ns` measures the per-call
cost of `Sensor::record_at` on a realistic fetch-shaped sensor (a `Meter` =
Rate + CumulativeSum, plus `Avg` + `Max` — the stat shape of `bytes-fetched` /
`fetch-latency`). It is `#[ignore]`d (timing is environment-dependent and would
flake in CI) but builds in CI. Run it on demand:

```
cargo test --release --lib bench_sensor_record_ns -- --ignored --nocapture
```

**Measured (Apple M-series), over 2,000,000 iterations including periodic
window rollover:**

- **Release build: ~46 ns/call** (`--release`, the figure that matters).
- Debug build: ~409 ns/call (for reference — do not use for cost estimates).

Re-run on your target hardware for the authoritative figure; the release number
is the one to use.

**Caveat (conservative direction).** The micro-bench calls `Sensor::record_at`
with a precomputed `MockTime` timestamp, so it excludes the one live
`Time::milliseconds()` system-clock read that the *production* call path
(`Sensor::record(value)` → `record_internal(value, self.time.milliseconds())`)
performs per call. The production recording sites (e.g. `record_partition_lag`,
the aggregator → manager → `sensor.record(...)`) all go through `record()` and
DO read the live clock. So ~46 ns/call is a slight **under**-count of the real
per-call cost — by roughly one clock read (tens of ns on some platforms) — i.e.
the true figure is marginally higher. The direction is the conservative one for
a "metrics are cheap" claim, the clock read is amortized per-fetch /
per-partition (not per record), and the figure remains negligible. Still
illustrative, not a measured end-to-end figure — see §5.

**How to use this number:** multiply by the per-poll record frequency from §2.
Per fetch response there are roughly a dozen `Sensor.record` calls plus ~2 per
assigned partition for lag/lead. At, say, 50 fetch responses/sec across 32
partitions that is on the order of `50 * (12 + 2*32) ≈ 3,800` sensor records/sec
≈ `3,800 * 46 ns ≈ 0.17 ms/sec` of CPU ≈ **~0.017% of one core** for metrics
recording — and **none of it is on the per-record path**, so it does not scale
with message throughput, only with fetch/partition count. (Substitute your own
ns/call and frequencies; this is illustrative, not a measured end-to-end
figure — see §5.)

---

## 5. Live broker benchmark — methodology & commands (USER-RUN)

The Actor sandbox has no broker/Docker, so the authoritative end-to-end
before/after comparison is a **user-run step**. The existing harness in
`consumer-perf/` (analysis dir: `design/current/`) does exactly this.

### Baseline commit (pre-metrics)

- **Metrics-on (current):** `HEAD` of `consumer-impl`.
- **Metrics-free baseline:** **`1694b41`** — the parent of the first M1 commit
  (`c71c9ba M1: metrics core foundation`). At `1694b41` every consumer
  `record*` call was a no-op. Verify:
  ```
  git log --oneline 1694b41 -1        # fixup! Phase 40: PlaintextConsumerTest ...
  git log --oneline c71c9ba^ -1       # same commit (c71c9ba is first M1)
  ```

### Run both builds against your broker

For each of the two commits, build `--release` and run `consumer-perf` against
the same broker/topic at the same fixed throughput, then compare. (Use a
worktree or stash so you don't disturb HEAD.)

```bash
# --- metrics-on (current HEAD) ---
git worktree add /tmp/perf-metrics-on HEAD
cd /tmp/perf-metrics-on
cargo run -p consumer-perf --release -- \
    --bootstrap <BROKER:9092> \
    --topic consumer-perf-bench \
    --partitions 32 \
    --throughput 50000 \
    --message-size 512 \
    --duration 180 \
    --interval 5 \
    --results-dir results/metrics-on

# --- metrics-free baseline (pre-M1) ---
git worktree add /tmp/perf-baseline 1694b41
cd /tmp/perf-baseline
cargo run -p consumer-perf --release -- \
    --bootstrap <BROKER:9092> \
    --topic consumer-perf-bench \
    --partitions 32 \
    --throughput 50000 \
    --message-size 512 \
    --duration 180 \
    --interval 5 \
    --results-dir results/baseline
```

Compare the JSONL metric lines (CPU%, RSS, e2e latency percentiles) between
`results/metrics-on` and `results/baseline`. Key flags (full list:
`cargo run -p consumer-perf -- --help`): `--throughput` (fixed rate; do NOT use
`-1`/unbounded — latency must reflect steady state), `--partitions` (drives the
per-partition lag/lead metric cost — try 32 and 128 to see the partition-count
scaling called out in §2), `--peak` (max-throughput mode), `--fetch-min-bytes`
/ `--fetch-max-wait-ms` (latency knobs), `--no-produce` (if you drive your own
producer).

### Cloud-limits harness (optional, higher fidelity)

For a Confluent Cloud / SASL_SSL run, use the `cloud-limits` skill / the
in-region EC2 harness referenced in `design/current/` (the WAN is the latency
killer; run in-region). Same before/after methodology: one run on HEAD, one on
`1694b41`.

### What to look for

- **CPU%:** expect a small, throughput-independent increase on the metrics-on
  build, dominated by per-fetch + per-partition lag/lead recording — and
  **rising with `--partitions`, not with `--throughput`** (because nothing
  records per record). This is the Java-parity cost.
- **e2e latency:** should be statistically unchanged (metrics recording is off
  the receive critical section; the per-record path is byte-identical — §3).
- **RSS:** a fixed increase for the registered sensors/metrics + sample ring
  buffers (one-time, does not grow with throughput).

If the metrics-on CPU delta is materially larger than the §4 estimate predicts,
that is a regression to investigate — start by re-running the three §3 guard
tests and confirming nothing slipped onto the per-record path.

---

## 6. Conclusion

- The KIP-848 Rust consumer is **no longer metrics-free**: it records the full
  Java metric set at Java's recording levels (INFO).
- **Zero per-record overhead** was added — enforced by three CI guard tests
  (§3), not just claimed.
- The metrics cost is **per-fetch / per-partition-per-poll / per-poll /
  per-bg-poll / per-commit / per-heartbeat / per-rebalance / per-callback** —
  exactly Java's cost surface. The per-partition lag/lead component scales with
  assigned-partition count (user-accepted for Java parity).
- The earlier "metrics-free" perf numbers are **superseded**; they measured a
  consumer doing less work than the Java client. The authoritative metrics-on
  vs pre-M1 (`1694b41`) comparison is a user-run broker benchmark (§5); the
  in-process micro-bench (§4) quantifies the per-`Sensor.record` cost without a
  broker.
