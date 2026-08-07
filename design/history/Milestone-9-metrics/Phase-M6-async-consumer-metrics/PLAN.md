# Phase M6 — AsyncConsumerMetrics (Actor 46)

Milestone-9 consumer metrics. Translates
`org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics`
and wires the previously no-op record sites in the background task, the
application/background event handlers, and the network client delegate.

## Scope

- `consumer::internals::async_consumer_metrics::AsyncConsumerMetrics`
  (flat under `src/consumer/internals/`, M3/M4/M5 precedent — no `metrics/`
  subdir despite Java's `metrics` package).
- Wire the 10 `record*` methods at the exact Java sites.
- Translate `AsyncConsumerMetricsTest` (9 parameterized tests × 2 groups).

## Recording levels (Java source = contract)

**CORRECTION to the task brief.** The brief asserted these sensors are
DEBUG. They are **not**. Every sensor in `AsyncConsumerMetrics.java` is
created with the plain `metrics.sensor(name)` overload, which
`Metrics.java:325` defines as `sensor(name, RecordingLevel.INFO)`. So per
the firm RECORDING-LEVEL PRINCIPLE (match Java's actual level per sensor),
all 10 sensors are **INFO** in Rust too. No DEBUG gating is added — that
would diverge from Java and would silently drop metric values the Java
client always records.

| Sensor | Java level | Rust level |
|---|---|---|
| time-between-network-thread-poll | INFO | INFO |
| application-event-queue-size | INFO | INFO |
| application-event-queue-time | INFO | INFO |
| application-event-queue-processing-time | INFO | INFO |
| application-events-expired-count | INFO | INFO |
| background-event-queue-size | INFO | INFO |
| background-event-queue-time | INFO | INFO |
| background-event-queue-processing-time | INFO | INFO |
| unsent-requests-queue-size | INFO | INFO |
| unsent-requests-queue-time | INFO | INFO |

## Perf

These are per-bg-poll / per-event-batch / per-unsent-request — NOT
per-record (CLAUDE.md §11 hot-path definition). They are off the fetch
record path entirely. `Sensor::record()` already short-circuits on
`should_record()` internally (`sensor.rs:231`), so even though these are
INFO (on by default), the only added work per record-free bg iteration is:

- one `time.milliseconds()` that Java's `runOnce` already computes
  (`current_time_ms`) — reused, no extra clock read;
- queue-depth `Arc<AtomicI64>` increments on enqueue (one relaxed atomic
  add per `add()`), the idiomatic-Rust analogue of Java's O(1)
  `BlockingQueue.size()` (the tokio mpsc sender exposes no `len()`).

No per-record allocation, no per-record sensor record. The §27 per-record
allocation-budget test is unaffected (no fetch-path change).

## Queue-depth counters (idiomatic-Rust deviation, documented)

Java reads `applicationEventQueue.size()` / `backgroundEventQueue.size()`
(O(1) on `LinkedBlockingQueue`). The Rust `mpsc::UnboundedSender` has no
`len()`. We mirror the size with a shared `Arc<AtomicI64>` per queue:
incremented (fetch_add) in `*EventHandler::add` BEFORE the size is recorded
(Java records `size()+1` before adding — we record the post-increment
value, same number), and reset to 0 by the drainer (the bg task's
`processApplicationEvents` / the app side's `processBackgroundEvents`).
Value-identical to Java; only the size source differs.

## Wiring sites (file : Java line → Rust site, frequency)

1. `ConsumerNetworkThread.run_once` — `recordTimeBetweenNetworkThreadPoll`
   (Java CNT.java:216) per bg poll; replaces the existing `log::trace!`
   placeholder.
2. `ConsumerNetworkThread.process_application_events` —
   `recordApplicationEventQueueSize(0)` (CNT:253),
   `recordApplicationEventQueueTime` per event (CNT:256),
   `recordApplicationEventQueueProcessingTime` (CNT:273).
3. `ConsumerNetworkThread.reapExpiredApplicationEvents` / cleanup —
   `recordApplicationEventExpiredSize` (CNT:282, CNT:427), fed by the
   reaper `reap(...)` return.
4. `NetworkClientDelegate.poll` — `recordUnsentRequestsQueueSize` (NCD:169).
5. `NetworkClientDelegate.try_send` / `check_disconnects` —
   `recordUnsentRequestsQueueTime` on each removal (NCD:203/214/242/248).
6. `ApplicationEventHandler.add` — `recordApplicationEventQueueSize`
   (AEH:99).
7. `BackgroundEventHandler.add` — `recordBackgroundEventQueueSize`
   (BEH:56); `drainEvents` → `recordBackgroundEventQueueSize(0)` (BEH:68)
   is folded into the app-side drainer (Rust handler is sender-only).
8. `AsyncKafkaConsumer.process_background_events` —
   `recordBackgroundEventQueueTime` per event (AKC:2206),
   `recordBackgroundEventQueueProcessingTime` (AKC:2219); set
   queue-depth counter to 0 at drain start (folds BEH:68 drainEvents).

## Ownership / plumbing

`AsyncKafkaConsumer` constructs one `Arc<AsyncConsumerMetrics>` against its
shared `Arc<Metrics>` (group `CONSUMER_METRIC_GROUP`), plus two shared
`Arc<AtomicI64>` queue-depth counters. These are threaded into the
handlers/delegate/bg-task. Since the constructors have many test call sites
(BEH×17, AEH×8, CNT×9, NCD×4), use the M4 post-construction setter pattern
(`set_async_consumer_metrics(...)`) so the no-arg `new` signatures and all
existing test call sites stay untouched. The live consumer construction
path calls the setters; tests that don't care leave the `Option` `None`
(record* become no-ops — same as the prior deferral).

## Tests

Translate `AsyncConsumerMetricsTest` inline in `async_consumer_metrics.rs`:
- `shouldMetricNames` (names present after create, absent after close)
- `shouldRecordTimeBetweenNetworkThreadPoll`
- `shouldRecordApplicationEventQueueSize`
- `shouldRecordApplicationEventQueueTime`
- `shouldRecordApplicationEventQueueProcessingTime`
- `shouldRecordUnsentRequestsQueueSize`
- `shouldRecordUnsentRequestsQueueTime`
- `shouldRecordBackgroundEventQueueSize`
- `shouldRecordBackgroundEventQueueTime`
- `shouldRecordBackgroundEventQueueProcessingTime`

Java parameterizes over `CONSUMER_METRIC_GROUP` and
`CONSUMER_SHARE_METRIC_GROUP` via `@MethodSource`. Translate the
parameterization as a loop over both group names inside each test. The
share group is exercised only as a metric-group-name string here (the
share consumer itself is out of scope per §20); `AsyncConsumerMetrics`
takes the group name as a plain `&str`, so both arms are valid.

Plus wiring smoke tests where cheap (e.g. handler `add` bumps the
queue-depth counter; bg drain resets it).

## Out of scope / skips

- `ShareConsumerImpl` record sites (BEH/processing-time at SCI:1236/1249) —
  share consumer is out of scope (§20). The same `AsyncConsumerMetrics`
  class is used by both; we wire only the `AsyncKafkaConsumer` sites.
- `KafkaShareConsumerMetrics` — out of scope.

## DoD

`cargo build` / `cargo test --lib` / `cargo xtask lint` /
`cargo xtask format-check` green after each commit group. Metric VALUES
asserted (123.0 / 10.0) like the Java test. Names + INFO level matched 1:1.
