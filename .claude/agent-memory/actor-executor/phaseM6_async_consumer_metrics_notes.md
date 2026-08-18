---
name: phaseM6-async-consumer-metrics
description: Milestone-9 Phase M6 — AsyncConsumerMetrics + bg-task/event-handler/delegate wiring; INFO not DEBUG; queue-depth AtomicI64 mirror
metadata:
  type: project
---

Milestone-9 Phase M6 (Actor 46): async-consumer background-task / event-queue metrics.

**Class**: `consumer::internals::async_consumer_metrics::AsyncConsumerMetrics` (flat under
`src/consumer/internals/`, M3/M4/M5 precedent — no `metrics/` subdir). 10 sensors:
time-between-network-thread-poll (Avg/Max), application/background event queue
size(Value)/time(Avg/Max)/processing-time(Avg/Max), application-events-expired-count(Value),
unsent-requests queue size(Value)/time(Avg/Max).

**Recording level CORRECTION**: the task brief asserted these are DEBUG. They are NOT. Java
creates every sensor via the plain `metrics.sensor(name)` overload, which `Metrics.java:325`
defines as `sensor(name, RecordingLevel.INFO)`. So matching Java = INFO (on by default). No DEBUG
gating. Documented in PLAN. The firm RECORDING-LEVEL PRINCIPLE = "match Java's actual level",
which here is INFO — do not force DEBUG just because the brief guessed it.

**Queue-depth mirror (idiomatic-Rust deviation, documented)**: Java reads
`applicationEventQueue.size()` / `backgroundEventQueue.size()` (O(1) on LinkedBlockingQueue). The
tokio `mpsc::UnboundedSender` has NO `len()`. Solution: shared `Arc<AtomicI64>` per queue,
`fetch_add(1)` in `*EventHandler::add` (record post-increment value == Java's `size()+1`),
rolled back with `fetch_sub(1)` on send failure, reset to 0 by the drainer (bg task's
`process_application_events` / app side's `process_background_events` — folds Java's
`drainEvents` `recordBackgroundEventQueueSize(0)`). Value-identical to Java.

**Wiring (Java line → Rust site)**:
- `recordTimeBetweenNetworkThreadPoll`: CNT.run_once (reuses iteration's `current_time_ms`, no
  extra clock read; only when `last_poll_time_ms != 0`).
- app-event size(0)/time/processing-time: CNT.process_application_events (clone metrics Arc up
  front so `&mut self` stays usable in the loop; `start_ms` before loop, record after).
- `recordApplicationEventExpiredSize`: CNT run_once reap site + cleanup reap_on_close (UNCONDITIONAL
  — Value stat tracks latest, 0 when nothing expired; matches Java).
- unsent-requests size: NetworkClientDelegate.poll (end); time: try_send (expiry+send) +
  check_disconnects (both removal arms). Helper `record_unsent_requests_queue_time(&unsent, now)`
  skips when `enqueue_time_ms < 0`. Uses `current_time_ms` (the delegate's `updatedNow`
  approximation) not a fresh `time.milliseconds()` — delegate has no Time source threaded.
- bg-event time/processing-time: AsyncKafkaConsumer.process_background_events (clone metrics Arc;
  reset queue-size counter + record size(0) on first event; record time per event; processing-time
  after loop guarded by `had_events`).
- `close()`: AKC close path removes AsyncConsumerMetrics sensors right after kafka_consumer_metrics
  (Java AKC:1574 closeQuietly).

**Plumbing**: M4/M5 post-construction `set_async_consumer_metrics(...)` setter on
ApplicationEventHandler / BackgroundEventHandler / NetworkClientDelegate / ConsumerNetworkThread —
keeps no-arg `new` + all test call sites (BEH×17, AEH×8, CNT×9, NCD×4) untouched. Live consumer
`Self::new` constructs ONE `Arc<AsyncConsumerMetrics>` (group `CONSUMER_METRIC_GROUP`) + two
`Arc<AtomicI64>` counters and wires every site BEFORE bg-task spawn. To wire handlers that get
`Arc`-wrapped at construction, had to relocate `BackgroundEventHandler::new`/`ApplicationEventHandler::new`
to build mutably (`let mut`), call the setter, THEN `Arc::new` — order matters (delegate clones the
bg handler Arc later). Added `async_consumer_metrics` + `background_event_queue_size` to the
`AsyncKafkaConsumerComponents` struct + `new_with_components` literal + BOTH component literals
(production `Self::new` ~1604, test fixture ~4734).

**Perf**: all per-bg-poll / per-event-batch / per-unsent-request, never per-record (CLAUDE.md §11).
`Sensor::record` short-circuits on `should_record()` internally (sensor.rs:231). Fetch/per-record
hot path untouched — `test_collect_fetch_per_record_allocation_budget` still passes.

**Tests**: AsyncConsumerMetricsTest (10, parameterized over CONSUMER_METRIC_GROUP +
CONSUMER_SHARE_METRIC_GROUP as a loop — Java @MethodSource) translated inline; values 123.0/10.0,
names, close-removal. +3 handler-side wiring smoke tests (queue-size record + rollback). Full lib
2113 green (was 2100). Java AKC test `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime`
deferred to M7 (needs public `metrics()` accessor + MockTime-injectable consumer fixture); skip
notes updated.

**Clippy gotcha**: nested `if cond { if let Some()... }` trips `collapsible_if` on this rustc →
use `if cond && let Some(..) = .. { }` let-chain.
