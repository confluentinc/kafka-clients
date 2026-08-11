---
name: review-m9-m6-async-consumer-metrics
description: M9 Phase M6 AsyncConsumerMetrics review — stale-defer comment trap, unconditional-vs-guarded size-0 record, queue-counter mirror of queue.size()
metadata:
  type: project
---

Phase M6 wired `AsyncConsumerMetrics` (10 INFO sensors) into bg task + event
handlers + delegate. Two real issues; production record path otherwise clean.

**Why:** Critic-46 review of commits 6df99e4/0cc48c4. Captures reusable audit
heuristics for metrics-wiring phases.

**How to apply:**

- **Stale-deferral-comment trap.** A pre-existing "test deferred because class X
  not yet translated" comment becomes a LIE the moment the phase translates X.
  When a phase adds the infra a prior comment said was missing, grep the test
  files for that comment's named tests and re-evaluate the defer. M6 left
  `ConsumerNetworkThreadTest.testRunOnceRecordTimeBetweenNetworkThreadPoll` +
  `...RecordApplicationEventQueueSizeAndApplicationEventQueueTime` deferred under
  a comment claiming "AsyncConsumerMetrics not yet translated / bg-task emits
  log::trace!" — both false post-M6. These are the ONLY tests exercising the
  record sites *inside run_once* (unit tests only test sensors in isolation).
  Defer rationale "needs public metrics()" was wrong: Java tests read
  `metrics.metric(metricName)` off a plain `Metrics`, and the CNT test module
  already has a `MockTime` with `sleep()`. Verify a claimed blocker by checking
  the fixture, not the prose.

- **Unconditional vs guarded metric record on drain.** Java `drainEvents()`
  (BackgroundEventHandler) records `recordBackgroundEventQueueSize(0)`
  UNCONDITIONALLY (no isEmpty early-return), so the gauge snaps to 0 on every
  idle drain. But Java `processApplicationEvents` (ConsumerNetworkThread) DOES
  early-return on empty before its size-0 record. Two sibling drain paths, two
  different behaviors — check each Java drainer for an early-return before
  copying a single "record 0 only when non-empty" pattern. Rust mirrored the
  app side correctly but wrongly guarded the background side with
  `if !had_events` → gauge lingers at stale peak during idle. Low severity
  (Value gauge, self-corrects) but a real Java mismatch.

- **Queue-depth Arc<AtomicI64> as queue.size() substitute — audit checklist.**
  tokio mpsc has no len(), so a shared `Arc<AtomicI64>` mirrors Java
  `BlockingQueue.size()`. Confirm: (a) enqueue records `fetch_add(1)+1` == Java
  `size()+1`, recorded BEFORE the actual send (Java records before
  `queue.add`); (b) rollback `fetch_sub(1)` on send failure; (c) drainer
  `store(0)`; (d) no other decrement (so can't go negative), no leak (reset each
  non-empty drain); (e) enqueue-during-drain transient race is present in Java
  too (same window) → behavior-faithful, NOT a bug. The app-side and bg-side
  must share the SAME Arc (one bumps, the other resets) — verify both are
  Arc::clone of one instance, wired before spawn.

- **Recording level: INFO is the on-by-default cost.** `Metrics.sensor(name)`
  (no RecordingLevel arg) = INFO (Metrics.java:325). DEBUG sensors use
  `sensor(name, RecordingLevel.DEBUG)`. Grep the Java ctor for which overload
  EACH sensor uses — don't assume. All 10 AsyncConsumerMetrics sensors are
  plain `sensor(name)` = INFO, so a default consumer records them every bg poll;
  that's Java-faithful, not a perf regression to flag.

- **Perf-gate for bg-loop metrics:** reuse the iteration's existing
  current_time_ms (no extra clock read), no per-iteration heap alloc (scratch
  Vec reuse + Arc refcount-clone not alloc), per-record fetch path untouched
  (grep that record sites live only in run_once / handlers / delegate.poll),
  Sensor::record short-circuits on should_record(). `record_at(v, timeMs)` for
  the explicit-timeMs Java `sensor.record(v, timeMs)`; `record(v)` for
  current-time.
