---
name: review-m8-latency-pollforfetches
description: pollForFetches latency-fix review — dropped retryBackoffMs clamp, await_wakeup race, §31 drain bound parity
metadata:
  type: project
---

Review of consumer steady-state latency commits (0a37f87 completion_notify,
a038aeb await_wakeup block + no-spin guard). Comments in
`design/history/Milestone-8/Phase-15-latency/COMMENTS.1.md`.

**Real finding — dropped `retryBackoffMs` clamp in `poll_for_fetches`:**
Java `AsyncKafkaConsumer.pollForFetches` (1888-1904) reduces pollTimeout to
retryBackoffMs (100ms) when no assigned partitions OR any partition lacks a
valid position. Rust port omits this entirely. Matters because
`OffsetsRequestManager` does NOT override `maximum_time_to_wait` (inherits
`i64::MAX` from request_manager.rs:72), so nothing clamps the wait during the
invalid-position/startup window → park up to MAX_POLL_TIMEOUT_MS=5s. Ties into
known initial-join latency bug.

**Verified-OK patterns (don't re-flag):**
- §31 rebalance-drain bound is IDENTICAL Rust↔Java: BackgroundEventHandler.add
  only sends on mpsc, does NOT poke FetchBuffer (both langs). RECONCILING is
  excluded from should_heartbeat_now() in BOTH (abstract_membership_manager.rs:207
  / AbstractMembershipManager.java:692). So no Rust-specific rebalance stall.
- await_wakeup select! against current_token().cancelled() is race-safe:
  CancellationToken is shared Arc, cancel-before-await still resolves cancelled().
  FetchBuffer::await_wakeup uses notified().enable()-before-recheck.
- handle_fetch_failure does NOT wake buffer — faithful to Java handleFetchFailure;
  no-spin guard still safe because next run_once re-evaluates (re-fetch or
  empty-inflight wake).
- send_prefetches in pollForFetches is a Rust ADDITION (Java relies on
  AsyncPollEvent→createFetchRequests, which Rust process(AsyncPoll) also drives
  at application_event_processor.rs:1362). Harmless: create_fetch_requests
  idempotent via in-flight skip; unbounded channel drained unbounded per cycle.

**Heuristic learned:** when reviewing a poll-timeout cap, check whether ALL
contributing request managers actually override maximum_time_to_wait — a
manager inheriting the i64::MAX default silently defeats a "min" that the Java
code relies on for short backoff waits.
