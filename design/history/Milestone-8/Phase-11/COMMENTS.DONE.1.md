# Phase 11 Batch 1 — Resolved Critic Comments (N=1)

Each section records the original Critic finding + the resolving commit.

---

## Issue 1: `unsubscribe()` lacks iterative `process_background_events` loop — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N).
- **Resolving commit**: `0d88bb9` (same commit — the iterative loop was
  already wired via [`process_background_events_until`] when the unsubscribe
  body landed; the deferral comment in the Actor's writeup overstated the
  seam).
- **Closing commit**: commit 4/N (`Phase 11 (4/N): AsyncKafkaConsumer — poll
  + checkInflightPoll + AsyncPollEvent lifecycle`) — at which point the
  `poll()` body also uses the same iterative drain pattern, confirming the
  helper is exercised on every blocking-style API entry per §31.
- **Verification**: lines 725-732 of `src/consumer/async_kafka_consumer.rs`
  route the unsubscribe future through `process_background_events_until`
  with the Java predicate (`GroupAuthorizationException` /
  `TopicAuthorizationException` swallowed).

---

## Issue 2: `process_background_events` skips `backgroundEventReaper.reap` — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N).
- **Resolving commit**: `0d88bb9` (same commit — the `reap` call was wired
  into the end-of-drain block at lines 905-909; the Actor's deferral
  uncertainty in the commit message overstated the seam).
- **Closing commit**: commit 4/N — verified that every blocking-style API
  entry (`poll`, `unsubscribe`, future commit/position/etc.) routes
  through `process_background_events` and therefore through the reap call.
- **Verification**: lines 905-909 of `src/consumer/async_kafka_consumer.rs`
  invoke `self.completable_event_reaper.lock().unwrap().reap(now_ms)` after
  the drain loop completes, regardless of error / no-error outcome
  (matches Java line 2222).

---

## Issue 8: `subscribe_with_listener` stores the listener BEFORE `add_and_get` confirms — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N).
- **Resolving commit**: `0d88bb9` (same commit — the listener mirror
  assignment was already gated behind `add_and_get(...).await?`; the
  Critic's reading was based on an earlier draft).
- **Closing commit**: commit 4/N — verified on re-read of lines 605-615,
  638-647, 671-680 that each subscribe variant stores the
  app-side listener (`*self.rebalance_listener.lock().unwrap() = Some(l)`)
  ONLY after `add_and_get` resolves `Ok(())`. The `?` operator short-
  circuits the function so the store is unreachable on failure.
- **Verification**: lines 605-615 (topics), 638-647 (client-side regex),
  671-680 (Re2J pattern) of `src/consumer/async_kafka_consumer.rs`.
