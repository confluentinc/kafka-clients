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

---

## Issue 3: State-read methods (`assignment`, `subscription`, `paused`, `client_id`, `current_lag`, `group_metadata`) do NOT enforce `ensure_open()` like Java does — RESOLVED

- **Original commit**: `77b0cbb` Phase 11 (2/N).
- **Resolving commit**: Phase 11 (7/N).
- **Resolution**: Chose option (b) — keep current silent-empty behavior
  for sync accessors, and document the divergence explicitly in
  doc-comments on each accessor. Rationale documented at the
  module-level "Sync state-read methods" comment in
  `async_kafka_consumer.rs`:
    - panicking on a pure accessor would diverge sharply from idiomatic
      Rust;
    - the strict closed-consumer check IS enforced on every `async fn`
      (poll / commit / position / committed / unsubscribe / close /
      etc.) via `ensure_open()`;
    - the relevant Java tests (`testListPartitionsAfterClose` style)
      will be listed in the commit-8 test-skip rationale.
- **Verification**: doc-comments now on `assignment()` / `subscription()`
  / `paused()` / `client_id()` / `current_lag()` / `group_metadata()`
  in `src/consumer/async_kafka_consumer.rs` (module-level "Sync
  state-read methods" comment + per-method doc lines), each calling out
  "Returns the empty / cached value silently when the consumer is
  closed (Java throws IllegalStateException)."

---

## Issue 4: `group_metadata()` does NOT call `throw_if_group_id_not_defined()` — diverges from Java — RESOLVED

- **Original commit**: `77b0cbb` Phase 11 (2/N).
- **Resolving commit**: Phase 11 (7/N).
- **Resolution**: Chose option (c) — document the divergence in the
  `group_metadata()` doc-comment AND list
  `testGroupMetadataAfterCreationWithGroupIdIsNull` in the commit (8/N)
  test-skip section with the matching rationale. The Phase 2 trait
  surface returns `ConsumerGroupMetadata` (no error channel), and
  changing the trait to `Result<ConsumerGroupMetadata, KafkaError>` is
  out of scope for Phase 11 (would propagate through every consumer
  impl). The strict-Java group-id check is already enforced on the
  error-bearing paths (`commit_*` / `subscribe` etc.) via
  `throw_if_group_id_not_defined()`.
- **Verification**: rationale captured in the rustdoc above
  `pub fn group_metadata(&self) -> ConsumerGroupMetadata` in
  `src/consumer/async_kafka_consumer.rs` — explicitly notes the
  Phase 2 trait constraint and the strict-Java surface on
  `commit_*` / `subscribe`. The commit (8/N) test-skip section will
  list `testGroupMetadataAfterCreationWithGroupIdIsNull` with this
  rationale.

---

## Issue 9: `paused_partitions()` returned by `paused()` is mutable in Rust where Java returns `Collections.unmodifiableSet(...)` — RESOLVED

- **Original commit**: `77b0cbb` Phase 11 (2/N).
- **Resolving commit**: Phase 11 (7/N).
- **Resolution**: Added a module-level doc-comment noting "Returns an
  owned mutable `HashSet` (Java returns
  `Collections.unmodifiableSet(...)`)." on `assignment()`,
  `subscription()`, `paused()`. The divergence is idiomatic-Rust
  (owned vs read-only view); the doc-comment makes it explicit so a
  user porting from Java sees the difference at the API doc level.
- **Verification**: doc-comments on `assignment()` / `subscription()`
  / `paused()` in `src/consumer/async_kafka_consumer.rs`.
