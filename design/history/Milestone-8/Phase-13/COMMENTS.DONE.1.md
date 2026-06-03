# Phase 13a — Production-code gaps resolved

All 4 production gaps surfaced by the `PlaintextConsumerAssignTest`
pilot translation (commit `972a053`) have been resolved. The single
fix — turning `fetch_offsets_with_retries` into an actual retry loop
and wiring `mark_coordinator_unknown` into the OffsetFetch/OffsetCommit
response paths — closed all 4 issues as the original
"Action items for the Manager" predicted: Issue 1 was the highest-impact
gap and Issues 2, 3, 4 were all downstream of the same root cause.

All 7 previously `#[ignore]`-gated tests have been un-ignored. Final
run: **8/8 tests pass** under
`cargo test --features integration-tests --test integration plaintext_consumer_assign_test -- --include-ignored --test-threads=1`.

---

## Issue 1: `committed()` does not retry on `NotCoordinator`

**Affected Rust tests:**
- `test_async_assign_and_commit_async_not_committed`
- `test_async_assign_and_commit_sync_not_committed`
- `test_async_assign_and_fetch_committed_offsets` (consumer 2 arm)

**Symptom:** A freshly constructed `AsyncKafkaConsumer` calling
`consumer.committed(&[tp])` immediately after `consumer.assign(...)`
surfaces `KafkaError::Generic(... error: NotCoordinator ...)` instead
of retrying coordinator lookup. Java's classic + KIP-848
`OffsetFetchRequestState` re-issues OffsetFetch after refreshing the
coordinator on `NotCoordinator`; the Rust translation did not.

**Root cause:** `fetch_offsets_with_retries` in
`src/consumer/internals/commit_request_manager.rs` was a mis-named
pass-through that did NOT retry — it just forwarded the single
OffsetFetch result to the caller.

**Java contract:** `KafkaConsumer.committed(...)` ultimately calls
`OffsetFetcherUtils.fetchCommittedOffsets(...)` →
`CommitRequestManager.fetchOffsets(...)` → an
`OffsetFetchRequestState` that re-enqueues on retriable errors
(`OffsetFetchRequestState.onResponse` retries `NotCoordinator`,
`CoordinatorLoadInProgress`, `CoordinatorNotAvailable`).

**Resolution:** Fixup commit `fixup! Phase 13a (1/N): Issue 1 —
fetch_offsets_with_retries retry loop` (against `972a053`).

Two-part fix:

1. **`fetch_offsets_with_retries` retry loop.** The driver now mirrors
   `commit_sync_with_retries`: on retriable errors (or
   `StaleMemberEpoch` with a valid member epoch — Java's
   `isStaleEpochErrorAndValidEpochAvailable`), advance the local
   `current_time_ms` by the configured `retry_backoff_ms`, check
   against `deadline_ms` (wrapping as `KafkaError::timeout` on expiry
   per Java's `maybeWrapAsTimeoutException`), and otherwise allocate a
   fresh `OffsetFetchRequestState` with continuity in the
   `RequestState.num_attempts` counter via a new
   `OffsetFetchRequestState::seed_failed_attempts` helper. The retry is
   pushed onto `unsent_offset_fetches` for the bg-task's next
   `poll_with_coordinator` to pick up.

2. **`mark_coordinator_unknown` wiring.**
   `CommitRequestManagerInner` grew a `Mutex<Option<Arc<CoordinatorRequestManager>>>`
   slot, set after construction via a new
   `CommitRequestManager::set_coordinator` method. The consumer's
   construction path (`async_kafka_consumer.rs`) wires the two
   managers up after both are built. Response handlers
   (`handle_offset_fetch_response`, `classify_and_complete_commit`)
   now call `coord.mark_coordinator_unknown(...)` on
   `NotCoordinator`/`CoordinatorNotAvailable` errors — exactly matching
   Java's `OffsetFetchRequestState.onFailure` and
   `OffsetCommitRequestState.onResponse`
   (`CommitRequestManager.java:804,1092`). The next bg-task
   `coord.poll_shared(now)` iteration then re-issues `FindCoordinator`,
   and the retry driver's next request lands on the freshly discovered
   coordinator.

---

## Issue 2: `commit_sync_offsets()` on a fresh consumer never recovers from `NotCoordinator`

**Affected Rust test:**
- `test_async_assign_and_consume_from_committed_offsets` (consumer 1 arm)

**Symptom:** Immediately after `consumer.assign(...)`,
`consumer.commit_sync_offsets(HashMap{tp: OffsetAndMetadata::new(10)})`
timed out at the full 60s `default.api.timeout.ms` budget.

**Resolution:** Closed by Issue 1's `mark_coordinator_unknown` wiring.
`commit_sync_with_retries` was already implementing the retry loop
correctly, but the response handler did not refresh the coordinator on
`NotCoordinator`/`CoordinatorNotAvailable` — so every retry hit the
same stale-coordinator broker. With `classify_and_complete_commit` now
calling `inner.mark_coordinator_unknown(...)`, the bg task's next
`poll_shared(now)` re-issues `FindCoordinator` and the next retry
attempt lands on the freshly discovered coordinator.

---

## Issue 3: `commit_sync()` after a successful poll cycle still times out

**Affected Rust test:**
- `test_async_assign_and_commit_sync_all_consumed`

**Symptom:** A consumer that just polled 10,000 records successfully
then timed out 60s on `commit_sync()`.

**Resolution:** Closed by the same fix as Issues 1/2. The previously
hypothesized "coordinator-channel liveness on assign-only flows" was
not the actual root cause — the broker the consumer initially routed
to for fetches was simply not the coordinator for offset commits, and
without coordinator refresh on NotCoordinator the commit retry loop
never converged. The retry driver was correct; only the trigger to
re-discover the coordinator was missing.

---

## Issue 4: `poll()` surfaces `NotCoordinator` as a fatal error

**Affected Rust tests:**
- `test_async_assign_and_consume`
- `test_async_assign_and_retrieving_committed_offsets_multiple_times`

**Symptom:** `consumer.poll(...)` returned
`Err(Generic(... error: NotCoordinator ...))`. The failing poll was
the first one after `assign(...)` on a fresh consumer with `group.id`
set. Without an explicit `seek(tp, offset)` before polling, the
consumer issued an `OffsetFetch` to resolve the starting position;
that OffsetFetch hit `NotCoordinator` and surfaced as a fatal `poll`
error.

**Resolution:** Closed by Issue 1's `fetch_offsets_with_retries` retry
loop. `OffsetsRequestManager::init_with_committed_offsets_if_needed`
calls `commit_rm.fetch_offsets(...)` to resolve initial positions; the
returned `oneshot::Receiver` now resolves with the retried value (or a
deadline-respecting `KafkaError::timeout`) instead of the first-attempt
error. Both affected tests now complete cleanly.

---

## Test-pool interference

The original note about sequential-run amplification of failures
vanished, as predicted. All 8 tests pass under one
`cargo test` invocation with `--test-threads=1`. The pool issue was
downstream of Issues 1/2/3/4 — leftover stalled-consumer state at
drop-time caused noise that is no longer present.

---

## Summary

One commit, one root cause, all four issues closed. The relevant
production-code diff is concentrated in:

- `src/consumer/internals/commit_request_manager.rs`
  - New `coordinator: Mutex<Option<Arc<CoordinatorRequestManager>>>`
    field on `CommitRequestManagerInner` + `CommitRequestManagerInner::mark_coordinator_unknown`
    helper.
  - New `CommitRequestManager::set_coordinator` setter.
  - `coordinator_node()` now returns the coordinator's currently known
    node when wired (used to be unconditionally `None`).
  - `handle_offset_fetch_response`: calls `mark_coordinator_unknown`
    on `NotCoordinator`/`CoordinatorNotAvailable` before completing
    the future exceptionally.
  - `classify_and_complete_commit`: now takes
    `&Arc<CommitRequestManagerInner>` instead of `&str` so it can
    invoke `mark_coordinator_unknown` on the same cases.
  - `OffsetFetchRequestState::seed_failed_attempts` — new helper
    mirroring `OffsetCommitRequestState::seed_failed_attempts`.
  - `fetch_offsets_with_retries` — rewritten as a real retry loop
    mirroring `commit_sync_with_retries`.
  - `current_time_ms_now` — new file-local helper (response handlers
    don't carry an injected `current_time_ms`).
- `src/consumer/async_kafka_consumer.rs`
  - Wiring call: `commit_arc.set_coordinator(Arc::clone(coord_arc))`
    after both managers are constructed.
- `tests/integration/plaintext_consumer_assign_test.rs`
  - 7 `#[ignore]` attributes removed.
  - Module-level rustdoc rewritten to reflect the closed gaps.

---

## Issue 7 — fetch_collector surfaces transient "No current assignment for partition" as fatal error during rebalance

Originally filed by: Manager during Phase 13a (3/N) — PlaintextConsumerSubscriptionTest.

**Affected Rust test:**
  - `tests/integration/plaintext_consumer_subscription_test.rs::test_async_consumer_re2j_pattern_expand_subscription`

**Symptom:** Test exercises `unsubscribe()` + `subscribe_pattern(broader_pattern)` to expand subscription. Mid-rebalance, `consumer.poll(100ms)` raised:

```
IllegalState("No current assignment for partition <topic1>-1")
```

**Java contract:** `pollForFetches()` does NOT raise on transient internal-state issues. A partition that becomes unassigned between `fetchablePartitions()` snapshot and the per-partition `position()` query — or that briefly has no position — is silently skipped. That partition produces no records this poll cycle; the next poll re-checks against a fresh snapshot.

**Root cause:** Two Rust call sites surfaced the `IllegalState` upward instead of skipping:

1. `src/consumer/internals/fetch_collector.rs:375-378` — the `FetchabilityCheck::MissingPosition` arm raised
   `KafkaError::illegal_state("Missing position for fetchable partition ...")`. This branch is "dead code"
   under Java's lock model because `isFetchable(tp) ⇒ hasValidPosition(tp)`. In Rust the same invariant
   holds *within a single lock guard*, but a `CompletedFetch` for a just-revoked partition can still land
   in the buffer between the bg task's prior fetchable snapshot and the collector's pass.
2. `src/consumer/internals/abstract_fetch.rs:529-540` — `prepare_fetch_requests` returned
   `Err(IllegalState("No current assignment for partition X"))` for a partition that became unassigned
   between the `fetchable_partitions()` snapshot (line 508-511) and the per-partition `position()` query.
   Java has the same window (Java methods are `synchronized` per call, not per scope) but the timing is
   tighter in Java's classic flow. The Rust KIP-848 bg-task interleaves application events between
   snapshot and query, so the race surfaces on every `unsubscribe + re-subscribe` rebalance.

The Phase 13a (2/N) fix to Issue 4 was correct for `OffsetOutOfRange` (real, surfaceable error) but
over-propagated these two transient signals.

**Fix landed:**

- `src/consumer/internals/fetch_collector.rs:375-403` — `FetchabilityCheck::MissingPosition` now drains
  the in-flight `CompletedFetch` and returns an empty `FetchPartitionOutcome` instead of `Err`. Mirrors
  the adjacent `NotAssigned` / `NotFetchable` arms.
- `src/consumer/internals/abstract_fetch.rs:526-571` — in `prepare_fetch_requests`, both `Ok(None)` (no
  position yet) and `Err(...)` (no current assignment) from `guard.position(&partition)` now `continue`
  to skip the partition, instead of returning `Err` for the whole batch.

Both call sites are commented with a reference to this Issue and a rationale block explaining the deviation
from Java's literal `throw IllegalStateException` behavior. The deviation is documented per CLAUDE.md §10
(deviations from Java need explicit rationale in source comments).

**Surfaceable errors preserved (regression-tested):**

- `OffsetOutOfRange` with no reset policy — still propagates (Issue 4 regression test
  `test_async_consumer_fetch_invalid_offset` passes).
- `TopicAuthorization` — still propagates
  (`test_fetch_with_topic_authorization_failed` passes).
- `CorruptMessage` — still propagates (`test_fetch_with_corrupt_message` passes).
- Unexpected error codes (catch-all `IllegalState` in `handle_initialize_errors`) — still propagate
  (`test_fetch_with_other_errors` passes).

**Validation:**

- `cargo test --lib fetch_collector` — 16/16 pass.
- `cargo test --lib` — 1700/1700 pass (no regression).
- Subscription suite: 9 pass + 2 ignored (Issue 6 still active).
- Fetch suite: 8 pass + 1 ignored (Issue 5 still active).
- Format-check + lint clean.

**Side effect on Issue 5:** This fix changes Issue 5's failure mode. Previously the
`by_duration:PT1H` test surfaced `IllegalState("Missing position for fetchable partition ...")` quickly.
With this fix, that error is now swallowed and the test will hang waiting for a position that never
materializes (until the test deadline). Issue 5 remains `#[ignore]`-gated; the underlying gap
(`by_duration` reset path is incomplete in `OffsetsRequestManager`) is unchanged.

**Files modified:**

- `src/consumer/internals/fetch_collector.rs` — `MissingPosition` arm now skips (24-line replacement).
- `src/consumer/internals/abstract_fetch.rs` — `prepare_fetch_requests` per-partition loop now skips
  `Ok(None)` and `Err(...)` from `position()` (43-line replacement with documentation).
- `tests/integration/plaintext_consumer_subscription_test.rs` — `#[ignore]` attribute removed from
  `test_async_consumer_re2j_pattern_expand_subscription`; module-level rustdoc updated (9 pass + 2
  ignored, was 8 pass + 3 ignored).
- `design/history/Milestone-8/Phase-13/COMMENTS.1.md` — Issue 7 block replaced with pointer to this entry.
