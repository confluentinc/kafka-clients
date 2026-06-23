# Critic 37 — Phase 37 resolved issues (Actor 37)

Resolved by Actor 37 in fixup commits referencing Phase 37. Build / `cargo test
--lib` / `cargo xtask lint` / `cargo xtask format-check` all green. The new
exclusion/discard/pause/seek tests are mutation-resistant (each was verified to
FAIL when the behavior-under-test is disabled).

---

## Issue 4 (RESOLVED) — `testFetchResultNotProcessedForPartitionsAwaitingCallbackCompletion` translated

- **Java Reference**: `FetchRequestManagerTest.java:323-341`.
- **Fix**: Added `test_fetch_result_not_processed_for_partitions_awaiting_callback_completion`
  to `mod round_trip`, plus an `assert_non_empty_fetch` helper mirroring Java's
  `assertNonEmptyFetch` (build → deliver 3 records → collect → assert position
  is 4). The test fetches tp0 successfully, marks it pending-on-assigned-callback
  (asserts NO fetch request is issued AND NO completed fetch is produced), then
  enables the callback and asserts fetching resumes.
- **Inflight variant**: the PLAN's "+ Inflight variant" refers to
  `testInflightFetchOnPendingPartitions` (java:283), which was ALREADY
  translated as `test_inflight_fetch_on_pending_partitions`. No additional
  variant exists in Java.
- **Mutation check**: skipping `mark_pending_on_assigned_callback` makes a fetch
  fire while "awaiting" → the `built.is_empty()` assertion fails. Confirmed.

## Issue 5 (RESOLVED) — pause/seek family (5 tests) translated

- **Java Reference**: `FetchRequestManagerTest.java`
  `testInFlightFetchOnPausedPartition` (1370),
  `testFetchOnCompletedFetchesForSomePausedPartitions` (1432),
  `testPartialFetchWithPausedPartitions` (1495),
  `testFetchDiscardedAfterPausedPartitionResumedAndSeekedToNewOffset` (1533),
  `testSeekBeforeException` (1841).
- **Fix**: Added all five to `mod round_trip`:
  - `test_in_flight_fetch_on_paused_partition` — fetch issued, partition paused
    before the response, deliver+collect returns no records.
  - `test_fetch_on_completed_fetches_for_some_paused_partitions` — tp0+tp1 on
    two nodes; pause tp0; only tp1 records returned; tp0 still buffered +
    completed fetches retained.
  - `test_partial_fetch_with_paused_partitions` — maxPollRecords=2, 3-record
    fetch partially collected (2), pause (asserts `has_completed_fetches` +
    `!has_available_fetches`), resume, last record returned, buffer drained.
  - `test_fetch_discarded_after_paused_partition_resumed_and_seeked_to_new_offset`
    — pause, deliver, re-seek to a new offset, resume, collect discards the
    buffered fetch (no records), buffer emptied.
  - `test_seek_before_exception` — tp0 returns 4 records collected 2-at-a-time;
    tp1 added, returns OFFSET_OUT_OF_RANGE; a seek on tp1 before collecting
    suppresses the OOR error (no error, no tp1 records).
- New harness helpers: `has_available_fetches`, `seek_unvalidated`,
  `mark_pending_on_assigned_callback`, `enable_partitions_awaiting_callback`,
  `mark_pending_revocation`.
- **Mutation checks**: each test FAILS when its pause / re-seek / final-seek is
  removed. Confirmed for all five.

## Issue 6 (RESOLVED) — `testFetchRequestWithBufferedPartitionPendingRevocation` translated

- **Java Reference**: `FetchRequestManagerTest.java:3760`.
- **Fix**: Added `test_fetch_request_with_buffered_partition_pending_revocation`:
  buffer tp0+tp1, collect tp0, mark tp1 pending-revocation (asserts tp1 is still
  buffered but no longer fetchable), assert the next build fetches only tp0.
  PLAN's enumerated buffered-partition list updated to include
  `PendingRevocation`.
- **Mutation check**: skipping `mark_pending_revocation` makes the next build
  include tp1 → `assert_next_build_fetches(&[tp(0)])` fails. Confirmed.

## Issue 7 (RESOLVED) — `test_fetch_request_with_buffered_partition_missing_position` now tests the real scenario

- **Java Reference**: `FetchRequestManagerTest.java:3697-3752`.
- **Root**: the old test reused the reset-offset mutation
  (`request_offset_reset_default`) and asserted no error — it did not reproduce
  Java's genuinely-null-position scenario.
- **Fix**: The test now overwrites tp1's position with `null` (new
  `#[cfg(test)] SubscriptionState::clear_position_for_test`, exposed on the
  harness as `clear_position`, mirroring Java's `subscriptions.position(tp1,
  null)`), keeping tp1 assigned and still buffered. It asserts: tp1's position
  reads back as null; tp1 is still `is_fetchable` (its `fetch_state` is still
  FETCHING — exactly like Java's fetch-state-based `hasValidPosition()`, which
  is what makes the partition pass the fetchable filter yet carry a null
  position); and the next build fetches only tp0.
- **Deliberate divergence from Java (documented at the test site + this entry).**
  Java's `createFetchRequests` future THROWS `IllegalStateException` (via
  `positionForPartition`, `AbstractFetch.java:508-515`). The Rust build loop
  instead `continue`s on an `Ok(None)` position — this is the **intentional,
  regression-tested Phase-13 fix** to `Phase-13/COMMENTS.DONE.1.md` Issue 7:
  re-raising `IllegalState` on a missing position over-propagated a transient
  rebalance-window race (the KIP-848 bg-task interleaves application events
  between the `fetchable_partitions()` snapshot and the per-partition
  `position()` query), regression-tested by
  `test_async_consumer_re2j_pattern_expand_subscription`. Re-raising
  `IllegalState` here (Critic option (a)) would re-break that test, so the test
  takes Critic option (b): reproduce Java's exact mutation and assert the Rust
  silent-skip, with the divergence documented in the test rustdoc. The
  reset-offset test (`..._reset_offset`) remains distinct.
- **Mutation check**: skipping the `clear_position` call makes tp1 fetchable
  with a valid position → the next build includes tp1 → the
  `assert_next_build_fetches(&[tp(0)])` assertion fails. Confirmed.

## Issue 8 (RESOLVED — documented skip) — abort-marker transaction tests

- **Java Reference**: `testMultipleAbortMarkers` (2443),
  `testReadCommittedAbortMarkerWithNoData` (2492),
  `testReadCommittedWithCommittedAndAbortedTransactions` (2367).
- **Fix**: Added an explicit DOCUMENTED-SKIP block at the test site in
  `fetch_request_manager.rs` (just above `test_read_committed_with_compacted_topic`)
  referencing the ControlRecordType limitation and COMMENTS.37 Issue 2, and
  updated `PLAN.md` to move these three from "translated 7b" into a clearly
  marked "DEFERRED to control-record production follow-up" section. They remain
  omitted until the control-record fix (Issue 2) lands. Consistent with the
  inline note already on
  `test_consumer_position_updated_when_skipping_aborted_transactions`.

## Issue 9 (RESOLVED) — no-new-leader test now asserts preferred-replica clearing

- **Java Reference**: `FetchRequestManagerTest.java:3190-3275`.
- **Fix**: `test_leadership_change_error_but_no_new_leader_information` rewritten
  to faithfully translate the full Java scenario: tp0+tp1 on two nodes, each
  given a preferred-read-replica (node 0) via an initial successful fetch +
  collect, then tp0 hit with a leadership error carrying NO new leader info.
  After collecting the error response it now asserts (per error code):
  - metadata's leader+epoch for tp0 is UNCHANGED (no node 999),
  - `metadata.update_requested()` is true (leadership error on tp0),
  - the preferred-read-replica is CLEARED for the errored tp0 only,
  - tp1's preferred-read-replica is still node 0,
  - both partitions remain fetchable.
- **Mutation check**: flipping tp0's second-response error code to `None`
  (so the preferred replica would not be cleared) makes the
  "tp0 preferred replica cleared" assertion fail. Confirmed.
