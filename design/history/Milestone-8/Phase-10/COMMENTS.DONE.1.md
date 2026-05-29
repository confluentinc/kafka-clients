# Phase 10 COMMENTS — Resolved (N=1)

Scope: Critic round-1 findings against commits `66948a0` (1/N),
`a0e3c19` (2/N), `4b9d99d` (2.5/N). All five items in
`COMMENTS.1.md` are resolved in this batch.

---

## 1. `maybe_auto_commit_async` is still a stub — wire-prereq #6 is NOT actually closed

**Resolution**: real bug, now fixed. Translated Java's `maybeAutoCommitAsync`
+ `requestAutoCommit` + `maybeResetTimerWithBackoff` lifecycle:
`subscriptions.allConsumed()` is snapshotted (using the
`Arc<Mutex<SubscriptionState>>` plumbed in commit 2.5); on an empty snapshot
the timer is still reset (Java unconditionally calls `resetAutoCommitTimer()`)
but no request is enqueued and the inflight flag stays clear; on a non-empty
snapshot an `OffsetCommitRequestState` is enqueued on
`pending.unsent_offset_commits` with `deadline_ms = i64::MAX`, the inflight
flag is raised, and a spawned task awaits the completion to clear the flag
and (on retriable failure) call `reset_timer_with_backoff(retry_backoff_ms)`.
Test `update_timer_and_maybe_commit_fires_when_timer_expired` rewritten as
an async test that seeds `subscriptions.allConsumed()` with a partition,
asserts the request lands in `unsent_offset_commits`, asserts the inflight
flag flips up while outstanding, and drives a success response to assert the
flag clears.

- **File**: `src/consumer/internals/commit_request_manager.rs`
- **Original full text** (preserved verbatim from `COMMENTS.1.md`):

> ## 1. `maybe_auto_commit_async` is still a stub — wire-prereq #6 is NOT actually closed
>
> - **File**: `src/consumer/internals/commit_request_manager.rs:1031-1057`
> - **Severity**: Bug / Missing Requirement (blocker for wire-prereq #6)
> - **Java Reference**: `CommitRequestManager.java:277-289`
>   (`maybeAutoCommitAsync()`)
>
> ### Description
>
> Commit 2/N's docstring on `update_timer_and_maybe_commit`
> (`commit_request_manager.rs:515-519`) says:
>
> > "in the Rust translation the timer-refresh step is implicit ... This
> > method is therefore a thin pass-through to the existing
> > `maybe_auto_commit_async` driver."
>
> …and the commit message claims wire-prereq #6 is closed. The hook is
> indeed thin — `update_timer_and_maybe_commit` just calls
> `maybe_auto_commit_async`. But the **underlying driver
> `maybe_auto_commit_async` is still the Phase-9 stub** — it flips the
> inflight flag, resets the timer, then flips the flag back, **without ever
> snapshotting `subscriptions.allConsumed()` or enqueueing an
> `OffsetCommitRequestState`**.
>
> The inline comment confirms this:
>
> ```rust
> // Java: auto-commit pulls from `subscriptions.allConsumed()`. We
> // don't have `SubscriptionState` plumbed in for Phase 9 yet (the
> // bg task wiring lands in Phase 10), so emit an empty-offsets
> // sentinel that resolves immediately. Phase 10 will replace this
> // with the actual `subscriptions.allConsumed()` snapshot.
> // No outbound request — just flip the flag back so the next
> // interval can fire.
> ```
>
> Commit 2.5/N **did plumb `subscriptions: Arc<Mutex<SubscriptionState>>`
> into `CommitRequestManagerInner`**, so the cited blocker no longer
> applies. But `maybe_auto_commit_async` was not updated.
>
> This means:
>
> - The processor's `update_timer_and_maybe_commit` call site
>   (`AsyncPoll` / `AssignmentChange` events, per the docstring at
>   `commit_request_manager.rs:512-514`) will fire the hook on schedule
>   but no `OffsetCommit` request is ever generated.
> - The bg-task's `poll_with_coordinator` path also calls
>   `maybe_auto_commit_async(current_time_ms)`
>   (`commit_request_manager.rs:967`); the same gap applies. So
>   auto-commit-on-interval is broken end-to-end.
>
> ### Expected
>
> Translate the body of Java's `maybeAutoCommitAsync`:
>
> 1. Snapshot `subscriptions.allConsumed()` (already accessible via
>    `self.inner.subscriptions`).
> 2. If empty, short-circuit per Java's `requestAutoCommit`.
> 3. Otherwise, build an `OffsetCommitRequestState` with
>    `deadline_ms = i64::MAX` (Java uses `Long.MAX_VALUE` for auto-commit).
> 4. Enqueue it on `pending.unsent_offset_commits`.
> 5. On completion (success or retriable error), invoke the equivalent of
>    Java's `maybeResetTimerWithBackoff` — on `RetriableCommitFailedError`
>    reset timer with `retry_backoff_ms`.
>
> ### Test rigor implication
>
> The test `update_timer_and_maybe_commit_fires_when_timer_expired`
> (`commit_request_manager.rs:2009-2020`) only asserts the timer reset; it
> does NOT check that an `OffsetCommitRequestState` is enqueued. The test
> therefore passes despite the gap above. A regression test should:
>
> - assert `pending.unsent_offset_commits` length grows when
>   `subscriptions.allConsumed()` is non-empty;
> - assert the request resolves the auto-commit `inflightCommitStatus`
>   flag correctly on response.

---

## 2. `auto_commit_sync_before_rebalance_with_retries` drops the `memberEpoch.isPresent()` guard from `isStaleEpochErrorAndValidEpochAvailable`

**Resolution**: real bug, now fixed. Captured
`member_info.member_epoch.is_some()` once at driver entry and added it as
a precondition for admitting `StaleMemberEpoch` to the retry gate, matching
Java's `isStaleEpochErrorAndValidEpochAvailable`. New regression test
`auto_commit_sync_before_rebalance_surfaces_stale_epoch_when_no_valid_epoch`
drives a `StaleMemberEpoch` failure with the default (`None`) member epoch
and asserts the future resolves with `StaleMemberEpoch`, not `Timeout`.

- **File**: `src/consumer/internals/commit_request_manager.rs`
- **Original full text** (preserved verbatim from `COMMENTS.1.md`):

> ## 2. `auto_commit_sync_before_rebalance_with_retries` drops the `memberEpoch.isPresent()` guard from `isStaleEpochErrorAndValidEpochAvailable`
>
> - **File**: `src/consumer/internals/commit_request_manager.rs:1651`
> - **Severity**: Behavior Mismatch (defect)
> - **Java Reference**: `CommitRequestManager.java:573-575`
>   (`isStaleEpochErrorAndValidEpochAvailable`), invoked at
>   `CommitRequestManager.java:349`.
>
> ### Description
>
> Java's predicate is
>
> ```java
> private boolean isStaleEpochErrorAndValidEpochAvailable(Throwable error) {
>     return error instanceof StaleMemberEpochException
>         && memberInfo.memberEpoch.isPresent();
> }
> ```
>
> …and Java's retry gate at line 349 is
>
> ```java
> if (error instanceof RetriableException
>     || isStaleEpochErrorAndValidEpochAvailable(error)) { ... }
> ```
>
> Rust collapses this to
>
> ```rust
> let is_retriable_for_rebalance =
>     err.is_retriable() || err.error() == Errors::StaleMemberEpoch;
> ```
>
> …**omitting** the `memberInfo.memberEpoch.isPresent()` arm. Consequence:
> if the consumer has already left the group (so `memberInfo.member_epoch
> == None`) and the broker still returns `StaleMemberEpoch`, Java surfaces
> the error to the caller as non-retriable, while Rust enters the retry
> loop. This will loop until the deadline expires and then surface a
> `Timeout` instead of the original `StaleMemberEpoch`. The user-observable
> contract differs.
>
> The same predicate is used in Java's `fetchOffsetsWithRetries`
> (`CommitRequestManager.java:559`); the Rust `fetch_offsets_with_retries`
> does not retry at all (it propagates errors directly), so that path is
> not affected.
>
> ### Expected
>
> Translate the full predicate:
>
> ```rust
> let is_retriable_for_rebalance = err.is_retriable() || {
>     let guard = inner.state.lock().expect(...);
>     err.error() == Errors::StaleMemberEpoch
>         && guard.member_info.member_epoch.is_some()
> };
> ```
>
> (or capture `member_info.member_epoch.is_some()` once at driver entry
> and reuse it on each retry, since the driver already keeps a
> `member_info` clone in scope).
>
> ### Test rigor implication
>
> No test exercises this path. A unit test driving
> `maybe_auto_commit_sync_before_rebalance` with a `StaleMemberEpoch`
> failure and `member_info.member_epoch == None` should assert the future
> surfaces `StaleMemberEpoch` (or its mapped form), not `Timeout`.

---

## 3. `auto_commit_sync_before_rebalance_with_retries` swaps the deadline-vs-UnknownTopicOrPartition check order

**Resolution**: real bug, now fixed. Reordered the retry gate to match
Java's `CommitRequestManager.java:350-368`: the deadline check (advance
local clock by `retry_backoff_ms`, then compare with `deadline_ms`) now
runs BEFORE the `UnknownTopicOrPartition` fatal-error short-circuit. New
regression test
`auto_commit_sync_before_rebalance_timeout_wins_over_unknown_topic_or_partition`
constructs a scenario where both conditions hold and asserts the surfaced
error is `KafkaError::Timeout`, not the raw `UnknownTopicOrPartition`.

- **File**: `src/consumer/internals/commit_request_manager.rs`
- **Original full text** (preserved verbatim from `COMMENTS.1.md`):

> ## 3. `auto_commit_sync_before_rebalance_with_retries` swaps the deadline-vs-UnknownTopicOrPartition check order
>
> - **File**: `src/consumer/internals/commit_request_manager.rs:1659-1668`
> - **Severity**: Behavior Mismatch (defect, edge case)
> - **Java Reference**: `CommitRequestManager.java:350-368`
>
> ### Description
>
> Java's order inside the retry gate is:
>
> 1. `if (requestAttempt.isExpired()) → wrap as TimeoutException`
> 2. `else if (error instanceof UnknownTopicOrPartitionException) → fatal,
>    surface the original error`
> 3. `else → retry`
>
> Rust's order is:
>
> 1. `if err.error() == UnknownTopicOrPartition → fatal`
> 2. `// advance time, then`
> 3. `if current_time_ms >= deadline_ms → Timeout`
> 4. `else → retry`
>
> The deviation matters when **both** conditions can be true:
> `UnknownTopicOrPartition` AND the deadline is already past. Java
> surfaces a wrapped `TimeoutException` carrying the
> `UnknownTopicOrPartition` as cause. Rust surfaces
> `UnknownTopicOrPartition` directly. A caller doing
> `is_retriable()`-based decision after the fact could legitimately get a
> different answer (Java's `Timeout` is also non-retriable but the error
> type is distinct).
>
> This is rarer than #2 since the rebalance flush is typically given a
> deadline far in the future. Still a real behavior deviation worth
> fixing for parity.
>
> ### Expected
>
> Reorder the Rust block to mirror Java:
>
> ```rust
> if !is_retriable_for_rebalance { ... }
> // advance time once for the retry-tick semantics
> let backoff = inner.retry_backoff_ms.max(0);
> current_time_ms = current_time_ms.saturating_add(backoff);
> if current_time_ms >= deadline_ms { break Err(KafkaError::timeout(...)); }
> if err.error() == Errors::UnknownTopicOrPartition { break Err(err); }
> // else retry
> ```
>
> …or alternatively check `is_expired` of the request state, not the
> local-clock advance — which is what Java does (it queries the
> `requestAttempt` timer, not a local advancing counter).

---

## 4. `entries_returns_managers_in_registration_order` test name overstates its assertion

**Resolution**: doc/test rigor issue, now fixed. Renamed to
`entries_returns_correct_count_when_five_slots_populated` and updated the
docstring to no longer claim order verification. The test was always a
count-shape test (this is what the body asserts); the rename brings the
contract in line with the assertion. Order verification is left to the
existing pair-wise `entries_includes_*_when_present` tests in this module
which are not subject to the "default trait method" identity-blocker.

- **File**: `src/consumer/internals/request_managers.rs`
- **Original full text** (preserved verbatim from `COMMENTS.1.md`):

> ## 4. `entries_returns_managers_in_registration_order` test name overstates its assertion
>
> - **File**: `src/consumer/internals/request_managers.rs:424-449`
> - **Severity**: Nit (test rigor)
> - **Java Reference**: N/A (Rust-only `entries()` helper); shape mirrors
>   `RequestManagers.java:91-101`.
>
> ### Description
>
> The test name is `entries_returns_managers_in_registration_order` but
> the body only asserts `entries.len() == 5`. No per-slot identity check.
> The docstring on the test (lines 418-423) acknowledges this:
>
> > "A per-slot identity assertion is unnecessary given the
> > destructure-driven implementation is a straight-line list of `if let
> > Some(...) list.push(...)` calls in the documented order."
>
> This is a weak claim — it's a behavioral contract being protected by
> "read the source". A regression that swaps `if let Some(o) =
> offsets.as_mut() { list.push(...) }` with the
> `topic_metadata` push order would still pass this test. Per DoD §3,
> assertions should be meaningful enough to fail on regression.
>
> ### Expected
>
> Either rename the test (`entries_returns_correct_count_when_five_slots_populated`)
> or actually assert order. Since `&mut dyn RequestManager` doesn't carry
> type identity, one approach is to give each manager a distinguishable
> `maximum_time_to_wait` return (e.g. with carefully-chosen distinct
> auto-commit intervals / coordinator backoffs) and check the vec values
> in order. Another is to expose a tiny `pub(crate) fn debug_name(&self)
> -> &'static str` test hook on `RequestManager`.
>
> The current test is otherwise correct but the name promises more than
> it delivers.

---

## 5. Stale comment in `maybe_auto_commit_async` — references "Phase 10 will replace this" but Phase 10 is current

**Resolution**: documentation hygiene, removed as part of fixing #1.
The "Phase 10 will replace this" comment block was the symptom of the
stub implementation; the rewrite in finding #1 replaces it with comments
that describe the actual translated behavior (Java references inline).

- **File**: `src/consumer/internals/commit_request_manager.rs`
- **Original full text** (preserved verbatim from `COMMENTS.1.md`):

> ## 5. Stale comment in `maybe_auto_commit_async` — references "Phase 10 will replace this" but Phase 10 is current
>
> - **File**: `src/consumer/internals/commit_request_manager.rs:1047-1056`
> - **Severity**: Nit (documentation hygiene)
>
> The inline comment claims "Phase 10 will replace this with the actual
> `subscriptions.allConsumed()` snapshot." Phase 10 IS this phase, and
> commit 2.5/N landed the plumbing. The comment is the symptom of the
> actual #1 bug above — once #1 is fixed the comment goes away.
