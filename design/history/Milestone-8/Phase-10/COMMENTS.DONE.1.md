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

---

## Round 2 — Phase 10

### Issue R2-5: `AsyncCommit` / `SyncCommit` empty-commit-manager branch eagerly fails `offsets_ready`

> - **File**:
>   `src/consumer/internals/events/application_event_processor.rs:716-719`
>   (and the symmetric `process_commit_sync` at `:769-771`).
> - **Commit**: `4c76cae` Phase 10 (5/N): async-dispatch arms
> - **Severity**: Behavior Mismatch (intentional deviation, but
>   worth verifying with a regression test)
> - **Java Reference**:
>   `ApplicationEventProcessor.java:244-260` (`process(AsyncCommitEvent)`)
>   and `:262-278` (`process(SyncCommitEvent)`). Both: empty-manager
>   branch only calls `event.future().completeExceptionally(...)`,
>   leaving `offsetsReady` un-completed; `AsyncKafkaConsumer.java:1050`
>   then waits on `offsetsReady` with `defaultApiTimeoutMs` and surfaces
>   a `TimeoutException`.
> - **Description**: The Rust arm completes BOTH handles
>   exceptionally (`offsets_ready.complete_exceptionally(err.clone());
>   handle.complete_exceptionally(err)`). The comment at `:706-715`
>   documents the deviation as "strictly faithful to Java's contract
>   (the primary handle's failure) while avoiding the app-side
>   blocking-then-timeout dance". This is plausibly an improvement
>   (immediate clear error vs delayed timeout), but the deviation
>   changes the observable error: Java callers see `TimeoutException`,
>   Rust callers see `IllegalStateException`. Phase 11's
>   `AsyncKafkaConsumer::commit_async` wiring will read whichever
>   handle resolves first — both fail now, so the error type the
>   caller observes depends on Phase-11 polling order.
> - **Expected**: Either (a) restore Java behaviour: only fail
>   `handle`, leave `offsets_ready` un-completed (Rust would still
>   hang the same way Java does if the app side awaits both); or
>   (b) keep the eager dual-fail and add a regression test that asserts
>   the contract — specifically that `commit_async_without_commit_manager_fails_with_illegal_state`
>   observes IllegalState on `offsets_ready` too. Today's test only
>   awaits `handle.receiver()` and does not pin the secondary handle's
>   state — a future change could revert to Java behaviour silently.
>   Without a test, the deviation is invisible to subsequent reviewers.
> - **Actual**: Both handles fail with `IllegalStateException`; no
>   test pins this contract.

**Resolution**: Restored Java behavior (option (a)). The empty-manager
branches in both `process_commit_async` and `process_commit_sync` now
only fail the primary `handle` and leave `offsets_ready` un-completed —
matching `ApplicationEventProcessor.java:244-278`. Both existing
regression tests (`commit_async_without_commit_manager_fails_with_illegal_state`
and `commit_sync_without_commit_manager_fails_with_illegal_state`) were
extended to pin the secondary-handle contract: a snapshot
`erased()` of `offsets_ready` taken before the variant moves it lets us
assert `!is_done()` on the inner slot, and `ready_rx.try_recv()` must
observe `TryRecvError::Empty`. This catches any future silent
revert to eager dual-fail.

### Issue R2-1: `run_once` Phase-3 placement: `membership.reconcile()` runs AFTER `offsets`/`fetch` poll

> - **File**: `src/consumer/internals/consumer_network_thread.rs:367-381`
> - **Commit**: `e86f7c1` Phase 10 (7/N): ConsumerNetworkThread runOnce
> - **Severity**: Behavior Mismatch
> - **Java Reference**: `ConsumerNetworkThread.java:222-226` (the
>   `for (RequestManager rm : requestManagers.entries())` loop) and
>   `RequestManagers.java:91-101` (entries order:
>   `coordinator → commit → heartbeat → membership → offsets →
>    topicMetadata → fetch`). `AbstractMembershipManager.java:1409`
>   shows `poll(now)` calls `maybeReconcile(false)`.
> - **Description**: The closure plan / round-1 aside required commit 7
>   to "explicitly call `membership.reconcile()` per iteration". The
>   call is now present, but it sits as Phase 3 — AFTER all entries are
>   polled (which already includes `offsets`, `topicMetadata`, `fetch`)
>   and BEFORE the network-client poll. Java's order places membership
>   BETWEEN `heartbeat` and `offsets` inside the same per-entry loop:
>   reconcile may transition the assignment (e.g. acknowledging a new
>   target), and Java's `offsets.poll()` / `fetch.poll()` then run with
>   the post-reconcile subscription state in the SAME iteration. Rust's
>   ordering means within a given `run_once`, offsets/fetch poll against
>   the **pre-reconcile** state; their visible effect of any reconcile
>   is delayed by one iteration.
> - **Expected**: Insert the `membership.reconcile().await` between the
>   heartbeat-poll step and the offsets-poll step of the Phase-2 entries
>   loop, mirroring Java's positional invariant. Concretely: split the
>   Phase-2 `rm_guard.entries()` walk into "managers before membership"
>   and "managers after membership", drive reconcile between them.
> - **Actual**: All entries (incl. offsets/fetch) poll first, then
>   membership reconciles, then network polls.

**Resolution**: Split the bg task's Phase-2 walk into "before-membership"
and "after-membership" halves, calling `membership.reconcile(now, false)`
between them. The boundary is provided by a new
`RequestManagers::membership_boundary()` accessor that returns the
position membership would occupy in Java's `entries()`:
`coordinator + commit + consumer_heartbeat` slot count. The PollResult
vec collected from `entries()` is now `split_off(boundary)`-ed; the
front half (coordinator/commit/heartbeat) feeds `add_all` first, then
reconcile runs, then the tail (offsets/topic_metadata/fetch and any
`dyn_managers`) feeds `add_all`. This preserves Java's positional
invariant: any subscription-state change from reconcile is visible to
the after-membership manager polls inside the SAME run-once iteration.
The existing `run_once_invokes_membership_reconcile` test continues to
cover dispatch; the ordering itself is enforced by construction
(`split_off(boundary)`).

### Issue R2-2: `reconcile` ignores Java's `canCommit` gate

> - **File**: `src/consumer/internals/consumer_membership_manager.rs:471`
>   (signature) + callers
>   - `src/consumer/internals/consumer_network_thread.rs:378`
>     (`membership.reconcile(current_time_ms).await` from `run_once`)
>   - `src/consumer/internals/events/application_event_processor.rs:1185`
>     (`mm.reconcile(poll_time_ms).await` from `process_async_poll`)
> - **Commit**: `e86f7c1` (7/N) + `4c76cae` (5/N)
> - **Severity**: Behavior Mismatch
> - **Java Reference**: `AbstractMembershipManager.java:824-854`
>   (`maybeReconcile(boolean canCommit)` — `if (autoCommitEnabled &&
>   !canCommit) return;`). Java's `entries().poll()` path passes
>   `false`; `process(AsyncPollEvent)` passes `true`.
> - **Description**: Rust's `ConsumerMembershipManager::reconcile(now)`
>   takes no `can_commit` parameter and always proceeds. Java skips the
>   reconciliation entirely when `autoCommitEnabled && !canCommit` —
>   i.e. the per-iteration `entries().poll()` call MUST NOT advance
>   reconciliation when auto-commit is enabled; only the AsyncPoll
>   path (which has just run `updateTimerAndMaybeCommit`) advances. The
>   Rust collapse means the per-iteration call from `run_once` can
>   advance reconciliation with un-committed offsets — exactly the
>   scenario the `canCommit` gate exists to prevent.
> - **Expected**: Pass a `can_commit: bool` parameter through to
>   `reconcile`; the `run_once` caller passes `false`, the `AsyncPoll`
>   caller passes `true`.
> - **Actual**: Both call sites use the same `reconcile(now)` and the
>   gate is missing.

**Resolution**: Added the `can_commit: bool` parameter to
`ConsumerMembershipManager::reconcile`. The gate
`if auto_commit_enabled && !can_commit { return Ok(()); }` lives
between step 4 (short-circuit ACK) and step 6 (mark-in-progress),
matching the position of Java's `AbstractMembershipManager.java:854`.
Updated call sites: `ConsumerNetworkThread::run_once` passes `false`
(per-iteration path); `ApplicationEventProcessor::process_async_poll`
passes `true` (post-`updateTimerAndMaybeCommit` path). Test-only
`reconcile(0, ...)` call sites in the membership manager's own tests
pass `true` (they pre-date the gate and exercise the full-reconcile
path with `commit_request_manager=None`, so the gate is inert
regardless of the bool). Two new regression tests
(`reconcile_can_commit_false_is_noop_when_auto_commit_enabled` and
`reconcile_can_commit_true_proceeds_when_auto_commit_enabled`)
construct a manager with a real `CommitRequestManager` and
`auto_commit_enabled=true`, then exercise both `can_commit` branches:
the `false` arm must NOT emit a rebalance-listener event and must
leave state in `Reconciling`; the `true` arm DOES emit and advances
to `Acknowledging`.

### Issue R2-3: `update_fetch_positions` caches synchronous `validate_positions_if_needed` errors that Java never caches

> - **File**: `src/consumer/internals/offsets_request_manager.rs:601-616`
>   (the outer `match` in `update_fetch_positions`) and
>   `:639-647` (the validate Err branch in
>   `update_fetch_positions_inner`).
> - **Commit**: `2b3d050` Phase 10 (3b/N): `update_fetch_positions`
> - **Severity**: Bug
> - **Java Reference**: `OffsetsRequestManager.java:235-264` (the outer
>   `updateFetchPositions`'s `try/catch (Exception e) {
>   result.completeExceptionally(maybeWrapAsKafkaException(e)); }`)
>   combined with `:280-306` (`cacheExceptionIfEventExpired(result,
>   deadlineMs)` is registered ONLY in `updatePositionsWithOffsets`,
>   not in the outer `updateFetchPositions`).
> - **Description**: Java caches an error via
>   `cachedUpdatePositionsException.set(error)` only on the OUTER
>   result of `updatePositionsWithOffsets`, NOT on the outer
>   `updateFetchPositions` result. So if `validatePositionsIfNeeded()`
>   throws (e.g. a cached `LogTruncationException` from a previous
>   validate response), Java's `catch` block completes the result
>   exceptionally and never registers a `whenComplete` hook —
>   **no caching happens**. Rust's `update_fetch_positions` outer
>   match calls `maybe_cache_update_positions_exception(&err, …)` on
>   every `Err((tx, err))` return from `_inner` — INCLUDING the
>   `validate_positions_if_needed` Err arm at `:639-641`. Result: a
>   validate-thrown `LogTruncationException` at-or-past-deadline gets
>   delivered to the caller in this call AND cached, then re-delivered
>   to the NEXT call via `take_cached_update_positions_exception` —
>   the user observes the same error twice.
> - **Expected**: Move the caching call into the spawned-followup path
>   only (the equivalent of Java's `updatePositionsWithOffsets`-internal
>   hook). The synchronous validate Err must NOT cache.
> - **Actual**: Validate-thrown errors with `current_time_ms >=
>   deadline_ms` are cached → double-delivery on the next call.

**Resolution**: Removed the `maybe_cache_update_positions_exception`
call from the outer `match` Err arm in
`OffsetsRequestManager::update_fetch_positions`. The synchronous error
path now just sends the error on the outer `tx` and returns —
matching Java's outer `catch (Exception e) {
result.completeExceptionally(maybeWrapAsKafkaException(e)); }`. The
spawned committed-offset followup retains its own
`cacheExceptionIfEventExpired`-equivalent block (inline, not via the
now-removed helper), preserving Java's
`updatePositionsWithOffsets`-internal caching for the genuinely
asynchronous error path. Deleted the now-dead
`maybe_cache_update_positions_exception` helper in favor of inline
caching in the spawned task. Regression test
`update_fetch_positions_does_not_cache_synchronous_validate_errors`:
pre-seeds a validate error via
`OffsetFetcherUtilsState::maybe_set_validate_error`, calls
`update_fetch_positions` with `current_time_ms == deadline_ms` (the
exact condition that previously triggered the bug), asserts the error
propagates AND `cached_update_positions_exception` stays empty.

### Issue R2-4: `OffsetsRequestManager::fetch_offsets` never clears the transient-topic registration

> - **File**: `src/consumer/internals/offsets_request_manager.rs:718-752`
>   (no `metadata.clear_transient_topics()` call), plus the
>   inline comment at `:1429-1442` acknowledging the deviation.
> - **Commit**: `86e698d` Phase 10 (3c/N): `fetch_offsets`
> - **Severity**: Behavior Mismatch
> - **Java Reference**: `OffsetsRequestManager.java:200-209` —
>   `listOffsetsRequestState.globalResult.whenComplete((result, error)
>   -> { metadata.clearTransientTopics(); … });`.
> - **Description**: Java registers a `whenComplete` on the global
>   result that calls `metadata.clearTransientTopics()` on completion
>   (success OR error). Rust skipped this entirely; topics
>   accumulated across the lifetime of the consumer (every
>   `list_offsets` / `current_lag` /
>   `init_with_committed_offsets_if_needed` call that touched a topic
>   not in the subscription set added it to `transientTopics`
>   forever). Long-lived consumers issuing periodic `endOffsets`
>   (current-lag) for ad-hoc partitions grow the topic set on every
>   metadata request — both wire-size and broker-side filtering cost
>   scale with this.
> - **Expected**: Wire the clear into the global-result completion
>   path — either inside `OffsetsManagerShared::apply_partial_result`
>   on the final-response branch (after waiters are routed) and in
>   `OffsetsManagerShared::fail_request_state`, or via a separate
>   `on_completed` arm. Java's hook fires on both success and failure.
> - **Actual**: Transient topics accumulate without bound across
>   `fetch_offsets` calls.

**Resolution**: Wired `metadata.clear_transient_topics()` into both
global-result completion paths inside `OffsetsManagerShared`. The
success branch in `apply_partial_result` fires the clear AFTER waiters
have been routed (mirroring Java's `whenComplete` running after the
result completes). `fail_request_state` gained a `self_arc: &Arc<Self>`
parameter (it was previously a free `fn` taking just the state) so it
can access `self_arc.metadata`; the clear fires after waiters have
been failed. The two existing callers of `fail_request_state`
(topic-authorization branch in `handle_fetch_offsets_response` and the
transport-error branch in the same function) were updated to pass the
`&self.shared`. The retry branch (parked on `requests_to_retry`) does
NOT fire the clear because Java's `whenComplete` only runs on global
completion — that fires later when the retry resolves. The stale
inline comment at the bottom of `poll()` that rationalized "deferring
the clear is benign" is replaced with a one-line pointer to the new
completion-path hooks. Added
`ConsumerMetadata::transient_topics_snapshot_for_test` (`#[cfg(test)]`)
so tests can observe the set. Two regression tests:
`fetch_offsets_clears_transient_topics_on_success` exercises the
success path; `fetch_offsets_clears_transient_topics_on_failure`
exercises the topic-authorization failure path. Both assert the
topic is in the transient set BEFORE the response completes and
absent AFTER.
