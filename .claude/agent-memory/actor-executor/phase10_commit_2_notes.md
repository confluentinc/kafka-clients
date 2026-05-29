---
name: phase10-commit-2-notes
description: Milestone-8 Phase 10 (2/N) wire-prereq #6/#8/#9 patterns; non-reentrant Mutex deadlock in build_offset_commit_unsent_request; Java framing mismatch for commit_sync timeout
metadata:
  type: project
---

Milestone-8 Phase 10 (2/N) closed wire-prereqs #6, #8, #9 in
`src/consumer/internals/commit_request_manager.rs`. Three non-obvious
findings worth keeping:

## 1. Non-reentrant `std::sync::Mutex` deadlock pattern

`build_offset_commit_unsent_request` was locking `inner.state` to write
back `last_epoch_sent_on_commit`, but it is called from inside
`poll_with_coordinator` while that function already holds the same
lock. With `std::sync::Mutex` (chosen for `SubscriptionState` /
`CommitRequestManagerState` per consumer-threading.md §16: short
critical sections, no `.await` while held), a recursive lock acquire
is a hard deadlock. No prior test exercised the code path because no
Phase-9 test called `poll_with_coordinator` with a request in the
unsent queue.

**Why:** This is a pre-existing Phase 9 bug exposed only when a test
drives `commit_sync` through a real send. The non-reentrant property
of `std::sync::Mutex` is documented but easy to forget when a helper
fn is extracted out of a larger critical section.

**How to apply:** When a helper function reads/writes state that the
caller already locks, push the read/write back into the caller's
critical section rather than re-locking. Or refactor to take
`&mut StateGuard` / `&mut StateField` so the type system surfaces the
locking. The fix here: writeback `guard.last_epoch_sent_on_commit =
commit.member_info.member_epoch` inside `poll_with_coordinator`'s
critical section, deleting the inner-helper lock.

## 2. `commit_sync` surfaces `TimeoutException`, NOT `RetriableCommitFailedException`

The Phase-10 task prompt suggested a test asserting
`RetriableCommitFailedError` as the final error from `commit_sync`
when retries exhaust the deadline. Java's
`CommitRequestManager.commitSyncWithRetries` actually wraps the
cause via `maybeWrapAsTimeoutException` and surfaces a
`TimeoutException`. `RetriableCommitFailedException` belongs to the
`commit_async` path
(`CommitRequestManager.commitAsyncExceptionForError`).

**Why:** Easy to confuse the two surfaces — both involve retry +
retriable errors, but Java's behaviour for each is asymmetric.

**How to apply:** When the user-task prompt names a specific error
type, verify against Java BEFORE writing the test. If the prompt is
wrong, document the divergence in the commit message and pick a test
name that matches the actual Java surface (here:
`commit_sync_surfaces_timeout_error_after_deadline_expiry`, asserting
`matches!(err, KafkaError::Timeout(_))`).

## 3. Retry continuity across consumed request-state instances

Java's `commitSyncWithRetries` recurses with the SAME
`OffsetCommitRequestState` instance, calling `resetFuture()` between
attempts. The Rust network send path consumes the state (moves it
into the spawned response-handler closure). To preserve
`numAttempts` continuity for `ExponentialBackoff`, each retry
creates a fresh state and seeds its inner `RequestState.num_attempts`
counter via `seed_failed_attempts(n, now_ms)` — which calls
`on_failed_attempt(now_ms)` `n` times.

**Why:** Without the seeding, every retry gets a fresh
`num_attempts = 0` and the exponential-backoff window resets to the
initial value. With seeding, the backoff window grows correctly
across attempts, matching Java's `RequestState.canSendRequest`
behaviour.

**How to apply:** When the Java pattern is "mutate the same state
across retries" but Rust's translation consumes the state, surface a
seed-helper that lets the next state inherit the prior's relevant
fields (here: `num_attempts`).

## 4. Java's `Timer.update(currentTimeMs)` is implicit in Rust

Java's `AutoCommitState` carries a stateful `Timer`; `shouldAutoCommit`
checks `timer.isExpired()` which reads the timer's internal
`currentTimeMs` (set by `Timer.update(...)`). Rust's translation
threads `current_time_ms` as a parameter through every query method
(`should_auto_commit(current_time_ms)`, `remaining_ms(current_time_ms)`,
etc.), so `updateAutoCommitTimer(currentTimeMs)` becomes a no-op in
Rust. `update_timer_and_maybe_commit` is therefore a thin
pass-through to `maybe_auto_commit_async(current_time_ms)`.

**Why:** A direct Java→Rust translation might add an unused
`update_timer(current_time_ms)` method. The Rust idiom is already
"current_time_ms is always a parameter" — no extra plumbing needed.

**How to apply:** When translating Java methods that rely on a
stateful `Timer.update(...)` followed by `Timer.isExpired()`, check
whether the Rust translation already passes `current_time_ms`
explicitly. If yes, the `update_*Timer` method becomes implicit and
the calling site simplifies.
