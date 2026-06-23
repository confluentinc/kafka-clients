---
name: review-m8-phase11-batch2
description: Patterns from reviewing Phase-11 batch 2 (commits 4-7) — poll, commit, seek/position/committed/lag/offsets/pause-resume/topic-metadata/enforceRebalance, close + Consumer trait impl + factory wire-up.
metadata:
  type: reference
---

# Phase-11 batch-2 review patterns

## Major contract gap: bg-task awaits ack, app-side doesn't drain → deadlock

The bg task's `invoke_rebalance_callback` (in
`abstract_membership_manager.rs:747`) **awaits** the listener-ack
oneshot. While awaiting, the bg task cannot process other app events.

If the app-side blocking API (`commit_sync`, `position`, `committed`,
`pause`, `resume`, etc.) calls `add_and_get` and `.await`s the typed
receiver WITHOUT iteratively draining bg events, a rebalance during
the wait causes deadlock until timeout.

Only `poll` (via `check_inflight_poll` → `process_background_events`)
and `unsubscribe` (via `process_background_events_until`) drain. The
other ~15 blocking-style APIs do NOT.

**Verification check for new commits:** every async fn that
`.await`s a typed receiver from `add_and_get` (or that has an internal
`oneshot::Receiver.await` like `commit_inner`'s `offsets_ready_rx`)
MUST route through `process_background_events_until`. Filed as Issue 10.

## Wakeup-trigger setActiveTask analog is missing

Java's `wakeupTrigger.setActiveTask(future)` interrupts the active
future when `wakeup()` is called. The Rust translation has
`WakeupTrigger::maybe_trigger_wakeup` (sync polling) but NO
`select!`-based mechanism that interrupts an in-flight `add_and_get`.

Only `poll()` and `position_timeout` observe the wakeup via
`maybe_trigger_wakeup()` at loop boundaries. Everything else
(`committed`, `commit_sync`'s receiver wait, `seek*`, `pause`,
`resume`, `partitions_for`, `list_topics`, `beginning/end_offsets`,
`offsets_for_times`, `leave_group_on_close`) is NOT interruptible by
`wakeup()`. The `_enable_wakeup` parameter on
`await_pending_async_commits_and_execute_commit_callbacks` is even
named-but-ignored, making the gap explicit.

Filed as Issue 11. The PLAN.md §11 wakeup audit (lines 489-491)
explicitly claims this is implemented; it isn't.

## Missing groupAssignmentSnapshot + missing MemberStateListener wire-up

Two related gaps in the close path:

1. `runRebalanceCallbacksOnClose` reads `groupAssignmentSnapshot.get()`
   in Java. The Rust translation reads
   `subscriptions.assigned_partitions()`. Two divergences:
   - For `assign(...)`-set partitions (manual assignment), Java would
     return early (snapshot is empty); Rust invokes
     `on_partitions_lost`.
   - For partially-revoked partitions, the snapshot lags
     `subscriptions`.
   Filed as Issue 12.

2. `group_metadata` cache is `Arc<Mutex<None>>` constructed but never
   updated. The doc-comment claims `MemberStateListener` (Phase 8b)
   updates it via `Self::update_group_metadata` — but neither the
   function nor the wire-up exist. Combined with (1), every close-time
   callback invocation goes to `on_partitions_lost` (member_epoch=-1).
   Filed as Issue 13.

These are deferred-but-undocumented gaps. The actor's commit message
for 7/N says "closes Issues 3, 4, 9" but Issues 12-13 (the real
divergence under those names) are new.

## Silent `.await.ok()` swallows non-Timeout errors

`position_timeout` line 1789-1793 uses `.await.ok()` which discards
ALL error variants. Java only catches `TimeoutException`. Result: if
the bg task panics/drops, `position` spin-loops the deadline instead
of failing fast. Pattern to watch for: any `.await.ok()` (or
`.await.unwrap_or_default()`) on a `Result<_, KafkaError>` where Java
catches only a specific exception.

Filed as Issue 14.

## `Math.min(timeout, requestTimeoutMs)` for close not translated

`createTimerForCloseRequests` (Java line 1590-1594) caps the close
timeout at `requestTimeoutMs`. Rust uses the raw user-supplied value.
Default config makes this invisible; user-supplied long timeouts
diverge. Filed as Issue 15.

## InvalidGroupIdException → KafkaError::IllegalArgument is wrong

Java's `InvalidGroupIdException` is a specific subclass; Rust collapses
it to `IllegalArgument`. Tests have to substring-match the message
instead of the variant — a smell that points at the missing variant.
Per CLAUDE.md rule 2 ("Java Exception → Rust Error"), a dedicated
`KafkaError::InvalidGroupId` variant should exist. Filed as Issue 16.

## Test that doesn't actually test the thing in the name

`commit_sync_invokes_interceptor_chain` builds the consumer with an
EMPTY `ConsumerInterceptors`. The test asserts only that the completer
saw the event — it doesn't observe the interceptor. Same pattern as
batch-1 Issue 7 (misleading test name). Filed as Issue 17.

**Heuristic:** when reviewing a test, check the fixture builder. If
the test name promises "X invokes Y", the fixture must register an
observable Y. Empty `ConsumerInterceptors::new(Vec::new())` is a red
flag.

## Single deadline vs Java's fresh-timer-after-commit

Java's `commitSync` creates `requestTimer = time.timer(timeout)` AFTER
`commit(...)` returns, so the total wall-clock can be up to `2 *
timeout`. Rust uses a single `deadline_ms`. Rust is arguably more
correct, but it's a documented divergence — file as such, low
severity. Filed as Issue 18.

## Debug-formatted error messages diverge from Java's toString

`{:?}` on `&[TopicPartition]` produces
`[TopicPartition { topic: "t", partition: 0 }]`. Java's
`Set.toString()` produces `[t-0]`. Any exact-message Java test would
fail in Rust. DoD §3: error messages are part of the behavioral
contract. Filed as Issue 19. Pattern: search for `format!("...{:?}", ...)`
on `partitions`, `offsets`, `topic_partitions` etc. in error builders.

## `close_then_apis_error` tests only 2 of 27 blocking APIs

Java's "throw-after-close" test pattern loops over EVERY public
method. Rust tests only `commit_sync` + `unsubscribe`. Pattern:
recommend a single test that calls each blocking API and asserts
`IllegalState` after close. Filed as Issue 20.

## Documented seams to NOT flag

- Factory `new_consumer` returns `unsupported_version` — Phase 12
  carry-over, cited in error message + PLAN.md. OK as documented seam.
- `current_lag` sync `fn` returning None — Phase 2 trait-surface
  decision, not a Phase 11 regression.
- `interceptors.on_consume` invocation goes through the mutex but
  emits no metrics on failure — PLAN.md explicitly listed this as
  out-of-scope.
- `enforce_rebalance` no-op + warn-log — matches Java's KIP-848 arm
  exactly.

## Process notes

- Phase 11 PLAN.md §11 audit list (lines 486-492) is the cleanest
  reference for "did the actor do X?" — go down it methodically.
- Each open uncertainty in the user's review request gets a verdict in
  COMMENTS — explicitly resolve OR file as new issue. The "matches
  Java exactly" verdict for uncertainty #3 (saturating_add for
  rebalance deadline) requires actual cross-check against
  `getDeadlineMsForTimeout`.
