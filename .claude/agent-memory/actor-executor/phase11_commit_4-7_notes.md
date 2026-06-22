---
name: phase11-commits-4-7
description: Milestone-8 Phase 11 commits 4-7 — AsyncKafkaConsumer poll/commit/seek/close + Consumer trait impl + auto-commit-before-rebalance wiring
metadata:
  type: project
---

# Phase 11 commits 4-7 — AsyncKafkaConsumer end-to-end (minus production ctor)

This batch landed the `AsyncKafkaConsumer<K, V>` trait surface end-to-end:
poll loop, commit family, seek/position/committed/lag, beginning/end
offsets, offsets-for-times, partitions-for/list-topics, pause/resume,
enforce-rebalance, close, and the `Consumer<K, V>` trait impl. The
production `new_consumer<K, V>` factory remains stubbed (deferred to
Phase 12) because the Java primary ctor at
`AsyncKafkaConsumer.java:285-600` is 350 LOC of network plumbing.

## Trait surface adjustments (Phase 2 had them as `fn`)

`assign`, `seek*`, `pause`, `resume` were declared `fn` in Phase 2 but
must be `async fn` because Java's implementations call
`applicationEventHandler.addAndGet(...)`, which blocks. The trait change
forces a parallel update on `MockConsumer` (added `async` keyword on
those methods) and the matching `mock_consumer_test.rs` call sites
(added `.await`). No body changes — the methods themselves were sync
in `MockConsumer` and remain semantically sync; only the signature is
async to satisfy the trait.

Doc-comments on each trait method carry a Java line citation
(`AsyncKafkaConsumer.java:1068`, `:1279`, `:1292`, `:1819`).

## §31 + §16 + §11 audit (passed in this batch)

  - §31: `process_background_events` runs at the top of every
    blocking-style API. `poll` calls it inside `check_inflight_poll`
    via `run_check_inflight_drain`; `commit_sync_internal` calls it
    via `commit_inner` (which calls `invoke_pending_callbacks`) and
    again via `await_pending_async_commits_and_execute_commit_callbacks`.
    `process_background_events_until` is the iterative-loop helper
    used by `unsubscribe` and similar APIs that need to drain
    listener callbacks during a single outstanding handle wait.
  - §16: `SubscriptionState` guards are held only for the duration of
    a single read / write; never across `.await`. Verified in
    `position`, `assign`, every subscribe variant, `pause`/`resume`,
    `committed`, the bg-task reconcile path.
  - §11: `wakeup_trigger.maybe_trigger_wakeup()` runs at every loop
    iteration in `poll` and `position`; on returning
    `KafkaError::Wakeup` the token is rotated via
    `wakeup_trigger.rotate()`. `close()` calls
    `wakeup_trigger.disable()` so subsequent `wakeup()`s are no-ops
    (Java's `disableWakeups`).

## Close path (commit 7)

Eight Java steps mapped 1:1:

  1. `wakeup_trigger.disable()`.
  2. `auto_commit_on_close(deadline)` — `commit_sync_timeout` +
     `CommitOnClose` event; group_id=None short-circuits.
  3. `stop_find_coordinator_on_close()` — `StopFindCoordinatorOnClose`.
  4. `run_rebalance_callbacks_on_close()` — `on_partitions_revoked`
     (memberEpoch > 0) or `on_partitions_lost` (memberEpoch <= 0)
     inline on the caller's task; listener-None short-circuits.
  5. `leave_group_on_close(deadline, op)` — `LeaveGroupOnClose` event;
     timeout is swallowed + logged (Java's
     `catch (TimeoutException)`).
  6. `await_pending_async_commits_and_execute_commit_callbacks` —
     final drain.
  7. `network_thread_close.signal_close()` + `wakeup()` +
     `await_join()`.
  8. Final reaper.

First-error tracking via `Option<KafkaError>` mirrors Java's
`AtomicReference<Throwable> firstException`. `close_internal` takes a
`swallow_exception` parameter; users get errors propagated by default
(matches `KafkaConsumer.close()` Java contract).

Idempotent: double-close returns Ok(()) (the `is_closed()` early
return short-circuits before step 1).

## Auto-commit-before-rebalance (Phase 10 carry-over closed)

`consumer_membership_manager.rs::reconcile()` now invokes
`CommitRequestManager::maybe_auto_commit_sync_before_rebalance(deadline_ms, now)`
between step 8 (`mark_pending_revocation`) and step 9 (the
`OnPartitionsRevoked` callback dispatch). Matches Java's
`AbstractMembershipManager.java:894-919`. The commit failure is
logged-and-swallowed (Java's `revokeAndAssign` proceeds anyway);
sender-dropped is similarly logged. The Phase-10 TODO at line 538
of `consumer_membership_manager.rs` is closed; the deferred-tests
docstring is rewritten to point at Phase 11 commits (8-10) for
behavioural coverage.

## Test fixture: `auto_complete_next_event`

The drainer helper in `async_kafka_consumer.rs` now handles
`SeekUnvalidated`, `ResetOffset`, `PausePartitions`, `ResumePartitions`
in addition to subscribe/assign/unsubscribe variants. Tests that
exercise async APIs hitting these events must spawn the drainer
before the call (the drainer task receives the envelope and completes
the handle so the consumer's `add_and_get` returns).

For close tests, the drainer must additionally handle `CommitSync`
(auto-commit fires inside close path) and `CommitAsync`. Without this
the close test hangs on the auto-commit step.

## Critic-comment fix patterns from this batch

  - **Doc-rationale for deliberate Java divergence**: when a Java
    contract (`throws IllegalStateException after close`,
    `throws InvalidGroupIdException for null group.id`) is dropped
    on a Rust accessor for idiomatic reasons, document the
    divergence at both the module level AND in the per-method
    rustdoc. Cite the matching Java tests that will be skipped in
    the test-translation commit (`testListPartitionsAfterClose`,
    `testGroupMetadataAfterCreationWithGroupIdIsNull`).
  - **Returning owned mutable sets vs Java's unmodifiableSet**:
    explicit doc-comment on every accessor noting the divergence.
    Idiomatic Rust returns owned `HashSet`; the divergence is
    observable only through user code that depended on
    `UnsupportedOperationException`.

## Files closing this batch

Production:
  - `src/consumer/async_kafka_consumer.rs` — close path + Consumer
    trait impl + doc-comments for Issues 3/4/9.
  - `src/consumer/mod.rs` — trait async on assign/seek/pause/resume +
    updated factory message.
  - `src/consumer/mock_consumer.rs` — async on assign/seek/pause/resume.
  - `src/consumer/internals/consumer_membership_manager.rs` —
    auto-commit-before-rebalance wiring + TODO close-out.

Tests:
  - `tests/consumer/mock_consumer_test.rs` — `.await` added at every
    now-async call site.
  - `src/consumer/async_kafka_consumer.rs#tests` — 6 close-path tests
    (close_is_idempotent, close_disables_wakeups,
    close_with_options_zero_timeout_completes,
    close_without_group_id_skips_leave_group,
    close_then_apis_error_with_already_closed,
    consumer_trait_impl_compiles).
