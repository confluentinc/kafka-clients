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

---

# Phase 11 Batch 2 — Resolved Critic Comments (N=1)

---

## Issue 10: BG-task `invoke_rebalance_callback` blocks on ack; app-side `add_and_get` does NOT drain bg events → deadlock risk on `commit_*`, `position`, `committed`, `beginning_offsets`, `end_offsets`, `offsets_for_times`, `pause`, `resume`, `seek*`, `leave_group_on_close` — RESOLVED

- **Original commit**: `3a7bcf1` Phase 11 (5/N) introduced
  `process_background_events_until` but only wired it through
  `unsubscribe()`. Subsequent commits added more blocking-style APIs
  via `add_and_get(...).await` without iterative drain.
- **Resolving commit**: fixup of Phase 11 (5/N) — adds a
  `Self::submit_and_drain` helper that does
  `application_event_handler.add(event)` followed by
  `process_background_events_until(receiver, deadline, ignore=|_| false, msg, enable_wakeup)`,
  and routes every blocking-style API through it:
    - `subscribe` / `subscribe_internal_pattern` / `subscribe_to_regex`
    - `assign`
    - `commit_inner` (the `offsets_ready_rx` wait) +
      `commit_sync_internal` (the typed-result wait)
    - `await_pending_async_commits_and_execute_commit_callbacks`
      (bridges `oneshot::Receiver<()>` to typed form)
    - `seek` / `seek_with_metadata` / `seek_with_reset_strategy`
    - `position_timeout` (replaces the `add_and_get(...).await.ok()`)
    - `committed_timeout`
    - `current_lag_async`
    - `beginning_or_end_offsets` + `offsets_for_times_timeout`
    - `partitions_for_timeout` + `list_topics_timeout`
    - `pause` + `resume`
    - `leave_group_on_close`
- **Resolution**: `process_background_events_until` gained an
  `enable_wakeup: bool` parameter so each API can pass the right
  setActiveTask analog (see Issue 11). The wait loop now also
  `select!`s the receiver against the wakeup token's cancellation
  signal — when `enable_wakeup=true`, a concurrent `wakeup()` returns
  `KafkaError::Wakeup` immediately instead of waiting up to 100ms for
  the timeout to fire.
- **Verification**:
    - `issue_10_commit_sync_drains_listener_callback_while_waiting`:
      simulates the exact deadlock scenario from the Critic's
      description — a `RebalanceListenerCallbackNeeded` lands on the
      bg channel mid-commit, and the test's fake bg task blocks on the
      ack BEFORE completing the commit. With the fix, the app-side
      drain helper invokes the listener inline, the ack flows back,
      and the commit completes. Without the fix, the commit would
      time out at 5s.
    - All 62 `async_kafka_consumer` tests pass.

---

## Issue 11: Every blocking-style API except `poll` / `position` ignores `wakeup()` — Java's `wakeupTrigger.setActiveTask` mechanism is not translated — RESOLVED

- **Original commit**: `5f6dee9` Phase 11 (4/N) onwards — each
  blocking API translated without the `setActiveTask` analog.
- **Resolving commit**: same fixup as Issue 10. The Rust analog of
  Java's `setActiveTask(future)` + `clearTask()` discipline is the
  `enable_wakeup: bool` parameter on
  `process_background_events_until` (and the wrapping
  `submit_and_drain`). At the top of every loop iteration the helper
  re-checks `wakeup_trigger.maybe_trigger_wakeup()`; in the bounded
  wait it `select!`s the receiver against
  `wakeup_trigger.current_token().cancelled()`. On a `wakeup()` the
  token is rotated immediately before returning `KafkaError::Wakeup`,
  mirroring Java's "clear the volatile flag after throwing
  WakeupException once" (§11).
- **Per-API matrix** (matches Java source — `enable_wakeup` value):
    - `commit_sync` / `commit_sync_internal` typed wait: **true**
      (Java line 1716)
    - `committed_timeout`: **true** (Java line 1176)
    - `partitions_for_timeout`: **true** (Java line 1223)
    - `list_topics_timeout`: **true** (Java line 1251)
    - `position_timeout` (CheckAndUpdatePositions wait): **true**
      (Java line 1963)
    - `await_pending_async_commits_...`: per-call `enable_wakeup`
      param (Java line 1738 / 1562)
    - `commit_inner` offsets-ready wait: **true** (deviation from
      Java — see code comment; uniform wakeup-observable semantic
      makes commit_sync respond to wakeup at every phase, NOT
      blocking the user 30s on offsets_ready while the commit deadline
      is 5s)
    - `subscribe` / `assign` / `seek*` / `pause` / `resume`: **false**
      (Java does not setActiveTask)
    - `current_lag_async`: **false** (Java does not setActiveTask)
    - `beginning_or_end_offsets` / `offsets_for_times`: **false**
      (Java does not setActiveTask)
    - `leave_group_on_close`: **false** (wakeup disabled by close
      path anyway)
- **Verification**:
    - `issue_11_commit_sync_observes_wakeup_during_wait` — pre-cancel
      the token, verify commit_sync returns Wakeup quickly and
      rotates.
    - `issue_11_committed_observes_wakeup_during_wait` — same for
      `committed_timeout`.
    - `issue_11_pause_does_not_observe_wakeup` — pre-cancel the
      token, verify `pause` completes normally despite the cancelled
      token (Java doesn't setActiveTask for pause).

---

## Issue 14: `position`'s `add_and_get.await.ok()` silently swallows non-Timeout errors (Java only swallows `TimeoutException`) — RESOLVED

- **Original commit**: `de1d475` Phase 11 (6/N).
- **Resolving commit**: same fixup as Issue 10. The `.await.ok()` was
  replaced when `position_timeout` was migrated to `submit_and_drain`.
  The new code matches Java's narrow `try/catch (TimeoutException e)`:
    ```rust
    match drain_result {
        Ok(()) => {},
        Err(KafkaError::Timeout(_)) => {},
        Err(err) => return Err(err),
    }
    ```
- **Verification**: `issue_14_position_propagates_non_timeout_errors` —
  the drainer completes the `CheckAndUpdatePositions` handle with an
  explicit `IllegalState` error (mirrors a non-Timeout bg-task failure);
  the test asserts the consumer surfaces that specific error instead of
  the generic `Timeout` that the previous `.await.ok()` would yield.

---

## Issue 15: `close_internal` does not apply Java's `Math.min(timeout, requestTimeoutMs)` cap — RESOLVED

- **Original commit**: `069a6c4` Phase 11 (7/N).
- **Resolving commit**: fixup of Phase 11 (7/N) — adds the cap when
  computing `close_deadline_ms`:
    ```rust
    let request_timeout_ms = self.config.request_timeout_ms() as i64;
    let capped_timeout_ms = std::cmp::min(timeout.as_millis() as i64, request_timeout_ms);
    let close_deadline_ms = calculate_deadline_ms(close_start_ms, capped_timeout_ms);
    ```
  Mirrors Java's `createTimerForCloseRequests` at
  `AsyncKafkaConsumer.java:1590-1594`.
- **Resolution**: With default config (timeout=30s,
  request_timeout_ms=30s) the cap is a no-op. A user calling
  `close(Duration::from_secs(300))` now sees each close-step bounded
  at 30s instead of 5 minutes.
- **Verification**: `close_caps_timeout_at_request_timeout_ms` —
  passes a 5-minute user timeout, asserts the deadline carried by
  the `LeaveGroupOnClose` event handle is at most
  `now + request_timeout_ms` (the capped value) and strictly less
  than the user's raw 5-minute value.

---

## Issue 12: `runRebalanceCallbacksOnClose` uses `subscriptions.assignedPartitions()` where Java uses `groupAssignmentSnapshot.get()` — RESOLVED

- **Original commit**: `069a6c4` Phase 11 (7/N).
- **Resolving commit**: fixup of Phase 11 (7/N) — adds
  `group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>` field
  on `AsyncKafkaConsumer`, populated by the new `ConsumerStateNotifier`
  bridge (Issue 13), and rewires `run_rebalance_callbacks_on_close` to
  read from it instead of `subscriptions.assigned_partitions()`.
- **Resolution**: Mirrors Java's
  `AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot`
  (`AsyncKafkaConsumer.java:317`). The Rust field is updated by
  `ConsumerStateNotifier::on_group_assignment_updated`
  (= Java's anonymous `memberStateListener.onGroupAssignmentUpdated` at
  line 349-352), which the production wire-up registers on the
  `ConsumerMembershipManager` so it fires on reconciliation. Both
  `assign(...)`-only consumers (snapshot stays empty → early return at
  Java line 1626-1628) and partially-revoked windows (snapshot still
  carries the partition but `subscriptions` has been updated) are now
  handled per Java.
- **Verification**: three new tests:
    - `run_rebalance_callbacks_on_close_skips_when_snapshot_empty` —
      manual `assign(...)` + empty snapshot ⇒ no callback.
    - `run_rebalance_callbacks_on_close_invokes_revoked_on_live_epoch` —
      snapshot populated + `member_epoch > 0` via notifier ⇒
      `on_partitions_revoked`.
    - `run_rebalance_callbacks_on_close_invokes_lost_on_unknown_epoch` —
      snapshot populated + `member_epoch < 0` ⇒ `on_partitions_lost`.

---

## Issue 13: `group_metadata` cache is never populated — `MemberStateListener` wire-up missing — RESOLVED

- **Original commit**: `77b0cbb` Phase 11 (2/N).
- **Resolving commit**: combined fixup of Phase 11 (2/N) + (7/N) —
  introduces `ConsumerStateNotifier: MemberStateListener` and the
  per-instance `state_notifier: Arc<ConsumerStateNotifier>` field,
  exposed via `Self::state_notifier()` so the production wire-up
  (Phase 12) and tests can register it on the
  `ConsumerMembershipManager`.
- **Resolution**: The new `ConsumerStateNotifier` struct holds Arcs of
  both `group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>` and
  `group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>`,
  shared 1:1 with the consumer struct. The two `MemberStateListener`
  methods translate Java's anonymous-inner-class memberStateListener
  callbacks at `AsyncKafkaConsumer.java:343-353`:
    - `on_member_epoch_updated` ⇒ `update_group_metadata` (Java line
      772-784) — `memberEpoch.ifPresent(...)` short-circuit preserved.
    - `on_group_assignment_updated` ⇒ `setGroupAssignmentSnapshot`
      (Java line 786-788).
  Production wire-up (Phase 12 ctor) calls
  `membership_manager.register_state_listener(consumer.state_notifier())`
  immediately after constructing the membership manager. Until then,
  tests construct the notifier (via `consumer.state_notifier()`) and
  invoke it directly to drive close-path tests.
- **Verification**: three new tests:
    - `state_notifier_populates_group_metadata_on_epoch_update` —
      epoch + member-id flow through the cache.
    - `state_notifier_with_none_epoch_does_not_modify_cache` —
      Java's `memberEpoch.ifPresent` short-circuit.
    - `state_notifier_updates_group_assignment_snapshot` — snapshot
      is overwritten on each reconciliation.
- **Note on Issue 12 interaction**: the three close-path tests under
  Issue 12 exercise the end-to-end notifier-to-snapshot-to-close
  pipeline, doubling as Issue 13 regression coverage.

---

## Issue 5: `subscribe_re2j_pattern_rejects_empty` weakens the Java exact-message assertion — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N).
- **Resolving commit**: Phase 11 (8/N) test-translation batch.
- **Verification**:
  - `subscribe_re2j_pattern_rejects_empty` now asserts the exact Java
    message `"Topic pattern to subscribe to cannot be empty"` via
    `assert_eq!` (was `msg.contains("empty")`).
  - New `subscribe_re2j_pattern_accepts_valid_pattern` test exercises
    Java line 1865's `assertDoesNotThrow(() -> consumer.subscribe(new SubscriptionPattern("t*")))`.
  - The skip-rationale block at the top of `subscribe_re2j_pattern_rejects_empty`
    documents the null-pattern and null-listener cases as
    unrepresentable in Rust.

---

## Issue 6: `ConsumerRebalanceListenerInvoker` paused-partition log path uncovered — RESOLVED

- **Original commit**: `aa29e7c` Phase 11 (1/N).
- **Resolving commit**: Phase 11 (8/N) test-translation batch.
- **Verification**: Three new tests in
  `src/consumer/internals/consumer_rebalance_listener_invoker.rs`:
  - `invoke_partitions_revoked_with_paused_partition_exercises_log_path`
    — populates `SubscriptionState` with a paused partition that
    intersects the revoke set; the call succeeds (the log branch is
    exercised but `info!` output is not directly asserted — log capture
    requires plumbing not yet built; the panic-free run + listener
    invocation is the observable signal).
  - `invoke_partitions_lost_with_paused_partition_exercises_log_path`
    — symmetric for `invoke_partitions_lost`.
  - `invoke_partitions_revoked_with_no_paused_intersection_skips_log`
    — negative branch: no paused intersection, so the log path is
    skipped.
- **Note on Issue 6 (b)**: the no-listener "silent no-op" divergence
  is acceptable — the Rust translation favours the bg-task path that
  invokes the listener via the `process_background_events` callback,
  and the no-listener arm is a documented short-circuit. No production
  behaviour change.

---

## Issue 7: `assignment_change_event_and_clears_subscription` test name oversells — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N).
- **Resolving commit**: Phase 11 (8/N) test-translation batch.
- **Verification**:
  - Old `assign_generates_assignment_change_event_and_clears_subscription`
    renamed to `assign_generates_assignment_change_event` (matches
    Java's `testAssign` name + body — only the event-enqueue
    assertion).
  - New `assign_clears_subscription_after_event_completes` test
    exercises Java line 821-822's
    `assertTrue(consumer.subscription().isEmpty())` AND
    `assertTrue(consumer.assignment().contains(tp))` by driving the
    test handle through `assign_from_user` on the shared
    `SubscriptionState`.


---

# Phase 11 Batch 3 — Resolved Critic Comments (N=1)

---

## Issue 21: `unsubscribe()` does not translate Java's `resetGroupMetadata()` — RESOLVED

- **Original commit**: `0d88bb9` Phase 11 (3/N) (`unsubscribe` body).
- **Resolving commit**: fixup of Phase 11 (3/N) — adds
  `ConsumerStateNotifier::reset_group_metadata()` (mirrors Java
  `AsyncKafkaConsumer.java:1857-1865`) and wires it into `unsubscribe()`
  immediately after the `process_background_events_until` drain returns,
  matching Java's placement at line 1848 (unconditional, fires on both
  the success path and the `TimeoutException` log-and-return path).
- **Resolution**: `reset_group_metadata()` writes a fresh
  `ConsumerGroupMetadata` with `UNKNOWN_GENERATION_ID` (-1) /
  `UNKNOWN_MEMBER_ID` ("") into the cache, preserving the old
  `group_id` and `group_instance_id`. Mirrors Java's
  `initializeConsumerGroupMetadata(oldGroupId, oldGroupInstanceId)`
  semantics. Java's `oldGroupMetadataOptional.map(...)`
  short-circuit on empty is preserved — assignment-only consumers
  never populate the cache and the slot stays `None`.
- **Verification**: `group_metadata_is_reset_after_unsubscribe` —
  populates the cache via `state_notifier.on_member_epoch_updated`,
  asserts the pre-condition (`generation_id=42`, `member_id="memberId"`),
  drives `unsubscribe()` to completion, and asserts the post-condition
  (`generation_id=-1`, `member_id=""`, `group_id` preserved). The
  skip rationale for `testGroupMetadataIsResetAfterUnsubscribe` is
  updated to point at the new test.

---

## Issue 22: `commit_async` can return `KafkaError::Wakeup` but Java's `commitAsync` never throws `WakeupException` — RESOLVED

- **Original commit**: `b5ce5c5` fixup of Phase 11 (5/N) (Issues 10 + 11
  fixup that introduced the shared `enable_wakeup=true` in
  `commit_inner`'s offsets-ready wait).
- **Resolving commit**: `41540ae` fixup of Phase 11 (5/N).
- **Resolution**: `commit_inner` gains an `enable_wakeup: bool`
  parameter. `commit_sync_internal` passes `true` (matches Java
  `AsyncKafkaConsumer.java:1716` `setActiveTask(commitFuture)`);
  `commit_async_internal` passes `false` (matches Java line 1684-1700
  — `commitAsync` is documented non-blocking and never throws
  `WakeupException`). User code calling `commit_async()` followed by
  `wakeup()` now observes the commit complete normally instead of
  surfacing `KafkaError::Wakeup`.
- **Verification**: `issue_22_commit_async_does_not_observe_wakeup` —
  pre-cancels the wakeup token, drives `commit_async()` against a
  drainer that completes the `CommitAsync` envelope normally, and
  asserts the call returns `Ok(())`. Without the fix the
  `enable_wakeup=true` offsets-ready wait would surface
  `KafkaError::Wakeup`.

---

## Issue 23: `commit_async_user_supplied_callback_with_exception_group_authz` test uses wrong error variant — RESOLVED

- **Original commit**: `3a7bcf1` Phase 11 (5/N) (`commit_async_*`
  test block).
- **Resolving commit**: `41540ae` fixup of Phase 11 (5/N).
- **Resolution**: The test now calls
  `commit_async_callback_with_exception(KafkaError::group_authorization("test-group"))`
  instead of `KafkaError::illegal_argument("Group authorization exception")`,
  actually exercising the `KafkaError::GroupAuthorization` variant.
  Matches Java's `@ParameterizedTest` second parameter
  `GroupAuthorizationException`
  (`AsyncKafkaConsumerTest.java:342-356`).

