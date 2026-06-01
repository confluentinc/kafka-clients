# Phase 12.5 — Resolved Critic comments

## Issue 1: Coordinator forwarder skips `getAndClearFatalError()` on failure paths — RESOLVED

- **File**: `src/consumer/internals/coordinator_request_manager.rs:307-336`
- **Severity**: Bug (Behavior Mismatch with Java)
- **Java Reference**: `CoordinatorRequestManager.java:123-131`
- **Description**: Java's `whenComplete` callback for `FindCoordinator`
  calls `getAndClearFatalError()` **unconditionally** at the very top,
  *before* branching on success vs. failure:

  ```java
  return unsentRequest.whenComplete((clientResponse, throwable) -> {
      getAndClearFatalError();                            // <-- ALWAYS clears
      if (clientResponse != null) {
          ... onResponse(clientResponse.receivedTimeMs(), response);
      } else {
          onFailedResponse(unsentRequest.handler().completionTimeMs(), throwable);
      }
  });
  ```

  The Rust forwarder only cleared `fatal_error` on the success path
  (`on_response_inner` line 209). Neither `Ok(Err(err))` nor
  `Err(_recv_err)` arms cleared it. As a result, a stale fatal error
  (e.g. `GROUP_AUTHORIZATION_FAILED`) from a prior attempt persisted
  across a subsequent transport-level failure that Java would have
  wiped clean.

- **Fix**: Moved the fatal-error clear to the top of the spawned
  forwarder, before `match response_rx.await`. Mirrors Java's
  `getAndClearFatalError()` at the top of the `whenComplete` lambda —
  the clear runs unconditionally regardless of which arm fires next.
  The existing `on_response_inner` still calls `.take()` on
  `fatal_error` — that is now redundant on the forwarder path
  (idempotent `Option::take`), but is preserved so the test-only
  `on_response(...)` entry point (line 199) retains its
  clear-on-success behavior independent of the forwarder.

- **Test gap addressed**: Added regression test
  `test_response_routing_failure_path_clears_fatal_error` that seeds a
  fatal via `Errors::GroupAuthorizationFailed` (using
  `expect_find_coordinator_request`), fires a transport-level
  `on_failure(NetworkException)` through the spawned forwarder, and
  asserts `manager.fatal_error().is_none()`. The test fails without
  the fix (verified locally before applying the patch).

- **Commits**:
  - `fixup! Phase 12.5 (1/N): Issue 1 — clear fatal_error on coordinator failure paths`

---

## Issue 2: Multi-yield assertion in regression tests is fragile — RESOLVED

- **File**: `src/consumer/internals/coordinator_request_manager.rs:786-792`
  (and identical pattern at `topic_metadata_request_manager.rs:1086-1087`)
- **Severity**: Test Reliability (nit)
- **Description**: Both new regression tests relied on
  `tokio::task::yield_now().await; tokio::task::yield_now().await;`
  to give the spawned forwarder a chance to run before the assertions.
  `yield_now` re-queues the current task; it does NOT guarantee the
  spawned task has executed.

- **Fix**:
  - `coordinator_request_manager.rs`: introduced a local
    `wait_until(predicate)` helper in the tests module that polls the
    predicate every 1ms with a 100ms wall-clock budget. The three
    regression tests (`test_response_routing_through_spawned_forwarder`,
    `test_response_routing_failure_path`, and the new
    `test_response_routing_failure_path_clears_fatal_error`) now wait
    deterministically on the side-effect being asserted.
  - `topic_metadata_request_manager.rs`:
    - The success-path test (`test_response_routing_through_spawned_forwarder`)
      uses `tokio::time::timeout(Duration::from_millis(100), &mut rx)`
      directly on the user-facing receiver — the load-bearing assertion.
    - The retriable-failure test (`test_response_routing_retriable_failure_path`)
      polls `manager.inflight_remaining_backoff_ms(0, 0) > 0`
      (the observable side effect of `on_failed_attempt`) in a 100ms
      bounded loop, since `on_failed_attempt` runs inside the spawned
      forwarder.

  Both are deterministic across current-thread and multi-thread Tokio
  runtimes; neither relies on `yield_now`'s scheduling implementation
  detail.

- **Commits**:
  - `fixup! Phase 12.5 (1/N): Issue 2 — robust forwarder-sync in regression tests`

---

## Issue 3: `on_response` error branch skips `reset_heartbeat_state()` that Java's `onErrorResponse` calls first — RESOLVED

- **File**: `src/consumer/internals/consumer_heartbeat_request_manager.rs:415-485` (the `on_response` method)
- **Severity**: Bug (Behavior Mismatch with Java)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java:351-446` — specifically line 356 `resetHeartbeatState();` runs at the TOP of `onErrorResponse`, BEFORE `heartbeatRequestState.onFailedAttempt(currentTimeMs)` (line 357) and the per-error switch.
- **Description**:
  Java's `onErrorResponse(R response, long currentTimeMs)` resets the per-request `HeartbeatState` (the consumer-specific `SentFields` field tracker) at the top, before classifying. The Rust `on_response` error branch went straight to `self.inner.classify_response_error(error, &error_message, completion_time_ms)`. `classify_response_error` calls `on_failed_attempt` (matches Java line 357), but the per-request `HeartbeatState::reset()` (Java line 356) was **never called**. The transport-failure path (`on_failure`) correctly called `self.reset_heartbeat_state()`; the response-error path did not.

  Net effect: after a heartbeat returned with an error code in the response body (e.g. `COORDINATOR_LOAD_IN_PROGRESS`, `INVALID_REQUEST`, `TOPIC_AUTHORIZATION_FAILED`), `SentFields` was NOT reset, so the next heartbeat's `build_request_data()` would diff against stale "sent" tracking and SKIP fields that Java would re-send (subscribed topic names, rebalance timeout, server assignor, local assignment, pattern). The broker may then assume the consumer is still using stale subscription state.

- **Fix**: Added `self.reset_heartbeat_state();` at the very top of the error branch in `on_response`, before `classify_response_error`. Mirrors Java line 356 exactly.

- **Regression test**: `issue3_error_response_resets_sent_fields` (in `consumer_heartbeat_request_manager.rs::tests`):
  1. Subscribes to `["t"]` so `SubscriptionState` carries a non-empty topic list.
  2. Drives `poll(now)` once and asserts the new `sent_fields_topics_populated()` test accessor returns `true` (sanity check that the first build populated `SentFields.subscribed_topic_names`).
  3. Synthesises a `ConsumerGroupHeartbeatResponse` with `error_code = CoordinatorLoadInProgress` and routes it through `unsent.handler().on_complete(response)`.
  4. Polls in a bounded loop until `sent_fields_topics_populated()` flips back to `false` — confirming the drain at the top of `poll(now)` invoked `reset_heartbeat_state()` on the error branch.

- **Commits**:
  - `fixup! Phase 12.5 (3/N): Issue 3 — reset_heartbeat_state on error response branch`

---

## Issue 4: Heartbeat `Fenced` / `Fatal` error actions never drive `transitionToFenced` / `transitionToFatal` in production — RESOLVED

- **File**: `src/consumer/internals/consumer_heartbeat_request_manager.rs:441-472` (the `Fenced` / `Fatal` arms of `on_response`); `src/consumer/internals/consumer_network_thread.rs::run_once` (new Phase 2.4 drain).
- **Severity**: Behavior Mismatch (Missing Requirement)
- **Java Reference**:
  - `AbstractHeartbeatRequestManager.java:411-427` — `FENCED_MEMBER_EPOCH` / `UNKNOWN_MEMBER_ID` arms call `membershipManager().transitionToFenced();` synchronously inside the `whenComplete` lambda.
  - `AbstractHeartbeatRequestManager.java:455-458` — `handleFatalFailure(Throwable error)` calls `membershipManager().transitionToFatal();` synchronously.
  - `ConsumerHeartbeatRequestManager.java:109` — `handleSpecificFailure` for `UnsupportedVersionException` calls `handleFatalFailure(...)` (also transitions to FATAL).

- **Description**:
  When Rust's `classify_response_error` returned `HeartbeatErrorAction::Fenced` or `HeartbeatErrorAction::Fatal(err)`, the drain emitted a `BackgroundEvent::Error` envelope but no production code path drove `transition_to_fenced` / `transition_to_fatal` on the membership state machine. `grep "\.transition_to_fatal\|\.transition_to_fenced" src/ | grep -v test` returned only test callsites. As a result, the membership manager could not recover from broker fencing or transition to FATAL — a fenced consumer would propagate the error to the user via `poll()` but the membership state would not advance to FENCED → JOINING, and the consumer could not rejoin after a server-side fence.

- **Fix (option 1 from Critic comment — production wiring)**:

  1. **Heartbeat-side side-channel** (`consumer_heartbeat_request_manager.rs`):
     - New `pub(crate) enum PendingMembershipTransition { Fenced, Fatal(KafkaError) }`.
     - New `mpsc::UnboundedChannel<PendingMembershipTransition>` field on `ConsumerHeartbeatRequestManager`.
     - `on_response` `Fenced` arm pushes `PendingMembershipTransition::Fenced` in addition to emitting `BackgroundEvent::Error`.
     - `on_response` `Fatal(err)` arm pushes `PendingMembershipTransition::Fatal(err.clone())` in addition to emitting the error event.
     - `on_failure` fatal path (non-retriable + `handle_specific_failure` returned false) pushes `PendingMembershipTransition::Fatal(error.clone())`.
     - `handle_specific_failure` (UnsupportedVersion arm) pushes `PendingMembershipTransition::Fatal(...)` to mirror Java's `handleFatalFailure` inside `ConsumerHeartbeatRequestManager.handleSpecificFailure`.
     - New `pub(crate) fn take_pending_membership_transitions(&mut self) -> Vec<PendingMembershipTransition>` drains the side-channel.

  2. **RequestManagers façade** (`request_managers.rs`):
     - New `pub(crate) fn take_pending_membership_transitions(&mut self)` delegates to `consumer_heartbeat.as_mut()?.take_pending_membership_transitions()`, returns empty Vec if no heartbeat manager is wired.

  3. **Bg-task drive** (`consumer_network_thread.rs` `run_once`):
     - New "Phase 2.4" block inserted between `entries().poll(now)` (which lets the heartbeat manager classify pending responses) and "Phase 2.5: membership.reconcile" (which observes the post-transition state).
     - Locks `request_managers`, drains transitions, drops the guard, then `await`s `membership.transition_to_fenced(now)` / `transition_to_fatal(now)` for each envelope. Failures are logged and swallowed (mirrors Java's `whenComplete` lambda which logs but does not rethrow).

  **§16 audit**: between draining the transitions and the cross-RM `membership.transition_to_*` `.await` calls, no `Mutex::lock()` is held — the `request_managers` guard is dropped before the `.await`. The membership manager's internal `Mutex` is acquired inside the transition methods, not from the caller side.

- **Regression tests** (in `consumer_heartbeat_request_manager.rs::tests`):

  1. `issue4_fenced_member_epoch_drives_transition_to_fenced` — drives a heartbeat, routes a `FENCED_MEMBER_EPOCH` response through the spawned forwarder, drives `poll(now)` until the drain classifies, asserts exactly one `PendingMembershipTransition::Fenced` envelope on the side-channel, drives `mm.transition_to_fenced(0).await`, asserts the membership state is `Joining` (post-fence rejoin — `transition_to_fenced` with empty assignment runs JOINING → FENCED → no listener → JOINING), and asserts at least one `BackgroundEvent::Error` envelope was emitted.

  2. `issue4_group_authorization_failed_drives_transition_to_fatal` — drives a heartbeat, routes a `GROUP_AUTHORIZATION_FAILED` response, drives `poll(now)` until the drain classifies, asserts exactly one `PendingMembershipTransition::Fatal(...)` envelope carrying `Errors::GroupAuthorizationFailed`, drives `mm.transition_to_fatal(0).await`, asserts the membership state is `Fatal`, and asserts at least one `BackgroundEvent::Error` envelope was emitted.

  Both tests use a new `drive_error_response_and_collect` helper that encapsulates the response-routing scaffolding (poll, on_complete, drain loop with 200ms deadline) so the per-error tests focus on the classification mapping.

- **Error-classification table** (Rust mapping verified against Java `AbstractHeartbeatRequestManager.java:351-446` and `ConsumerHeartbeatRequestManager.java:98-160`):

  | Rust error code               | Rust action                          | Side-channel emission     | Java behaviour                                              |
  |-------------------------------|--------------------------------------|---------------------------|-------------------------------------------------------------|
  | `NotCoordinator`              | `Handled` (mark coord unknown, reset)| none                      | `coordinatorRequestManager.markCoordinatorUnknown` + reset  |
  | `CoordinatorNotAvailable`     | `Handled` (mark coord unknown, reset)| none                      | same as above                                               |
  | `CoordinatorLoadInProgress`   | `Handled` (backoff + retry)          | none                      | log + backoff, no transition                                |
  | `GroupAuthorizationFailed`    | `Fatal(GAFE)`                        | `Fatal(...)`              | `handleFatalFailure` → `transitionToFatal`                  |
  | `TopicAuthorizationFailed`    | `Handled` (emit ErrorEvent)          | none                      | `backgroundEventHandler.add(ErrorEvent)`, no transition     |
  | `InvalidRequest`              | `Fatal(...)`                         | `Fatal(...)`              | `handleFatalFailure` → `transitionToFatal`                  |
  | `GroupMaxSizeReached`         | `Fatal(...)`                         | `Fatal(...)`              | same as above                                               |
  | `UnsupportedAssignor`         | `Fatal(...)`                         | `Fatal(...)`              | same as above                                               |
  | `FencedMemberEpoch`           | `Fenced`                             | `Fenced`                  | `membershipManager().transitionToFenced()` + skip backoff   |
  | `UnknownMemberId`             | `Fenced`                             | `Fenced`                  | same as above                                               |
  | `InvalidRegularExpression`    | `Fatal(...)`                         | `Fatal(...)`              | `handleFatalFailure` → `transitionToFatal`                  |
  | `UnsupportedVersion` (response)| `Fatal(...)` via `handle_specific_exception_in_response` | `Fatal(...)` | `handleSpecificExceptionInResponse` → `handleFatalFailure` |
  | `UnreleasedInstanceId`        | `Fatal(...)` via `handle_specific_exception_in_response` | `Fatal(...)` | classify + `handleFatalFailure`                            |
  | `FencedInstanceId`            | `Fatal(...)` via `handle_specific_exception_in_response` | `Fatal(...)` | classify + `handleFatalFailure`                            |
  | `UnsupportedVersion` (transport / `on_failure`) | fatal via `handle_specific_failure` | `Fatal(...)` | `handleSpecificFailure` → `handleFatalFailure`           |
  | any other (transport non-retriable + `handle_specific_failure` returns false) | fatal | `Fatal(...)` | `handleFatalFailure`                                       |

  Every error code that Java passes to `transitionToFenced` / `transitionToFatal` now routes to the same call in Rust via the side-channel.

- **Commits**:
  - `fixup! Phase 12.5 (3/N): Issue 4 — drive transition_to_fenced/_fatal from heartbeat response classifier`

## Issue 6: Rust Fenced arm emits `BackgroundEvent::Error` that Java does not — RESOLVED

- **File**: `src/consumer/internals/consumer_heartbeat_request_manager.rs` (the `Fenced` arm of `on_response`)
- **Severity**: Behavior Mismatch (Java divergence)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java:411-427`

### Resolution

The `Fenced` arm of `ConsumerHeartbeatRequestManager::on_response` previously
emitted a `BackgroundEvent::Error { error: KafkaError::FencedMemberEpoch ... }`
alongside the `PendingMembershipTransition::Fenced` side-channel push. This
diverged from Java: `AbstractHeartbeatRequestManager.java:411-427` — the
`FENCED_MEMBER_EPOCH` and `UNKNOWN_MEMBER_ID` arms call **only**
`membershipManager().transitionToFenced()` + `heartbeatRequestState.reset()`.
They do NOT call `backgroundEventHandler.add(new ErrorEvent(...))` — that is
reserved for `handleFatalFailure` (`:455-458`).

Net effect of the prior behaviour: a Rust consumer that got fenced would
surface `KafkaError::FencedMemberEpoch` from `poll()` via the §31 background-
event drain, where a Java consumer in the same situation continues to return
`ConsumerRecords::empty()` and rejoins transparently via the
`transitionToFenced` → `onPartitionsLost` → JOINING flow.

### Fix

1. Removed the `background_event_handler.add(BackgroundEvent::Error { ... }, ...)`
   block from the `Fenced` arm. The membership state listener and the
   `onPartitionsLost` callback (driven by the Phase 2.4 `transition_to_fenced.await`
   drain in `consumer_network_thread.rs`) remain the correct propagation paths.
2. Updated the comment block in the `Fenced` arm to spell out the Java contract
   and call out the contrast with `handleFatalFailure` (which DOES emit
   ErrorEvent — line 456) so future readers don't reintroduce the divergence.
3. Updated test `issue4_fenced_member_epoch_drives_transition_to_fenced`:
   - Replaced the `BackgroundEvent::Error must be emitted on the fence path`
     assertion with the inverted assertion: no `BackgroundEvent::Error` envelope
     was emitted (the side-channel transition is the only side effect).
   - Added a citation to `AbstractHeartbeatRequestManager.java:411-427` so the
     test pins the Java parity, not just the Rust behaviour at time of writing.

The `Fatal` path is unaffected — Java's `handleFatalFailure` does emit
`ErrorEvent` (`:455-458`), and the Rust `Fatal` arm of `on_response` and the
non-retriable branch of `on_failure` both continue to emit `BackgroundEvent::Error`
correctly. The `issue4_group_authorization_failed_drives_transition_to_fatal`
test still asserts the ErrorEvent emission on the Fatal path.

### Java vs Rust ErrorEvent emission matrix (post-fix)

| Heartbeat outcome              | Java                                              | Rust (post-fix)                             |
|--------------------------------|---------------------------------------------------|---------------------------------------------|
| Fenced (FENCED_MEMBER_EPOCH)   | `transitionToFenced()` only, no ErrorEvent       | `PendingMembershipTransition::Fenced`, no ErrorEvent |
| Fenced (UNKNOWN_MEMBER_ID)     | `transitionToFenced()` only, no ErrorEvent       | `PendingMembershipTransition::Fenced`, no ErrorEvent |
| Fatal (response body)          | `handleFatalFailure(...)` → ErrorEvent + Fatal   | ErrorEvent + `PendingMembershipTransition::Fatal(...)` |
| Fatal (transport non-retriable)| `handleFatalFailure(...)` → ErrorEvent + Fatal   | ErrorEvent + `PendingMembershipTransition::Fatal(...)` |
| TopicAuthorizationFailed       | ErrorEvent only, no transition                   | ErrorEvent only, no transition (`HeartbeatErrorAction::Handled` after emit) |

### Commits

- `fixup! Phase 12.5 (3/N): Issue 6 — Fenced arm should not emit BackgroundEvent::Error`

## Issue 5: Unknown heartbeat response error codes fall through to `Handled` in Rust but `Fatal` in Java — RESOLVED

- **File**: `src/consumer/internals/consumer_heartbeat_request_manager.rs` (the `final_action` match around `handle_specific_exception_in_response`)
- **Severity**: Bug (Behavior Mismatch with Java)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java:435-441`

### Resolution

Java's `default:` arm of `onErrorResponse`'s switch
(`AbstractHeartbeatRequestManager.java:435-441`) calls
`handleSpecificExceptionInResponse(...)`; if that returns `false` (no
consumer-specific handler matched), Java falls back to
`handleFatalFailure(error.exception(errorMessage))` —
i.e. **emit `ErrorEvent` + `transitionToFatal`**. Net effect: any
unknown / future heartbeat-response error code puts the Java consumer
into `FATAL` state and propagates the failure to the user via `poll()`.

The Rust translation in commit `1950caf` used
`.unwrap_or(HeartbeatErrorAction::Handled)` when the specific handler
returned `None`, silently swallowing the unknown error. The member
stayed in its current state and the heartbeat kept retrying
indefinitely — diverging from Java's fail-fast contract for the
catch-all branch.

Error codes affected (non-exhaustive — anything not in the abstract
switch and not in `{UnsupportedVersion, UnreleasedInstanceId,
FencedInstanceId}`): `RebalanceInProgress`, `IllegalGeneration`,
`UnknownTopicOrPartition`, `RequestTimedOut`, plus any future broker
error codes added in 4.3+.

### Fix

1. Replaced `.unwrap_or(HeartbeatErrorAction::Handled)` with
   `.unwrap_or_else(|| { log::error!(...); HeartbeatErrorAction::Fatal(KafkaError::with_message(error, error_message.clone())) })`
   inside the `DelegateToSpecific` arm of the `final_action` match.
   The `log::error!` call mirrors Java's
   `logger.error("{} failed due to unexpected error {}: {}", ...)` at
   line 438, ensuring the unknown-code branch is loud at the log level
   too — not just routed through the side-channel.

2. Added regression test
   `issue5_unknown_error_code_falls_through_to_fatal` that:
   - Drives a `RebalanceInProgress` response through the spawned
     forwarder (`RebalanceInProgress` is in the
     "unknown to the heartbeat classifier" set — not enumerated in
     the abstract switch, not recognised by the consumer-specific
     handler).
   - Asserts exactly one `PendingMembershipTransition::Fatal(...)`
     envelope was emitted, carrying the original `RebalanceInProgress`
     error code (the catch-all preserves the broker's error code; it
     does not substitute a generic one).
   - Drives `mm.transition_to_fatal(0).await` (what the bg-task Phase
     2.4 drain does) and asserts the membership state advances to
     `Fatal`.
   - Asserts at least one `BackgroundEvent::Error` envelope was
     emitted on the background-event channel — matches Java's
     `handleFatalFailure` → `backgroundEventHandler.add(new ErrorEvent(error))`
     at `:455-458`.

### Updated classification-table catch-all row

The error-classification matrix in this file (above, Issue 4) listed
the catch-all behavior as Rust `unwrap_or(Handled)` for unknown
codes. Post-fix, the row becomes:

| Rust error code           | Rust action  | Side-channel emission | Java behaviour                                                       |
|---------------------------|--------------|-----------------------|----------------------------------------------------------------------|
| any other (response body) | `Fatal(...)` | `Fatal(...)`          | `default:` arm → `handleSpecificExceptionInResponse` returns false → `handleFatalFailure` |

This row now matches the existing transport-failure catch-all row
(both fall through to the same Fatal path on the unknown-code
boundary).

### Commits

- `fixup! Phase 12.5 (3/N): Issue 5 — unknown-error-code fatal fallback in heartbeat classifier`

## Issue 7: `CommitRequestManager` not registered as a `MemberStateListener` — RESOLVED

- **Discovered by**: Actor (running the integration suite at the end of
  Phase 12.5 commit (4/N)).
- **File**: `src/consumer/async_kafka_consumer.rs:986-1023`
- **Severity**: Bug (Behavior Mismatch with Java — silently breaks all
  `commit_sync` / `commit_async` / auto-commit calls for KIP-848
  consumers in a group).
- **Java Reference**: `RequestManagers.java:273-274` (KIP-848 /
  `consumer` group protocol arm).
- **Description**: Java registers TWO listeners on the membership
  manager:

  ```java
  membershipManager.registerStateListener(commitRequestManager);
  membershipManager.registerStateListener(applicationThreadMemberStateListener);
  ```

  `commitRequestManager` implements `MemberStateListener` — its
  `onMemberEpochUpdated(epoch, memberId)` writes the broker-assigned
  UUID into its internal `MemberInfo`, which is then read at
  `OffsetCommitRequest` build time.

  Pre-fix, Rust's `new_with_components` registered only the
  `state_notifier` (Java's `applicationThreadMemberStateListener`
  equivalent). The `CommitRequestManager.member_info.member_id` stayed
  at the default empty string, so every OffsetCommit went out with the
  wrong member id and the broker returned `UNKNOWN_MEMBER_ID`. The
  integration test `test_commit_sync_then_resume_in_same_group` was the
  first thing to drive the full
  `ConsumerMembershipManager::on_heartbeat_success` →
  listener-fanout → commit-request-build chain end-to-end, exposing the
  gap.

### Fix

`src/consumer/async_kafka_consumer.rs` `new` ctor: register `commit`
on `membership.abstract_mm` as a `MemberStateListener` BEFORE
registering `state_notifier`, mirroring Java's exact two-line ordering.
The `commit` Arc was moved into `RequestManagers::new` earlier in the
ctor, so the listener handle is read back via
`request_managers.lock().commit_handle()`.

### Regression test

`issue_7_commit_request_manager_registered_as_member_state_listener`
(`src/consumer/async_kafka_consumer.rs`):

1. Builds an `AsyncKafkaConsumer` via the production `new(config)` ctor
   (with `group.id` set and `group.protocol=consumer`, pointed at a
   refused localhost port — the smoke-test pattern).
2. Reaches into `consumer.request_managers.lock().{commit, consumer_membership}`.
3. Asserts `commit.member_info_for_test().member_id == ""` pre-update.
4. Captures `membership.abstract_mm.inner.lock().member_id` (the
   auto-generated UUID).
5. Calls `update_member_epoch(42)` on the membership inner — this
   fans out to all registered listeners.
6. Asserts `commit.member_info_for_test().member_id` now equals the
   membership manager's UUID, AND
   `member_info.member_epoch == Some(42)`.

Reverting the registration in `new_with_components` makes this test
fail with `left: "" / right: "<uuid>"`. Verified locally.

### Commits

- `fixup! Phase 12.5 (3/N): Issue 7 — register CommitRequestManager as MemberStateListener`
