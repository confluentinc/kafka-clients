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
