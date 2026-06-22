---
name: phase10-commit-6-notes
description: Milestone-8 Phase 10 commit 6/N — ApplicationEventProcessorTest translation (full Java parity), reusable patterns for mocking-less commit/offset stubs and Mockito-style dispatch verifications
metadata:
  type: project
---

# Phase 10 (6/N): ApplicationEventProcessorTest — translate all in-scope tests

## What landed

`src/consumer/internals/events/application_event_processor.rs` — full
Java-parity translation of [[application-event-processor]] tests:

- 18 new tests covering commit happy/error paths, unsubscribe with
  group-id, AsyncPoll full chain, pattern-subscription version-gating,
  refresh-committed-offsets failure, and Mockito-style dispatch
  verifications.
- 4 Streams tests skipped (§20 — out of scope).

## Patterns worth recording

### 1. `complete_first_unsent_*_for_test` helpers — Mockito stub replacement

Java tests stub manager methods via Mockito; Rust uses real managers.
For commit / offset-fetch happy-path tests where the spawned task awaits
a manager future, the test must complete that future from the test side.

Two new helpers in `CommitRequestManager`:

  - `complete_first_unsent_commit_for_test(offsets)` — pop the first
    queued `OffsetCommitRequestState` and resolve it with the given map.
  - `fail_first_unsent_commit_for_test(err)` — same shape, but resolves
    with `complete_err`.

These mirror the existing `complete_first_unsent_fetch_for_test` from
the Phase 7d work. The pattern: the AEP spawns a task that awaits the
manager future; the test's `yield_until` loop polls the manager state,
calls the helper as soon as the unsent request is queued, and then
awaits the primary handle. Drop-the-lock-before-await discipline is
preserved — the helper takes the state lock, pops the request, drops
the guard, then completes the sender.

### 2. `set_cached_update_positions_exception_for_test` — error-injection

For tests that need `update_fetch_positions` to fail (e.g.
`testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout`),
Java mocks the manager to return a failed future. Rust adds a
`#[cfg(test)] pub(crate)` setter on `OffsetsRequestManager` that
pre-populates `cached_update_positions_exception`. The next call to
`update_fetch_positions` takes the cached exception and routes it
through the spawned task's error path, ending up in `state.error()`.

### 3. `setup_processor_with_fetch(group_id, with_fetch)` — fixture extension

Phase 10 commit 4/5's `setup_processor(with_group_id)` left fetch=None
because the sync arms didn't need it. Commit 6's AsyncPoll happy-path
test needs a real `FetchRequestManager` because the spawned task awaits
the `create_fetch_requests()` ack receiver. Solution: extend the
fixture builder. `setup_processor` is now a thin wrapper around
`setup_processor_with_fetch(group_id, false)`.

The real fetch manager's ack receiver only resolves when `poll()` is
called on the manager (with no fetchable partitions, the ack resolves
to `Ok(())`). The test spawns a "driver" tokio task that periodically
locks `RequestManagers`, calls `RequestManager::poll(fetch_mgr, now)`,
drops the lock, and sleeps 5 ms. Once the AEP-spawned task observes
state.is_complete, the driver is aborted.

### 4. `yield_until` helper — predicate spin

Several tests need to wait for spawned continuations to make progress.
The shared `yield_until(predicate, timeout)` async helper spins every
5 ms up to the timeout. Used both for "request was queued" detection
(commit helper return value) and "state was completed" assertions.

### 5. Mockito dispatch-verification tests → enum-match smoke tests

Java's `testApplicationEventIsProcessed` (parameterized) and
`testListOffsetsEventIsProcessed` (parameterized) are pure
dispatch-overload checks via Mockito. In Rust the enum match is
exhaustive (compiler-checked), so the equivalent translation is:
construct a representative event of each kind, invoke `processor.process`,
and observe the receiver resolves without panic. We use a
parameterized loop for the boolean parameter (per DoD §3
`@ParameterizedTest` translation rule), and a single function with
multiple dispatches for the variadic case.

### 6. `metadata.bootstrap(Vec::new())` to advance `update_version`

Java stubs `metadata.updateVersion()` to return controlled values.
Rust uses `metadata.metadata_arc().bootstrap(Vec::new())` to advance
the internal counter. Calling `bootstrap` with an empty address list
bumps `update_version` by 1 and is otherwise harmless for the AEP
unit tests. Used by
`update_pattern_subscription_event_only_takes_effect_when_metadata_advances`.

### 7. `publish_topic_metadata` — cluster topic population

For tests that exercise pattern subscription's
`subscribeFromPattern(cluster.topics())` flow, the cluster must contain
a matching topic. We construct a synthetic `MetadataResponse` with one
topic + one partition + one broker, then call
`metadata.metadata_arc().update_with_current_request_version(...)` to
install it. This is the same pattern used by
`consumer_metadata::tests::test_pattern_subscription`.

### 8. `metadata_version_snapshot` direct write in tests

The `testUpdatePatternSubscriptionNotInvokedWhenMetadataNotUpdated`
test needs the processor's snapshot in sync with the current
metadata `update_version` so the gating check skips the rebuild. The
test reaches into the processor's private field directly:
`processor.metadata_version_snapshot = fx.metadata.update_version();`.
Allowed because tests are in the same `mod tests` block as the impl.

### 9. Java assertions that map to Rust behaviour-observation

Java tests use `verify(membershipManager).leaveGroup()` etc. — direct
mock-call verification. Rust observes the side effect (`unsubscribe`
clears the subscription, `leave_group` resolves the handle). This is a
STRONGER guarantee than Java's mock-call check because we're running
the real implementation, not stubbing it out. The trade-off: less
direct verification of "which method was called", more verification of
"the right state transitions happened".

### 10. `refresh_committed_offsets_*` tests reveal an `is_ignorable_async_poll_error` subtlety

Java's `assertFalse(event.error().isEmpty())` only holds because the
Java test uses `new Throwable("Intentional failure")` (NOT a
`TimeoutException`). Our `is_ignorable_async_poll_error` returns true
ONLY for timeouts; a generic `IllegalStateException` propagates to
`state.error()` as Java intends. The test asserts the error message
content so a regression that broadens the ignorable predicate (e.g. to
"all retriable errors") would fail.

## Tests inventory

Java tests examined: 39 (36 `@Test` + 3 `@ParameterizedTest`).
Skipped (§20 Streams): 6.
In-scope: 33.
Rust translated total: 52 (commits 4/5 contributed 34; commit 6 adds 18).

Streams skips:
  - `testStreamsOnTasksRevokedCallbackCompletedEvent`
  - `testStreamsOnTasksRevokedCallbackCompletedEventWithoutStreamsMembershipManager`
  - `testStreamsOnTasksAssignedCallbackCompletedEvent`
  - `testStreamsOnTasksAssignedCallbackCompletedEventWithoutStreamsMembershipManager`
  - `testStreamsOnAllTasksLostCallbackCompletedEvent`
  - `testStreamsOnAllTasksLostCallbackCompletedEventWithoutStreamsMembershipManager`

## Deferrals (still owned by later commits)

  - **Commit 7/N (PLAN.md)**: bg-task `run_once` loop.
  - **Phase 11**: AsyncKafkaConsumer.commit_async callback wrapper.
