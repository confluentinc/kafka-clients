# Milestone-8 Phase-6 review (N=1) — RESOLVED items

Issues from `COMMENTS.1.md` that have been addressed. Original review text
is preserved verbatim; each entry ends with a **Resolution** note.

---

## Finding 1: `on_failed_response` mis-classifies `KafkaError::Timeout` as fatal — Java treats it as retriable

- **File**: `src/consumer/internals/coordinator_request_manager.rs:194-212`
- **Severity**: Behavior Mismatch (blocking for Phase 10)
- **Java Reference**: `CoordinatorRequestManager.java:199-217`

**Description**

Java's `onFailedResponse(currentTimeMs, Throwable)` checks
`exception instanceof RetriableException`. `TimeoutException` extends
`RetriableException`, so a timeout failure goes down the "retriable" branch:
it does **not** set `fatalError`. The Rust translation calls
`error.is_retriable()` which delegates to `self.kafka_error()`. Looking at
`src/common/kafka_error.rs:378-383`, `KafkaError::Timeout(_)` returns `None`
from `kafka_error()`, so `is_retriable()` returns **`false`** for
`KafkaError::Timeout`. This means routing a timeout through
`on_failed_response` will:

1. Mark coordinator unknown (matches Java — both paths do this).
2. Skip the retriable branch (Java would have taken it).
3. Skip the `GroupAuthorizationFailed` branch.
4. Fall through to `self.fatal_error = Some(error)` — set a fatal error.

Java does NOT set `fatalError` on `TimeoutException`. Once Phase 10 wires
the bg task to route the `oneshot::Receiver`'s `Err(KafkaError::Timeout)`
through `on_failed_response`, a transient timeout will be permanently
classified as a fatal error — surfaced through `fatal_error()` to the
heartbeat / commit managers, which propagate it to the application thread.

**Why the existing tests miss this:** `testNetworkTimeout` (the
Coordinator-side test that supposedly exercises this) manually calls
`mark_coordinator_unknown` and `request_state.on_failed_attempt`, bypassing
`on_failed_response` entirely. The Java test routes the failure through
`whenComplete` → `onFailedResponse`, which is the actual production path.

**Expected**

`KafkaError::is_retriable()` returns `true` for `KafkaError::Timeout(_)`,
matching Java's class hierarchy (`TimeoutException extends
RetriableException extends ApiException`). Alternatively, the
`CoordinatorRequestManager` checks `matches!(error, KafkaError::Timeout(_))`
explicitly alongside `error.is_retriable()`.

**Actual**

`KafkaError::Timeout(_).is_retriable()` returns `false`. A `Timeout`
routed through `on_failed_response` is permanently classified as fatal.

**Resolution**: `KafkaError::is_retriable()` now special-cases
`Self::Timeout(_) => true` alongside the existing
`kafka_error().is_some_and(|e| e.is_retriable())` fallthrough. Regression
test `test_on_failed_response_timeout_is_retriable_not_fatal` in
`CoordinatorRequestManager` routes a `KafkaError::timeout(...)` through
`on_failed_response` and asserts `fatal_error()` is `None`. See commit
`0b7f296` (`Phase 6 fixup: KafkaError::Timeout is_retriable`).

---

## Finding 2: Missing translation of `NetworkClientDelegateTest.testTimeoutBeforeSend` and `testTimeoutAfterSend`

- **File**: `src/consumer/internals/network_client_delegate.rs` (tests module, lines 759-1012)
- **Severity**: Missing Requirement (high)
- **Java Reference**: `NetworkClientDelegateTest.java:125-150`

**Description**

Two key tests from `NetworkClientDelegateTest` are not translated:

- `testTimeoutBeforeSend` (lines 125-137): enqueues a request, the target
  node is unreachable, advances time past `REQUEST_TIMEOUT_MS`, polls
  again, asserts the future resolves with a `TimeoutException`.
- `testTimeoutAfterSend` (lines 139-150): enqueues a request, sends it,
  advances time past `REQUEST_TIMEOUT_MS`, polls again, asserts the
  future resolves with a `DisconnectException`.

These exercise the `try_send` expiry branch and the `check_disconnects`
path — both implemented in Rust at `src/consumer/internals/network_client_delegate.rs:642-650`
(expiry timeout) and `:717-738` (disconnect handling). The current Rust
test suite has neither, so the expiry timeout path is **completely
untested** despite being implemented.

DoD §3 requires translating all Java tests unless explicitly justified as
not relevant. The Phase-6 plan does not list these as out-of-scope.

**Expected**

Both tests translated. `testTimeoutBeforeSend` should:
- Mark the node unreachable on `MockClient` (or equivalent stub).
- Add the request.
- Advance the time provider past `request_timeout_ms`.
- Poll the delegate.
- Assert the response receiver resolves with `Err(KafkaError::Timeout)`.

**Actual**

Neither test exists in Rust. The expiry code at lines 642-650 has no
direct test coverage.

**Resolution**: Both tests translated as `test_timeout_before_send` and
`test_timeout_after_send` in the same module. The first marks the sole
node unreachable, advances time past `REQUEST_TIMEOUT_MS`, polls, and
asserts the receiver resolves with `KafkaError::Timeout`. The second
dispatches the request successfully, advances time past the timeout, and
asserts the receiver resolves with a `KafkaError` whose `error()` is
`Errors::NetworkException` (Rust's analog of Java's
`DisconnectException`, surfaced via `MockClient`'s
`checkTimeoutOfPendingRequests` → `disconnect_node` →
`was_disconnected=true` ClientResponse → `FutureCompletionHandler::on_complete_ref`
→ `on_failure(NetworkException)`). See commit `91c104c`.

---

## Finding 3: Missing translation of `testEnsureCorrectCompletionTimeOnComplete`, `testPollWithOnClose`, `testCheckDisconnectsWithOnClose`

- **File**: `src/consumer/internals/network_client_delegate.rs` (tests module)
- **Severity**: Missing Requirement (medium)
- **Java Reference**: `NetworkClientDelegateTest.java:163-171, 287-329`

**Description**

Three additional tests from `NetworkClientDelegateTest` are not translated:

- `testEnsureCorrectCompletionTimeOnComplete` (lines 163-171): asserts
  `handler.completionTimeMs()` reflects the response's `receivedTimeMs`
  on the success path (`onComplete`). The mirror test
  `testEnsureCorrectCompletionTimeOnFailure` IS translated; its sibling
  is not. Rust's `on_complete` sets `completion_time_ms` at line 321
  (`self.set_completion_time(completion_time_ms)`) — exercised path,
  no direct test.

- `testPollWithOnClose` (lines 287-305): exercises
  `NetworkClientDelegate::poll(timeoutMs, currentTimeMs, true)` — the
  `on_close = true` branch. Rust translates this branch in
  `check_disconnects` at line 729-734, but only via the metadata-error
  tests (which exercise a different branch).

- `testCheckDisconnectsWithOnClose` (lines 307-329): explicitly tests
  the `node == None && on_close` branch of `check_disconnects` —
  unsent request with no node assignment, polled on close, expected to
  be removed with `NetworkException`. Rust code at line 729-734 does
  exactly this; no test covers it.

These are not "supplier-related" or "metrics-related" cases that the
Phase 6 plan defers — they exercise core Rust code paths.

**Expected**

Three tests translated. `testCheckDisconnectsWithOnClose` should be
straightforward — add request with `node: None`, poll with `on_close =
true`, assert receiver resolves with `KafkaError::new(Errors::NetworkException)`.

**Actual**

None of the three are present.

**Resolution**: All three tests translated in the same module:
`test_ensure_correct_completion_time_on_complete` builds a synthetic
non-disconnected `ClientResponse`, calls `handler.on_complete(...)`, and
asserts `completion_time_ms()` reflects the response's
`received_time_ms`. `test_poll_with_on_close` dispatches a request, calls
`poll_on_close`, verifies the in-flight survives (its node was resolved
via `least_loaded_node` and never written back to the `UnsentRequest`'s
`node` field, so the `None if on_close` branch does not match), and then
drains via `client.respond(...)`. `test_check_disconnects_with_on_close`
exercises the `None if on_close` branch directly: unsent request with no
node, polled on close → completed with `NetworkException`. See commit
`91c104c`.

---

## Finding 4: `testMarkCoordinatorUnknownLoggingAccuracy` translation asserts nothing

- **File**: `src/consumer/internals/coordinator_request_manager.rs:354-370`
- **Severity**: Behavior Mismatch / Trivial-passing test (medium)
- **Java Reference**: `CoordinatorRequestManagerTest.java:84-114`

**Description**

The Java test uses a `LogCaptureAppender` to assert that
`markCoordinatorUnknown` emits the right warning log entries at the
right times — specifically `assertEquals(oneMinute, firstLogMs.get())`
after one minute of disconnection and `assertEquals(oneMinute * 2, ...)`
after two.

The Rust translation calls `mark_coordinator_unknown(...)` three times,
but has no log-capture assertion, no state assertion on the manager
(neither `coordinator()`, `fatal_error()`, nor any internal state), nor
any other invariant check. The comment at line 348-353 acknowledges
this:

> We don't capture log output here; instead we exercise the timing
> invariants [...] by calling `mark_coordinator_unknown` repeatedly and
> checking the internal state via the public accessors.

But the test doesn't actually check any internal state — the only
externally-visible state is `coordinator()` (always `None` because we
never enter the if-branch that has a coordinator) and `fatal_error()`
(always `None`). The test would pass if `mark_coordinator_unknown` were
a no-op.

This is the same anti-pattern flagged in M8 Phase 2 — trivial-passing
tests where the assertion is structurally always-true. Per DoD §3 and
test_coverage_gap memory: assertions must be load-bearing.

**Expected**

Either:
1. Use `tracing-test` (or similar capture crate) to assert log output.
2. Make `total_disconnected_min` and `time_marked_unknown_ms`
   `pub(crate)` for tests so the state transitions can be asserted
   directly (state assertions: after step 1 `total_disconnected_min ==
   0`; after step 2 `total_disconnected_min == 1`; after step 3
   `total_disconnected_min == 2`).
3. Mark the test as `#[ignore]` with a comment explaining why a Rust
   log-capture is deferred.

**Actual**

Test passes trivially — no meaningful assertion.

**Resolution**: Took Option 2: the test now reads `time_marked_unknown_ms`
and `total_disconnected_min` directly (the test module is in the same
file, so no visibility relaxation is required). Assertions cover the
three documented transitions:

- After mark at `t=0`: `time_marked_unknown_ms == 0`,
  `total_disconnected_min == 0` (duration 0 < 60_000).
- After mark at `t=60_000`: anchor unchanged, `total_disconnected_min == 1`
  (one warning would fire).
- After mark at `t=120_000`: anchor unchanged, `total_disconnected_min == 2`
  (another warning).

See commit `bf30a2a` (`fixup! a8d1c5d`).

---

## Finding 5: `RequestManagers` skeleton tests omit "Java has no analog" disclosure

- **File**: `src/consumer/internals/request_managers.rs:117-165`
- **Severity**: Minor / Documentation
- **Java Reference**: `RequestManagersTest.java:42-131`

**Description**

The Java `RequestManagersTest` has two cases (`testMemberStateListenerRegistered`,
`testStreamMemberStateListenerRegistered`) — both exercise the supplier
factory. The Phase 6 plan explicitly defers these to Phase 10 with the
supplier (PLAN.md:78-81: "supplier-exercising cases defer to Phase 10").
DoD §3 requires "explain why they are not relevant and why they can be
skipped" for skipped tests.

The Rust tests (`entries_empty_when_no_coordinator`,
`entries_includes_coordinator_when_present`, `close_is_idempotent`,
`entries_order_is_deterministic`) are reasonable container-shape tests
but none of them map to a Java test. The file does not contain a
one-line comment per deferred Java test (per DoD §3 rationale
requirement) at the top of the test module.

The PLAN.md does explain the deferred cases, but the test module itself
should record the same so a future reader does not need to cross-reference.

**Expected**

A comment block at the top of the `tests` module listing both Java
tests by name with a one-line rationale, matching the DoD §3
expectation:

```rust
// Java RequestManagersTest cases deferred to Phase 10 with the
// supplier factory (PLAN.md "Out of scope"):
//
// - testMemberStateListenerRegistered: requires
//   ConsumerHeartbeatRequestManager (Phase 8) + supplier (Phase 10).
// - testStreamMemberStateListenerRegistered: Streams is out of
//   milestone scope per consumer-threading.md §20.
```

**Actual**

No per-test deferral rationale in the file.

**Resolution**: Added a comment block at the top of the test module
listing both deferred Java cases with one-line rationale matching the
DoD §3 expectation. The note also clarifies that the Rust container-shape
tests (`entries_*`, `close_is_idempotent`) have no Java analog by
design — Java exposes the `Optional`s directly and `entries()` is a
Rust-only helper. See commit `0eaf42a` (`fixup! 857dde0`).
