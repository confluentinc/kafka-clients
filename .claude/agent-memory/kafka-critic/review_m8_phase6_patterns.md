---
name: m8-phase6-patterns
description: M8 Phase-6 review findings — KafkaError::Timeout vs RetriableException mismatch, NetworkClientDelegate test coverage gaps
metadata:
  type: feedback
---

Recurring Phase-6 (NetworkClientDelegate / CoordinatorRequestManager / RequestManager skeleton) patterns:

1. **`KafkaError::Timeout(_)` is NOT retriable in Rust today.** Java's
   `TimeoutException extends RetriableException`, so `instanceof
   RetriableException` is true. The Rust `KafkaError::Timeout` variant
   has no underlying `KafkaGenericError`, so `is_retriable()` returns
   `false`. Any consumer manager that uses `error.is_retriable()` to
   decide "retry vs set fatal_error" mis-classifies timeouts. Fix at
   `src/common/kafka_error.rs:422-424` (special-case `Self::Timeout`).
   Reason: Java contract; surfaces when Phase 10 wires bg-task failure
   routing.
   How to apply: review any `KafkaError::is_retriable()` callsite in
   consumer internals against Java's RetriableException hierarchy.

2. **Test coverage gap pattern for NetworkClientDelegate**: the
   timeout-before-send / timeout-after-send branches exercise expiry
   logic in `try_send`, and the `on_close=true` branch exercises
   `check_disconnects`'s second arm. Both are commonly omitted from the
   Rust port even when the production code IS translated. When
   reviewing any Java→Rust port of `NetworkClientDelegateTest`, grep
   for `testTimeout`, `testPollWithOnClose`, `testCheckDisconnectsWithOnClose`.

3. **Trivial-passing log-output tests**: when Java tests use
   `LogCaptureAppender` to assert log content, Rust translations often
   omit the assertion entirely (because log capture in Rust requires
   `tracing-test` or similar). If the test body has no other state
   assertion, it passes trivially. Look for translated tests with the
   comment "we don't capture log output" and no state assertion — flag
   them and suggest either log-capture, internal-state exposure, or
   `#[ignore]`.

4. **`whenComplete` translation pattern**: Java's
   `whenComplete(BiConsumer<Response, Throwable>)` translates to a
   `oneshot::Receiver` + `take_response_receiver()` pattern where the
   bg task routes the result to `manager.on_response` or
   `manager.on_failed_response`. The post-dispatch route is provided by
   the bg task, not the manager. This is correct; do not flag.

5. **`FutureCompletionHandler::new_with_receiver()` pattern**: this is
   the right shape for Rust because `oneshot::Sender` and `Receiver`
   are paired. Java's `new FutureCompletionHandler()` (parameterless)
   creates an internal `CompletableFuture` that doubles as both
   sender-side and receiver-side. Rust must externalize the receiver
   so the bg task / manager can `.await` it. Do not flag.

6. **`CoordinatorRequestManager::new` with `assert!(!group_id.is_empty())`**:
   acceptable. Java's `Objects.requireNonNull(groupId)` is a programmer
   error check, and panic is the Rust analog. `Result` would force every
   caller to handle the error including config-validated callers.

7. **`testMarkCoordinatorUnknownLoggingAccuracy` mis-translation**:
   identical pattern occurs in any Java test using `LogCaptureAppender`.
   The Java test is asserting that the warning *is logged* with the right
   timestamp; the Rust test that just calls the function 3 times asserts
   nothing.
