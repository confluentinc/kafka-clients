# Translation Plan: KAFKA-19481 — Fix flaky test testConsumerGroupHeartbeatWithRegex

**AK commit:** `3f75f450ad86267771f1444e9579de77ae948ccc`
**AK branch:** trunk
**PR:** #58
**Rust branch:** `kafka-translate/3f75f450ad86267771f1444e9579de77ae948ccc`

---

## Summary of the Apache Kafka Commit

This is a **test-only fix**. No production code was changed.

The fix addresses a flaky integration test
`AuthorizerIntegrationTest.testConsumerGroupHeartbeatWithRegex` which fails
intermittently with:

```
org.opentest4j.AssertionFailedError: Unexpected assignment
ConsumerGroupHeartbeatResponseData(..., assignment=null)
==> expected: not <null>
```

**Root cause:** The test depends on the server-side async operation
`refreshRegularExpressions` (inside `GroupMetadataManager`) completing before
the second heartbeat assertion runs. That operation resolves regex-based topic
subscriptions to matching topic names. When the async work finishes, the broker
advances the member epoch from 1 → 2 via `handleRegularExpressionsResult`. If
the assertion fires before that step, the assignment is still `null`.

**Fix applied in AK:**
- Changed `val response` to `var response` so the retry loop can rebind it.
- Wrapped `sendAndReceiveRegexHeartbeat(response, listenerName, Some(1))` in
  `TestUtils.tryUntilNoAssertionError()` which retries the assertion block with
  a 100 ms polling interval up to a 15-second deadline, swallowing
  `AssertionError` on each attempt and propagating any final failure.

**Changed file:**
```
core/src/test/scala/integration/kafka/api/AuthorizerIntegrationTest.scala
```

---

## Rust Translation Analysis

### Does the test exist in Rust?

No. The Rust codebase does not currently have an equivalent of
`AuthorizerIntegrationTest`. The current integration tests
(`tests/integration/`) cover:

- TCP connection establishment
- `ApiVersions` negotiation
- `Metadata` queries
- SSL and SASL PLAIN authentication
- Producer functionality

Consumer group heartbeat (`ConsumerGroupHeartbeat` API key 68) and
authorizer/ACL integration testing are not yet implemented in the Rust client
or its test suite.

### Is there production code to translate?

No. The AK commit touches only a Scala test file. No Rust library source files
need to change.

### What needs to be done?

Because the test being fixed does not exist in Rust, the commit is not directly
applicable today. However, this commit establishes a **test pattern** that must
be followed when consumer group heartbeat tests are eventually ported:

1. **Add `try_until_no_assertion_error` helper** to the shared test utilities
   in `tests/common/`. This is the Rust equivalent of
   `TestUtils.tryUntilNoAssertionError()` and will be needed by any future
   consumer-group integration test that touches async server-side operations.

2. **Document the pattern** for future translators: whenever a Rust integration
   test asserts on a response field that depends on a server-side async
   operation completing (e.g. topic-regex resolution, partition reassignment),
   the assertion must be wrapped in `try_until_no_assertion_error`.

---

## Implementation Plan

### Phase 1 — Add `try_until_no_assertion_error` to test utilities

**File:** `tests/common/mod.rs` (or a new `tests/common/test_utils.rs`)

Add an async helper:

```rust
/// Retry `f` until it returns `Ok(T)` or the deadline is exceeded.
///
/// Equivalent to `kafka.utils.TestUtils.tryUntilNoAssertionError` in the
/// Apache Kafka Scala test utilities. Polls every `pause` until `wait_time`
/// elapses, then propagates the last error.
pub async fn try_until_no_assertion_error<F, Fut, T, E>(
    wait_time: Duration,
    pause: Duration,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let deadline = tokio::time::Instant::now() + wait_time;
    let mut last_err;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                last_err = e;
                if tokio::time::Instant::now() >= deadline {
                    return Err(last_err);
                }
                tokio::time::sleep(pause).await;
            }
        }
    }
}
```

Default constants (matching the Java/Scala values):
- `wait_time = Duration::from_millis(15_000)`
- `pause = Duration::from_millis(100)`

### Phase 2 — Tests

Add a unit test for `try_until_no_assertion_error` itself:
- Verify it returns `Ok` on the first passing attempt.
- Verify it retries on transient failures and eventually returns `Ok`.
- Verify it returns the final `Err` when the deadline is exceeded.

No integration-test changes are needed at this stage (the consumer group
heartbeat tests are out of scope for this PR).

### Phase 3 — No-op confirmation

Confirm via `cargo build` and `cargo test` that:
- The existing test suite is still green.
- The new helper compiles and its unit tests pass.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| `tests/common/test_utils.rs` | Create (or extend `mod.rs`) | Add `try_until_no_assertion_error` async helper |
| `tests/common/mod.rs` | Modify | Re-export new module / function |

No changes to `src/` (library code) are required.

---

## Out of Scope

- Translating `AuthorizerIntegrationTest` itself — that requires consumer group
  heartbeat support, ACL infrastructure, and a KRaft-capable test broker, none
  of which are present yet.
- Any changes to the `ConsumerGroupHeartbeat` request/response generated types
  (those already exist via the code generator from the JSON spec).

---

## Definition of Done

- [ ] `try_until_no_assertion_error` helper added to `tests/common/`.
- [ ] Unit tests for the helper pass (`cargo test`).
- [ ] `cargo build` succeeds with no warnings.
- [ ] `cargo xtask lint` passes.
- [ ] `cargo xtask format-check` passes.
