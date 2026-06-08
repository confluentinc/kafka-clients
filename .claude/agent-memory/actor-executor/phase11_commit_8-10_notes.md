---
name: phase11-commit-8-10-notes
description: Phase 11 commits 8-10 — AsyncKafkaConsumerTest translation patterns; bg-task drainer-task pattern for assertions
metadata:
  type: feedback
---

# Phase 11 commit (8/10/N) AsyncKafkaConsumerTest translation patterns

Three commits translated the Java test file
(`AsyncKafkaConsumerTest.java`, 2226 LOC, 98 tests) into the inline
test module on `async_kafka_consumer.rs`. Reusable patterns:

## 1. Drainer-task pattern for end-to-end event assertions

**Why**: Java's Mockito stubs immediately satisfy `addAndGet(...)` on
the mocked `ApplicationEventHandler`. The Rust translation has a
single-task test fixture without a bg task — events are sent on the
mpsc channel and the test acts as a fake bg task by spawning a drainer
task that pulls events and completes their handles.

**How to apply**: For tests that need to observe a specific event AND
let the app-side call return, spawn a drainer task BEFORE invoking the
consumer API:

```rust
let drainer = tokio::spawn(async move {
    while let Some(env) = handles.app_event_rx.recv().await {
        match env.event {
            ApplicationEvent::TargetEvent { handle, .. } => {
                // capture observable side-effects via Arc<Mutex<...>>
                handle.complete(value);
                return;
            },
            _ => {},
        }
    }
});

consumer.api_call().await.expect("ok");
drainer.await.expect("drainer ok");
```

For tests that run the close-path (which drops the application event
sender), use `drop(consumer)` to let the drainer's `recv().await`
terminate, then `let _ = drainer.await;`.

## 2. `@ParameterizedTest` becomes one test method per parameter

**Why**: Per CLAUDE.md Tests section: "@RepeatedTest(N) annotations
become loops in Rust, not single invocations." `@ParameterizedTest`
with discrete parameter values translates to multiple test methods
(one per value), which gives JUnit-like test discovery in cargo test.

**How to apply**: Extract the test body into a private async helper
function, then add per-parameter `#[tokio::test]` wrappers.

```rust
async fn close_leaves_group_for_timeout_inner(timeout_ms: u64) {
    // ... shared body ...
}

#[tokio::test]
async fn close_leaves_group_timeout_zero() {
    close_leaves_group_for_timeout_inner(0).await;
}

#[tokio::test]
async fn close_leaves_group_timeout_default() {
    close_leaves_group_for_timeout_inner(DEFAULT_CLOSE_TIMEOUT_MS as u64).await;
}
```

## 3. Exact-message assertions for DoD §3 compliance

**Why**: DoD §3: "Error message content is asserted, not just is_err()
— error messages are part of the behavioral contract." A typo in the
production error message slips past `msg.contains(...)` substring
checks.

**How to apply**: Use `assert_eq!` against the Java string verbatim.
For `KafkaError` variants:

```rust
let err = consumer.api_call().await.expect_err("must err");
match err {
    KafkaError::IllegalArgument(msg) => {
        assert_eq!(msg, "Topic pattern to subscribe to cannot be empty");
    },
    other => panic!("expected IllegalArgument, got {other:?}"),
}
```

## 4. SKIP rationale block at the TOP of each commit's test block

**Why**: DoD §3 requires "one-line rationale" for each skipped Java
test. Placing SKIP comments individually next to each Java method
(rather than at the top of the block) splinters the rationale across
the file. The Critic walks the entire test block looking for "what's
covered, what's deferred, why."

**How to apply**: Open each commit's test section with a block comment
that lists ALL skipped Java tests with their one-line rationale.
Reference PLAN.md deferral numbers explicitly.

```rust
// ─── Phase 11 commit (N/N) Java test translations: <area> ───
//
// SKIPs (commit N batch):
//   - testFoo — PLAN deferral #X (<reason>).
//   - testBar — covered by inline `existing_test_name`.
//   - testBaz — Phase 12 (integration tests).
```

## 5. The `OffsetCommitCallbackInvoker` callback fires through a
   chain: enqueue + invoke_pending_callbacks

**Why**: `commit_async_offsets_with_callback` enqueues the user
callback on the `OffsetCommitCallbackInvoker` AFTER the spawned
continuation completes. Tests asserting the callback fired need to:
  1. Await the pending-async-commit oneshot (drains the spawned
     continuation that calls `enqueue_user_callback_invocation`),
  2. Call `invoke_pending_callbacks()` explicitly (Java's
     `forceCommitCallbackInvocation` helper).

**How to apply**:

```rust
consumer.commit_async_offsets_with_callback(offsets, cb).await.expect("ok");

// Step 1: drain the spawned continuation
if let Some(rx) = consumer.last_pending_async_commit.take() {
    let _ = rx.await;
}

// Step 2: invoke pending callbacks
consumer.offset_commit_callback_invoker.invoke_pending_callbacks().await;
```

## 6. `make_completable_event` lives in
   `consumer::internals::events::completable_event::make_completable_event`

**Why**: There is no top-level re-export of this function on the
`events` module. Importing as
`crate::consumer::internals::events::make_completable_event` fails.

**How to apply**: Either use the full path
`crate::consumer::internals::events::completable_event::make_completable_event`,
or add a `use` at the top of the test module.

## 7. `OffsetAndTimestamp::new(offset, timestamp)` returns
   `Result<Self, KafkaError>`

**Why**: Unlike Java's eager constructor, the Rust constructor
validates inputs (negative offsets / timestamps). Tests that fabricate
test data must `.expect("ok")` the result.

**How to apply**:

```rust
Some(crate::consumer::OffsetAndTimestamp::new(5, 1).expect("ok"))
```
