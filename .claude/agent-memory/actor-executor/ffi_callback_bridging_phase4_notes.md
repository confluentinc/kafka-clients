---
name: ffi-callback-bridging-phase4
description: FFI callback-bridging Phase 4 — ConsumerHandle_t guard bypass, Handle::block_on in-runtime rejection instead of panic, using a Phase-3 commit callback as a deterministic "guard is held" probe
metadata:
  type: project
---

Phase 4 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `942056c` on `ffi-callback-bridging`: `kafka_consumer_ConsumerHandle_t`
in the new `src/ffi/consumer_handle.rs` (23 exported functions).

**Why:** C/Python callbacks need a reentrancy path into the consumer; the plain
`kafka_consumer_Consumer_*` surface rejects reentrant calls with
`ConcurrentModification` by design.

**How to apply** — findings that carry into Phases 5-6:

1. **`FfiConsumerHandle::consumer_handle` IS `Consumer::handle()`**, captured
   once at construction in both `KafkaConsumer_new` and `MockConsumer_new`, so
   cloning it needs no access guard. Added `pub(crate) clone_core_handle` in
   `consumer.rs` returning `(ConsumerHandle, runtime_handle)` — smaller diff
   than widening the struct/field visibility, and the new module still owns the
   wrapper + docs.

2. **`Handle::block_on` panics inside a runtime; return an error instead.**
   Every blocking entry point calls `Handle::try_current().is_ok()` first and
   returns `illegal_state` — a panic crossing `extern "C"` is worse than an
   error, and this is reachable in practice through the teardown
   `enqueue_or_run_inline` fallback (plan risk 3). Phase 6's Python
   `py_ConsumerHandle_*` wrappers inherit this behavior; the sync getters do
   NOT block so they still work in an async context.

3. **The Phase-3 commit callback is a deterministic "the guard is held right
   now" probe.** `commit_async_with_callback` goes through `sync_void_op`, so
   the app thread holds the owner guard for the whole call, and the mock fires
   `on_complete` inline → the C callback runs on the dispatcher thread inside
   that window. From there `Consumer_commit_sync` returns
   ConcurrentModification (code -1) while `ConsumerHandle_*` ops do not — the
   guard-bypass proof needs no threads, sleeps or `wait_for`. Phase 5's
   "listener calls `ConsumerHandle_commit_sync` without deadlock" test can use
   the same shape.

4. **Mock-derived handles cover only the `Mock` arm.** Everything mock-based
   returns `unsupported_version` ("not supported on a MockConsumer handle"), so
   add at least one broker-free real-`KafkaConsumer` test: the handle shares the
   consumer's `SubscriptionState`, so `subscribe()` on the consumer is visible
   through `ConsumerHandle_subscription`, and `position()` on an unassigned
   partition fails immediately with "partitions assigned to this consumer"
   (no broker round trip). That exercises the `Async` arm in milliseconds.

5. The plan's "15 async ops" undercounts — the core handle has **17**
   (`commit_sync`/`commit_sync_offsets`/`commit_async`/`commit_async_offsets`
   are four, not two).

Environment notes from Phases 1-3 all still held (no cmake — compile the Unity
suites directly against `target/release/libconfluent_kafka.a`; no venv;
`cargo xtask lint` skips the `ffi` feature so run
`cargo clippy --all-targets --features ffi -- -D warnings` separately).

See [[ffi-callback-bridging-phase1]], [[ffi-callback-bridging-phase2]],
[[ffi-callback-bridging-phase3]].
