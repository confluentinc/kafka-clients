---
name: review-ffi-callback-bridging
description: Critic review of the C-FFI callback-bridging layer (CallbackTarget / dispatch_and_wait / ConsumerHandle_t / rebalance listener) — release-timing contract traps and the audit heuristics that found them
metadata:
  type: project
---

Review of `aa65232..HEAD` on `ffi-callback-bridging` (Phases 1-5:
`kafka_common_KafkaError_new`, `CallbackTarget`, `dispatch_and_wait`,
`Producer_send_with_callback`, `Consumer_commit_async_with_callback`,
`kafka_consumer_ConsumerHandle_t`, `ConsumerRebalanceListener_t`,
`MockConsumer_rebalance`). Unsafe soundness, guard pairing, cbindgen
completeness, and the §31-adapted C tests were all clean; the four findings were
all *contract/doc vs. implementation* divergences.

**Why these matter:** the FFI hands `user_data` ownership to Rust and the C/Python
binding balances its refcount off the `user_data_destroy` hook, so the exact
*timing* of that hook is a load-bearing part of the public contract — not a doc
detail.

**How to apply — audit heuristics that paid off here:**

1. **"Consumed unconditionally, released on any error path" is almost always
   wrong.** Java `SubscriptionState.subscribe(...)` calls
   `registerRebalanceListener(listener)` *before* `setSubscriptionType(...)`,
   which is the call that throws. So the core keeps the registration even when
   the FFI returns an error → the destroy hook fires later (at replacement /
   consumer destroy), not "right away". Check the *order* of register-vs-validate
   in the core before believing an FFI release-timing claim.
2. **Look for a *successful* call that also releases.** `subscribe_internal_topics`
   short-circuits `topics.is_empty()` into `unsubscribe()` and drops the listener
   while returning `Ok`. `MockConsumer::subscribe_with_listener` has no such
   short-circuit — so the same FFI call releases on a real consumer and does not
   on the mock. Enumerate release triggers from the core, not from the doc list.
3. **A destroy-hook test that only drives guard rejection proves nothing.**
   Guard rejection happens *before* the op closure runs, so the capturing closure
   is simply dropped — the easy path. Demand a case that reaches the core and
   fails *there* (e.g. `assign()` then `subscribe_with_listener()`).
4. **Callback obligation across `RecordAccumulator::append`.** `do_send_bytes`
   moves the `Callback` into `append`; `append`'s `?` on
   `free.allocate(...).await` (buffer exhaustion / `max.block.ms`) drops it
   unfired, and `do_send_bytes` then maps that api-exception to
   `Ok(KafkaFuture::failed(...))`. Java's `doSend` catch(ApiException) *does* fire
   `callback.onCompletion(nullMetadata, e)`. So "invoked exactly once" is false on
   that path for both `send_async` and the new `send_with_callback`.
5. **Cross-check sibling docs for the reentrancy carve-out.** The rebalance
   listener typedef points at `ConsumerHandle_t`; the commit-callback doc says a
   blanket "must not call back into this consumer" — even though a C test proves
   handle ops work from a commit callback. Java's `acquire()` is reentrant
   same-thread, so Java users *do* call `consumer.*` from `onComplete`.

**Non-issues I verified and should not re-report:**

- `ConsumerHandle::commit_sync` deliberately does **not** drain background events
  (`async_kafka_consumer.rs` ~426), so the sanctioned reentrancy path cannot
  recursively re-enter the single dispatcher FIFO. No self-deadlock.
- `invoke_pending_callbacks` has only 3 production call sites, all app-side
  `&mut self` methods — never the bg task, so the commit adapter's
  `dispatch_and_wait` can't freeze the loop that must service it.
- `ensure_blocking_allowed()` (`Handle::try_current().is_ok()`) covers every
  `ConsumerHandle_t` entry point that calls `block_on`; the dispatcher thread
  never carries a runtime context (proved by the reentrancy C test reaching the
  core instead of getting `IllegalState`).
- `unsubscribe`/`close` keeping the registered listener is Java-faithful
  (`SubscriptionState.java` `unsubscribe()` does not clear `rebalanceListener`).
- `MockConsumer_rebalance` matches `MockConsumer.java:127-143` order exactly.
- The guard being non-reentrant even same-thread (unlike Java's refcounted
  `acquire()`) is a pre-existing accepted FFI design with `ConsumerHandle_t` as
  the escape hatch — matches the Rust core's own §31 rule.

**Environment:** `cmake` is absent on this machine; compile the Unity suites
directly (see actor memory `ffi_callback_bridging_phase1_notes.md`). `cargo xtask
lint` does NOT see `src/ffi/` — run
`cargo clippy --all-targets --features ffi` separately. Small standalone C probes
linked against `target/release/libconfluent_kafka.a` are the fastest way to
*confirm* a suspected release-timing divergence before reporting it.

Related: [[review_python_bindings_callbacks]],
[[review_wakeup_primitive_confusion]].
