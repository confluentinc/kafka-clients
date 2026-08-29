---
name: review-wakeup-primitive-confusion
description: Two consumer wakeup primitives (user WakeupTrigger vs bg-task Notify/selector) get confused in both directions; plus the FFI detached-dispatcher UAF pattern
metadata:
  type: project
---

## The two primitives (never interchangeable)

1. **User-facing**: `WakeupTrigger` (rotating `CancellationToken`) — only
   `Consumer::wakeup()` / `ConsumerHandle::wakeup()` may fire it. Firing it
   internally makes the caller's own `poll()` return `KafkaError::Wakeup` with
   no user `wakeup()` call.
2. **Internal bg-task wake**: the app-event `Arc<Notify>`
   (`ApplicationEventHandler::wake_background_task()`, added `bb9ebb0`) or the
   delegate's `wakeup_handle()`. Java analog: `wakeupNetworkThread()` →
   `ConsumerNetworkThread.wakeup()` → `networkClientDelegate.wakeup()` →
   `Selector.wakeup()`. Java's `ConsumerNetworkThread.wakeup()` does **NOT**
   touch `wakeupTrigger` — check `ConsumerNetworkThread.java:322-326` before
   accepting any "Mirror of Java's wakeup()" rustdoc.

**Both failure directions occur, audit for both:**
- *Leaks a user wakeup*: internal wake routed through `WakeupTrigger`
  (`bb9ebb0` fixed the §31 listener-ack site; `ConsumerNetworkThread::wakeup()`
  at `consumer_network_thread.rs:364` still does it, currently uncalled).
- *Inert wake*: `close_internal` calls `wakeup_trigger.disable()` at step 1, so
  every later `WakeupTrigger::wakeup()` is a silent no-op. `signal_close_fn` /
  `wakeup_fn` (`async_kafka_consumer.rs:2269-2276`) are built as
  trigger-only, so the shutdown wake does nothing and `close()` waits out the
  in-flight `poll_default` (≤ `MAX_POLL_TIMEOUT_MS` = 5 s). Reported as Critic-3
  Issue 1. Corollary: `AsyncKafkaConsumer::wakeup()`'s second call
  (`:2660`) is a no-op whose comment claims it unblocks `KafkaClient::poll`.

Grep recipe: `\.wakeup()` across `src/consumer/`, then classify each site as
user-facing vs internal *by intent*, and for internal sites check whether
`disable()` could already have run.

## Fixture-substitutes-a-working-primitive (high-yield, found twice)

A unit-test fixture that stands in for a production closure must fire the *same*
primitive. `spawn_dedicated_bg` (`async_kafka_consumer.rs:9967-9970`) builds
`wakeup_fn = wake.notify_one()` — a real bg poke — so
`dedicated_close_handle_joins_cleanly` proves prompt shutdown for a path
production doesn't have. `bb9ebb0` fixed the same divergence in
`make_test_consumer_with_channels` but did not extend the audit. **Whenever a
`Box<dyn Fn>` / callback is injected in tests, diff the fixture body against the
production construction site.**

## `run_once` wake-arm audit (verified correct, don't re-derive)

`run_once` has no early `return`; the `event_notify.notified()` arm
(`consumer_network_thread.rs:729`) is armed every iteration, guarded only by
`if !poked`, and `notify_one()` stores a permit when nobody is parked — so a
poke landing in Phases 1-2.6 or 5-7 is not lost, worst case one back-to-back
iteration late. Single-slot permit coalescing with `add()` is harmless.

## FFI detached-dispatcher = user_data UAF pattern

`kafka_producer_Producer_destroy` (`src/ffi/producer.rs:899-909`) and
`kafka_consumer_Consumer_destroy` (`src/ffi/consumer.rs:532-536`) **deliberately
detach, never join, the callback dispatcher thread** (joining could deadlock).
Producer delivery callbacks only *enqueue* a `CompletionJob`
(`make_record_callback` → `enqueue_or_run_inline`); consumer callbacks
`dispatch_and_wait`, but that sends before awaiting and
`runtime.shutdown_background()` cancels the awaiter, leaving the job queued.

⇒ **Any C/C++ caller that frees `user_data` after `..._destroy()` returns has a
use-after-free**, and `flush()`/`close()` do NOT guarantee the C callback ran
(only that the Rust side enqueued it). This bit `bindings/c/grpc_server/server.cc`
`Close` (`delete state` at `:571`/`:1162`) — Critic-3 Issue 6. The safe shape is
session-lifetime `user_data` with `user_data_destroy = nullptr`, which is what
that file already intended. Check this on every new C-side `user_data` owner.

## Multilanguage callback-log harness (phase 7) — verified clean

Kind strings / `"<topic>-<partition>"` offset key / empty-string-on-success agree
across `grpc_translate.py`, `server.cc:198-207`, `tests/common/callback_log.rs`.
Async-Python listener is plain (not a coroutine) and `_ListenerAdapter._invoke`
only schedules onto the loop for awaitable results — no deadlock. All three new
tests have teeth (an empty log fails each). Revoke forced by *replacing* the
subscription with the listener re-registered — correct, since `unsubscribe()`
fires `lost` and a plain `subscribe()` releases the listener.

**Test-honesty gap worth checking on any `wait_for_kind`-style helper:** it
returns on the *first* matching entry, so an `assert_eq!(len, 1)` "fires exactly
once" claim cannot detect a double invocation (Critic-3 Issue 5). "At least once"
is pinned; "at most once" is not.
