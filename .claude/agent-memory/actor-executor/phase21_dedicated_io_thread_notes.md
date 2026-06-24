---
name: phase21-dedicated-io-thread
description: Milestone-8 Phase 21 — consumer bg loop on dedicated std::thread + current_thread runtime; BgJoin enum; Dedicated await_join (oneshot done + spawn_blocking reap); zero behavior divergence
metadata:
  type: project
---

Phase 21 (N=21) moved the **production** consumer bg loop off `tokio::spawn`
(caller's multi-thread runtime) onto its OWN dedicated `std::thread` hosting a
`current_thread` tokio runtime. Reason: ~54% of consumer CPU was tokio
multi-thread scheduler park/unpark churn from one high-frequency IO task on a
~12-worker pool. Pure execution-strategy change, ZERO behavior divergence.

**Where:** `src/consumer/async_kafka_consumer.rs`.
- Production spawn site is in `new()` only (was `~1201`). `new_with_components`
  + unit tests use a no-op `tokio::spawn(async move {})` handle and the
  `Spawned` path — left verbatim.
- `NetworkThreadCloseHandle` got an internal `enum BgJoin { Spawned(Option<JoinHandle<()>>),
  Dedicated { done: Option<oneshot::Receiver<()>>, thread: Option<std::thread::JoinHandle<()>> } }`.
  `new(...)` unchanged (→ Spawned); added `new_dedicated(signal, wakeup, oneshot::Receiver<()>, std::thread::JoinHandle<()>)`.

**Dedicated `await_join` semantics (must match Spawned):**
1. `done.take()` then `done_rx.await` — bg loop fires `done_tx.send(())` AFTER
   `while is_running() { run_once().await } cleanup().await`. A closed/dropped
   receiver (`Err`) is treated as clean exit (loop already done) via `let _ =`.
2. Reap OS thread via `tokio::task::spawn_blocking(move || thread.join()).await`
   so close future is NOT blocked on the join.
3. Panic mapping: `Ok(Err(_panic))` from `thread.join()` → SAME message shape
   `KafkaError::illegal_state("Consumer network thread terminated with error: ...")`
   as the Spawned JoinError path.
4. Idempotent: second call → `None` handles → `Ok(())`. Never hangs.

**Runtime build:** `Builder::new_current_thread().enable_all()` — `enable_all()`
is REQUIRED (IO driver for Selector/mio, time driver for heartbeat/poll timers).

**§10/§11/§31 preserved:** current_thread runtime supports `tokio::spawn` for
any bg sub-tasks; verified bg path has NO `Handle::current()`, `block_on`,
`block_in_place`, or `spawn_blocking` (only `await_join`'s spawn_blocking runs
on the app runtime). Listener/commit callbacks already run on the APP task (bg
only enqueues + awaits the oneshot per §31) — unchanged.

**Drop:** there is NO `impl Drop` for `AsyncKafkaConsumer` (pre-existing). Both
Spawned (tokio detach) and Dedicated (std::thread detach) leave the bg running
if `close()` is never called — same as before, no regression. Shutdown is
driven by `close()`: signal_close → wakeup → await_join.

**Tests added (unit, no broker):** model the dedicated lifecycle with a real
std::thread + current_thread runtime + running AtomicBool + wakeup Notify +
done oneshot:
- `dedicated_close_handle_joins_cleanly`
- `dedicated_await_join_is_idempotent`
- `dedicated_thread_panic_maps_to_illegal_state` (prints a panic backtrace to
  stderr — cosmetic, expected).
Integration `plaintext_consumer_*` exercise the real dedicated path e2e.
