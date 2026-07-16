---
name: review-ffi-async-dropguard-teardown
description: FFI async RAII completion-guard fires on tokio runtime-drop CANCELLATION during destroy, not just panic — verify teardown-safety, not only panic-safety
metadata:
  type: project
---

Verified in M10 Phase-3 hardening commit `be6ac2e` (`PanicCompletionGuard<P>` in
`src/ffi/share_consumer.rs`). The share-consumer async FFI helpers
(`async_void_op` / `async_value_op` / `poll_async`) spawn a task, arm a one-shot
RAII drop-guard before the `.await`, and `disarm()` it on the normal path; on an
armed `Drop` the guard enqueues an error completion (releases the single-owner
`Arc<AtomicU64>` + fires the C callback). Reviewed SOUND, not blocking. Related:
[[review-m10-phase3]], [[review-m10-phase2-share-ffi]].

**The non-obvious insight (the reason a "panic-safety" guard needs teardown
review):** an armed drop-guard fires on *any* armed `Drop`, not only a panic
unwind. Dropping a multi-thread tokio `Runtime` (what `_destroy` does:
`drop(handle.runtime.take())`) **drops in place the future of every task suspended
at an `.await`**, running that future's `Drop` — so the guard ALSO fires during
teardown for any in-flight op awaiting network/timer/channel. The commit's own doc
comment ("only happens on a panic unwind") is therefore incomplete — filed as LOW
non-blocking finding F1.

**How to establish tokio runtime-drop semantics (reusable):** don't argue from
memory — write a standalone cargo experiment pinned to the repo's tokio version
(`grep -A2 'name = "tokio"' Cargo.lock`): multi-thread runtime, spawn a task that
holds an armed RAII bomb across `tokio::time::sleep(1h).await`, wait until it's
parked, then `drop(rt)` and check the bomb's fire counter. Result: fires 0→1
DURING the drop. (Standalone crate = touches no project source, respects the
Critic no-edit rule.)

**Why the teardown fire is safe here (audit checklist for any such guard):**
- `on_panic` + the enqueued job must capture ONLY owned data (an `Arc` clone of
  the owner cell, a `Sender` clone, `Copy` fn-ptr/target, a freshly-boxed error,
  move-only payload) — NEVER the `&'static handle` (that's used only in the normal
  body before the await). Grep the closure captures.
- `_destroy` ordering is the linchpin: step 1 `drop(runtime.take())` is a BLOCKING
  shutdown that runs every future-drop (⇒ every guard fire + enqueue) to
  completion BEFORE step 2 `drop(handle)` frees the box. So the fire happens while
  the box + `completion_tx` are still alive ⇒ `tx.send` succeeds ⇒ job queued (not
  inline), runs later on the DETACHED dispatcher on owned data. No UAF.
- No deadlock: `std::sync::mpsc::Sender::send` is non-blocking/unbounded; the
  dispatcher is detached (destroy never joins it).
- The single-owner `acquire` guard caps in-flight ops at ONE ⇒ at most one guard
  fires during teardown.
- Doesn't break `test_destroy_blocks_until_in_flight_async_completes`: that test's
  consumer blocks the worker SYNCHRONOUSLY (`barrier.wait()`), so the runtime
  can't cancel it → normal `disarm` path, no guard fire.
- Behaviorally the late error callback is rule-ALIGNED (CLAUDE.md §5/§9.5:
  explicit error beats silently-dropped callback), and consistent with the
  pre-existing "completions fire on the detached dispatcher after destroy" design.

**disarm-vs-Drop mutual exclusion (core soundness):** normal path awaits →
`disarm()` `.take()`s payload (`armed=None` ⇒ end-of-scope Drop is a no-op) →
ordinary job enqueued; abnormal path drops the still-armed guard → error job.
There must be NO `.await` between `disarm()` and `enqueue_or_run_inline` (there
isn't) — otherwise a cancel in that window loses the completion. Exactly-once
release, exactly-once fire, never both.

**Teeth pattern for panic-safety:** a `PanicConsumer` test double whose awaited
ops `panic!`; assert (a) callback fires with the internal error within a timeout
AND (b) a follow-up `acquire` succeeds. Pre-fix both legs fail (no enqueue →
recv_timeout trips; guard stuck → acquire fails). tokio's task harness catches the
unwind (no process abort). NOTE: a `panic!` in an `async fn` body panics on FIRST
poll (synchronous), so these tests exercise the PANIC trigger; the teardown-cancel
trigger is a separate path (future dropped without panic) — same `Drop`, so the
mechanism is shared but the triggers differ.
