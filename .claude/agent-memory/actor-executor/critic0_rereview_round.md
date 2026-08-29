---
name: critic0-rereview-round
description: Critic-0 re-review of the fix commits — translate Java `finally` as an RAII guard not enumerated exits, model the bare `KafkaException` as its own KafkaError variant, and how to un-defang a guard test whose two primitives merged
metadata:
  type: project
---

Critic 0 re-reviewed the six earlier fix commits on `ffi-callback-bridging` and
found 5 issues; all fixed (`f7e5eff`, `b53c941`, `62ea49e`, `25d731d`). Four
patterns generalize.

**1. A Java `finally` must become an RAII guard, not N hand-written exits.**
`RecordAccumulator.append` is bracketed by `finally { free.deallocate(buffer); }`
with `buffer = null` only on `newBatchCreated`. Two rounds of "add a
`deallocate` to the exit I noticed" left the third exit leaking — and it was the
one on the *success* path, so no error-path test could see it. The fix is a
`PooledBuffer<'a> { pool: &'a BufferPool, buffer: Option<Vec<u8>> }` with a
`Drop`; `pooled.buffer.take()` at the batch handoff is the literal `buffer =
null`. Borrow the resource (`&'a`), don't `Arc::clone` it — this is a
per-record send-path allocation (CLAUDE.md §11). Repo precedent for the shape:
`ReleaseGuard<'_>` in `src/ffi/consumer.rs`. Corollary: **dropping a pooled
`Vec` is not deallocating it** — `BufferPool::allocate` debits
`non_pooled_available_memory`, only `deallocate` credits it back and notifies a
waiter, so a dropped buffer shrinks the pool permanently.

**2. `KafkaException` vs `ApiException` is a behavioural bit, and the enum
needed a variant for it.** `KafkaProducer.doSend` catches `ApiException` (fires
the callback + failed future) *before* `KafkaException` (rethrows, callback
never fires). Rust had no variant that was the latter but not the former, so
"Producer closed while send in progress" (`RecordAccumulator.java:427`) and
"Producer closed while allocating memory" (`BufferPool.java:119`/`:157`) were
modelled as `Generic(UnknownServerError)`, for which `is_api_exception()` is
`true`. Added `KafkaError::Kafka(String)` / `KafkaError::kafka(msg)`: excluded
from `is_api_exception()`, included in `is_kafka_exception()`, no `Errors` code
(so `error()`/FFI code are unchanged), Display `"KafkaError: {msg}"`. Rule:
before wiring a `catch`-arm dispatch, check the *whole* exception taxonomy of
the throw sites, and grep for sibling throws with the same shape — the
`BufferPool` half of this bug was not in the review comment.

**3. When Java has one field and Rust has two copies, the "keep" writes matter
as much as the "clear" writes.** The previous round fixed listener-less
`subscribe` failing to clear the app-side mirror; this round was the mirror
image — `unsubscribe()` *cleared* it where `SubscriptionState.unsubscribe()`
keeps it (`registerRebalanceListener` is only reachable from the three
`subscribe` overloads). Observable symptom: a
`ConsumerRebalanceListenerCallbackNeeded` enqueued while the registration was
live but drained after `unsubscribe()` returned took the `None => Ok(())` arm
and silently skipped the user callback. Before writing the test, trace what Java
does *end to end*: here `runRebalanceCallbacksOnClose()` returns early on an
empty assignment, so retention is NOT observable through `close()` — asserting
that would have been wrong. Test the drained-callback path instead.

**4. Un-defanging a guard test whose two primitives merged.** `af60796` made
`NetworkThreadCloseHandle::wakeup()` fire the same `Arc<Notify>` as
`wake_background_task()`, which silently made the Phase-41b guard's
`maybe_trigger_wakeup().is_ok()` assertion pass under the very mutation its
docstring described. Two reusable moves:
  - Say in the docstring **which assertion catches which mutation**, and name
    the load-bearing one (`!bg_wakeup_called`, a fixture flag wrapping the
    production closure) so nobody deletes it as redundant.
  - A negative assertion needs a **negative control**: `maybe_trigger_wakeup()`
    returns `Ok` for a *disabled* trigger, so the test now ends by firing the
    trigger deliberately and asserting it *does* cancel the token. Same idea
    applied in reverse for `AsyncKafkaConsumer::wakeup`'s "redundant" second
    statement: it is load-bearing only in the post-`disable()` window (a
    `ConsumerHandle::wakeup()` racing `close()`), so
    `wakeup_pokes_the_bg_task_even_when_the_trigger_is_disabled` disables the
    trigger, asserts the token is NOT cancelled, and asserts the `Notify` permit
    appeared anyway. Prefer proving a "redundant" statement with a test over
    deleting it.

**Verification deltas.** `plaintext_consumer*` = 72 tests, 13 s (pooled
brokers). The 5 host C suites = 90 tests via the direct `cc` recipe. Producer
integration tests still exist ONLY under `multilanguage-tests` (see
[[critic1-ffi-contract-round]]), so a core producer change is verified through
the gRPC suite, which needs the Linux cross-build + image rebuild from
[[multilanguage-suite-on-macos]].

See [[critic1-ffi-contract-round]], [[critic3-wakeup-and-logstate-round]],
[[ffi-callback-bridging-phase2]], [[phase41-consumer-handle]].
