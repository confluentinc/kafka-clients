# COMMENTS.DONE.5 — resolved record for the Critic (N=5) review of M3/P1 "Completion bridge + first async op"

Review target: commits `134dc0d..HEAD` (`285b04c`, `d6f3022`, `3d0245f`, `1243350`,
`259d098`) on `prashah_dev_asyncbridge_scaffolding`, plus the Finding-1 fixup
(`9f50ed2`) and its re-review. All three findings from `COMMENTS.5.md` are resolved and
moved here — Finding 1 fixed in code (its GCHandle-free part then corrected by
Finding 3); Finding 2 accepted and documented as deferred-by-design to N=6 (not a code
change); Finding 3 fixed in code (callback becomes the sole owner of the `GCHandle`
free).

---

## Finding 1 — [MEDIUM] Sync `Dispose` with an async op in flight leaks the per-op GCHandle + strands the op `Task` — **FIXED**

> **Corrected in part by Finding 3 (below).** The fix recorded in this section
> introduced `FaultAndReclaim`, which freed the `GCHandle` from `Dispose` — itself a
> case-B use-after-free. The current code faults the `Task` only (`FaultTaskOnly`) and
> leaves the free to the completion callback (the sole owner). This section is kept as
> the historical record of the Finding-1 fix; read Finding 3 for the corrected shape,
> the test names, and the accepted case-A residual. The strand fix (observe the op
> `Task` to a terminal state) is unchanged and still in force.

**Where:** `src/Confluent.Kafka.ShareConsumer/Internal/NativeConsumer.cs` (`Dispose`);
`src/Confluent.Kafka.ShareConsumer/Internal/OperationCompletionSource.cs`;
test `tests/.../Interop/ConsumerAsyncTeardownTests.cs` (`Dispose_WithOpInFlight_*`).

**Root cause (verified against the Rust core).** `Dispose` called
`Consumer_close_with_timeout` → `Consumer_destroy` with no drain. But
`close_with_timeout` is a *guarded* sync op: `sync_void_op` calls `acquire(h)`
(`src/ffi/consumer.rs:2495`). While an async op is genuinely in flight, the core guard
is held — `async_void_op` acquires it **synchronously** at `:2525` (before spawning)
and releases it only inside the completion job at `:2546`, right *before* firing the
callback. So the guard being held ⟺ the callback has not fired. In that window
`close_with_timeout`'s `acquire` fails and it returns `ConcurrentModification`
(`:2496`) **without closing or joining**; `Dispose` swallowed that error and proceeded
to `Consumer_destroy`, whose `runtime.shutdown_background()` (`:503`) **cancels** the
in-flight future → the C callback never fires → `OperationCompletionSource.FreeGcHandle`
/ `Complete` are never called. Result: the per-op `GCHandle` (+ the `OperationCompletionSource`
and its `TaskCompletionSource`) leaked, and the op's `Task` was never completed.

**Fix.** A deterministic **post-destroy reclaim** in the sync `Dispose` (respecting all
constraints — NOT sync-over-async, race-safe, `DisposeAsync` unchanged):

- New `OperationCompletionSource.FaultAndReclaim(Exception)` — disposes the cancellation
  registration, releases the (managed) guard, faults the awaiter's `Task`
  (`TrySetException`), and frees the rooting `GCHandle` (`FreeGcHandle`). It reuses the
  same idempotent primitives the callback uses: `TrySetException` no-ops on a completed
  `Task`, `FreeGcHandle` is `Interlocked`-idempotent — so it introduces **no new race**
  and can neither double-complete nor double-free.
- `NativeConsumer` now tracks the pending op's context (`_inFlightContext`, published in
  `SubmitVoidOperation` alongside the existing `_inFlightOperation` Task). After
  `TryBeginClose` wins, `Dispose` snapshots that context, runs
  `close_with_timeout` → `Consumer_destroy` (unchanged shape), and **then** — once
  destroy has cancelled the callback so it can no longer fire —
  `pending?.FaultAndReclaim(new ObjectDisposedException(...))`. Ordered after destroy so
  a callback that raced to fire *before* destroy makes the reclaim a harmless no-op; and
  it never awaits/`.Wait()`s the op `Task` (not sync-over-async). `DisposeAsync` still
  drains (wakeup + await) so its callback fires normally — its behavior is unchanged.

**Test change (the masking test).** `Dispose_WithOpInFlight_Returns` discarded the op
`Task` (`_ = consumer.SubscribeAsync(...)`) and asserted only that `Dispose` returned,
so the strand/leak was unobservable. Replaced by
`Dispose_WithOpInFlight_ReturnsAndCompletesTheOpTask`, which **observes** the op `Task`
and asserts it reaches a **terminal** state (under the `TestTimeout` hang guard, so a
stranded `Task` fails fast), plus a churn/GC variant
(`Dispose_WithOpInFlight_Churned_EveryOpTaskCompletes_NoLeak`).

Because instant Mock ops make the guard-rejected strand path **non-deterministic** at
the integration level (verified: with the reclaim removed, the integration tests still
pass — the op completes before `close_with_timeout` runs, same constraint D2 documents),
the deterministic proof of the reclaim primitive is at the component level, in
`ConsumerCompletionBridgeTests`: `FaultAndReclaim_FaultsTheTask`,
`_AfterCompletion_IsNoOp`, `_ThenCallbackFires_NoDoubleFreeOrDoubleComplete`, and
`_FreesGcHandle_UnrootsTheContext` (a weak-reference free-proof — verified to FAIL if
`FreeGcHandle` is skipped, i.e. it genuinely catches a leaked handle).

**STATUS.** D4 and the "N=5 deferred hardening — DONE" wording corrected: an
op-in-flight sync `Dispose` now faults the op `Task` + frees its `GCHandle` instead of
stranding/leaking. Added a "Review outcome (M3/P1)" section.

**Verification (all green):** `cargo build --features ffi` OK; `dotnet build` 0/0 across
netstandard2.0 / net8.0 / net10.0 (lib) + net8.0 / net10.0 (tests); `dotnet test -f
net10.0` 52 passed / 0 failed (no hang, `TestTimeout` guards); `dotnet format
--verify-no-changes` clean.

---

## Finding 2 — [LOW / latent] Cross-thread `Wakeup()` / `GroupId()` TOCTOU vs teardown → potential UAF — **ACCEPTED, deferred to N=6 (documented, no code change)**

**Where:** `src/Confluent.Kafka.ShareConsumer/Internal/NativeConsumer.cs` (`Wakeup`,
`GroupId`).

**Assessment.** The thread-safe-closed-flag read and the `_handle.DangerousGetHandle()`
deref are not atomic, so a concurrent `Dispose`/`DisposeAsync` on another thread could
free the handle in between → the native call dereferences destroyed memory. This is
plan-consistent: the M3/P1 PLAN (§Teardown) deliberately declined per-call
`SafeHandle.DangerousAddRef`/`Release` ("guarded the CKD way — thread-safe closed check
+ access guard; NO per-call `SafeHandle` AddRef"), matching confluent-kafka-dotnet's
non-AddRef hot-path idiom. `Wakeup()`/`GroupId()` are **internal-only** this phase with
no cross-thread wakeup-vs-dispose caller, so the race is **not reachable** now.

**Resolution.** Not a code fix. Carried forward as a deferred-hardening item in
`design/current/STATUS.md` — "Deferred hardening (N=6 — public client cross-thread
wakeup): `Wakeup()`/`GroupId()` TOCTOU vs teardown" — mirroring how the N=5 teardown
item was carried: it states the hazard, why it is deferred, and the two resolutions to
consider when the public `IConsumer`/`KafkaConsumer` surface makes `Wakeup()` genuinely
cross-thread ((a) per-call AddRef/Release on exactly those methods, or (b) document the
no-concurrent-teardown precondition).

---

## Finding 3 — [LOW / latent, memory-safety] The Finding-1 fix's sync-`Dispose` reclaim freed the per-op `GCHandle` before a straggler completion callback could dereference it (case B) — **RESOLVED**

**Where:** `src/Confluent.Kafka.ShareConsumer/Internal/OperationCompletionSource.cs`
(`FaultAndReclaim` → now `FaultTaskOnly`);
`src/Confluent.Kafka.ShareConsumer/Internal/NativeConsumer.cs` (`Dispose`);
`src/Confluent.Kafka.ShareConsumer/Internal/Interop/ConsumerCallbacks.cs`
(`OnOperation` — the sole `GCHandle` free);
tests `tests/.../Interop/ConsumerCompletionBridgeTests.cs` (the four `FaultTaskOnly_*`
tests) + `ConsumerAsyncTeardownTests.cs`.

**Root cause (verified against the C ABI + both siblings).** The Finding-1 fix had the
synchronous `Dispose` call `FaultAndReclaim`, which faulted the op `Task` **and** freed
the per-op `GCHandle`. Freeing the `GCHandle` from `Dispose` is the bug. The managed
side cannot distinguish two orderings that both present as "core guard held ⇒
`close_with_timeout` rejected":

- **case A** — the op's future is still running → `Consumer_destroy`'s
  `shutdown_background` cancels it → the completion job is never created → the callback
  never fires; and
- **case B** — the op already completed and its completion job is **queued** but the
  dispatcher has not yet run it (the guard is released *inside* that queued job). The
  ABI's `Consumer_destroy` closes the completion channel and detaches the dispatcher
  **without joining**, and the dispatcher loop drains any already-queued jobs before
  exiting — so that queued job runs **after** `Consumer_destroy` returns and fires the
  callback.

In case B, if `Dispose`'s `FreeGcHandle()` won the race against the dispatcher, the
straggler callback then executed `OnOperation` → `GCHandle.FromIntPtr(userData).Target`
on a **freed** (potentially recycled) `GCHandle` — a use-after-free (best case a caught
`NullReferenceException` + a leaked failure-op error handle; worst case the slot
recycled to a live `OperationCompletionSource`, completing the wrong op's `Task` and
freeing *its* rooting handle → cascading UAF). This violates ffi §B6 ("the `GCHandle`
must stay alive from submit until the callback fires").

**Verified pattern against BOTH siblings over the same/analogous core (callback is the
SOLE owner; teardown drains, never reclaims):**

- **In-repo Python** (`bindings/python`): `Py_INCREF(cb)` at submit, `Py_DECREF(cb)` in
  the op trampoline (`_confluentkafka.c`, `consumer_op_trampoline`). The callback is the
  sole owner of the context free; teardown never reclaims — `consumer.py` `close()`
  drains (awaits the op) then a bare `_destroy()`. The async `_run_async` callback
  tolerates a gone awaiter (frees the payload if the future is cancelled/done).
- **confluent-kafka-dotnet** (librdkafka): `GCHandle.Alloc` at submit
  (`Producer.cs:307`), `gch.Free()` in the delivery-report callback (`Producer.cs:221`)
  — callback is the sole owner; `Dispose` never reclaims per-op handles, it drains
  (`callbackTask.Wait()`, `Producer.cs:450`) then destroys.

Our `FaultAndReclaim` (reclaim-from-`Dispose`) was the outlier; the fix aligns with both.

**Fix.** `FaultAndReclaim` is replaced by `FaultTaskOnly(Exception)`, which faults the
op `Task` (`TrySetException` — preserves Finding 1's strand fix so a fire-and-forget
awaiter does not hang), releases the managed access guard, and disposes the cancellation
registration — all idempotent — but does **not** free the `GCHandle` and does nothing
that races the still-pending native callback. The completion callback
(`ConsumerCallbacks.OnOperation` → `FreeGcHandle` in `finally`) remains the **sole
owner** of the free, and stays safe when it fires **after** `Dispose`: `TrySet*` no-ops
on a faulted `Task`, `FreeGcHandle` is `Interlocked`-idempotent, and the guard/registration
releases are idempotent. `Dispose` now calls `FaultTaskOnly` after
`TryBeginClose` + `close_with_timeout` + `Consumer_destroy`, and frees no `GCHandle`
anywhere on any teardown path (grep-confirmed: `FreeGcHandle` is called only from the
callback and from `AbandonBeforeSubmit`, the submit-threw path where native never ran).
`DisposeAsync` is unchanged (it drains: wakeup + await the in-flight op → the callback
frees the `GCHandle` normally → `close_async` → destroy).

**Accepted residual (case A).** If `Consumer_destroy` cancels the op's future before its
completion job is enqueued, the callback never fires, so that one op's `GCHandle` +
context leaks — a rare, one-time, teardown-only leak in a misuse case (sync `Dispose`
with an unawaited in-flight op). This is exactly the residual the Python (bare `_destroy`
after drain) and CKD (unflushed-at-destroy) siblings accept; documented in STATUS (D4)
with the steer to `DisposeAsync` (drains → no leak).

**Test change (closing the Finding-3 coverage gap).** The prior
`FaultAndReclaim_ThenCallbackFires_*` test drove `Complete` + `FreeGcHandle` directly on
a **strong** reference, so it never exercised the `FromIntPtr`-after-free path the
finding is about. The four `FaultTaskOnly_*` tests now:
`_FaultsTheTask` (strand fix; handle not freed by `FaultTaskOnly`);
`_AfterCompletion_IsNoOp` (callback via the real path first, then `FaultTaskOnly` is a
no-op); `_ThenCallbackFires_CaseB_IsSafe` (the straggler callback runs through the real
`ConsumerCallbacks.Operation` → `GCHandle.FromIntPtr` recovery path — proving it
recovers a **live** handle, completes as a no-op on the faulted `Task`, and frees once);
`_DoesNotUnrootContext_TheCallbackDoes` (weak-ref proof that `FaultTaskOnly` leaves the
context rooted, and the callback — not `Dispose` — unroots it). The
`Dispose_WithOpInFlight_*` teardown regressions still hold (op `Task` reaches a terminal
state; no strand, no hang).

**Verification (all green):** `cargo build --features ffi` OK; `dotnet build` 0/0 across
netstandard2.0 / net8.0 / net10.0 (lib) + net8.0 / net10.0 (tests); `dotnet test -f
net10.0` all pass (no hang, `TestTimeout` guards); `dotnet format --verify-no-changes`
clean.
