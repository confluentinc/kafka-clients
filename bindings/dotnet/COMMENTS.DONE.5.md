# COMMENTS.DONE.5 — resolved record for the Critic (N=5) review of M3/P1 "Completion bridge + first async op"

Review target: commits `134dc0d..HEAD` (`285b04c`, `d6f3022`, `3d0245f`, `1243350`,
`259d098`) on `prashah_dev_asyncbridge_scaffolding`. Both findings from `COMMENTS.5.md`
are resolved and moved here — Finding 1 fixed in code; Finding 2 accepted and
documented as deferred-by-design to N=6 (not a code change).

---

## Finding 1 — [MEDIUM] Sync `Dispose` with an async op in flight leaks the per-op GCHandle + strands the op `Task` — **FIXED**

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
