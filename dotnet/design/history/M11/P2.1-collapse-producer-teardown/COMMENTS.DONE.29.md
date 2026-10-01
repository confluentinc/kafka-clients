# COMMENTS.DONE.29 — M11/P2.1 "Collapse producer teardown into NativeProducer + drop Close(TimeSpan)"

Closed record (Manager-authored — CLEAN on first review, no fix cycle, so no Actor
decisions file). N=29. Critic N=29 verdict: **CLEAN — 0 issues.** Mode A held.
Commit `84647361` (stacks on the M11/P2 commits; the pending `fixup! 2d97172e` untouched).

## What changed

### (A) Teardown collapse (behavior-preserving for the 3 kept flavors)
- Merged the wrappers' `_closed`/`TryBeginClose()` latch + P1's `_disposed` guard into ONE
  `NativeConsumer`-style latch in `NativeProducer` (`_closed` + `TryBeginClose` =
  `Interlocked.CompareExchange(ref _closed,1,0)==0` + `ThrowIfClosed`).
- Absorbed the 3 kept `ProducerTeardown` bodies into `NativeProducer`:
  - `Dispose()` = sync `Producer_close` (via `CloseSync`, error read+freed & **SWALLOWED**) → `_handle.Dispose()` (`Producer_destroy`) in `finally`. (= old `CloseSyncThenDestroy`.)
  - `DisposeAsync()` = `await CloseWithCallback()` (`Producer_close_async`), **SWALLOW** `KafkaException` → destroy in `finally`. (= old `CloseBestEffortThenDestroyAsync`; primary.)
  - `Close(ct)` = `ct.ThrowIfCancellationRequested()` → latch → `await CloseWithCallback()` → destroy in `finally`, **SURFACE** the error. (= old `CloseGracefulThenDestroyAsync`.)
  - Kept relocated: `CloseWithCallback`/`CloseSync` building blocks + the span-the-op `SafeHandle` `DangerousAddRef`/`SetHandleRef` ref (destroy-while-close-in-flight UAF safety, released in `OperationCompletionSource.FreeGcHandle`).
- Deleted `Internal/ProducerTeardown.cs`.
- `AsyncKafkaProducer`/`AsyncMockProducer` → thin forwarders (`_native.Close(ct)`/`.Dispose()`/`.DisposeAsync()`); their dead `_closed`/`TryBeginClose` removed.

### (B) Removed `Close(TimeSpan)` (Python-producer parity — no timed close)
- Deleted `IAsyncProducer.Close(TimeSpan, CancellationToken)` + both impls; did NOT relocate `CloseWithDeadlineThenDestroyAsync` — the entire `.NET`-timer machinery (`Task.WhenAny`/`Task.Delay`/linked CTS/`ObserveEventually`) is gone.
- Kept `Close(CancellationToken = default)`. `IAsyncProducer` now = `Flush(ct)` / `Close(ct)` / `PartitionsFor(topic, ct)` (still no `Send` — phase C).
- Safe API reduction: `Close(TimeSpan)` was only introduced in M11/P2 on this unmerged branch.

## Decisions / micro-deviations (Critic-verified)
1. **Merged latch** — Close/Dispose/DisposeAsync mutually one-shot (first wins does close→destroy; losers no-op — the `Close(ct)` loser returns `Task.CompletedTask`, no second destroy); post-teardown ops throw `ObjectDisposedException` via `ThrowIfClosed`. No path lets two callers reach `Producer_destroy`.
2. **Dropped `ThrowIfClosed`/`RegisterCancellation(None)` from the relocated async-close bridge** — verified behavior-neutral AND necessary: with the merged latch, `_closed==1` the instant `TryBeginClose` wins, so retaining `ThrowIfClosed` in the bridge would now throw; the old `_disposed` was `0` during the close, so the old bridge's `ThrowIfDisposed` was a no-op then. `RegisterCancellation(CancellationToken.None,…)` registered nothing.
3. **`Close(ct)` does `ThrowIfCancellationRequested()` BEFORE `TryBeginClose()`** — matches the old ordering; a pre-canceled token throws without consuming the latch, so a later real `Close`/`Dispose`/`DisposeAsync` still wins and destroys (not left un-destroyable). Verified by the kept `Close_PreCanceledToken_ThrowsOperationCanceled`.
4. **Considered & dismissed (NOT a defect):** the merged latch sets `_closed=1` at teardown *start*, so a concurrent `Flush`/`PartitionsFor` during an in-flight async close now throws `ObjectDisposedException` (the old two-latch design would have submitted it). This is (i) a sanctioned consequence of the approved "merge into ONE latch", (ii) an exact match to the `NativeConsumer` precedent, (iii) unreachable under the single-owner producer, (iv) the more-correct behavior.

## Verification (Critic N=29 — CLEAN)
- 3 kept flavors byte-for-byte parity vs the pre-collapse `ProducerTeardown` bodies; swallow/surface split, span-the-op ref, destroy-always-in-`finally` preserved.
- `Close(TimeSpan)` + all timer machinery fully removed (grep-confirmed; only doc-comment mentions of the removal + the test-class name `PublicProducerTeardownTests` survive). `_disposed`/`ThrowIfDisposed` fully renamed to `_closed`/`ThrowIfClosed`, no residue.
- ONLY sanctioned test change: the 4 `CloseTimeout_*` tests deleted from `PublicProducerPeripheralTests.cs`; every other teardown/peripheral test kept byte-identical (11 teardown bodies unchanged). No orphaned `using`s/helpers.
- Thin forwarders confirmed; consumer suite unaffected (no consumer file touched).
- Mode A: `git diff --stat prashah_dev_producer_foundation..HEAD` zero over `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust core. No `Send`/`ProducerRecord`/§A7.
- DoD (Actor): `dotnet build` 0W/0E across net462/net8.0/net10.0; `dotnet test -f net10.0` 466 passed / 0 failed (470 − 4 deleted); `dotnet format` clean.

## Mode A invariant
Held. Zero change under `src/**`, `src/ffi/**`, `confluent_kafka.h`, or Rust core. Pure internal
relocation + one deliberate public-API reduction; all logic stays in the Rust core.
