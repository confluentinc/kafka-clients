# M11/P2.1 — "Collapse producer teardown into `NativeProducer` + drop `Close(TimeSpan)`"

Status: **APPROVED (user 2026-08-12).** N=29. Executor: `dotnet-actor` (reviewed by `dotnet-critic`). Behavior-preserving teardown collapse **plus** a deliberate small public-API reduction (remove `Close(TimeSpan)`, strict Python-producer parity).
Branch: **same** `prashah_dev_producer_async_peripherals` (commits stack on the M11/P2 commits; do NOT create a new branch; leave the pending `fixup!` `2d97172e` untouched). Do not disturb PR #157/#154/#150.

## 0 · Scope boundary (READ FIRST)

M11/P2.1 does **two** things:
- **(A) Teardown collapse (behavior-preserving)** — relocate + inline the producer teardown into one layer (`NativeProducer`), matching the **consumer's** shape, and delete `ProducerTeardown`. The M11/P2 two-layer split existed only for a self-imposed "don't touch P1's `NativeProducer.Dispose` pin" constraint; **the user has lifted that constraint**, so evolving P1's pinned `Dispose`/`DisposeAsync` (destroy-only → graceful-close→destroy) is approved.
- **(B) Drop `Close(TimeSpan)` (deliberate public-API reduction)** — Python's producer close is `def close(self):` / `async def close(self):` with **no timeout param** (unlike the consumer's `close(self, timeout=None)`). The producer is scoped to Python's implemented surface, so the timed-close overload is removed. Safe because `Close(TimeSpan)` was only introduced in M11/P2 on this **unmerged** branch — removing it now breaks nothing released.

**Hard invariants:** no `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust-core change (Mode A); no `Send`/`ProducerRecord`/`RecordMetadata`/pull-pump/§A7 (all phase C / N=30); apart from the deliberate `Close(TimeSpan)` removal in (B), NO behavior change to the kept teardown methods. If the Actor drifts toward behavior change on a kept method (or an ABI/`Send` change), STOP.

## 1 · Motivation & the target shape

M11/P2 shipped teardown as two layers: `NativeProducer.Dispose`/`DisposeAsync` pinned to `Producer_destroy`-only (M11/P1), plus a static `ProducerTeardown` doing *graceful close → destroy* on top, with the public `AsyncKafkaProducer`/`AsyncMockProducer` owning the one-shot close latch and calling the four `ProducerTeardown` bodies. The **consumer** does it in one layer: `NativeConsumer` owns all teardown (`Dispose`/`DisposeAsync`/`CloseSync`/`CloseWithCallback` + the atomic `_closed` latch + `TryBeginClose`), and the public types are **thin forwarders** (`AsyncKafkaConsumer.Dispose() => _native.Dispose()` `:204`; `DisposeAsync() => _native.DisposeAsync()` `:207`; `Close(ct) => _native.CloseWithCallback(ct).AsTask()` `:179-180`). P2.1 makes the producer match this — and trims `Close(TimeSpan)`.

## 2 · Verified findings (file:line)

- **The four `ProducerTeardown` bodies today (`Internal/ProducerTeardown.cs`):**
  - `CloseGracefulThenDestroyAsync(native)` (`:48`) — `Producer_close_async` → destroy, **surface** → public `Close(ct)`. **KEEP** (relocate).
  - `CloseBestEffortThenDestroyAsync(native)` (`:68`) — `Producer_close_async` → destroy, **swallow** → `DisposeAsync()`. **KEEP** (relocate).
  - `CloseSyncThenDestroy(native)` (`:91`) — sync `Producer_close` → destroy, **swallow** → `Dispose()`. **KEEP** (relocate).
  - `CloseWithDeadlineThenDestroyAsync(native, timeout, ct)` (`:113`, the `.NET`-timer race at `:124-138`) — → public `Close(TimeSpan)`. **DELETE ENTIRELY** (scope B — not relocated).
- **Callers (latch in the wrappers):** `AsyncKafkaProducer.cs:90/104/113/120`, `AsyncMockProducer.cs:87/101/110/117`, each gated by `TryBeginClose()` (`Interlocked` `_closed`, `AsyncKafkaProducer.cs:59` / `AsyncMockProducer.cs:120`). `NativeProducer` keeps P1's destroy-only `Dispose`/`DisposeAsync` + the building blocks `CloseWithCallback` / `CloseSync` + the span-the-op `SafeHandle` ref.
- **Consumer precedent to mirror (`NativeConsumer`):** `CloseSync()` (`:1646`), `CloseWithCallback`, `Dispose()` (`:2281`), `DisposeAsync` (primary); atomic `_closed` (`:128`) + `TryBeginClose()` (`:3223`) + `ThrowIfClosed`. (Note: the consumer HAS `CloseSyncWithTimeout` `:1678` because Java/Python consumer close IS timed — the producer deliberately does NOT get a timed analog, scope B.)
- **Test safety net:** `PublicProducerTeardownTests.cs` drives teardown through the public wrappers; it references `ProducerTeardown` only in a **doc comment** (`:37`). The `Close()`/`Dispose`/`DisposeAsync`/idempotency/post-dispose tests stay green **unchanged**; the `Close(TimeSpan)` tests are **deleted** (scope B).
- **Python parity (scope B basis):** Python producer `close` has no timeout param (verified by the coordinator); the consumer's does. `Close(CancellationToken = default)` is kept — the optional token is the standard .NET async idiom (`bindings/CLAUDE.md §2`), not a parity violation.

## 3 · Deliverables

### (A) Collapse teardown into `NativeProducer`
1. Move the `_closed`/`TryBeginClose()` one-shot latch from the wrappers into `NativeProducer`, **merging it with P1's idempotent-dispose guard** into a single `NativeConsumer`-style latch: `Close`/`Dispose`/`DisposeAsync` are mutually one-shot (first wins, does close→destroy; later calls are no-ops), and post-teardown ops still throw `ObjectDisposedException`.
2. Absorb the **three kept** bodies into `NativeProducer`, mirroring `NativeConsumer` method-for-method:
   - `NativeProducer.Dispose()` = sync `Producer_close` (via `CloseSync`) → destroy, **swallow** (was `CloseSyncThenDestroy`; evolves P1's destroy-only pin).
   - `NativeProducer.DisposeAsync()` = `Producer_close_async` (via `CloseWithCallback`) → destroy, **swallow** (was `CloseBestEffortThenDestroyAsync`; primary path).
   - `NativeProducer.Close(ct)` (behind public `Close`) = `Producer_close_async` → destroy, **surface** (was `CloseGracefulThenDestroyAsync`).
   - **Keep, relocated unchanged:** `CloseWithCallback`/`CloseSync` building blocks, the span-the-op `SafeHandle` `DangerousAddRef`/`DangerousRelease` ref (destroy-while-close-in-flight UAF safety), the swallow-vs-surface split.
3. **Delete `Internal/ProducerTeardown.cs`.**
4. `AsyncKafkaProducer` / `AsyncMockProducer` → **thin forwarders**: `Close(ct) => _native.Close(ct)`, `Dispose() => _native.Dispose()`, `DisposeAsync() => _native.DisposeAsync()` (like the consumer wrappers). Remove the wrappers' now-dead `_closed`/`TryBeginClose`. Update the wrappers' XML-doc referencing `ProducerTeardown` (`AsyncKafkaProducer.cs:47`).

### (B) Remove `Close(TimeSpan)`
5. Delete `IAsyncProducer.Close(TimeSpan, CancellationToken)` and its `AsyncKafkaProducer`/`AsyncMockProducer` implementations.
6. **Do NOT carry over** `CloseWithDeadlineThenDestroyAsync` — the entire `.NET`-side timer machinery (`Task.WhenAny` + `Task.Delay` + linked CTS + `ObserveEventually`) goes away.
7. Delete the `Close(TimeSpan)` tests from `PublicProducerTeardownTests.cs` (and any `Close(TimeSpan)` case in `PublicProducerPeripheralTests.cs`, e.g. the negative-timeout precondition test strengthened in `2d97172e`).
8. **KEEP `Close(CancellationToken = default)`** (≈ Java/Python `close()`).

**Result:** `NativeProducer` ends with **three** teardown flavors — `Dispose()` (sync close→destroy, swallow), `DisposeAsync()` (async close→destroy, swallow), `Close(ct)` (async close→destroy, surface). No timed close anywhere.

## 4 · Behavior-preserving guarantee + verification

The three **kept** flavors, the swallow-vs-surface split, the one-shot latch/idempotency, and the span-the-op ref are **byte-for-byte identical** — a pure relocation + inlining. The ONE deliberate behavior/API change is the `Close(TimeSpan)` removal (scope B). Verification:
- **All non-`Close(TimeSpan)` teardown tests stay green UNCHANGED** (`Close()`, `Dispose`, `DisposeAsync`, idempotency, post-dispose→`ObjectDisposedException`). **If any of those needs a modified assertion, that signals a behavior change in the collapse → STOP and flag.** (The `ProducerTeardown` doc-comment mention at `:37` may be re-pointed at `NativeProducer` — cosmetic.)
- The `Close(TimeSpan)` tests are **deleted** (the one sanctioned test change).
- The **full consumer suite stays green** (consumer untouched; the collapse shares only `NativeProducer`/the C ABI, not consumer types).
- Full suite green (count drops by the removed `Close(TimeSpan)` tests) on net8.0 + net10.0.

## 5 · Definition of Done

- `dotnet build` **0W/0E across the TFM matrix** (net462/net8.0/net10.0); `dotnet test` green (kept teardown tests unchanged; `Close(TimeSpan)` tests removed; consumer suite green); `dotnet format --verify-no-changes` clean.
- **Mode A hard line:** `git diff --stat` scoped to `bindings/dotnet/src/**` + `bindings/dotnet/tests/**` — **zero** change to `confluent_kafka.h`, `src/ffi/**`, Rust core.
- **API:** `IAsyncProducer` loses `Close(TimeSpan, CancellationToken)`; keeps `Flush`/`Close(ct)`/`PartitionsFor` (still no `Send`). `ProducerTeardown.cs` deleted; wrappers are thin forwarders; the latch + three teardown flavors live in `NativeProducer`. No `.NET`-timer machinery remains.
- No `Send`/`ProducerRecord`/pull-pump/§A7 introduced.

## 6 · Governance / handoff (Manager, N=29)

Executor **`dotnet-actor N=29`** (collapse: relocate the three kept bodies + latch into `NativeProducer`, delete `ProducerTeardown`, thin the wrappers; removal: delete `Close(TimeSpan)` + its timer machinery + its tests). Reviewer **`dotnet-critic N=29`** (kept-method behavior byte-for-byte parity — swallow/surface split, idempotent merged latch, `ObjectDisposedException`-after-teardown, span-the-op ref; `Close(TimeSpan)` FULLY removed incl. all timer machinery, with interface/impls/tests consistent — no dangling refs; the deleted `Close(TimeSpan)` tests are the ONLY sanctioned test change, all other teardown tests unchanged; consumer suite unaffected; no ABI/`Send`/§A7/Mode-A change). Same branch `prashah_dev_producer_async_peripherals` (stack on M11/P2; leave `2d97172e` untouched). Per-path `git add` with the guard (never the root `.claude/agents/dotnet-*.md` discovery copies, `COMMENTS.*29.md`, agent-memory, `target-linux*`, staged `.so`/`.dylib`); commits `--no-gpg-sign` + `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. On close: archive `COMMENTS.DONE.29.md` under `design/history/M11/P2.1-collapse-producer-teardown/`, update `design/current/STATUS.md` (M11/P2.1 entry), reset `COMMENTS.29.md`.
