# M3/P2 — Single-owner alignment (drop the managed guard + in-flight tracking; keep the completion bridge)

**Status:** REVISED (incorporates the four approved revisions — branch/#135 shape, `ffi-marshalling.md` §B1/§B5/§B7 Actor deliverable, and the four open questions resolved as confirmed decisions) — **awaiting final human approval before implementation.** Do NOT spawn Actor/Critic or write code until approved.
**Mode:** A (consumer C ABI already landed — no Rust authoring; every C-ABI function this phase touches already exists in `confluent_kafka.h`. No `src/ffi` / Rust / header changes).
**Review counter:** N=6 (global monotonic; M0/P0=1, M1/P1=2, M2/P1=3, M2/P2=4, M3/P1=5, **M3/P2=6**).
**Personas:** `dotnet-actor` (Actor N=6) implements; `dotnet-critic` (Critic N=6) reviews. NEVER the Rust `actor-executor` / `kafka-critic`.
**Builds on:** M3/P1 (the completion bridge + managed guard + in-flight tracking). This is a **follow-on that lands on top of M3/P1** as additive commits on the **same branch** (`prashah_dev_asyncbridge_scaffolding` = PR #135's head), so it **extends PR #135** with no history rewrite (see Governance).

---

## Why this phase exists

M3/P1 built the consumer's foreign-thread completion bridge (native callback → `TaskCompletionSource`) and, on top of it, two pieces of **managed defensive machinery** that go beyond the in-repo Python sibling:

1. a managed **one-op-in-flight access guard** (`ConsumerAccessGuard`) that mirrors — does not replace — the Rust core's own guard, and
2. **single-slot in-flight tracking** (`_inFlightContext` / `_inFlightOperation`) used to (a) fault a stranded op's `Task` from the synchronous `Dispose` and (b) drain the in-flight op in `DisposeAsync`.

That tracking machinery carries a known latent race, already documented in `STATUS.md` as an "N=6 deferred" item — **the publish window**: in `NativeConsumer.SubmitVoidOperation` the native `submit(...)` (which spawns the op and takes the *core* guard) runs **before** the two `Volatile.Write`s that publish `_inFlightContext` / `_inFlightOperation`. A concurrent teardown that runs entirely inside that window sees `null` for both fields, so it can neither drain (`DisposeAsync`) nor fault (`Dispose`) the op; the following `Consumer_destroy` cancels the op, its callback never fires, the `Task` **strands** and its `GCHandle` **leaks**. The M3/P1 machinery also generated **three** review findings total (Findings 1, 2, 3 — see M3/P1 `COMMENTS.DONE.5.md`).

**Decision for M3/P2:** align the completion-bridge/teardown to the **in-repo Python sibling's model** (`bindings/python/consumer.py`, `bindings/python/_confluentkafka.c`): **"single-owner, not thread-safe."** Remove the managed guard and the in-flight tracking fields, accept the same misuse-only residuals Python accepts, and **keep the completion bridge itself**. This **eliminates the publish window** (no tracking fields → no window), matches the mature sibling built over the same core, and is **easily reversible** (localized, non-breaking) if hardening is ever needed later.

The Python sibling is explicit about this contract (`bindings/python/consumer.py` header docstring, lines 19–21):

> The Rust consumer is single-owner (one operation in flight). Concurrent use surfaces as a `KafkaError` (ConcurrentModification) or, for the non-blocking state reads, a `RuntimeError`.

and its state reads (`group_metadata()` et al.) map the core's `None` (concurrent-rejection) to `_concurrent_error()` = `RuntimeError("KafkaConsumer is not safe for multi-threaded access.")` (lines 223–224, 270–274). The managed guard we are removing was a .NET-only addition on top of that model; the core guard is still there and still does the real work.

---

## Scope

**In scope:** simplify the M3/P1 completion-bridge/teardown machinery to the Python single-owner model — remove the managed guard + in-flight tracking, keep the bridge core and the 5 invariants, add per-method concurrency surfacing on the sync state read. Consumer, **internal-only** (`NativeConsumer` stays internal; no new public type; `KafkaException` remains the only public type). Capture the rationale + accepted residuals in code comments. Update `STATUS.md` and reconcile its deferred items. **Update the FFI rules doc** (`.claude/rules/ffi-marshalling.md` §B1/§B5/§B7) so the Critic's review ground truth matches the single-owner code this phase lands (Actor deliverable — see "FFI rules-doc update").

**Explicitly out of scope (do NOT build):** `poll` and the entire receive path (Category 3/4 handles, `ConsumerRecord(s)`, length-delimited `out_len` strings §B3, copy-out §6.4); any new async op beyond the existing proof pair (`SubscribeAsync` / `SeekAsync`) + the existing `close_async`; any public client surface (`IConsumer` / `KafkaConsumer` / public `MockConsumer` / `ConsumerGroupMetadata` / `ConsumerRecord(s)` / rebalance listeners); all producer interop + the §A7 producer completion decision; serializers / generic `Consumer<TKey,TValue>`; typed `KafkaException` subclasses; **any `src/ffi` / Rust / header change** (Mode A). The **marshalling-location** question (does `FromHandle` run on the dispatcher thread) is **not** touched here (see "Marshalling stays put").

---

## The change spec — DROP / KEEP / ADD

Verified against the current code at line level. Anchors: `Internal/ConsumerAccessGuard.cs`, `Internal/OperationCompletionSource.cs`, `Internal/NativeConsumer.cs`, `Internal/Interop/ConsumerCallbacks.cs`.

### DROP

1. **`ConsumerAccessGuard.cs` (whole file)** + all its wiring:
   - the `_guard` field in `NativeConsumer` (`NativeConsumer.cs:82`);
   - `_guard.EnterOperation()` in `SubmitVoidOperation` (`:549`);
   - `_guard.EnterStateRead()` + `_guard.Release()` in `GroupId` (`:360`, `:382`).
2. **`NativeConsumer._inFlightContext`** field (`:102`) + its `Volatile.Write` publish (`:571`).
3. **`NativeConsumer._inFlightOperation`** field (`:91`) + its `Volatile.Write` publish (`:572`).
4. **`OperationCompletionSource`**: the `ConsumerAccessGuard? guard` ctor param + `_guard` field (`:70`, `:79–82`) and every `_guard?.Release()` call (in `Complete` `:137`, in `AbandonBeforeSubmit` `:175`). Ctor becomes parameterless (or takes no guard).
5. **`OperationCompletionSource.FaultTaskOnly`** method (`:221–230`) — removed entirely (nothing calls it once `Dispose` stops tracking/faulting).
6. **`NativeConsumer.Dispose`**: the `_inFlightContext` snapshot read (`:422`) + the `pending?.FaultTaskOnly(...)` call (`:451`). `Dispose` becomes: `TryBeginClose` → `close_with_timeout` → release handle (`Consumer_destroy`). No fault, no tracking.
7. **`NativeConsumer.DisposeAsync`**: the entire drain block — the `_inFlightOperation` read (`:475`), the `ConsumerWakeup` (`:478`), and the `await pending` (`:481`). `DisposeAsync` becomes: `TryBeginClose` → `close_async` (`await CloseAsyncInternal()`) → release handle (`Consumer_destroy`).
8. **Tests** — drop / repurpose (see the Tests section for the exact list):
   - `ConsumerAccessGuardTests.cs` (whole file) — the guard is gone;
   - the four `FaultTaskOnly_*` bridge component tests in `ConsumerCompletionBridgeTests.cs`;
   - the `Dispose_WithOpInFlight_*` "op completes / no-strand" assertions in `ConsumerAsyncTeardownTests.cs`;
   - the concurrency-matrix tests (the async-op-→-`KafkaException` and state-read-→-`InvalidOperationException` overlap assertions currently proven at the guard-component level).

### KEEP — the 5 invariants (non-negotiable) + the bridge core

These are what keep .NET **no-worse-than-Python** after the managed guard is gone. The **core** guard in Rust still serializes ops; these five are the managed-side safety that does not depend on the managed guard:

1. **Per-op self-rooting `GCHandle`** — `GCHandle.Alloc(context, Normal)` + `SetGcHandle` in `SubmitVoidOperation` (`:552–553`) and `CloseAsyncInternal` (`:518–519`). Roots the per-op context from submit until the callback fires.
2. **Callback = sole owner of the free** — `FreeGcHandle` is called ONLY from `ConsumerCallbacks.OnOperation` (the `finally`, `ConsumerCallbacks.cs:79`) and from `AbandonBeforeSubmit` (the submit-threw path where native never ran, `OperationCompletionSource.cs:176`). **No teardown-side free anywhere.** (This invariant already holds post-Finding-3 and must be preserved; removing `FaultTaskOnly` does not touch it.)
3. **`_closed` stays atomic** — the `int _closed` field + `TryBeginClose` (`Interlocked.CompareExchange`, `:581`) + `ThrowIfClosed` (`Volatile.Read`, `:585`). **This is the teardown gate, NOT the guard** — keep it. It is the no-GIL requirement: a plain `bool` would be a torn-read race .NET has and Python's GIL hides. (Removing the managed guard does NOT remove `_closed`.)
4. **`SafeHandle` + `TaskCompletionSource`** built-in thread-safety — `SafeConsumerHandle` (`ownsHandle`, release → `Consumer_destroy`) and the TCS's own thread-safe `TrySet*`. Unchanged.
5. **`RunContinuationsAsynchronously`** on the TCS (`OperationCompletionSource.cs:66`) — mandatory: the callback fires on the core's foreign dispatcher thread, so the awaiter's continuation must not run there (ffi §B7).

Also KEEP (unchanged unless a DROP item forces a mechanical edit):
- the whole `ConsumerCallbacks` trampoline (delegate type + `static readonly Operation` + the no-throw `OnOperation` body);
- all 4 async `NativeMethods` DllImports (`subscribe_async` / `seek_async` / `wakeup` / `close_async`) and the M2 `close_with_timeout` / `destroy`;
- `OperationCompletionSource` **minus** the guard bits: the TCS, `SetGcHandle`, `RegisterCancellation` (cancellation → `wakeup` → `OperationCanceledException`), `Complete` (error marshalling via `FromHandle`, cancel-translation), `AbandonBeforeSubmit`, `FreeGcHandle`, `TrySetException`;
- **`Wakeup` keeps its `if (Volatile.Read(ref _closed) != 0) return;` check** (`:340–343`). Do NOT drop it to match Python — it is strictly safer than Python's unconditional `_lib.Consumer_wakeup(self._h)` and is cheap. (Python has no closed-flag on `wakeup`; keeping ours is an accepted, deliberate divergence in the safe direction.)

### ADD

- **Per-method concurrency-rejection surfacing on the sync state read `GroupId`.** After removing the managed `EnterStateRead`/`Release`, `GroupId` must still surface a concurrent-rejection the Python way. The core's `Consumer_group_metadata` returns a **null** handle when its own guard rejects concurrent access (`NativeConsumer.cs:363–369` already reads this and currently returns `null`). Change that path to **throw `InvalidOperationException`** ("KafkaConsumer is not safe for multi-threaded access.") instead of returning `null` — mirroring Python's `None → RuntimeError` in `_concurrent_error()` (`consumer.py:223–224`, `270–274`) and the CLAUDE.md §3 idiom map row (`ConcurrentModificationException` → `InvalidOperationException` for a concurrent sync state read).
- **No ADD needed for the async ops.** A concurrent async op is rejected by the **core** inline (the `async_void_op` guard fires the callback inline on the caller thread with a `ConcurrentModification` error — M3/P1 source-verified finding #2), which the existing bridge surfaces as a **faulted `Task`** carrying a `KafkaException` (via `Complete` → `FromHandle`). That is exactly the Python-async behavior; accept the faulted-`Task` delivery instead of a synchronous throw. (Before, the managed `EnterOperation` threw synchronously; after, the core delivers the same rejection through the `Task`. Same observable contract per the idiom map: concurrent async op → `KafkaException`.)

### Also KEEP — explicit non-change: marshalling stays put

**Marshalling stays on the dispatcher thread** for this void-only phase. Reading the `KafkaError` via `KafkaException.FromHandle` inside `Complete` (which runs on the core's dispatcher thread) is just a handle read + copy-out; it is orthogonal to this refactor and is the documented §B7 .NET design — a legitimate no-GIL divergence from Python. **WHERE marshalling runs is revisited at the poll / receive path** (where a full batch is marshalled), not here. This plan changes *who tracks/guards the op*, not *where its error is marshalled*.

---

## Rationale + accepted residuals — MUST be captured in code

Named Actor deliverable: capture the **rationale** and the **explicitly-accepted residuals** in **XML doc comments (`///`)** where doc comments already exist or are natural, and in **plain code comments (`//`)** where they do not — on the affected members. Concretely:

- **`NativeConsumer` class doc + `Dispose` / `DisposeAsync`:** the single-owner "not thread-safe" model; that an **unawaited in-flight op at teardown may strand + leak** (accepted, misuse-only); that `DisposeAsync` is the preferred teardown but that **under this model neither `Dispose` nor `DisposeAsync` drains a *separate* op** — `DisposeAsync` closes gracefully (`close_async` joins the bg task) but no longer wakes+awaits a tracked in-flight op, because the op is single-owner and there is no concurrent submitter to drain; matches the Python sibling (`close()` drains its *own* awaited op, then bare `_destroy`). WHY: single-owner Kafka contract, cross-binding parity, reversible.
- **`Wakeup` / `GroupId`:** the check-then-use handle **TOCTOU** vs a concurrent teardown is an **accepted residual** under the not-thread-safe contract (reachable only under cross-thread misuse; Python has the same, more exposed — its `wakeup` has no closed check at all).
- **`OperationCompletionSource`:** the callback is the **sole owner** of the `GCHandle` free (invariant #2); why `RunContinuationsAsynchronously` is mandatory (invariant #5).
- Where the Apache-2.0 header + existing docstring conventions already apply, follow them (every changed/new `.cs` file keeps the header; no TODO/FIXME).

---

## FFI rules-doc update — `ffi-marshalling.md` §B1/§B5/§B7 (Actor deliverable)

The Critic reviews against the FFI rules doc as ground truth, so the doc must describe the model this phase lands, not the M3/P1 managed-guard model. This is a **C#-side documentation edit the `dotnet-actor` owns** (it documents .NET interop behavior, not Rust). Update `bindings/dotnet/.claude/rules/ffi-marshalling.md` in these three sections — accurate to the code M3/P2 produces, no overstatement:

- **§B1 (consumer thread model).** Today it asserts a **managed** access guard serializes ops ("One operation in flight per consumer — the access guard serializes ops … released just before the callback fires"). After M3/P2: the **Rust core** guard is the serializer; the **managed mirror is removed**. A concurrent async op is rejected by the core and surfaces as a **faulted `Task`** carrying `ConcurrentModification` (delivered inline by the core, not by a managed pre-check); a concurrent **sync state read** surfaces as `InvalidOperationException` from the null-handle path. Update the `Dispose` line from "wakes+awaits the in-flight op" to the single-owner teardown (`close_(with_timeout|async) → destroy`, no separate-op drain). **Preserve** the still-true parts: foreign-thread dispatcher callback, `RunContinuationsAsynchronously`, no-throw boundary.

- **§B5 (error model — wakeup / concurrent).** Today the "Concurrent use" row implies a **managed** split (state read → `InvalidOperationException`, async op → `KafkaException`) enforced managed-side. After M3/P2, keep the **observable** split but re-attribute the mechanism: the async-op `KafkaException` (`ConcurrentModification`) is delivered by the **core inline** through the faulted `Task` (not a managed synchronous throw from an `EnterOperation` pre-check); the state-read `InvalidOperationException` is thrown from `GroupId`'s **null-handle** (core-rejection) path. Keep the Python-parity framing and the wakeup / `CancellationToken` shapes unchanged.

- **§B7 (async completion / teardown).** Today it says `Dispose` "drain / `wakeup` the in-flight op → `Consumer_close` → `Consumer_destroy`" and describes the guard "released just before the callback fires". After M3/P2: teardown is `close_(with_timeout|async) → destroy` with **no separate-op drain** — under single-owner the awaiter *is* the disposer, so there is no concurrent submitter to drain; and the serializing guard is the **core's**, not a managed one. **Preserve**: the callback = sole owner of the `GCHandle` free, `RunContinuationsAsynchronously` mandatory (callback on the foreign dispatcher thread), no-throw boundary, and that a bare `Consumer_destroy` without the graceful close still strands + leaks (now the *accepted* single-owner residual for an unawaited op).

Edits must match the code the plan produces — do not overstate (e.g. do not claim the managed guard still exists, and do not claim `DisposeAsync` drains a separately-submitted op). `.claude/rules/ffi-marshalling.md` is added to "Files touched → Modified (design/rules)". **DoD/verification note:** the Critic checks the §B1/§B5/§B7 text against the landed `NativeConsumer` / `OperationCompletionSource` code — the doc and code must agree (a mismatch is a review finding).

---

## Accepted residuals (enumerated) + STATUS reconciliation

These are **explicitly accepted, misuse-only, not-reachable-while-internal** residuals — Python parity — under the single-owner not-thread-safe contract:

- **teardown-with-unawaited-in-flight-op → strand + one-time `GCHandle`/context leak.** The Finding-1 behavior (sync `Dispose` faulting the stranded `Task`) is intentionally **NOT re-added** — the managed fault machinery is exactly what we are removing. An op that is submitted and then not awaited across a teardown may strand its `Task` and leak its per-op `GCHandle` once. Python accepts the same (bare `_destroy` after draining its *own* awaited op); `DisposeAsync` on the *awaiting* task is the clean path.
- **`Wakeup` / `GroupId` handle TOCTOU vs teardown → UAF under cross-thread misuse.** (Finding 2 hazard; accepted-by-design here.)
- **submit-vs-`destroy` handle race → UAF under cross-thread misuse.** (The `DangerousGetHandle()` in `SubmitVoidOperation` vs a concurrent `Consumer_destroy`.)

**What this refactor ELIMINATES:** the M3/P1 **publish window** (the strand+leak race from op-live-before-`_inFlight*`-published). With the tracking fields gone, there is no window and nothing to publish — the race is structurally removed, not merely deferred.

**STATUS reconciliation (resolve the N=6 numbering collision).** `STATUS.md` currently carries two "Deferred hardening (N=6 …)" items pre-labeled for a *future* review:
1. "N=6 — public client cross-thread wakeup: `Wakeup()`/`GroupId()` TOCTOU vs teardown"; and
2. "N=6 — concurrent public client + teardown: op-submit vs concurrent teardown window."
M3/P2 **takes review counter N=6**, so those labels now collide with this phase. Resolve cleanly:
- **Item 1** (the `Wakeup`/`GroupId` TOCTOU) is **re-contextualized as an accepted-by-design residual of the single-owner model** (it is now one of the three enumerated residuals above), not a pending N=6 fix. If a *future* hardening of it is ever wanted, it is renumbered to **N≥7** (whenever the public client makes `Wakeup()` genuinely cross-thread).
- **Item 2** (the op-submit-vs-teardown window) is **ELIMINATED** by this phase (the publish window is gone), so it is closed, not carried. Any residual submit-vs-`destroy` *handle* race is folded into residual 3 above (accepted-by-design), and any future hardening is **N≥7**.

The Actor updates `STATUS.md` to make this reconciliation explicit — no dangling or contradictory "N=6 deferred" label remains after this phase (the only N=6 is M3/P2 itself).

---

## Files touched

**Deleted:**
- `src/Confluent.Kafka/Internal/ConsumerAccessGuard.cs`
- `tests/Confluent.Kafka.UnitTests/ConsumerAccessGuardTests.cs`

**Modified (library):**
- `src/Confluent.Kafka/Internal/OperationCompletionSource.cs` — drop the guard param/field + `_guard?.Release()` calls; delete `FaultTaskOnly`; refresh the class/`Complete`/`AbandonBeforeSubmit` docs (sole-owner-free, `RunContinuationsAsynchronously`).
- `src/Confluent.Kafka/Internal/NativeConsumer.cs` — drop `_guard`, `_inFlightContext`, `_inFlightOperation` + their wiring; simplify `SubmitVoidOperation` (no guard, no publish), `Dispose` (no snapshot/fault), `DisposeAsync` (no drain block); `GroupId` throws `InvalidOperationException` on the null/concurrent-rejection path; rewrite the class doc + `Dispose`/`DisposeAsync`/`Wakeup`/`GroupId` docs to the single-owner model with the accepted residuals.
- (`ConsumerCallbacks.cs`, `NativeMethods.cs`, `SafeConsumerHandle.cs`, `SafeConsumerPropertiesHandle.cs`, `Utf8Marshal.cs`, `KafkaException.cs` — **unchanged**.)

**Modified (tests):**
- `tests/Confluent.Kafka.UnitTests/Interop/ConsumerCompletionBridgeTests.cs` — remove the four `FaultTaskOnly_*` tests; keep every bridge success/failure/no-throw/GC-keep-alive/`RunContinuationsAsynchronously`/chained-ops test.
- `tests/Confluent.Kafka.UnitTests/Interop/ConsumerAsyncTeardownTests.cs` — repurpose the `Dispose_WithOpInFlight_*` cases to assert `Dispose`/`DisposeAsync` **return without hanging** (NOT that an in-flight op's `Task` completes); keep double/mixed/concurrent-teardown-safe and use-after-dispose → `ObjectDisposedException`.
- `tests/Confluent.Kafka.UnitTests/Interop/ConsumerAsyncOperationTests.cs` — drop the guard-overlap concurrency assertions; **add** `GroupId` concurrent-rejection → `InvalidOperationException`; keep `Wakeup` (safe/reusable), cancellation → `OperationCanceledException`, and the guarded-`GroupId` round-trip (now unguarded, still round-trips incl. non-ASCII).

**Modified (design/rules):**
- `bindings/dotnet/design/current/STATUS.md` — record the M3/P2 decision + the STATUS reconciliation above (Actor deliverable).
- `bindings/dotnet/.claude/rules/ffi-marshalling.md` — update §B1/§B5/§B7 to the single-owner / no-managed-guard model so the Critic's review ground truth matches the code (Actor deliverable — see "FFI rules-doc update").

---

## Tests — drop / repurpose / add

**Drop:**
- `ConsumerAccessGuardTests.cs` (whole file) — the guard is gone.
- The four `FaultTaskOnly_*` component tests in `ConsumerCompletionBridgeTests.cs`.
- The async-op-overlap-→-`KafkaException` and state-read-overlap-→-`InvalidOperationException` **guard-component** overlap assertions (the concurrency matrix).

**Repurpose:**
- `Dispose_WithOpInFlight_*` (in `ConsumerAsyncTeardownTests.cs`) → assert `Dispose` / `DisposeAsync` **return** (no hang, under `TestTimeout`). They must NOT assert that a separate in-flight op's `Task` reaches a terminal state (that machinery is removed). Keep the churn/GC variant only as a "teardown returns under churn" no-hang check.

**Add:**
- `GroupId` under a **concurrent core-guard rejection** → `InvalidOperationException` (mirrors Python `_concurrent_error`). If a deterministic concurrent overlap is not reproducible broker-free (instant Mock ops made it non-deterministic in M3/P1 — see M3/P1 D2), test the mapping at the smallest reachable seam: assert that the `null`-metadata path throws `InvalidOperationException` (not returns `null`). Document any determinism deviation in `COMMENTS.DONE.6.md`, as M3/P1 did for D1/D2.

**Keep (unchanged):**
- The bridge success (`subscribe_async`), failure (`seek_async` unassigned → `KafkaException`), no-throw boundary, GCHandle keep-alive under GC, `RunContinuationsAsynchronously` (continuation off the completing thread), and chained-ops tests.
- Cancellation → `OperationCanceledException` (pre-canceled token).
- `Wakeup` safe / reusable; double/mixed/concurrent teardown safe; use-after-dispose → `ObjectDisposedException`.
- All carried M0–M2 tests.

---

## Verification gates (.NET DoD — NOT `cargo xtask` / `make verify`)

Per `bindings/dotnet/CLAUDE.md §7` and `.claude/rules/definition-of-done.md`:

1. **`cargo build --features ffi`** — native cdylib + regenerated header present. **Run FIRST** (CLAUDE.md §7.1). (No ABI change this phase, but the native must exist for the .NET build/test to load.)
2. **`dotnet build`** — **0 warnings / 0 errors** across all library TFMs (`netstandard2.0;net8.0;net10.0`) and both test TFMs (`net8.0;net10.0`), with `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. No new public type → no new CS1591 surface. Apache-2.0 header on every changed/new `.cs`; **no TODO/FIXME**.
3. **`dotnet test -f net10.0`** — the bridge / op / cancellation / teardown tests pass and **do NOT hang** (every awaited op AND teardown under a `TestTimeout` guard). The dropped tests are removed cleanly (no dangling references to `ConsumerAccessGuard` / `FaultTaskOnly` / `_inFlight*`).
4. **`dotnet format --verify-no-changes`** — clean.
5. **CI-only caveat (not blocking):** only the .NET 10 runtime is installed locally → the net8.0 test *run* + net462 (via netstandard2.0) are CI-only; both *build* legs must pass.
6. **Docs match code:** `ffi-marshalling.md` §B1/§B5/§B7 must describe the single-owner / no-managed-guard model the landed `NativeConsumer` / `OperationCompletionSource` code actually implements (no managed guard, faulted-`Task` concurrent-async, `InvalidOperationException` state read, `close → destroy` teardown with no separate-op drain). A doc/code mismatch is a Critic finding.

---

## Governance

- **Relationship to PR #135 — extends #135 on the same branch (CONFIRMED).** M3/P2 commits land **directly on `prashah_dev_asyncbridge_scaffolding`**, which **is PR #135's head branch** (base `prashah_dev_dotnet_errormodel_safehandle_scaffolding`). So these commits **extend PR #135** — #135 grows to contain the full "M3/P1 adds the guard + in-flight tracking, then M3/P2 removes it" build-then-simplify arc. There is **no new branch and no separate PR**.
  - **Additive commits — NO history rewrite.** M3/P2 lands as new commits on top of M3/P1's; do NOT rewrite, rebase, squash, or amend #135's M3/P1 commits. "Do not amend #135" now means *do not rewrite its history* — just add on top. Keep the history honest: M3/P1 **built** the guard + tracking, then M3/P2 **chose to simplify** to the Python single-owner model, and both steps stay visible in #135's commit log.
- **Personas:** `dotnet-actor` (N=6) implements; `dotnet-critic` (N=6) reviews. NEVER the Rust `actor-executor` / `kafka-critic`. Nested-agent discovery does not work — copy both persona files to repo-root `.claude/agents/` to invoke them, and keep those root copies **untracked** (CLAUDE.md §8.4).
- **Review mechanics:** working `bindings/dotnet/COMMENTS.6.md` (gitignored) → resolved to `bindings/dotnet/COMMENTS.DONE.6.md`. NEVER `git add` either at the binding root; the single tracked record is the Manager's archived copy at `design/history/M3/P2-single-owner-alignment/COMMENTS.DONE.6.md`.
- **Precedents to cite in code/comments:** the in-repo Python sibling (`bindings/python/consumer.py` single-owner docstring + `_concurrent_error`; `bindings/python/_confluentkafka.c` op trampoline `Py_DECREF` = callback-sole-owner-free), and confluent-kafka-dotnet (`/Users/pranavshah/WorkSpace/Confluent/confluent-kafka-dotnet`: `gch.Free()` in the delivery-report callback = callback-sole-owner; `Dispose` drains via `callbackTask.Wait()` then destroys = teardown-drains-not-reclaims).
- **Commit style:** small incremental commits, each passing the .NET gates; Apache-2.0 header on new files. Verify each commit's staged file list excludes agent-memory + the root persona copies + `COMMENTS.*.md`.

---

## Confirmed decisions (M3/P2)

All four questions raised in the first review round are now **confirmed by the user**. They are decided, not open; the "Risks / open questions" section below is retained only as a resolution log.

- **D-Q1 — `DisposeAsync` no longer drains a separately-submitted in-flight op. CONFIRMED.** After dropping the drain block, `DisposeAsync` is `close_async → destroy` (no wakeup+await of a *tracked* op); it closes gracefully (`close_async` joins the bg task) but does not special-case a separately-submitted in-flight op. This matches Python's `close()` (drains its *own* awaited op, then bare `_destroy`). The accepted residual — **an *unawaited* in-flight op + teardown may strand + leak once** — is already the first enumerated residual above; this decision just marks it decided.
- **D-Q2 — `GroupId`: `return null` → `throw InvalidOperationException` on the core's concurrent-rejection (null-handle) path. CONFIRMED.** Internal-only (no public contract breaks); mirrors Python `_concurrent_error()` (`None → RuntimeError`) and the CLAUDE.md §3 idiom-map row (concurrent sync state read → `InvalidOperationException`).
- **D-Q3 — branch / PR shape. RESOLVED by Revision 1 / Governance.** M3/P2 lands as additive commits on **`prashah_dev_asyncbridge_scaffolding`** (PR #135's head), extending #135 — **no new branch, no separate PR, no history rewrite.**
- **D-Q4 — `GroupId` concurrent-test determinism. CONFIRMED approach: do NOT write a flaky forced-overlap test.** Broker-free `MockConsumer` ops resolve instantly (the core guard is held only microseconds), and the one guard-holding op with a controllable duration is `poll` (out of scope). So the concurrent → `InvalidOperationException` overlap is **not deterministically reproducible this phase**. Instead, test the **reachable seam** — `GroupId` round-trips the group id normally (incl. non-ASCII), asserting the mapping at the smallest reachable point (the null-handle path throws `InvalidOperationException`, not returns `null`) — and **document the determinism deviation in `COMMENTS.DONE.6.md`**, mirroring exactly how M3/P1 documented **D1** (wakeup-fault not reachable without `poll`) and **D2** (guard matrix tested at the component level, not via a forced native overlap).
  - **Honest cost, noted:** removing the managed guard also **removes the deterministic component-level test M3/P1 had** (`ConsumerAccessGuardTests`), so this one concurrency behavior **regresses from deterministic (component-level) to non-deterministic** — accepted and documented in `COMMENTS.DONE.6.md`.

---

## Risks / open questions for the user — RESOLVED (resolution log)

*(All four confirmed — see "Confirmed decisions (M3/P2)". Kept for traceability.)*

1. **RESOLVED (D-Q1) — DisposeAsync semantics under single-owner.** `DisposeAsync` becomes `close_async → destroy` (no wakeup+await of a tracked op); it does NOT special-case a separately-submitted in-flight op — matching Python's `close()`. Accepted residual: an unawaited in-flight op + teardown may strand + leak once.
2. **RESOLVED (D-Q2) — `GroupId` throw vs the current `null` return.** The concurrent-rejection path changes from `return null` to `throw InvalidOperationException`. Internal-only (no public contract break); matches Python + the idiom map.
3. **RESOLVED (D-Q3) — Branch/PR shape.** M3/P2 lands as additive commits on `prashah_dev_asyncbridge_scaffolding` (PR #135's head), **extending #135** — no new branch, no separate PR, no history rewrite (Revision 1 / Governance).
4. **RESOLVED (D-Q4) — Determinism of the `GroupId` concurrent test.** No flaky forced-overlap test; test the reachable seam and document the determinism deviation in `COMMENTS.DONE.6.md`, consistent with M3/P1 D1/D2. Accepted honest cost: dropping the managed guard removes M3/P1's deterministic component-level `ConsumerAccessGuardTests`, so this behavior regresses to non-deterministic.
