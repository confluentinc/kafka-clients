# COMMENTS.DONE.11 — M5/P2 "Consumer Position" (Actor N=11 → Critic N=11)

Closed record for the **M5/P2 — Consumer Position** phase (the tracked archive under the
phase directory, per CLAUDE.md §8.4; the binding-root `COMMENTS.11.md` /
`COMMENTS.DONE.11.md` are local working files and stay untracked). Approved plan:
`design/history/M5/P2-consumer-position/PLAN.md`.

**Mode A** (no Rust authored). Scope: `Task<long> Position(TopicPartition, CancellationToken
= default)` on `IAsyncConsumer` — the third completion-bridge shape (the **scalar** callback
`(int64_t, error*, ud)`). No pre-existing `COMMENTS.11.md` entries to resolve — fresh phase
(initial Actor implementation of an APPROVED plan, not a fixup cycle).

## Decisions / deviations recorded (PLAN §8)

1. **Reuse `OperationCompletionSource<long>` verbatim for the scalar bridge.** No new
   context type, no edit to `OperationCompletionSource.cs`. The generic bridge is already
   result-type-agnostic and does no native reads: `CompleteWithResult(position)` on success,
   `Complete(error)` on failure. The scalar (`long`) is blittable, so the trampoline's
   "marshalling" is trivial (no copy-out). This avoids a bespoke `ScalarCompletionSource`
   duplicating the 5-invariant machinery.

2. **`SubmitScalarOperation<T>` added as a parallel helper, not a generalization of
   `SubmitOperation`.** The scalar callback (`PositionCallback`, `(long, error*, ud)`) has a
   different unmanaged signature than the poll callback (`PollCallback`, `(records*, error*,
   ud)`), so the native submit lambda's parameter type differs. Threading a second callback
   type through the shipped `SubmitOperation<TResult>` would perturb the proven poll seam for
   no benefit. The new helper is a line-for-line clone of `SubmitOperation<TResult>` with
   `Poll → Position`; the void (`SubmitVoidOperation`) and owned-handle (`SubmitOperation`)
   paths are byte-for-byte untouched. Same for the new `NativeScalarSubmit` delegate type
   (parallel to `NativeResultSubmit`).

3. **One `Position` method, no `TimeSpan` overload.** Java's timed `position(tp, Duration)`
   is deferred until the C ABI exposes a timed `position_async` (the shipped `Close`
   precedent for a missing timed ABI). We do NOT add a `TimeSpan` overload that silently
   ignores the value, nor simulate the deadline binding-side. The `CancellationToken` is
   **user-initiated cancellation only — NOT a timeout/deadline**; it maps to `wakeup()`
   (best-effort), mirroring `PollWithCallback`.

4. **The sync `Consumer_position` deliberately NOT declared.** Position blocks in Java → the
   async form is used; wrapping the sync `Consumer_position` in `Task.Run` would be the
   forbidden sync-over-async (ffi §B7). Only `Consumer_position_async` is declared in
   `NativeMethods`.

5. **The one structural difference from `OnPoll`.** `OnPosition`'s `finally` frees **only**
   the per-op `GCHandle` (`context?.FreeGcHandle()`) — there is **no** `NativeMethods.*Destroy`
   call, because the scalar result carries no owned handle. The error handle (failure path)
   is still freed exactly once inside `Complete → KafkaException.FromHandle`.

## Reachability limits (recorded, not silently skipped — D-Q4 / M5/P1 precedent)

6. **Deterministic forced-concurrency / in-flight-wakeup overlap is NOT reachable
   broker-free.** Mock ops resolve instantly; the only guard-holding op with a controllable
   duration is `poll` (it check-and-clears the wakeup flag at Step 4). The mock's `position()`
   does **not** check-and-clear the wakeup flag, so a `Wakeup()`-then-`Position()` sequence
   does not deterministically fault (unlike poll). The concurrency test therefore asserts the
   **reachable seam** (a `Position` round-trips on a free guard) and relies on **code
   inspection** of the shared `Complete(error)` path — reused verbatim from poll — for the
   core-rejection → faulted-`Task` (`ConcurrentModification`) mapping. The wakeup test asserts
   the deterministic **non-corruption / reusable-after-wakeup** property. A genuinely in-flight
   overlap needs a controllable-duration guard-holding mock op — a Rust-core dependency (the
   M3/P3 / D-Q4 / M5/P1 ceiling), not a .NET change.

7. **The `position(tp, Duration)` timeout behavior is not testable** — there is no timed ABI
   form (the overload is deferred, decision 3).

8. **Precondition test placement — negative partition.** `TopicPartition`'s own ctor rejects a
   negative partition (`ArgumentOutOfRangeException`, "Partition must not be negative."), so a
   negative value cannot reach `Position` through a constructed `TopicPartition`. The test
   asserts that ctor guard's exception type + message (which is what `PositionWithCallback`
   would throw were the value smuggled in via a `default`-style struct); the
   `PositionWithCallback` negative-partition guard remains in place as the belt-and-suspenders
   check the ffi §B5 discipline requires. Null topic is reachable via `default(TopicPartition)`
   (a null `Topic`), asserted directly against `Position`.

## Free-exactly-once audit (PLAN §7.7 — the scalar bridge's central obligation)

- Per-op `GCHandle` freed exactly once on **every** path: success (`CompleteWithResult` →
  `finally FreeGcHandle`), operational failure (`Complete(error)` → `finally FreeGcHandle`),
  inline core-rejection (same failure path — the core fires the callback inline), no-throw
  catch (`finally FreeGcHandle`), and submit-threw (`AbandonBeforeSubmit` in
  `SubmitScalarOperation`, since native never ran so the callback never fires). `FreeGcHandle`
  is `Interlocked`-guarded → idempotent.
- Error handle freed exactly once on the failure path via `KafkaException.FromHandle` (its own
  `finally`).
- **No result-handle destroy** in `OnPosition`'s `finally` — the scalar owns none (verified by
  inspection; the sole structural difference from `OnPoll`).

## Commits (branch `prashah_dev_public_consumer_remaining`)

- `7642b18` wire Consumer Position via the scalar completion bridge (`ConsumerCallbacks`
  `OnPosition`/`PositionCallback` + `NativeConsumer` `SubmitScalarOperation`/`PositionWithCallback`
  + `NativeMethods` `Consumer_position_async` + `IAsyncConsumer.Position` + forwards)
- `312f1ab` `PublicConsumerPositionTests` at the test root
- `143ea0b` archive approved plan
- `37fceb9` doc-sync (STATUS entry; `IAsyncConsumer` remarks — `position` removed from
  not-yet-wired)

## DoD (all green — Actor)

1. `cargo build --features ffi` — no ABI change (Mode A); header + native present.
2. `dotnet build` — 0 warnings / 0 errors across all library TFMs (netstandard2.0 / net8.0 /
   net10.0) + all test TFMs (net462 / net8.0 / net10.0). CS1591 on the new public member;
   Apache-2.0 header on the one new file; no TODO/FIXME.
3. `dotnet test` (net10.0, the local runtime) — **141 → 153** (+12), green across ≥4 full
   runs; D8.8 serial execution unchanged. net8.0 *run* + net462 are CI/Windows-only; all three
   test *build* legs pass locally.
4. `dotnet format --verify-no-changes` — clean.

---

## Critic N=11 — review outcome (closed)

**Review (`7642b18` / `312f1ab` / `143ea0b` / `37fceb9`): CLEAN, 0 genuine findings, phase
PASSES.** Independently verified against the C ABI header + the Kafka Java public-API shape +
the approved PLAN (CLAUDE.md §8.2 ground truth):

- **Free-exactly-once (central obligation):** `OnPosition`'s `finally` calls **only**
  `context?.FreeGcHandle()` — **no `*Destroy`** (the one intended difference from `OnPoll`,
  which also does `ConsumerRecordsDestroy`). The per-op `GCHandle` is freed on every path
  (success `CompleteWithResult`; operational failure + inline core-rejection `Complete(error)`;
  no-throw `catch`; submit-threw `AbandonBeforeSubmit`) through the `Interlocked`-guarded
  idempotent `FreeGcHandle`. Error handle freed exactly once via `Complete → FromHandle`.
  Success uses `CompleteWithResult(position)` (not `Complete(IntPtr.Zero)`, which would leave
  the `Task<long>` uncompleted). No double-free / missed-free / UAF.
- **Structural fidelity:** `OperationCompletionSource.cs` **not edited**; `RunContinuationsAsynchronously`
  inherited. The proven paths are **byte-for-byte untouched** (`NativeConsumer.cs` /
  `ConsumerCallbacks.cs` / `NativeMethods.cs` are pure additions, zero deletions);
  `SubmitScalarOperation<T>` / `NativeScalarSubmit` are a line-for-line clone of
  `SubmitOperation<T>`. Call-scoped topic pin matches the `SeekWithCallback` precedent (header
  confirms no borrow past submit — no UAF). One `[DllImport]`, `EntryPoint =
  kafka_consumer_Consumer_position_async`, Cdecl, correct type map; sync `Consumer_position`
  deliberately NOT declared (verified absent).
- **Shape / decision hygiene:** `Task<long> Position(...)` on `IAsyncConsumer` (not
  `IConsumerCommon`), both clients forward, one method (no `TimeSpan` overload), the
  `CancellationToken` documented as **cancellation-not-timeout**, `position` removed from the
  not-yet-wired remarks, CLAUDE.md §3 sketch already matched.
- **Tests:** at the **test root** `PublicConsumerPositionTests.cs` (not `Interop/` — the M5/P1
  correction); unassigned-partition `KafkaException` message asserted verbatim against
  `mock_consumer.rs`; null-topic / negative-partition tests reachable and honest against the
  `TopicPartition` ctor; the D-Q4 non-reachability (mock `position()` does not check the wakeup
  flag — confirmed in core) documented, not silently skipped.
- **DoD independently observed:** `cargo build --features ffi` current; `dotnet build` **0/0**
  across all six TFM legs; `dotnet test -f net10.0` **153 passed / 0 failed**, looped **22×**
  all green (Position tests 8× green in isolation); `dotnet format --verify-no-changes` clean.
  No `COMMENTS.11.md` findings written.

**Loop closed:** Actor N=11 → Critic N=11 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments.
