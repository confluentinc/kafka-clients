# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

- **Milestone 3 / Phase 1 — "Completion bridge + first async op (consumer,
  proof-of-plumbing)": DONE (2026-07-27).** The foreign-thread completion callback
  → `Task` bridge — the riskiest new machinery — de-risked BEFORE poll / the
  receive path. Activates the `_async`/callback ABI for the first time (Mode A, no
  Rust authored). Delivered: the void-result bridge (`OperationCompletionSource`,
  `TaskCompletionSource` with `RunContinuationsAsynchronously`, GCHandle keep-alive
  submit→fire, no-throw callback boundary, free-exactly-once); the managed
  one-op-in-flight `ConsumerAccessGuard` (mirrors — does not replace — the core
  guard); two thin proof ops on one bridge — `SubscribeAsync` (SUCCESS) and
  `SeekAsync` unassigned (FAILURE); `Wakeup()` + `CancellationToken` mapping;
  async-aware teardown (`IAsyncDisposable.DisposeAsync` drain→`close_async`→destroy,
  un-defers M2/P1 D3) with the N=5-deferred teardown-thread-safety hardening folded
  in (thread-safe closed flag). NO poll / receive path (Category 3/4 handles,
  `ConsumerRecord(s)`, length-delimited `out_len` strings, copy-out), NO other async
  ops, NO public client type (`KafkaException` remains the only public type) — all
  deferred. Approved plan + closed record:
  `design/history/M3/P1-completion-bridge/`.
- **Milestone 2 / Phase 2 — "SafeHandle marshaller-return hardening": DONE
  (2026-07-22).** The three owned-handle constructors
  (`ConsumerProperties_new` / `KafkaConsumer_new` / `MockConsumer_new`) now
  return their `SafeHandle` subtype **directly** instead of a raw `IntPtr`, so the
  interop marshaller creates-and-sets the handle atomically inside a constrained
  region — closing the M2/P1 `new + SetHandle` allocation-gap window (an async
  abort on net462, or OOM, between obtaining the pointer and `SetHandle` would
  leak the native handle). A **hardening** change, not a bug fix (M2/P1 is correct
  on net8/net10). Mode A (no ABI/Rust change; SafeHandle-return is a classic
  `[DllImport]` feature supported on the netstandard2.0 floor incl. net462).
  `SafeConsumerHandle.FromRaw` removed; `SafeConsumerPropertiesHandle.Create`
  collapses to the marshaller return. NO new public API, NO completion bridge, NO
  poll/subscribe/commit — unchanged from M2/P1 scope. Approved plan + closed
  record: `design/history/M2/P2-safehandle-return-hardening/`.
- **Milestone 2 / Phase 1 — "Error model + first SafeHandle (consumer client
  lifecycle)": DONE (2026-07-21).** The first PUBLIC type (`KafkaException`) plus
  the Category-1 owned-handle consumer lifecycle (create → close → destroy), kept
  INTERNAL (`NativeConsumer`, tested via `InternalsVisibleTo`). Activated the five
  `kafka_common_KafkaError_*` DllImports (declared in M1/P1) as live callers via
  `KafkaException.FromHandle`. Mode A (consumer C ABI already landed — no Rust
  authoring). NO completion bridge, NO poll/subscribe/commit, NO producer, NO
  Category 3/4 receive-path handles, NO public client type yet — all deferred.
  Approved plan + closed record: `design/history/M2/P1-error-model-safehandle/`.
- **Milestone 1 / Phase 1 — "Interop scaffolding + native-load probe": DONE
  (2026-07-20).** The client-agnostic interop FOUNDATION: the `NativeMethods` P/Invoke
  class (8 shared-foundation declarations), the `Utf8Marshal` marshalling helpers, the
  native-copy MSBuild target (un-defers M0/P0 decision D2), and a consumer-namespaced
  native-load probe. Mode A (C ABI already landed — no Rust authoring). NO public
  managed API, NO `SafeHandle`, NO completion bridge, NO Kafka logic yet — all
  deferred to later phases by scope.
- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).** Pure
  structural skeleton (see `design/history/M0/P0-scaffolding/`).

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              strong-name signing (shared .snk, both projects)
│                                              — NO AllowUnsafeBlocks here (library-only, M1/P1 D2)
├─ .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka.ShareConsumer/
│     ├─ Confluent.Kafka.ShareConsumer.csproj  ← TFMs netstandard2.0;net8.0;net10.0;
│     │                                           M1/P1: + <AllowUnsafeBlocks> (library only),
│     │                                           + native-copy MSBuild target (per-OS filename via
│     │                                           IsOSPlatform, profile from $(Configuration),
│     │                                           repo root 4 levels up, <Content> transitive,
│     │                                           + <Error> guard if native absent)
│     ├─ KafkaException.cs                      ← M2/P1: FIRST public type. sealed KafkaException :
│     │                                            Exception, flat Code/IsRetriable/IsFatal + Message;
│     │                                            internal FromHandle(IntPtr) (msg before free, copy
│     │                                            out, destroy in finally); flat-now/typed-later
│     └─ Internal/
│        ├─ NativeConsumer.cs                   ← M2/P1: internal lifecycle wrapper (unsafe-free, D4):
│        │                                         config -> ConsumerProperties_put -> KafkaConsumer_new
│        │                                         (FromHandle on out_error) -> SafeConsumerHandle;
│        │                                         graceful Dispose (close_with_timeout -> destroy);
│        │                                         preconditions -> ArgumentNullException/ArgumentException.
│        │                                         M2/P2: consumes the SafeHandle returns (dispose the
│        │                                         IsInvalid handle on error; defensive IsInvalid guard).
│        │                                         M3/P1: proof async ops (SubscribeAsync/SeekAsync via a
│        │                                         shared SubmitVoidOperation), Wakeup(), guarded GroupId()
│        │                                         state read; thread-safe closed flag (folds N=5 deferred);
│        │                                         IAsyncDisposable.DisposeAsync (drain->close_async->destroy)
│        ├─ OperationCompletionSource.cs        ← M3/P1: per-op callback->TCS context; TCS built with
│        │                                         RunContinuationsAsynchronously; guard release before Task
│        │                                         completion; KafkaError->KafkaException (FromHandle);
│        │                                         CancellationToken->wakeup + OperationCanceledException;
│        │                                         idempotent GCHandle free (Complete/AbandonBeforeSubmit)
│        ├─ ConsumerAccessGuard.cs              ← M3/P1: Interlocked one-op-in-flight guard (mirrors core):
│        │                                         async op -> KafkaException; state read -> InvalidOperation
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                        ← internal static class NativeMethods: M1/P1 (8
│           │                                      shared decls) + M2/P1 consumer lifecycle
│           │                                      (KafkaConsumer_new/MockConsumer_new/close/
│           │                                      close_with_timeout/destroy) + group-metadata trio.
│           │                                      M2/P2: the three constructors return their SafeHandle
│           │                                      subtype directly (marshaller create-and-set).
│           │                                      M3/P1: 4 async decls — subscribe_async (topics as
│           │                                      IntPtr[] = const char* const*) / seek_async / wakeup /
│           │                                      close_async (op-callback as a kept-alive Cdecl delegate)
│           ├─ ConsumerCallbacks.cs                   ← M3/P1: [UnmanagedFunctionPointer(Cdecl)]
│           │                                      OperationCallback delegate type + one static readonly
│           │                                      rooted instance + the no-throw callback body
│           ├─ SafeHandleZeroIsInvalid.cs             ← M2/P1: shared base, IsInvalid => handle==Zero (D2)
│           ├─ SafeConsumerPropertiesHandle.cs        ← M2/P1: config handle (-> ConsumerProperties_destroy);
│           │                                            M2/P2: Create collapses to the marshaller return
│           ├─ SafeConsumerHandle.cs                  ← M2/P1: client handle (-> Consumer_destroy, bare
│           │                                            last-resort; graceful close is in NativeConsumer).
│           │                                            M2/P2: FromRaw removed (arrives marshaller-wrapped)
│           └─ Utf8Marshal.cs                          ← internal static class Utf8Marshal: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/  ← TFMs net8.0;net10.0, unsafe-free
      ├─ TfmSentinelTests.cs                    ← M0/P0 TFM-sentinel smoke test (root: harness-level)
      ├─ KafkaExceptionTests.cs                 ← M2/P1: public-type test (root): classic -> Code 35
      │                                            + I1 both-false + msg; café msg echo; FromHandle(Zero)
      ├─ ConsumerAccessGuardTests.cs            ← M3/P1: concurrency exception-type matrix (component,
      │                                            deterministic): async op -> KafkaException; state read
      │                                            -> InvalidOperationException; reusable after Release
      ├─ TestTimeout.cs                         ← M2/P1: fail-fast deadline helper (hang -> test failure).
      │                                            M3/P1: + async Run(Func<Task>) overload (bridge/drain guard)
      └─ Interop/                               ← mirrors the library interop area (public test
         │                                         classes; "Interop" not "Internal/Interop" — the
         │                                         Internal visibility marker is library-only, §2)
         ├─ NativeLoadProbeTests.cs             ← M1/P1: 2 tests, both invoke a native [DllImport]
         │                                         (smoke new/put/destroy; non-ASCII put no-crash).
         │                                         M2/P2: props via `using SafeConsumerPropertiesHandle`
         │                                         (Dispose frees; asserts !IsInvalid)
         ├─ Utf8MarshalTests.cs                 ← M1/P1: managed Utf8Marshal codec round-trip
         │                                           + PtrToString(Zero)==null (no native call)
         ├─ SafeConsumerHandleTests.cs          ← M2/P1: lifecycle (mock + real), double-Dispose,
         │                                           use-after-Dispose, create/dispose many.
         │                                           M2/P2: KEY regression — classic-protocol
         │                                           KafkaConsumer_new -> IsInvalid handle + non-null
         │                                           out_error; Dispose skips ReleaseHandle (no spurious
         │                                           destroy); error round-trips via FromHandle
         ├─ ConsumerConfigMarshalTests.cs       ← M2/P1: config success + preconditions (null dict /
         │                                           null value / post-Dispose)
         ├─ Utf8RoundTripTests.cs               ← M2/P1 (D5 CLOSED): non-ASCII group.id -> group_metadata
         │                                         -> group_id readback == input (broker-free)
         ├─ ConsumerCompletionBridgeTests.cs    ← M3/P1: SUCCESS (subscribe, churned) + FAILURE (seek
         │                                         unassigned -> KafkaException Code -1/flags/Message,
         │                                         churned); no-throw boundary; GCHandle keep-alive under
         │                                         GC; RunContinuationsAsynchronously (bridge driven
         │                                         directly, off the completing thread); chained ops
         ├─ ConsumerAsyncOperationTests.cs      ← M3/P1: wakeup (safe/reusable/during-op); cancellation
         │                                         (pre-canceled -> OperationCanceledException); guarded
         │                                         GroupId read round-trip (incl. non-ASCII)
         └─ ConsumerAsyncTeardownTests.cs       ← M3/P1: DisposeAsync + Dispose with op in flight RETURN
                                                   (drain/no-hang); double/mixed/concurrent teardown safe;
                                                   use-after-dispose -> ObjectDisposedException
```

## Verification state (M3/P1 DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. No new public
  type → no new CS1591 surface. The ns2.0 leg resolves `IAsyncDisposable` /
  `ValueTask` via `Microsoft.Bcl.AsyncInterfaces` (M3/P1 D3).
- `dotnet test -f net10.0` — **52 passed, 0 failed** (M3/P1 set incl. the N=5
  Finding-1/Finding-3 fixups: 5 access guard, 12 completion bridge — the four
  `FaultTaskOnly_*` among them, 6 async op, teardown, and the carried M0–M2 tests);
  ~370 ms — every awaited op / teardown under a `TestTimeout` hang guard, so a
  bridge/drain hang would fail fast.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M3/P1)

- **D1 — wakeup-fault on the in-flight proof op is NOT reachable this phase
  (source-verified deviation from the PLAN's literal wakeup test).** A
  `MockConsumer` observes `wakeup()` **only** inside `poll()`
  (`src/consumer/mock_consumer.rs` poll Step 4); `subscribe`/`seek` never check the
  flag, and `acquire()` (`src/ffi/consumer.rs`) does not either — so the "in-flight
  op faults with a Wakeup `KafkaException` once" assertion needs `poll` (out of
  scope). The full wakeup + cancellation machinery is implemented (correct once poll
  lands); the tested slices are the reachable ones: `Wakeup()` is safe / leaves the
  consumer reusable, and a **pre-canceled** token maps to
  `OperationCanceledException` deterministically. The in-flight-cancel → wakeup →
  `OperationCanceledException` translation is wired (`RegisterCancellation`) but
  only deterministically exercisable once a wakeup-observing op exists.
- **D2 — the concurrency exception-type matrix is tested at the `ConsumerAccessGuard`
  component level (deterministic), not via a forced native op overlap.** Instant
  Mock ops make a genuine submit→callback overlap non-deterministic; the guard is a
  pure managed mirror, so its rejection types (async op → `KafkaException`; state
  read → `InvalidOperationException`) are fully proven as a component. The guard's
  wiring into `NativeConsumer` is exercised by the op / group-metadata tests
  (released between ops; a guarded `GroupId()` round-trips).
- **D3 — `Microsoft.Bcl.AsyncInterfaces` (8.0.0) added for the ns2.0 leg only.** It
  supplies `IAsyncDisposable` + the `ValueTask` async builder absent on the
  netstandard2.0 floor (built-in on net8.0+) — the enabling dependency for the
  primary `DisposeAsync`. A standard facade, conditioned exactly like `System.Memory`
  (ns2.0-only); no NuGet packaging of the binding itself (ffi §0.2 unchanged).
- **D4 — sync `Dispose` kept M2-shape (no drain) + a post-destroy Task-fault.**
  `Dispose` stays `close_with_timeout` → destroy (thread-safe closed flag added),
  NOT sync-over-async; the drain-first path is `DisposeAsync` (primary, ffi §B7).
  **Post-Critic (N=5) fix (Findings 1 + 3):** `close_with_timeout` is a *guarded* sync
  op, so while an async op genuinely holds the core guard the close is rejected
  (ConcurrentModification) and does **not** drain — the following `Consumer_destroy`
  then cancels the op's callback, which (before the fix) stranded the op `Task`
  (Finding 1). `Dispose` now, **after** destroy, faults any pending op's `Task`
  (`OperationCompletionSource.FaultTaskOnly` → `ObjectDisposedException`) so a
  fire-and-forget awaiter cannot strand — via idempotent primitives (`TrySetException`
  no-ops if completed), race-safe against a callback that fired before destroy, and NOT
  sync-over-async (it never waits on the op `Task`). Crucially it does **not** free the
  `GCHandle`: the completion callback is the **sole owner** of that free (Finding 3),
  aligning with the in-repo Python (`Py_DECREF` in the op trampoline; close drains then
  bare `_destroy`) and confluent-kafka-dotnet (`gch.Free()` in the delivery-report
  callback; `Dispose` drains via `callbackTask.Wait()` then destroys). Freeing it from
  `Dispose` was the case-B use-after-free — a completion job queued before destroy fires
  *after* it (the ABI drains queued dispatcher jobs without joining) and must recover a
  live handle. **Accepted residual (case A):** if destroy cancels the op before its
  callback is queued, that one op's `GCHandle` leaks — a rare, one-time, teardown-only
  leak in a misuse case (unawaited in-flight op + sync `Dispose`); the Python/CKD
  siblings accept the same residual, and `DisposeAsync` drains so it has no leak. So an
  op-in-flight sync `Dispose` no longer strands the `Task`; users wanting no leak use
  `DisposeAsync`.
- **N=5 deferred hardening — DONE.** The non-atomic `_disposed` bool is replaced by a
  thread-safe closed flag (`Interlocked`, `TryBeginClose`) + the §B5 access guard, so
  double / concurrent / mixed `Dispose`/`DisposeAsync` are safe. Per the deferred
  note, teardown is guarded the CKD way (thread-safe closed check + access guard) —
  NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle-param.

## Verification state (M2/P2 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + header present (run FIRST).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. Removing the
  now-unused `using System;` from the two SafeHandle files and `NativeLoadProbeTests`
  kept IDE0005 from failing the build.
- `dotnet test -f net10.0` — **20 passed, 0 failed** (19 M2/P1 carried + 1 new
  M2/P2 failure-path regression); ~250 ms.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P2)

- **SafeHandle-return over `new + SetHandle`** — the three owned-handle
  constructors return their `SafeHandle` subtype directly; the marshaller invokes
  the private parameterless ctor and sets the handle atomically. Classic
  `[DllImport]` feature (no `[LibraryImport]`), supported on the netstandard2.0
  floor incl. net462 — the TFM where the async-abort window actually exists.
- **`FromRaw` removed entirely** — the handle now arrives marshaller-wrapped; no
  call site needs a thin non-marshalling helper, so none was kept (PLAN §2).
- **Defensive `IsInvalid`-without-error guard throws `KafkaException`** — the ABI
  contract says a null `out_error` implies a non-null handle, so a `(null handle,
  null error)` return is a core contract violation (not a caller programmer error),
  surfaced on the operational `KafkaException` surface with a descriptive message.
  Can't-happen per the header; the guard exists so an IsInvalid handle is never
  stored (a later `Handle` read would hand back a null pointer).
- **Unchanged from M2/P1** — `ReleaseHandle` bodies, `NativeConsumer.Dispose`
  graceful close→destroy, the `put` loop, D6 (props stays a SafeHandle in-param),
  the `KafkaError` five decls, `KafkaException.FromHandle`.

## Verification state (M2/P1 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. CS1591 is enforced on the public `KafkaException`; no analyzer
  suppressions were needed (the Java-style standard exception constructors satisfy
  CA1032).
- `dotnet test -f net10.0` — **19 passed, 0 failed** (4 M1/P1 carried + 15 new:
  6 error/precondition, 5 lifecycle, 3 config-marshal, 1 D5 round-trip); ~266 ms
  total (broker-less close is near-instant, so the fail-fast timeout guard never
  trips).
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P1)

- **D2** — `SafeHandleZeroIsInvalid` base (`IsInvalid => handle == Zero`), not
  `SafeHandleZeroOrMinusOneIsInvalid` (−1 is not our contract; all `_destroy` are
  null-safe).
- **D3 (deviation from CLAUDE.md §4)** — synchronous `IDisposable.Dispose()` only
  this phase; `IAsyncDisposable.DisposeAsync()` deferred with the completion bridge
  (the only close primitive in scope is the synchronous
  `Consumer_close_with_timeout`; wiring `DisposeAsync` now would be
  sync-over-async or depend on the deferred bridge). Recorded in
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`.
- **D4** — the lifecycle wrapper (`NativeConsumer`) lives under `Internal/` (not
  `Internal/Interop/`): `unsafe`-free (safe `Utf8Marshal.Pin` + `SafeHandle`),
  keeping `unsafe` quarantined to `Internal/Interop/`.
- **D5 — CLOSED (verified empirically).** A configured non-ASCII `group.id`
  surfaces broker-free / pre-join (the core stubs `group_metadata()` from the
  configured id before join), so the UTF-8 config-value round-trip
  (`Consumer_group_metadata` → `group_id` → `PtrToString` == input) is kept, not
  deferred. Adds the three group-metadata DllImports + an owned Category-3 handle
  marshal-then-destroy in the test.
- **D6** — `props` passed to `KafkaConsumer_new` as the SafeHandle type (marshaller
  does DangerousAddRef/Release); disposed in a `finally` after the call (header:
  caller retains props ownership).
- **Dispose close-error handling** — `Dispose()` consumes the close error via
  `FromHandle` (freed exactly once) but does NOT rethrow (Dispose must not throw;
  surfacing close errors is the future `CloseAsync(TimeSpan)`'s job).
- **Decision reversal (post-M2/P2, PR #134 review) — `KafkaException` un-sealed.**
  `KafkaException` is now `public class` (not `public sealed class`), aligning with
  CLAUDE.md §3's sketch (which already shows `public class KafkaException`) + the
  flat-now/typed-later intent (§4 / ffi §A5) — reversing the M2/P1 PLAN's `sealed`
  choice, per user direction during the PR #134 review. Non-breaking (source +
  binary compatible). The archived M2/P1 PLAN + `COMMENTS.DONE.3` are left intact
  as the historical record; this reversal lives here in current STATUS only.

## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green)

- `cargo build --features ffi` — cdylib `target/debug/libconfluent_kafka.dylib`
  + generated header `target/include/confluent_kafka.h` produced (run FIRST,
  CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. `/unsafe+` on the
  library triggered **no** analyzer warnings (CA5392 is opt-in; SYSLIB1054 is
  Info-severity) — so **no suppressions were needed** (M1/P1 decision D5).
- `dotnet test -f net10.0` — **4 passed, 0 failed** (2 native-load probe + 1
  `Utf8Marshal` codec + the M0/P0 sentinel). The native loaded, the first
  `[DllImport]` round-tripped, and UTF-8 marshalled into native correctly.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking this phase):** only the .NET 10 runtime is installed
  locally (`dotnet --list-runtimes` shows only Microsoft.NETCore.App 10.0.x).
  The net8.0 test *run* needs the .NET 8 runtime and net462 needs Windows — both
  are **CI-only**. Both *build* legs succeed; only the *runs* are deferred.
- **Handled by the two-stage pipeline (CLAUDE.md §7.1):** the native-copy target
  uses `<Content>` (not `<None>`) so the cdylib flows transitively to the
  referencing TEST project's output dir, where the probe resolves it via default
  `[DllImport]` probing.

## Decisions in force (M1/P1)

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target landed; per-OS
  filename via MSBuild, profile from `$(Configuration)`, repo root 4 levels up,
  `<Content>` transitive, never a hardcoded path/filename (ffi §0.2).
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free; `unsafe` confined to `Internal/Interop/`.
- **D3** — classic `[DllImport]`, uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8Marshal.Pin` = disposable call-scoped pin (`using`); `Utf8Marshal.PtrToString`
  = NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions contingent; none fired, none added.

Deviations recorded during execution (see the archived review record under
`design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`):
- XML-comment MSB4025 fix in the csproj comment (literal `--` illegal in XML
  comments — reworded).
- `.gitkeep` deletion grouped into the csproj commit (commit-grouping only).

## Watch-item for future phases — RESOLVED post-close (`9ae31fa`)

- `PinnedUtf8String` was a `readonly struct` holding a `GCHandle`; `Dispose()`
  freed a compiler defensive copy. Correct under M1/P1's single-`using`
  ownership, but a later phase that **stores or copies** a `PinnedUtf8String`
  would have hit the false "idempotent for a single owner" claim (double-`Dispose`
  / disposed by-value copy would double-free the runtime handle). Recorded in
  `.claude/agent-memory/dotnet-critic/interop_review_patterns.md`.
- **Resolved in `9ae31fa`:** `PinnedUtf8String` is now a `sealed class`, so
  `Dispose()` mutates the real `GCHandle` field (no defensive copy) — the unpin
  is genuinely idempotent and the value-copy double-free hazard is gone. Verified:
  `dotnet build` 0/0 all TFMs, `dotnet test -f net10.0` 4/4, format clean. The
  archived `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md` is left
  unchanged as the phase-close snapshot.

## Review outcome (M3/P1)

Critic (N=5) review of `285b04c`/`d6f3022`/`3d0245f`/`1243350`/`259d098`: all four
DoD gates independently re-verified green; deviations D1–D4 verified sound; the core
bridge (free-once, GCHandle keep-alive, `RunContinuationsAsynchronously`, no-throw
boundary, `DisposeAsync` drain, marshalling, error classification, scope) had **no
defects**. Two findings, both on the sync-teardown / handle-lifetime edges (not the
async bridge):
- **Finding 1 [MEDIUM] — FIXED (its GCHandle-free part corrected by Finding 3).** Sync
  `Dispose` with an async op in flight stranded the op `Task` (and, originally, leaked
  the per-op `GCHandle`): the guarded `close_with_timeout` is rejected (no drain) while
  the op holds the core guard, then `Consumer_destroy` cancels the callback. Fixed by a
  post-destroy Task-fault (`OperationCompletionSource.FaultTaskOnly`; see D4); the
  masking `Dispose_WithOpInFlight` test now observes the op `Task` to a terminal state
  (+ a churn/GC variant). Not sync-over-async, race-safe.
- **Finding 3 [LOW/latent, memory-safety] — RESOLVED (re-review of the Finding-1 fixup).**
  The Finding-1 fix originally freed the `GCHandle` from `Dispose` (`FaultAndReclaim`);
  that is a case-B use-after-free — a completion job queued before `Consumer_destroy`
  fires *after* it (the ABI drains queued dispatcher jobs without joining) and
  dereferences the freed/recycled handle via `GCHandle.FromIntPtr(userData).Target`.
  Resolved by making the completion callback the **sole owner** of the `GCHandle` free
  (`FaultTaskOnly` faults the `Task` only), matching the in-repo Python +
  confluent-kafka-dotnet callback-frees / teardown-drains-not-reclaims pattern. Accepted
  case-A residual: a one-time teardown-only leak if destroy cancels the op before its
  callback is queued (both siblings accept the same); `DisposeAsync` drains and has no
  leak. New OCS component tests drive the straggler callback through the real
  `GCHandle.FromIntPtr` recovery path (proving no UAF). See COMMENTS.DONE.5.
- **Finding 2 [LOW/latent] — ACCEPTED, deferred to N=6 (documented, no code change).**
  Cross-thread `Wakeup()`/`GroupId()` TOCTOU vs teardown; plan-consistent
  (per-call AddRef deliberately declined) and not reachable while internal-only. Carried
  as a deferred-hardening item (see "Deferred hardening (N=6 …)" above).

## Review outcome (M2/P2)

Critic (N=4) review of commits `3359b70`, `6aa2f92` (via `git log`/`git show`):
**0 genuine findings** — clean. Verified the hardening contract exactly: the three
owned-handle constructors return their `SafeHandle` subtype directly (atomic
marshaller create-and-set — the `new + SetHandle` / `FromRaw` two-step is gone,
repo-wide sweep confirms no stale raw-`IntPtr`-return call site); both SafeHandle
subtypes retain the private parameterless ctor (no `MissingMethodException`);
`ownsHandle:true` + `IsInvalid => Zero` + both `ReleaseHandle` bodies unchanged;
the fallible path disposes the IsInvalid handle (ReleaseHandle skipped — no
spurious `Consumer_destroy`) then throws `FromHandle(outError)`; D6 + the graceful
`Dispose` preserved; the new failure-path regression is sound (drives the real
null-native-return 50× and asserts IsInvalid + non-null `out_error` + the
`FromHandle` round-trip). The Critic **independently re-verified** the DoD on this
machine (read-only): `dotnet build` 0/0 across all TFMs, `dotnet test -f net10.0`
20 passed/0 failed. No fix cycle required (one Actor pass → one Critic pass →
close).

## Review outcome (M2/P1)

Critic (N=3) review of commits `558de6a`..`c45f914` (via `git log`/`git show`,
not `cargo xtask await-commit`): **0 genuine findings** — clean. Verified the
full boundary: every `[DllImport]` matches the header (Cdecl, `int64_t`→`long`,
`[MarshalAs(I1)]` on the bool getters, hand-marshalled UTF-8, `out IntPtr` for
`KafkaError_t**`); `FromHandle` (null=success, message-before-free, copy-out,
`_destroy` in `finally`, freed exactly once); `SafeHandle` lifecycle
(`IsInvalid => Zero`, graceful `close_with_timeout` → release, props as the
SafeHandle D6, D5 group-metadata handle read-then-destroy once); preconditions →
`Argument*`/`ObjectDisposedException` (never `KafkaException`); `KafkaException`
the only new public type; D2/D3/D5/D6 recorded; no persona/agent-memory files
committed. No fix cycle required (one Actor pass → one Critic pass → close, as
M1/P1).

## Review outcome (M1/P1)

Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`: **0 genuine
findings** — clean, independently build/test/format-verified. No fix cycle
required (one Actor pass → one Critic pass → close).

## Governance pointers

- Approved plans: `design/history/M3/P1-completion-bridge/PLAN.md` (current),
  `design/history/M2/P2-safehandle-return-hardening/PLAN.md`,
  `design/history/M2/P1-error-model-safehandle/PLAN.md`,
  `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review records:
  `design/history/M2/P2-safehandle-return-hardening/COMMENTS.DONE.4.md`,
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`,
  `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`. (M3/P1: no Critic
  review yet — the archived record will be `COMMENTS.DONE.5.md`.)
- Personas: `dotnet-actor` (Actor N=5), `dotnet-critic` (Critic N=5). NEVER the
  Rust `actor-executor` / `kafka-critic`. The working `COMMENTS.5.md` is
  gitignored; the archived record is `COMMENTS.DONE.5.md`.

## Next up (not started)

The **first public client operations + the receive path** (still Mode A). From the
completion bridge now in place:
- The public `IConsumer` / `KafkaConsumer` / `MockConsumer` types (the internal
  `NativeConsumer` lifecycle + bridge fold into the public client), promoting the
  proof ops to the real API surface (`SubscribeAsync` / `SeekAsync` public, then
  `CommitAsync` / `position` on the same void/scalar bridges).
- `PollAsync` and the entire receive path: Category 3/4 handles (poll-batch
  borrow-roots / views), length-delimited `out_len` receive strings (§B3), the
  copy-out `ConsumerRecord(s)` surface (§B4 / CLAUDE.md §6.4), and the per-record
  allocation-budget test.
- The wakeup-fault + in-flight-cancellation slices that need a wakeup-observing op
  (poll) to be deterministically testable (M3/P1 D1).
Sequence and exact scope to be set in the next PLAN (Manager, with approval).

### Deferred hardening (N=5) — teardown thread-safety: **DONE (M3/P1, 2026-07-27)**

Delivered this phase (see "Decisions in force (M3/P1)"): the non-atomic `_disposed`
bool is replaced by a thread-safe closed flag (`Interlocked` + `TryBeginClose`) plus
the §B5 access guard, so double / concurrent / mixed `Dispose`/`DisposeAsync` are
safe and use-after-dispose throws. Guarded the CKD way (thread-safe closed check +
access guard) — NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle
param, matching the deferred note's guidance. `DisposeAsync` is the primary
drain-first path (drain in-flight → `close_async` → destroy); `Dispose` stays the
M2-shape blocking fallback — **now with a post-destroy Task-fault** (Critic N=5
Findings 1 + 3, see D4) so an op-in-flight sync `Dispose` faults the op `Task` (never
freeing the `GCHandle` — the completion callback is the sole owner) instead of
stranding, with an accepted one-time case-A teardown residual (`DisposeAsync` has no
leak). Ops remain single-threaded-with-rejection; only `wakeup()` is cross-thread.

### Deferred hardening (N=6 — public client cross-thread wakeup): `Wakeup()`/`GroupId()` TOCTOU vs teardown

**Hazard (Critic N=5 Finding 2, LOW/latent — accepted, not fixed this phase).**
`Wakeup()` reads the thread-safe closed flag then dereferences
`_handle.DangerousGetHandle()`; `GroupId()` has the same `ThrowIfClosed` →
`DangerousGetHandle` shape. The closed-flag read and the handle deref are **not
atomic**: a concurrent `Dispose`/`DisposeAsync` on another thread can run
`TryBeginClose` → close → `_handle.Dispose()` (`Consumer_destroy`) in between, after
which `DangerousGetHandle()` returns the freed pointer and the native call
dereferences destroyed native memory (use-after-free). The thread-safe closed flag
makes *double/concurrent Dispose* safe, but it does **not** make a concurrent
*handle user* vs teardown safe — that is what `SafeHandle.DangerousAddRef/Release`
exists for.

**Why deferred (not a blocking defect now).** Plan-consistent: the PLAN (§Teardown)
deliberately declined per-call `SafeHandle` AddRef ("guarded the CKD way — thread-safe
closed check + access guard; NO per-call `SafeHandle` AddRef"), matching
confluent-kafka-dotnet's own non-AddRef hot-path idiom. `Wakeup()`/`GroupId()` are
**internal-only** this phase with no cross-thread wakeup-vs-dispose caller, so the race
is **not reachable** now. The canonical `wakeup()` usage (thread A blocked in `poll`,
thread B wakes it, thread A then disposes after `poll` returns) does not race wakeup
against dispose.

**Resolution to consider (when the public client wires cross-thread `Wakeup()`).**
Either (a) take a per-call `SafeHandle.DangerousAddRef`/`DangerousRelease` around the
native call in `Wakeup()`/`GroupId()` (revisiting the PLAN's declined-AddRef decision
for exactly the cross-thread-callable methods), or (b) explicitly document the
precondition that `Wakeup()`/`GroupId()` must not be called concurrently with teardown.
Decide when the public `IConsumer`/`KafkaConsumer` surface makes `Wakeup()` genuinely
cross-thread (the same trigger as the N=5 item).

### Deferred hardening (N=6 — concurrent public client + teardown): op-submit vs concurrent teardown window

**Hazard (post-merge review of PR #135, LOW/latent — accepted, not fixed this phase;
same "concurrent public client + teardown thread-safety" bucket as the `Wakeup()` /
`GroupId()` item above).** `SubmitVoidOperation` calls the native `*_async` op —
which spawns the op and takes the *core* access guard — **before** it publishes
`_inFlightContext` / `_inFlightOperation` (in `NativeConsumer.SubmitVoidOperation`
the `submit(...)` call precedes the two `Volatile.Write`s). A concurrent
`Dispose` / `DisposeAsync` on another thread that runs entirely inside that window
reads a **null** `_inFlightContext` / `_inFlightOperation`, so it can neither drain
(`DisposeAsync`) nor fault (`Dispose` → `FaultTaskOnly`) the op; the following
`Consumer_destroy` then **cancels** the in-flight op, whose completion callback never
fires — so the op `Task` **strands** and its `GCHandle` **leaks**. Unlike the
`Wakeup()` / `GroupId()` TOCTOU (a use-after-free), this is a strand + leak, and it is
a gap in an *intended-safe* path: "teardown while an op is in flight" is designed to
be safe (drain in `DisposeAsync`, `FaultTaskOnly` in `Dispose`) and IS safe once the
op is published — the hole is only the narrow pre-publish window. The existing
`SubmitVoidOperation` comment orders `_inFlightContext` before the `Task`, but does
not cover "teardown runs before *either* write while the op is already in flight from
the native submit."

**Why deferred (not a blocking defect now).** Internal-only this phase — no public
surface lets a caller submit an op on one thread while disposing on another, so the
race is **not reachable** now; the window is microseconds; single-threaded usage is
unaffected. Same family as the `Wakeup()` / `GroupId()` item, so it lands with the
same concurrent-public-client trigger.

**Resolution to consider (when the public client makes submit + teardown genuinely
concurrent).** Make op-submit and teardown **mutually exclusive** — e.g. a teardown
lock around (submit + publish) vs. the `Dispose` / `DisposeAsync` body, or have
teardown coordinate with the access guard — so teardown either observes the in-flight
op or is serialized after its submit. A reorder alone is NOT sufficient: publishing the
context *before* `submit(...)` would instead let a concurrent `destroy` race the native
call (`DangerousGetHandle()` on a freed handle → use-after-free), which is worse.
