# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

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
│        │                                         preconditions -> ArgumentNullException/ArgumentException
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                        ← internal static class NativeMethods: M1/P1 (8
│           │                                      shared decls) + M2/P1 consumer lifecycle
│           │                                      (KafkaConsumer_new/MockConsumer_new/close/
│           │                                      close_with_timeout/destroy) + group-metadata trio
│           ├─ SafeHandleZeroIsInvalid.cs             ← M2/P1: shared base, IsInvalid => handle==Zero (D2)
│           ├─ SafeConsumerPropertiesHandle.cs        ← M2/P1: config handle (-> ConsumerProperties_destroy)
│           ├─ SafeConsumerHandle.cs                  ← M2/P1: client handle (-> Consumer_destroy, bare
│           │                                            last-resort; graceful close is in NativeConsumer)
│           └─ Utf8Marshal.cs                          ← internal static class Utf8Marshal: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/  ← TFMs net8.0;net10.0, unsafe-free
      ├─ TfmSentinelTests.cs                    ← M0/P0 TFM-sentinel smoke test (root: harness-level)
      ├─ KafkaExceptionTests.cs                 ← M2/P1: public-type test (root): classic -> Code 35
      │                                            + I1 both-false + msg; café msg echo; FromHandle(Zero)
      ├─ TestTimeout.cs                         ← M2/P1: fail-fast deadline helper (hang -> test failure)
      └─ Interop/                               ← mirrors the library interop area (public test
         │                                         classes; "Interop" not "Internal/Interop" — the
         │                                         Internal visibility marker is library-only, §2)
         ├─ NativeLoadProbeTests.cs             ← M1/P1: 2 tests, both invoke a native [DllImport]
         │                                         (smoke new/put/destroy; non-ASCII put no-crash)
         ├─ Utf8MarshalTests.cs                 ← M1/P1: managed Utf8Marshal codec round-trip
         │                                           + PtrToString(Zero)==null (no native call)
         ├─ SafeConsumerHandleTests.cs          ← M2/P1: lifecycle (mock + real), double-Dispose,
         │                                           use-after-Dispose, create/dispose many
         ├─ ConsumerConfigMarshalTests.cs       ← M2/P1: config success + preconditions (null dict /
         │                                           null value / post-Dispose)
         └─ Utf8RoundTripTests.cs               ← M2/P1 (D5 CLOSED): non-ASCII group.id -> group_metadata
                                                   -> group_id readback == input (broker-free)
```

## Verification state (M2/P1 DoD — Actor, all green)

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

## Review outcome (M1/P1)

Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`: **0 genuine
findings** — clean, independently build/test/format-verified. No fix cycle
required (one Actor pass → one Critic pass → close).

## Governance pointers

- Approved plans: `design/history/M2/P1-error-model-safehandle/PLAN.md` (current),
  `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review records: `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`
  (current), `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`.
- Personas: `dotnet-actor` (Actor N=3), `dotnet-critic` (Critic N=3). NEVER the
  Rust `actor-executor` / `kafka-critic`. The working `COMMENTS.3.md` is
  gitignored; the archived record is `COMMENTS.DONE.3.md`.

## Next up (not started)

The **completion bridge + first public client operations** (still Mode A). From
the lifecycle foundation now in place:
- The consumer push completion bridge (callback → `TaskCompletionSource` with
  `RunContinuationsAsynchronously`, ffi §B6/§B7), enabling `PollAsync` /
  `CommitAsync` / `position` and the public `IConsumer` / `KafkaConsumer` /
  `MockConsumer` types (the lifecycle wrapper folds into the public client).
- `IAsyncDisposable.DisposeAsync()` (D3-deferred) lands with the bridge
  (`Consumer_close_async`).
- Category 3/4 receive-path handles (poll-batch borrow-roots / views) and the
  copy-out `ConsumerRecord(s)` surface (CLAUDE.md §6.4).
Sequence and exact scope to be set in the next PLAN (Manager, with approval).
