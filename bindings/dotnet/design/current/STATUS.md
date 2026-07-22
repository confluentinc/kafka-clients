# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

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
│        │                                         IsInvalid handle on error; defensive IsInvalid guard)
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                        ← internal static class NativeMethods: M1/P1 (8
│           │                                      shared decls) + M2/P1 consumer lifecycle
│           │                                      (KafkaConsumer_new/MockConsumer_new/close/
│           │                                      close_with_timeout/destroy) + group-metadata trio.
│           │                                      M2/P2: the three constructors return their SafeHandle
│           │                                      subtype directly (marshaller create-and-set)
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
      ├─ TestTimeout.cs                         ← M2/P1: fail-fast deadline helper (hang -> test failure)
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
         └─ Utf8RoundTripTests.cs               ← M2/P1 (D5 CLOSED): non-ASCII group.id -> group_metadata
                                                   -> group_id readback == input (broker-free)
```

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

- Approved plans: `design/history/M2/P2-safehandle-return-hardening/PLAN.md`
  (current), `design/history/M2/P1-error-model-safehandle/PLAN.md`,
  `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review records:
  `design/history/M2/P2-safehandle-return-hardening/COMMENTS.DONE.4.md` (current),
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`,
  `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`.
- Personas: `dotnet-actor` (Actor N=4), `dotnet-critic` (Critic N=4). NEVER the
  Rust `actor-executor` / `kafka-critic`. The working `COMMENTS.4.md` is
  gitignored; the archived record is `COMMENTS.DONE.4.md`.

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

### Deferred hardening (N=5 — concurrent public client / DisposeAsync): teardown thread-safety

Carry this as an explicit in-scope item when the N=5 PLAN is drafted.

- The `NativeConsumer.Dispose` close path calls `Consumer_close_with_timeout` with
  `_handle.DangerousGetHandle()` (raw `IntPtr`, no AddRef). This is safe under the
  CURRENT single-threaded `NativeConsumer` lifecycle: `this` roots `_handle` for
  the call (no finalizer race), and there is no concurrent managed `Dispose`. It
  also MATCHES CKD's idiom (raw `IntPtr` for ~all operations incl.
  `consumer_close`/`destroy`, ~71:1 vs SafeHandle params; no per-op AddRef) — it is
  NOT a deviation or defect.
- The current `_disposed` guard is a non-atomic bool — adequate for the
  single-threaded internal wrapper; it doubles as a use-after-free guard against
  double-`Dispose` (a second close would pass a freed pointer).
- When the concurrent public client + `DisposeAsync` land (N=5), guard teardown the
  CKD way: a THREAD-SAFE closed/disposed state check (e.g. the `SafeHandle`'s own
  `IsClosed`, or an atomic flag) + the §B5 one-op-in-flight access guard. Do NOT
  convert close/destroy to `SafeHandle` P/Invoke params or add manual AddRef — that
  diverges from CKD and adds per-call cost on the hot ops (poll/commit).
- Operations themselves stay single-threaded-with-rejection (Java parity; the
  core's access guard rejects concurrent ops per §B5); only `wakeup()` is
  cross-thread. This note is about safe TEARDOWN under misuse, not about supporting
  concurrent operations.
