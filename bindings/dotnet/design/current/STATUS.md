# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

- **Milestone 1 / Phase 1 — "Interop scaffolding + native-load probe": DONE
  (2026-07-20).** The client-agnostic interop FOUNDATION: the `Native` P/Invoke
  class (8 shared-foundation declarations), the `Utf8` marshalling helpers, the
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
│     └─ Internal/
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ Native.cs                        ← internal static class Native: 8 classic
│           │                                      [DllImport("confluent_kafka", Cdecl)] decls,
│           │                                      full ABI symbol as EntryPoint, I1 on both bools
│           └─ Utf8.cs                          ← internal static class Utf8: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/  ← TFMs net8.0;net10.0, unsafe-free
      ├─ TfmSentinelTests.cs                    ← M0/P0 TFM-sentinel smoke test
      └─ NativeLoadProbeTests.cs                ← M1/P1: 3 probe tests (smoke; non-ASCII put;
                                                   Utf8 round-trip + PtrToString(Zero)==null)
```

## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green)

- `cargo build --features ffi` — cdylib `target/debug/libconfluent_kafka.dylib`
  + generated header `target/include/confluent_kafka.h` produced (run FIRST,
  CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. `/unsafe+` on the
  library triggered **no** analyzer warnings (CA5392 is opt-in; SYSLIB1054 is
  Info-severity) — so **no suppressions were needed** (M1/P1 decision D5).
- `dotnet test -f net10.0` — **4 passed, 0 failed** (3 probe + the M0/P0
  sentinel). The native loaded, the first `[DllImport]` round-tripped, and UTF-8
  marshalled into native correctly.
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
- **D4** — `Utf8.Pin` = disposable call-scoped pin (`using`); `Utf8.PtrToString`
  = NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions contingent; none fired, none added.

Deviations recorded during execution (see the archived review record under
`design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`):
- XML-comment MSB4025 fix in the csproj comment (literal `--` illegal in XML
  comments — reworded).
- `.gitkeep` deletion grouped into the csproj commit (commit-grouping only).

## Watch-item for future phases (NOT a current finding)

- `PinnedUtf8String` is a `readonly struct` holding a `GCHandle`; `Dispose()`
  frees a compiler defensive copy. Correct under M1/P1's single-`using`
  ownership, but a later phase that **stores or copies** a `PinnedUtf8String`
  must revisit the "idempotent for a single owner" claim (double-`Dispose` /
  disposed by-value copy would not be idempotent). Recorded in
  `.claude/agent-memory/dotnet-critic/interop_review_patterns.md`.

## Review outcome (M1/P1)

Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`: **0 genuine
findings** — clean, independently build/test/format-verified. No fix cycle
required (one Actor pass → one Critic pass → close).

## Governance pointers

- Approved plan: `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review record: `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`.
- Personas: `dotnet-actor` (Actor N=2), `dotnet-critic` (Critic N=2). NEVER the
  Rust `actor-executor` / `kafka-critic`.
- The next requirement takes **N=3** with a fresh (gitignored) working
  `COMMENTS.3.md`; `COMMENTS.2.md` ends this phase with no open items.

## Next up (not started)

The first **public managed API + `SafeHandle`** phase (still Mode A — the
producer/consumer C ABI has landed). Candidates, from the interop foundation
now in place:
- The consumer or producer client lifecycle: a `SafeHandle` subclass over
  `Consumer_t` / `Producer_t` (`new` → `close`/`destroy`), wired to
  `IAsyncDisposable`/`IDisposable` per ffi §A2/§B2.
- The flat `KafkaException` + `KafkaException.FromHandle` (the 5 `KafkaError`
  declarations already staged in `Native` become live callers — their
  `EntryPoint`s get their first runtime validation here) per ffi §A5/§B5.
- Config marshalling (`ConsumerProperties`/`ProducerProperties_put` from an
  `IReadOnlyDictionary<string,string>`) per CLAUDE.md §4.
Sequence and exact scope to be set in the next PLAN (Manager, with approval).
