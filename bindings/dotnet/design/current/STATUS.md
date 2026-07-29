# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

Newest first.

- **Milestone 1 / Phase 1 — "Interop scaffolding + native-load probe": DONE
  (2026-07-20).** The client-agnostic interop FOUNDATION: the `NativeMethods` P/Invoke
  class (8 shared-foundation declarations), the `Utf8Marshal` marshalling helpers, the
  native-copy MSBuild target (un-defers M0/P0 decision D2), and a consumer-namespaced
  native-load probe. Mode A (C ABI already landed — no Rust authoring). NO public
  managed API, NO `SafeHandle`, NO completion bridge, NO Kafka logic yet — all
  deferred to later phases by scope.
- **Milestone 0 / Phase 1 — "Rename binding identity": DONE (2026-07-29).**
  The binding identity is `Confluent.Kafka` (was
  `Confluent.Kafka.ShareConsumer`): solution, strong-name key, both project
  directories and their csprojs, `<RootNamespace>`/`<AssemblyName>`/`<Product>`,
  `<AssemblyOriginatorKeyFile>`, the `InternalsVisibleTo` grant, the
  `ProjectReference`, and the test file-scoped namespace.

  **Compliance work, not preference** — CLAUDE.md §2's file map and §4's
  *Namespace / package id* row already read `Confluent.Kafka`, so the M0/P0
  artifacts were the drift, not the rulebook. Adds **no capability**; same
  milestone because it only corrects M0/P0's output. The old name described a
  KIP-932 feature that `.claude/rules/consumer-threading.md` §20 puts explicitly
  out of scope, so it was actively misleading.

  The strong-name key was **not** regenerated — the `.snk` is byte-identical and
  the public-key token stays `a6a493010a30d243`, so the assembly identity is
  unchanged (only `InternalsVisibleTo Include=` moved; its `Key=` blob is
  verbatim).

  ⚠ **CLAUDE.md §4's package-id pre-publish gate remains OPEN.** The binding now
  shares the `Confluent.Kafka` id with confluent-kafka-dotnet, meaning a project
  can hold ckd 2.x **or** this client, never both (so ckd's Schema-Registry /
  OAuthBearer packages can't be mixed in). This phase makes the *name* collide,
  so the gate is now held shut **structurally** rather than by prose:
  `Microsoft.NET.Sdk` defaults `IsPackable` to **true** for a library and
  `PackageId` to **`$(AssemblyName)`**, so writing neither is the *packable*
  state — a bare `dotnet pack` would emit a package id byte-equal to ckd's. The
  library csproj therefore sets `<IsPackable>false</IsPackable>` explicitly (and
  nothing else packaging-related). Check the **evaluated** property, never the
  absence of an element: `dotnet msbuild <library>.csproj -getProperty:IsPackable`
  → `false`. "No `Pack*` metadata" was never the right
  test either — `Directory.Build.props`'s `<Authors>`/`<Company>`/`<Product>`/
  `<Copyright>` flow into a nuspec on their own. There is also no publish
  automation in the repo (no `.github/workflows`, no `dotnet pack` / `nuget push`
  target anywhere), so nothing can trip the gate today. The decision — own SR
  integration, or diverge the id — is still owed **before any publish**, and
  un-defers by flipping that one line.
- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).** Pure
  structural skeleton (see `design/history/M0/P0-scaffolding/`).

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.sln                       ← classic .sln, both projects + a "build" solution folder
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              strong-name signing (shared .snk, both projects)
│                                              — NO AllowUnsafeBlocks here (library-only, M1/P1 D2)
├─ .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka/
│     ├─ Confluent.Kafka.csproj                ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on the ns2.0
│     │                                           leg only, GenerateDocumentationFile,
│     │                                           IsPackable=false (M0/P1 — holds §4's id gate shut),
│     │                                           InternalsVisibleTo → UnitTests (public key);
│     │                                           M1/P1: + <AllowUnsafeBlocks> (library only),
│     │                                           + native-copy MSBuild target (per-OS filename via
│     │                                           IsOSPlatform, profile from $(Configuration),
│     │                                           repo root 4 levels up, <Content> transitive,
│     │                                           + <Error> guard if native absent)
│     └─ Internal/
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                 ← internal static class NativeMethods: 8 classic
│           │                                      [DllImport("confluent_kafka", Cdecl)] decls,
│           │                                      full ABI symbol as EntryPoint, I1 on both bools
│           └─ Utf8Marshal.cs                   ← internal static class Utf8Marshal: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.UnitTests/               ← TFMs net8.0;net10.0, unsafe-free
      ├─ Confluent.Kafka.UnitTests.csproj      ← xUnit + Microsoft.NET.Test.Sdk, ProjectReference
      ├─ TfmSentinelTests.cs                   ← M0/P0 TFM-sentinel smoke test (root: harness-level)
      └─ Interop/                              ← mirrors the library interop area (public test
         │                                         classes; "Interop" not "Internal/Interop" — the
         │                                         Internal visibility marker is library-only, §2)
         ├─ NativeLoadProbeTests.cs            ← M1/P1: 2 tests, both invoke a native [DllImport]
         │                                         (smoke new/put/destroy; non-ASCII put no-crash)
         └─ Utf8MarshalTests.cs                ← M1/P1: managed Utf8Marshal codec round-trip
                                                   + PtrToString(Zero)==null (no native call)
```

## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green; re-verified after the M0/P1 rename merge)

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

Additional gates specific to M0/P1 (the rename), all green:

- The pre-rename `src/` and `tests/` project directories are fully gone from
  disk — stale `obj/` and `bin/` were destroyed *before* the moves, since
  `git mv` relocates only tracked files and untracked build output would
  otherwise have kept the old directories alive with a stale assembly and a
  cached `project.assets.json` naming the old `AssemblyName`.
- No tracked path carries the old identity, and no build or code file mentions
  it. The exact invariant is **scoped to build and code**, and both halves are
  checkable:
  `grep -rIn "ShareConsumer" . --exclude-dir=design --exclude='COMMENTS*.md'` →
  empty, and `git ls-files | grep -i shareconsumer` → empty. It is scoped rather
  than absolute because **four** documentation surfaces under `design/` name the
  old identity deliberately — the archived M0/P0 `PLAN.md` (6 occurrences,
  including its dated supersession note), the archived M0/P0
  `COMMENTS.DONE.1.md` (2), this phase's own
  `design/history/M0/P1-rename-identity/PLAN.md` (18 — a rename plan must name
  what it renames), and this file itself — its transition narrative above and
  its *Governance pointers* section below. That section links the first three;
  the fourth is this file. The M0/P1 review record `COMMENTS.DONE.1.md` quotes
  them too, hence the second exclusion.
- `.snk` byte-identical across the move (SHA-256 `d33f5c98…8eb197`); the
  `InternalsVisibleTo` `Key=` blob still equals `sn -tp` on the key file, and
  `Include=` matches the test project's `<AssemblyName>`.
- All 7 path changes recorded by git as **renames**, not delete+create.
- `Confluent.Kafka.sln` — a surgical 2-line edit (the two project entries): the
  7 GUIDs, `ProjectConfigurationPlatforms`, `NestedProjects`, the `build`
  folder's `SolutionItems`, and the UTF-8 BOM are all unchanged. The solution
  was **not** regenerated (SDK 10 would emit `.slnx`; classic `.sln` is a
  standing M0/P0 deviation).

## Decisions in force (M1/P1)

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target landed; per-OS
  filename via MSBuild, profile from `$(Configuration)`, repo root 4 levels up,
  `<Content>` transitive, never a hardcoded path/filename (ffi §0.2 — the
  **pre-publish** half of the two-phase delivery model).
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free; `unsafe` confined to `Internal/Interop/`.
- **D3** — classic `[DllImport]`, uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8Marshal.Pin` = disposable call-scoped pin (`using`); `Utf8Marshal.PtrToString`
  = NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions contingent; none fired, none added.

## Decisions in force (M0/P0)

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
- **D2** — Native-copy MSBuild target + Rust build deferred to the first
  implementation phase — **un-deferred in M1/P1 D1 above**.
- **D3** — Empty folders via `.gitkeep`, no placeholder types.
- **D4** — Test framework = xUnit.

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

## Post-plan additions (M0/P0, interactive — 2026-07-20)

Made after the Critic (N=1) close, in an interactive review pass — these are
NOT part of the approved plan and were NOT put through a separate Critic cycle:
- **Solution items** — a `build` solution folder embedding
  `Directory.Build.props` + `.editorconfig` (ckd parity).
- **`GenerateDocumentationFile=true`** — added to the *library* csproj (NOT the
  shared props, which would make CS1591 break the test build under
  `TreatWarningsAsErrors`). Under TWAE this forces every public API member to
  carry an XML doc — faithful to CLAUDE.md §4 (javadoc → C# XML docs). This
  reverses the initial "dropped" deviation, better-scoped.
- **Strong-naming** — one shared key `Confluent.Kafka.snk` (the file was renamed
  in M0/P1; the key itself is byte-identical and was never regenerated),
  `SignAssembly` wired in `Directory.Build.props` (both projects), and the
  `InternalsVisibleTo` public key on the library csproj. Decided early on
  purpose: adding a strong name after the first published package is a
  binary-breaking change. The `.snk` is committed (identity, not a secret;
  publisher trust is NuGet/Authenticode signing at publish).
- **Review record renamed** — `COMMENTS.1.closed.md` → `COMMENTS.DONE.1.md`
  (the old name was swallowed by the repo-root `COMMENTS\.[0-9]*\.md` gitignore;
  the `DONE` name is tracked and matches the documented mechanics).

## Review outcome

- **M1/P1** — Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`:
  **0 genuine findings** — clean, independently build/test/format-verified. No fix
  cycle required (one Actor pass → one Critic pass → close).
- **M0/P1** — Critic (N=1): 3 review cycles, 4 items, **all closed**
  (`COMMENTS.1.md` empty). The substantive one was the `IsPackable` default
  inversion recorded above; items 2–4 were STATUS.md documentation-accuracy
  defects.
- **M0/P0** — Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`:
  **0 genuine findings** — clean skeleton, verified by an independent
  build/test/format run.

## Governance pointers

Current phase (**M1/P1**):

- Approved plan: `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review record: `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`.
- Personas: `dotnet-actor` (Actor N=2), `dotnet-critic` (Critic N=2). NEVER the
  Rust `actor-executor` / `kafka-critic`.
- The next requirement takes **N=3** with a fresh (gitignored) working
  `COMMENTS.3.md`; `COMMENTS.2.md` ends this phase with no open items.

Previous phase (**M0/P1** — rename identity):

- Approved plan: `design/history/M0/P1-rename-identity/PLAN.md`.
- Closed review record: `design/history/M0/P1-rename-identity/COMMENTS.DONE.1.md`.

Previous phase (**M0/P0** — scaffolding):

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`. Carries a dated
  supersession note: the phase shipped under the
  `Confluent.Kafka.ShareConsumer` identity, and its body is preserved verbatim
  as the record of what was approved and verified at the time.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.DONE.1.md` —
  left **verbatim** on purpose. It records the Critic's *verified* finding about
  the then-current `InternalsVisibleTo` name, so editing it would make a
  historical verification claim describe an assembly name that did not exist
  when the check ran.

## Next up (not started)

The first **public managed API + `SafeHandle`** phase (still Mode A — the
producer/consumer C ABI has landed). Candidates, from the interop foundation
now in place:
- The consumer or producer client lifecycle: a `SafeHandle` subclass over
  `Consumer_t` / `Producer_t` (`new` → `close`/`destroy`), wired to
  `IAsyncDisposable`/`IDisposable` per ffi §A2/§B2.
- The flat `KafkaException` + `KafkaException.FromHandle` (the 5 `KafkaError`
  declarations already staged in `NativeMethods` become live callers — their
  `EntryPoint`s get their first runtime validation here) per ffi §A5/§B5.
- Config marshalling (`ConsumerProperties`/`ProducerProperties_put` from an
  `IReadOnlyDictionary<string,string>`) per CLAUDE.md §4.
Sequence and exact scope to be set in the next PLAN (Manager, with approval).
