# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).**
  Pure structural skeleton stood up. No P/Invoke, no `NativeMethods`, no
  `SafeHandle`, no managed API, no Kafka logic yet — all deferred to later
  phases by scope.
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
  OAuthBearer packages can't be mixed in). This phase makes the *name* collide
  but publishes nothing: there is no `PackageId`, no `Pack*` metadata, and the
  library sets no `IsPackable`. The decision — own SR integration, or diverge the
  id — is still owed **before any publish**.

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.sln                       ← classic .sln, both projects + a "build" solution folder
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              common metadata (Confluent Inc.), strong-name signing (shared .snk, both projects)
├─ .editorconfig                             ← dotnet/runtime style (_camelCase/s_, PascalCase,
│                                              Allman, System.* usings first, CA1715 I-prefix)
├─ .gitignore                                ← bin/ obj/ .DS_Store *.user + test artifacts
├─ src/
│  └─ Confluent.Kafka/
│     ├─ Confluent.Kafka.csproj               ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on ns2.0
│     │                                           leg only, GenerateDocumentationFile, InternalsVisibleTo → UnitTests (public key)
│     ├─ Internal/.gitkeep                    ← empty by design (D3), no types yet
│     └─ Internal/Interop/.gitkeep            ← empty by design (D3), P/Invoke boundary later
└─ tests/
   └─ Confluent.Kafka.UnitTests/
      ├─ Confluent.Kafka.UnitTests.csproj    ← TFMs net8.0;net10.0, xUnit +
      │                                                     Microsoft.NET.Test.Sdk, ProjectReference
      └─ TfmSentinelTests.cs                 ← one trivial TFM-sentinel smoke test
```

## Verification state (re-verified at branch HEAD after the M0/P1 rename)

- `dotnet restore` — clean.
- `dotnet build` — 0 warnings, 0 errors across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active.
- `dotnet test -f net10.0` — 1 passed, 0 failed (TFM sentinel).
- `dotnet format --verify-no-changes` — clean.
- Strong-naming — both assemblies signed with the shared key; matching
  public-key token `a6a493010a30d243`.
- **CI-only (not blocking this phase):** net8.0 test *run* requires the .NET 8
  runtime (only .NET 10 installed locally — `dotnet --list-runtimes` shows only
  Microsoft.NETCore.App 10.0.9); net462 test *run* requires Windows. Both build
  legs succeed; only the *runs* are deferred to CI.

Additional gates specific to M0/P1 (the rename), all green:

- The pre-rename `src/` and `tests/` project directories are fully gone from
  disk — stale `obj/` and `bin/` were destroyed *before* the moves, since
  `git mv` relocates only tracked files and untracked build output would
  otherwise have kept the old directories alive with a stale assembly and a
  cached `project.assets.json` naming the old `AssemblyName`.
- No tracked path carries the old identity (`git ls-files` is clean), and no
  build or code file mentions it. Two documentation surfaces name it
  deliberately: the archived M0/P0 record, and this file's transition narrative
  above — see *Governance pointers*.
- `.snk` byte-identical across the move (SHA-256 `d33f5c98…8eb197`); the
  `InternalsVisibleTo` `Key=` blob still equals `sn -tp` on the key file, and
  `Include=` matches the test project's `<AssemblyName>`.
- All 7 path changes recorded by git as **renames**, not delete+create.
- `Confluent.Kafka.sln` — a surgical 2-line edit (the two project entries): the
  7 GUIDs, `ProjectConfigurationPlatforms`, `NestedProjects`, the `build`
  folder's `SolutionItems`, and the UTF-8 BOM are all unchanged. The solution
  was **not** regenerated (SDK 10 would emit `.slnx`; classic `.sln` is a
  standing M0/P0 deviation).

## Decisions in force (from the approved plan)

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
- **D2** — Native-copy MSBuild target + Rust build DEFERRED to the first
  implementation phase — **M1/P0** (the skeleton has nothing to P/Invoke).
- **D3** — Empty folders via `.gitkeep`, no placeholder types.
- **D4** — Test framework = xUnit.

Deviations recorded during initial scaffolding (see the archived review record
under `design/history/M0/P0-scaffolding/`):
- Solution regenerated as classic `.sln` (SDK 10 defaults to `.slnx`).
- Test package versions pinned to offline-available builds: xunit 2.9.3,
  xunit.runner.visualstudio 2.8.2, Microsoft.NET.Test.Sdk 17.14.1,
  System.Memory 4.5.5.

## Post-plan additions (interactive, 2026-07-20)

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

- **M0/P0** — Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`:
  **0 genuine findings** — clean skeleton, verified by an independent
  build/test/format run. No fix cycle required.
- **M0/P1** — Critic (N=1) review pending at the time of writing; the Actor's
  own gates (above) are green.

## Governance pointers

Current phase (**M0/P1**):

- Approved plan: `design/history/M0/P1-rename-identity/PLAN.md`.
- Closed review record: `design/history/M0/P1-rename-identity/COMMENTS.DONE.1.md`
  (written when the Critic loop closes).

Previous phase (**M0/P0**):

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`. Carries a dated
  supersession note: the phase shipped under the
  `Confluent.Kafka.ShareConsumer` identity, and its body is preserved verbatim
  as the record of what was approved and verified at the time.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.DONE.1.md` —
  left **verbatim** on purpose. It records the Critic's *verified* finding about
  the then-current `InternalsVisibleTo` name, so editing it would make a
  historical verification claim describe an assembly name that did not exist
  when the check ran.

Active review file for the next requirement: `bindings/dotnet/COMMENTS.1.md`
(gitignored; reset — no open items).

## Next up (not started)

First implementation phase — **M1/P0** (Mode A per CLAUDE.md §6.2): wire the
`NativeMethods` P/Invoke class + a first `SafeHandle` + the native-copy MSBuild
target (D2 un-defers here), against the already-landed producer/consumer C ABI.
Requires `cargo build --features ffi` to produce the cdylib first
(CLAUDE.md §7.1).
