# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).**
  Pure structural skeleton stood up. No P/Invoke, no `Native`, no `SafeHandle`,
  no managed API, no Kafka logic yet — all deferred to later phases by scope.

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln        ← classic .sln, both projects + a "build" solution folder
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              common metadata (Confluent Inc.), strong-name signing (shared .snk, both projects)
├─ .editorconfig                             ← dotnet/runtime style (_camelCase/s_, PascalCase,
│                                              Allman, System.* usings first, CA1715 I-prefix)
├─ .gitignore                                ← bin/ obj/ .DS_Store *.user + test artifacts
├─ src/
│  └─ Confluent.Kafka.ShareConsumer/
│     ├─ Confluent.Kafka.ShareConsumer.csproj  ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on ns2.0
│     │                                           leg only, GenerateDocumentationFile, InternalsVisibleTo → UnitTests (public key)
│     ├─ Internal/.gitkeep                    ← empty by design (D3), no types yet
│     └─ Internal/Interop/.gitkeep            ← empty by design (D3), P/Invoke boundary later
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/
      ├─ Confluent.Kafka.ShareConsumer.UnitTests.csproj  ← TFMs net8.0;net10.0, xUnit +
      │                                                     Microsoft.NET.Test.Sdk, ProjectReference
      └─ TfmSentinelTests.cs                 ← one trivial TFM-sentinel smoke test
```

## Verification state (re-verified at branch HEAD, incl. post-plan additions)

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

## Decisions in force (from the approved plan)

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
- **D2** — Native-copy MSBuild target + Rust build DEFERRED to the first
  implementation phase (skeleton has nothing to P/Invoke).
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
- **Strong-naming** — one shared key `Confluent.Kafka.ShareConsumer.snk`,
  `SignAssembly` wired in `Directory.Build.props` (both projects), and the
  `InternalsVisibleTo` public key on the library csproj. Decided early on
  purpose: adding a strong name after the first published package is a
  binary-breaking change. The `.snk` is committed (identity, not a secret;
  publisher trust is NuGet/Authenticode signing at publish).
- **Review record renamed** — `COMMENTS.1.closed.md` → `COMMENTS.DONE.1.md`
  (the old name was swallowed by the repo-root `COMMENTS\.[0-9]*\.md` gitignore;
  the `DONE` name is tracked and matches the documented mechanics).

## Review outcome

Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`: **0 genuine
findings** — clean skeleton, verified by an independent build/test/format run.
No fix cycle required.

## Governance pointers

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.DONE.1.md`.
- Active review file for the next requirement: `bindings/dotnet/COMMENTS.1.md`
  (reset — no open items).

## Next up (not started)

First implementation phase (Mode A per CLAUDE.md §6.2): wire the `Native`
P/Invoke class + a first `SafeHandle` + the native-copy MSBuild target (D2
un-defers here), against the already-landed producer/consumer C ABI. Requires
`cargo build --features ffi` to produce the cdylib first (CLAUDE.md §7.1).
