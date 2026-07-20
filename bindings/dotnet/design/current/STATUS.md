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
├─ Confluent.Kafka.ShareConsumer.sln        ← classic .sln, references both projects
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              common metadata (Confluent Inc.)
├─ .editorconfig                             ← dotnet/runtime style (_camelCase/s_, PascalCase,
│                                              Allman, System.* usings first, CA1715 I-prefix)
├─ .gitignore                                ← bin/ obj/ .DS_Store *.user + test artifacts
├─ src/
│  └─ Confluent.Kafka.ShareConsumer/
│     ├─ Confluent.Kafka.ShareConsumer.csproj  ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on ns2.0
│     │                                           leg only, InternalsVisibleTo → UnitTests
│     ├─ Internal/.gitkeep                    ← empty by design (D3), no types yet
│     └─ Internal/Interop/.gitkeep            ← empty by design (D3), P/Invoke boundary later
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/
      ├─ Confluent.Kafka.ShareConsumer.UnitTests.csproj  ← TFMs net8.0;net10.0, xUnit +
      │                                                     Microsoft.NET.Test.Sdk, ProjectReference
      └─ TfmSentinelTests.cs                 ← one trivial TFM-sentinel smoke test
```

## Verification state (as of phase close)

- `dotnet restore` — clean.
- `dotnet build` — 0 warnings, 0 errors across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active.
- `dotnet test -f net10.0` — 1 passed, 0 failed (TFM sentinel).
- `dotnet format --verify-no-changes` — clean.
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

Deviations recorded during execution (see the archived review record under
`design/history/M0/P0-scaffolding/`):
- Solution regenerated as classic `.sln` (SDK 10 defaults to `.slnx`).
- `GenerateDocumentationFile` dropped from the library csproj (would turn every
  undocumented public member into a build error under `TreatWarningsAsErrors`;
  out of scope for M0/P0, non-breaking to add later).
- Test package versions pinned to offline-available builds: xunit 2.9.3,
  xunit.runner.visualstudio 2.8.2, Microsoft.NET.Test.Sdk 17.14.1,
  System.Memory 4.5.5.

## Review outcome

Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`: **0 genuine
findings** — clean skeleton, verified by an independent build/test/format run.
No fix cycle required.

## Governance pointers

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.1.closed.md`.
- Active review file for the next requirement: `bindings/dotnet/COMMENTS.1.md`
  (reset — no open items).

## Next up (not started)

First implementation phase (Mode A per CLAUDE.md §6.2): wire the `Native`
P/Invoke class + a first `SafeHandle` + the native-copy MSBuild target (D2
un-defers here), against the already-landed producer/consumer C ABI. Requires
`cargo build --features ffi` to produce the cdylib first (CLAUDE.md §7.1).
